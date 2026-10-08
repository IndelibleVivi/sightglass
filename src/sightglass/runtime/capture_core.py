"""Core orchestration of complete edge operations, outside the WindowDB writer.

The existing reader owns admission/ACK/delivery. Its final transaction records the
capture terminal too; only an after-commit local Event releases the edge stream.
Resource acquisition admits verified CAS bytes before independent processor work.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable
from dataclasses import MISSING, asdict, fields
from typing import Any

from sightglass.contracts.capture import CaptureExpectation, CaptureProtocolError, CaptureRequest
from sightglass.contracts.common import utc_now
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.resources import SourceResource
from sightglass.model.coverage import key_from_position, state_frontier
from sightglass.operations import operation_budget, operation_remaining_seconds
from sightglass.reader.service import ReaderService, _CatalogConversation, _SyncConversationPlan
from sightglass.resources.service import _ResolvedSource
from sightglass.source.capture import projection_origin_epoch
from sightglass.source.capture.codec import canonical_json, json_value, typed_value
from sightglass.source.capture.receiver import CaptureReceiver
from sightglass.source.capture.resource import resource_descriptor_digest
from sightglass.source.remote import RemoteCaptureProvider

from .activation import require_core_activation
from .capture_journal import CAPTURE_REQUEST_TOOL, WindowCaptureJournal, _identity
from .config import SightglassConfig
from .edge_relay import (
    CaptureBroker,
    CaptureBrokerServer,
    CaptureWaitTimeout,
    PreparedRemoteCapture,
    RelayDisconnected,
)

CAPTURE_WAIT_SECONDS = 15.0


class CoreCapture:
    def __init__(self, service: ReaderService, config: SightglassConfig) -> None:
        if not isinstance(service.provider, RemoteCaptureProvider):
            raise RuntimeError("core capture requires a remote source binding")
        self.service, self.config = service, config
        self.settings = service.provider.settings
        self.database = service.repository.database
        self.journal = WindowCaptureJournal(self.database)
        self.expected = CaptureExpectation(
            self.settings.account_id,
            self.settings.source_instance_id,
            projection_origin_epoch(service.provider),
            service._policy_revision(),
            self.settings.egress_revision,
            self.settings.conversations,
        )
        self.receiver = CaptureReceiver(
            self.journal, self.expected, stream_epoch=self.settings.stream_epoch
        )
        self.broker = CaptureBroker(
            self.expected,
            token_hash=self.settings.edge_token_hash,
            lookup_terminal=self.receiver.lookup_terminal,
            recover=self._recover,
            core_generation=config.activation_generation,
            ownership_guard=lambda: require_core_activation(config),
        )
        self.server = CaptureBrokerServer(self.broker, self.settings.socket_path)
        self._catalog_due = 0.0
        service.resource_service.remote_acquire = self.acquire_resource

    def start(self) -> None:
        self.server.start()

    def close(self) -> None:
        self.server.close()

    def status(self) -> dict[str, Any]:
        position = self.journal.stream_position(
            self.settings.source_instance_id, self.settings.account_id
        )
        return {
            "schema": "sightglass.capture-core-status.v1",
            "edge_connected": bool(getattr(self.broker, "connected", False)),
            "next_sequence": position.next_sequence if position else 1,
            "interpretation_epoch_preserved": (
                self.expected.origin_epoch == self.service._projection_inventory_epoch()
            ),
        }

    def _authorize(self, request: CaptureRequest) -> None:
        self.service.reader.require_active()
        if request.policy_revision != self.service._policy_revision():
            raise SightglassError(ErrorCode.POLICY_DENIED)
        sources = tuple(getattr(request, "conversation_source_ids", ()))
        if request.conversation_source_id is not None:
            sources += (request.conversation_source_id,)
        account_id = self.service.repository.account_id_for(request.account_id)
        for source_id in sources:
            self.service.reader.authorize(
                self.service.repository.conversation_id_for(account_id, source_id)
            )

    def _record_request(self, request: CaptureRequest) -> None:
        self._authorize(request)
        now = utc_now().isoformat()
        metadata = canonical_json(
            {"generation": self.config.activation_generation, "request": json_value(request)}
        ).decode()
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO access_receipts(receipt_id,reader_id,tool_name,conversation_id,"
                "scope_kind,scope_digest,message_count,resource_count,bytes_returned,started_at,"
                "completed_at,outcome,warning_codes_json) "
                "VALUES(?,?,?,NULL,'capture-request',NULL,0,0,0,?,?,'pending',?)",
                (
                    _identity("request", request.request_id),
                    self.service.reader.reader_id,
                    CAPTURE_REQUEST_TOOL,
                    now,
                    now,
                    metadata,
                ),
            )

    def _claim_request(self, request: CaptureRequest, outcome: str) -> None:
        # The hook runs in the reader's outer writer. If timeout/recovery won,
        # its failure rolls back all bodies/ACK/delivery already staged here.
        admissible = (
            ("pending",)
            if outcome == "accepted"
            else ("pending", "detached", "disconnected", "failed")
        )
        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT warning_codes_json FROM access_receipts WHERE receipt_id=? AND tool_name=?",
                (_identity("request", request.request_id), CAPTURE_REQUEST_TOOL),
            ).fetchone()
            metadata = json.loads(row[0]) if row is not None else {}
            if metadata.get("generation") != self.config.activation_generation or canonical_json(
                metadata.get("request")
            ) != canonical_json(request):
                raise CaptureProtocolError("capture_request_binding_changed")
            slots = ",".join("?" for _ in admissible)
            changed = connection.execute(
                f"UPDATE access_receipts SET outcome=? WHERE receipt_id=? AND tool_name=? "
                f"AND outcome IN ({slots})",
                (
                    outcome,
                    _identity("request", request.request_id),
                    CAPTURE_REQUEST_TOOL,
                    *admissible,
                ),
            )
            if changed.rowcount != 1:
                raise CaptureProtocolError("capture_request_terminal_conflict")

    def _failed_request(self, request: CaptureRequest, outcome: str) -> None:
        # Preserve the exact binding for a late sealed receipt's recovery rejection.
        try:
            with operation_budget(2.0):
                with self.database.transaction(maintenance=True) as connection:
                    connection.execute(
                        "UPDATE access_receipts SET outcome=? "
                        "WHERE receipt_id=? AND tool_name=? AND outcome='pending'",
                        (outcome, _identity("request", request.request_id), CAPTURE_REQUEST_TOOL),
                    )
        except SightglassError:
            # The durable exact request binding remains. A late sealed capture
            # still enters reject recovery; diagnostics cannot mask its caller error.
            pass

    def _submit(self, values: dict[str, Any]) -> PreparedRemoteCapture:
        values = {
            **values,
            "request_id": "sgreq_" + uuid.uuid4().hex,
            "policy_revision": self.service._policy_revision(),
        }
        decoded = {}
        for field in fields(CaptureRequest):
            value = values.get(field.name, field.default)
            if value is MISSING:
                raise CaptureProtocolError("capture_request_field_required")
            decoded[field.name] = json_value(value)
        request: CaptureRequest = typed_value(CaptureRequest, decoded)
        if not bool(getattr(self.broker, "connected", False)):
            raise SightglassError(
                ErrorCode.SERVICE_UNAVAILABLE, retryable=True, details={"reason": "edge_offline"}
            )
        self._record_request(request)
        remaining = operation_remaining_seconds()
        timeout = (
            min(CAPTURE_WAIT_SECONDS, remaining) if remaining is not None else CAPTURE_WAIT_SECONDS
        )
        try:
            return self.broker.submit(request, timeout=max(0.1, timeout))
        except (CaptureWaitTimeout, RelayDisconnected) as exc:
            self._failed_request(
                request, "detached" if isinstance(exc, CaptureWaitTimeout) else "disconnected"
            )
            raise SightglassError(
                ErrorCode.SERVICE_UNAVAILABLE,
                retryable=True,
                details={"reason": "edge_capture_unavailable"},
            ) from exc
        except CaptureProtocolError as exc:
            self._failed_request(request, "failed")
            raise SightglassError(
                ErrorCode.SOURCE_SNAPSHOT_FAILED,
                retryable=True,
                details={"reason": "capture_protocol_rejected"},
            ) from exc

    def _hooks(
        self,
        ticket: PreparedRemoteCapture,
    ) -> tuple[Callable[[Any], None], Callable[[], None]]:
        prepared = self.receiver.prepare(ticket.envelope)
        acknowledgements = []

        def before_commit(_connection: Any) -> None:
            self._authorize(ticket.request)
            self._claim_request(ticket.request, "accepted")
            ack = self.receiver.admit_in_transaction(
                prepared,
                lambda _provider, _document: None,
                receipt_id=_identity("terminal", ticket.request.request_id),
            )
            if ack.terminal != "accepted":
                raise CaptureProtocolError("capture_request_terminal_conflict")
            acknowledgements.append(ack)

        def after_commit() -> None:
            ticket.complete(acknowledgements[-1])

        return before_commit, after_commit

    def _reject(self, ticket: PreparedRemoteCapture) -> None:
        # A failed/cancelled request releases the ordered stream without advancing
        # reader ACK or admitting a body. This bounded transaction contains no RPC.
        try:
            prepared = self.receiver.prepare(ticket.envelope)
            with operation_budget(2.0):
                with self.database.transaction(maintenance=True):
                    require_core_activation(self.config)
                    duplicate = self.receiver.lookup_terminal(ticket.envelope)
                    if duplicate is not None:
                        self.database.wake_after_commit(lambda: ticket.complete(duplicate))
                        return
                    terminal = prepared.document.receipt.terminal
                    self._claim_request(
                        ticket.request, "cancelled" if terminal == "cancelled" else "rejected"
                    )
                    ack = self.receiver.admit_in_transaction(
                        prepared,
                        None,
                        receipt_id=_identity("terminal", ticket.request.request_id),
                        reject=True,
                    )
                    self.database.wake_after_commit(lambda: ticket.complete(ack))
        finally:
            if ticket.durable_ack is None:
                self.broker.abandon(ticket)

    def _recover(self, ticket: PreparedRemoteCapture) -> None:
        # Reconnect can cancel an exact previously authorized pending task. It may
        # never silently accept an unknown capture or reconstruct query text.
        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT warning_codes_json FROM access_receipts WHERE receipt_id=? AND tool_name=?",
                (_identity("request", ticket.request.request_id), CAPTURE_REQUEST_TOOL),
            ).fetchone()
        if row is None:
            raise CaptureProtocolError("unknown_capture_request_requires_operator_recovery")
        metadata = json.loads(row[0])
        if metadata.get("generation") != self.config.activation_generation or canonical_json(
            metadata.get("request")
        ) != canonical_json(ticket.request):
            raise CaptureProtocolError("capture_recovery_binding_mismatch")
        self._reject(ticket)

    def _verify_values(self, ids: tuple[str, ...], arguments: dict[str, Any]) -> dict[str, Any]:
        rows = [self.service.repository.message_position_row(identity) for identity in ids]
        if any(row is None for row in rows):
            raise SightglassError(ErrorCode.MESSAGE_NOT_FOUND)
        conversations: set[str] = set()
        account_id = self.service.repository.account_id_for(self.settings.account_id)
        for row in rows:
            assert row is not None
            if str(row["account_id"]) != account_id:
                raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)
            context = self.service.repository.conversation_context(str(row["conversation_id"]))
            if context is None:
                raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
            conversations.add(str(context["source_conversation_id"]))
        if not conversations:
            if arguments.get("account_id") not in {None, account_id}:
                raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)
            selected = arguments.get("conversation_ids") or tuple(
                str(row["conversation_id"])
                for row in self.service.repository.account_conversations(account_id)
                if self.service.reader.policy.permits(str(row["conversation_id"]))
                and str(row["source_conversation_id"]) in self.settings.conversations
            )
            for conversation_id in selected:
                context = self.service.repository.conversation_context(str(conversation_id))
                if context is not None:
                    conversations.add(str(context["source_conversation_id"]))
        # An empty local result confirms a bounded current source lease; it never
        # claims that the full source history was searched.
        conversations = set(sorted(conversations)[:200])
        return {
            "operation": "verify",
            "account_id": self.settings.account_id,
            "conversation_source_ids": sorted(conversations),
            "limit": 200,
            "message_ids": [str(row["source_message_id"]) for row in rows if row is not None],
        }

    def call(self, name: str, arguments: dict[str, Any], invoke: Callable[[], Any]) -> Any:
        if self.service.local_only_tool_call(name, arguments):
            return invoke()
        if name == "wechat_read_resource":
            return invoke()  # ResourceService acquires its exact locator once below.
        search_ids: tuple[str, ...] | None = None
        discovery = None
        updates_after = None
        updates_message_ids = None
        updates_reconcile_revision = None
        if name == "wechat_read_messages":
            plan = self.service.replica.prepare_fresh_message_capture(arguments)
            values = {
                key: value
                for key, value in asdict(plan).items()
                if key in CaptureRequest.__dataclass_fields__
            }
            values["account_id"] = plan.source_account_key
            updates_after = plan.after if arguments.get("mode") == "updates" else None
            if arguments.get("mode") == "updates":
                updates_message_ids = plan.updates_message_ids
                updates_reconcile_revision = plan.updates_reconcile_revision
        elif name == "wechat_search_messages":
            search_ids = self.service.replica.fresh_search_candidate_ids(arguments)
            values = self._verify_values(search_ids, arguments)
        elif name in {"wechat_find_links", "wechat_retrieve"}:
            discovery = self.service.replica.prepare_fresh_discovery(name, arguments)
            values = self._verify_values(discovery.message_ids, arguments)
        else:
            # All other ordinary tools retain their local replica contract. This
            # branch does not manufacture RPC calls for individual provider methods.
            return invoke()
        ticket = self._submit(values)
        try:
            before, after = self._hooks(ticket)
            with self.service.captured_provider(
                ticket.provider,
                before_commit=before,
                after_commit=after,
                search_candidate_ids=search_ids,
                updates_after=updates_after,
                updates_message_ids=updates_message_ids,
                updates_reconcile_revision=updates_reconcile_revision,
                discovery=discovery,
            ):
                result = invoke()
            if ticket.durable_ack is None:
                self._reject(ticket)
            return result
        except BaseException:
            if ticket.durable_ack is None:
                self._reject(ticket)
            raise

    def _admit_catalog(self, ticket: PreparedRemoteCapture) -> None:
        before, after = self._hooks(ticket)
        with self.service.captured_provider(
            ticket.provider,
            before_commit=before,
            after_commit=after,
        ):
            with self.service._source_read() as (stack, snapshot):
                catalog = self.service._read_catalog(snapshot)
                facts = self.service._catalog_facts(snapshot)
                with self.service._admission(stack):
                    self.service._persist_catalog(catalog, snapshot)
                    for account_id, _ in catalog.accounts:
                        current = {
                            entry.conversation_id
                            for entry in catalog.catalog
                            if entry.account_id == account_id
                            and self.service.reader.policy.permits(entry.conversation_id)
                        }
                        handled = self.service._catalog_handled_ids(
                            account_id,
                            projection_epoch=self.expected.origin_epoch,
                        )
                        previous = self.service.repository.source_catalog_state(account_id)
                        self.service.repository.record_source_catalog_state(
                            account_id=account_id,
                            inventory_epoch=(
                                self.expected.origin_epoch
                                if current <= handled
                                else snapshot.inventory_digest
                            ),
                            coverage_state="complete" if facts.complete else "partial",
                            observed_at=snapshot.fresh_as_of,
                            next_cursor_token=(previous["next_cursor_token"] if previous else None),
                        )

    def sync_once(self) -> dict[str, Any]:
        result = {
            "schema": "sightglass.source-sync.v1",
            "conversation_count": 0,
            "message_count": 0,
            "pending_conversation_count": 0,
            "conflict_conversation_count": 0,
        }
        self.service.reader.require_active()
        self.service.residency.release_expired_leases(limit=100)
        if time.monotonic() >= self._catalog_due:
            ticket = self._submit({"operation": "catalog", "account_id": self.settings.account_id})
            try:
                self._admit_catalog(ticket)
            except BaseException:
                if ticket.durable_ack is None:
                    self._reject(ticket)
                raise
            self._catalog_due = time.monotonic() + 30.0
            return result
        account_id = self.service.repository.account_id_for(self.settings.account_id)
        entries = [
            row
            for row in self.service.repository.account_conversations(account_id)
            if self.service.reader.policy.permits(str(row["conversation_id"]))
            and self.service._residency_decision(str(row["conversation_id"])).collect_bodies
            and str(row["source_conversation_id"]) in self.settings.conversations
        ]
        if not entries:
            return result
        catalog_state = self.service.repository.source_catalog_state(account_id)
        if catalog_state is None:
            self._catalog_due = 0.0
            return result
        ordered = sorted(entries, key=lambda row: str(row["conversation_id"]))
        cursor = self.service._decode_catalog_cursor(
            catalog_state["next_cursor_token"],
            account_id=account_id,
        )
        chosen = next((row for row in ordered if str(row["conversation_id"]) == cursor), ordered[0])
        successor = ordered[(ordered.index(chosen) + 1) % len(ordered)]
        conversation_id = str(chosen["conversation_id"])
        state = self.service.repository.source_conversation_state(conversation_id)
        frontier = state_frontier(state)
        initial = (
            state is None or str(state["source_inventory_epoch"]) != self.expected.origin_epoch
        )
        values: dict[str, Any] = {
            "account_id": self.settings.account_id,
            "conversation_source_id": str(chosen["source_conversation_id"]),
            "operation": "recent" if initial else "range",
            "limit": 200,
            "direction": "backward" if initial else "forward",
        }
        if not initial:
            values["after"] = json_value(frontier)
        ticket = self._submit(values)
        try:
            before, after = self._hooks(ticket)
            with self.service.captured_provider(
                ticket.provider, before_commit=before, after_commit=after
            ):
                with self.service._source_read() as (stack, snapshot):
                    catalog = self.service._read_catalog(snapshot)
                    entry = next(
                        item for item in catalog.catalog if item.conversation_id == conversation_id
                    )
                    sources = ticket.envelope.document().evidence.messages
                    coverage = ticket.envelope.document().receipt.coverage
                    if initial:
                        floor = min(
                            (item.sort_key for item in sources),
                            key=lambda key: key.as_tuple(),
                            default=None,
                        )
                        history_complete = not coverage.has_more_before
                    else:
                        assert state is not None
                        floor = key_from_position(state["contiguous_floor_position"])
                        history_complete = bool(state["history_complete"])
                    plan = _SyncConversationPlan(
                        _CatalogConversation(
                            account_id, self.settings.account_id, conversation_id, entry.source
                        ),
                        self.service._prepare_messages(sources),
                        coverage.has_more_after,
                        None if initial else frontier,
                        floor,
                        history_complete,
                    )
                    with self.service._admission(stack):
                        self.service._persist_catalog(catalog, snapshot)
                        count, pending = self.service._admit_sync_plan(
                            plan,
                            snapshot,
                            inventory_epoch=self.expected.origin_epoch,
                        )
                        self.service.repository.record_source_catalog_state(
                            account_id=account_id,
                            inventory_epoch=str(catalog_state["source_inventory_epoch"]),
                            coverage_state=str(catalog_state["coverage_state"]),
                            observed_at=str(catalog_state["last_observed_at"]),
                            next_cursor_token=self.service._encode_catalog_cursor(
                                account_id,
                                str(successor["conversation_id"]),
                            ),
                        )
            result.update(
                conversation_count=1, message_count=count, pending_conversation_count=int(pending)
            )
            return result
        except BaseException:
            if ticket.durable_ack is None:
                self._reject(ticket)
            raise

    def acquire_resource(self, row: Any, captured_revision: str) -> _ResolvedSource:
        resources = self.service.resource_service
        context = self.service.repository.conversation_context(str(row["conversation_id"]))
        if context is None:
            raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        descriptor = SourceResource(
            int(row["source_ordinal"]),
            str(row["kind"]),
            str(row["source_resource_key"]),
            row["mime_type"],
            row["original_name"],
            row["declared_size"],
            row["declared_hash"],
            str(row["availability"]),
        )
        revision = resources.resource_revision_fields(row)
        variant = (
            "original"
            if row["availability"] in {"local_available", "archive_available"}
            else "thumbnail"
        )
        ticket = self._submit(
            {
                "operation": "resource",
                "account_id": str(context["source_account_key"]),
                "conversation_source_id": str(context["source_conversation_id"]),
                "focus_source_message_id": str(row["source_message_id"]),
                "resource_key": str(row["source_resource_key"]),
                "resource_variant": variant,
                "resource_descriptor": json_value(descriptor),
                "resource_descriptor_digest": resource_descriptor_digest(descriptor),
                "expected_resource_revision": captured_revision,
                "resource_revision_json": json.dumps(
                    revision, sort_keys=True, separators=(",", ":")
                ),
            }
        )
        try:
            before, after = self._hooks(ticket)
            provider = ticket.provider
            with provider.snapshot() as snapshot:
                payload = provider.read_resource(
                    str(row["source_resource_key"]), max_bytes=32 * 1024**2, snapshot=snapshot
                )
            staged = []
            original, warnings, source_variant = resources._ensure_source_payload(
                row,
                snapshot,
                staged,
                acquired=(payload.data, payload.variant),
            )
            prior = tuple(
                (variant, str(binding["object_digest"]))
                for variant in ("original", "thumbnail")
                if (
                    binding := self.service.repository.resource_binding(
                        str(row["resource_id"]), variant
                    )
                )
                is not None
            )
            resolved = _ResolvedSource(
                original,
                tuple(warnings),
                source_variant,
                resources._source_receipt(snapshot),
                captured_revision,
                prior,
            )
            with self.database.transaction() as connection:
                resources._verify_resolved_revision(str(row["resource_id"]), resolved)
                resources._persist_staged(staged)
                before(connection)
                self.database.wake_after_commit(after)
            return resolved
        except BaseException:
            if ticket.durable_ack is None:
                self._reject(ticket)
            raise

    def backfill_once(self) -> dict[str, Any]:
        eligible = tuple(
            value
            for value in self.service.residency.historical_conversation_ids()
            if self.service.reader.policy.permits(value)
        )
        job = self.service.repository.next_backfill_job(eligible)
        if job is None:
            return {"schema": "sightglass.backfill-step.v1", "state": "idle"}
        if int(job["processed_messages"]) >= int(job["max_messages"]):
            return self.service.process_backfill_once(batch_limit=199)
        target = self.service._persisted_source_target(str(job["conversation_id"]))
        state = self.service.repository.source_conversation_state(target.conversation_id)
        earliest = (
            key_from_position(state["contiguous_floor_position"])
            if state is not None and state["coverage_version"]
            else None
        )
        count = min(199, int(job["max_messages"]) - int(job["processed_messages"]))
        ticket = self._submit(
            {
                "operation": "range",
                "account_id": target.source_account_key,
                "conversation_source_id": target.source_conversation_id,
                "after": None,
                "before": json_value(earliest),
                "direction": "backward",
                "limit": count + 1,
                "time_after_utc": job["requested_after"],
                "time_before_utc": job["requested_before"],
            }
        )
        try:
            before, after = self._hooks(ticket)
            with self.service.captured_provider(
                ticket.provider, before_commit=before, after_commit=after
            ):
                return self.service.process_backfill_once(batch_limit=199)
        except BaseException:
            if ticket.durable_ack is None:
                self._reject(ticket)
            raise

    def catalog_call(self, invoke: Callable[[], Any]) -> Any:
        ticket = self._submit({"operation": "catalog", "account_id": self.settings.account_id})
        try:
            before, after = self._hooks(ticket)
            with self.service.captured_provider(
                ticket.provider, before_commit=before, after_commit=after
            ):
                result = invoke()
            if ticket.durable_ack is None:
                self._reject(ticket)
            return result
        except BaseException:
            if ticket.durable_ack is None:
                self._reject(ticket)
            raise

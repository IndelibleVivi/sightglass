"""Admitted, bounded local reads and atomic update-request outcome correlation."""

from __future__ import annotations

import copy
import inspect
import json
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from sightglass.contracts.common import SourceSortKey, parse_aware_datetime, utc_now
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.identity import SourceParticipantFilter
from sightglass.contracts.messages import ParsedMessage, SourceMessagePage
from sightglass.model.coverage import record_window
from sightglass.model.current_body import current_search_document
from sightglass.model.lexical import LEXICAL_RECIPE
from sightglass.model.repositories import message_search_fields
from sightglass.operations import check_operation_budget
from sightglass.reader.cursors import cursor_scope, search_scope_digest
from sightglass.reader.projections import CompactBodyBudgetAllocator
from sightglass.source.base import SourceHealth, SourceScope, SourceSnapshot
from sightglass.source.identity import opaque_id

if TYPE_CHECKING:
    from sightglass.reader.service import ReaderService

UPDATE_REQUEST_TOOL = "_sightglass_updates_request.v1"
UPDATE_REQUEST_SCHEMA = "sightglass.updates-request.v1"
UPDATE_REQUEST_REPLAY_DAYS = 30
UPDATE_FRONTIER_TOOL = "_sightglass_updates_frontier.v1"
UPDATE_FRONTIER_SCHEMA = "sightglass.updates-frontier.v1"
UPDATE_RECONCILE_SCHEMA = "sightglass.updates-reconcile.v1"
REPLICA_SEARCH_BUDGET = 2_000


@dataclass(frozen=True)
class PreparedFreshDiscovery:
    tool: str
    page: dict[str, Any]
    message_ids: tuple[str, ...]
    policy_revision: str
    epoch: str
    watermark: int
    request_digest: str


@dataclass(frozen=True)
class MessageCapturePlan:
    operation: str
    account_id: str
    source_account_key: str
    conversation_id: str
    conversation_source_id: str
    limit: int
    direction: str
    after: SourceSortKey | None = None
    before: SourceSortKey | None = None
    time_after_utc: str | None = None
    time_before_utc: str | None = None
    participant_filters: tuple[SourceParticipantFilter, ...] = ()
    focus_source_message_id: str | None = None
    context_before: int = 0
    context_after: int = 0
    message_ids: tuple[str, ...] = ()
    # The opaque signed dependency fence is checked against the sealed provider
    # during the canonical boundary read. No account-wide generation is guessed.
    cursor_source_binding: dict[str, Any] | None = None
    expected_generations: tuple[tuple[str, str], ...] = ()
    updates_message_ids: tuple[str, ...] | None = None
    updates_reconcile_revision: int | None = None


def request_spool_ids(connection: Any) -> set[str]:
    """Unexpired private spool IDs retained by GC and paired-state copying.

    Absolute paths and query text are never persisted in the correlation record.
    Operators may reclaim these spools after the declared replay horizon.
    """
    retained = set()
    for row in connection.execute(
        "SELECT warning_codes_json FROM access_receipts WHERE tool_name=?", (UPDATE_REQUEST_TOOL,)
    ):
        try:
            metadata = json.loads(row[0])
            if (metadata["schema"] == UPDATE_REQUEST_SCHEMA
                    and parse_aware_datetime(metadata["expires_at"]) > utc_now()
                    and re.fullmatch(r"[A-Za-z0-9_-]+", metadata["payload_id"])):
                retained.add(metadata["payload_id"])
        except (ValueError, KeyError, TypeError):
            continue
    return retained


class ReplicaReader:
    def __init__(self, service: ReaderService) -> None:
        self.service = service
        self.repository = service.repository

    def _account(self, requested: str | None) -> str:
        accounts = self.repository.active_account_ids()
        selected = requested or (accounts[0] if len(accounts) == 1 else None)
        if selected not in accounts:
            raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)
        assert selected is not None
        return selected

    def status(self) -> dict[str, Any]:
        accounts = [
            {"account_id": str(row["account_id"]),
             "display_name": str(row["current_display_name"]), "active": bool(row["active"])}
            for row in self.repository.active_accounts()
        ]
        result = self.service._status_payload(
            SourceHealth(configured=True, available=False, account_count=len(accounts),
                         source_state="unknown", fresh_as_of="", inventory_digest="",
                         generation_set_digest="", shard_counts={},
                         warnings=("replica_view_not_live",)),
            accounts,
        )
        result["read_plane"]["default_view"] = "replica"
        result["read_plane"]["freshness"] = "bounded_stale"
        return result

    def fresh_search_candidate_ids(self, arguments: dict[str, Any]) -> tuple[str, ...]:
        """Pure local prefix for one sealed current-source verification operation."""
        with self.repository.database.read_snapshot():
            scope = self.service.retrieval._scope(
                arguments.get("account_id"), tuple(arguments.get("conversation_ids") or ()),
                tuple(arguments.get("participant_ids") or ()), arguments.get("after"),
                arguments.get("before"),
            )
            after_key = None
            cursor = arguments.get("cursor")
            if cursor:
                payload = self.service.token_codec.decode(cursor)
                if payload.get("kind") != "search":
                    raise SightglassError(ErrorCode.CURSOR_INVALID)
                position = payload.get("position", {}).get("sort")
                if not isinstance(position, list) or len(position) != 4:
                    raise SightglassError(ErrorCode.CURSOR_INVALID)
                after_key = tuple(position)
            rows = self.repository.search_candidate_window(
                scope.conversation_ids, after_key=after_key, after_utc=scope.after,
                before_utc=scope.before, lexical_queries=(" ".join(self.service._query_parts(
                    arguments.get("query", ""))),), limit=200,
            )
            return tuple(str(row["message_id"]) for row in rows)

    def _delivery_scope(
        self, delivery_id: str, conversation_id: str, scope_kind: str, scope_key: str,
    ) -> Any:
        delivery = self.repository.delivery(delivery_id)
        if delivery is None or any(str(delivery[key]) != value for key, value in (
            ("reader_id", self.service.reader.reader_id), ("conversation_id", conversation_id),
            ("scope_kind", scope_kind), ("scope_key", scope_key),
        )) or delivery["status"] not in {"pending", "acknowledged"}:
            raise SightglassError(ErrorCode.DELIVERY_ACK_INVALID)
        return delivery

    def _effective_update_sequence(
        self, conversation_id: str, scope_kind: str, scope_key: str,
        ack_delivery_id: str | None,
    ) -> int:
        committed = self.repository.update_position(
            self.service.reader.reader_id, conversation_id, scope_kind, scope_key)
        if ack_delivery_id is not None:
            delivery = self._delivery_scope(ack_delivery_id, conversation_id, scope_kind, scope_key)
            committed = max(committed, int(delivery["to_observation_seq"]))
        return committed

    def fresh_updates_after(
        self, conversation_id: str, *, scope_kind: str = "conversation", scope_key: str = "*",
        ack_delivery_id: str | None = None,
    ) -> SourceSortKey | None:
        self.service._conversation_context(conversation_id)
        epoch = self.service._projection_inventory_epoch()
        committed = self._effective_update_sequence(
            conversation_id, scope_kind, scope_key, ack_delivery_id)
        timeline = self.repository.timeline_position(
            self.service.reader.reader_id, conversation_id, scope_kind, scope_key)
        keys = []
        if timeline is not None:
            row = self.repository.message_position_row(str(timeline["committed_message_id"]))
            if row is not None and row["projection_epoch"] == epoch:
                keys.append(self.service._row_sort_key(row))
        if ack_delivery_id is not None:
            frontier = self._update_frontier(
                ack_delivery_id, conversation_id, scope_kind, scope_key)
            if frontier is not None:
                keys.append(frontier)
        if committed:
            latest = self.repository.latest_observed_message_position(
                conversation_id, epoch, observation_watermark=committed)
            if latest is not None:
                keys.append(self.service._row_sort_key(latest))
        return max(keys, key=lambda key: key.as_tuple()) if keys else None

    def _reconcile_id(self, conversation_id: str) -> str:
        return opaque_id("wxupdatescan", self.service.reader.reader_id, conversation_id)

    def _reconcile_scope(self, conversation_id: str) -> str:
        return self.service._scope_digest({
            "conversation_id": conversation_id, **self._request_policy_scope(),
        })

    def fresh_updates_reconcile_position(
        self, conversation_id: str,
    ) -> tuple[SourceSortKey | None, int, int]:
        """One conversation scan coordinate, independent of every delivery/ACK scope.

        Policy/epoch changes restart the bounded scan, while acknowledged reader
        progress remains intact. A revision fences a completed-pass reset to None.
        """
        self.service.reader.require_active()
        self.service._conversation_context(conversation_id)
        cursor = self.repository.update_reconciliation_cursor(self._reconcile_id(conversation_id))
        if cursor is None or cursor["scope_digest"] != self._reconcile_scope(conversation_id):
            return None, 0, 0
        try:
            metadata = json.loads(cursor["warning_codes_json"])
            revision, scan_pass = metadata["revision"], metadata["pass"]
            position = metadata["after"]
            if (metadata["schema"] != UPDATE_RECONCILE_SCHEMA
                    or type(revision) is not int or revision < 1
                    or type(scan_pass) is not int or scan_pass < 0
                    or position is not None and (not isinstance(position, list)
                                                or len(position) != 4)):
                raise ValueError("invalid reconciliation cursor")
            return SourceSortKey(*position) if position else None, revision, scan_pass
        except (KeyError, TypeError, ValueError) as exc:
            raise SightglassError(ErrorCode.CURSOR_STALE) from exc

    @staticmethod
    def capture_message_bounds(
        *, mode: str, before: int, after: int, bounded: int,
        speaker_view: str, query: str | None, cursor: bool = False,
    ) -> tuple[int, int, int, int]:
        budget = 200 - int(cursor)
        if mode == "context" or mode == "speaker" and speaker_view == "with_context":
            if before + after + 1 > budget:
                raise SightglassError(
                    ErrorCode.QUERY_INVALID,
                    details={"reason": "capture context window exceeds bounded message budget",
                             "max_messages": budget},
                )
        bounded = min(bounded, budget)
        if mode == "speaker" and speaker_view == "with_context":
            bounded = min(bounded, max(1, budget // (before + after + 1)))
        fetch = min(budget, bounded) if mode != "speaker" else (
            max(1, budget // (before + after + 1))
            if speaker_view == "with_context" else budget if query else bounded)
        return before, after, bounded, fetch

    def prepare_fresh_message_capture(self, arguments: dict[str, Any]) -> MessageCapturePlan:
        """Pure, authorized capture plan using the canonical request/cursor validation."""
        selected = {key: value for key, value in arguments.items() if key != "response_profile"}
        selected.setdefault("mode", "recent")
        selected["participant_ids"] = tuple(selected.get("participant_ids") or ())
        selected["view"] = "fresh"
        settings = self.service._validate_message_arguments(**selected)
        self.service.reader.require_active()
        mode = selected["mode"]
        conversation_id = selected.get("conversation_id")
        target_row = None
        if selected.get("anchor"):
            target_row = self.service._local_anchor_row(selected["anchor"], conversation_id)
        elif mode in {"message", "context"}:
            target_row = self.repository.message_position_row(selected.get("message_id", ""))
            if target_row is None:
                raise SightglassError(ErrorCode.MESSAGE_NOT_FOUND,
                                     details={"coverage": {"state": "not_yet_observed"}})
        if target_row is not None:
            if conversation_id and target_row["conversation_id"] != conversation_id:
                raise SightglassError(ErrorCode.MESSAGE_NOT_FOUND)
            if (selected.get("message_id")
                    and target_row["message_id"] != selected["message_id"]):
                raise SightglassError(ErrorCode.CURSOR_INVALID)
            conversation_id = str(target_row["conversation_id"])
        if not conversation_id:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        context = self.service._conversation_context(conversation_id)
        filters = ()
        participants = settings["participant_ids"]
        if participants:
            filters = self.repository.participant_source_filters(conversation_id, participants)
        before, after, bounded, fetch = self.capture_message_bounds(
            mode=mode, before=selected.get("before", 30), after=selected.get("after", 20),
            bounded=settings["bounded"], speaker_view=selected.get("speaker_view", "only"),
            query=settings["query"],
            cursor=bool(selected.get("cursor")),
        )
        direction = selected.get("direction", "backward")
        boundary = None
        binding = None
        cursor = selected.get("cursor")
        if cursor:
            payload = self.service.timeline_cursors.verify(
                cursor, reader_id=self.service.reader.reader_id,
                account_id=str(context["account_id"]), conversation_id=conversation_id,
                mode=mode, direction=direction, scope_kind=settings["scope_kind"],
                scope_key=settings["scope_key"], view="fresh",
            )
            row = self.repository.message_position_row(payload["position"]["message_id"])
            if (row is None or row["conversation_id"] != conversation_id
                    or payload["position"]["sort"] != [row["sort_primary"], row["sort_seq"],
                                                       row["sort_tie"]]):
                raise SightglassError(ErrorCode.CURSOR_STALE)
            if (payload["source"].get("projection_epoch")
                    != self.service._projection_inventory_epoch()):
                raise SightglassError(ErrorCode.CURSOR_STALE)
            boundary = self.service._row_sort_key(row)
            binding = payload["source"]
        operation = "recent" if mode == "recent" and cursor is None else "range"
        focus_id = str(target_row["source_message_id"]) if target_row is not None else None
        message_ids = (boundary.source_message_id,) if boundary else ()
        if mode == "message":
            operation, message_ids = "verify", (focus_id,) if focus_id else ()
        elif mode == "context":
            operation = "context"
        if mode == "updates":
            scope_kind, scope_key = cursor_scope(participants, settings["query"])
            committed = self._effective_update_sequence(
                conversation_id, scope_kind, scope_key, selected.get("ack_delivery_id"))
            candidates = self.repository.observation_rows_after(
                conversation_id, committed, participants,
                projection_epoch=self.service._projection_inventory_epoch(), limit=200,
            )
            updates_ids = tuple(str(row["message_id"]) for row in candidates) or None
            if updates_ids is not None:
                operation = "verify"
                message_ids = tuple(str(row["source_message_id"]) for row in candidates)
                boundary = None
                reconcile_revision = None
            else:
                operation = "range"
                boundary, reconcile_revision, _ = self.fresh_updates_reconcile_position(
                    conversation_id)
            direction, fetch = "forward", 200
        else:
            updates_ids = None
            reconcile_revision = None
        return MessageCapturePlan(
            operation, str(context["account_id"]), str(context["source_account_key"]),
            conversation_id, str(context["source_conversation_id"]),
            fetch if mode in {"speaker", "updates"} else bounded,
            direction, after=boundary if direction == "forward" else None,
            before=boundary if direction == "backward" else None,
            time_after_utc=settings["time_after"], time_before_utc=settings["time_before"],
            participant_filters=filters if mode == "speaker" else (),
            focus_source_message_id=focus_id,
            context_before=before if mode == "context" or (
                mode == "speaker" and selected.get("speaker_view") == "with_context") else 0,
            context_after=after if mode == "context" or (
                mode == "speaker" and selected.get("speaker_view") == "with_context") else 0,
            message_ids=message_ids, cursor_source_binding=binding,
            updates_message_ids=updates_ids,
            updates_reconcile_revision=reconcile_revision,
        )

    def prepare_fresh_discovery(
        self, tool: str, arguments: dict[str, Any],
    ) -> PreparedFreshDiscovery:
        selected = {key: value for key, value in arguments.items() if key != "response_profile"}
        selected["view"] = "fresh"
        if tool == "wechat_find_links":
            page = self.service.retrieval._find_links(**selected)
        elif tool == "wechat_retrieve":
            page = self.service.retrieval._retrieve(**selected)
        else:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        ids = self._result_message_ids(page)
        if len(ids) > 200:
            raise SightglassError(ErrorCode.SOURCE_INCOMPLETE,
                                 details={"warning_codes": ["capture_result_budget_exceeded"]})
        return PreparedFreshDiscovery(
            tool, page, tuple(sorted(ids)), self.service._materialized_cursor_revision(),
            self.service._projection_inventory_epoch(),
            int(page["source_receipt"]["freshness"]["observation_watermark"]),
            self._discovery_digest(tool, selected),
        )

    def _discovery_digest(self, tool: str, arguments: dict[str, Any]) -> str:
        method = (self.service.retrieval._find_links if tool == "wechat_find_links"
                  else self.service.retrieval._retrieve)
        bound = inspect.signature(method).bind(**arguments)
        bound.apply_defaults()
        return self.service._scope_digest(dict(bound.arguments))

    def captured_discovery_page(
        self, tool: str, arguments: dict[str, Any],
    ) -> dict[str, Any] | None:
        prepared = self.service._captured_discovery.get()
        if prepared is None:
            return None
        if (not isinstance(prepared, PreparedFreshDiscovery) or prepared.tool != tool
                or prepared.request_digest != self._discovery_digest(tool, arguments)):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if (prepared.policy_revision != self.service._materialized_cursor_revision()
                or prepared.epoch != self.service._projection_inventory_epoch()):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        selected = tuple(arguments.get("conversation_ids") or ())
        scope = self.service.retrieval._scope(
            arguments.get("account_id"), selected, tuple(arguments.get("participant_ids") or ()),
            arguments.get("after"), arguments.get("before"),
        )
        if any(self.repository.materialized_snapshot_changed(
            value, projection_epoch=prepared.epoch, observation_watermark=prepared.watermark)
                for value in scope.conversation_ids):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        return copy.deepcopy(prepared.page)

    @staticmethod
    def _result_message_ids(result: dict[str, Any]) -> set[str]:
        ids = {str(item["message_id"]) for item in result.get("items", ())}
        for context in result.get("contexts", ()):
            ids.update(str(row[0]) for row in context.get("messages", ()))
            ids.update(str(item["message_id"]) for item in context.get("links", ()))
        return ids

    def record_captured_updates(
        self, context: Any, snapshot: SourceSnapshot, page: SourceMessagePage,
        after: SourceSortKey | None, revision: int | None,
    ) -> dict[str, Any]:
        """Admit a partial scan page without advancing background or reader state."""
        from sightglass.reader.service import _MaterializedTarget

        conversation = str(context["conversation_id"])
        epoch = self.service._projection_inventory_epoch()
        expected_after, expected_revision, scan_pass = self.fresh_updates_reconcile_position(
            conversation)
        if (after, revision) != (expected_after, expected_revision):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        if not page.messages and page.has_more_after:
            raise SightglassError(ErrorCode.SOURCE_INCOMPLETE, retryable=True,
                                 details={"reason": "reconciliation_page_has_no_position"})
        if page.messages:
            first, last = page.messages[0].sort_key, page.messages[-1].sort_key
            record_window(self.repository.database, conversation, epoch,
                          after or first, last, snapshot.fresh_as_of)
        next_position = page.messages[-1].sort_key if page.has_more_after else None
        self.repository.record_update_reconciliation_cursor(
            receipt_id=self._reconcile_id(conversation), reader_id=self.service.reader.reader_id,
            conversation_id=conversation, scope_digest=self._reconcile_scope(conversation),
            completed_at=snapshot.fresh_as_of,
            metadata={"schema": UPDATE_RECONCILE_SCHEMA,
                      "after": list(next_position.as_tuple()) if next_position else None,
                      "revision": expected_revision + 1,
                      "pass": scan_pass + int(not page.has_more_after)},
        )
        state = self.repository.source_conversation_state(conversation)
        target = _MaterializedTarget(context, state, self.repository.source_catalog_state(
            str(context["account_id"])), view="fresh")
        receipt = self.service._materialized_receipt(
            target, observation_watermark=self.repository.observation_watermark())
        receipt.update(complete=False, view="fresh", served_from="captured_source",
                       fresh_as_of=snapshot.fresh_as_of,
                       inventory_digest=snapshot.inventory_digest,
                       generation_set_digest=snapshot.generation_set_digest)
        receipt["freshness"].update(state="live_validated", live_refresh_confirmed=True)
        receipt["coverage"]["conversation"] = "captured_page"
        receipt["coverage"]["notes"].append("reconciliation_partial_current_page")
        receipt["updates_reconciliation"] = {
            "pass": scan_pass, "revision": expected_revision + 1,
            "page_messages": len(page.messages), "has_more_after": page.has_more_after,
            "pass_completed": not page.has_more_after, "cross_page_snapshot": False,
        }
        return receipt

    def captured_updates_receipt(self, context: Any, snapshot: SourceSnapshot) -> dict[str, Any]:
        from sightglass.reader.service import _MaterializedTarget

        target = _MaterializedTarget(context, self.repository.source_conversation_state(
            str(context["conversation_id"])), self.repository.source_catalog_state(
            str(context["account_id"])), view="fresh")
        receipt = self.service._materialized_receipt(
            target, observation_watermark=self.repository.observation_watermark())
        receipt.update(complete=False, view="fresh", served_from="captured_source",
                       fresh_as_of=snapshot.fresh_as_of, inventory_digest=snapshot.inventory_digest,
                       generation_set_digest=snapshot.generation_set_digest)
        receipt["freshness"].update(state="live_validated", live_refresh_confirmed=True)
        receipt["coverage"]["conversation"] = "verified_current_subset"
        return receipt

    def _frontier_id(self, delivery_id: str) -> str:
        return opaque_id("wxupdatefrontier", self.service.reader.reader_id, delivery_id)

    def _frontier_digest(self, conversation_id: str, scope_kind: str, scope_key: str) -> str:
        return self.service._scope_digest({
            "conversation_id": conversation_id, "scope_kind": scope_kind, "scope_key": scope_key,
            **self._request_policy_scope(),
        })

    def record_update_frontier(
        self, delivery_id: str, *, conversation_id: str, scope_kind: str, scope_key: str,
        frontier: SourceSortKey,
    ) -> None:
        self.repository.record_update_delivery_frontier(
            receipt_id=self._frontier_id(delivery_id), reader_id=self.service.reader.reader_id,
            conversation_id=conversation_id,
            scope_digest=self._frontier_digest(conversation_id, scope_kind, scope_key),
            completed_at=utc_now().isoformat(),
            metadata={"schema": UPDATE_FRONTIER_SCHEMA, "position": list(frontier.as_tuple())},
        )

    def _update_frontier(
        self, delivery_id: str, conversation_id: str, scope_kind: str, scope_key: str,
    ) -> SourceSortKey | None:
        receipt = self.repository.update_delivery_frontier(self._frontier_id(delivery_id))
        if receipt is None:
            return None
        if receipt["scope_digest"] != self._frontier_digest(conversation_id, scope_kind, scope_key):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        metadata = json.loads(receipt["warning_codes_json"])
        if metadata.get("schema") != UPDATE_FRONTIER_SCHEMA:
            raise SightglassError(ErrorCode.CURSOR_STALE)
        return SourceSortKey(*metadata["position"])

    def acknowledge_delivery(self, **arguments: Any) -> None:
        frontier = self._update_frontier(
            arguments["delivery_id"], arguments["conversation_id"],
            arguments["scope_kind"], arguments["scope_key"])
        self.repository.acknowledge_delivery(**arguments)
        if frontier is None:
            return
        context = self.service._conversation_context(arguments["conversation_id"])
        identity = opaque_id("wxmsg", str(context["account_id"]), frontier.source_message_id)
        row = self.repository.message_position_row(identity)
        if row is None or self.service._row_sort_key(row) != frontier:
            raise SightglassError(ErrorCode.CURSOR_STALE)
        self.repository.commit_timeline_position(
            reader_id=self.service.reader.reader_id, conversation_id=arguments["conversation_id"],
            scope_kind=arguments["scope_kind"], scope_key=arguments["scope_key"], row=row,
            updated_at=arguments["acknowledged_at"], seed_update_cursor=False,
            admitted_message_ids=(),
        )
        self.repository.consume_update_delivery_frontier(self._frontier_id(arguments["delivery_id"]))

    def validate_fresh(
        self, scope: Any, result: dict[str, Any], *, policy_revision: str,
    ) -> None:
        """Confirm every returned observed row through a current bounded source lease.

        Empty/partial resident recall remains bounded recall, and still requires a
        current source lease. A changed source is admitted and rejects the old result
        so a retry selects its new canonical version; no stale result is returned.
        """
        ids = self._result_message_ids(result)
        watermark = result["source_receipt"]["freshness"]["observation_watermark"]
        rows = self.repository.frozen_message_rows(tuple(ids))
        if len(rows) != len(ids):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        targets = {value: self.service._persisted_source_target(value)
                   for value in scope.conversation_ids}
        if len(targets) == 1:
            target = next(iter(targets.values()))
            lease = self.service._source_session(SourceScope.conversation(
                target.source_account_key, target.source_conversation_id))
        else:
            lease = self.service._source_read()
        changed = False
        with lease as (stack, snapshot):
            prepared = []
            for row in rows:
                self.service.reader.authorize(str(row["conversation_id"]))
                target = targets.get(str(row["conversation_id"]))
                if target is None:
                    raise SightglassError(ErrorCode.POLICY_DENIED)
                source = self.service.provider.get_message(
                    target.source_account_key, str(row["source_message_id"]), snapshot)
                if source is None:
                    raise SightglassError(ErrorCode.SOURCE_INCOMPLETE, retryable=True,
                                         details={"warning_codes": ["fresh_target_unavailable"]})
                prepared.append((row, self.service._prepare_messages((source,))))
            with self.service._admission(stack):
                if (policy_revision != self.service._materialized_cursor_revision()
                        or scope.epoch != self.service._projection_inventory_epoch()
                        or any(self.repository.materialized_snapshot_changed(
                            value, projection_epoch=scope.epoch, observation_watermark=watermark)
                               for value in scope.conversation_ids)):
                    raise SightglassError(ErrorCode.CURSOR_STALE)
                for row, message in prepared:
                    context = self.service._conversation_context(str(row["conversation_id"]))
                    self.service._ingest_prepared_messages(context, message)
                    current = self.repository.message_position_row(str(row["message_id"]))
                    assert current is not None
                    if current["current_observation_seq"] != row["current_observation_seq"]:
                        changed = True
        if changed:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        result["freshness"] = "live_validated"
        result["source_receipt"].update(view="fresh", complete=False)
        result["source_receipt"]["freshness"].update(state="live_validated",
                                                     live_refresh_confirmed=True)
        result["source_receipt"]["coverage"]["conversation"] = "validated_resident_subset"

    def find_conversations(
        self, query: str, *, account_id: str | None, kinds: tuple[str, ...],
        recent_only: bool, limit: int, cursor: str | None,
    ) -> dict[str, Any]:
        with self.repository.database.read_snapshot():
            account = self._account(account_id)
            candidates = []
            for row in self.repository.account_conversations(account):
                conversation = str(row["conversation_id"])
                if not self.service.reader.policy.permits(conversation):
                    continue
                if kinds and row["kind"] not in kinds:
                    continue
                if recent_only and row["last_message_at"] is None:
                    continue
                title = str(row["current_title"])
                aliases = self.repository.observed_conversation_aliases(conversation)
                match = next((value for value in (title, *aliases)
                              if not query or query.casefold() in value.casefold()), None)
                if match is None:
                    continue
                candidates.append({
                    "conversation_id": conversation, "kind": str(row["kind"]), "title": title,
                    "matched": {"value": match, "kind": "title" if match == title else "alias"},
                    "last_message_at": row["last_message_at"],
                })
            positioned = sorted((self.service._activity_position(
                item["last_message_at"], item["kind"], item["conversation_id"]), item)
                for item in candidates)
            total = len(candidates)
            scope = self.service._scope_digest({"query": query.casefold(), "kinds": sorted(kinds),
                                               "recent_only": recent_only, "view": "replica"})
            epoch = self.service._scope_digest({"candidates": candidates})
            revision = self.service._materialized_cursor_revision()
            if cursor:
                payload = self.service.account_cursors.verify(
                    cursor, kind="catalog", reader_id=self.service.reader.reader_id,
                    account_id=account, scope_key=scope, policy_revision=revision,
                )
                if payload["snapshot"].get("catalog_epoch") != epoch:
                    raise SightglassError(ErrorCode.CURSOR_STALE)
                positioned = [item for item in positioned if item[0] > payload["position"]]
            selected = positioned[:limit]
            has_more = len(positioned) > len(selected)
            ambiguous = bool(query) and total > 1
            for _, item in selected:
                item["ambiguity"] = {"requires_selection": ambiguous}
            catalog = self.repository.source_catalog_state(account)
            coverage = str(catalog["coverage_state"]) if catalog else "unknown"
            return {
                "schema": "sightglass.conversation-catalog.v2", "query": query,
                "account_id": account, "recent_only": recent_only, "ambiguous": ambiguous,
                "total_matches": total, "truncated": has_more,
                "candidates": [item for _, item in selected],
                "page": {"truncated": has_more, "next_cursor": (
                    self.service.account_cursors.issue(
                        kind="catalog", reader_id=self.service.reader.reader_id, account_id=account,
                        scope_key=scope, policy_revision=revision, position=selected[-1][0],
                        snapshot={"catalog_epoch": epoch, "view": "replica"},
                    ) if has_more and selected else None)},
                "coverage": {"catalog": coverage, "conversation": "observed_catalog",
                             "notes": ["not_found_is_coverage_bounded"]},
                "source_receipt": {"served_from": "window_db", "view": "replica",
                                   "complete": False,
                                   "freshness": {"state": "bounded_stale",
                                                 "live_refresh_confirmed": False}},
            }

    def read_inbox(self, *, account_id: str | None, **arguments: Any) -> dict[str, Any]:
        with self.repository.database.read_snapshot():
            account = self._account(account_id)
            epoch = self.service._projection_inventory_epoch()
            state = self.repository.source_catalog_state(account)
            ids = self.repository.resident_conversation_ids(account, epoch)
            degraded = self.repository.degraded_conversation_ids(
                account, error_code="duplicate_message_identity_conflict"
            )
            result = self.service._read_indexed_inbox(
                external_account_id=account, catalog_coverage=(
                    str(state["coverage_state"]) if state else "unknown"),
                active_conversations_only=False,
                catalog_fresh_as_of=str(state["last_observed_at"]) if state else "",
                epoch_conversation_ids=ids - degraded, degraded_conversation_ids=degraded,
                live_refresh_available=False, projection_epoch=epoch, **arguments,
            )
            result["source_receipt"]["view"] = "replica"
            return result

    def search_messages(
        self, *, query: str, account_id: str | None, conversation_ids: tuple[str, ...],
        participant_ids: tuple[str, ...], sender_query: str | None, after: str | None,
        before: str | None, cursor: str | None, reading_token: str | None, limit: int,
    ) -> dict[str, Any]:
        if reading_token is not None:
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        with self.repository.database.read_snapshot():
            scope = self.service.retrieval._scope(account_id, conversation_ids,
                                                  participant_ids, after, before)
            sender_candidates = {}
            if sender_query and not participant_ids:
                sender_candidates = self.service._search_sender_candidates(
                    scope.conversation_ids, sender_query
                )
                if len(sender_candidates) == 1:
                    participant_ids = (next(iter(sender_candidates)),)
                    scope = self.service.retrieval._scope(account_id, conversation_ids,
                                                          participant_ids, after, before)
                elif cursor:
                    raise SightglassError(ErrorCode.CURSOR_INVALID)
            arguments = {"scope": scope.__dict__, "view": "replica",
                         "query_digest": search_scope_digest(
                             conversation_ids=scope.conversation_ids,
                             participant_ids=participant_ids, query=query,
                             after=scope.after, before=scope.before),
                         "sender_query_digest": self.service._scope_digest(
                             {"sender": sender_query})}
            snapshot, digest, position = self.service.retrieval._snapshot(
                kind="search-replica", scope=scope, arguments=arguments, cursor=cursor
            )
            if position is not None and (len(position) != 4 or not isinstance(position[0], str)):
                raise SightglassError(ErrorCode.CURSOR_INVALID)
            ambiguous = bool(sender_query and not participant_ids)
            rows = [] if ambiguous else self.repository.search_candidate_window(
                scope.conversation_ids, after_key=tuple(position) if position else None,
                after_utc=scope.after, before_utc=scope.before,
                participant_ids=scope.participant_ids, projection_epoch=scope.epoch,
                observation_watermark=snapshot["observation_watermark"],
                lexical_queries=(" ".join(self.service._query_parts(query)),),
                limit=REPLICA_SEARCH_BUDGET + 1,
            )
            matched = []
            frontier = None
            scanned = 0
            for row in rows[:REPLICA_SEARCH_BUDGET]:
                check_operation_budget()
                scanned += 1
                frontier = row
                if self.service._text_matches(current_search_document(row), query):
                    matched.append(row)
                    if len(matched) >= limit:
                        break
            has_more = scanned < len(rows)
            scan_partial = scanned == REPLICA_SEARCH_BUDGET and has_more
            contexts = {str(row["conversation_id"]): self.service._conversation_context(
                str(row["conversation_id"])) for row in matched}
            account = self.repository.account_row(scope.account_id)
            timezone = self.service._reader_timezone(account)
            prepared = self.service.compact_projector.prepare(
                matched, timezone_name=timezone, include_resource_indicators=True,
                focus_message_ids=frozenset(), context_only_ids=frozenset(),
                late_arrival_ids=frozenset(),
            )

            def build(selected: slice, message_rows_complete: bool) -> dict[str, Any]:
                chosen = matched[selected]
                compact = self.service.compact_projector.render(prepared[selected])
                truncated = has_more or not message_rows_complete
                boundary = chosen[-1] if len(chosen) < len(matched) and chosen else frontier
                next_cursor = (self.service.retrieval._cursor(
                    "search-replica", scope, digest, snapshot,
                    list(self.service._search_keyset(boundary)))
                    if truncated and boundary is not None else None)
                conversations = tuple(dict.fromkeys(str(row["conversation_id"]) for row in chosen))
                receipts = self.service.retrieval._receipts(
                    scope, snapshot, returned=len(chosen), examined=scanned,
                    budget=REPLICA_SEARCH_BUDGET, has_more=truncated, scan_partial=scan_partial,
                )
                receipts["source_receipt"].update(view="replica", complete=False)
                receipts["source_receipt"]["coverage"]["conversation"] = "resident_subset"
                receipts["index_receipt"] = {"kind": "lexical", "recipe": LEXICAL_RECIPE,
                    "backend": "trigram_candidates_or_bounded_literal_scan",
                    "coverage": "resident_subset", "candidate_recall_is_evidence": False}
                compact["markers"]["matches"] = {
                    str(index): [key for key, value in message_search_fields(ParsedMessage(
                        kind=row["kind"], text=row["text"],
                        structured=json.loads(row["structured_json"] or "{}"),
                    )).items()
                                 if self.service._text_matches(value, query)]
                    for index, row in enumerate(chosen)
                }
                result = {
                    "schema": "sightglass.search-results.v2", "projection": "compact",
                    "query": query, "account_id": scope.account_id,
                    "ambiguous_sender": len(sender_candidates) > 1,
                    "participant_candidates": list(sender_candidates.values())[:limit]
                    if ambiguous else [], "timezone": timezone,
                    "conversations": [{"id": value, "title": contexts[value]["current_title"],
                                       "kind": contexts[value]["kind"]} for value in conversations],
                    "hit_conversations": [conversations.index(str(row["conversation_id"]))
                                          for row in chosen],
                    "people": compact["people"], "fields": compact["fields"],
                    "hits": compact["messages"], "markers": compact["markers"],
                    "page": {"next_cursor": next_cursor, "truncated": truncated,
                             "message_rows_complete": message_rows_complete}, **receipts,
                }
                CompactBodyBudgetAllocator(
                    max_payload_chars=self.service.reader.policy.max_compact_payload_chars,
                    max_body_chars=self.service.reader.policy.max_compact_body_chars_per_message,
                ).apply(result, rows_key="hits")
                return result

            _, result = self.service._fit_compact_page(
                len(matched), direction="forward", build=build)
            return result

    def update_request_binding(self, request_id: str | None, **arguments: Any) -> str | None:
        if request_id is None:
            return None
        if (not isinstance(request_id, str)
                or re.fullmatch(r"[A-Za-z0-9_-]{8,128}", request_id) is None):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        return self.service.token_codec.private_digest("updates-request.v1", {
            "reader": self.service.reader.reader_id, **arguments, **self._request_policy_scope(),
        })

    def _request_policy_scope(self) -> dict[str, Any]:
        profile = self.repository.database.reader_profile(self.service.reader.reader_id)
        return {"policy": self.service._policy_revision(),
                "policy_revision": int(profile["policy_revision"]) if profile else 0,
                "epoch": self.service._projection_inventory_epoch()}

    def replay_update_request(
        self, request_id: str | None, request_binding: str | None,
    ) -> dict[str, Any] | None:
        if request_id is None:
            return None
        receipt = self.repository.update_request_outcome(opaque_id(
            "wxrequest", self.service.reader.reader_id, request_id))
        if receipt is None:
            return None
        if receipt["scope_digest"] != request_binding:
            raise SightglassError(ErrorCode.QUERY_INVALID,
                                 details={"reason": "request_id_scope_changed"})
        metadata = json.loads(receipt["warning_codes_json"])
        if (metadata.get("schema") != UPDATE_REQUEST_SCHEMA
                or parse_aware_datetime(metadata["expires_at"]) <= utc_now()):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        if "error" in metadata:
            return metadata["error"]
        delivery_id = metadata.get("delivery_id")
        if delivery_id:
            delivery = self.repository.delivery(delivery_id)
            if delivery is None or delivery["status"] == "expired":
                raise SightglassError(ErrorCode.DELIVERY_ACK_INVALID)
        return self.service.delivery_store.read(
            str(self.service.delivery_store.root / (metadata["payload_id"] + ".json")),
            metadata["payload_digest"],
        )

    def record_update_error(
        self, request_id: str | None, request_binding: str | None,
        error: SightglassError, *, conversation_id: str,
    ) -> None:
        if request_id is None:
            return
        assert request_binding is not None
        now = utc_now()
        self.repository.record_update_request_outcome(
            receipt_id=opaque_id("wxrequest", self.service.reader.reader_id, request_id),
            reader_id=self.service.reader.reader_id, conversation_id=conversation_id,
            scope_digest=request_binding, completed_at=now.isoformat(),
            metadata={"schema": UPDATE_REQUEST_SCHEMA, "error": error.as_dict(),
                      "expires_at": (now + timedelta(days=UPDATE_REQUEST_REPLAY_DAYS)).isoformat()},
        )

    def record_update_outcome(
        self, request_id: str | None, request_binding: str | None, page: dict[str, Any], *,
        conversation_id: str, payload_id: str | None = None, payload_digest: str | None = None,
    ) -> dict[str, Any]:
        if request_id is None:
            return page
        replay = self.replay_update_request(request_id, request_binding)
        if replay is not None:
            return replay
        assert request_binding is not None
        now = utc_now()
        if payload_id is None:
            payload_id = opaque_id("wxrequestpayload", self.service.reader.reader_id,
                                   request_id, now.isoformat(timespec="microseconds"))
            _, payload_digest = self.service.delivery_store.write(payload_id, page)
        self.repository.record_update_request_outcome(
            receipt_id=opaque_id("wxrequest", self.service.reader.reader_id, request_id),
            reader_id=self.service.reader.reader_id, conversation_id=conversation_id,
            scope_digest=request_binding, completed_at=now.isoformat(timespec="microseconds"),
            metadata={"schema": UPDATE_REQUEST_SCHEMA, "payload_id": payload_id,
                      "payload_digest": payload_digest,
                      "delivery_id": page["page"].get("delivery_id"),
                      "expires_at": (now + timedelta(days=UPDATE_REQUEST_REPLAY_DAYS)).isoformat()},
        )
        return page

    def update_replay_ready(self, arguments: dict[str, Any]) -> bool:
        request_id = arguments.get("request_id")
        if isinstance(request_id, str) and self.repository.update_request_outcome(
            opaque_id("wxrequest", self.service.reader.reader_id, request_id)) is not None:
            return True
        if arguments.get("ack_delivery_id") is None:
            from sightglass.reader.cursors import cursor_scope
            scope_kind, scope_key = cursor_scope(tuple(arguments.get("participant_ids") or ()),
                                                 arguments.get("query"))
            return self.repository.pending_delivery(
                self.service.reader.reader_id, str(arguments.get("conversation_id")),
                scope_kind, scope_key,
            ) is not None
        return False

    def read_updates(self, *, ack_delivery_id: str | None, **arguments: Any) -> dict[str, Any]:
        from sightglass.reader.service import _CatalogFacts, _MaterializedTarget

        conversation = arguments["conversation_id"]
        with self.repository.database.transaction():
            replay = self.replay_update_request(
                arguments["request_id"], arguments["request_binding"])
            if replay is not None:
                return replay
            self.service._ensure_update_reader_profile()
            context = self.service._conversation_context(conversation)
            if ack_delivery_id is not None:
                self.acknowledge_delivery(
                    delivery_id=ack_delivery_id, reader_id=self.service.reader.reader_id,
                    conversation_id=conversation, scope_kind=arguments["scope_kind"],
                    scope_key=arguments["scope_key"], acknowledged_at=utc_now().isoformat(),
                )
            pending = self.repository.pending_delivery(
                self.service.reader.reader_id, conversation, arguments["scope_kind"],
                arguments["scope_key"],
            )
            if pending is not None:
                page = self.service.delivery_store.read(
                    pending["payload_ref"], pending["payload_digest"])
                return self.record_update_outcome(
                    arguments["request_id"], arguments["request_binding"], page,
                    conversation_id=conversation, payload_id=pending["delivery_id"],
                    payload_digest=pending["payload_digest"],
                )
            state = self.repository.source_conversation_state(conversation)
            if (state is not None
                    and state["last_error_code"] == "duplicate_message_identity_conflict"):
                raise SightglassError(ErrorCode.SOURCE_INCOMPLETE)
            target = _MaterializedTarget(context, state, self.repository.source_catalog_state(
                str(context["account_id"])), view="replica")
            receipt = self.service._materialized_receipt(
                target, observation_watermark=self.repository.observation_watermark())
            snapshot = SourceSnapshot(receipt["inventory_digest"], receipt["generation_set_digest"],
                                      receipt["fresh_as_of"], (), "replica")
            return self.service._publish_updates(
                context=context, snapshot=snapshot,
                facts=_CatalogFacts(False, False), source_receipt_override=receipt, **arguments,
            )

"""Execute and close one local bounded source operation before transfer."""

from __future__ import annotations

import hashlib
import threading
import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta

from sightglass.contracts.capture import (
    SOURCE_IDENTITY_CONFLICT_REASON,
    CaptureCeiling,
    CaptureContextWindow,
    CaptureCoverage,
    CaptureDocument,
    CaptureEvidence,
    CaptureOrigin,
    CaptureProtocolError,
    CaptureReceipt,
    CaptureRequest,
    ResourceCaptureBinding,
)
from sightglass.contracts.common import utc_now
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.messages import SourceMessage, SourceMessagePage
from sightglass.operations import check_operation_budget, operation_budget
from sightglass.source.base import (
    ContextSourceProvider,
    SourceScope,
    SourceSnapshot,
    WeChatSourceProvider,
)
from sightglass.source.parser import PARSER_VERSION

from .codec import SealedCapture, canonical_json, strict_json


def projection_origin_epoch(provider: WeChatSourceProvider) -> str:
    # Exactly ReaderService._projection_inventory_epoch's active meaning; the
    # transport implementation must never replace the native v6 descriptor here.
    return hashlib.sha256(
        canonical_json(
            {
                "schema": "sightglass.tail-projection.v1",
                "provider_implementation": provider.descriptor.implementation,
                "parser_version": PARSER_VERSION,
            }
        )
    ).hexdigest()


class CaptureExecutor:
    def __init__(
        self,
        provider: WeChatSourceProvider,
        ceiling: CaptureCeiling,
        *,
        source_instance_id: str,
        origin_epoch: str | None = None,
        resource_binding: Callable[[CaptureRequest, SourceSnapshot], ResourceCaptureBinding]
        | None = None,
        timeout_seconds: float = 120.0,
        receipt_ttl_seconds: float = 120.0,
    ) -> None:
        if not 0 < timeout_seconds <= 150 or not 0 < receipt_ttl_seconds <= 120:
            raise ValueError("capture time budget exceeds the receipt boundary")
        self.provider = provider
        self.ceiling = ceiling
        self.source_instance_id = source_instance_id
        self.origin_epoch = origin_epoch or projection_origin_epoch(provider)
        self.resource_binding = resource_binding or getattr(
            provider, "capture_resource_binding", None
        )
        self.timeout_seconds = timeout_seconds
        self.receipt_ttl_seconds = receipt_ttl_seconds

    def capture(
        self,
        request: CaptureRequest,
        *,
        stream_epoch: str,
        sequence: int,
        batch_id: str | None = None,
        cancelled: threading.Event | None = None,
    ) -> SealedCapture:
        captured_at = utc_now().isoformat()
        snapshot: SourceSnapshot | None = None
        evidence = CaptureEvidence()
        resource = b""
        coverage = CaptureCoverage("none")
        terminal = "complete"
        reason = None
        selected: tuple[tuple[str, str], ...] = ()
        try:
            self.ceiling.authorize(request)
            with operation_budget(self.timeout_seconds, cancelled=cancelled):
                scope = (
                    SourceScope.catalog()
                    if request.operation == "catalog"
                    else SourceScope.resource(
                        request.resource_key or "",
                        account_id=request.account_id,
                        conversation_source_id=request.conversation_source_id,
                        source_message_id=request.focus_source_message_id,
                    )
                    if request.operation == "resource"
                    else SourceScope.conversations(
                        request.account_id, request.conversation_source_ids
                    )
                    if request.conversation_source_ids
                    else SourceScope.conversation(
                        request.account_id,
                        request.conversation_source_id or "",
                    )
                )
                with self.provider.session(scope) as snapshot:
                    evidence, resource, coverage = self._execute(request, snapshot)
                    if request.operation not in {"catalog", "resource"}:
                        selected_map: dict[str, str] = {}
                        for conversation_id in request.conversations:
                            for key, generation in self.provider.search_generation_binding(
                                request.account_id,
                                conversation_id,
                                snapshot=snapshot,
                            ):
                                if key in selected_map and selected_map[key] != generation:
                                    raise CaptureProtocolError("selected_generation_changed")
                                selected_map[key] = generation
                        selected = tuple(sorted(selected_map.items()))
                        if (
                            request.expected_generations
                            and selected != request.expected_generations
                        ):
                            raise CaptureProtocolError("selected_generation_changed")
                    elif request.operation == "catalog":
                        selected = snapshot.generation_by_shard
                    else:
                        selected = tuple(sorted(snapshot.dependency_generation_by_shard.items()))
                    check_operation_budget()
                # Source session exit validation has completed. No transport code
                # has been invoked, and no WindowDB writer exists in this executor.
        except (SightglassError, CaptureProtocolError) as exc:
            terminal = "cancelled" if cancelled is not None and cancelled.is_set() else "rejected"
            reason = exc.code.value if isinstance(exc, SightglassError) else exc.reason
            warning_codes = (
                exc.details.get("warning_codes") if isinstance(exc, SightglassError) else ()
            )
            if (isinstance(exc, SightglassError) and exc.code == ErrorCode.SOURCE_INCOMPLETE
                    and isinstance(warning_codes, (list, tuple))
                    and SOURCE_IDENTITY_CONFLICT_REASON in warning_codes):
                reason = SOURCE_IDENTITY_CONFLICT_REASON
            evidence, resource, coverage = CaptureEvidence(), b"", CaptureCoverage("none")
        except Exception:
            terminal, reason = "rejected", "INTERNAL_ERROR"
            evidence, resource, coverage = CaptureEvidence(), b"", CaptureCoverage("none")
        sealed_at = utc_now()
        descriptor = self.provider.descriptor
        origin = CaptureOrigin(
            source_instance_id=self.source_instance_id,
            origin_epoch=self.origin_epoch,
            stream_epoch=stream_epoch,
            sequence=sequence,
            batch_id=batch_id or uuid.uuid4().hex,
            egress_revision=self.ceiling.revision,
            provider_kind=descriptor.kind,
            provider_implementation=descriptor.implementation,
            provider_mode=descriptor.source_mode,
            message_sender_evidence_complete=descriptor.message_sender_evidence_complete,
            supports_resources=descriptor.supports_resources,
            inventory_digest=snapshot.inventory_digest if snapshot else "unavailable",
            generation_set_digest=snapshot.generation_set_digest if snapshot else "unavailable",
            source_fresh_as_of=snapshot.fresh_as_of if snapshot else captured_at,
            selected_generations=selected,
            captured_at=captured_at,
        )
        document = CaptureDocument(
            request=request,
            origin=origin,
            evidence=evidence,
            receipt=CaptureReceipt(
                terminal=terminal,  # type: ignore[arg-type]
                sealed_at=sealed_at.isoformat(),
                fresh_until=(sealed_at + timedelta(seconds=self.receipt_ttl_seconds)).isoformat(),
                coverage=coverage,
                reason=reason,
            ),
        )
        try:
            return SealedCapture.seal(document, resource)
        except CaptureProtocolError as exc:
            # An oversized roster/body must produce a terminal rejection rather
            # than a truncated message page masquerading as source coverage.
            if exc.reason not in {"metadata_size_invalid", "resource_size_invalid"}:
                raise
            return SealedCapture.seal(
                replace(
                    document,
                    evidence=CaptureEvidence(),
                    receipt=replace(
                        document.receipt,
                        terminal="rejected",
                        reason=exc.reason,
                        coverage=CaptureCoverage("none"),
                    ),
                )
            )

    def _execute(
        self,
        request: CaptureRequest,
        snapshot: SourceSnapshot,
    ) -> tuple[CaptureEvidence, bytes, CaptureCoverage]:
        accounts = tuple(
            item
            for item in self.provider.list_accounts(snapshot)
            if item.source_account_key == self.ceiling.account_id
        )
        if len(accounts) != 1:
            raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)
        if request.operation == "catalog":
            conversations = tuple(
                item
                for item in self.provider.list_conversations(
                    request.account_id,
                    snapshot,
                )
                if item.source_conversation_id in self.ceiling.conversations
            )
            return (
                CaptureEvidence(accounts=accounts, conversations=conversations),
                b"",
                CaptureCoverage(
                    "catalog",
                    catalog_complete=self.provider.catalog_complete(snapshot),
                    active_conversations_only=self.provider.active_conversations_only(snapshot),
                ),
            )
        if request.operation == "resource":
            if self.resource_binding is None:
                raise CaptureProtocolError("resource_binding_validator_required")
            binding = self.resource_binding(request, snapshot)
            expected = ResourceCaptureBinding(
                request.account_id,
                request.conversation_source_id or "",
                request.focus_source_message_id or "",
                request.resource_key or "",
                request.resource_descriptor_digest or "",
                request.expected_resource_revision or "",
            )
            if binding != expected:
                raise CaptureProtocolError("resource_binding_mismatch")
            payload = self.provider.read_resource(
                request.resource_key or "",
                max_bytes=request.max_resource_bytes,
                snapshot=snapshot,
            )
            if (
                payload.source_resource_key != request.resource_key
                or payload.variant != request.resource_variant
                or self.resource_binding(request, snapshot) != binding
            ):
                raise CaptureProtocolError("resource_revision_changed")
            return (
                CaptureEvidence(
                    accounts=accounts,
                    resource_binding=binding,
                    resource_variant=payload.variant,
                ),
                payload.data,
                CaptureCoverage("resource"),
            )
        conversations = []
        participants = []
        for conversation_id in request.conversations:
            conversation = self.provider.get_conversation(
                request.account_id, conversation_id, snapshot
            )
            if conversation is None:
                raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
            conversations.append(conversation)
            # Native messages already carry stable sender/current-label evidence.
            # Match the ordinary reader's fence: a bounded body read must not
            # depend on an unrelated 200-message participant-discovery window.
            if not self.provider.descriptor.message_sender_evidence_complete:
                participants.extend(
                    self.provider.list_participants(
                        request.account_id,
                        conversation_id,
                        snapshot,
                    )
                )
        base = CaptureEvidence(
            accounts=accounts, conversations=tuple(conversations), participants=tuple(participants)
        )
        conversation_id = request.conversation_source_id or ""
        if request.operation == "recent":
            page = self.provider.read_recent(
                request.account_id, conversation_id, request.limit, snapshot
            )
        elif request.operation == "range":
            page = self.provider.read_range(
                request.account_id,
                conversation_id,
                after=request.after,
                before=request.before,
                direction=request.direction,
                limit=request.limit,
                snapshot=snapshot,
                participant_source_ids=request.participant_filters,
                time_after_utc=request.time_after_utc,
                time_before_utc=request.time_before_utc,
            )
        elif request.operation == "context":
            focus = self.provider.get_message(
                request.account_id, request.focus_source_message_id or "", snapshot
            )
            if focus is None or focus.source_conversation_id != conversation_id:
                raise SightglassError(ErrorCode.MESSAGE_NOT_FOUND)
            page = self._context(request, focus, snapshot)
        elif request.operation == "verify":
            messages: list[SourceMessage] = []
            missing: list[str] = []
            for source_id in request.message_ids:
                check_operation_budget()
                message = self.provider.get_message(request.account_id, source_id, snapshot)
                if message is None:
                    missing.append(source_id)
                elif message.source_conversation_id not in request.conversations:
                    raise CaptureProtocolError("verification_conversation_mismatch")
                else:
                    messages.append(message)
            return (
                replace(base, messages=tuple(messages), missing_message_ids=tuple(missing)),
                b"",
                (CaptureCoverage("verification")),
            )
        elif request.operation == "discovery":
            discovery = self.provider.scan_discovery_page(
                request.account_id,
                conversation_id,
                snapshot=snapshot,
                position=strict_json(request.position_json.encode())
                if request.position_json
                else None,
                limit=request.limit,
                time_after_utc=request.time_after_utc,
                time_before_utc=request.time_before_utc,
            )
            found: dict[str, SourceMessage] = {}
            positions: list[str] = []
            for candidate, position in zip(discovery.messages, discovery.positions, strict=True):
                check_operation_budget()
                message = self.provider.get_message(
                    request.account_id, candidate.source_message_id, snapshot
                )
                if message is None or message.source_message_id in found:
                    continue
                if message.source_conversation_id != conversation_id:
                    raise CaptureProtocolError("discovery_conversation_mismatch")
                found[message.source_message_id] = message
                positions.append(canonical_json(position).decode())
            return (
                replace(
                    base,
                    messages=tuple(found.values()),
                    discovery_positions_json=tuple(positions),
                    discovery_next_position_json=(
                        canonical_json(discovery.next_position).decode()
                        if discovery.next_position is not None
                        else None
                    ),
                ),
                b"",
                CaptureCoverage(
                    "discovery",
                    scanned_rows=discovery.scanned_rows,
                    discovery_has_more=discovery.has_more,
                ),
            )
        else:
            raise CaptureProtocolError("unsupported_operation")
        if request.operation == "range" and (request.context_before or request.context_after):
            message_map = {item.source_message_id: item for item in page.messages}
            windows = []
            for focus in page.messages:
                check_operation_budget()
                sides = []
                for direction, radius in (
                    ("backward", request.context_before),
                    ("forward", request.context_after),
                ):
                    side = (
                        self.provider.read_range(
                            request.account_id,
                            conversation_id,
                            after=focus.sort_key if direction == "forward" else None,
                            before=focus.sort_key if direction == "backward" else None,
                            direction=direction,
                            limit=radius,
                            snapshot=snapshot,
                        )
                        if radius
                        else SourceMessagePage(())
                    )
                    sides.append(side)
                    for message in side.messages:
                        previous = message_map.get(message.source_message_id)
                        if previous is not None and previous != message:
                            raise CaptureProtocolError("context_identity_conflict")
                        message_map[message.source_message_id] = message
                windows.append(
                    CaptureContextWindow(
                        focus.source_message_id,
                        tuple(item.source_message_id for item in sides[0].messages),
                        tuple(item.source_message_id for item in sides[1].messages),
                        sides[0].has_more_before,
                        sides[1].has_more_after,
                    )
                )
            base = replace(
                base,
                range_page_message_ids=tuple(item.source_message_id for item in page.messages),
                context_windows=tuple(windows),
            )
            captured_messages = tuple(
                sorted(message_map.values(), key=lambda item: item.sort_key.as_tuple())
            )
        else:
            captured_messages = page.messages
        if request.operation == "range":
            message_map = {item.source_message_id: item for item in captured_messages}
            missing_boundaries: list[str] = []
            for source_id in request.message_ids:
                check_operation_budget()
                boundary = self.provider.get_message(request.account_id, source_id, snapshot)
                if boundary is None:
                    missing_boundaries.append(source_id)
                elif boundary.source_conversation_id != conversation_id:
                    raise CaptureProtocolError("boundary_conversation_mismatch")
                else:
                    previous = message_map.get(source_id)
                    if previous is not None and previous != boundary:
                        raise CaptureProtocolError("boundary_identity_conflict")
                    message_map[source_id] = boundary
            captured_messages = tuple(
                sorted(message_map.values(), key=lambda item: item.sort_key.as_tuple())
            )
            base = replace(
                base,
                range_page_message_ids=tuple(item.source_message_id for item in page.messages),
                missing_message_ids=tuple(missing_boundaries),
            )
        return (
            replace(base, messages=captured_messages),
            b"",
            CaptureCoverage(
                "context" if request.operation == "context" else "page",
                has_more_before=page.has_more_before,
                has_more_after=page.has_more_after,
            ),
        )

    def _context(
        self,
        request: CaptureRequest,
        focus: SourceMessage,
        snapshot: SourceSnapshot,
    ) -> SourceMessagePage:
        if isinstance(self.provider, ContextSourceProvider):
            return self.provider.read_context(
                request.account_id,
                request.conversation_source_id or "",
                focus=focus,
                before=request.context_before,
                after=request.context_after,
                snapshot=snapshot,
            )
        pages = []
        for direction, radius in (
            ("backward", request.context_before),
            ("forward", request.context_after),
        ):
            if radius:
                pages.append(
                    self.provider.read_range(
                        request.account_id,
                        request.conversation_source_id or "",
                        after=focus.sort_key if direction == "forward" else None,
                        before=focus.sort_key if direction == "backward" else None,
                        direction=direction,
                        limit=radius,
                        snapshot=snapshot,
                    )
                )
            else:
                pages.append(SourceMessagePage(()))
        messages = {item.source_message_id: item for page in pages for item in page.messages}
        messages[focus.source_message_id] = focus
        return SourceMessagePage(
            tuple(sorted(messages.values(), key=lambda item: item.sort_key.as_tuple())),
            pages[0].has_more_before,
            pages[1].has_more_after,
        )

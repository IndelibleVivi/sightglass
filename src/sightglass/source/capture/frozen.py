"""A sealed operation replayed through existing source interfaces, locally."""

from __future__ import annotations

import secrets
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from typing import Any

from sightglass.contracts.capture import (
    CaptureProtocolError,
    CaptureRequest,
    ResourceCaptureBinding,
)
from sightglass.contracts.common import SourceSortKey
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.identity import (
    ConversationCandidate,
    SourceAccount,
    SourceConversation,
    SourceParticipant,
    SourceParticipantFilter,
)
from sightglass.contracts.messages import SourceDiscoveryPage, SourceMessage, SourceMessagePage
from sightglass.contracts.resources import SourceResourcePayload
from sightglass.source.base import (
    SourceHealth,
    SourcePreparationStep,
    SourceProviderDescriptor,
    SourceScope,
    SourceSnapshot,
)

from .codec import SealedCapture, strict_json


class FrozenCaptureProvider:
    def __init__(self, envelope: SealedCapture) -> None:
        document = envelope.document()
        if document.receipt.terminal != "complete":
            try:
                code = ErrorCode(document.receipt.reason or "SOURCE_SNAPSHOT_FAILED")
            except ValueError:
                code = ErrorCode.SOURCE_SNAPSHOT_FAILED
            raise SightglassError(code, retryable=True)
        self.envelope = envelope
        self._document = document
        self._tokens: set[str] = set()
        self._lock = threading.RLock()
        origin = document.origin
        self._descriptor = SourceProviderDescriptor(
            kind=origin.provider_kind,
            implementation=origin.provider_implementation,
            source_mode=origin.provider_mode,
            platform=("darwin", "linux"),
            supports_incremental=True,
            supports_resources=origin.supports_resources,
            requires_running_app_for_key_refresh=False,
            message_sender_evidence_complete=origin.message_sender_evidence_complete,
        )

    @property
    def descriptor(self) -> SourceProviderDescriptor:
        return self._descriptor

    @property
    def request(self) -> CaptureRequest:
        return self._document.request

    @property
    def origin_epoch(self) -> str:
        return self._document.origin.origin_epoch

    def health(self) -> SourceHealth:
        origin = self._document.origin
        return SourceHealth(
            True,
            True,
            1,
            "complete",
            origin.source_fresh_as_of,
            origin.inventory_digest,
            origin.generation_set_digest,
            {},  # The sealed operation makes no account-wide inventory count claim.
            warnings=("sealed_operation_capture",),
        )

    def _assert(self, snapshot: SourceSnapshot) -> None:
        with self._lock:
            if snapshot.token not in self._tokens:
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED)

    def _target(
        self,
        account_id: str,
        conversation_id: str | None = None,
        snapshot: SourceSnapshot | None = None,
    ) -> None:
        if account_id != self.request.account_id:
            raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)
        if conversation_id is not None and conversation_id not in {
            item.source_conversation_id for item in self._document.evidence.conversations
        }:
            raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        scope = snapshot.scope if snapshot is not None else None
        if scope is not None and conversation_id is not None:
            if (
                scope.conversation_source_id is not None
                and scope.conversation_source_id != conversation_id
            ):
                raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
            if (
                scope.kind == "conversations"
                and conversation_id not in scope.conversation_source_ids
            ):
                raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)

    @contextmanager
    def snapshot(self) -> Iterator[SourceSnapshot]:
        origin = self._document.origin
        token = secrets.token_hex(16)
        snapshot = SourceSnapshot(
            origin.inventory_digest,
            origin.generation_set_digest,
            origin.source_fresh_as_of,
            origin.selected_generations,
            token,
            dependency_generation_by_shard=dict(origin.selected_generations),
        )
        with self._lock:
            self._tokens.add(token)
        try:
            yield snapshot
            self._assert(snapshot)
        finally:
            with self._lock:
                self._tokens.discard(token)

    @contextmanager
    def session(self, scope: SourceScope) -> Iterator[SourceSnapshot]:
        if scope.kind != "catalog":
            if scope.account_id is not None:
                self._target(scope.account_id)
            if scope.conversation_source_id is not None:
                if scope.conversation_source_id not in self.request.conversations:
                    raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
            if scope.kind == "conversations" and any(
                value not in self.request.conversations for value in scope.conversation_source_ids
            ):
                raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
            if scope.kind == "resource" and scope.source_resource_key != self.request.resource_key:
                raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
            if scope.kind == "message" and scope.source_message_id not in {
                item.source_message_id for item in self._document.evidence.messages
            } | set(self._document.evidence.missing_message_ids):
                raise SightglassError(ErrorCode.MESSAGE_NOT_FOUND)
        with self.snapshot() as snapshot:
            yield replace(snapshot, scope=scope)

    def list_accounts(self, snapshot: SourceSnapshot) -> list[SourceAccount]:
        self._assert(snapshot)
        return list(self._document.evidence.accounts)

    def list_conversations(
        self, account_id: str, snapshot: SourceSnapshot
    ) -> list[SourceConversation]:
        self._assert(snapshot)
        self._target(account_id)
        scope = snapshot.scope
        return [
            item
            for item in self._document.evidence.conversations
            if scope is None
            or scope.kind == "catalog"
            or scope.conversation_source_id == item.source_conversation_id
            or item.source_conversation_id in scope.conversation_source_ids
        ]

    def get_conversation(
        self,
        account_id: str,
        conversation_source_id: str,
        snapshot: SourceSnapshot,
    ) -> SourceConversation | None:
        self._assert(snapshot)
        self._target(account_id, conversation_source_id, snapshot)
        return next(
            (
                item
                for item in self._document.evidence.conversations
                if item.source_conversation_id == conversation_source_id
            ),
            None,
        )

    def resolve_conversation(
        self,
        account_id: str,
        query: str,
        snapshot: SourceSnapshot,
    ) -> list[ConversationCandidate]:
        result = []
        for conversation in self.list_conversations(account_id, snapshot):
            for value, kind in (
                (conversation.title, "title"),
                *((alias, "alias") for alias in conversation.aliases),
            ):
                if not query or query.casefold() in value.casefold():
                    result.append(ConversationCandidate(conversation, value, kind))
                    break
        return result

    def list_participants(
        self,
        account_id: str,
        conversation_source_id: str,
        snapshot: SourceSnapshot,
    ) -> list[SourceParticipant]:
        self._assert(snapshot)
        self._target(account_id, conversation_source_id, snapshot)
        if self.request.operation == "catalog":
            raise CaptureProtocolError("roster_not_captured")
        return [
            item
            for item in self._document.evidence.participants
            if item.source_conversation_id == conversation_source_id
        ]

    def resolve_participant(
        self,
        account_id: str,
        conversation_source_id: str,
        query: str,
        snapshot: SourceSnapshot,
    ) -> list[SourceParticipant]:
        return [
            item
            for item in self.list_participants(account_id, conversation_source_id, snapshot)
            if not query or any(query.casefold() in label.label.casefold() for label in item.labels)
        ]

    def _page(self, *, limit: int, direction: str) -> SourceMessagePage:
        if type(limit) is not int or not 1 <= limit <= self.request.limit:
            raise CaptureProtocolError("replay_page_limit_exceeded")
        all_messages = tuple(
            sorted(
                (
                    item
                    for item in self._document.evidence.messages
                    if self.request.operation != "range"
                    or item.source_message_id in self._document.evidence.range_page_message_ids
                ),
                key=lambda item: item.sort_key.as_tuple(),
            )
        )
        selected = all_messages[:limit] if direction == "forward" else all_messages[-limit:]
        coverage = self._document.receipt.coverage
        return SourceMessagePage(
            selected,
            coverage.has_more_before
            or (len(selected) < len(all_messages) and direction == "backward"),
            coverage.has_more_after
            or (len(selected) < len(all_messages) and direction == "forward"),
        )

    def read_recent(
        self,
        account_id: str,
        conversation_source_id: str,
        limit: int,
        snapshot: SourceSnapshot,
    ) -> SourceMessagePage:
        self._assert(snapshot)
        self._target(account_id, conversation_source_id, snapshot)
        if self.request.operation != "recent":
            raise CaptureProtocolError("recent_operation_not_captured")
        return self._page(limit=limit, direction="backward")

    def read_range(
        self,
        account_id: str,
        conversation_source_id: str,
        *,
        after: SourceSortKey | None,
        before: SourceSortKey | None,
        direction: str,
        limit: int,
        snapshot: SourceSnapshot,
        participant_source_ids: tuple[SourceParticipantFilter, ...] = (),
        time_after_utc: str | None = None,
        time_before_utc: str | None = None,
    ) -> SourceMessagePage:
        self._assert(snapshot)
        self._target(account_id, conversation_source_id, snapshot)
        request = self.request
        if request.operation != "range" or (
            after != request.after
            or before != request.before
            or direction != request.direction
            or participant_source_ids != request.participant_filters
            or time_after_utc != request.time_after_utc
            or time_before_utc != request.time_before_utc
        ):
            captured = {item.source_message_id: item for item in self._document.evidence.messages}
            for window in self._document.evidence.context_windows:
                focus = captured[window.focus_source_message_id]
                if (
                    participant_source_ids
                    or time_after_utc is not None
                    or time_before_utc is not None
                ):
                    continue
                if (
                    direction == "backward"
                    and after is None
                    and before == focus.sort_key
                    and limit == request.context_before
                    and limit > 0
                ):
                    return SourceMessagePage(
                        tuple(captured[value] for value in window.before_message_ids),
                        has_more_before=window.has_more_before,
                    )
                if (
                    direction == "forward"
                    and before is None
                    and after == focus.sort_key
                    and limit == request.context_after
                    and limit > 0
                ):
                    return SourceMessagePage(
                        tuple(captured[value] for value in window.after_message_ids),
                        has_more_after=window.has_more_after,
                    )
            raise CaptureProtocolError("range_operation_not_captured")
        return self._page(limit=limit, direction=direction)

    def read_context(
        self,
        account_id: str,
        conversation_source_id: str,
        *,
        focus: SourceMessage,
        before: int,
        after: int,
        snapshot: SourceSnapshot,
    ) -> SourceMessagePage:
        self._assert(snapshot)
        self._target(account_id, conversation_source_id, snapshot)
        request = self.request
        if request.operation != "context" or (
            focus.source_message_id != request.focus_source_message_id
            or before != request.context_before
            or after != request.context_after
        ):
            raise CaptureProtocolError("context_operation_not_captured")
        coverage = self._document.receipt.coverage
        return SourceMessagePage(
            self._document.evidence.messages, coverage.has_more_before, coverage.has_more_after
        )

    def get_message(
        self,
        account_id: str,
        source_message_id: str,
        snapshot: SourceSnapshot,
    ) -> SourceMessage | None:
        self._assert(snapshot)
        self._target(account_id)
        message = next(
            (
                item
                for item in self._document.evidence.messages
                if item.source_message_id == source_message_id
            ),
            None,
        )
        if message is not None:
            self._target(account_id, message.source_conversation_id, snapshot)
        if snapshot.scope is not None and snapshot.scope.kind == "message":
            if snapshot.scope.source_message_id != source_message_id:
                raise CaptureProtocolError("message_not_in_replay_session")
        if message is not None or source_message_id in self._document.evidence.missing_message_ids:
            return message
        raise CaptureProtocolError("message_not_captured")

    def search_generation_binding(
        self,
        account_id: str,
        conversation_source_id: str,
        *,
        snapshot: SourceSnapshot,
    ) -> tuple[tuple[str, str], ...]:
        self._assert(snapshot)
        self._target(account_id, conversation_source_id, snapshot)
        return self._document.origin.selected_generations

    def scan_discovery_page(
        self,
        account_id: str,
        conversation_source_id: str,
        *,
        snapshot: SourceSnapshot,
        position: dict[str, Any] | None = None,
        limit: int = 100,
        time_after_utc: str | None = None,
        time_before_utc: str | None = None,
    ) -> SourceDiscoveryPage:
        self._assert(snapshot)
        self._target(account_id, conversation_source_id, snapshot)
        request, evidence, coverage = (
            self.request,
            self._document.evidence,
            self._document.receipt.coverage,
        )
        expected_position = (
            strict_json(request.position_json.encode()) if request.position_json else None
        )
        if request.operation != "discovery" or (
            position != expected_position
            or limit != request.limit
            or time_after_utc != request.time_after_utc
            or time_before_utc != request.time_before_utc
        ):
            raise CaptureProtocolError("discovery_operation_not_captured")
        return SourceDiscoveryPage(
            evidence.messages,
            tuple(strict_json(item.encode()) for item in evidence.discovery_positions_json),
            strict_json(evidence.discovery_next_position_json.encode())
            if evidence.discovery_next_position_json
            else None,
            coverage.discovery_has_more,
            coverage.scanned_rows,
        )

    def prepare_search_page(
        self,
        account_id: str,
        conversation_source_id: str,
        *,
        direction: str,
        limit: int,
        snapshot: SourceSnapshot,
        after: SourceSortKey | None = None,
        before: SourceSortKey | None = None,
        time_after_utc: str | None = None,
        time_before_utc: str | None = None,
        batch_size: int = 256,
        check_authority: Callable[[], None] | None = None,
    ) -> Iterator[SourcePreparationStep]:
        if check_authority is not None:
            check_authority()
        page = self.read_range(
            account_id,
            conversation_source_id,
            direction=direction,
            limit=limit,
            snapshot=snapshot,
            after=after,
            before=before,
            time_after_utc=time_after_utc,
            time_before_utc=time_before_utc,
        )
        count = len(self._document.origin.selected_generations)
        yield SourcePreparationStep("complete", len(page.messages), count, count, page)

    def capture_resource_binding(
        self,
        request: CaptureRequest,
        snapshot: SourceSnapshot,
    ) -> ResourceCaptureBinding:
        self._assert(snapshot)
        binding = self._document.evidence.resource_binding
        if request != self.request or binding is None:
            raise CaptureProtocolError("resource_binding_not_captured")
        return binding

    def read_resource(
        self,
        source_resource_key: str,
        *,
        max_bytes: int,
        snapshot: SourceSnapshot,
    ) -> SourceResourcePayload:
        self._assert(snapshot)
        if self.request.operation != "resource" or source_resource_key != self.request.resource_key:
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
        if max_bytes < len(self.envelope.resource):
            raise SightglassError(ErrorCode.RESOURCE_TOO_LARGE)
        return SourceResourcePayload(
            source_resource_key,
            self.envelope.resource,
            self._document.evidence.resource_variant or "original",
        )

    def catalog_complete(self, snapshot: SourceSnapshot) -> bool:
        self._assert(snapshot)
        return self._document.receipt.coverage.catalog_complete

    def active_conversations_only(self, snapshot: SourceSnapshot) -> bool:
        self._assert(snapshot)
        return self._document.receipt.coverage.active_conversations_only

    def validate_snapshot(self, snapshot: SourceSnapshot) -> None:
        self._assert(snapshot)

    def close(self) -> None:
        with self._lock:
            self._tokens.clear()

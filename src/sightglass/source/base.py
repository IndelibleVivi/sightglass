from __future__ import annotations

import heapq
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

from sightglass.contracts.capture import CaptureRequest, ResourceCaptureBinding
from sightglass.contracts.common import SourceSortKey
from sightglass.contracts.identity import (
    ConversationCandidate,
    SourceAccount,
    SourceConversation,
    SourceParticipant,
    SourceParticipantFilter,
)
from sightglass.contracts.messages import (
    SourceDiscoveryPage,
    SourceMessage,
    SourceMessagePage,
)
from sightglass.contracts.resources import SourceResourcePayload
from sightglass.operations import check_operation_budget

SourceScopeKind = Literal["catalog", "conversation", "conversations", "message", "resource"]


@dataclass(frozen=True)
class SourceScope:
    """Typed dependency scope for one live/cold source read.

    A scope names the exact target a read is allowed to depend on. ``catalog`` keeps
    the historical account-wide snapshot semantics; the narrower scopes let a read
    prove only the database rows, auxiliary mappings, and files it actually touches
    instead of a global inventory equality check.

    The scope is evidence *about what the read may depend on*, never a grant of
    authority: providers still re-validate every selected dependency and readers keep
    their own policy checks.
    """

    kind: SourceScopeKind
    account_id: str | None = None
    conversation_source_id: str | None = None
    source_message_id: str | None = None
    source_resource_key: str | None = None
    conversation_source_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        values = {
            "account_id": self.account_id,
            "conversation_source_id": self.conversation_source_id,
            "source_message_id": self.source_message_id,
            "source_resource_key": self.source_resource_key,
        }
        if any(value is not None and not value for value in values.values()):
            raise ValueError("source scope identifiers must be non-empty")
        if self.kind == "conversations":
            if (
                self.account_id is None or self.conversation_source_id is not None
                or self.source_message_id is not None or self.source_resource_key is not None
                or not 1 <= len(self.conversation_source_ids) <= 200
                or len(set(self.conversation_source_ids)) != len(self.conversation_source_ids)
                or any(not value for value in self.conversation_source_ids)
            ):
                raise ValueError("conversations scope requires one exact bounded target set")
            return
        if self.conversation_source_ids:
            raise ValueError("only conversations scope carries a target set")
        if self.kind == "catalog":
            if any(value is not None for value in values.values()):
                raise ValueError("catalog scope cannot carry a target")
            return
        if self.kind == "conversation":
            if (
                self.account_id is None
                or self.conversation_source_id is None
                or self.source_message_id is not None
                or self.source_resource_key is not None
            ):
                raise ValueError("conversation scope requires only account and conversation")
            return
        if self.kind == "message":
            if (
                self.account_id is None
                or self.source_message_id is None
                or self.source_resource_key is not None
            ):
                raise ValueError("message scope requires account and message")
            return
        if self.kind == "resource":
            if self.source_resource_key is None:
                raise ValueError("resource scope requires a resource key")
            return
        raise ValueError(f"unsupported source scope kind: {self.kind}")

    @classmethod
    def catalog(cls) -> SourceScope:
        return cls(kind="catalog")

    @classmethod
    def conversation(cls, account_id: str, conversation_source_id: str) -> SourceScope:
        return cls(
            kind="conversation",
            account_id=account_id,
            conversation_source_id=conversation_source_id,
        )

    @classmethod
    def conversations(
        cls, account_id: str, conversation_source_ids: tuple[str, ...],
    ) -> SourceScope:
        return cls(kind="conversations", account_id=account_id,
                   conversation_source_ids=conversation_source_ids)

    @classmethod
    def message(
        cls,
        account_id: str,
        source_message_id: str,
        *,
        conversation_source_id: str | None = None,
    ) -> SourceScope:
        return cls(
            kind="message",
            account_id=account_id,
            source_message_id=source_message_id,
            conversation_source_id=conversation_source_id,
        )

    @classmethod
    def resource(
        cls,
        source_resource_key: str,
        *,
        account_id: str | None = None,
        conversation_source_id: str | None = None,
        source_message_id: str | None = None,
    ) -> SourceScope:
        return cls(
            kind="resource",
            source_resource_key=source_resource_key,
            account_id=account_id,
            conversation_source_id=conversation_source_id,
            source_message_id=source_message_id,
        )


@dataclass(frozen=True)
class SourceHealth:
    configured: bool
    available: bool
    account_count: int
    source_state: str
    fresh_as_of: str
    inventory_digest: str
    generation_set_digest: str
    shard_counts: dict[str, int]
    warnings: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        return self.configured and self.available and self.source_state == "complete"

    def as_dict(self) -> dict[str, Any]:
        return {
            "configured": self.configured,
            "available": self.available,
            "account_count": self.account_count,
            "source_state": self.source_state,
            "fresh_as_of": self.fresh_as_of,
            "inventory_digest": self.inventory_digest,
            "generation_set_digest": self.generation_set_digest,
            "shard_counts": dict(self.shard_counts),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class SourceSnapshot:
    inventory_digest: str
    generation_set_digest: str
    fresh_as_of: str
    generation_by_shard: tuple[tuple[str, str], ...]
    token: str
    scope: SourceScope | None = None
    # Providers populate this per-snapshot map only for message shards that served
    # the current target. Cursor binding can therefore detect replacement of a
    # selected shard without inheriting unrelated account-wide generations.
    dependency_generation_by_shard: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class SourceProviderDescriptor:
    kind: str
    implementation: str
    source_mode: Literal["synthetic", "live"]
    platform: tuple[str, ...]
    supports_incremental: bool
    supports_resources: bool
    requires_running_app_for_key_refresh: bool
    # True only when every returned SourceMessage already carries the stable
    # sender identity and current label evidence needed for message projection.
    # Such providers need no separate roster scan before ordinary message reads.
    message_sender_evidence_complete: bool = False


@dataclass(frozen=True)
class SourcePreparationStep:
    """One bounded search-preparation step, not a durable source checkpoint.

    Counts/phase are content-free. Only the final step carries a private source page;
    that page still needs the enclosing conversation session's exit validation before
    admission. Continue the iterator on the same thread/session. After cancellation,
    session loss or restart, discard it and start a new attempt at zero.
    """

    phase: Literal["positions", "payload", "complete"]
    scanned_rows: int
    completed_shards: int
    total_shards: int
    page: SourceMessagePage | None = None


def _check_preparation(check_authority: Callable[[], None] | None) -> None:
    check_operation_budget()
    if check_authority is not None:
        check_authority()
    check_operation_budget()


@dataclass(frozen=True)
class _PreparationCandidate:
    key: tuple[Any, ...]
    value: Any
    forward: bool

    def __lt__(self, other: _PreparationCandidate) -> bool:
        return self.key > other.key if self.forward else self.key < other.key


class _PreparationWindow:
    """Keep only the requested chronological positions, with the worst at root."""

    def __init__(self, limit: int, *, forward: bool) -> None:
        self.limit = limit
        self.forward = forward
        self._heap: list[_PreparationCandidate] = []

    def offer(self, key: tuple[Any, ...], value: Any) -> None:
        candidate = _PreparationCandidate(key, value, self.forward)
        if len(self._heap) < self.limit:
            heapq.heappush(self._heap, candidate)
        elif (key < self._heap[0].key if self.forward else key > self._heap[0].key):
            heapq.heapreplace(self._heap, candidate)

    def selected(self) -> list[Any]:
        return [
            item.value for item in sorted(
                self._heap, key=lambda item: item.key, reverse=not self.forward
            )
        ]


@runtime_checkable
class ContextSourceProvider(Protocol):
    """Optional exact neighborhood read around a focus from the same session.

    The page includes the focus and its immediate unfiltered neighbors in canonical
    order. Each has_more flag requires a further unique neighbor on that requested
    side; a zero-radius side performs no neighbor seek and has a false flag.
    """

    def read_context(
        self,
        account_id: str,
        conversation_source_id: str,
        *,
        focus: SourceMessage,
        before: int,
        after: int,
        snapshot: SourceSnapshot,
    ) -> SourceMessagePage: ...


class WeChatSourceProvider(Protocol):
    @property
    def descriptor(self) -> SourceProviderDescriptor: ...

    def health(self) -> SourceHealth: ...

    def list_accounts(self, snapshot: SourceSnapshot) -> list[SourceAccount]: ...

    def list_conversations(
        self, account_id: str, snapshot: SourceSnapshot
    ) -> list[SourceConversation]: ...

    def get_conversation(
        self,
        account_id: str,
        conversation_source_id: str,
        snapshot: SourceSnapshot,
    ) -> SourceConversation | None: ...

    def resolve_conversation(
        self, account_id: str, query: str, snapshot: SourceSnapshot
    ) -> list[ConversationCandidate]: ...

    def list_participants(
        self,
        account_id: str,
        conversation_source_id: str,
        snapshot: SourceSnapshot,
    ) -> list[SourceParticipant]: ...

    def resolve_participant(
        self,
        account_id: str,
        conversation_source_id: str,
        query: str,
        snapshot: SourceSnapshot,
    ) -> list[SourceParticipant]: ...

    def read_recent(
        self,
        account_id: str,
        conversation_source_id: str,
        limit: int,
        snapshot: SourceSnapshot,
    ) -> SourceMessagePage: ...

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
    ) -> SourceMessagePage: ...

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
    ) -> Iterator[SourcePreparationStep]: ...

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
    ) -> SourceDiscoveryPage: ...

    def search_generation_binding(
        self,
        account_id: str,
        conversation_source_id: str,
        *,
        snapshot: SourceSnapshot,
    ) -> tuple[tuple[str, str], ...]: ...

    def get_message(
        self,
        account_id: str,
        source_message_id: str,
        snapshot: SourceSnapshot,
    ) -> SourceMessage | None: ...

    def read_resource(
        self,
        source_resource_key: str,
        *,
        max_bytes: int,
        snapshot: SourceSnapshot,
    ) -> SourceResourcePayload: ...

    def capture_resource_binding(
        self, request: CaptureRequest, snapshot: SourceSnapshot,
    ) -> ResourceCaptureBinding: ...

    def catalog_complete(self, snapshot: SourceSnapshot) -> bool: ...

    def active_conversations_only(self, snapshot: SourceSnapshot) -> bool: ...

    def snapshot(self) -> AbstractContextManager[SourceSnapshot]: ...

    def session(self, scope: SourceScope) -> AbstractContextManager[SourceSnapshot]: ...

from __future__ import annotations

import copy
import hashlib
import json
import re
import sys
import threading
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from sightglass.contracts.common import (
    Coverage,
    SourceReceipt,
    SourceSortKey,
    parse_aware_datetime,
    to_utc_iso,
    utc_now,
)
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.identity import (
    SourceAccount,
    SourceConversation,
    SourceParticipant,
)
from sightglass.contracts.messages import SourceMessage, SourceMessagePage
from sightglass.contracts.voice import VoiceReadSettings
from sightglass.model.coverage import key_from_position, record_window, state_frontier
from sightglass.model.current_body import current_search_document
from sightglass.model.repositories import WindowRepository, message_search_fields
from sightglass.operations import (
    check_operation_budget,
    local_read_only_requested,
    operation_budget,
)
from sightglass.policy.readers import ReaderContext
from sightglass.reader.cursors import (
    AccountCursorCodec,
    SearchCursorCodec,
    TimelineCursorCodec,
    cursor_scope,
    search_scope_digest,
    source_sort_key,
)
from sightglass.reader.deliveries import DeliveryPayloadStore
from sightglass.reader.projections import (
    CompactBodyBudgetAllocator,
    CompactMessageProjector,
    CompactPreparedRow,
    DetailMessageProjector,
    trim_detail_projection,
)
from sightglass.reader.retrieval import (
    CONTEXT_RADIUS_MESSAGES,
    CONTEXT_RADIUS_SECONDS,
    MAX_CONTEXT_LINKS,
    RetrievalService,
)
from sightglass.reader.voice_read import VoiceCandidate, VoiceReadPreparation
from sightglass.residency.decisions import ResidencyDecision, ResidencySettings
from sightglass.residency.repository import ResidencyRepository
from sightglass.resources.processors import processor_status
from sightglass.resources.service import ResourceService
from sightglass.resources.types import ResourceReadPayload
from sightglass.semantic.service import SemanticService
from sightglass.source.base import (
    ContextSourceProvider,
    SourceHealth,
    SourceScope,
    SourceSnapshot,
    WeChatSourceProvider,
)
from sightglass.source.identity import SignedTokenCodec, opaque_id
from sightglass.source.links import extract_links, hint_evidence, normalize_domain
from sightglass.source.parser import PARSER_VERSION, parse_message
from sightglass.storage import storage_scope
from sightglass.voice.service import VoiceService

CATALOG_ROTATION_SCHEMA = "sightglass.catalog-rotation.v1"
CONVERSATION_IDENTITY_CONFLICT_WARNING = "duplicate_message_identity_conflict"
DEGRADED_BACKFILL_STATE = "partial"
# Search validates at most this many durable-index candidates per call. The scan
# reads ordered keyset batches and carries its exact position into the continuation
# cursor, so a later page never revalidates candidates already consumed.
SEARCH_SCAN_BATCH_LIMIT = 200
SEARCH_SCAN_CANDIDATE_BUDGET = 1_000
SEARCH_PREPARATION_MESSAGE_BUDGET = 200
SEARCH_PREPARATION_CONVERSATION_BUDGET = 25
# One discovery attempt scans at most this many raw source rows before yielding so
# the job stays bounded and cancellable; the job resumes from the stored source
# position on the next attempt instead of re-reading the same page forever.
DISCOVERY_CONVERSATION_SCAN_BUDGET = 1_000


def _is_conversation_identity_conflict(error: SightglassError) -> bool:
    """True when one conversation's shards disagree about a message identity.

    The source reader fails closed on a conflicting duplicate identity rather than
    merging or guessing. That verdict is scoped to the conversation being read, so the
    sync degrades just that conversation instead of failing the whole rotation.
    """

    if error.code != ErrorCode.SOURCE_INCOMPLETE:
        return False
    codes = error.details.get("warning_codes")
    return isinstance(codes, list | tuple) and CONVERSATION_IDENTITY_CONFLICT_WARNING in codes


@dataclass(frozen=True)
class _CatalogConversation:
    account_id: str
    source_account_key: str
    conversation_id: str
    source: SourceConversation


@dataclass(frozen=True)
class _SyncConversationPlan:
    entry: _CatalogConversation
    prepared: tuple[_PreparedMessage, ...]
    pending: bool
    frontier: SourceSortKey | None
    floor: SourceSortKey | None
    history_complete: bool


@dataclass(frozen=True)
class _PreparedMessage:
    source: SourceMessage
    participant: SourceParticipant | None
    parsed: Any


@dataclass(frozen=True)
class _CatalogRead:
    """Provider account/conversation catalog read outside the writer transaction."""

    accounts: tuple[tuple[str, SourceAccount], ...]
    catalog: tuple[_CatalogConversation, ...]

    @property
    def mappings(self) -> dict[str, str]:
        return {account_id: source.source_account_key for account_id, source in self.accounts}


@dataclass(frozen=True)
class _SourceTarget:
    """Provider-facing source identifiers for one conversation."""

    account_id: str
    source_account_key: str
    conversation_id: str
    source_conversation_id: str
    roster_complete: bool = False

    def as_read_context(self) -> dict[str, Any]:
        """Source-facing conversation view usable before the row is admitted."""

        return {
            "account_id": self.account_id,
            "conversation_id": self.conversation_id,
            "source_account_key": self.source_account_key,
            "source_conversation_id": self.source_conversation_id,
            "roster_complete": self.roster_complete,
        }


@dataclass(frozen=True)
class _CatalogFacts:
    """Catalog-level facts captured while the source snapshot is still open."""

    complete: bool
    active_conversations_only: bool


@dataclass(frozen=True)
class _MaterializedTarget:
    context: Any
    conversation_state: Any | None
    catalog_state: Any | None
    target_row: Any | None = None
    view: str = "auto"


@dataclass(frozen=True)
class _FrozenMaterializedPage:
    page: dict[str, Any]
    target: _MaterializedTarget
    rows: list[Any]
    focus_ids: frozenset[str]
    voice_candidates: tuple[VoiceCandidate, ...]
    receipt: dict[str, Any]


class ReaderService:
    def __init__(
        self,
        provider: WeChatSourceProvider,
        repository: WindowRepository,
        reader: ReaderContext,
        token_codec: SignedTokenCodec,
        auth_token_hash: str | None = None,
        voice_service: VoiceService | None = None,
        voice_settings: VoiceReadSettings | None = None,
        default_view: str = "auto",
    ) -> None:
        if default_view not in {"auto", "replica"}:
            raise ValueError("default_view must be auto or replica")
        self.default_view = default_view
        self._provider = provider
        self._operation_provider: ContextVar[WeChatSourceProvider | None] = ContextVar(
            "sightglass_reader_operation_provider", default=None
        )
        self._capture_hooks: ContextVar[tuple[Callable[[Any], None] | None,
                                              Callable[[], None] | None]] = ContextVar(
            "sightglass_reader_capture_hooks", default=(None, None)
        )
        self._captured_search_candidates: ContextVar[tuple[str, ...] | None] = ContextVar(
            "sightglass_reader_captured_search_candidates", default=None
        )
        self._captured_updates_after: ContextVar[SourceSortKey | None] = ContextVar(
            "sightglass_reader_captured_updates_after", default=None
        )
        self._captured_updates_ids: ContextVar[tuple[str, ...] | None] = ContextVar(
            "sightglass_reader_captured_updates_ids", default=None
        )
        self._captured_updates_reconcile_revision: ContextVar[int | None] = ContextVar(
            "sightglass_reader_captured_updates_reconcile_revision", default=None
        )
        self._captured_discovery: ContextVar[Any | None] = ContextVar(
            "sightglass_reader_captured_discovery", default=None
        )
        self.repository = repository
        self.reader = reader
        self.token_codec = token_codec
        self.auth_token_hash = auth_token_hash
        self.search_preparation: Any | None = None
        self.storage = repository.database.storage
        self.voice_settings = voice_settings or VoiceReadSettings()
        self.voice = (
            VoiceReadPreparation(voice_service, self.voice_settings, repository)
            if voice_service is not None
            else None
        )
        self.timeline_cursors = TimelineCursorCodec(token_codec)
        self.search_cursors = SearchCursorCodec(token_codec)
        self.account_cursors = AccountCursorCodec(token_codec)
        self.delivery_store = DeliveryPayloadStore(repository.database.path, storage=self.storage)
        self.residency = ResidencyRepository(repository.database)
        self.resource_service = ResourceService(
            provider,
            repository,
            reader,
            projection_epoch=self._projection_inventory_epoch,
        )
        self.detail_projector = DetailMessageProjector(repository, token_codec)
        self.compact_projector = CompactMessageProjector(repository)
        self.semantic: SemanticService | None = None
        self.semantic_unavailable_reason: str | None = None
        self.retrieval = RetrievalService(self)
        from sightglass.reader.replica import ReplicaReader

        self.replica = ReplicaReader(self)
        self._status_lock = threading.Lock()
        self._cold_status = self._status_payload(
            SourceHealth(
                configured=True,
                available=False,
                account_count=0,
                source_state="unknown",
                fresh_as_of="",
                inventory_digest="",
                generation_set_digest="",
                shard_counts={},
                warnings=("source_health_not_yet_observed",),
            ),
            [],
        )
        self._cached_status: dict[str, Any] | None = copy.deepcopy(self._cold_status)

    @property
    def provider(self) -> WeChatSourceProvider:
        """An immutable captured provider is scoped to this operation's context."""
        return self._operation_provider.get() or self._provider

    @provider.setter
    def provider(self, provider: WeChatSourceProvider) -> None:
        # Existing fixture/setup callers select the base provider before use.
        # Per-operation capture must use captured_provider instead.
        self._provider = provider

    @contextmanager
    def captured_provider(
        self, provider: WeChatSourceProvider, *,
        before_commit: Callable[[Any], None] | None = None,
        after_commit: Callable[[], None] | None = None,
        search_candidate_ids: tuple[str, ...] | None = None,
        updates_after: SourceSortKey | None = None,
        updates_message_ids: tuple[str, ...] | None = None,
        updates_reconcile_revision: int | None = None,
        discovery: Any | None = None,
    ) -> Iterator[None]:
        if (search_candidate_ids is not None and len(search_candidate_ids) > 200
                or updates_message_ids is not None and len(updates_message_ids) > 200):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        token = self._operation_provider.set(provider)
        hook_token = self._capture_hooks.set((before_commit, after_commit))
        search_token = self._captured_search_candidates.set(search_candidate_ids)
        updates_token = self._captured_updates_after.set(updates_after)
        updates_ids_token = self._captured_updates_ids.set(updates_message_ids)
        reconcile_token = self._captured_updates_reconcile_revision.set(updates_reconcile_revision)
        discovery_token = self._captured_discovery.set(discovery)
        try:
            yield
        finally:
            self._captured_discovery.reset(discovery_token)
            self._captured_updates_reconcile_revision.reset(reconcile_token)
            self._captured_updates_ids.reset(updates_ids_token)
            self._captured_updates_after.reset(updates_token)
            self._captured_search_candidates.reset(search_token)
            self._capture_hooks.reset(hook_token)
            self._operation_provider.reset(token)

    def resolve_view(self, view: str | None = None, *, refresh: bool = False) -> str:
        if view is not None and view not in {"replica", "fresh"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if refresh and view == "replica":
            raise SightglassError(ErrorCode.QUERY_INVALID)
        return "fresh" if refresh else (view or self.default_view)

    @staticmethod
    def _view_scope(scope_key: str, view: str) -> str:
        return scope_key if view == "auto" else opaque_id("wxviewscope", scope_key, view)

    def _status_payload(
        self, health: SourceHealth, accounts: list[dict[str, Any]]
    ) -> dict[str, Any]:
        descriptor = self.provider.descriptor
        local_messages = self.repository.has_materialized_read_plane(
            self._projection_inventory_epoch()
        )
        local_resources = self.repository.has_resource_bindings()
        usable = health.complete or local_messages or local_resources
        result = {
            "schema": "sightglass.status.v1",
            "ready": usable and not self.reader.paused,
            "paused": self.reader.paused,
            "reader": {
                "reader_id": self.reader.reader_id,
                "display_name": self.reader.display_name,
            },
            "source": {
                **health.as_dict(),
                "kind": descriptor.kind,
                "implementation": descriptor.implementation,
                "mode": descriptor.source_mode,
            },
            "accounts": accounts,
            # Local readability is distinct from live-source refresh health: an
            # unavailable provider does not by itself make already-materialized content
            # unreadable, and a local read is never presented as a fresh live
            # confirmation. Callers that need current live data must read the live
            # refresh fields, not this block.
            "read_plane": {
                "schema": "sightglass.read-plane.v1",
                "live_refresh_available": bool(health.complete),
                "local_cache_reads": local_resources,
                "local_message_reads": local_messages,
            },
            "readiness": {
                "indexed_reads": "ready" if local_messages else "empty",
                "live_refresh": "ready" if health.complete else "degraded",
                "resource_cache": "ready" if local_resources else "empty",
                "resource_acquisition": (
                    "ready"
                    if health.complete and descriptor.supports_resources
                    else "degraded"
                ),
                "voice": "configured" if self.voice is not None else "disabled",
                **self.retrieval.readiness(),
            },
            "capabilities": {
                "messages": self.reader.policy.messages,
                "search": self.reader.policy.search,
                "updates": self.reader.policy.messages,
                "resources": (
                    descriptor.supports_resources and self.reader.policy.resource_metadata
                ),
                "resource_metadata": (
                    descriptor.supports_resources and self.reader.policy.resource_metadata
                ),
                "resource_preview": (
                    descriptor.supports_resources and self.reader.policy.resource_preview
                ),
                "resource_original": (
                    descriptor.supports_resources and self.reader.policy.resource_original
                ),
                "wgo_knowledge": False,
                "synthetic_only": descriptor.source_mode == "synthetic",
                "live_refresh": descriptor.source_mode == "live",
                "background_incremental": descriptor.supports_incremental,
            },
            "resource_processors": processor_status(),
            "window_db": {"schema_version": self.repository.database.schema_version},
        }
        return self._with_storage_status(result)

    def _with_storage_status(self, result: dict[str, Any]) -> dict[str, Any]:
        result["storage"] = self.repository.database.storage_status()
        if result["storage"]["state"] == "hard_limit":
            result["ready"] = False
        return result

    def local_only_tool_call(self, name: str, arguments: dict[str, Any]) -> bool:
        """Inspectable, state-aware classification of demonstrably local-only reads.

        Used by the daemon to route a call without claiming foreground priority from
        the background source worker. Only tools whose exact arguments resolve to local
        state are eligible: status is local, and a resource read is local only when the
        private CAS already holds the bytes the requested mode needs, the reader is
        authorized, and the canonical resolver is active. Any other tool, or any
        resource that would need a provider snapshot, is source-required and keeps the
        existing foreground exclusion. The check never guesses from the tool name alone.
        """

        if name == "wechat_read_messages":
            return self.local_message_read_ready(arguments)
        if name in {"wechat_search_messages", "wechat_find_links", "wechat_retrieve"}:
            try:
                view = self.resolve_view(arguments.get("view"),
                                         refresh=arguments.get("refresh", False))
            except SightglassError:
                return True
            if view == "replica":
                return True
            if view == "fresh":
                return False
        if self.default_view == "replica" and name in {
            "wechat_status", "wechat_find_conversations", "wechat_find_participants",
            "wechat_read_inbox",
        }:
            return True
        if name == "wechat_search_messages" and self.search_preparation is not None:
            return self.search_preparation.local_request(arguments)
        if name == "wechat_read_inbox":
            return self.local_inbox_read_ready(arguments)
        if name in {"wechat_find_resources", "wechat_find_links", "wechat_retrieve"}:
            # Discovery requests only enqueue/poll bounded preparation or read the
            # materialized result. The preparation worker owns every source lease;
            # the initiating call must not wait for source foreground exclusion.
            return True
        if name == "wechat_read_resource":
            resource_id = arguments.get("resource_id")
            mode = arguments.get("mode")
            if not isinstance(resource_id, str) or not isinstance(mode, str):
                return False
            try:
                return self.resource_service.local_read_ready(resource_id, mode)
            except Exception:
                # A probe must never turn a source-required call into a failure here;
                # fall back to the foreground path, which reports the real error.
                return False
        return False

    def _read_accounts(self, snapshot: SourceSnapshot) -> tuple[tuple[str, SourceAccount], ...]:
        """Read provider accounts without opening the conversation catalog."""

        accounts = tuple(
            (
                self.repository.account_id_for(source_account.source_account_key),
                source_account,
            )
            for source_account in self.provider.list_accounts(snapshot)
        )
        with self._status_lock:
            cached = copy.deepcopy(self._cached_status or self._cold_status)
            cached["accounts"] = [
                {
                    "account_id": account_id,
                    "display_name": source_account.display_name,
                    "active": True,
                }
                for account_id, source_account in accounts
            ]
            cached["source"]["account_count"] = len(accounts)
            self._cached_status = cached
        return accounts

    def _read_catalog(self, snapshot: SourceSnapshot) -> _CatalogRead:
        """Read the provider catalog without opening a window.db writer transaction."""

        accounts = self._read_accounts(snapshot)
        catalog: list[_CatalogConversation] = []
        for account_id, source_account in accounts:
            check_operation_budget()
            for source_conversation in self.provider.list_conversations(
                source_account.source_account_key, snapshot
            ):
                check_operation_budget()
                catalog.append(
                    _CatalogConversation(
                        account_id=account_id,
                        source_account_key=source_account.source_account_key,
                        conversation_id=self.repository.conversation_id_for(
                            account_id, source_conversation.source_conversation_id
                        ),
                        source=source_conversation,
                    )
                )
        return _CatalogRead(accounts, tuple(catalog))

    def _persist_catalog(
        self, catalog_read: _CatalogRead, snapshot: SourceSnapshot
    ) -> dict[str, str]:
        """Write the account, conversation, and reader identity rows of one snapshot."""

        mappings: dict[str, str] = {}
        for account_id, source_account in catalog_read.accounts:
            self.repository.upsert_account(source_account, snapshot.fresh_as_of)
            mappings[account_id] = source_account.source_account_key
        for entry in catalog_read.catalog:
            self.repository.upsert_conversation(
                entry.account_id, entry.source, snapshot.fresh_as_of
            )
        self.repository.upsert_reader(
            self.reader.reader_id,
            self.reader.display_name,
            self.reader.policy.as_dict(),
            snapshot.fresh_as_of,
            auth_token_hash=self.auth_token_hash,
        )
        return mappings

    def _catalog_facts(self, snapshot: SourceSnapshot) -> _CatalogFacts:
        """Capture catalog-level flags while the source snapshot is still open."""

        return _CatalogFacts(
            complete=bool(self.provider.catalog_complete(snapshot)),
            active_conversations_only=bool(self.provider.active_conversations_only(snapshot)),
        )

    def _persisted_catalog_facts(
        self, target: _SourceTarget, snapshot: SourceSnapshot
    ) -> _CatalogFacts:
        """Project catalog coverage without rescanning an already admitted live target."""

        state = self.repository.source_catalog_state(target.account_id)
        generations_match = self.repository.source_shard_generations(target.account_id) == dict(
            snapshot.generation_by_shard
        )
        complete = bool(
            state is not None
            and str(state["source_inventory_epoch"])
            == self._projection_inventory_epoch()
            and str(state["coverage_state"]) == "complete"
            and generations_match
        )
        return _CatalogFacts(complete=complete, active_conversations_only=False)

    def _source_target(self, catalog_read: _CatalogRead, conversation_id: str) -> _SourceTarget:
        """Resolve source ids for a conversation without requiring an admitted row."""

        entry = next(
            (item for item in catalog_read.catalog if item.conversation_id == conversation_id),
            None,
        )
        if entry is not None:
            self.reader.authorize(conversation_id)
            return _SourceTarget(
                account_id=entry.account_id,
                source_account_key=entry.source_account_key,
                conversation_id=entry.conversation_id,
                source_conversation_id=entry.source.source_conversation_id,
                roster_complete=bool(entry.source.roster_complete),
            )
        return self._persisted_source_target(conversation_id)

    def _persisted_source_target(self, conversation_id: str) -> _SourceTarget:
        """Resolve a previously admitted target without refreshing the full catalog."""

        row = self.repository.conversation_context(conversation_id)
        if row is None:
            raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        self.reader.authorize(conversation_id)
        return _SourceTarget(
            account_id=str(row["account_id"]),
            source_account_key=str(row["source_account_key"]),
            conversation_id=conversation_id,
            source_conversation_id=str(row["source_conversation_id"]),
            roster_complete=bool(row["roster_complete"]),
        )

    @contextmanager
    def _source_read(self) -> Iterator[tuple[ExitStack, SourceSnapshot]]:
        """Open a provider snapshot outside any window.db writer transaction."""

        stack = ExitStack()
        check_operation_budget()
        try:
            if self.storage is not None:
                self.storage.require()
            stack.enter_context(storage_scope(self.storage))
            snapshot = stack.enter_context(self.provider.snapshot())
            yield stack, snapshot
        except BaseException:
            stack.__exit__(*sys.exc_info())
            raise
        else:
            stack.close()
            self._refresh_status_after_read(snapshot)

    def _refresh_status_after_read(self, snapshot: SourceSnapshot) -> None:
        with self._status_lock:
            cached = copy.deepcopy(self._cached_status or self._cold_status)
            cached["ready"] = not self.reader.paused
            cached["source"].update(
                {
                    "configured": True,
                    "available": True,
                    "source_state": "complete",
                    "fresh_as_of": snapshot.fresh_as_of,
                    "inventory_digest": snapshot.inventory_digest,
                    "generation_set_digest": snapshot.generation_set_digest,
                    "warnings": [],
                }
            )
            local_messages = self.repository.has_materialized_read_plane(
                self._projection_inventory_epoch()
            )
            local_resources = self.repository.has_resource_bindings()
            cached["read_plane"].update(
                {
                    "live_refresh_available": True,
                    "local_cache_reads": local_resources,
                    "local_message_reads": local_messages,
                }
            )
            cached["readiness"].update(
                {
                    "indexed_reads": "ready" if local_messages else "empty",
                    "live_refresh": "ready",
                    "resource_cache": "ready" if local_resources else "empty",
                    "resource_acquisition": (
                        "ready" if self.provider.descriptor.supports_resources else "degraded"
                    ),
                }
            )
            self._cached_status = cached

    @contextmanager
    def _source_session(
        self,
        scope: SourceScope,
        *,
        accounted_generations: dict[str, str] | None = None,
    ) -> Iterator[tuple[ExitStack, SourceSnapshot]]:
        """Open a dependency-scoped provider session outside any writer transaction.

        Used for target reads whose exact dependency is already known (one
        conversation or one message). When ``accounted_generations`` is supplied, the
        exposed snapshot reports the account's last admitted shard generations, so
        coverage math stays honest about a read that only refreshed one target instead
        of rescanning the whole catalog. The provider still validates only the
        databases the scoped read actually opened.
        """

        stack = ExitStack()
        check_operation_budget()
        try:
            if self.storage is not None:
                self.storage.require()
            stack.enter_context(storage_scope(self.storage))
            session_snapshot = stack.enter_context(self.provider.session(scope))
            if accounted_generations is not None:
                session_snapshot = replace(
                    session_snapshot,
                    generation_by_shard=tuple(sorted(accounted_generations.items())),
                )
            yield stack, session_snapshot
        except BaseException:
            stack.__exit__(*sys.exc_info())
            raise
        else:
            stack.close()
            # A narrow session proves only its selected dependency set. It must not
            # overwrite account-wide source health/digests or claim that the catalog
            # is complete; those facts belong exclusively to a full source snapshot.

    @contextmanager
    def _admission(self, snapshot_stack: ExitStack) -> Iterator[None]:
        """Run one window.db admission transaction that is validated before commit.

        The source read phase must already be finished: this transaction only performs
        local window.db work, and its commit happens only after the provider snapshot
        validates itself successfully.
        """

        with self.repository.database.transaction() as connection:
            yield
            snapshot_stack.close()
            before_commit, after_commit = self._capture_hooks.get()
            if before_commit is not None:
                before_commit(connection)
            if after_commit is not None:
                self.repository.database.wake_after_commit(after_commit)

    def status(self, detail: str = "summary") -> dict[str, Any]:
        if detail not in {"summary", "sources", "capabilities"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if self.default_view == "replica":
            return self.replica.status()
        health = self.provider.health()
        if health.complete and (self.storage is None or self.storage.status()["admission_allowed"]):
            with self._source_read() as (stack, snapshot):
                account_read = _CatalogRead(self._read_accounts(snapshot), ())
                with self._admission(stack):
                    self._persist_catalog(account_read, snapshot)
        accounts = [
            {
                "account_id": str(row["account_id"]),
                "display_name": str(row["current_display_name"]),
                "active": bool(row["active"]),
            }
            for row in self.repository.active_accounts()
        ]
        result = self._status_payload(health, accounts)
        with self._status_lock:
            self._cached_status = copy.deepcopy(result)
        return result

    def cached_status(self, detail: str = "summary") -> dict[str, Any]:
        """Return the last completed health snapshot without source or writer work."""

        if detail not in {"summary", "sources", "capabilities"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if self.default_view == "replica":
            return self.replica.status()
        with self._status_lock:
            cached = copy.deepcopy(self._cached_status)
        if cached is None:
            cached = copy.deepcopy(self._cold_status)
        cached["paused"] = self.reader.paused
        live_ready = bool(cached["source"].get("source_state") == "complete")
        cached["read_plane"]["live_refresh_available"] = live_ready
        cached["readiness"]["live_refresh"] = "ready" if live_ready else "degraded"
        cached["readiness"]["resource_acquisition"] = (
            "ready"
            if live_ready and self.provider.descriptor.supports_resources
            else "degraded"
        )
        cached["ready"] = bool(
            live_ready
            or cached["read_plane"].get("local_message_reads")
            or cached["read_plane"].get("local_cache_reads")
        ) and not self.reader.paused
        return self._with_storage_status(cached)

    def _mark_resource_cache_ready(self) -> None:
        """Refresh only the in-memory summary after a verified resource cache hit."""

        with self._status_lock:
            cached = copy.deepcopy(self._cached_status or self._cold_status)
            cached["read_plane"]["local_cache_reads"] = True
            cached["readiness"]["resource_cache"] = "ready"
            if not self.reader.paused:
                cached["ready"] = True
            self._cached_status = cached

    def scope_summary(self) -> dict[str, Any]:
        """Return content-free account/catalog policy counts for the operator."""

        with self._source_read() as (stack, snapshot):
            catalog_read = self._read_catalog(snapshot)
            facts = self._catalog_facts(snapshot)
            with self._admission(stack):
                mappings = self._persist_catalog(catalog_read, snapshot)
        rows = [
            row
            for account_id in mappings
            for row in self.repository.account_conversations(account_id)
        ]
        return {
            "schema": "sightglass.scope-status.v1",
            "mode": self.reader.policy.mode,
            "account_count": len(mappings),
            "discoverable_conversation_count": len(rows),
            "authorized_conversation_count": sum(
                self.reader.policy.permits(str(row["conversation_id"])) for row in rows
            ),
            "direct_count": sum(str(row["kind"]) == "direct" for row in rows),
            "group_count": sum(str(row["kind"]) == "group" for row in rows),
            "allow_count": len(self.reader.policy.allowed_conversation_ids),
            "deny_count": len(self.reader.policy.denied_conversation_ids),
            "catalog_complete": facts.complete,
            "active_conversations_only": facts.active_conversations_only,
        }

    def _account_source_key(
        self, mappings: dict[str, str], requested_account_id: str | None
    ) -> tuple[str, str]:
        if requested_account_id:
            if requested_account_id not in mappings:
                raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)
            return requested_account_id, mappings[requested_account_id]
        if len(mappings) != 1:
            raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)
        account_id = next(iter(mappings))
        return account_id, mappings[account_id]

    def _policy_revision(self) -> str:
        encoded = json.dumps(
            self.reader.policy.as_dict(), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _materialized_cursor_revision(self) -> str:
        """Bind local traversal to policy and the current identity projection."""

        with self.repository.database.connection() as connection:
            maintenance = connection.execute(
                "SELECT revision FROM observation_maintenance_state WHERE singleton=1"
            ).fetchone()
        encoded = json.dumps(
            {
                "observation_repair_revision": int(maintenance[0]) if maintenance else 0,
                "residency_revision": self.residency.revision(),
                "policy": self._policy_revision(),
                "identity_correction_revision": (self.repository.identity_correction_revision()),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _projection_inventory_epoch(self) -> str:
        """Bind persisted current-tail projections to interpretation code.

        A source append or catalog activity change must not invalidate every persisted
        sender/text projection. Source inventory and physical generations retain their
        separate validation paths; this epoch changes only when provider/parser meaning
        changes, making the bounded live-tail rotation re-admit each permitted tail once.
        """

        encoded = json.dumps(
            {
                "schema": "sightglass.tail-projection.v1",
                "provider_implementation": self.provider.descriptor.implementation,
                "parser_version": PARSER_VERSION,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _scope_digest(value: dict[str, Any]) -> str:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _catalog_handled_ids(
        self,
        account_id: str,
        *,
        projection_epoch: str,
    ) -> set[str]:
        """Current-catalog conversations whose projection is settled for readiness.

        A conversation counts as handled when it either carries the current projection
        epoch or is explicitly degraded with the exact identity-conflict attention code.
        A degraded conversation is *not* stamped with the epoch: it is excluded from the
        indexed inbox so its stale sender projection cannot leak, while the rest of the
        account can still complete. No other missing or error state qualifies.
        """

        handled = self.repository.indexed_conversation_ids(
            account_id,
            inventory_epoch=projection_epoch,
        )
        handled |= self.repository.degraded_conversation_ids(
            account_id,
            error_code=CONVERSATION_IDENTITY_CONFLICT_WARNING,
        )
        # On-demand catalog readiness needs no tail collection. This is an
        # eligibility decision; it writes no source coverage or sync frontier.
        handled |= self.residency.on_demand_conversation_ids(account_id)
        return handled

    @staticmethod
    def _activity_position(
        last_message_at: str | None, kind: str, conversation_id: str
    ) -> list[Any]:
        if last_message_at is None:
            return [1, 0, kind, conversation_id]
        micros = int(parse_aware_datetime(last_message_at).timestamp() * 1_000_000)
        return [0, -micros, kind, conversation_id]

    def sync_source_once(
        self,
        *,
        initial_tail: int = 50,
        batch_limit: int = 200,
        conversation_limit: int = 25,
    ) -> dict[str, Any]:
        """Admit a bounded, rotating live tail for the authorized catalog.

        The provider snapshot, catalog scan, and per-conversation source reads all run
        before the window.db writer transaction opens, so a slow native scan cannot
        hold the ``BEGIN IMMEDIATE`` lock that operator and reader requests need.
        Persistence stays in one short transaction whose commit still happens only
        after the snapshot context validates itself inside that transaction.
        """

        self.residency.release_expired_leases(limit=100)
        self.reader.require_active()
        initial_tail = max(1, min(int(initial_tail), 500))
        batch_limit = max(1, min(int(batch_limit), 500))
        conversation_limit = max(1, min(int(conversation_limit), 500))
        self._pause_backfill_under_pressure()

        conversation_count = 0
        message_count = 0
        pending_conversation_count = 0
        conflicted_conversation_ids: list[str] = []

        with self._source_read() as (stack, snapshot):
            accounts = self._read_accounts(snapshot)
            projection_epoch = self._projection_inventory_epoch()
            if self._source_snapshot_already_indexed(accounts, snapshot):
                stack.close()
                return {
                    "schema": "sightglass.source-sync.v1",
                    "conversation_count": 0,
                    "message_count": 0,
                    "pending_conversation_count": 0,
                    "conflict_conversation_count": 0,
                }
            catalog_read = self._read_catalog(snapshot)
            coverage_state = "complete" if self._catalog_facts(snapshot).complete else "partial"
            plans: list[_SyncConversationPlan] = []
            next_cursor_by_account: dict[str, str | None] = {}
            for account_id, _source_account in catalog_read.accounts:
                check_operation_budget()
                chosen, next_cursor = self._rotate_catalog(
                    account_id,
                    [
                        entry
                        for entry in catalog_read.catalog
                        if entry.account_id == account_id
                        and self.reader.policy.permits(entry.conversation_id)
                    ],
                    limit=conversation_limit,
                    inventory_epoch=projection_epoch,
                )
                next_cursor_by_account[account_id] = next_cursor
                for entry in chosen:
                    check_operation_budget()
                    conversation_state = self.repository.source_conversation_state(
                        entry.conversation_id
                    )
                    latest = state_frontier(conversation_state)
                    legacy = (
                        conversation_state is not None
                        and not conversation_state["coverage_version"]
                    )
                    floor = (
                        key_from_position(conversation_state["contiguous_floor_position"])
                        if conversation_state is not None
                        else None
                    )
                    history_complete = (
                        bool(conversation_state["history_complete"])
                        if conversation_state is not None
                        else False
                    )
                    projection_stale = bool(
                        conversation_state is None
                        or str(conversation_state["source_inventory_epoch"] or "")
                        != projection_epoch
                    )
                    # On-demand conversations do not collect bodies continuously.
                    # Skip the source body read entirely rather than reading a bounded
                    # tail only to discard it; catalog metadata stays current and any
                    # bounded foreground read re-enters through the source on demand.
                    if not self._residency_decision(entry.conversation_id).collect_bodies:
                        continue
                    try:
                        if (latest is None or projection_stale) and not legacy:
                            page = self.provider.read_recent(
                                entry.source_account_key,
                                entry.source.source_conversation_id,
                                initial_tail,
                                snapshot,
                            )
                            selected = page.messages
                            pending = page.has_more_after
                            floor = min(
                                (item.sort_key for item in selected),
                                key=lambda key: key.as_tuple(),
                                default=None,
                            )
                            latest = None
                            history_complete = not page.has_more_before
                        else:
                            page = self.provider.read_range(
                                entry.source_account_key,
                                entry.source.source_conversation_id,
                                after=latest,
                                before=None,
                                direction="forward",
                                limit=batch_limit + 1,
                                snapshot=snapshot,
                            )
                            selected = tuple(
                                message
                                for message in page.messages
                                if latest is None or message.sort_key.as_tuple() > latest.as_tuple()
                            )[:batch_limit]
                            pending = page.has_more_after or len(page.messages) > batch_limit
                            if latest is None:
                                floor = selected[0].sort_key if selected else None
                                history_complete = True
                    except SightglassError as error:
                        if not _is_conversation_identity_conflict(error):
                            raise
                        # One conversation whose shards disagree about a message identity
                        # must not stop the rotation: mark it degraded, leave it out of
                        # this round, and let it recover when the conflict clears. Nothing
                        # is merged, guessed, or dropped, and the rotation cursor still
                        # advances past it in the admission step below.
                        conflicted_conversation_ids.append(entry.conversation_id)
                        continue
                    plans.append(
                        _SyncConversationPlan(
                            entry=entry,
                            prepared=self._prepare_messages(selected),
                            pending=pending,
                            frontier=latest,
                            floor=floor,
                            history_complete=history_complete,
                        )
                    )

            with self._admission(stack):
                self._persist_catalog(catalog_read, snapshot)
                for conversation_id in conflicted_conversation_ids:
                    self.repository.mark_source_conversation_attention(
                        conversation_id=conversation_id,
                        backfill_state=DEGRADED_BACKFILL_STATE,
                        error_code=CONVERSATION_IDENTITY_CONFLICT_WARNING,
                        observed_at=snapshot.fresh_as_of,
                    )
                for plan in plans:
                    check_operation_budget()
                    conversation_count += 1
                    admitted, pending = self._admit_sync_plan(
                        plan,
                        snapshot,
                        inventory_epoch=projection_epoch,
                    )
                    message_count += admitted
                    pending_conversation_count += int(pending)
                for account_id, _source_account in catalog_read.accounts:
                    account_catalog = [
                        entry
                        for entry in catalog_read.catalog
                        if entry.account_id == account_id
                        and self.reader.policy.permits(entry.conversation_id)
                    ]
                    tail_times = self.repository.source_conversation_tail_times(account_id)
                    catalog_caught_up = all(
                        entry.source.last_message_at_utc is None
                        or not self._residency_decision(entry.conversation_id).collect_bodies
                        or (
                            tail_times.get(entry.conversation_id) is not None
                            and parse_aware_datetime(entry.source.last_message_at_utc)
                            <= parse_aware_datetime(str(tail_times[entry.conversation_id]))
                        )
                        for entry in account_catalog
                    )
                    if catalog_caught_up:
                        # A physical generation is fully admitted only after every
                        # permitted catalog tail has caught up. Recording all shard
                        # generations after a bounded partial slice would make the
                        # next poll no-op and permanently skip another changed chat.
                        self.repository.record_source_shard_states(
                            account_id=account_id,
                            inventory_epoch=snapshot.inventory_digest,
                            generation_by_shard=snapshot.generation_by_shard,
                            observed_at=snapshot.fresh_as_of,
                        )
                    # Promotion is judged against the conversations observed by *this*
                    # exact source read, not a persisted membership view: the catalog state
                    # row still carries the previous ``last_observed_at`` here, and each
                    # live snapshot advances ``fresh_as_of``, so joining freshly written
                    # conversation timestamps to it could resolve to an empty set and
                    # wrongly promote the epoch. ``account_catalog`` is this read's own
                    # policy-permitted membership.
                    current_catalog = {entry.conversation_id for entry in account_catalog}
                    handled = self._catalog_handled_ids(
                        account_id,
                        projection_epoch=projection_epoch,
                    )
                    catalog_epoch = (
                        projection_epoch
                        if current_catalog <= handled
                        else snapshot.inventory_digest
                    )
                    self.repository.record_source_catalog_state(
                        account_id=account_id,
                        inventory_epoch=catalog_epoch,
                        coverage_state=coverage_state,
                        observed_at=snapshot.fresh_as_of,
                        next_cursor_token=next_cursor_by_account.get(account_id),
                    )
        return {
            "schema": "sightglass.source-sync.v1",
            "conversation_count": conversation_count,
            "message_count": message_count,
            "pending_conversation_count": pending_conversation_count,
            "conflict_conversation_count": len(conflicted_conversation_ids),
        }

    def _source_snapshot_already_indexed(
        self,
        accounts: tuple[tuple[str, SourceAccount], ...],
        snapshot: SourceSnapshot,
    ) -> bool:
        """Skip an unchanged native poll only after every permitted chat has a tail."""

        if not self.provider.descriptor.supports_incremental or not accounts:
            return False
        current_generations = dict(snapshot.generation_by_shard)
        projection_epoch = self._projection_inventory_epoch()
        for account_id, _source_account in accounts:
            state = self.repository.source_catalog_state(account_id)
            if (
                state is None
                or str(state["source_inventory_epoch"] or "") != projection_epoch
                or str(state["coverage_state"]) not in {"complete", "partial"}
                or self.repository.source_shard_generations(account_id) != current_generations
            ):
                return False
            current_catalog = self.repository.current_catalog_conversation_ids(account_id)
            permitted = {
                str(row["conversation_id"])
                for row in self.repository.account_conversations(account_id)
                if self.reader.policy.permits(str(row["conversation_id"]))
            }
            handled = self._catalog_handled_ids(
                account_id,
                projection_epoch=projection_epoch,
            )
            if not (permitted & current_catalog) <= handled:
                return False
            for conversation_id in permitted & current_catalog:
                if not self._residency_decision(conversation_id).collect_bodies:
                    continue
                conversation_state = self.repository.source_conversation_state(conversation_id)
                if (
                    conversation_state is not None
                    and str(conversation_state["last_error_code"] or "")
                    == CONVERSATION_IDENTITY_CONFLICT_WARNING
                ):
                    continue
                if (
                    conversation_state is None
                    or not conversation_state["coverage_version"]
                    or not conversation_state["forward_complete"]
                ):
                    return False
        return True

    def _residency_settings(self) -> ResidencySettings:
        return self.residency.settings()

    def _residency_decision(self, conversation_id: str) -> ResidencyDecision:
        return self.residency.resolve_decision(
            conversation_id, authorized=self.reader.policy.permits(conversation_id)
        )

    def _admit_sync_plan(
        self,
        plan: _SyncConversationPlan,
        snapshot: SourceSnapshot,
        *,
        inventory_epoch: str,
    ) -> tuple[int, bool]:
        conversation_id = plan.entry.conversation_id
        context = self._conversation_context(conversation_id)
        admitted = len(self._ingest_prepared_messages(context, plan.prepared, background=True))
        indexed_after, indexed_before = self.repository.observation_bounds(conversation_id)
        selected = tuple(item.source for item in plan.prepared)
        frontier = max(
            (item.sort_key for item in selected),
            key=lambda key: key.as_tuple(),
            default=plan.frontier,
        )
        if selected:
            lower = plan.frontier or min(
                (item.sort_key for item in selected), key=lambda key: key.as_tuple()
            )
            assert frontier is not None
            record_window(
                self.repository.database,
                conversation_id,
                inventory_epoch,
                lower,
                frontier,
                snapshot.fresh_as_of,
            )
        self.repository.record_source_conversation_state(
            conversation_id=conversation_id,
            inventory_epoch=inventory_epoch,
            tail=frontier,
            indexed_before=indexed_before,
            indexed_after=indexed_after,
            backfill_state="complete" if plan.history_complete and not plan.pending else "partial",
            observed_at=snapshot.fresh_as_of,
            coverage_version=1,
            contiguous_floor=plan.floor,
            history_complete=plan.history_complete,
            forward_complete=not plan.pending,
        )
        return admitted, plan.pending

    def _rotate_catalog(
        self,
        account_id: str,
        catalog: list[_CatalogConversation],
        *,
        limit: int,
        inventory_epoch: str,
    ) -> tuple[list[_CatalogConversation], str | None]:
        """Return the next bounded catalog window and the successor rotation position.

        Rotation follows a stable catalog order from the persisted cursor, so every
        conversation is admitted within ``ceil(catalog / limit)`` rounds. Evidently
        changed conversations (never admitted, or reporting unread messages) are
        preferred inside that bound; the head of the rotation is always admitted so a
        standing backlog of unread conversations cannot starve the rest of the catalog.
        """

        ordered = sorted(catalog, key=lambda entry: entry.conversation_id)
        if not ordered:
            return [], None
        start = 0
        state = self.repository.source_catalog_state(account_id)
        if state is not None:
            resume = self._decode_catalog_cursor(state["next_cursor_token"], account_id=account_id)
            if resume is not None:
                start = next(
                    (
                        index
                        for index, entry in enumerate(ordered)
                        if entry.conversation_id == resume
                    ),
                    0,
                )
        rotated = ordered[start:] + ordered[:start]
        indexed = self.repository.indexed_conversation_ids(
            account_id,
            inventory_epoch=inventory_epoch,
        )
        admitted_tail_times = self.repository.source_conversation_tail_times(account_id)

        def changed_or_unseen(entry: _CatalogConversation) -> bool:
            if entry.conversation_id not in indexed or entry.source.unread_count > 0:
                return True
            source_activity = entry.source.last_message_at_utc
            admitted_tail = admitted_tail_times.get(entry.conversation_id)
            return (
                source_activity is not None
                and admitted_tail is not None
                and parse_aware_datetime(source_activity) > parse_aware_datetime(admitted_tail)
            )

        chosen = [rotated[0]]
        budget = limit - 1
        if budget > 0:
            tail = rotated[1:]
            preferred = [entry for entry in tail if changed_or_unseen(entry)][:budget]
            if len(preferred) < budget:
                preferred_ids = {entry.conversation_id for entry in preferred}
                preferred.extend(
                    entry for entry in tail if entry.conversation_id not in preferred_ids
                )
                preferred = preferred[:budget]
            chosen.extend(preferred)
        consumed = {entry.conversation_id for entry in chosen}
        advance = 0
        for entry in rotated:
            if entry.conversation_id not in consumed:
                break
            advance += 1
        successor = ordered[(start + advance) % len(ordered)].conversation_id
        return chosen, self._encode_catalog_cursor(account_id, successor)

    def _encode_catalog_cursor(self, account_id: str, conversation_id: str) -> str:
        return self.token_codec.encode(
            {
                "schema": CATALOG_ROTATION_SCHEMA,
                "account_id": account_id,
                "next": conversation_id,
            }
        )

    def _decode_catalog_cursor(self, token: Any, *, account_id: str) -> str | None:
        """Read the private rotation cursor, restarting rotation when unusable.

        This is internal worker state rather than reader input, so an unreadable or
        out-of-catalog position falls back to the head of the catalog by design; the
        window stays bounded and coverage is still guaranteed from that point.
        """

        if not isinstance(token, str) or not token:
            return None
        try:
            payload = self.token_codec.decode(token)
        except SightglassError:
            return None
        if (
            payload.get("schema") != CATALOG_ROTATION_SCHEMA
            or payload.get("account_id") != account_id
            or not isinstance(payload.get("next"), str)
        ):
            return None
        return str(payload["next"])

    def queue_backfill(
        self,
        *,
        account_id: str | None = None,
        conversation_id: str | None = None,
        after: str | None = None,
        before: str | None = None,
        max_messages: int = 10_000,
    ) -> dict[str, Any]:
        self.reader.require_active()
        if self.storage is not None:
            self.storage.require(background=True)
        if max_messages < 1 or max_messages > 1_000_000:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        after_utc = to_utc_iso(after) if after else None
        before_utc = to_utc_iso(before) if before else None
        if after_utc and before_utc and after_utc >= before_utc:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        created_at = utc_now().isoformat(timespec="microseconds")
        queued: list[str] = []
        with self._source_read() as (stack, snapshot):
            catalog_read = self._read_catalog(snapshot)
            with self._admission(stack):
                mappings = self._persist_catalog(catalog_read, snapshot)
                external_account_id, _source_key = self._account_source_key(mappings, account_id)
                if conversation_id is not None:
                    context = self._conversation_context(conversation_id)
                    if str(context["account_id"]) != external_account_id:
                        raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
                    conversation_ids = (conversation_id,)
                else:
                    conversation_ids = tuple(
                        str(row["conversation_id"])
                        for row in self.repository.account_conversations(external_account_id)
                        if self.reader.policy.permits(str(row["conversation_id"]))
                    )
                for selected_id in conversation_ids:
                    decision = self._residency_decision(selected_id)
                    if decision.mode != "keep":
                        continue
                    if decision.mode == "keep" and not decision.allow_historical_backfill:
                        self.residency.set(selected_id, mode="keep", keep_backfill=True)
                    job_id = opaque_id(
                        "wxbackfill",
                        external_account_id,
                        selected_id,
                        created_at,
                        max_messages,
                    )
                    self.repository.create_backfill_job(
                        job_id=job_id,
                        account_id=external_account_id,
                        conversation_id=selected_id,
                        inventory_epoch=snapshot.inventory_digest,
                        requested_after=after_utc,
                        requested_before=before_utc,
                        max_messages=max_messages,
                        created_at=created_at,
                    )
                    queued.append(job_id)
        return {
            "schema": "sightglass.backfill-queue.v1",
            "queued_job_count": len(queued),
            "job_ids": queued,
            "estimated_growth_bytes": len(queued) * max_messages * 16_384,
            "growth_estimate_kind": "planning_16k_per_message",
        }

    def _pause_backfill_under_pressure(self, expected_bytes: int = 0) -> bool:
        if self.storage is None:
            return False
        try:
            self.storage.require(expected_bytes, background=True)
        except SightglassError as error:
            if error.code != ErrorCode.STORAGE_PRESSURE:
                raise
            if self.repository.next_backfill_job() is not None:
                with self.repository.database.transaction(maintenance=True):
                    self.repository.set_backfills_paused(
                        True, updated_at=utc_now().isoformat(timespec="microseconds")
                    )
            return True
        return False

    def process_backfill_once(self, *, batch_limit: int = 200) -> dict[str, Any]:
        batch_limit = max(1, min(int(batch_limit), 500))
        if self._pause_backfill_under_pressure(batch_limit * 16_384):
            return {
                "schema": "sightglass.backfill-step.v1",
                "state": "paused",
                "reason": "storage_pressure",
                "message_count": 0,
            }
        job = self.repository.next_backfill_job(tuple(
            item for item in self.residency.historical_conversation_ids()
            if self.reader.policy.permits(item)
        ))
        if job is None:
            return {"schema": "sightglass.backfill-step.v1", "state": "idle"}
        decision = self._residency_decision(str(job["conversation_id"]))
        if not decision.allow_historical_backfill:
            return {
                "schema": "sightglass.backfill-step.v1",
                "state": "paused",
                "reason": "residency_selection",
                "message_count": 0,
            }
        job_id = str(job["job_id"])
        processed = int(job["processed_messages"])
        remaining = int(job["max_messages"]) - processed
        if remaining <= 0:
            completed_at = utc_now().isoformat(timespec="microseconds")
            self.repository.update_backfill_job(
                job_id,
                state="completed",
                processed_messages=processed,
                updated_at=completed_at,
            )
            return {
                "schema": "sightglass.backfill-step.v1",
                "state": "completed",
                "message_count": 0,
            }
        selected_limit = min(batch_limit, remaining)
        state = "running"
        message_count = 0
        exhausted = False
        with self._source_read() as (stack, snapshot):
            if str(job["source_inventory_epoch"] or "") != snapshot.inventory_digest:
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
            conversation_id = str(job["conversation_id"])
            target = self._persisted_source_target(conversation_id)
            conversation_state = self.repository.source_conversation_state(conversation_id)
            # Historical backfill must not certify that the current live tail was
            # reprojected under new provider/parser semantics. Preserve the tail's
            # existing epoch; only sync_source_once may advance that repair marker.
            conversation_projection_epoch = (
                str(conversation_state["source_inventory_epoch"] or "")
                if conversation_state is not None
                else snapshot.inventory_digest
            )
            earliest = (
                key_from_position(conversation_state["contiguous_floor_position"])
                if conversation_state is not None and conversation_state["coverage_version"]
                else None
            )
            page = self.provider.read_range(
                target.source_account_key,
                target.source_conversation_id,
                after=None,
                before=earliest,
                direction="backward",
                limit=selected_limit + 1,
                snapshot=snapshot,
                time_after_utc=job["requested_after"],
                time_before_utc=job["requested_before"],
            )
            candidates = tuple(
                message
                for message in page.messages
                if earliest is None or message.sort_key.as_tuple() < earliest.as_tuple()
            )
            selected = candidates[-selected_limit:]
            prepared = self._prepare_messages(selected)
            with self._admission(stack):
                context = self._conversation_context(conversation_id)
                message_ids = self._ingest_prepared_messages(context, prepared, background=True)
                message_count = len(message_ids)
                processed += len(selected)
                exhausted = not page.has_more_before and len(candidates) <= selected_limit
                state = (
                    "completed" if exhausted or processed >= int(job["max_messages"]) else "running"
                )
                observed_at = snapshot.fresh_as_of
                bounds_after, bounds_before = self.repository.observation_bounds(conversation_id)
                self.repository.record_source_conversation_state(
                    conversation_id=conversation_id,
                    inventory_epoch=conversation_projection_epoch,
                    tail=state_frontier(conversation_state)
                    or (selected[-1].sort_key if selected else None),
                    indexed_before=bounds_before,
                    indexed_after=bounds_after,
                    backfill_state="complete" if exhausted else "partial",
                    observed_at=observed_at,
                    coverage_version=1,
                    contiguous_floor=selected[0].sort_key if selected else earliest,
                    history_complete=exhausted
                    and not job["requested_after"]
                    and not job["requested_before"],
                    forward_complete=bool(conversation_state["forward_complete"])
                    if earliest is not None and conversation_state is not None
                    else not page.has_more_after,
                )
                if selected:
                    record_window(
                        self.repository.database,
                        conversation_id,
                        conversation_projection_epoch,
                        selected[0].sort_key,
                        earliest or selected[-1].sort_key,
                        observed_at,
                    )
                self.repository.update_backfill_job(
                    job_id,
                    state=state,
                    processed_messages=processed,
                    updated_at=observed_at,
                )
        return {
            "schema": "sightglass.backfill-step.v1",
            "state": state,
            "message_count": message_count,
            "processed_messages": processed,
        }

    def backfill_status(self) -> dict[str, Any]:
        counts = self.repository.backfill_status_counts()
        return {
            "schema": "sightglass.backfill-status.v1",
            "state_counts": counts,
            "active_job_count": counts.get("queued", 0) + counts.get("running", 0),
            "storage": self.repository.database.storage_status(),
        }

    def set_backfill_paused(self, paused: bool) -> dict[str, Any]:
        if not paused and self.storage is not None:
            self.storage.require(background=True)
        with self.repository.database.transaction(maintenance=True):
            updated = self.repository.set_backfills_paused(
                paused, updated_at=utc_now().isoformat(timespec="microseconds")
            )
        return {
            "schema": "sightglass.backfill-control.v1",
            "paused": paused,
            "updated_job_count": updated,
        }

    def find_conversations(
        self,
        query: str,
        *,
        account_id: str | None = None,
        kinds: tuple[str, ...] = ("direct", "group"),
        recent_only: bool = False,
        limit: int = 20,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        self.reader.require_active()
        bounded = self.reader.bound_limit(limit)
        if self.default_view == "replica":
            return self.replica.find_conversations(
                query, account_id=account_id, kinds=kinds, recent_only=recent_only,
                limit=bounded, cursor=cursor,
            )
        with self._source_read() as (stack, snapshot):
            catalog_read = self._read_catalog(snapshot)
            mappings = catalog_read.mappings
            external_account_id, source_account_key = self._account_source_key(mappings, account_id)
            candidates = self.provider.resolve_conversation(source_account_key, query, snapshot)
            facts = self._catalog_facts(snapshot)
            with self._admission(stack):
                self._persist_catalog(catalog_read, snapshot)
                selected_candidates: list[tuple[list[Any], Any, str]] = []
                for candidate in candidates:
                    if kinds and candidate.conversation.kind not in kinds:
                        continue
                    if recent_only and candidate.conversation.last_message_at_utc is None:
                        continue
                    conversation_id = self.repository.upsert_conversation(
                        external_account_id,
                        candidate.conversation,
                        snapshot.fresh_as_of,
                    )
                    if not self.reader.policy.permits(conversation_id):
                        continue
                    selected_candidates.append(
                        (
                            self._activity_position(
                                candidate.conversation.last_message_at_utc,
                                candidate.conversation.kind,
                                conversation_id,
                            ),
                            candidate,
                            conversation_id,
                        )
                    )
                selected_candidates.sort(key=lambda item: item[0])
                total_matches = len(selected_candidates)
                # The epoch binds everything that decides the filtered candidate
                # set, its order, and the projected candidate fields, so a title,
                # matched alias, kind, or activity change under the same scope fails
                # the continuation instead of silently moving a candidate past the
                # point the caller already returned. Only this digest is signed.
                catalog_epoch = self._scope_digest(
                    {
                        "total": total_matches,
                        "candidates": [
                            {
                                "position": position,
                                "conversation_id": conversation_id,
                                "kind": candidate.conversation.kind,
                                "title": candidate.conversation.title,
                                "matched_value": candidate.matched_value,
                                "matched_kind": candidate.matched_kind,
                            }
                            for position, candidate, conversation_id in selected_candidates
                        ],
                    }
                )
                scope_key = self._scope_digest(
                    {
                        "query": query.casefold(),
                        "kinds": sorted(set(kinds)),
                        "recent_only": bool(recent_only),
                    }
                )
                policy_revision = self._policy_revision()
                if cursor:
                    cursor_payload = self.account_cursors.verify(
                        cursor,
                        kind="catalog",
                        reader_id=self.reader.reader_id,
                        account_id=external_account_id,
                        scope_key=scope_key,
                        policy_revision=policy_revision,
                    )
                    if cursor_payload["snapshot"].get("catalog_epoch") != catalog_epoch:
                        raise SightglassError(ErrorCode.CURSOR_STALE)
                    position = cursor_payload["position"]
                    selected_candidates = [
                        item for item in selected_candidates if item[0] > position
                    ]
                page_candidates = selected_candidates[:bounded]
                projected: list[dict[str, Any]] = []
                for _position, candidate, conversation_id in page_candidates:
                    projected.append(
                        {
                            "conversation_id": conversation_id,
                            "kind": candidate.conversation.kind,
                            "title": candidate.conversation.title,
                            "matched": {
                                "value": candidate.matched_value,
                                "kind": candidate.matched_kind,
                            },
                            "last_message_at": candidate.conversation.last_message_at_utc,
                            "ambiguity": {"requires_selection": False},
                        }
                    )
                ambiguous = total_matches > 1 and bool(query)
                for item in projected:
                    item["ambiguity"]["requires_selection"] = ambiguous
                has_more = len(selected_candidates) > len(page_candidates)
                next_cursor = (
                    self.account_cursors.issue(
                        kind="catalog",
                        reader_id=self.reader.reader_id,
                        account_id=external_account_id,
                        scope_key=scope_key,
                        policy_revision=policy_revision,
                        position=page_candidates[-1][0],
                        snapshot={"catalog_epoch": catalog_epoch},
                    )
                    if has_more and page_candidates
                    else None
                )
                coverage = Coverage(
                    catalog=("complete" if facts.complete else "partial"),
                    conversation="observed_catalog",
                    active_conversations_only=facts.active_conversations_only,
                    notes=("not_found_is_coverage_bounded",) if not projected else (),
                )
                return {
                    "schema": "sightglass.conversation-catalog.v2",
                    "query": query,
                    "account_id": external_account_id,
                    "recent_only": bool(recent_only),
                    "ambiguous": ambiguous,
                    "total_matches": total_matches,
                    "truncated": has_more,
                    "candidates": projected,
                    "page": {
                        "next_cursor": next_cursor,
                        "truncated": has_more,
                    },
                    "coverage": coverage.as_dict(),
                }

    def read_inbox(
        self,
        *,
        account_id: str | None = None,
        after: str | None = None,
        before: str | None = None,
        kinds: tuple[str, ...] = ("direct", "group"),
        unread_only: bool = False,
        include_latest: str = "metadata",
        cursor: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        self.reader.require_active()
        if include_latest not in {"metadata", "text"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if any(kind not in {"direct", "group"} for kind in kinds):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        after_utc = to_utc_iso(after) if after else None
        before_utc = to_utc_iso(before) if before else None
        bounded = self.reader.bound_limit(limit)
        if self.default_view == "replica":
            return self.replica.read_inbox(
                account_id=account_id, after_utc=after_utc, before_utc=before_utc,
                kinds=kinds, unread_only=unread_only, include_latest=include_latest,
                cursor=cursor, bounded=bounded,
            )

        indexed_account = self._native_indexed_inbox_account(account_id)
        if indexed_account is not None:
            external_account_id, catalog_state, projection_epoch, degraded_ids = indexed_account
            internal_account_id = str(catalog_state["account_id"])
            # Both the served and the degraded sets are scoped to the persisted *current*
            # catalog: a historical active row outside the latest complete observation
            # must neither leak back into inbox even if it carries the current semantic
            # epoch, nor inflate ``degraded_conversations``.
            current_catalog = self.repository.current_catalog_conversation_ids(
                internal_account_id
            )
            degraded_ids &= current_catalog
            epoch_conversation_ids = self.repository.indexed_conversation_ids(
                internal_account_id,
                inventory_epoch=projection_epoch,
            )
            epoch_conversation_ids |= (
                self.repository.resident_conversation_ids(internal_account_id, projection_epoch)
                & self.residency.on_demand_conversation_ids(internal_account_id)
            )
            epoch_conversation_ids = (epoch_conversation_ids & current_catalog) - degraded_ids
            with self._status_lock:
                cached_source = copy.deepcopy(
                    (self._cached_status or self._cold_status).get("source", {})
                )
            return self._read_indexed_inbox(
                external_account_id=external_account_id,
                after_utc=after_utc,
                before_utc=before_utc,
                kinds=kinds,
                unread_only=unread_only,
                include_latest=include_latest,
                cursor=cursor,
                bounded=bounded,
                catalog_coverage=str(catalog_state["coverage_state"]),
                active_conversations_only=False,
                catalog_fresh_as_of=str(catalog_state["last_observed_at"]),
                epoch_conversation_ids=epoch_conversation_ids,
                degraded_conversation_ids=degraded_ids,
                live_refresh_available=(
                    cached_source.get("source_state") == "complete"
                ),
                live_warning_codes=tuple(
                    str(value) for value in cached_source.get("warnings", ())
                ),
            )

        with self._source_read() as (stack, snapshot):
            catalog_read = self._read_catalog(snapshot)
            mappings = catalog_read.mappings
            facts = self._catalog_facts(snapshot)
            with self._admission(stack):
                self._persist_catalog(catalog_read, snapshot)
                external_account_id, _source_account_key = self._account_source_key(
                    mappings, account_id
                )
                return self._read_indexed_inbox(
                    external_account_id=external_account_id,
                    after_utc=after_utc,
                    before_utc=before_utc,
                    kinds=kinds,
                    unread_only=unread_only,
                    include_latest=include_latest,
                    cursor=cursor,
                    bounded=bounded,
                    catalog_coverage="complete" if facts.complete else "partial",
                    active_conversations_only=facts.active_conversations_only,
                    catalog_fresh_as_of=snapshot.fresh_as_of,
                )

    def _native_indexed_inbox_account(
        self, requested_account_id: str | None
    ) -> tuple[str, Any, str, set[str]] | None:
        """Select a validated native account whose admitted catalog may feed inbox."""

        descriptor = self.provider.descriptor
        if not (
            descriptor.kind == "macos-wechat"
            and descriptor.source_mode == "live"
            and descriptor.supports_incremental
        ):
            return None
        projection_epoch = self._projection_inventory_epoch()
        projection_pending = False
        mappings: dict[str, str] = {}
        states: dict[str, Any] = {}
        degraded_by_account: dict[str, set[str]] = {}
        for account in self.repository.active_accounts():
            account_id = str(account["account_id"])
            source_account_key = account["source_account_key"]
            state = self.repository.source_catalog_state(account_id)
            current_catalog = self.repository.current_catalog_conversation_ids(account_id)
            permitted = {
                str(row["conversation_id"])
                for row in self.repository.account_conversations(account_id)
                if self.reader.policy.permits(str(row["conversation_id"]))
            }
            handled = self._catalog_handled_ids(
                account_id,
                projection_epoch=projection_epoch,
            )
            readiness_pending = not (permitted & current_catalog) <= handled
            if (
                source_account_key is None
                or state is None
                or str(state["source_inventory_epoch"] or "") != projection_epoch
                or str(state["coverage_state"]) not in {"complete", "partial"}
                or not state["last_observed_at"]
                or readiness_pending
            ):
                if (
                    state is not None
                    and (
                        str(state["source_inventory_epoch"] or "") != projection_epoch
                        or readiness_pending
                    )
                ):
                    projection_pending = True
                continue
            mappings[account_id] = str(source_account_key)
            states[account_id] = state
            degraded_by_account[account_id] = self.repository.degraded_conversation_ids(
                account_id,
                error_code=CONVERSATION_IDENTITY_CONFLICT_WARNING,
            )
        if not mappings:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                retryable=True,
                details={
                    "warning_codes": [
                        "source_projection_refresh_pending"
                        if projection_pending
                        else "source_catalog_not_ready"
                    ]
                },
            )
        external_account_id, _source_account_key = self._account_source_key(
            mappings, requested_account_id
        )
        return (
            external_account_id,
            states[external_account_id],
            projection_epoch,
            degraded_by_account.get(external_account_id, set()),
        )

    def local_inbox_read_ready(self, arguments: dict[str, Any]) -> bool:
        """Whether native inbox can return its index or readiness error locally."""

        account_id = arguments.get("account_id")
        if account_id is not None and not isinstance(account_id, str):
            return False
        try:
            return self._native_indexed_inbox_account(account_id) is not None
        except SightglassError:
            # Native inbox never falls back to source discovery. Its cold/degraded
            # readiness result must not wait for foreground source ownership.
            return True

    def _inbox_epoch(
        self,
        eligible: list[tuple[list[Any], Any]],
        *,
        include_latest: str,
    ) -> str:
        """Digest the exact pre-cursor snapshot one inbox page promises.

        The observation watermark keeps appends stable; this binds the *filtered*
        snapshot the projection returns, so a conversation becoming visible or
        hidden, a title/unread/kind change, or a latest sender/row correction under
        an unchanged watermark fails the continuation instead of silently dropping a
        conversation. Latest text is bound only when the projection returns it, and
        only this digest reaches the signed cursor.
        """

        rows: list[dict[str, Any]] = []
        for position, row in eligible:
            sender_snapshot = json.loads(str(row["latest_sender_snapshot"]))
            entry: dict[str, Any] = {
                "position": position,
                "conversation_id": str(row["conversation_id"]),
                "kind": str(row["kind"]),
                "title": str(row["current_title"]),
                "unread_count": max(0, int(row["unread_count"] or 0)),
                "latest_message_id": str(row["latest_message_id"]),
                "latest_sent_at": str(row["latest_sent_at"]),
                "latest_kind": str(row["latest_kind"]),
                "sender": {
                    "participant_id": row["latest_sender_id"],
                    "label": row["latest_sender_label"],
                    "is_self": bool(row["latest_sender_is_self"]),
                    "shown_as": sender_snapshot.get("shown_as"),
                    "identity_state": row["latest_sender_resolution_state"],
                    "identity_confidence": row["latest_sender_identity_confidence"],
                },
            }
            if include_latest == "text":
                text = row["latest_text"]
                entry["text"] = str(text) if text is not None else None
            rows.append(entry)
        return self._scope_digest({"total": len(eligible), "rows": rows})

    def _read_indexed_inbox(
        self,
        *,
        external_account_id: str,
        after_utc: str | None,
        before_utc: str | None,
        kinds: tuple[str, ...],
        unread_only: bool,
        include_latest: str,
        cursor: str | None,
        bounded: int,
        catalog_coverage: str,
        active_conversations_only: bool,
        catalog_fresh_as_of: str,
        epoch_conversation_ids: set[str] | None = None,
        degraded_conversation_ids: set[str] | None = None,
        live_refresh_available: bool = True,
        live_warning_codes: tuple[str, ...] = (),
        projection_epoch: str | None = None,
    ) -> dict[str, Any]:
        scope_key = self._scope_digest(
            {
                "after": after_utc,
                "before": before_utc,
                "kinds": sorted(set(kinds)),
                "unread_only": bool(unread_only),
                "include_latest": include_latest,
                **({"view": "replica", "projection_epoch": projection_epoch}
                   if projection_epoch is not None else {}),
            }
        )
        policy_revision = self._materialized_cursor_revision()
        position: list[Any] | None = None
        cursor_snapshot: dict[str, Any] | None = None
        if cursor:
            payload = self.account_cursors.verify(
                cursor,
                kind="inbox",
                reader_id=self.reader.reader_id,
                account_id=external_account_id,
                scope_key=scope_key,
                policy_revision=policy_revision,
            )
            cursor_snapshot = payload["snapshot"]
            observation_seq = payload["snapshot"].get("observation_seq")
            if not isinstance(observation_seq, int) or observation_seq < 0:
                raise SightglassError(ErrorCode.CURSOR_INVALID)
            position = payload["position"]
        else:
            observation_seq = self.repository.observation_watermark()
        # The indexed native inbox serves only conversations whose persisted projection
        # carries the current epoch. Explicitly degraded conflicts are excluded (their
        # stale sender projection must not leak) and reported as a bounded aggregate.
        degraded_ids = degraded_conversation_ids or set()
        epoch_indexed_ids = epoch_conversation_ids
        excluded_degraded = len(
            {
                conversation_id
                for conversation_id in degraded_ids
                if self.reader.policy.permits(conversation_id)
                and (epoch_indexed_ids is None or conversation_id not in epoch_indexed_ids)
            }
        )
        eligible: list[tuple[list[Any], Any]] = []
        for row in self.repository.inbox_rows(
            external_account_id, observation_seq=observation_seq, projection_epoch=projection_epoch,
        ):
            conversation_id = str(row["conversation_id"])
            if not self.reader.policy.permits(conversation_id):
                continue
            if epoch_indexed_ids is not None and conversation_id not in epoch_indexed_ids:
                continue
            if kinds and str(row["kind"]) not in kinds:
                continue
            sent_at = str(row["latest_sent_at"])
            if after_utc is not None and sent_at < after_utc:
                continue
            if before_utc is not None and sent_at >= before_utc:
                continue
            if unread_only and int(row["unread_count"] or 0) < 1:
                continue
            row_position = self._activity_position(sent_at, str(row["kind"]), conversation_id)
            eligible.append((row_position, row))
        eligible.sort(key=lambda item: item[0])
        inbox_epoch = self._inbox_epoch(eligible, include_latest=include_latest)
        if cursor_snapshot is not None and cursor_snapshot.get("inbox_epoch") != inbox_epoch:
            raise SightglassError(ErrorCode.CURSOR_STALE)
        indexed_conversations = len(
            {str(row["conversation_id"]) for _position, row in eligible}
        )
        selected = [
            item for item in eligible if position is None or item[0] > position
        ]
        page_rows = selected[:bounded]
        items: list[dict[str, Any]] = []
        for _row_position, row in page_rows:
            sender_snapshot = json.loads(str(row["latest_sender_snapshot"]))
            latest = {
                "message_id": str(row["latest_message_id"]),
                "sender": {
                    "participant_id": row["latest_sender_id"],
                    "label": row["latest_sender_label"],
                    "is_self": bool(row["latest_sender_is_self"]),
                    "shown_as": sender_snapshot.get("shown_as"),
                    "identity_state": row["latest_sender_resolution_state"],
                    "identity_confidence": row["latest_sender_identity_confidence"],
                },
                "kind": str(row["latest_kind"]),
            }
            if include_latest == "text":
                text = row["latest_text"]
                latest["text"] = (
                    str(text)[: self.reader.policy.max_compact_body_chars_per_message]
                    if text is not None
                    else None
                )
            items.append(
                {
                    "conversation_id": str(row["conversation_id"]),
                    "kind": str(row["kind"]),
                    "title": str(row["current_title"])[:512],
                    "last_message_at": str(row["latest_sent_at"]),
                    "unread_count": max(0, int(row["unread_count"] or 0)),
                    "latest": latest,
                }
            )
        has_more = len(selected) > len(page_rows)
        next_cursor = (
            self.account_cursors.issue(
                kind="inbox",
                reader_id=self.reader.reader_id,
                account_id=external_account_id,
                scope_key=scope_key,
                policy_revision=policy_revision,
                position=page_rows[-1][0],
                snapshot={
                    "observation_seq": observation_seq,
                    "inbox_epoch": inbox_epoch,
                },
            )
            if has_more and page_rows
            else None
        )
        return {
            "schema": "sightglass.inbox-page.v1",
            "account_id": external_account_id,
            "include_latest": include_latest,
            "items": items,
            "page": {
                "next_cursor": next_cursor,
                "truncated": has_more,
                "snapshot_observation_seq": observation_seq,
            },
            "coverage": {
                "catalog": catalog_coverage,
                "catalog_fresh_as_of": catalog_fresh_as_of,
                "active_conversations_only": active_conversations_only,
                "indexed_conversations": indexed_conversations,
                "message_scope": "resident",
                "degraded_conversations": excluded_degraded,
            },
            "source_receipt": {
                "complete": catalog_coverage == "complete",
                "fresh_as_of": catalog_fresh_as_of,
                "warnings": list(
                    dict.fromkeys(
                        (
                            "materialized_projection_not_live",
                            *(live_warning_codes if not live_refresh_available else ()),
                        )
                    )
                ),
                "served_from": "window_db",
                "freshness": {
                    "state": "bounded_stale",
                    "live_refresh_confirmed": False,
                    "live_refresh_available": live_refresh_available,
                    "observation_watermark": observation_seq,
                },
            },
        }

    def _reader_timezone(self, account: Any) -> str:
        return self.reader.timezone or str(account["reader_timezone"])

    def _conversation_context(self, conversation_id: str) -> Any:
        row = self.repository.conversation_context(conversation_id)
        if row is None:
            raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        self.reader.authorize(conversation_id)
        return row

    def _read_participants(
        self, target: _SourceTarget, snapshot: SourceSnapshot
    ) -> list[SourceParticipant]:
        return self.provider.list_participants(
            target.source_account_key,
            target.source_conversation_id,
            snapshot,
        )

    def _read_message_participants(
        self, target: _SourceTarget, snapshot: SourceSnapshot
    ) -> list[SourceParticipant]:
        if self.provider.descriptor.message_sender_evidence_complete:
            return []
        return self._read_participants(target, snapshot)

    def _index_participants(
        self,
        context: Any,
        participants: list[SourceParticipant],
        snapshot: SourceSnapshot,
    ) -> list[tuple[SourceParticipant, str, str]]:
        indexed = []
        for participant in participants:
            participant_id, membership_id = self.repository.index_participant(
                str(context["account_id"]),
                str(context["conversation_id"]),
                participant,
                snapshot.fresh_as_of,
            )
            indexed.append((participant, participant_id, membership_id))
        return indexed

    def _participant_coverage(
        self,
        context: Any,
        source_participants: list[tuple[SourceParticipant, str, str]],
        facts: _CatalogFacts,
    ) -> Coverage:
        observed_after, observed_before = self.repository.observation_bounds(
            str(context["conversation_id"])
        )
        source_times = sorted(
            participant.last_spoke_at_utc
            for participant, _participant_id, _membership_id in source_participants
            if participant.last_spoke_at_utc
        )
        if source_times:
            observed_after = observed_after or source_times[0]
            observed_before = observed_before or source_times[-1]
        roster_complete = bool(context["roster_complete"])
        return Coverage(
            catalog="complete" if facts.complete else "partial",
            conversation="observed_catalog",
            roster="complete" if roster_complete else "partial",
            observed_time_after=observed_after,
            observed_time_before=observed_before,
            active_conversations_only=facts.active_conversations_only,
            notes=(
                ("not_found_is_coverage_bounded", "includes_message_sender_observations")
                if not roster_complete
                else ("includes_message_sender_observations",)
            ),
        )

    def find_participants(
        self,
        conversation_id: str,
        query: str,
        *,
        active_after: str | None = None,
        detail_level: str = "labels",
        limit: int = 20,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if detail_level not in {"compact", "labels", "debug"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if detail_level == "debug":
            self.reader.require_identity_debug()
        if active_after is not None:
            active_after = to_utc_iso(active_after)
        bounded = self.reader.bound_limit(limit)
        if self.default_view == "replica":
            with self.repository.database.read_snapshot():
                context = self._conversation_context(conversation_id)
                return self._participant_page(
                    context=context, indexed=[], facts=_CatalogFacts(False, False),
                    conversation_id=conversation_id, query=query, active_after=active_after,
                    detail_level=detail_level, bounded=bounded, cursor=cursor, view="replica",
                )
        with self._source_read() as (stack, snapshot):
            # A live provider can resolve an already-admitted conversation from its
            # persisted row instead of rebuilding the whole account catalog, exactly
            # as the ordinary message read does. The source roster is still read for
            # participant discovery, so labels remain current-source evidence rather
            # than stale durable rows.
            live_target_fast_path = (
                self.provider.descriptor.source_mode == "live"
                and self.repository.conversation_context(conversation_id) is not None
            )
            if live_target_fast_path:
                catalog_read = _CatalogRead((), ())
                facts = None
            else:
                catalog_read = self._read_catalog(snapshot)
                facts = self._catalog_facts(snapshot)
            target = self._source_target(catalog_read, conversation_id)
            if facts is None:
                facts = self._persisted_catalog_facts(target, snapshot)
            participants = self._read_participants(target, snapshot)
            with self._admission(stack):
                if not live_target_fast_path:
                    self._persist_catalog(catalog_read, snapshot)
                context = self._conversation_context(conversation_id)
                indexed = self._index_participants(context, participants, snapshot)
                return self._participant_page(
                    context=context, indexed=indexed, facts=facts,
                    conversation_id=conversation_id, query=query, active_after=active_after,
                    detail_level=detail_level, bounded=bounded, cursor=cursor,
                )

    def _participant_page(
        self, *, context: Any, indexed: list[tuple[SourceParticipant, str, str]],
        facts: _CatalogFacts, conversation_id: str, query: str, active_after: str | None,
        detail_level: str, bounded: int, cursor: str | None, view: str = "auto",
    ) -> dict[str, Any]:
        candidates = self.repository.participant_candidates(conversation_id, query)
        if active_after is not None:
            candidates = [
                item
                for item in candidates
                if item["last_spoke_at"] and item["last_spoke_at"] >= active_after
            ]
        total_matches = len(candidates)
        ambiguous = total_matches > 1
        participant_epoch = self._scope_digest({"candidates": candidates})
        scope_key = self._scope_digest(
            {
                "conversation_id": conversation_id,
                "query": query.casefold(),
                "active_after": active_after,
                "detail_level": detail_level,
                **({"view": view} if view != "auto" else {}),
            }
        )
        policy_revision = (self._materialized_cursor_revision() if view == "replica"
                           else self._policy_revision())
        account_id = str(context["account_id"])
        positioned = [
            ([str(item["label"]), str(item["participant_id"])], item)
            for item in candidates
        ]
        if cursor:
            cursor_payload = self.account_cursors.verify(
                cursor,
                kind="participants",
                reader_id=self.reader.reader_id,
                account_id=account_id,
                scope_key=scope_key,
                policy_revision=policy_revision,
            )
            if cursor_payload["snapshot"].get("participant_epoch") != participant_epoch:
                raise SightglassError(ErrorCode.CURSOR_STALE)
            position = cursor_payload["position"]
            positioned = [item for item in positioned if item[0] > position]
        page_candidates = positioned[:bounded]
        projected = [dict(item) for _position, item in page_candidates]
        if detail_level == "compact":
            projected = [
                {key: value for key, value in item.items() if key != "labels"}
                for item in projected
            ]
        elif detail_level == "debug":
            for item in projected:
                source_keys = self.repository.participant_key_details(
                    item["participant_id"]
                )
                item["source_keys"] = source_keys
                item["source_key_kinds"] = [
                    source_key["kind"] for source_key in source_keys
                ]
        has_more = len(positioned) > len(page_candidates)
        next_cursor = (
            self.account_cursors.issue(
                kind="participants",
                reader_id=self.reader.reader_id,
                account_id=account_id,
                scope_key=scope_key,
                policy_revision=policy_revision,
                position=page_candidates[-1][0],
                snapshot={"participant_epoch": participant_epoch},
            )
            if has_more and page_candidates
            else None
        )
        return {
            "schema": "sightglass.participant-candidates.v1",
            "conversation_id": conversation_id,
            "query": query,
            "ambiguous": ambiguous,
            "total_matches": total_matches,
            "truncated": has_more,
            "candidates": projected,
            "page": {
                "next_cursor": next_cursor,
                "truncated": has_more,
            },
            "coverage": self._participant_coverage(context, indexed, facts).as_dict(),
            **({"source_receipt": {"served_from": "window_db", "view": "replica",
                                   "complete": False,
                                   "freshness": {"state": "bounded_stale",
                                                 "live_refresh_confirmed": False}}}
               if view == "replica" else {}),
        }
    @staticmethod
    def _source_participant_for_message(message: SourceMessage) -> SourceParticipant | None:
        if (
            message.wechat_type in {10000, 10002}
            and not message.sender_keys
            and not message.sender_labels
        ):
            return None
        if any(key.principal_eligible for key in message.sender_keys):
            state = "stable"
            confidence = "exact"
        elif message.sender_keys:
            state = "conversation_local"
            confidence = "strong"
        else:
            state = "alias_only"
            confidence = "unknown"
        return SourceParticipant(
            source_conversation_id=message.source_conversation_id,
            identity_keys=message.sender_keys,
            labels=message.sender_labels,
            is_self=message.is_outgoing,
            resolution_state=state,
            identity_confidence=confidence,
            source_membership_id=(
                f"message:{message.source_message_id}" if not message.sender_keys else None
            ),
            last_spoke_at_utc=message.sent_at_utc,
        )

    def _prepare_messages(
        self, messages: tuple[SourceMessage, ...]
    ) -> tuple[_PreparedMessage, ...]:
        prepared: list[_PreparedMessage] = []
        for message in messages:
            check_operation_budget()
            prepared.append(
                _PreparedMessage(
                    source=message,
                    participant=self._source_participant_for_message(message),
                    parsed=parse_message(message),
                )
            )
        return tuple(prepared)

    def _ingest_prepared_messages(
        self, context: Any, messages: tuple[_PreparedMessage, ...], *, background: bool = False
    ) -> tuple[str, ...]:
        conversation_id = str(context["conversation_id"])
        decision = self._residency_decision(conversation_id)
        if background:
            if not decision.collect_bodies:
                return ()
            if decision.mode == "recent":
                from datetime import timedelta
                cutoff = datetime.now(UTC) - timedelta(days=decision.recent_window_days or 30)
                messages = tuple(item for item in messages
                                 if datetime.fromisoformat(item.source.sent_at_utc) >= cutoff)
        self.repository.database.reserve_growth(sum(
            16_384 + 4 * len(item.source.raw_content.encode("utf-8")) for item in messages
        ))
        message_ids: list[str] = []
        for item in messages:
            check_operation_budget()
            message = item.source
            identity = opaque_id("wxmsg", str(context["account_id"]), message.source_message_id)
            exists = self.repository.message_position_row(identity) is not None
            owner = self.residency.admission_owner(
                identity, exists=exists, decision=decision, sent_at=message.sent_at_utc
            )
            participant = item.participant
            participant_id: str | None = None
            membership_id: str | None = None
            if participant is not None:
                participant_id, membership_id = self.repository.index_participant(
                    str(context["account_id"]),
                    str(context["conversation_id"]),
                    participant,
                    message.observed_at_utc,
                )
            message_ids.append(
                self.repository.upsert_message(
                    str(context["account_id"]),
                    str(context["conversation_id"]),
                    participant_id,
                    membership_id,
                    message,
                    item.parsed,
                    projection_epoch=self._projection_inventory_epoch(),
                )
            )
            self.residency.record_admission(
                message_ids[-1],
                conversation_id=conversation_id,
                owner=owner,
                decision=decision,
                sent_at=message.sent_at_utc,
            )
        if message_ids:
            self.residency.enforce_caps(decision, protect=() if background else tuple(message_ids))
            if not background and decision.mode != "keep":
                self.residency.record_foreground_lease(
                    conversation_id=conversation_id,
                    scope_key="admission:" + self.reader.reader_id,
                    message_ids=tuple(message_ids),
                    projection_epoch=self._projection_inventory_epoch(),
                    settings=self._residency_settings(),
                )
        return tuple(message_ids)

    def _ingest_messages(
        self, context: Any, messages: tuple[SourceMessage, ...]
    ) -> tuple[str, ...]:
        return self._ingest_prepared_messages(context, self._prepare_messages(messages))

    def _coverage(self, context: Any, facts: _CatalogFacts, *, complete: bool = True) -> Coverage:
        observed_after, observed_before = self.repository.observation_bounds(
            str(context["conversation_id"])
        )
        return Coverage(
            catalog="complete" if facts.complete else "partial",
            conversation="complete" if complete else "partial",
            roster="complete" if bool(context["roster_complete"]) else "partial",
            observed_time_after=observed_after,
            observed_time_before=observed_before,
            active_conversations_only=facts.active_conversations_only,
        )

    def _page_response(
        self,
        *,
        projection: str,
        mode: str,
        context: Any,
        rows: list[Any],
        snapshot: SourceSnapshot,
        facts: _CatalogFacts,
        has_more_before: bool,
        has_more_after: bool,
        participant_ids: tuple[str, ...],
        speaker_view: str | None,
        include_resources: str,
        system_policy: str,
        focus_message_ids: frozenset[str] = frozenset(),
        context_only_ids: frozenset[str] = frozenset(),
        late_arrival_ids: frozenset[str] = frozenset(),
        next_cursor: str | None = None,
        delivery_id: str | None = None,
        truncated: bool = False,
        hidden_system_count: int = 0,
        projected_detail: list[dict[str, Any]] | None = None,
        compact_prepared: list[CompactPreparedRow] | None = None,
        message_rows_complete: bool = True,
        voice_reserve_chars: int = 0,
        source_receipt_override: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if source_receipt_override is None:
            coverage = self._coverage(context, facts)
            receipt = SourceReceipt(
                complete=(
                    coverage.catalog == "complete" and coverage.conversation == "complete"
                ),
                fresh_as_of=snapshot.fresh_as_of,
                inventory_digest=snapshot.inventory_digest,
                generation_set_digest=snapshot.generation_set_digest,
                returned_count=len(rows),
                hidden_system_count=hidden_system_count,
                coverage=coverage,
            ).as_dict()
        else:
            receipt = copy.deepcopy(source_receipt_override)
            receipt["returned_count"] = len(rows)
            receipt["hidden_system_count"] = hidden_system_count
        if projection == "detail":
            projected = projected_detail or self.detail_projector.project(
                rows,
                timezone_name=self._reader_timezone(context),
                include_resources=include_resources == "metadata",
                focus_ids=participant_ids,
                context_only_ids=context_only_ids,
                late_arrival_ids=late_arrival_ids,
            )
            return {
                "schema": "sightglass.message-page.v1",
                "projection": "detail",
                "mode": mode,
                "conversation": {
                    "conversation_id": str(context["conversation_id"]),
                    "title": str(context["current_title"]),
                    "kind": str(context["kind"]),
                },
                "messages": projected,
                "focus": {
                    "participant_ids": list(participant_ids),
                    "speaker_view": speaker_view,
                    "matched_message_count": (
                        sum(item["retrieval"]["focus_match"] for item in projected)
                        if participant_ids
                        else 0
                    ),
                    "context_message_count": sum(
                        item["retrieval"]["context_only"] for item in projected
                    ),
                },
                "page": {
                    "has_more_before": has_more_before,
                    "has_more_after": has_more_after,
                    "next_cursor": next_cursor,
                    "delivery_id": delivery_id,
                    "replayed": False,
                    "truncated": truncated,
                },
                "source_receipt": receipt,
            }
        if projection != "compact":
            raise SightglassError(ErrorCode.QUERY_INVALID)
        prepared = compact_prepared
        if prepared is None:
            prepared = self.compact_projector.prepare(
                rows,
                timezone_name=self._reader_timezone(context),
                include_resource_indicators=include_resources == "indicator",
                focus_message_ids=focus_message_ids,
                context_only_ids=context_only_ids,
                late_arrival_ids=late_arrival_ids,
            )
        compact = self.compact_projector.render(prepared)
        page = {
            "schema": "sightglass.message-batch.v1",
            "projection": "compact",
            "mode": mode,
            "conversation": {
                "id": str(context["conversation_id"]),
                "title": str(context["current_title"]),
                "kind": str(context["kind"]),
            },
            "timezone": self._reader_timezone(context),
            **compact,
            "focus": {
                "participant_ids": list(participant_ids),
                "speaker_view": speaker_view,
                "matched_message_count": (len(focus_message_ids) if participant_ids else 0),
                "context_message_count": len(context_only_ids),
            },
            "page": {
                "has_more_before": has_more_before,
                "has_more_after": has_more_after,
                "next_cursor": next_cursor,
                "delivery_id": delivery_id,
                "replayed": False,
                "message_rows_complete": message_rows_complete,
            },
            "source_receipt": receipt,
        }
        CompactBodyBudgetAllocator(
            max_payload_chars=self.reader.policy.max_compact_payload_chars,
            max_body_chars=self.reader.policy.max_compact_body_chars_per_message,
            reserve_chars=voice_reserve_chars,
        ).apply(page)
        return page

    @staticmethod
    def _fit_compact_page(
        count: int,
        *,
        direction: str,
        build: Callable[[slice, bool], dict[str, Any]],
    ) -> tuple[slice, dict[str, Any]]:
        full_slice = slice(0, count)
        try:
            return full_slice, build(full_slice, True)
        except SightglassError as exc:
            if (
                exc.code != ErrorCode.OUTPUT_BUDGET_EXCEEDED
                or exc.details.get("reason") != "fixed_envelope"
                or count <= 1
            ):
                raise

        low = 1
        high = count - 1
        best: tuple[slice, dict[str, Any]] | None = None
        while low <= high:
            check_operation_budget()
            size = (low + high) // 2
            selected = slice(0, size) if direction == "forward" else slice(count - size, count)
            try:
                page = build(selected, False)
            except SightglassError as exc:
                if (
                    exc.code != ErrorCode.OUTPUT_BUDGET_EXCEEDED
                    or exc.details.get("reason") != "fixed_envelope"
                ):
                    raise
                high = size - 1
                continue
            best = selected, page
            low = size + 1
        if best is None:
            raise SightglassError(
                ErrorCode.OUTPUT_BUDGET_EXCEEDED,
                details={
                    "reason": "fixed_envelope",
                    "minimum_rows": 1,
                },
            )
        return best

    @staticmethod
    def _apply_system_policy(rows: list[Any], system_policy: str) -> tuple[list[Any], int]:
        if system_policy not in {"include", "omit"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if system_policy == "include":
            return rows, 0
        hidden = sum(row["kind"] in {"system", "recalled"} for row in rows)
        return [row for row in rows if row["kind"] not in {"system", "recalled"}], hidden

    def _prepare_detail_rows(
        self,
        rows: list[Any],
        *,
        context: Any,
        direction: str,
        include_resources: str,
        participant_ids: tuple[str, ...],
        context_only_ids: frozenset[str],
        late_arrival_ids: frozenset[str] = frozenset(),
        voice_reserve_chars: int = 0,
    ) -> tuple[list[Any], list[dict[str, Any]], bool]:
        projected = self.detail_projector.project(
            rows,
            timezone_name=self._reader_timezone(context),
            include_resources=include_resources == "metadata",
            focus_ids=participant_ids,
            context_only_ids=context_only_ids,
            late_arrival_ids=late_arrival_ids,
        )
        return trim_detail_projection(
            rows,
            projected,
            direction=direction,
            max_payload_chars=max(
                1,
                min(
                    self.reader.policy.max_detail_payload_chars,
                    self.reader.policy.max_text_chars_per_call,
                )
                - max(0, voice_reserve_chars),
            ),
        )

    @staticmethod
    def _query_parts(query: str) -> tuple[str, ...]:
        if query.count('"') % 2:
            raise SightglassError(
                ErrorCode.QUERY_INVALID,
                details={"reason": "quoted search phrase is not closed"},
            )
        return tuple(
            (quoted or term).casefold()
            for quoted, term in re.findall(r'"([^"]+)"|(\S+)', query, flags=re.DOTALL)
            if quoted or term
        )

    @classmethod
    def _text_matches(cls, text: str | None, query: str | None) -> bool:
        if not query:
            return True
        if text is None:
            return False
        value = text.casefold()
        return all(part in value for part in cls._query_parts(query))

    @staticmethod
    def _matching_search_fields(parsed: Any, query_parts: tuple[str, ...]) -> tuple[str, ...]:
        fields = message_search_fields(parsed)
        if not query_parts:
            return ()
        document = "\n".join(fields.values()).casefold()
        if not all(part in document for part in query_parts):
            return ()
        return tuple(
            name
            for name, value in fields.items()
            if any(part in value.casefold() for part in query_parts)
        )

    def _search_source_receipt(
        self,
        *,
        snapshot: SourceSnapshot,
        facts: _CatalogFacts,
        account_id: str,
        conversation_ids: tuple[str, ...],
        all_authorized_conversations: bool,
        after_utc: str | None,
        before_utc: str | None,
        returned_count: int,
        scan_budget_exhausted: bool = False,
        candidates_scanned: int = 0,
        prepared_full_conversation_ids: frozenset[str] = frozenset(),
    ) -> dict[str, Any]:
        states = [
            self.repository.source_conversation_state(conversation_id)
            for conversation_id in conversation_ids
        ]
        indexed = []
        for conversation_id, state in zip(conversation_ids, states, strict=True):
            if state is not None:
                bounds = state["indexed_after"], state["indexed_before"]
            else:
                # Historical windows do not certify a catalog tail or create sync
                # state. Existing bounded timeline seeks still show their index coverage.
                bounds = self.repository.materialized_observation_bounds(
                    conversation_id, projection_epoch=self._projection_inventory_epoch(),
                    observation_watermark=self.repository.observation_watermark(),
                )
            if bounds[0] is not None or bounds[1] is not None:
                indexed.append({"indexed_after": bounds[0], "indexed_before": bounds[1]})
        generations_match = self.repository.source_shard_generations(account_id) == dict(
            snapshot.generation_by_shard
        )
        history_complete_count = sum(
            state is not None
            and bool(state["coverage_version"])
            and bool(state["history_complete"])
            and bool(state["forward_complete"])
            and str(state["backfill_state"]) == "complete"
            and str(state["source_inventory_epoch"] or "") == self._projection_inventory_epoch()
            and (generations_match or conversation_id in prepared_full_conversation_ids)
            for conversation_id, state in zip(conversation_ids, states, strict=True)
        )
        history_complete = history_complete_count == len(conversation_ids)
        catalog_complete = facts.complete
        shard_counts = self.repository.source_shard_state_counts(account_id)
        unavailable_shards = sum(
            count for state, count in shard_counts.items() if state != "available"
        )
        warnings: list[str] = []
        if not catalog_complete:
            warnings.append("catalog_partial")
        if not history_complete:
            warnings.append("history_not_fully_indexed")
        if unavailable_shards:
            warnings.append("source_shards_unavailable")
        if scan_budget_exhausted:
            warnings.append("search_scan_budget_exhausted")
        complete = catalog_complete and history_complete and unavailable_shards == 0
        indexed_after_values = [
            str(state["indexed_after"]) for state in indexed if state["indexed_after"] is not None
        ]
        indexed_before_values = [
            str(state["indexed_before"]) for state in indexed if state["indexed_before"] is not None
        ]
        return {
            "complete": complete,
            "fresh_as_of": snapshot.fresh_as_of,
            "inventory_digest": snapshot.inventory_digest,
            "generation_set_digest": snapshot.generation_set_digest,
            "returned_count": returned_count,
            "coverage": {
                "catalog": "complete" if catalog_complete else "partial",
                "conversation": (
                    "complete" if history_complete else ("indexed" if indexed else "not_indexed")
                ),
                "time_range": "complete" if history_complete else "indexed",
                "authorized_conversations": len(conversation_ids),
            },
            "search": {
                "index": "window_db.search_text.v2",
                "canonical_validated": True,
                "time_after": after_utc,
                "time_before": before_utc,
                "all_authorized_conversations": (all_authorized_conversations and catalog_complete),
                "indexed_conversation_count": len(indexed),
                "history_complete_conversation_count": history_complete_count,
                "indexed_source_generations_match": generations_match,
                "oldest_indexed_at": (min(indexed_after_values) if indexed_after_values else None),
                "newest_indexed_at": (
                    max(indexed_before_values) if indexed_before_values else None
                ),
                "unindexed_source_shards": unavailable_shards,
                "scan": {
                    "budget_exhausted": scan_budget_exhausted,
                    "candidates_scanned": candidates_scanned,
                    "candidate_budget": SEARCH_SCAN_CANDIDATE_BUDGET,
                },
            },
            "warnings": warnings,
        }

    def _record_source_windows(
        self, conversation_id: str, snapshot: SourceSnapshot,
        windows: list[tuple[SourceSortKey, SourceSortKey]],
    ) -> None:
        for lower, upper in windows:
            record_window(
                self.repository.database, conversation_id, self._projection_inventory_epoch(),
                lower, upper, snapshot.fresh_as_of,
            )

    def _record_recent_source_state(
        self, conversation_id: str, snapshot: SourceSnapshot,
        page: SourceMessagePage, sources: tuple[SourceMessage, ...],
    ) -> None:
        lower = min(sources, key=lambda item: item.sort_key.as_tuple()).sort_key
        upper = max(sources, key=lambda item: item.sort_key.as_tuple()).sort_key
        epoch = self._projection_inventory_epoch()
        previous = self.repository.source_conversation_state(conversation_id)
        frontier = (
            state_frontier(previous)
            if previous is not None
            and str(previous["source_inventory_epoch"] or "") == epoch
            else None
        )
        # An existing unverified legacy state is repaired by bounded forward
        # traversal. A disconnected recent window never advances its frontier.
        can_seed = (
            previous is None or str(previous["source_inventory_epoch"] or "") != epoch
        )
        if can_seed:
            version = 1
            forward_complete = not page.has_more_after
            history_complete = not page.has_more_before
        else:
            assert previous is not None
            version = int(previous["coverage_version"])
            history_complete = bool(previous["history_complete"])
            forward_complete = bool(
                frontier
                and upper.as_tuple() <= frontier.as_tuple()
                and previous["forward_complete"]
            )
        seed_frontier = upper if can_seed else frontier
        indexed_after, indexed_before = self.repository.materialized_observation_bounds(
            conversation_id, projection_epoch=epoch,
            observation_watermark=self.repository.observation_watermark(),
        )
        self.repository.record_source_conversation_state(
            conversation_id=conversation_id,
            inventory_epoch=epoch,
            tail=seed_frontier,
            indexed_before=indexed_before,
            indexed_after=indexed_after,
            backfill_state="complete"
            if version and history_complete and forward_complete
            else "partial",
            observed_at=snapshot.fresh_as_of,
            coverage_version=version,
            contiguous_floor=lower if can_seed else None,
            history_complete=history_complete,
            forward_complete=forward_complete,
        )

    def _read_hydrate_page(
        self, target: _SourceTarget, snapshot: SourceSnapshot
    ) -> tuple[SourceMessage, ...]:
        page = self.provider.read_range(
            target.source_account_key,
            target.source_conversation_id,
            after=None,
            before=None,
            direction="forward",
            limit=10_001,
            snapshot=snapshot,
        )
        if page.has_more_after or len(page.messages) > 10_000:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                details={"warning_codes": ["m2_bounded_hydrate_limit_reached"]},
            )
        return page.messages

    def _anchor_target(
        self,
        anchor: str,
        requested_conversation_id: str | None,
        catalog_read: _CatalogRead,
        snapshot: SourceSnapshot,
    ) -> tuple[_SourceTarget, Any, SourceMessage]:
        row = self._local_anchor_row(anchor, requested_conversation_id)
        target = self._source_target(catalog_read, str(row["conversation_id"]))
        source = self.provider.get_message(
            target.source_account_key, str(row["source_message_id"]), snapshot
        )
        if source is None or source.sort_key.as_tuple() != source_sort_key(row).as_tuple():
            raise SightglassError(ErrorCode.CURSOR_STALE)
        return target, row, source

    def _timeline_boundary(
        self,
        *,
        cursor: str,
        context: Any,
        mode: str,
        direction: str,
        scope_kind: str,
        scope_key: str,
        snapshot: SourceSnapshot,
        view: str = "auto",
    ) -> Any:
        payload = self.timeline_cursors.verify(
            cursor,
            reader_id=self.reader.reader_id,
            account_id=str(context["account_id"]),
            conversation_id=str(context["conversation_id"]),
            mode=mode,
            direction=direction,
            scope_kind=scope_kind,
            scope_key=scope_key,
            view=view,
        )
        source_binding = payload["source"]
        if (
            source_binding.get("projection_epoch") != self._projection_inventory_epoch()
            or (
                snapshot.scope is None
                and (
                    source_binding["inventory_digest"] != snapshot.inventory_digest
                    or source_binding["generation_set_digest"]
                    != snapshot.generation_set_digest
                )
            )
        ):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        row = self.repository.message_position_row(payload["position"]["message_id"])
        if row is None:
            raise SightglassError(ErrorCode.CURSOR_STALE)
        expected_sort = [str(row["sort_primary"]), int(row["sort_seq"]), int(row["sort_tie"])]
        if (
            str(row["account_id"]) != str(context["account_id"])
            or str(row["conversation_id"]) != str(context["conversation_id"])
            or payload["position"]["sort"] != expected_sort
        ):
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        current = self.provider.get_message(
            str(context["source_account_key"]), str(row["source_message_id"]), snapshot
        )
        if (
            current is None
            or current.sort_key.as_tuple() != source_sort_key(row).as_tuple()
        ):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        if snapshot.scope is not None:
            dependency_digest = self._dependency_generation_digest(snapshot)
            if (
                dependency_digest is None
                or source_binding.get("dependency_generation_digest") != dependency_digest
            ):
                raise SightglassError(ErrorCode.CURSOR_STALE)
        return row

    @staticmethod
    def _dependency_generation_digest(snapshot: SourceSnapshot) -> str | None:
        if not snapshot.dependency_generation_by_shard:
            return None
        encoded = json.dumps(
            sorted(snapshot.dependency_generation_by_shard.items()),
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _next_cursor(
        self,
        *,
        rows: list[Any],
        context: Any,
        mode: str,
        direction: str,
        scope_kind: str,
        scope_key: str,
        snapshot: SourceSnapshot,
        has_more: bool,
        view: str = "auto",
    ) -> str | None:
        if not rows or not has_more:
            return None
        boundary = rows[0] if direction == "backward" else rows[-1]
        return self.timeline_cursors.issue(
            reader_id=self.reader.reader_id,
            account_id=str(context["account_id"]),
            conversation_id=str(context["conversation_id"]),
            mode=mode,
            direction=direction,
            scope_kind=scope_kind,
            scope_key=scope_key,
            row=boundary,
            inventory_digest=snapshot.inventory_digest,
            generation_set_digest=snapshot.generation_set_digest,
            dependency_generation_digest=self._dependency_generation_digest(snapshot),
            projection_epoch=self._projection_inventory_epoch(),
            view=view,
        )

    def _read_updates(self, **arguments: Any) -> dict[str, Any]:
        try:
            return self._read_updates_page(**arguments)
        except SightglassError as error:
            delivery_id = arguments.get("ack_delivery_id")
            if error.code != ErrorCode.STORAGE_PRESSURE or delivery_id is None:
                raise
            conversation_id = str(arguments["conversation_id"])
            self.reader.require_active()
            self._conversation_context(conversation_id)
            participants = tuple(sorted(set(arguments.get("participant_ids", ()))))
            scope_kind, scope_key = cursor_scope(participants, arguments.get("query"))
            ack_error = SightglassError(
                ErrorCode.STORAGE_PRESSURE, retryable=True,
                details={**error.details, "ack_committed": True, "ack_delivery_id": delivery_id},
            )
            with self.repository.database.transaction(maintenance=True):
                self._ensure_update_reader_profile()
                if participants:
                    self.repository.participant_source_filters(conversation_id, participants)
                self.replica.acknowledge_delivery(
                    delivery_id=delivery_id, reader_id=self.reader.reader_id,
                    conversation_id=conversation_id, scope_kind=scope_kind, scope_key=scope_key,
                    acknowledged_at=utc_now().isoformat(timespec="microseconds"),
                )
                binding = self.replica.update_request_binding(
                    arguments.get("request_id"), conversation_id=conversation_id,
                    scope_kind=scope_kind, scope_key=scope_key, view=arguments.get("view", "auto"),
                    ack_delivery_id=delivery_id, projection=arguments["projection"],
                    limit=arguments["limit"], include_resources=arguments["include_resources"],
                    system_policy=arguments["system_policy"],
                    voice_policy=arguments.get("voice_policy", "off"),
                )
                self.replica.record_update_error(
                    arguments.get("request_id"), binding, ack_error,
                    conversation_id=conversation_id,
                )
            raise ack_error from error

    def _ensure_update_reader_profile(self) -> None:
        profile = self.repository.database.reader_profile(self.reader.reader_id)
        policy_json = json.dumps(self.reader.policy.as_dict(), sort_keys=True)
        if (profile is not None and bool(profile["active"])
                and str(profile["display_name"]) == self.reader.display_name
                and str(profile["policy_json"]) == policy_json):
            return
        with self.repository.database.transaction(maintenance=True):
            self.repository.upsert_reader(
                self.reader.reader_id, self.reader.display_name, self.reader.policy.as_dict(),
                utc_now().isoformat(timespec="microseconds"),
            )

    def _read_updates_page(
        self,
        *,
        projection: str,
        conversation_id: str,
        participant_ids: tuple[str, ...],
        query: str | None,
        ack_delivery_id: str | None,
        limit: int,
        include_resources: str,
        system_policy: str,
        voice_policy: str = "off",
        view: str = "auto",
        request_id: str | None = None,
    ) -> dict[str, Any]:
        participant_ids = tuple(sorted(set(participant_ids)))
        scope_kind, scope_key = cursor_scope(participant_ids, query)
        self.reader.require_active()
        self.reader.authorize(conversation_id)
        self._ensure_update_reader_profile()
        if participant_ids:
            self.repository.participant_source_filters(conversation_id, participant_ids)
        request_binding = self.replica.update_request_binding(
            request_id, conversation_id=conversation_id, scope_kind=scope_kind,
            scope_key=scope_key, view=view, ack_delivery_id=ack_delivery_id,
            projection=projection, limit=limit, include_resources=include_resources,
            system_policy=system_policy, voice_policy=voice_policy,
        )
        replay = self.replica.replay_update_request(request_id, request_binding)
        if replay is not None:
            return replay
        pending = self.repository.pending_delivery(
            self.reader.reader_id, conversation_id, scope_kind, scope_key
        )
        if ack_delivery_id is None and pending is not None:
            page = self.delivery_store.read(
                str(pending["payload_ref"]), str(pending["payload_digest"])
            )
            if request_id is None:
                return page
            with self.repository.database.transaction(maintenance=True):
                return self.replica.record_update_outcome(
                    request_id, request_binding, page, conversation_id=conversation_id,
                    payload_id=str(pending["delivery_id"]),
                    payload_digest=str(pending["payload_digest"]),
                )

        if view == "replica":
            return self.replica.read_updates(
                projection=projection, conversation_id=conversation_id,
                participant_ids=participant_ids, query=query,
                ack_delivery_id=ack_delivery_id, scope_kind=scope_kind, scope_key=scope_key,
                limit=limit, include_resources=include_resources, system_policy=system_policy,
                voice_policy=voice_policy, view=view, request_id=request_id,
                request_binding=request_binding,
            )

        with self._source_read() as (stack, snapshot):
            catalog_read = self._read_catalog(snapshot)
            facts = self._catalog_facts(snapshot)
            target = self._source_target(catalog_read, conversation_id)
            participants = self._read_message_participants(target, snapshot)
            captured_page = None
            captured_ids = None
            if self._operation_provider.get() is not None:
                captured_ids = self._captured_updates_ids.get()
                if captured_ids is not None:
                    captured_rows = self.repository.frozen_message_rows(captured_ids)
                    if len(captured_rows) != len(captured_ids):
                        raise SightglassError(ErrorCode.CURSOR_STALE)
                    verified = []
                    for row in captured_rows:
                        if row["conversation_id"] != conversation_id:
                            raise SightglassError(ErrorCode.CURSOR_INVALID)
                        source = self.provider.get_message(
                            target.source_account_key, str(row["source_message_id"]), snapshot)
                        if source is None:
                            raise SightglassError(ErrorCode.SOURCE_INCOMPLETE, retryable=True)
                        verified.append(source)
                    hydrated_messages = tuple(verified)
                else:
                    if self._captured_updates_reconcile_revision.get() is None:
                        raise SightglassError(ErrorCode.QUERY_INVALID,
                                             details={"reason": "missing_reconciliation_revision"})
                    captured_page = self.provider.read_range(
                        target.source_account_key, target.source_conversation_id,
                        after=self._captured_updates_after.get(), before=None,
                        direction="forward", limit=200, snapshot=snapshot,
                        participant_source_ids=(),
                    )
                    hydrated_messages = captured_page.messages
            else:
                hydrated_messages = self._read_hydrate_page(target, snapshot)
            prepared_hydrated_messages = self._prepare_messages(hydrated_messages)
            with self._admission(stack):
                replay = self.replica.replay_update_request(request_id, request_binding)
                if replay is not None:
                    return replay
                if ack_delivery_id is None:
                    pending = self.repository.pending_delivery(
                        self.reader.reader_id,
                        conversation_id,
                        scope_kind,
                        scope_key,
                    )
                    if pending is not None:
                        page = self.delivery_store.read(
                            str(pending["payload_ref"]), str(pending["payload_digest"])
                        )
                        return self.replica.record_update_outcome(
                            request_id, request_binding, page, conversation_id=conversation_id,
                            payload_id=str(pending["delivery_id"]),
                            payload_digest=str(pending["payload_digest"]),
                        )
                self._persist_catalog(catalog_read, snapshot)
                context = self._conversation_context(conversation_id)
                self._index_participants(context, participants, snapshot)
                if participant_ids:
                    self.repository.participant_source_filters(conversation_id, participant_ids)
                if ack_delivery_id is not None:
                    self.replica.acknowledge_delivery(
                        delivery_id=ack_delivery_id,
                        reader_id=self.reader.reader_id,
                        conversation_id=conversation_id,
                        scope_kind=scope_kind,
                        scope_key=scope_key,
                        acknowledged_at=utc_now().isoformat(timespec="microseconds"),
                    )
                    pending = self.repository.pending_delivery(
                        self.reader.reader_id, conversation_id, scope_kind, scope_key
                    )
                    if pending is not None:
                        page = self.delivery_store.read(
                            str(pending["payload_ref"]), str(pending["payload_digest"])
                        )
                        return self.replica.record_update_outcome(
                            request_id, request_binding, page, conversation_id=conversation_id,
                            payload_id=str(pending["delivery_id"]),
                            payload_digest=str(pending["payload_digest"]),
                        )
                if (captured_page is not None
                        and self.replica.fresh_updates_reconcile_position(conversation_id)[:2]
                        != (self._captured_updates_after.get(),
                            self._captured_updates_reconcile_revision.get())):
                    raise SightglassError(ErrorCode.CURSOR_STALE)
                self._ingest_prepared_messages(context, prepared_hydrated_messages)
                capture_receipt = None
                if captured_page is not None:
                    capture_receipt = self.replica.record_captured_updates(
                        context, snapshot, captured_page, self._captured_updates_after.get(),
                        self._captured_updates_reconcile_revision.get())
                if self._operation_provider.get() is not None and capture_receipt is None:
                    capture_receipt = self.replica.captured_updates_receipt(context, snapshot)
                captured_message_ids = (tuple(opaque_id(
                    "wxmsg", str(context["account_id"]), source.source_message_id)
                    for source in hydrated_messages)
                    if self._operation_provider.get() is not None else None)
                return self._publish_updates(
                    context=context, snapshot=snapshot, facts=facts,
                    projection=projection, conversation_id=conversation_id,
                    participant_ids=participant_ids, query=query,
                    scope_kind=scope_kind, scope_key=scope_key, limit=limit,
                    include_resources=include_resources, system_policy=system_policy,
                    voice_policy=voice_policy, view=view, request_id=request_id,
                    request_binding=request_binding,
                    source_receipt_override=capture_receipt,
                    capture_has_more=bool(captured_page and captured_page.has_more_after),
                    captured_message_ids=captured_message_ids,
                    captured_empty_frontier=(hydrated_messages[-1].sort_key
                                             if captured_page and hydrated_messages else None),
                )

    def _publish_updates(
        self, *, context: Any, snapshot: SourceSnapshot, facts: _CatalogFacts,
        projection: str, conversation_id: str, participant_ids: tuple[str, ...],
        query: str | None, scope_kind: str, scope_key: str, limit: int,
        include_resources: str, system_policy: str, voice_policy: str,
        view: str, request_id: str | None, request_binding: str | None,
        source_receipt_override: dict[str, Any] | None = None,
        capture_has_more: bool = False,
        captured_message_ids: tuple[str, ...] | None = None,
        captured_empty_frontier: SourceSortKey | None = None,
    ) -> dict[str, Any]:
        committed = self.repository.update_position(
            self.reader.reader_id, conversation_id, scope_kind, scope_key
        )
        local_current = view == "replica" or captured_message_ids is not None
        observations = self.repository.observation_rows_after(
            conversation_id, committed, participant_ids,
            projection_epoch=self._projection_inventory_epoch() if local_current else None,
            limit=(201 if captured_message_ids is not None
                   else 10_001 if view == "replica" else None),
        )
        captured_unverified_more = False
        if captured_message_ids is not None:
            admitted = set(captured_message_ids)
            prefix = []
            for row in observations:
                if str(row["message_id"]) not in admitted:
                    captured_unverified_more = True
                    break
                prefix.append(row)
            observations = prefix
        scan_partial = view == "replica" and len(observations) > 10_000
        if view == "replica":
            observations = observations[:10_000]
        scanned_only_to = None
        filtered = [row for row in observations if self._text_matches(row["text"], query)]
        if local_current and observations and not filtered:
            # A zero-hit bounded filter page represents the examined range. Its
            # exact empty delivery lets ACK advance only this filter's position.
            scanned_only_to = int(observations[-1]["observation_seq"])
        elif (captured_message_ids is not None and not observations
              and captured_empty_frontier is not None and not captured_unverified_more):
            # A verified zero-hit page needs an exact delivery to ACK this scope's
            # observed timeline boundary. Reconciliation has its own scan cursor.
            scanned_only_to = committed
        scanned_observations = observations
        observations = filtered
        source_has_more = (capture_has_more or captured_unverified_more or scan_partial
                           or len(observations) > limit)
        delivery_candidates = observations[:limit]
        timeline = self.repository.timeline_position(
            self.reader.reader_id, conversation_id, scope_kind, scope_key
        )
        late_ids: set[str] = set()
        if timeline is not None:
            tie = json.loads(str(timeline["committed_sort_tie"]))
            timeline_key = (
                str(timeline["committed_sort_primary"]),
                int(tie[0]),
                int(tie[1]),
                str(tie[2]),
            )
            late_ids = {
                str(row["message_id"])
                for row in delivery_candidates
                if source_sort_key(row).as_tuple() <= timeline_key
            }
        visible_observations, _hidden_system_count = self._apply_system_policy(
            delivery_candidates, system_policy
        )
        voice_candidates = self._voice_candidates(
            voice_policy,
            tuple(str(row["message_id"]) for row in visible_observations),
        )
        voice_reserve = self._voice_reserve(voice_policy, voice_candidates)
        page: dict[str, Any]
        if projection == "detail":
            visible_observations, projected_detail, text_truncated = (
                self._prepare_detail_rows(
                    visible_observations,
                    context=context,
                    direction="forward",
                    include_resources=include_resources,
                    participant_ids=participant_ids,
                    context_only_ids=frozenset(),
                    late_arrival_ids=frozenset(late_ids),
                    voice_reserve_chars=voice_reserve,
                )
            )
            if visible_observations:
                to_sequence = max(
                    int(row["observation_seq"]) for row in visible_observations
                )
            elif delivery_candidates:
                to_sequence = max(
                    int(row["observation_seq"]) for row in delivery_candidates
                )
            else:
                to_sequence = scanned_only_to
            delivery_observations = [
                row
                for row in delivery_candidates
                if to_sequence is not None and int(row["observation_seq"]) <= to_sequence
            ]
            hidden_system_count = (
                sum(row["kind"] in {"system", "recalled"} for row in delivery_observations)
                if system_policy == "omit"
                else 0
            )
            delivery_id = (
                opaque_id(
                    "wxdelivery",
                    self.reader.reader_id,
                    conversation_id,
                    scope_kind,
                    scope_key,
                    committed,
                    to_sequence,
                    utc_now().isoformat(timespec="microseconds"),
                )
                if to_sequence is not None
                else None
            )
            if to_sequence is None:
                has_more = source_has_more
            else:
                has_more = (
                    source_has_more
                    or text_truncated
                    or any(
                        int(row["observation_seq"]) > to_sequence
                        for row in delivery_candidates
                    )
                )
            page = self._page_response(
                projection="detail",
                mode="updates",
                context=context,
                rows=visible_observations,
                snapshot=snapshot,
                facts=facts,
                source_receipt_override=source_receipt_override,
                has_more_before=False,
                has_more_after=has_more,
                participant_ids=participant_ids,
                speaker_view="only" if participant_ids else None,
                include_resources=include_resources,
                system_policy=system_policy,
                focus_message_ids=frozenset(
                    str(row["message_id"]) for row in visible_observations
                ),
                late_arrival_ids=frozenset(late_ids),
                delivery_id=delivery_id,
                truncated=has_more,
                hidden_system_count=hidden_system_count,
                projected_detail=projected_detail,
                voice_reserve_chars=voice_reserve,
            )
        else:
            prepared = self.compact_projector.prepare(
                visible_observations,
                timezone_name=self._reader_timezone(context),
                include_resource_indicators=include_resources == "indicator",
                focus_message_ids=frozenset(
                    str(row["message_id"]) for row in visible_observations
                ),
                context_only_ids=frozenset(),
                late_arrival_ids=frozenset(late_ids),
            )

            def build_compact_update(
                selected: slice, message_rows_complete: bool
            ) -> dict[str, Any]:
                candidate_rows = visible_observations[selected]
                candidate_prepared = prepared[selected]
                if candidate_rows:
                    candidate_to_sequence = max(
                        int(row["observation_seq"]) for row in candidate_rows
                    )
                elif delivery_candidates:
                    candidate_to_sequence = max(
                        int(row["observation_seq"]) for row in delivery_candidates
                    )
                else:
                    candidate_to_sequence = scanned_only_to
                candidate_delivery_rows = [
                    row
                    for row in delivery_candidates
                    if candidate_to_sequence is not None
                    and int(row["observation_seq"]) <= candidate_to_sequence
                ]
                candidate_hidden_count = (
                    sum(
                        row["kind"] in {"system", "recalled"}
                        for row in candidate_delivery_rows
                    )
                    if system_policy == "omit"
                    else 0
                )
                candidate_delivery_id = (
                    opaque_id(
                        "wxdelivery",
                        self.reader.reader_id,
                        conversation_id,
                        scope_kind,
                        scope_key,
                        committed,
                        candidate_to_sequence,
                        utc_now().isoformat(timespec="microseconds"),
                    )
                    if candidate_to_sequence is not None
                    else None
                )
                candidate_has_more = source_has_more or (
                    candidate_to_sequence is not None
                    and any(
                        int(row["observation_seq"]) > candidate_to_sequence
                        for row in delivery_candidates
                    )
                )
                return self._page_response(
                    projection="compact",
                    mode="updates",
                    context=context,
                    rows=candidate_rows,
                    snapshot=snapshot,
                    facts=facts,
                    source_receipt_override=source_receipt_override,
                    has_more_before=False,
                    has_more_after=candidate_has_more,
                    participant_ids=participant_ids,
                    speaker_view="only" if participant_ids else None,
                    include_resources=include_resources,
                    system_policy=system_policy,
                    focus_message_ids=frozenset(
                        str(row["message_id"]) for row in candidate_rows
                    ),
                    late_arrival_ids=frozenset(late_ids),
                    delivery_id=candidate_delivery_id,
                    hidden_system_count=candidate_hidden_count,
                    compact_prepared=candidate_prepared,
                    message_rows_complete=message_rows_complete,
                    voice_reserve_chars=voice_reserve,
                )

            selected, page = self._fit_compact_page(
                len(visible_observations),
                direction="forward",
                build=build_compact_update,
            )
            visible_observations = visible_observations[selected]
            if visible_observations:
                to_sequence = max(
                    int(row["observation_seq"]) for row in visible_observations
                )
            elif delivery_candidates:
                to_sequence = max(
                    int(row["observation_seq"]) for row in delivery_candidates
                )
            else:
                to_sequence = scanned_only_to
            delivery_observations = [
                row
                for row in delivery_candidates
                if to_sequence is not None and int(row["observation_seq"]) <= to_sequence
            ]
            delivery_id = page["page"]["delivery_id"]
        if view == "fresh":
            page["source_receipt"]["view"] = "fresh"
        if view == "replica":
            page["page"]["observation_window"] = {
                "after": committed, "through": to_sequence, "scan_partial": scan_partial,
            }
        page = self._attach_voice(
            page,
            policy=voice_policy,
            candidates=voice_candidates,
            rows=visible_observations,
            conversation_id=conversation_id,
            projection=projection,
            focus_message_ids=frozenset(
                str(row["message_id"]) for row in visible_observations
            ),
        )
        if to_sequence is None:
            return self.replica.record_update_outcome(request_id, request_binding, page,
                                                     conversation_id=conversation_id)
        assert to_sequence is not None and delivery_id is not None
        payload_ref, payload_digest = self.delivery_store.write(delivery_id, page)
        self.repository.create_pending_delivery(
            delivery_id=delivery_id,
            reader_id=self.reader.reader_id,
            conversation_id=conversation_id,
            scope_kind=scope_kind,
            scope_key=scope_key,
            from_observation_seq=committed + 1,
            to_observation_seq=to_sequence,
            payload_digest=payload_digest,
            payload_ref=payload_ref,
            projection_schema_version=str(page["schema"]),
            created_at=utc_now().isoformat(timespec="microseconds"),
        )
        if captured_message_ids is not None:
            traversed = [row for row in scanned_observations
                         if int(row["observation_seq"]) <= to_sequence]
            frontier = (max((source_sort_key(row) for row in traversed),
                            key=lambda key: key.as_tuple())
                        if traversed else captured_empty_frontier)
            if frontier is not None:
                self.replica.record_update_frontier(
                    delivery_id, conversation_id=conversation_id, scope_kind=scope_kind,
                    scope_key=scope_key, frontier=frontier,
                )
        return self.replica.record_update_outcome(
            request_id, request_binding, page, conversation_id=conversation_id,
            payload_id=delivery_id, payload_digest=payload_digest,
        )
    def _voice_policy(self, requested: str | None) -> str:
        """Resolve one read's voice policy; message-only or disabled readers stay unchanged."""

        if requested is not None and requested not in {"auto", "cached", "off"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if self.voice is None or not self.voice_settings.enabled:
            return "off"
        if not self.reader.policy.resource_preview:
            return "off"
        return self.voice_settings.policy_for(requested)

    def _voice_candidates(
        self, policy: str, message_ids: tuple[str, ...]
    ) -> tuple[VoiceCandidate, ...]:
        if self.voice is None or policy == "off" or not message_ids:
            return ()
        return self.voice.collect(message_ids)

    def _voice_reserve(self, policy: str, candidates: tuple[VoiceCandidate, ...]) -> int:
        if self.voice is None:
            return 0
        return self.voice.reserve_chars(policy, candidates)

    def _voice_page_budget(self, projection: str) -> int:
        if projection == "compact":
            return self.reader.policy.max_compact_payload_chars
        return min(
            self.reader.policy.max_detail_payload_chars,
            self.reader.policy.max_text_chars_per_call,
        )

    def _require_voice_read(self, conversation_id: str) -> None:
        """Re-check pause, resource capability, and conversation policy before preparing."""

        self.reader.require_active()
        self.reader.require_resource("preview")
        self.reader.authorize(conversation_id)

    def _attach_voice(
        self,
        page: dict[str, Any],
        *,
        policy: str,
        candidates: tuple[VoiceCandidate, ...],
        rows: list[Any],
        conversation_id: str,
        projection: str,
        focus_message_ids: frozenset[str] = frozenset(),
    ) -> dict[str, Any]:
        """Attach the page-level voice sidecar once the delivered rows are fixed."""

        if self.voice is None or policy == "off" or not candidates:
            return page
        self._require_voice_read(conversation_id)
        return self.voice.attach(
            page,
            reader_id=self.reader.reader_id,
            message_ids=tuple(str(row["message_id"]) for row in rows),
            candidates=candidates,
            account_binding_id=self.voice.account_binding_id(candidates[0].account_id),
            policy=policy,
            budget_chars=self._voice_page_budget(projection),
            focus_message_ids=focus_message_ids,
        )

    def _local_anchor_row(
        self, anchor: str, requested_conversation_id: str | None
    ) -> Any:
        payload = self.token_codec.decode(anchor)
        if (
            payload.get("schema") != 1
            or not isinstance(payload.get("account_id"), str)
            or not isinstance(payload.get("conversation_id"), str)
            or not isinstance(payload.get("message_id"), str)
            or not isinstance(payload.get("sort"), list)
            or len(payload["sort"]) != 4
        ):
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        if (
            requested_conversation_id is not None
            and payload["conversation_id"] != requested_conversation_id
        ):
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        row = self.repository.message_position_row(payload["message_id"])
        if row is None:
            raise SightglassError(ErrorCode.CURSOR_STALE)
        expected_sort = [
            str(row["sort_primary"]),
            int(row["sort_seq"]),
            int(row["sort_tie"]),
            str(row["message_id"]),
        ]
        if (
            str(row["account_id"]) != payload["account_id"]
            or str(row["conversation_id"]) != payload["conversation_id"]
            or payload["sort"] != expected_sort
        ):
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        return row

    def _materialized_target(
        self,
        *,
        mode: str,
        conversation_id: str | None,
        message_id: str | None,
        anchor: str | None,
        view: str = "auto",
    ) -> _MaterializedTarget | None:
        if mode not in {"recent", "context", "range", "message", "speaker"}:
            return None
        target_row = None
        if mode == "context" and anchor is not None:
            target_row = self._local_anchor_row(anchor, conversation_id)
            if message_id is not None and message_id != str(target_row["message_id"]):
                raise SightglassError(ErrorCode.CURSOR_INVALID)
            conversation_id = str(target_row["conversation_id"])
        elif mode in {"message", "context"}:
            if message_id is None:
                raise SightglassError(ErrorCode.QUERY_INVALID)
            target_row = self.repository.message_position_row(message_id)
            if target_row is None:
                raise SightglassError(
                    ErrorCode.MESSAGE_NOT_FOUND,
                    details={"coverage": {"state": "not_yet_observed"}},
                )
            if (
                conversation_id is not None
                and conversation_id != str(target_row["conversation_id"])
            ):
                raise SightglassError(ErrorCode.MESSAGE_NOT_FOUND)
            conversation_id = str(target_row["conversation_id"])
        if conversation_id is None:
            return None
        context = self.repository.conversation_context(conversation_id)
        if context is None:
            # A cold opaque conversation target may still be valid in the configured
            # source catalog.  It is not materialized yet, so leave it to the existing
            # source-backed path instead of turning the local-plane probe into a false
            # CONVERSATION_NOT_FOUND result.
            return None
        self.reader.authorize(conversation_id)
        epoch = self._projection_inventory_epoch()
        with self.repository.database.connection() as connection:
            expired = connection.execute(
                "SELECT 1 FROM message_body_residency WHERE conversation_id=? "
                "AND expires_at<=? LIMIT 1", (conversation_id, datetime.now(UTC).isoformat())
            ).fetchone()
        if expired and view != "replica":
            return None
        state = self.repository.source_conversation_state(conversation_id)
        if state is not None and (
            str(state["last_error_code"] or "")
            == CONVERSATION_IDENTITY_CONFLICT_WARNING
        ):
            return None
        if target_row is not None and (
            str(target_row["current_state"]) != "present"
            or str(target_row["projection_epoch"] or "") != epoch
            or target_row["current_observation_seq"] is None
        ):
            return None
        if target_row is None and view != "replica":
            # An exact admitted target proves its local version, not the current
            # tail. Unanchored timeline reads still need independent tail evidence.
            if state is None or str(state["source_inventory_epoch"] or "") != epoch:
                if state is not None or self._residency_decision(conversation_id).mode == "keep":
                    return None
                with self.repository.database.connection() as connection:
                    cached = connection.execute(
                        "SELECT 1 FROM read_lease l WHERE l.conversation_id=? "
                        "AND l.projection_epoch=? AND l.expires_at>? AND l.scope_key=? "
                        "AND EXISTS(SELECT 1 FROM read_lease_message p "
                        "JOIN messages m USING(message_id) "
                        "WHERE p.lease_id=l.lease_id "
                        "AND p.observation_seq=m.current_observation_seq "
                        "AND m.body_available=1 AND m.projection_epoch=l.projection_epoch) LIMIT 1",
                        (
                            conversation_id,
                            epoch,
                            datetime.now(UTC).isoformat(),
                            "recent:" + self.reader.reader_id,
                        ),
                    ).fetchone()
                if not cached:
                    return None
            if not self.repository.has_materialized_messages(
                epoch, conversation_id=conversation_id
            ):
                return None
        catalog_state = self.repository.source_catalog_state(str(context["account_id"]))
        return _MaterializedTarget(context, state, catalog_state, target_row, view)

    def local_message_read_ready(self, arguments: dict[str, Any]) -> bool:
        """Whether this exact message call can be served without the provider."""

        mode = arguments.get("mode", "recent")
        refresh = arguments.get("refresh", False)
        try:
            selected = {key: value for key, value in arguments.items()
                        if key != "response_profile"}
            selected.setdefault("mode", "recent")
            selected["participant_ids"] = tuple(selected.get("participant_ids") or ())
            settings = self._validate_message_arguments(**selected)
        except SightglassError:
            return True
        except (TypeError, ValueError):
            # Public MCP validation reports malformed argument shapes locally.
            return True
        view = settings["resolved_view"]
        if view == "replica":
            return True
        if mode == "updates":
            try:
                self.reader.require_active()
                self.reader.authorize(str(arguments["conversation_id"]))
            except SightglassError:
                return True
            return self.replica.update_replay_ready(arguments)
        cursor = arguments.get("cursor")
        cursor_kind = None
        if cursor is not None:
            if not isinstance(cursor, str):
                return False
            try:
                cursor_kind = self.token_codec.decode(cursor).get("kind")
                if view == "fresh" and cursor_kind != "timeline":
                    return True
            except SightglassError:
                return True
        try:
            target = self._materialized_target(
                mode=mode,
                conversation_id=(
                    str(arguments["conversation_id"])
                    if isinstance(arguments.get("conversation_id"), str)
                    else None
                ),
                message_id=(
                    str(arguments["message_id"])
                    if isinstance(arguments.get("message_id"), str)
                    else None
                ),
                anchor=(
                    str(arguments["anchor"])
                    if isinstance(arguments.get("anchor"), str)
                    else None
                ),
            )
            # Fresh acquisition needs a source lease only after the canonical
            # arguments, target signature, message identity and policy are valid.
            if view == "fresh" or cursor_kind not in {None, "timeline-materialized"}:
                return False
            # A materialized continuation with no valid local target is already
            # stale; execution reports that error without contacting the provider.
            return not refresh and (target is not None or cursor is not None)
        except SightglassError:
            return True

    @staticmethod
    def _row_sort_key(row: Any) -> SourceSortKey:
        return SourceSortKey(
            str(row["sort_primary"]),
            int(row["sort_seq"]),
            int(row["sort_tie"]),
            str(row["source_message_id"]),
        )

    def _materialized_receipt(
        self,
        target: _MaterializedTarget,
        *,
        observation_watermark: int,
    ) -> dict[str, Any]:
        epoch = self._projection_inventory_epoch()
        catalog_coverage = (
            str(target.catalog_state["coverage_state"])
            if target.catalog_state is not None
            and str(target.catalog_state["source_inventory_epoch"] or "") == epoch
            else "unknown"
        )
        state = target.conversation_state
        current_tail = state is not None and str(state["source_inventory_epoch"] or "") == epoch
        if state is not None and current_tail:
            backfill_state = (
                str(state["backfill_state"])
                if state["coverage_version"]
                and state["history_complete"]
                and state["forward_complete"]
                else "partial"
            )
            fresh_as_of = str(state["updated_at"])
        else:
            backfill_state = "partial"
            if target.target_row is not None:
                fresh_as_of = str(target.target_row["last_seen_at"])
            else:
                with self.repository.database.connection() as connection:
                    cached = connection.execute(
                        "SELECT updated_at FROM source_read_windows WHERE conversation_id=? "
                        "AND projection_epoch=? ORDER BY window_id DESC LIMIT 1",
                        (target.context["conversation_id"], epoch),
                    ).fetchone()
                fresh_as_of = (str(cached[0]) if cached is not None
                               else str(target.context["last_seen_at"]))
        conversation_coverage = "complete" if backfill_state == "complete" else "indexed"
        with self.repository.database.connection() as connection:
            window_count = int(
                connection.execute(
                    (
                        "SELECT COUNT(*) FROM source_read_windows WHERE conversation_id=? AND "
                        "projection_epoch=?"
                    ),
                    (target.context["conversation_id"], epoch),
                ).fetchone()[0]
            )
        if window_count > 1:
            conversation_coverage = "indexed"
        if target.view == "replica":
            conversation_coverage = "resident_subset"
        observed_after, observed_before = self.repository.materialized_observation_bounds(
            str(target.context["conversation_id"]),
            projection_epoch=epoch,
            observation_watermark=observation_watermark,
        )
        warnings = ["materialized_projection_not_live"]
        if catalog_coverage != "complete":
            warnings.append("catalog_partial")
        if conversation_coverage != "complete":
            warnings.append("history_not_fully_indexed")
        last_error = state["last_error_code"] if state is not None else None
        if last_error:
            warnings.append("live_refresh_degraded")
        projection_digest = opaque_id("wxprojection", str(target.context["account_id"]), epoch)
        snapshot_digest = opaque_id("wxsnapshot", projection_digest, str(observation_watermark))
        return {
            "complete": catalog_coverage == "complete" and conversation_coverage == "complete",
            "fresh_as_of": fresh_as_of,
            "inventory_digest": projection_digest,
            "generation_set_digest": snapshot_digest,
            "returned_count": 0,
            "hidden_system_count": 0,
            "coverage": Coverage(
                catalog=catalog_coverage,
                conversation=conversation_coverage,
                roster=("complete" if bool(target.context["roster_complete"]) else "partial"),
                observed_time_after=observed_after,
                observed_time_before=observed_before,
                active_conversations_only=False,
                notes=("bounded_materialized_view", "has_more_describes_admitted_messages"),
            ).as_dict(),
            "warnings": warnings,
            "served_from": "window_db",
            "view": target.view if target.view != "auto" else "materialized",
            "continuity": {
                "state": "unverified"
                if not window_count
                else ("disjoint_windows" if window_count > 1 else "validated_window"),
                "validated_window_count": window_count,
                "context_neighbors": "same_validated_window_only",
            },
            "freshness": {
                "state": "bounded_stale",
                "live_refresh_confirmed": False,
                "projection_epoch": epoch,
                "observation_watermark": observation_watermark,
                "live_error_code": str(last_error) if last_error else None,
            },
        }

    def _materialized_boundary(
        self,
        *,
        cursor: str,
        target: _MaterializedTarget,
        mode: str,
        direction: str,
        scope_kind: str,
        scope_key: str,
    ) -> tuple[Any, int]:
        epoch = self._projection_inventory_epoch()
        payload = self.timeline_cursors.verify_materialized(
            cursor,
            reader_id=self.reader.reader_id,
            account_id=str(target.context["account_id"]),
            conversation_id=str(target.context["conversation_id"]),
            mode=mode,
            direction=direction,
            scope_kind=scope_kind,
            scope_key=scope_key,
            projection_epoch=epoch,
            policy_revision=self._materialized_cursor_revision(),
            view=target.view,
        )
        watermark = int(payload["projection"]["observation_watermark"])
        if self.repository.materialized_snapshot_changed(
            str(target.context["conversation_id"]),
            projection_epoch=epoch,
            observation_watermark=watermark,
        ):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        row = self.repository.message_position_row(payload["position"]["message_id"])
        if row is None:
            raise SightglassError(ErrorCode.CURSOR_STALE)
        expected_sort = [
            str(row["sort_primary"]),
            int(row["sort_seq"]),
            int(row["sort_tie"]),
        ]
        if (
            str(row["account_id"]) != str(target.context["account_id"])
            or str(row["conversation_id"]) != str(target.context["conversation_id"])
            or str(row["projection_epoch"] or "") != epoch
            or payload["position"]["sort"] != expected_sort
            or int(row["first_observation_seq"] or watermark + 1) > watermark
            or int(row["current_observation_seq"] or watermark + 1) > watermark
        ):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        return row, watermark

    def _next_materialized_cursor(
        self,
        *,
        rows: list[Any],
        target: _MaterializedTarget,
        mode: str,
        direction: str,
        scope_kind: str,
        scope_key: str,
        observation_watermark: int,
        has_more: bool,
    ) -> str | None:
        if not rows or not has_more:
            return None
        boundary = rows[0] if direction == "backward" else rows[-1]
        return self.timeline_cursors.issue_materialized(
            reader_id=self.reader.reader_id,
            account_id=str(target.context["account_id"]),
            conversation_id=str(target.context["conversation_id"]),
            mode=mode,
            direction=direction,
            scope_kind=scope_kind,
            scope_key=scope_key,
            row=boundary,
            projection_epoch=self._projection_inventory_epoch(),
            observation_watermark=observation_watermark,
            policy_revision=self._materialized_cursor_revision(),
            view=target.view,
        )

    @staticmethod
    def _trim_materialized_page(
        rows: list[Any], *, limit: int, direction: str
    ) -> tuple[list[Any], bool, bool]:
        if len(rows) <= limit:
            return rows, False, False
        if direction == "backward":
            return rows[-limit:], True, False
        return rows[:limit], False, True

    def _finish_materialized_page(
        self,
        *,
        target: _MaterializedTarget,
        mode: str,
        projection: str,
        include_resources: str,
        system_policy: str,
        participant_ids: tuple[str, ...],
        speaker_view: str,
        direction: str,
        scope_kind: str,
        scope_key: str,
        observation_watermark: int,
        rows: list[Any],
        scanned_rows: list[Any],
        focus_message_ids: frozenset[str],
        context_only_ids: frozenset[str],
        has_more_before: bool,
        has_more_after: bool,
        voice_policy: str,
    ) -> _FrozenMaterializedPage:
        projection_maximum = (
            self.reader.policy.max_compact_messages_per_call
            if projection == "compact"
            else self.reader.policy.max_detail_messages_per_call
        )
        if len(rows) > projection_maximum:
            raise SightglassError(ErrorCode.OUTPUT_BUDGET_EXCEEDED)
        rows, hidden_system_count = self._apply_system_policy(rows, system_policy)
        visible_ids = frozenset(str(row["message_id"]) for row in rows)
        focus_message_ids = frozenset(focus_message_ids & visible_ids)
        context_only_ids = frozenset(context_only_ids & visible_ids)
        voice_candidates = self._voice_candidates(
            voice_policy, tuple(str(row["message_id"]) for row in rows)
        )
        voice_reserve = self._voice_reserve(voice_policy, voice_candidates)
        receipt = self._materialized_receipt(target, observation_watermark=observation_watermark)
        pseudo_snapshot = SourceSnapshot(
            inventory_digest=str(receipt["inventory_digest"]),
            generation_set_digest=str(receipt["generation_set_digest"]),
            fresh_as_of=str(receipt["fresh_as_of"]),
            generation_by_shard=(),
            token="materialized",
        )
        facts = _CatalogFacts(
            complete=receipt["coverage"]["catalog"] == "complete",
            active_conversations_only=False,
        )

        def cursor_for(candidate_rows: list[Any], more: bool) -> str | None:
            return (
                self._next_materialized_cursor(
                    rows=candidate_rows or scanned_rows,
                    target=target,
                    mode=mode,
                    direction=direction,
                    scope_kind=scope_kind,
                    scope_key=scope_key,
                    observation_watermark=observation_watermark,
                    has_more=more,
                )
                if mode in {"recent", "range", "speaker"}
                else None
            )

        participant_focus = participant_ids if mode == "speaker" else ()
        if projection == "detail":
            rows, projected_detail, text_truncated = self._prepare_detail_rows(
                rows,
                context=target.context,
                direction=direction,
                include_resources=include_resources,
                participant_ids=participant_focus,
                context_only_ids=context_only_ids,
                voice_reserve_chars=voice_reserve,
            )
            if text_truncated:
                if direction == "backward":
                    has_more_before = True
                else:
                    has_more_after = True
            visible_ids = frozenset(str(row["message_id"]) for row in rows)
            focus_message_ids = frozenset(focus_message_ids & visible_ids)
            context_only_ids = frozenset(context_only_ids & visible_ids)
            focus_rows = [row for row in rows if str(row["message_id"]) in focus_message_ids]
            more = has_more_before if direction == "backward" else has_more_after
            page = self._page_response(
                projection="detail",
                mode=mode,
                context=target.context,
                rows=rows,
                snapshot=pseudo_snapshot,
                facts=facts,
                has_more_before=has_more_before,
                has_more_after=has_more_after,
                participant_ids=participant_focus,
                speaker_view=speaker_view if mode == "speaker" else None,
                include_resources=include_resources,
                system_policy=system_policy,
                focus_message_ids=focus_message_ids,
                context_only_ids=context_only_ids,
                next_cursor=cursor_for(focus_rows, more),
                truncated=has_more_before or has_more_after or text_truncated,
                hidden_system_count=hidden_system_count,
                projected_detail=projected_detail,
                voice_reserve_chars=voice_reserve,
                source_receipt_override=receipt,
            )
        else:
            prepared = self.compact_projector.prepare(
                rows,
                timezone_name=self._reader_timezone(target.context),
                include_resource_indicators=include_resources == "indicator",
                focus_message_ids=focus_message_ids,
                context_only_ids=context_only_ids,
                late_arrival_ids=frozenset(),
            )

            def build(selected: slice, complete: bool) -> dict[str, Any]:
                candidate_rows = rows[selected]
                candidate_prepared = prepared[selected]
                candidate_ids = frozenset(str(row["message_id"]) for row in candidate_rows)
                candidate_focus_ids = frozenset(focus_message_ids & candidate_ids)
                candidate_context_ids = frozenset(context_only_ids & candidate_ids)
                candidate_focus_rows = [
                    row for row in candidate_rows if str(row["message_id"]) in candidate_focus_ids
                ]
                candidate_more_before = has_more_before or (
                    not complete and direction == "backward"
                )
                candidate_more_after = has_more_after or (not complete and direction == "forward")
                more = candidate_more_before if direction == "backward" else candidate_more_after
                return self._page_response(
                    projection="compact",
                    mode=mode,
                    context=target.context,
                    rows=candidate_rows,
                    snapshot=pseudo_snapshot,
                    facts=facts,
                    has_more_before=candidate_more_before,
                    has_more_after=candidate_more_after,
                    participant_ids=participant_focus,
                    speaker_view=speaker_view if mode == "speaker" else None,
                    include_resources=include_resources,
                    system_policy=system_policy,
                    focus_message_ids=candidate_focus_ids,
                    context_only_ids=candidate_context_ids,
                    next_cursor=cursor_for(candidate_focus_rows, more),
                    hidden_system_count=hidden_system_count,
                    compact_prepared=candidate_prepared,
                    message_rows_complete=complete,
                    voice_reserve_chars=voice_reserve,
                    source_receipt_override=receipt,
                )

            selected, page = self._fit_compact_page(len(rows), direction=direction, build=build)
            rows = rows[selected]
            visible_ids = frozenset(str(row["message_id"]) for row in rows)
            focus_message_ids = frozenset(focus_message_ids & visible_ids)

        return _FrozenMaterializedPage(
            page, target, rows, focus_message_ids, voice_candidates, receipt
        )

    def _read_materialized_messages(self, **arguments: Any) -> dict[str, Any] | None:
        # Freeze cursor validation, rows, labels/resources, budgets and receipt in
        # one query-only SQLite view. Progress/voice writes begin after it closes.
        with self.repository.database.read_snapshot():
            frozen = self._select_materialized_messages(**arguments)
        if frozen is None:
            return None
        mode = arguments["mode"]
        focus_rows = [row for row in frozen.rows if str(row["message_id"]) in frozen.focus_ids]
        if mode in {"recent", "range", "speaker"} and focus_rows:
            committed = max(focus_rows, key=lambda row: self._row_sort_key(row).as_tuple())
            self.repository.commit_timeline_position(
                reader_id=self.reader.reader_id,
                conversation_id=str(frozen.target.context["conversation_id"]),
                scope_kind=arguments["scope_kind"],
                scope_key=arguments["scope_key"],
                row=committed,
                updated_at=str(frozen.receipt["fresh_as_of"]),
                seed_update_cursor=True,
                admitted_message_ids=tuple(str(row["message_id"]) for row in focus_rows),
                observation_watermark=int(frozen.receipt["freshness"]["observation_watermark"]),
            )
        return self._attach_voice(
            frozen.page,
            policy=arguments["voice_policy"],
            candidates=frozen.voice_candidates,
            rows=frozen.rows,
            conversation_id=str(frozen.target.context["conversation_id"]),
            projection=arguments["projection"],
            focus_message_ids=frozen.focus_ids,
        )

    def _select_materialized_messages(
        self,
        *,
        mode: str,
        conversation_id: str | None,
        message_id: str | None,
        anchor: str | None,
        before: int,
        after: int,
        limit: int,
        direction: str,
        cursor: str | None,
        participant_ids: tuple[str, ...],
        speaker_view: str,
        time_after: str | None,
        time_before: str | None,
        query: str | None,
        projection: str,
        include_resources: str,
        system_policy: str,
        scope_kind: str,
        scope_key: str,
        voice_policy: str,
        view: str = "auto",
    ) -> _FrozenMaterializedPage | None:
        if cursor is not None:
            cursor_kind = self.token_codec.decode(cursor).get("kind")
            if cursor_kind != "timeline-materialized":
                if view == "replica":
                    raise SightglassError(ErrorCode.CURSOR_INVALID)
                return None
        target = self._materialized_target(
            mode=mode,
            conversation_id=conversation_id,
            message_id=message_id,
            anchor=anchor,
            view=view,
        )
        if target is None:
            if cursor is not None:
                raise SightglassError(ErrorCode.CURSOR_STALE)
            if local_read_only_requested():
                raise SightglassError(
                    ErrorCode.SOURCE_INCOMPLETE,
                    retryable=True,
                    details={"warning_codes": ["materialized_projection_changed"]},
                )
            if view == "replica":
                raise SightglassError(
                    ErrorCode.SOURCE_INCOMPLETE,
                    details={"view": "replica", "coverage": {"state": "unavailable"},
                             "warning_codes": ["replica_scope_unavailable"]},
                )
            return None
        if cursor is not None:
            boundary, watermark = self._materialized_boundary(
                cursor=cursor,
                target=target,
                mode=mode,
                direction=direction,
                scope_kind=scope_kind,
                scope_key=scope_key,
            )
        else:
            boundary = None
            watermark = self.repository.observation_watermark()
        epoch = self._projection_inventory_epoch()
        conversation = str(target.context["conversation_id"])
        boundary_key = self._row_sort_key(boundary) if boundary is not None else None
        focus_rows: list[Any]
        scanned_rows: list[Any] = []
        rows: list[Any]
        context_only_ids: frozenset[str] = frozenset()
        has_more_before = False
        has_more_after = False

        if mode == "message":
            assert target.target_row is not None
            rows = self.repository.materialized_message_rows(
                conversation,
                projection_epoch=epoch,
                observation_watermark=watermark,
                limit=1,
                direction="forward",
                message_id=str(target.target_row["message_id"]),
            )
            if not rows:
                raise SightglassError(
                    ErrorCode.SOURCE_INCOMPLETE if view == "replica"
                    else ErrorCode.MESSAGE_NOT_FOUND,
                    details={"view": "replica", "coverage": {"state": "body_unavailable"}}
                    if view == "replica" else None,
                )
            focus_rows = rows
        elif mode == "context":
            assert target.target_row is not None
            target_rows = self.repository.materialized_message_rows(
                conversation,
                projection_epoch=epoch,
                observation_watermark=watermark,
                limit=1,
                direction="forward",
                message_id=str(target.target_row["message_id"]),
            )
            if not target_rows:
                raise SightglassError(
                    ErrorCode.CURSOR_STALE if anchor else ErrorCode.SOURCE_INCOMPLETE
                    if view == "replica" else ErrorCode.MESSAGE_NOT_FOUND,
                    details={"view": "replica", "coverage": {"state": "body_unavailable"}}
                    if view == "replica" else None,
                )
            target_row = target_rows[0]
            before_rows = (
                self.repository.materialized_message_rows(
                    conversation,
                    projection_epoch=epoch,
                    observation_watermark=watermark,
                    limit=max(1, before + 1),
                    direction="backward",
                    before=self._row_sort_key(target_row),
                    continuity_with=self._row_sort_key(target_row),
                )
                if before
                else []
            )
            after_rows = (
                self.repository.materialized_message_rows(
                    conversation,
                    projection_epoch=epoch,
                    observation_watermark=watermark,
                    limit=max(1, after + 1),
                    direction="forward",
                    after=self._row_sort_key(target_row),
                    continuity_with=self._row_sort_key(target_row),
                )
                if after
                else []
            )
            if len(before_rows) > before:
                before_rows = before_rows[-before:]
                has_more_before = True
            if len(after_rows) > after:
                after_rows = after_rows[:after]
                has_more_after = True
            rows = [*before_rows, target_row, *after_rows]
            focus_rows = [target_row]
            context_only_ids = frozenset(
                str(row["message_id"]) for row in (*before_rows, *after_rows)
            )
        elif mode in {"recent", "range"}:
            queried = self.repository.materialized_message_rows(
                conversation,
                projection_epoch=epoch,
                observation_watermark=watermark,
                limit=limit + 1,
                direction=direction,
                after=boundary_key if direction == "forward" else None,
                before=boundary_key if direction == "backward" else None,
                time_after_utc=time_after if mode == "range" else None,
                time_before_utc=time_before if mode == "range" else None,
            )
            rows, has_more_before, has_more_after = self._trim_materialized_page(
                queried, limit=limit, direction=direction
            )
            focus_rows = rows
        else:
            self.repository.participant_source_filters(conversation, participant_ids)
            fetch_limit = 10_001 if query else limit + 1
            candidates = self.repository.materialized_message_rows(
                conversation,
                projection_epoch=epoch,
                observation_watermark=watermark,
                limit=fetch_limit,
                direction=direction,
                after=boundary_key if direction == "forward" else None,
                before=boundary_key if direction == "backward" else None,
                participant_ids=participant_ids,
                time_after_utc=time_after,
                time_before_utc=time_before,
            )
            scanned_rows = candidates
            matching = [row for row in candidates if self._text_matches(row["text"], query)]
            raw_may_continue = len(candidates) == fetch_limit
            if direction == "backward":
                focus_rows = matching[-limit:]
                has_more_before = raw_may_continue or len(matching) > len(focus_rows)
            else:
                focus_rows = matching[:limit]
                has_more_after = raw_may_continue or len(matching) > len(focus_rows)
            by_id = {str(row["message_id"]): row for row in focus_rows}
            context_ids: set[str] = set()
            if speaker_view == "with_context":
                for focus in focus_rows:
                    if before:
                        neighbors = self.repository.materialized_message_rows(
                            conversation,
                            projection_epoch=epoch,
                            observation_watermark=watermark,
                            limit=before,
                            direction="backward",
                            before=self._row_sort_key(focus),
                            continuity_with=self._row_sort_key(focus),
                        )
                        for row in neighbors:
                            by_id.setdefault(str(row["message_id"]), row)
                            context_ids.add(str(row["message_id"]))
                    if after:
                        neighbors = self.repository.materialized_message_rows(
                            conversation,
                            projection_epoch=epoch,
                            observation_watermark=watermark,
                            limit=after,
                            direction="forward",
                            after=self._row_sort_key(focus),
                            continuity_with=self._row_sort_key(focus),
                        )
                        for row in neighbors:
                            by_id.setdefault(str(row["message_id"]), row)
                            context_ids.add(str(row["message_id"]))
                context_ids.difference_update(str(row["message_id"]) for row in focus_rows)
            rows = sorted(by_id.values(), key=lambda row: self._row_sort_key(row).as_tuple())
            context_only_ids = frozenset(context_ids)

        focus_ids = frozenset(str(row["message_id"]) for row in focus_rows)
        return self._finish_materialized_page(
            target=target,
            mode=mode,
            projection=projection,
            include_resources=include_resources,
            system_policy=system_policy,
            participant_ids=participant_ids,
            speaker_view=speaker_view,
            direction=direction,
            scope_kind=scope_kind,
            scope_key=scope_key,
            observation_watermark=watermark,
            rows=rows,
            scanned_rows=scanned_rows or focus_rows,
            focus_message_ids=focus_ids,
            context_only_ids=context_only_ids,
            has_more_before=has_more_before,
            has_more_after=has_more_after,
            voice_policy=voice_policy,
        )

    def _live_target_scope(
        self,
        *,
        mode: str,
        conversation_id: str | None,
        message_id: str | None,
        anchor: str | None,
    ) -> tuple[SourceScope, str] | None:
        """Resolve the exact dependency scope for one already-admitted target read.

        A live provider can refresh one persisted conversation or message without
        rebuilding the account catalog. Returns ``(scope, internal_account_id)`` for
        that case, or ``None`` when the operation genuinely needs catalog scope (a
        non-live provider, an unadmitted target, or a
        policy denial the catalog path must report).
        """

        if self.provider.descriptor.source_mode != "live":
            return None
        target_conversation_id = conversation_id
        if mode == "context" and anchor is not None:
            position = self._local_anchor_row(anchor, conversation_id)
            target_conversation_id = str(position["conversation_id"])
        if not target_conversation_id and message_id and mode in {"message", "context"}:
            position = self.repository.message_position_row(message_id)
            if position is None:
                return None
            target_conversation_id = str(position["conversation_id"])
        if not target_conversation_id:
            return None
        context = self.repository.conversation_context(target_conversation_id)
        if context is None or not self.reader.policy.permits(target_conversation_id):
            return None
        account_key = str(context["source_account_key"])
        source_conversation_id = str(context["source_conversation_id"])
        if mode == "message" and message_id:
            position = self.repository.message_position_row(message_id)
            if position is None:
                return None
            scope = SourceScope.message(
                account_key,
                str(position["source_message_id"]),
                conversation_source_id=source_conversation_id,
            )
        else:
            scope = SourceScope.conversation(account_key, source_conversation_id)
        return scope, str(context["account_id"])

    def _validate_message_arguments(
        self,
        *,
        mode: str,
        conversation_id: str | None = None,
        message_id: str | None = None,
        anchor: str | None = None,
        before: int = 30,
        after: int = 20,
        limit: int | None = None,
        direction: str = "backward",
        cursor: str | None = None,
        ack_delivery_id: str | None = None,
        participant_ids: tuple[str, ...] = (),
        speaker_view: str = "only",
        time_after: str | None = None,
        time_before: str | None = None,
        query: str | None = None,
        projection: str | None = None,
        include_resources: str | None = None,
        system_policy: str = "include",
        strict: bool = True,
        voice: str | None = None,
        refresh: bool = False,
        view: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        if type(refresh) is not bool or (refresh and (mode == "updates" or cursor is not None)):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        resolved_view = self.resolve_view(view, refresh=refresh)
        if request_id is not None and mode != "updates":
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if request_id is not None and (
            not isinstance(request_id, str)
            or re.fullmatch(r"[A-Za-z0-9_-]{8,128}", request_id) is None
        ):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        voice_policy = self._voice_policy(voice)
        if resolved_view == "replica" and voice_policy == "auto":
            voice_policy = "cached"
        if not strict:
            raise SightglassError(
                ErrorCode.QUERY_INVALID,
                details={"reason": "M2 supports strict source reads only"},
            )
        if mode not in {"recent", "context", "updates", "range", "message", "speaker"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        projection = projection or ("detail" if mode == "message" else "compact")
        if projection not in {"compact", "detail"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        include_resources = include_resources or (
            "indicator" if projection == "compact" else "metadata"
        )
        if query is not None and not self._query_parts(query):
            query = None
        if anchor is not None and mode != "context":
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if message_id is not None and mode not in {"message", "context"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if cursor is not None and mode not in {"recent", "range", "speaker"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if participant_ids and mode not in {"speaker", "updates"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if query and mode not in {"speaker", "updates"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if (time_after or time_before) and mode not in {"range", "speaker"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if speaker_view != "only" and mode != "speaker":
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if direction != "backward" and mode in {"context", "message", "updates"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if mode == "recent" and direction != "backward":
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if mode not in {"context", "speaker"} and (before != 30 or after != 20):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        try:
            time_after = to_utc_iso(time_after) if time_after else None
            time_before = to_utc_iso(time_before) if time_before else None
        except ValueError as exc:
            raise SightglassError(ErrorCode.QUERY_INVALID) from exc
        valid_resource_projection = (
            projection == "compact" and include_resources in {"none", "indicator"}
        ) or (projection == "detail" and include_resources in {"none", "metadata"})
        if not valid_resource_projection:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if direction not in {"forward", "backward"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if before < 0 or after < 0:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        effective_limit = limit
        if effective_limit is None:
            if mode == "message":
                effective_limit = 1
            elif projection == "detail":
                effective_limit = self.reader.policy.max_detail_messages_per_call
            else:
                effective_limit = 30 if mode == "recent" else 100
        bounded = self.reader.bound_message_limit(effective_limit, projection=projection, mode=mode)
        if mode == "updates" and (not conversation_id or anchor or message_id or cursor):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if ack_delivery_id is not None and mode != "updates":
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if mode == "context" and before + after + 1 > bounded:
            raise SightglassError(
                ErrorCode.QUERY_INVALID,
                details={"reason": "context window exceeds requested limit"},
            )
        if mode == "speaker" and (
            not participant_ids or speaker_view not in {"only", "with_context"}
        ):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if mode == "range" and cursor is None and time_after is None and time_before is None:
            raise SightglassError(
                ErrorCode.QUERY_INVALID,
                details={"reason": "range requires cursor or absolute time boundary"},
            )

        participant_ids = tuple(sorted(set(participant_ids)))
        scope_kind, scope_key = cursor_scope(
            participant_ids if mode == "speaker" else (),
            query if mode == "speaker" else None,
            time_after if mode in {"range", "speaker"} else None,
            time_before if mode in {"range", "speaker"} else None,
            system_policy,
        )
        if resolved_view == "auto" and view is None and cursor:
            payload = self.token_codec.decode(cursor)
            # Legacy local callers continue a refreshed source page by passing
            # only its signed cursor. Preserve that default-auto call shape while
            # explicit views and the replica default retain strict view binding.
            if payload.get("kind") == "timeline" and payload.get("view") == "fresh":
                resolved_view = "fresh"
        return {
            "projection": projection, "include_resources": include_resources,
            "query": query, "time_after": time_after, "time_before": time_before,
            "bounded": bounded, "voice_policy": voice_policy,
            "resolved_view": resolved_view, "participant_ids": participant_ids,
            "scope_kind": scope_kind, "scope_key": scope_key,
        }

    def read_messages(
        self,
        *,
        mode: str,
        conversation_id: str | None = None,
        message_id: str | None = None,
        anchor: str | None = None,
        before: int = 30,
        after: int = 20,
        limit: int | None = None,
        direction: str = "backward",
        cursor: str | None = None,
        ack_delivery_id: str | None = None,
        participant_ids: tuple[str, ...] = (),
        speaker_view: str = "only",
        time_after: str | None = None,
        time_before: str | None = None,
        query: str | None = None,
        projection: str | None = None,
        include_resources: str | None = None,
        system_policy: str = "include",
        strict: bool = True,
        voice: str | None = None,
        refresh: bool = False,
        view: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        settings = self._validate_message_arguments(
            mode=mode, conversation_id=conversation_id, message_id=message_id, anchor=anchor,
            before=before, after=after, limit=limit, direction=direction, cursor=cursor,
            ack_delivery_id=ack_delivery_id, participant_ids=participant_ids,
            speaker_view=speaker_view, time_after=time_after, time_before=time_before,
            query=query, projection=projection, include_resources=include_resources,
            system_policy=system_policy, strict=strict, voice=voice, refresh=refresh,
            view=view, request_id=request_id,
        )
        projection = str(settings["projection"])
        include_resources = str(settings["include_resources"])
        query = settings["query"]
        time_after, time_before = settings["time_after"], settings["time_before"]
        bounded, voice_policy = settings["bounded"], settings["voice_policy"]
        resolved_view, participant_ids = settings["resolved_view"], settings["participant_ids"]
        scope_kind, scope_key = settings["scope_kind"], settings["scope_key"]
        if mode == "updates":
            assert conversation_id is not None
            return self._read_updates(
                projection=projection, conversation_id=conversation_id,
                participant_ids=participant_ids, query=query,
                ack_delivery_id=ack_delivery_id, limit=bounded,
                include_resources=include_resources, system_policy=system_policy,
                voice_policy=voice_policy, view=resolved_view, request_id=request_id,
            )
        if self._operation_provider.get() is not None:
            before, after, bounded, _fetch = self.replica.capture_message_bounds(
                mode=mode, before=before, after=after, bounded=bounded,
                speaker_view=speaker_view, query=query,
                cursor=bool(cursor),
            )
        if resolved_view != "replica":
            self.residency.release_expired_leases()
        materialized = None
        if resolved_view == "fresh":
            if cursor and self.token_codec.decode(cursor).get("kind") != "timeline":
                raise SightglassError(ErrorCode.CURSOR_INVALID)
            # Keep the same local target/signature/policy validation. Explicit
            # refresh changes evidence acquisition, never target authorization.
            self._materialized_target(
                mode=mode,
                conversation_id=conversation_id,
                message_id=message_id,
                anchor=anchor,
            )
        else:
            materialized = self._read_materialized_messages(
                mode=mode,
                conversation_id=conversation_id,
                message_id=message_id,
                anchor=anchor,
                before=before,
                after=after,
                limit=bounded,
                direction=direction,
                cursor=cursor,
                participant_ids=participant_ids,
                speaker_view=speaker_view,
                time_after=time_after,
                time_before=time_before,
                query=query,
                projection=projection,
                include_resources=include_resources,
                system_policy=system_policy,
                scope_kind=scope_kind,
                scope_key=scope_key,
                voice_policy=voice_policy,
                view=resolved_view,
            )
        if materialized is not None:
            return materialized
        # Foreground admission may reclaim expired disposable read leases first.
        self.residency.release_expired_leases()
        live_scope = self._live_target_scope(
            mode=mode,
            conversation_id=conversation_id,
            message_id=message_id,
            anchor=anchor,
        )
        if live_scope is not None:
            scope, scope_account_id = live_scope
            source_context = self._source_session(
                scope,
                accounted_generations=self.repository.source_shard_generations(scope_account_id),
            )
        else:
            source_context = self._source_read()
        with source_context as (stack, snapshot):
            live_target_fast_path = live_scope is not None
            if live_target_fast_path:
                catalog_read = _CatalogRead((), ())
                facts = None
            else:
                catalog_read = self._read_catalog(snapshot)
                facts = self._catalog_facts(snapshot)
            source_row = None
            target_source: SourceMessage | None = None
            target: _SourceTarget
            if mode == "context" and anchor:
                target, source_row, target_source = self._anchor_target(
                    anchor, conversation_id, catalog_read, snapshot
                )
                context = target.as_read_context()
                if message_id and message_id != str(source_row["message_id"]):
                    raise SightglassError(ErrorCode.CURSOR_INVALID)
                message_id = str(source_row["message_id"])
                conversation_id = str(source_row["conversation_id"])
            elif mode in {"message", "context"}:
                if not message_id:
                    raise SightglassError(ErrorCode.QUERY_INVALID)
                source_row = self.repository.message_position_row(message_id)
                if source_row is None:
                    raise SightglassError(
                        ErrorCode.MESSAGE_NOT_FOUND,
                        details={"coverage": {"state": "not_yet_observed"}},
                    )
                if conversation_id and conversation_id != str(source_row["conversation_id"]):
                    raise SightglassError(ErrorCode.MESSAGE_NOT_FOUND)
                conversation_id = str(source_row["conversation_id"])
                target = self._source_target(catalog_read, conversation_id)
                context = target.as_read_context()
            else:
                if not conversation_id:
                    raise SightglassError(ErrorCode.QUERY_INVALID)
                target = self._source_target(catalog_read, conversation_id)
                context = target.as_read_context()
            if facts is None:
                facts = self._persisted_catalog_facts(target, snapshot)
            participants = self._read_message_participants(target, snapshot)
            cursor_row = None
            if cursor:
                cursor_row = self._timeline_boundary(
                    cursor=cursor,
                    context=context,
                    mode=mode,
                    direction=direction,
                    scope_kind=scope_kind,
                    scope_key=scope_key,
                    snapshot=snapshot,
                    view=resolved_view,
                )
            after_key = (
                source_sort_key(cursor_row) if cursor_row and direction == "forward" else None
            )
            before_key = (
                source_sort_key(cursor_row) if cursor_row and direction == "backward" else None
            )

            validated_source_windows: list[tuple[SourceSortKey, SourceSortKey]] = []
            scan_boundary_source: SourceMessage | None = None
            focus_sources: tuple[SourceMessage, ...]
            combined_sources: tuple[SourceMessage, ...]
            context_only_sources: set[str] = set()
            has_more_before = False
            has_more_after = False
            if mode == "recent":
                if cursor:
                    source_page = self.provider.read_range(
                        str(context["source_account_key"]),
                        str(context["source_conversation_id"]),
                        after=after_key,
                        before=before_key,
                        direction=direction,
                        limit=bounded,
                        snapshot=snapshot,
                    )
                else:
                    source_page = self.provider.read_recent(
                        str(context["source_account_key"]),
                        str(context["source_conversation_id"]),
                        bounded,
                        snapshot,
                    )
                focus_sources = combined_sources = source_page.messages
                has_more_before = source_page.has_more_before
                has_more_after = source_page.has_more_after
            elif mode == "message":
                assert source_row is not None
                target_source = self.provider.get_message(
                    str(context["source_account_key"]),
                    str(source_row["source_message_id"]),
                    snapshot,
                )
                if target_source is None:
                    raise SightglassError(
                        ErrorCode.MESSAGE_NOT_FOUND,
                        details={"coverage": self._coverage(context, facts).as_dict()},
                    )
                focus_sources = combined_sources = (target_source,)
            elif mode == "context":
                assert source_row is not None
                if target_source is None:
                    target_source = self.provider.get_message(
                        str(context["source_account_key"]),
                        str(source_row["source_message_id"]),
                        snapshot,
                    )
                if target_source is None:
                    raise SightglassError(
                        ErrorCode.CURSOR_STALE if anchor else ErrorCode.MESSAGE_NOT_FOUND
                    )
                focus_sources = (target_source,)
                if isinstance(self.provider, ContextSourceProvider):
                    source_page = self.provider.read_context(
                        str(context["source_account_key"]),
                        str(context["source_conversation_id"]),
                        focus=target_source,
                        before=before,
                        after=after,
                        snapshot=snapshot,
                    )
                    combined_sources = source_page.messages
                    has_more_before = source_page.has_more_before
                    has_more_after = source_page.has_more_after
                else:
                    before_page = (
                        self.provider.read_range(
                            str(context["source_account_key"]),
                            str(context["source_conversation_id"]),
                            after=None,
                            before=target_source.sort_key,
                            direction="backward",
                            limit=before,
                            snapshot=snapshot,
                        )
                        if before
                        else SourceMessagePage(())
                    )
                    after_page = (
                        self.provider.read_range(
                            str(context["source_account_key"]),
                            str(context["source_conversation_id"]),
                            after=target_source.sort_key,
                            before=None,
                            direction="forward",
                            limit=after,
                            snapshot=snapshot,
                        )
                        if after
                        else SourceMessagePage(())
                    )
                    combined_sources = (*before_page.messages, target_source, *after_page.messages)
                    has_more_before = before_page.has_more_before
                    has_more_after = after_page.has_more_after
                context_only_sources.update(
                    item.source_message_id
                    for item in combined_sources
                    if item.source_message_id != target_source.source_message_id
                )
            elif mode == "range":
                source_page = self.provider.read_range(
                    str(context["source_account_key"]),
                    str(context["source_conversation_id"]),
                    after=after_key,
                    before=before_key,
                    direction=direction,
                    limit=bounded,
                    snapshot=snapshot,
                    time_after_utc=time_after,
                    time_before_utc=time_before,
                )
                focus_sources = combined_sources = source_page.messages
                has_more_before = source_page.has_more_before
                has_more_after = source_page.has_more_after
            else:
                filters = self.repository.participant_source_filters(
                    conversation_id, participant_ids
                )
                fetch_limit = 10_001 if query else bounded
                if self._operation_provider.get() is not None:
                    _, _, _, fetch_limit = self.replica.capture_message_bounds(
                        mode=mode, before=before, after=after, bounded=bounded,
                        speaker_view=speaker_view, query=query,
                        cursor=bool(cursor),
                    )
                source_page = self.provider.read_range(
                    str(context["source_account_key"]),
                    str(context["source_conversation_id"]),
                    after=after_key,
                    before=before_key,
                    direction=direction,
                    limit=fetch_limit,
                    snapshot=snapshot,
                    participant_source_ids=filters,
                    time_after_utc=time_after,
                    time_before_utc=time_before,
                )
                if source_page.messages:
                    scan_boundary_source = (
                        source_page.messages[0]
                        if direction == "backward"
                        else source_page.messages[-1]
                    )
                focus_candidates = [
                    item
                    for item in source_page.messages
                    if self._text_matches(parse_message(item).text, query)
                ]
                if direction == "backward":
                    focus_sources = tuple(focus_candidates[-bounded:])
                else:
                    focus_sources = tuple(focus_candidates[:bounded])
                filtered_more = len(focus_candidates) > len(focus_sources)
                has_more_before = source_page.has_more_before or (
                    direction == "backward" and filtered_more
                )
                has_more_after = source_page.has_more_after or (
                    direction == "forward" and filtered_more
                )
                source_by_id = {item.source_message_id: item for item in focus_sources}
                if speaker_view == "with_context":
                    for focus in focus_sources:
                        check_operation_budget()
                        neighborhood = [focus]
                        if before:
                            neighbor_page = self.provider.read_range(
                                str(context["source_account_key"]),
                                str(context["source_conversation_id"]),
                                after=None,
                                before=focus.sort_key,
                                direction="backward",
                                limit=before,
                                snapshot=snapshot,
                            )
                            neighborhood.extend(neighbor_page.messages)
                            for item in neighbor_page.messages:
                                source_by_id.setdefault(item.source_message_id, item)
                                context_only_sources.add(item.source_message_id)
                        if after:
                            neighbor_page = self.provider.read_range(
                                str(context["source_account_key"]),
                                str(context["source_conversation_id"]),
                                after=focus.sort_key,
                                before=None,
                                direction="forward",
                                limit=after,
                                snapshot=snapshot,
                            )
                            neighborhood.extend(neighbor_page.messages)
                            for item in neighbor_page.messages:
                                source_by_id.setdefault(item.source_message_id, item)
                                context_only_sources.add(item.source_message_id)
                        validated_source_windows.append(
                            (
                                min(
                                    neighborhood, key=lambda item: item.sort_key.as_tuple()
                                ).sort_key,
                                max(
                                    neighborhood, key=lambda item: item.sort_key.as_tuple()
                                ).sort_key,
                            )
                        )
                    context_only_sources.difference_update(
                        item.source_message_id for item in focus_sources
                    )
                combined_sources = tuple(
                    sorted(source_by_id.values(), key=lambda item: item.sort_key.as_tuple())
                )

            projection_maximum = (
                self.reader.policy.max_compact_messages_per_call
                if projection == "compact"
                else self.reader.policy.max_detail_messages_per_call
            )
            if len(combined_sources) > projection_maximum:
                raise SightglassError(ErrorCode.OUTPUT_BUDGET_EXCEEDED)
            visible_source_ids = {item.source_message_id for item in combined_sources}
            if scan_boundary_source is None and focus_sources:
                scan_boundary_source = (
                    focus_sources[0] if direction == "backward" else focus_sources[-1]
                )
            admission_sources = combined_sources
            if (
                scan_boundary_source is not None
                and scan_boundary_source.source_message_id not in visible_source_ids
            ):
                admission_sources = (*combined_sources, scan_boundary_source)
            prepared_messages = self._prepare_messages(admission_sources)
            with self._admission(stack):
                self._persist_catalog(catalog_read, snapshot)
                context = self._conversation_context(conversation_id)
                self._index_participants(context, participants, snapshot)
                admitted_message_ids = self._ingest_prepared_messages(context, prepared_messages)
                if mode == "recent" and self._residency_decision(conversation_id).mode != "keep":
                    self.residency.record_foreground_lease(
                        conversation_id=conversation_id,
                        scope_key="recent:" + self.reader.reader_id,
                        message_ids=admitted_message_ids,
                        projection_epoch=self._projection_inventory_epoch(),
                        settings=self._residency_settings(),
                    )
                if combined_sources and mode != "speaker":
                    lower = min(
                        combined_sources, key=lambda item: item.sort_key.as_tuple()
                    ).sort_key
                    upper = max(
                        combined_sources, key=lambda item: item.sort_key.as_tuple()
                    ).sort_key
                    if cursor_row is not None and mode in {"recent", "range"}:
                        # The signed source boundary was reconciled in this same
                        # session and joins the exclusive page to its earlier window.
                        anchor_key = source_sort_key(cursor_row)
                        lower = min(lower, anchor_key, key=lambda key: key.as_tuple())
                        upper = max(upper, anchor_key, key=lambda key: key.as_tuple())
                    validated_source_windows.append((lower, upper))
                self._record_source_windows(conversation_id, snapshot, validated_source_windows)
                if (
                    mode == "recent"
                    and cursor is None
                    and combined_sources
                    and self.provider.descriptor.source_mode == "live"
                    and self.provider.descriptor.supports_incremental
                ):
                    self._record_recent_source_state(
                        conversation_id, snapshot, source_page, combined_sources
                    )
                source_to_message = dict(
                    zip(
                        (item.source_message_id for item in admission_sources),
                        admitted_message_ids,
                        strict=True,
                    )
                )
                focus_message_ids = tuple(
                    source_to_message[item.source_message_id] for item in focus_sources
                )
                context_only_ids = frozenset(
                    source_to_message[source_id]
                    for source_id in context_only_sources
                    if source_id in source_to_message
                )

            scan_boundary_row = (
                self.repository.message_position_row(
                    source_to_message[scan_boundary_source.source_message_id]
                )
                if scan_boundary_source is not None
                else None
            )
            projected_message_ids = tuple(
                source_to_message[source_id] for source_id in visible_source_ids
            )
            rows = (
                self.repository.message_rows(
                    conversation_id,
                    limit=max(1, len(projected_message_ids)),
                    direction="forward",
                    message_ids=projected_message_ids,
                )
                if projected_message_ids
                else []
            )
            rows, hidden_system_count = self._apply_system_policy(rows, system_policy)
            voice_candidates = self._voice_candidates(
                voice_policy, tuple(str(row["message_id"]) for row in rows)
            )
            voice_reserve = self._voice_reserve(voice_policy, voice_candidates)
            focus_message_id_set = frozenset(focus_message_ids)
            visible_message_ids = frozenset(str(row["message_id"]) for row in rows)
            context_only_ids = frozenset(context_only_ids & visible_message_ids)
            focus_message_id_set = frozenset(focus_message_id_set & visible_message_ids)
            if projection == "detail":
                rows, projected_detail, text_truncated = self._prepare_detail_rows(
                    rows,
                    context=context,
                    direction=direction,
                    include_resources=include_resources,
                    participant_ids=participant_ids if mode == "speaker" else (),
                    context_only_ids=context_only_ids,
                    voice_reserve_chars=voice_reserve,
                )
                if text_truncated:
                    if direction == "backward":
                        has_more_before = True
                    else:
                        has_more_after = True
                visible_message_ids = frozenset(str(row["message_id"]) for row in rows)
                context_only_ids = frozenset(context_only_ids & visible_message_ids)
                focus_message_id_set = frozenset(focus_message_id_set & visible_message_ids)
                focus_rows = [row for row in rows if str(row["message_id"]) in focus_message_id_set]
                has_more = has_more_before if direction == "backward" else has_more_after
                next_cursor = (
                    self._next_cursor(
                        rows=focus_rows
                        or ([scan_boundary_row] if scan_boundary_row is not None else []),
                        context=context,
                        mode=mode,
                        direction=direction,
                        scope_kind=scope_kind,
                        scope_key=scope_key,
                        snapshot=snapshot,
                        has_more=has_more,
                        view=resolved_view,
                    )
                    if mode in {"recent", "range", "speaker"}
                    else None
                )
                page_result = self._page_response(
                    projection=projection,
                    mode=mode,
                    context=context,
                    rows=rows,
                    snapshot=snapshot,
                    facts=facts,
                    has_more_before=has_more_before,
                    has_more_after=has_more_after,
                    participant_ids=participant_ids if mode == "speaker" else (),
                    speaker_view=speaker_view if mode == "speaker" else None,
                    include_resources=include_resources,
                    system_policy=system_policy,
                    focus_message_ids=focus_message_id_set,
                    context_only_ids=context_only_ids,
                    next_cursor=next_cursor,
                    truncated=has_more_before or has_more_after or text_truncated,
                    hidden_system_count=hidden_system_count,
                    projected_detail=projected_detail,
                    voice_reserve_chars=voice_reserve,
                )
                if mode in {"recent", "range", "speaker"} and focus_rows:
                    committed_row = max(
                        focus_rows,
                        key=lambda row: source_sort_key(row).as_tuple(),
                    )
                    self.repository.commit_timeline_position(
                        reader_id=self.reader.reader_id,
                        conversation_id=conversation_id,
                        scope_kind=scope_kind,
                        scope_key=scope_key,
                        row=committed_row,
                        updated_at=snapshot.fresh_as_of,
                        seed_update_cursor=True,
                        admitted_message_ids=tuple(str(row["message_id"]) for row in focus_rows),
                    )
                if resolved_view == "fresh":
                    page_result["source_receipt"]["view"] = "fresh"
                return self._attach_voice(
                    page_result,
                    policy=voice_policy,
                    candidates=voice_candidates,
                    rows=rows,
                    conversation_id=conversation_id,
                    projection="detail",
                    focus_message_ids=focus_message_id_set,
                )

            prepared = self.compact_projector.prepare(
                rows,
                timezone_name=self._reader_timezone(context),
                include_resource_indicators=include_resources == "indicator",
                focus_message_ids=focus_message_id_set,
                context_only_ids=context_only_ids,
                late_arrival_ids=frozenset(),
            )

            def build_compact_page(selected: slice, message_rows_complete: bool) -> dict[str, Any]:
                candidate_rows = rows[selected]
                candidate_prepared = prepared[selected]
                candidate_ids = frozenset(str(row["message_id"]) for row in candidate_rows)
                candidate_focus_ids = frozenset(focus_message_id_set & candidate_ids)
                candidate_context_ids = frozenset(context_only_ids & candidate_ids)
                candidate_focus_rows = [
                    row for row in candidate_rows if str(row["message_id"]) in candidate_focus_ids
                ]
                candidate_more_before = has_more_before or (
                    not message_rows_complete and direction == "backward"
                )
                candidate_more_after = has_more_after or (
                    not message_rows_complete and direction == "forward"
                )
                candidate_has_more = (
                    candidate_more_before if direction == "backward" else candidate_more_after
                )
                candidate_cursor = (
                    self._next_cursor(
                        rows=candidate_focus_rows
                        or ([scan_boundary_row] if scan_boundary_row is not None else []),
                        context=context,
                        mode=mode,
                        direction=direction,
                        scope_kind=scope_kind,
                        scope_key=scope_key,
                        snapshot=snapshot,
                        has_more=candidate_has_more,
                        view=resolved_view,
                    )
                    if mode in {"recent", "range", "speaker"}
                    else None
                )
                return self._page_response(
                    projection="compact",
                    mode=mode,
                    context=context,
                    rows=candidate_rows,
                    snapshot=snapshot,
                    facts=facts,
                    has_more_before=candidate_more_before,
                    has_more_after=candidate_more_after,
                    participant_ids=participant_ids if mode == "speaker" else (),
                    speaker_view=speaker_view if mode == "speaker" else None,
                    include_resources=include_resources,
                    system_policy=system_policy,
                    focus_message_ids=candidate_focus_ids,
                    context_only_ids=candidate_context_ids,
                    next_cursor=candidate_cursor,
                    hidden_system_count=hidden_system_count,
                    compact_prepared=candidate_prepared,
                    message_rows_complete=message_rows_complete,
                    voice_reserve_chars=voice_reserve,
                )

            selected, page_result = self._fit_compact_page(
                len(rows), direction=direction, build=build_compact_page
            )
            rows = rows[selected]
            selected_ids = frozenset(str(row["message_id"]) for row in rows)
            focus_message_id_set = frozenset(focus_message_id_set & selected_ids)
            focus_rows = [row for row in rows if str(row["message_id"]) in focus_message_id_set]
            if mode in {"recent", "range", "speaker"} and focus_rows:
                committed_row = max(
                    focus_rows,
                    key=lambda row: source_sort_key(row).as_tuple(),
                )
                self.repository.commit_timeline_position(
                    reader_id=self.reader.reader_id,
                    conversation_id=conversation_id,
                    scope_kind=scope_kind,
                    scope_key=scope_key,
                    row=committed_row,
                    updated_at=snapshot.fresh_as_of,
                    seed_update_cursor=True,
                    admitted_message_ids=tuple(str(row["message_id"]) for row in focus_rows),
                )
            if resolved_view == "fresh":
                page_result["source_receipt"]["view"] = "fresh"
            return self._attach_voice(
                page_result,
                policy=voice_policy,
                candidates=voice_candidates,
                rows=rows,
                conversation_id=conversation_id,
                projection="compact",
                focus_message_ids=focus_message_id_set,
            )

    @staticmethod
    def _search_keyset(row: Any) -> tuple[str, int, int, str]:
        """Opaque ordered scan key of one durable-index candidate row."""

        return (
            str(row["sort_primary"]),
            int(row["sort_seq"]),
            int(row["sort_tie"]),
            str(row["message_id"]),
        )

    @staticmethod
    def _search_text_matches(row: Any, query_parts: tuple[str, ...]) -> bool:
        """Term prefilter over the durable row's own searchable text.

        Uses the same Unicode casefold and literal terms as final canonical matching.
        It runs in Python so the
        candidate scan stays bounded to the ordered keyset window instead of scanning
        the whole body history for an absent term. Final delivery still re-validates
        each hit against the parsed current-source message.
        """

        if not query_parts:
            return True
        # One canonical owner of the recall document (card search text, else the
        # body) so strict prefiltering and final canonical matching cannot diverge.
        value = current_search_document(row).casefold()
        return all(part in value for part in query_parts)

    def _search_scope(
        self,
        snapshot: SourceSnapshot,
        account_id: str | None,
        conversation_ids: tuple[str, ...],
    ) -> tuple[_CatalogRead, str, _CatalogFacts, tuple[str, ...], dict[str, _SourceTarget]]:
        """Resolve one authorized search scope without opening a writer transaction."""

        catalog_read = self._read_catalog(snapshot)
        external_account_id, _source_key = self._account_source_key(
            catalog_read.mappings, account_id
        )
        facts = self._catalog_facts(snapshot)
        available: dict[str, Any] = {
            str(row["conversation_id"]): row
            for row in self.repository.account_conversations(external_account_id)
        }
        for entry in catalog_read.catalog:
            if entry.account_id == external_account_id:
                available.setdefault(entry.conversation_id, entry)
        selected_ids = tuple(dict.fromkeys(conversation_ids or tuple(available)))
        if any(value not in available for value in selected_ids):
            raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        selected_ids = tuple(
            value for value in selected_ids if self.reader.policy.permits(value)
        )
        if conversation_ids and len(selected_ids) != len(
            tuple(dict.fromkeys(conversation_ids))
        ):
            raise SightglassError(ErrorCode.POLICY_DENIED)
        targets = {value: self._source_target(catalog_read, value) for value in selected_ids}
        return catalog_read, external_account_id, facts, selected_ids, targets

    def _index_search_roster(
        self, account_id: str | None, conversation_ids: tuple[str, ...]
    ) -> tuple[str, str]:
        """Index the selected conversation rosters so a sender query can resolve.

        A ``sender_query`` is bound into the signed cursor as the canonical
        participant it resolves to, so the member index must exist before the scan
        binds its scope. This is one short, snapshot-validated write and the only
        search path that reads conversation rosters. It returns the source binding
        it resolved against so the caller can refuse to scan under any other one.
        """

        with self._source_read() as (stack, snapshot):
            catalog_read, _account_id, _facts, selected_ids, targets = self._search_scope(
                snapshot, account_id, conversation_ids
            )
            source_participants = {
                value: self._read_participants(targets[value], snapshot) for value in selected_ids
            }
            with self._admission(stack):
                self._persist_catalog(catalog_read, snapshot)
                for value in selected_ids:
                    context = self._conversation_context(value)
                    self._index_participants(context, source_participants[value], snapshot)
            return snapshot.inventory_digest, snapshot.generation_set_digest

    def _search_sender_candidates(
        self, selected_ids: tuple[str, ...], sender_query: str
    ) -> dict[str, dict[str, Any]]:
        candidates: dict[str, dict[str, Any]] = {}
        for conversation_id in selected_ids:
            for item in self.repository.participant_candidates(conversation_id, sender_query):
                candidates.setdefault(str(item["participant_id"]), item)
        return candidates

    def _verify_search_cursor(
        self, cursor: str, *, account_id: str, scope_key: str, snapshot: SourceSnapshot
    ) -> tuple[str, int, int, str]:
        payload = self.search_cursors.verify(
            cursor, reader_id=self.reader.reader_id, account_id=account_id, scope_key=scope_key
        )
        source = payload["source"]
        if (
            source["inventory_digest"] != snapshot.inventory_digest
            or source["generation_set_digest"] != snapshot.generation_set_digest
            or source.get("projection_epoch") != self._projection_inventory_epoch()
        ):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        sort = payload["position"]["sort"]
        return str(sort[0]), int(sort[1]), int(sort[2]), str(sort[3])

    def _prepare_search_candidates(
        self, *, query: str = "", account_id: str | None, conversation_ids: tuple[str, ...],
        participant_ids: tuple[str, ...], sender_query: str | None,
        after_utc: str | None, before_utc: str | None, limit: int,
        roster_binding: tuple[str, str] | None,
        incremental: bool = False,
        checkpoint: dict[str, Any] | None = None,
        progress: Callable[[dict[str, Any]], None] | None = None,
        check_authority: Callable[[], None] | None = None,
    ) -> tuple[dict[str, Any], dict[str, tuple[str, str]]]:
        """Admit one bounded source window in the requested search scope.

        Catalog resolution finishes before any conversation lease opens. Each page
        uses the ordinary canonical range/recent provider and admission path; this
        operation never advances source rotation or reader traversal/delivery state.
        """
        with self._source_read() as (stack, snapshot):
            if roster_binding is not None and roster_binding != (
                snapshot.inventory_digest, snapshot.generation_set_digest
            ):
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
            catalog, external_account_id, _facts, selected_ids, targets = self._search_scope(
                snapshot, account_id, conversation_ids
            )
            if sender_query and not participant_ids:
                candidates = self._search_sender_candidates(selected_ids, sender_query)
                if len(candidates) != 1:
                    return {"performed": False}, {}
                participant_ids = (next(iter(candidates)),)
            for participant_id in participant_ids:
                if self.repository.participant_account_id(participant_id) != external_account_id:
                    raise SightglassError(ErrorCode.PARTICIPANT_OUT_OF_SCOPE)
            with self._admission(stack):
                self._persist_catalog(catalog, snapshot)

        chosen = selected_ids[:SEARCH_PREPARATION_CONVERSATION_BUDGET]
        remaining = SEARCH_PREPARATION_MESSAGE_BUDGET
        messages_admitted = 0
        pending = 0
        full_bindings: dict[str, tuple[str, str]] = {}
        generations: dict[str, list[list[str]]] = {}
        bounded_range = after_utc is not None or before_utc is not None
        saved = checkpoint or {}
        done = int(saved.get("done", 0)) if saved.get("chosen") == list(chosen) else 0
        if done:
            messages_admitted = int(saved["message_count"])
            pending = int(saved["pending_conversation_count"])
            full_bindings = {key: tuple(value) for key, value in saved.get("bindings", {}).items()}
            generations = saved.get("generations", {})
        state = {"chosen": list(chosen), "done": done, "message_count": messages_admitted,
                 "pending_conversation_count": pending, "bindings": full_bindings,
                 "generations": generations}
        if progress is not None:
            progress({"checkpoint": state})
        for index, conversation_id in enumerate(chosen):
            check_operation_budget()
            target = targets[conversation_id]
            page_limit = min(
                (max(50, limit) if bounded_range else SEARCH_PREPARATION_MESSAGE_BUDGET),
                remaining // (len(chosen) - index),
            )
            if index < done:
                remaining -= page_limit
                continue
            if check_authority is not None:
                check_authority()
            scope = SourceScope.conversation(
                target.source_account_key, target.source_conversation_id
            )
            with self._source_session(scope) as (stack, snapshot):
                if incremental:
                    iterator = self.provider.prepare_search_page(
                        target.source_account_key, target.source_conversation_id,
                        direction="forward" if bounded_range else "backward", limit=page_limit,
                        snapshot=snapshot, time_after_utc=after_utc, time_before_utc=before_utc,
                        batch_size=1024, check_authority=check_authority,
                    )
                    page = None
                    try:
                        while True:
                            # The outer worker bounds the attempt; each SQL quantum
                            # also remains cancellable and cannot inherit 120s SQL.
                            with operation_budget(2.0):
                                step = next(iterator)
                            if progress is not None:
                                progress({"phase": step.phase, "scanned_rows": step.scanned_rows,
                                          "completed_shards": step.completed_shards,
                                          "total_shards": step.total_shards})
                            if step.page is not None:
                                page = step.page
                                break
                    finally:
                        close = getattr(iterator, "close", None)
                        if close is not None:
                            close()
                    assert page is not None
                elif bounded_range:
                    page = self.provider.read_range(
                        target.source_account_key, target.source_conversation_id,
                        after=None, before=None, direction="forward", limit=page_limit,
                        snapshot=snapshot, time_after_utc=after_utc, time_before_utc=before_utc,
                    )
                else:
                    page = self.provider.read_recent(
                        target.source_account_key, target.source_conversation_id,
                        page_limit, snapshot,
                    )
                if incremental:
                    generations[conversation_id] = [
                        list(item)
                        for item in self.provider.search_generation_binding(
                            target.source_account_key,
                            target.source_conversation_id,
                            snapshot=snapshot,
                        )
                    ]
                prepared = self._prepare_messages(page.messages)
                selective = self._residency_decision(conversation_id).mode != "keep"
                if selective:
                    parts = self._query_parts(query)
                    prepared = tuple(
                        item
                        for item in prepared
                        if not parts or self._matching_search_fields(item.parsed, parts)
                    )
                with self._admission(stack):
                    if check_authority is not None:
                        check_authority()
                    context = self._conversation_context(conversation_id)
                    if selective and participant_ids:
                        selected: list[_PreparedMessage] = []
                        for item in prepared:
                            if item.participant is None:
                                continue
                            canonical, _membership = self.repository.index_participant(
                                str(context["account_id"]), conversation_id, item.participant,
                                item.source.observed_at_utc,
                            )
                            if canonical in participant_ids:
                                selected.append(item)
                        prepared = tuple(selected)
                    admitted_ids = self._ingest_prepared_messages(context, prepared)
                    if selective:
                        # Each retained hit establishes only its exact observed focus;
                        # scanned nonmatches are not resident neighbors or full coverage.
                        self._record_source_windows(
                            conversation_id,
                            snapshot,
                            [(item.source.sort_key, item.source.sort_key) for item in prepared],
                        )
                    elif page.messages:
                        lower = min(
                            page.messages, key=lambda item: item.sort_key.as_tuple()
                        ).sort_key
                        upper = max(
                            page.messages, key=lambda item: item.sort_key.as_tuple()
                        ).sort_key
                        self._record_source_windows(conversation_id, snapshot, [(lower, upper)])
                        if not bounded_range:
                            self._record_recent_source_state(
                                conversation_id, snapshot, page, page.messages
                            )
                if (
                    not selective
                    and not bounded_range
                    and not page.has_more_before
                    and not page.has_more_after
                    and snapshot.scope is None
                ):
                    # A synthetic/catalog snapshot can prove the same full source
                    # generation as the final scan. A native dependency lease must
                    # not invent account-wide generation evidence.
                    full_bindings[conversation_id] = (
                        snapshot.inventory_digest,
                        snapshot.generation_set_digest,
                    )
            admitted = len(admitted_ids)
            remaining -= page_limit
            messages_admitted += admitted
            pending += int(page.has_more_after if bounded_range else page.has_more_before)
            if progress is not None:
                state = {
                    "chosen": list(chosen),
                    "done": index + 1,
                    "message_count": messages_admitted,
                    "pending_conversation_count": pending,
                    "bindings": full_bindings,
                    "generations": generations,
                }
                progress({"checkpoint": state})
        return {
            "performed": True,
            "conversation_count": len(chosen),
            "message_count": messages_admitted,
            "message_budget": SEARCH_PREPARATION_MESSAGE_BUDGET,
            "conversation_budget": SEARCH_PREPARATION_CONVERSATION_BUDGET,
            "unprepared_conversation_count": len(selected_ids) - len(chosen),
            "pending_conversation_count": pending,
            **({"generations": generations} if incremental else {}),
        }, full_bindings

    def _discovery_message_matches(
        self,
        prepared: _PreparedMessage,
        *,
        query_parts: tuple[str, ...],
        hints: tuple[str, ...],
        domains: tuple[str, ...],
        kinds: frozenset[str],
        kind_fallback: bool = False,
        strict_links: bool = False,
    ) -> bool:
        """Whether one observed message is matching evidence for a cold discovery read.

        Links-only recall must key on the observed URL/domain/hint fields rather than
        an empty lexical term: an empty query must not admit the whole scanned page.
        Text recall reuses the canonical casefold/literal terms over the same search
        fields the resident read validates against. This runs on the parsed current
        source message, so it decides only which *observed* rows become resident; the
        ordinary read still re-derives the answer from the admitted canonical rows.
        """

        links, _complete = extract_links(prepared.parsed.text, prepared.parsed.structured)
        if strict_links:
            return any(
                (not domains or link.normalized_host in domains)
                and (not hints or bool(hint_evidence(link.normalized_host, hints)))
                and (not query_parts or self._text_matches(
                    "\n".join(str(value or "") for value in
                              (link.normalized_url, link.title, link.description)),
                    " ".join(query_parts),
                ))
                for link in links
            )
        kind_permitted = (
            not kinds or "message" in kinds or prepared.parsed.kind in kinds
            or "link" in kinds and bool(links)
        )
        if not kind_permitted:
            return False
        if kind_fallback:
            return True
        link_text = "\n".join(
            "\n".join(
                str(value)
                for value in (link.normalized_url, link.title, link.description,
                              link.source_path, link.normalized_host)
            )
            for link in links
        )
        hosts = tuple(dict.fromkeys(link.normalized_host for link in links))
        document = "\n".join(message_search_fields(prepared.parsed).values())
        text_match = bool(query_parts) and self._text_matches(document, " ".join(query_parts))
        link_query_match = bool(query_parts) and self._text_matches(
            link_text, " ".join(query_parts)
        )
        domain_match = bool(domains) and any(host in domains for host in hosts)
        hint_match = bool(hints) and any(hint_evidence(host, hints) for host in hosts)
        if domains and not domain_match:
            return False
        matched_link = domain_match or hint_match or link_query_match
        wants_link = "link" in kinds
        wants_message = "message" in kinds or not kinds
        if not query_parts and not domains and not hints:
            # A bare discovery request with no recall term is kind-scoped.
            return bool(links) if wants_link and not wants_message else bool(document.strip())
        if wants_link and not wants_message:
            # Links-only recall requires real observed link/URL evidence. A plain
            # text hit with no link is not a link candidate and must not be cached.
            return matched_link
        # Hints are an approximate auxiliary signal, never a hard filter: a plain
        # concept/text hit stays eligible even when no hostname hint matches. Only
        # an explicit domain constraint is a hard recall predicate.
        return matched_link or text_match or any(
            self._text_matches(document, hint) for hint in hints
        )

    def _discovery_context(
        self,
        target: _SourceTarget,
        snapshot: SourceSnapshot,
        matches: tuple[_PreparedMessage, ...],
        *,
        after_utc: str | None,
        before_utc: str | None,
    ) -> tuple[tuple[_PreparedMessage, ...], int]:
        """Read bounded source neighbors across page edges and exact reply targets.

        Each focus keeps at most eight bodies on each side and 32 nearby link
        messages, plus its explicit reply. Reads reuse canonical context or bounded
        ranges under the same validated lease; no whole-history import occurs.
        """
        retained: dict[str, _PreparedMessage] = {}
        scanned = 0
        for item in matches:
            retained[item.source.source_message_id] = item
            instant = parse_aware_datetime(item.source.sent_at_utc)
            lower = to_utc_iso(instant - timedelta(seconds=CONTEXT_RADIUS_SECONDS))
            upper = to_utc_iso(instant + timedelta(seconds=CONTEXT_RADIUS_SECONDS))
            lower = max(lower, after_utc) if after_utc else lower
            upper = min(upper, before_utc) if before_utc else upper
            links_left = MAX_CONTEXT_LINKS
            if isinstance(self.provider, ContextSourceProvider):
                # Native context already chooses verified index seeks or one
                # bounded-memory position pass. Never repeat an unindexed ORDER BY
                # query per page. The enclosing attempt deadline/cancellation owns
                # this pass; payload cardinality remains bounded.
                page = self.provider.read_context(
                    target.source_account_key, target.source_conversation_id,
                    focus=item.source, before=MAX_CONTEXT_LINKS,
                    after=MAX_CONTEXT_LINKS, snapshot=snapshot,
                )
                scanned += len(page.messages)
                neighbors = self._prepare_messages(page.messages)
            else:
                rows: list[SourceMessage] = []
                for direction in ("backward", "forward"):
                    with operation_budget(2.0):
                        page = self.provider.read_range(
                            target.source_account_key, target.source_conversation_id,
                            snapshot=snapshot, direction=direction, limit=MAX_CONTEXT_LINKS,
                            before=item.source.sort_key if direction == "backward" else None,
                            after=item.source.sort_key if direction == "forward" else None,
                            time_after_utc=lower, time_before_utc=upper,
                        )
                    rows.extend(page.messages)
                scanned += len(rows)
                neighbors = self._prepare_messages(tuple(rows))
            for direction in ("backward", "forward"):
                ordered = sorted((neighbor for neighbor in neighbors
                    if lower <= neighbor.source.sent_at_utc < upper
                    and ((neighbor.source.sort_key.as_tuple() < item.source.sort_key.as_tuple())
                         if direction == "backward" else
                         (neighbor.source.sort_key.as_tuple() > item.source.sort_key.as_tuple()))),
                    key=lambda value: value.source.sort_key.as_tuple(),
                    reverse=direction == "backward",
                )
                for index, neighbor in enumerate(ordered):
                    links, _complete = extract_links(
                        neighbor.parsed.text, neighbor.parsed.structured
                    )
                    if index < CONTEXT_RADIUS_MESSAGES or (links and links_left):
                        retained[neighbor.source.source_message_id] = neighbor
                        if links:
                            links_left = max(0, links_left - 1)
            source_id = item.parsed.structured.get("reply_target_source_message_id")
            if not isinstance(source_id, str) or not source_id:
                continue
            with operation_budget(2.0):
                message = self.provider.get_message(
                    target.source_account_key, source_id, snapshot
                )
            scanned += int(message is not None)
            if (
                message is not None
                and message.source_conversation_id == target.source_conversation_id
                and (after_utc is None or message.sent_at_utc >= after_utc)
                and (before_utc is None or message.sent_at_utc < before_utc)
            ):
                retained[message.source_message_id] = self._prepare_messages((message,))[0]
        return tuple(retained.values()), scanned

    def _prepare_discovery_candidates(
        self,
        *,
        kind: str,
        query: str,
        hints: tuple[str, ...],
        domains: tuple[str, ...],
        kinds: tuple[str, ...] = (),
        account_id: str | None,
        conversation_ids: tuple[str, ...],
        participant_ids: tuple[str, ...],
        after_utc: str | None,
        before_utc: str | None,
        limit: int,
        checkpoint: dict[str, Any] | None = None,
        progress: Callable[[dict[str, Any]], None] | None = None,
        check_authority: Callable[[], None] | None = None,
    ) -> tuple[dict[str, Any], dict[str, tuple[str, str]]]:
        """Admit only the matching observed messages one cold link/retrieval needs.

        Uses bounded provider-owned physical candidates and canonical admission: catalog
        resolution finishes before any conversation lease opens; each page has a
        cancellable 2-second operation budget. Within one selected conversation this advances in
        bounded, restart-safe pages keyed by the private provider position, so a match
        older than the first page (behind many later rows) is still reachable across
        attempts and restarts instead of being silently absent. Only the matched rows
        are admitted under an on_demand decision, never scanned nonmatches.
        """

        query_parts = self._query_parts(query) if query else ()
        normalized_domains = tuple(
            value for value in (normalize_domain(item) for item in domains) if value is not None
        )
        if not kinds and kind == "links":
            kinds = ("link",)
        kind_set = frozenset(kinds)
        with self._source_read() as (stack, snapshot):
            catalog, external_account_id, _facts, selected_ids, targets = self._search_scope(
                snapshot, account_id, conversation_ids
            )
            for participant_id in participant_ids:
                if self.repository.participant_account_id(participant_id) != external_account_id:
                    raise SightglassError(ErrorCode.PARTICIPANT_OUT_OF_SCOPE)
            with self._admission(stack):
                self._persist_catalog(catalog, snapshot)

        chosen = selected_ids
        saved = checkpoint or {}
        if saved.get("chosen") != list(chosen):
            saved = {}
        done = int(saved.get("done", 0))
        messages_admitted = int(saved.get("message_count", 0))
        matched_messages = int(saved.get("matched_count", 0))
        generations: dict[str, list[list[str]]] = saved.get("generations", {})
        positions: dict[str, dict[str, Any] | None] = saved.get("positions", {})
        # The admit budget is per bounded run, not cumulative across continuations:
        # a resumed run must be able to make forward progress after an earlier run
        # already admitted its own bounded page.
        remaining = SEARCH_PREPARATION_MESSAGE_BUDGET
        scanned_this_attempt = 0
        conversations_this_attempt = 0

        def snapshot_state() -> dict[str, Any]:
            return {
                "chosen": list(chosen),
                "done": done,
                "message_count": messages_admitted,
                "matched_count": matched_messages,
                "generations": generations,
                "positions": positions,
            }

        if progress is not None:
            progress({"checkpoint": snapshot_state()})

        # Every conversation the checkpoint already touched -- completed ones and
        # the unfinished one holding a saved resume position -- must still map to the
        # same selected logical shards. A selected-shard replacement fails closed
        # instead of resuming a stale position; ordinary appends and unrelated-shard
        # replacement leave the selected binding unchanged.
        revalidate_ids = list(chosen[:done]) + [
            value for value in chosen[done:] if positions.get(value)
        ]
        for completed_id in revalidate_ids:
            expected = generations.get(completed_id)
            if not expected:
                continue
            target = targets[completed_id]
            scope = SourceScope.conversation(
                target.source_account_key, target.source_conversation_id
            )
            if check_authority is not None:
                check_authority()
            with self._source_session(scope) as (_stack, revalidation):
                current = [
                    list(item)
                    for item in self.provider.search_generation_binding(
                        target.source_account_key,
                        target.source_conversation_id,
                        snapshot=revalidation,
                    )
                ]
            if current != expected:
                raise SightglassError(
                    ErrorCode.SOURCE_GENERATION_CHANGED,
                    retryable=True,
                    details={"reason": "prepared_source_replaced"},
                )

        incomplete = False
        while (
            done < len(chosen)
            and scanned_this_attempt < DISCOVERY_CONVERSATION_SCAN_BUDGET
            and conversations_this_attempt < SEARCH_PREPARATION_CONVERSATION_BUDGET
        ):
            check_operation_budget()
            # Worst-case focus + body neighbors + link neighbors + exact reply.
            cost_per_match = (
                2 + 2 * CONTEXT_RADIUS_MESSAGES + MAX_CONTEXT_LINKS
                if kind != "links" else 1
            )
            if remaining < cost_per_match:
                incomplete = True
                break
            conversation_id = chosen[done]
            target = targets[conversation_id]
            if check_authority is not None:
                check_authority()
            position = positions.get(conversation_id)
            page_limit = min(
                max(50, limit), remaining, DISCOVERY_CONVERSATION_SCAN_BUDGET - scanned_this_attempt
            )
            scope = SourceScope.conversation(
                target.source_account_key, target.source_conversation_id
            )
            with self._source_session(scope) as (stack, snapshot):
                current_generation = [list(item) for item in
                    self.provider.search_generation_binding(
                        target.source_account_key, target.source_conversation_id,
                        snapshot=snapshot,
                    )]
                expected_generation = generations.get(conversation_id)
                if expected_generation and current_generation != expected_generation:
                    raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED,
                        retryable=True, details={"reason": "prepared_source_replaced"})
                with operation_budget(2.0):
                    page = self.provider.scan_discovery_page(
                        target.source_account_key, target.source_conversation_id,
                        limit=page_limit, snapshot=snapshot, position=position,
                        time_after_utc=after_utc, time_before_utc=before_utc,
                    )
                if check_authority is not None:
                    check_authority()
                if progress is not None:
                    progress({"phase": "candidate_page",
                              "messages_scanned": scanned_this_attempt + page.scanned_rows})
                generations[conversation_id] = current_generation
                page_scanned = page.scanned_rows
                context_scanned = 0
                candidates = self._prepare_messages(page.messages)
                match_limit = remaining // cost_per_match
                payload_cost = 2 * MAX_CONTEXT_LINKS + 3 if kind != "links" else 1
                match_limit = min(match_limit, max(0,
                    (DISCOVERY_CONVERSATION_SCAN_BUDGET - scanned_this_attempt
                     - page_scanned) // payload_cost))
                selected_matches: dict[str, _PreparedMessage] = {}
                next_position = position
                stopped = False

                def matches_query(item: _PreparedMessage) -> bool:
                    return self._discovery_message_matches(
                        item, query_parts=query_parts, hints=hints,
                        domains=normalized_domains, kinds=kind_set,
                        kind_fallback=kind != "links" and bool(kinds) and not hints,
                        strict_links=kind == "links",
                    ) and not (item.participant is None and participant_ids)

                for candidate, candidate_position in zip(candidates, page.positions, strict=True):
                    if matches_query(candidate):
                        reserved_context = len(selected_matches) * (payload_cost - 1)
                        if (
                            len(selected_matches) >= match_limit
                            or scanned_this_attempt + page_scanned + reserved_context
                            + payload_cost > DISCOVERY_CONVERSATION_SCAN_BUDGET
                        ):
                            stopped = True
                            break
                        # Physical scan rows are recall candidates. Reconcile the
                        # exact canonical identity across shards before admitting.
                        with operation_budget(2.0):
                            canonical_source = self.provider.get_message(
                                target.source_account_key,
                                candidate.source.source_message_id, snapshot,
                            )
                        page_scanned += int(canonical_source is not None)
                        if (
                            canonical_source is not None
                            and canonical_source.source_conversation_id
                                == target.source_conversation_id
                            and (after_utc is None or canonical_source.sent_at_utc >= after_utc)
                            and (before_utc is None or canonical_source.sent_at_utc < before_utc)
                        ):
                            canonical = self._prepare_messages((canonical_source,))[0]
                            if matches_query(canonical):
                                selected_matches[canonical_source.source_message_id] = canonical
                    next_position = candidate_position
                if not stopped:
                    next_position = page.next_position
                elif next_position == position:
                    incomplete = True
                    break
                matches = tuple(selected_matches.values())
                if kind == "links":
                    prepared = matches
                else:
                    prepared, context_scanned = self._discovery_context(
                        target, snapshot, matches,
                        after_utc=after_utc, before_utc=before_utc,
                    )
                with self._admission(stack):
                    if check_authority is not None:
                        check_authority()
                    context = self._conversation_context(conversation_id)
                    if participant_ids:
                        # Canonical identity/alias resolution runs inside the short
                        # admission transaction, so no identity write survives a
                        # cancelled or stale selected source.
                        selected_prepared: list[_PreparedMessage] = []
                        for item in prepared:
                            if item.participant is None:
                                # Hard sender scope excludes rows whose speaker is not
                                # resolvable to an eligible canonical participant.
                                if not participant_ids:
                                    selected_prepared.append(item)
                                continue
                            canonical, _membership = self.repository.index_participant(
                                str(context["account_id"]),
                                conversation_id,
                                item.participant,
                                item.source.observed_at_utc,
                            )
                            if canonical in participant_ids:
                                selected_prepared.append(item)
                        prepared = tuple(selected_prepared)
                    admitted_ids = self._ingest_prepared_messages(context, prepared)
                    self._record_source_windows(
                        conversation_id, snapshot,
                        [(item.source.sort_key, item.source.sort_key) for item in prepared],
                    )
            matched_messages += len(matches)
            messages_admitted += len(admitted_ids)
            remaining = max(0, remaining - len(prepared))
            scanned_this_attempt += page_scanned + context_scanned
            positions[conversation_id] = next_position
            if page.has_more or stopped:
                if progress is not None:
                    progress({"checkpoint": snapshot_state()})
                continue
            done += 1
            conversations_this_attempt += 1
            positions.pop(conversation_id, None)
            if progress is not None:
                progress({"checkpoint": snapshot_state()})

        unprepared = len(selected_ids) - len(chosen)
        complete = done >= len(chosen) and not incomplete and unprepared == 0
        remaining_conversations = []
        if done < len(chosen):
            remaining_conversations = [chosen[index] for index in range(done, len(chosen))]
        result = {
            "performed": True,
            "kind": kind,
            "complete": complete,
            "conversation_count": len(chosen),
            "prepared_conversation_count": done,
            "message_count": messages_admitted,
            "matched_message_count": matched_messages,
            "message_budget": SEARCH_PREPARATION_MESSAGE_BUDGET,
            "scanned_row_count": scanned_this_attempt,
            "admitted_this_attempt": SEARCH_PREPARATION_MESSAGE_BUDGET - remaining,
            "scan_budget": DISCOVERY_CONVERSATION_SCAN_BUDGET,
            "conversation_budget": SEARCH_PREPARATION_CONVERSATION_BUDGET,
            "unprepared_conversation_count": unprepared,
            "pending_conversation_count": len(chosen) - done,
            "remaining_conversation_ids": remaining_conversations,
            "unprepared_conversation_ids": [
                value for value in selected_ids if value not in set(chosen)
            ],
            "generations": generations,
        }
        return result, {}

    def _search_scan(
        self,
        *,
        selected_ids: tuple[str, ...],
        targets: dict[str, _SourceTarget],
        query_parts: tuple[str, ...],
        after_utc: str | None,
        before_utc: str | None,
        bounded: int,
        resume_key: tuple[str, int, int, str] | None,
        snapshot: SourceSnapshot,
    ) -> tuple[list[tuple[Any, _PreparedMessage, tuple[str, ...]]], bool, bool, Any | None, int]:
        """Validate one bounded keyset window of durable-index candidates.

        Candidates are read in ordered keyset batches and every scanned candidate,
        including a stale or filtered one, advances the returned frontier, so a
        continuation never revalidates work the previous call already consumed.

        Returns the matched sources, an exact ``candidate_has_more`` fact, whether
        the candidate budget (rather than the index or the output limit) ended the
        scan, the last consumed candidate, and the number examined. ``page`` stays
        complete when the output limit is reached exactly at the end of the index.
        """

        captured_ids = self._captured_search_candidates.get()
        if captured_ids is not None:
            return self._captured_search_scan(
                captured_ids=captured_ids, selected_ids=selected_ids, targets=targets,
                query_parts=query_parts, after_utc=after_utc, before_utc=before_utc,
                bounded=bounded, resume_key=resume_key, snapshot=snapshot,
            )

        matched: list[tuple[Any, _PreparedMessage, tuple[str, ...]]] = []
        scanned = 0
        frontier: Any | None = None
        after_key = resume_key
        candidates_exhausted = False
        while len(matched) < bounded and scanned < SEARCH_SCAN_CANDIDATE_BUDGET:
            check_operation_budget()
            batch_size = min(SEARCH_SCAN_BATCH_LIMIT, SEARCH_SCAN_CANDIDATE_BUDGET - scanned)
            # Recall is an ordered keyset window of the durable index, *not* a SQL
            # ``LIKE '%term%'`` predicate. That predicate cannot use an index, so an
            # absent or rare term forced SQLite to scan the entire indexed body
            # history before ``LIMIT`` could stop. Every row this window returns is
            # counted against the candidate budget and the term filter is applied
            # below (the same predicate final canonical validation re-applies), so a
            # zero-hit query examines at most the bounded budget instead of all
            # history, while a matching hit is still validated against the current
            # source before delivery.
            batch = self.repository.search_candidate_window(
                selected_ids, after_key=after_key, after_utc=after_utc,
                before_utc=before_utc, lexical_queries=(" ".join(query_parts),), limit=batch_size
            )
            if not batch:
                candidates_exhausted = True
                break
            consumed = 0
            for row in batch:
                consumed += 1
                scanned += 1
                frontier = row
                after_key = self._search_keyset(row)
                if query_parts and not self._search_text_matches(row, query_parts):
                    continue
                if (after_utc is not None and str(row["sent_at_utc"]) < after_utc) or (
                    before_utc is not None and str(row["sent_at_utc"]) >= before_utc
                ):
                    continue
                target = targets[str(row["conversation_id"])]
                source = self.provider.get_message(
                    target.source_account_key,
                    str(row["source_message_id"]),
                    snapshot,
                )
                parsed = parse_message(source) if source is not None else None
                matched_fields = (
                    self._matching_search_fields(parsed, query_parts)
                    if parsed is not None
                    else ()
                )
                if (
                    source is None
                    or source.sort_key.as_tuple() != source_sort_key(row).as_tuple()
                    or (query_parts and not matched_fields)
                ):
                    continue
                matched.append(
                    (
                        row,
                        _PreparedMessage(
                            source=source,
                            participant=self._source_participant_for_message(source),
                            parsed=parsed,
                        ),
                        matched_fields,
                    )
                )
                if len(matched) >= bounded:
                    break
            if len(matched) >= bounded:
                # The output limit stopped the scan. Unexamined rows in this batch
                # prove more candidates; at a batch boundary, a bounded one-row peek
                # is the only honest way to tell an exact end from a full window.
                if consumed < len(batch):
                    candidate_has_more = True
                elif len(batch) < batch_size:
                    candidate_has_more = False
                else:
                    candidate_has_more = self._search_candidate_exists(
                        selected_ids, after_key, after_utc=after_utc,
                        before_utc=before_utc, query_parts=query_parts,
                    )
                return matched, candidate_has_more, False, frontier, scanned
            if len(batch) < batch_size:
                candidates_exhausted = True
                break
        candidate_has_more = (
            False
            if candidates_exhausted or frontier is None
            else self._search_candidate_exists(
                selected_ids, after_key, after_utc=after_utc,
                before_utc=before_utc, query_parts=query_parts,
            )
        )
        return matched, candidate_has_more, candidate_has_more, frontier, scanned

    def _captured_search_scan(
        self, *, captured_ids: tuple[str, ...], selected_ids: tuple[str, ...],
        targets: dict[str, _SourceTarget], query_parts: tuple[str, ...],
        after_utc: str | None, before_utc: str | None, bounded: int,
        resume_key: tuple[str, int, int, str] | None, snapshot: SourceSnapshot,
    ) -> tuple[list[tuple[Any, _PreparedMessage, tuple[str, ...]]], bool, bool, Any | None, int]:
        # The receiver must supply one contiguous ordered candidate prefix. Reject
        # omissions rather than advance a signed frontier over uncaptured rows.
        rows = self.repository.search_candidate_window(
            selected_ids, after_key=resume_key, after_utc=after_utc, before_utc=before_utc,
            lexical_queries=(" ".join(query_parts),), limit=200,
        )
        if captured_ids != tuple(str(row["message_id"]) for row in rows[:len(captured_ids)]):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        if not captured_ids and rows:
            raise SightglassError(ErrorCode.SOURCE_INCOMPLETE, retryable=True,
                                 details={"warning_codes": ["capture_candidates_unavailable"]})
        matched = []
        frontier = None
        scanned = 0
        for row in rows[:len(captured_ids)]:
            check_operation_budget()
            target = targets[str(row["conversation_id"])]
            source = self.provider.get_message(
                target.source_account_key, str(row["source_message_id"]), snapshot,
            )
            scanned += 1
            frontier = row
            if source is None:
                continue
            parsed = parse_message(source)
            fields = self._matching_search_fields(parsed, query_parts)
            if (source.sort_key.as_tuple() != source_sort_key(row).as_tuple()
                    or query_parts and not fields):
                continue
            matched.append((row, _PreparedMessage(
                source, self._source_participant_for_message(source), parsed), fields))
            if len(matched) >= bounded:
                break
        more = frontier is not None and self._search_candidate_exists(
            selected_ids, self._search_keyset(frontier), after_utc=after_utc,
            before_utc=before_utc, query_parts=query_parts,
        )
        return matched, more, bool(more and scanned == len(captured_ids)), frontier, scanned

    def _search_candidate_exists(
        self,
        selected_ids: tuple[str, ...],
        after_key: tuple[str, int, int, str] | None,
        *,
        after_utc: str | None = None,
        before_utc: str | None = None,
        query_parts: tuple[str, ...] = (),
    ) -> bool:
        """Report whether one bounded peek finds another candidate past the key."""

        if after_key is None:
            return False
        return bool(
            self.repository.search_candidate_window(
                selected_ids, after_key=after_key, after_utc=after_utc,
                before_utc=before_utc,
                lexical_queries=(" ".join(query_parts),) if query_parts else (), limit=1,
            )
        )

    def _ambiguous_sender_page(
        self,
        *,
        snapshot: SourceSnapshot,
        facts: _CatalogFacts,
        external_account_id: str,
        selected_ids: tuple[str, ...],
        conversation_ids: tuple[str, ...],
        candidates: dict[str, dict[str, Any]],
        after_utc: str | None,
        before_utc: str | None,
        bounded: int,
        query: str,
    ) -> dict[str, Any]:
        """Return bounded sender candidates instead of guessing a canonical scope."""

        account_row = self.repository.account_row(external_account_id)
        assert account_row is not None
        compact = self.compact_projector.render([])
        source_receipt = self._search_source_receipt(
            snapshot=snapshot,
            facts=facts,
            account_id=external_account_id,
            conversation_ids=selected_ids,
            all_authorized_conversations=not bool(conversation_ids),
            after_utc=after_utc,
            before_utc=before_utc,
            returned_count=0,
        )
        result = {
            "schema": "sightglass.search-results.v2",
            "projection": "compact",
            "query": query,
            "account_id": external_account_id,
            "ambiguous_sender": len(candidates) > 1,
            "participant_candidates": list(candidates.values())[:bounded],
            "timezone": self._reader_timezone(account_row),
            "conversations": [],
            "hit_conversations": [],
            "people": compact["people"],
            "fields": compact["fields"],
            "hits": [],
            "markers": compact["markers"],
            "page": {
                "next_cursor": None,
                "truncated": False,
                "message_rows_complete": True,
            },
            "source_receipt": source_receipt,
        }
        CompactBodyBudgetAllocator(
            max_payload_chars=self.reader.policy.max_compact_payload_chars,
            max_body_chars=(self.reader.policy.max_compact_body_chars_per_message),
        ).apply(result, rows_key="hits")
        return result

    def continuation_limit(self, limit: int | None, token: str | None, *, default: int) -> int:
        if limit is not None:
            return limit
        if token:
            payload = self.token_codec.decode(token)
            if payload.get("kind") == "search-preparation":
                if self.search_preparation is None:
                    raise SightglassError(ErrorCode.CURSOR_INVALID)
                return self.search_preparation.continuation_limit(token)
        return default

    def search_messages(
        self,
        *,
        query: str,
        account_id: str | None = None,
        conversation_ids: tuple[str, ...] = (),
        participant_ids: tuple[str, ...] = (),
        sender_query: str | None = None,
        after: str | None = None,
        before: str | None = None,
        cursor: str | None = None,
        reading_token: str | None = None,
        limit: int | None = None,
        strict: bool = True,
        view: str | None = None,
    ) -> dict[str, Any]:
        self.reader.require_search()
        if not strict:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        resolved_view = self.resolve_view(view)
        limit = self.continuation_limit(limit, reading_token or cursor, default=20)
        bounded = self.reader.bound_limit(limit)
        participant_ids = tuple(sorted(set(participant_ids)))
        query_parts = self._query_parts(query)
        if not query_parts and (not participant_ids or after is None or before is None):
            raise SightglassError(
                ErrorCode.QUERY_INVALID,
                details={"reason": "empty query requires participant_ids and bounded time"},
            )
        try:
            after_utc = to_utc_iso(after) if after else None
            before_utc = to_utc_iso(before) if before else None
        except ValueError as exc:
            raise SightglassError(ErrorCode.QUERY_INVALID) from exc
        if after_utc is not None and before_utc is not None and after_utc >= before_utc:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if resolved_view == "replica":
            return self.replica.search_messages(
                query=query, account_id=account_id, conversation_ids=conversation_ids,
                participant_ids=participant_ids, sender_query=sender_query,
                after=after_utc, before=before_utc, cursor=cursor, reading_token=reading_token,
                limit=bounded,
            )
        if resolved_view == "fresh" and reading_token is not None:
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        if cursor and self.token_codec.decode(cursor).get("kind") == "search-replica":
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        async_preparation = None
        captured_search = self._captured_search_candidates.get() is not None
        if reading_token and cursor:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        preparation_token = reading_token
        if cursor and self.token_codec.decode(cursor).get("kind") == "search-preparation":
            preparation_token, cursor = cursor, None
        if (not captured_search and resolved_view == "auto"
                and self.search_preparation is not None and cursor is None):
            async_preparation = self.search_preparation.request(
                query=query, account_id=account_id, conversation_ids=conversation_ids,
                participant_ids=participant_ids, sender_query=sender_query,
                after_utc=after_utc, before_utc=before_utc, limit=bounded,
                token=preparation_token,
            )
            if async_preparation.get("schema") == "sightglass.search-preparation.v1":
                return async_preparation
        elif preparation_token:
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        if cursor:
            # Validate signature, reader, shape and expiry before any content read.
            # The actual account/filter/source binding is checked below as well.
            payload = self.token_codec.decode(cursor)
            self.search_cursors.verify(
                cursor, reader_id=self.reader.reader_id,
                account_id=str(payload.get("account_id", "")),
                scope_key=str(payload.get("scope_key", "")),
            )
        resolve_sender = bool(sender_query) and not participant_ids
        roster_binding: tuple[str, str] | None = None
        if cursor and resolve_sender:
            # Existing canonical sender bindings suffice to reject a mismatched
            # continuation before fetching another roster. The ordinary roster
            # refresh still follows for an otherwise valid sender traversal.
            with self._source_read() as (_stack, snapshot):
                _catalog, scoped_account, _facts, selected, _targets = self._search_scope(
                    snapshot, account_id, conversation_ids
                )
                candidates = self._search_sender_candidates(selected, sender_query or "")
                if len(candidates) != 1:
                    raise SightglassError(ErrorCode.CURSOR_INVALID)
                self._verify_search_cursor(
                    cursor, account_id=scoped_account, snapshot=snapshot,
                    scope_key=self._view_scope(search_scope_digest(
                        conversation_ids=selected, participant_ids=(next(iter(candidates)),),
                        query=query, after=after_utc, before=before_utc,
                    ), resolved_view),
                )
        if resolve_sender and not captured_search:
            # A sender query is a canonical participant scope, so the rosters that
            # resolve it are indexed before the scope is bound into the cursor.
            roster_binding = self._index_search_roster(account_id, conversation_ids)
        preparation: dict[str, Any] = {"performed": False}
        prepared_full_bindings: dict[str, tuple[str, str]] = {}
        if async_preparation is not None:
            preparation = async_preparation["preparation"]
            prepared_full_bindings = {
                key: tuple(value) for key, value in async_preparation["bindings"].items()
            }
        elif cursor is None and not captured_search:
            preparation, prepared_full_bindings = self._prepare_search_candidates(
                query=query, account_id=account_id, conversation_ids=conversation_ids,
                participant_ids=participant_ids, sender_query=sender_query,
                after_utc=after_utc, before_utc=before_utc, limit=bounded,
                roster_binding=roster_binding,
            )
        with self._source_read() as (stack, snapshot):
            if roster_binding is not None and roster_binding != (
                snapshot.inventory_digest,
                snapshot.generation_set_digest,
            ):
                # The roster preflight resolved its canonical sender scope against
                # one source version. Scanning under a different version would mix
                # two versions, so fail closed under the ordinary generation-change
                # contract instead of silently combining them.
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
            (
                catalog_read,
                external_account_id,
                facts,
                selected_ids,
                targets,
            ) = self._search_scope(snapshot, account_id, conversation_ids)
            if async_preparation is not None:
                expected = preparation.pop("generations", {})
                for prepared_conversation, binding in expected.items():
                    target = targets.get(prepared_conversation)
                    if target is None:
                        raise SightglassError(ErrorCode.CURSOR_STALE)
                    current = self.provider.search_generation_binding(
                        target.source_account_key, target.source_conversation_id, snapshot=snapshot,
                    )
                    if [list(item) for item in current] != binding:
                        raise SightglassError(ErrorCode.CURSOR_STALE,
                                             details={"reason": "prepared_source_replaced"})
            effective_participant_ids = participant_ids
            if resolve_sender:
                sender_candidates = self._search_sender_candidates(selected_ids, sender_query or "")
                if len(sender_candidates) != 1:
                    if cursor:
                        raise SightglassError(ErrorCode.CURSOR_INVALID)
                    with self._admission(stack):
                        self._persist_catalog(catalog_read, snapshot)
                    return self._ambiguous_sender_page(
                        snapshot=snapshot,
                        facts=facts,
                        external_account_id=external_account_id,
                        selected_ids=selected_ids,
                        conversation_ids=conversation_ids,
                        candidates=sender_candidates,
                        after_utc=after_utc,
                        before_utc=before_utc,
                        bounded=bounded,
                        query=query,
                    )
                effective_participant_ids = (next(iter(sender_candidates)),)
            for participant_id in effective_participant_ids:
                if self.repository.participant_account_id(participant_id) != external_account_id:
                    raise SightglassError(ErrorCode.PARTICIPANT_OUT_OF_SCOPE)
            scope_key = search_scope_digest(
                conversation_ids=selected_ids,
                participant_ids=effective_participant_ids,
                query=query,
                after=after_utc,
                before=before_utc,
            )
            scope_key = self._view_scope(scope_key, resolved_view)
            resume_key: tuple[str, int, int, str] | None = None
            if cursor:
                resume_key = self._verify_search_cursor(
                    cursor, account_id=external_account_id, scope_key=scope_key, snapshot=snapshot
                )
            matched, candidate_has_more, scan_budget_exhausted, frontier, scanned = (
                self._search_scan(
                    selected_ids=selected_ids,
                    targets=targets,
                    query_parts=query_parts,
                    after_utc=after_utc,
                    before_utc=before_utc,
                    bounded=bounded,
                    resume_key=resume_key,
                    snapshot=snapshot,
                )
            )
            page_truncated = candidate_has_more
            with self._admission(stack):
                self._persist_catalog(catalog_read, snapshot)
                contexts = {value: self._conversation_context(value) for value in selected_ids}
                for stale_row, prepared_source, _matched_fields in matched:
                    self._ingest_prepared_messages(
                        contexts[str(stale_row["conversation_id"])], (prepared_source,)
                    )
                # Freeze the newly admitted version: filters and projection must use
                # the canonical row, never the pre-ingest cached candidate.
                frozen = {
                    str(row["message_id"]): row
                    for row in self.repository.frozen_message_rows(
                        tuple(str(row["message_id"]) for row, _prepared, _fields in matched)
                    )
                }
                canonical: list[Any] = []
                match_fields_by_message_id: dict[str, tuple[str, ...]] = {}
                for stale_row, _prepared, matched_fields in matched:
                    row = frozen.get(str(stale_row["message_id"]))
                    if row is None or str(row["current_state"]) != "present":
                        continue
                    if query_parts and not self._text_matches(
                        current_search_document(row), query
                    ):
                        continue
                    if (
                        effective_participant_ids
                        and str(row["sender_id"]) not in effective_participant_ids
                    ):
                        continue
                    if after_utc is not None and str(row["sent_at_utc"]) < after_utc:
                        continue
                    if before_utc is not None and str(row["sent_at_utc"]) >= before_utc:
                        continue
                    canonical.append(row)
                    match_fields_by_message_id[str(row["message_id"])] = matched_fields
                canonical.sort(key=self._search_keyset)
                if resume_key is not None:
                    canonical = [
                        row for row in canonical if self._search_keyset(row) > resume_key
                    ]
                canonical = canonical[:bounded]
                account_row = self.repository.account_row(external_account_id)
                assert account_row is not None
                timezone_name = self._reader_timezone(account_row)
                prepared = self.compact_projector.prepare(
                    canonical,
                    timezone_name=timezone_name,
                    include_resource_indicators=True,
                    focus_message_ids=frozenset(),
                    context_only_ids=frozenset(),
                    late_arrival_ids=frozenset(),
                )

                def build_search_page(
                    selected: slice, message_rows_complete: bool
                ) -> dict[str, Any]:
                    candidate_rows = canonical[selected]
                    compact = self.compact_projector.render(prepared[selected])
                    truncated = page_truncated or not message_rows_complete
                    cropped = len(candidate_rows) < len(canonical)
                    cursor_row = candidate_rows[-1] if cropped else frontier
                    next_cursor = (
                        self.search_cursors.issue(
                            reader_id=self.reader.reader_id,
                            account_id=external_account_id,
                            scope_key=scope_key,
                            row=cursor_row,
                            inventory_digest=snapshot.inventory_digest,
                            generation_set_digest=snapshot.generation_set_digest,
                            projection_epoch=self._projection_inventory_epoch(),
                        )
                        if truncated and cursor_row is not None
                        else None
                    )
                    hit_conversation_ids = tuple(
                        dict.fromkeys(str(row["conversation_id"]) for row in candidate_rows)
                    )
                    conversation_indices = {
                        conversation_id: index
                        for index, conversation_id in enumerate(hit_conversation_ids)
                    }
                    source_receipt = self._search_source_receipt(
                        snapshot=snapshot,
                        facts=facts,
                        account_id=external_account_id,
                        conversation_ids=selected_ids,
                        all_authorized_conversations=not bool(conversation_ids),
                        after_utc=after_utc,
                        before_utc=before_utc,
                        returned_count=len(candidate_rows),
                        scan_budget_exhausted=scan_budget_exhausted,
                        candidates_scanned=scanned,
                        prepared_full_conversation_ids=frozenset(
                            conversation_id
                            for conversation_id, binding in prepared_full_bindings.items()
                            if binding == (
                                snapshot.inventory_digest, snapshot.generation_set_digest
                            )
                        ),
                    )
                    source_receipt["search"]["preparation"] = preparation
                    if resolved_view == "fresh":
                        source_receipt["view"] = "fresh"
                    if preparation.get("pending_conversation_count") or preparation.get(
                        "unprepared_conversation_count"
                    ):
                        source_receipt["warnings"].append("search_source_preparation_partial")
                    markers = compact["markers"]
                    if query_parts:
                        markers["matches"] = {
                            str(index): list(
                                match_fields_by_message_id.get(str(row["message_id"]), ("text",))
                            )
                            for index, row in enumerate(candidate_rows)
                        }
                    result = {
                        "schema": "sightglass.search-results.v2",
                        "projection": "compact",
                        "query": query,
                        "account_id": external_account_id,
                        "ambiguous_sender": False,
                        "participant_candidates": [],
                        "timezone": timezone_name,
                        "conversations": [
                            {
                                "id": conversation_id,
                                "title": str(contexts[conversation_id]["current_title"]),
                                "kind": str(contexts[conversation_id]["kind"]),
                            }
                            for conversation_id in hit_conversation_ids
                        ],
                        "hit_conversations": [
                            conversation_indices[str(row["conversation_id"])]
                            for row in candidate_rows
                        ],
                        "people": compact["people"],
                        "fields": compact["fields"],
                        "hits": compact["messages"],
                        "markers": markers,
                        "page": {
                            "next_cursor": next_cursor,
                            "truncated": truncated,
                            "message_rows_complete": message_rows_complete,
                        },
                        "source_receipt": source_receipt,
                        "freshness": "live_validated",
                        "execution": {
                            "state": "partial" if scan_budget_exhausted else "complete",
                            "stop_reason": (
                                "candidate_budget" if scan_budget_exhausted
                                else "output_limit" if truncated else None
                            ),
                            "candidates_examined": scanned,
                            "candidate_budget": SEARCH_SCAN_CANDIDATE_BUDGET,
                            "continuation_available": next_cursor is not None,
                        },
                        "index_receipt": {
                            "kind": "lexical",
                            "backend": "bounded_literal_scan",
                            "recipe": "casefold-literal-v1",
                            "coverage": (
                                "complete" if source_receipt.get("complete") else "partial"
                            ),
                        },
                    }
                    CompactBodyBudgetAllocator(
                        max_payload_chars=self.reader.policy.max_compact_payload_chars,
                        max_body_chars=(self.reader.policy.max_compact_body_chars_per_message),
                    ).apply(result, rows_key="hits")
                    return result

                _selected, result = self._fit_compact_page(
                    len(canonical), direction="forward", build=build_search_page
                )
                return result

    def list_resources(self, message_id: str) -> dict[str, Any]:
        return self.resource_service.list_resources(message_id)

    def _voice_resource_text(
        self,
        resource_id: str,
        *,
        page: int | None,
        start_line: int | None,
        end_line: int | None,
        member: str | None,
        sheet: str | None,
        cell_range: str | None,
        max_bytes: int,
    ) -> ResourceReadPayload | None:
        """Derive one voice resource's transcript; only voice resources leave this branch.

        The transcript is keyed by the resource binding evidence recorded at admission,
        so this derived read does not re-open the source original. It is still a text
        read: the ordinary text-mode selector rules and output budgets are applied
        before any transcript text is released.
        """

        if self.voice is None or not self.voice_settings.enabled:
            return None
        policy = self.voice_settings.policy_for(None)
        if policy == "off" or not self.reader.policy.resource_preview:
            return None
        row = self.repository.resource_context(resource_id)
        if row is None:
            raise SightglassError(ErrorCode.RESOURCE_NOT_FOUND)
        candidate = self.voice.candidate_for_resource(row)
        if candidate is None:
            return None
        bounded_bytes = self.resource_service.validate_text_read(
            page=page,
            start_line=start_line,
            end_line=end_line,
            member=member,
            sheet=sheet,
            cell_range=cell_range,
            max_bytes=max_bytes,
        )
        if page is not None or member is not None:
            # A transcript is neither a PDF page nor an archive member.
            raise SightglassError(ErrorCode.QUERY_INVALID)
        try:
            resolver: Any = json.loads(str(row["resolver_json"]))
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise SightglassError(ErrorCode.INTERNAL_ERROR) from exc
        if not isinstance(resolver, dict) or resolver.get("active", True) is False:
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
        self._require_voice_read(str(row["conversation_id"]))
        return self.voice.resource_text(
            reader_id=self.reader.reader_id,
            candidate=candidate,
            account_binding_id=self.voice.account_binding_id(candidate.account_id),
            policy=policy,
            start_line=start_line,
            end_line=end_line,
            max_chars=self.reader.policy.max_text_chars_per_call,
            max_bytes=bounded_bytes,
        )

    def find_resources(self, **arguments: Any) -> dict[str, Any]:
        with self.repository.database.read_snapshot():
            return self._find_materialized_resources(**arguments)

    def _find_materialized_resources(
        self,
        *,
        query: str,
        account_id: str | None,
        conversation_ids: tuple[str, ...],
        kinds: tuple[str, ...],
        format_families: tuple[str, ...],
        after: str | None,
        before: str | None,
        availability: tuple[str, ...],
        cursor: str | None,
        limit: int,
    ) -> dict[str, Any]:
        """Search the authorized materialized resource catalog without source I/O."""

        self.reader.require_resource("metadata")
        normalized_query = " ".join(str(query).split())
        if len(normalized_query) > 200:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        allowed_families = {
            "image",
            "audio",
            "video",
            "pdf",
            "workbook",
            "presentation",
            "archive",
            "text",
            "office",
            "binary",
        }
        selected_families = tuple(dict.fromkeys(format_families))
        if any(value not in allowed_families for value in selected_families):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        selected_conversations = tuple(dict.fromkeys(conversation_ids))
        for conversation_id in selected_conversations:
            self.reader.authorize(conversation_id)
        accounts = self.repository.active_account_ids()
        if account_id is None:
            if len(accounts) != 1:
                raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)
            selected_account = accounts[0]
        elif account_id not in accounts:
            raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)
        else:
            selected_account = account_id
        bounded = self.reader.bound_limit(limit)
        after_utc = to_utc_iso(parse_aware_datetime(after)) if after is not None else None
        before_utc = to_utc_iso(parse_aware_datetime(before)) if before is not None else None
        if after_utc is not None and before_utc is not None and after_utc >= before_utc:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        scope = {
            "query": normalized_query.casefold(),
            "account_id": selected_account,
            "conversation_ids": selected_conversations,
            "kinds": tuple(dict.fromkeys(kinds)),
            "format_families": selected_families,
            "after": after_utc,
            "before": before_utc,
            "availability": tuple(dict.fromkeys(availability)),
        }
        replica_epoch = (self._projection_inventory_epoch()
                         if self.default_view == "replica" else None)
        if replica_epoch is not None:
            scope.update(view="replica", projection_epoch=replica_epoch)
        scope_key = hashlib.sha256(
            json.dumps(scope, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        policy_revision = (self._materialized_cursor_revision() if replica_epoch is not None
                           else self._policy_revision())
        position: tuple[str, int, int, int, str] | None = None
        if cursor is not None:
            payload = self.account_cursors.verify(
                cursor,
                kind="resources",
                reader_id=self.reader.reader_id,
                account_id=selected_account,
                scope_key=scope_key,
                policy_revision=policy_revision,
            )
            raw_position = payload["position"]
            if (
                len(raw_position) != 5
                or not isinstance(raw_position[0], str)
                or type(raw_position[1]) is not int
                or type(raw_position[2]) is not int
                or type(raw_position[3]) is not int
                or not isinstance(raw_position[4], str)
            ):
                raise SightglassError(ErrorCode.CURSOR_INVALID)
            position = (
                raw_position[0],
                raw_position[1],
                raw_position[2],
                raw_position[3],
                raw_position[4],
            )
            snapshot = payload["snapshot"]
            observation_watermark = snapshot.get("observation_watermark")
            if (
                snapshot.get("mode") != "materialized_resources"
                or type(observation_watermark) is not int
                or observation_watermark < 0
            ):
                raise SightglassError(ErrorCode.CURSOR_INVALID)
        else:
            observation_watermark = self.repository.observation_watermark()
        if replica_epoch is not None and cursor is not None:
            permitted_scope = selected_conversations or tuple(
                str(row["conversation_id"]) for row in self.repository.account_conversations(
                    selected_account) if self.reader.policy.permits(str(row["conversation_id"]))
            )
            if any(self.repository.materialized_snapshot_changed(
                value, projection_epoch=replica_epoch,
                observation_watermark=observation_watermark,
            ) for value in permitted_scope):
                raise SightglassError(ErrorCode.CURSOR_STALE)
        permitted = (
            tuple(sorted(self.reader.policy.allowed_conversation_ids))
            if self.reader.policy.mode == "allowlist"
            else None
        )
        rows = self.repository.find_resources(
            account_id=selected_account,
            query=normalized_query,
            conversation_ids=selected_conversations,
            kinds=tuple(dict.fromkeys(kinds)),
            format_families=selected_families,
            after=after_utc,
            before=before_utc,
            availability=tuple(dict.fromkeys(availability)),
            permitted_conversations=permitted,
            denied_conversations=tuple(sorted(self.reader.policy.denied_conversation_ids)),
            observation_watermark=observation_watermark,
            position=position,
            limit=bounded + 1,
            projection_epoch=replica_epoch,
        )
        has_more = len(rows) > bounded
        selected = rows[:bounded]
        items = []
        for row in selected:
            descriptor = self.resource_service._descriptor(row)
            items.append(
                {
                    "resource": descriptor,
                    "message_id": str(row["message_id"]),
                    "conversation": {
                        "conversation_id": str(row["conversation_id"]),
                        "kind": str(row["conversation_kind"]),
                        "title": str(row["conversation_title"]),
                    },
                    "sender": {
                        "participant_id": str(row["sender_id"]) if row["sender_id"] else None,
                        "label": str(row["sender_label"]) if row["sender_label"] else None,
                    },
                    "sent_at": str(row["sent_at_utc"]),
                }
            )
        next_cursor = None
        if has_more and selected:
            row = selected[-1]
            next_cursor = self.account_cursors.issue(
                kind="resources",
                reader_id=self.reader.reader_id,
                account_id=selected_account,
                scope_key=scope_key,
                policy_revision=policy_revision,
                position=[
                    str(row["sent_at_utc"]),
                    int(row["sort_seq"]),
                    int(row["sort_tie"]),
                    int(row["source_ordinal"]),
                    str(row["resource_id"]),
                ],
                snapshot={
                    "mode": "materialized_resources",
                    "observation_watermark": observation_watermark,
                },
            )
        receipt = self.resource_service._source_receipt(None, local=True)
        if replica_epoch is not None:
            receipt.update(view="replica", complete=False,
                           coverage={"conversation": "resident_subset"})
            receipt["freshness"].update(observation_watermark=observation_watermark,
                                        projection_epoch=replica_epoch)
        return {
            "schema": "sightglass.resource-search.v1",
            "items": items,
            "page": {"next_cursor": next_cursor, "has_more": has_more},
            "source_receipt": receipt,
        }

    def read_resource(
        self,
        *,
        resource_id: str,
        mode: str,
        page: int | None,
        start_line: int | None,
        end_line: int | None,
        max_bytes: int,
        member: str | None = None,
        sheet: str | None = None,
        cell_range: str | None = None,
        reading_token: str | None = None,
    ) -> ResourceReadPayload:
        if mode == "text":
            derived = self._voice_resource_text(
                resource_id,
                page=page,
                start_line=start_line,
                end_line=end_line,
                member=member,
                sheet=sheet,
                cell_range=cell_range,
                max_bytes=max_bytes,
            )
            if derived is not None:
                if reading_token is not None:
                    raise SightglassError(ErrorCode.QUERY_INVALID)
                return derived
        result = self.resource_service.read_resource(
            resource_id=resource_id,
            mode=mode,
            page=page,
            start_line=start_line,
            end_line=end_line,
            max_bytes=max_bytes,
            member=member,
            sheet=sheet,
            cell_range=cell_range,
            reading_token=reading_token,
        )
        if self.resource_service.local_read_ready(resource_id, mode):
            self._mark_resource_cache_ready()
        return result

    def search_resource_text(
        self,
        *,
        resource_id: str,
        query: str,
        limit: int,
    ) -> dict[str, Any]:
        return self.resource_service.search_resource_text(
            resource_id=resource_id,
            query=query,
            limit=limit,
        )

    def record_access_receipt(
        self,
        *,
        tool_name: str,
        conversation_id: str | None,
        scope_kind: str | None,
        scope_values: tuple[str, ...],
        result: dict[str, Any],
        started_at: str,
        binary_bytes_returned: int = 0,
    ) -> None:
        completed_at = utc_now().isoformat(timespec="microseconds")
        self.repository.upsert_reader(
            self.reader.reader_id,
            self.reader.display_name,
            self.reader.policy.as_dict(),
            completed_at,
        )
        body = json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        messages = result.get("messages", result.get("hits", []))
        message_count = len(messages) if isinstance(messages, list) else 0
        resource_count = sum(
            len(item.get("resources", []))
            for item in messages
            if isinstance(item, dict) and isinstance(item.get("resources", []), list)
        )
        resource_count += sum(
            int(item[5])
            for item in messages
            if isinstance(item, list) and len(item) == 6 and isinstance(item[5], int)
        )
        if isinstance(result.get("resources"), list):
            resource_count += len(result["resources"])
        if isinstance(result.get("resource"), dict):
            resource_count += 1
        outcome = "ok" if result.get("ok", True) is not False else str(result.get("code"))
        warning_codes: tuple[str, ...] = ()
        details = result.get("details")
        if isinstance(details, dict) and isinstance(details.get("warning_codes"), list):
            warning_codes = tuple(str(value) for value in details["warning_codes"])
        source_receipt = result.get("source_receipt")
        if isinstance(source_receipt, dict) and isinstance(source_receipt.get("warnings"), list):
            warning_codes = tuple(
                dict.fromkeys(
                    (*warning_codes, *(str(value) for value in source_receipt["warnings"]))
                )
            )
        if conversation_id is None:
            if isinstance(result.get("conversation_id"), str):
                conversation_id = result["conversation_id"]
            elif isinstance(result.get("conversation"), dict) and isinstance(
                result["conversation"].get("id"), str
            ):
                conversation_id = result["conversation"]["id"]
            elif isinstance(result.get("resource"), dict) and isinstance(
                result["resource"].get("conversation_id"), str
            ):
                conversation_id = result["resource"]["conversation_id"]
            elif isinstance(result.get("resources"), list) and result["resources"]:
                first_resource = result["resources"][0]
                if isinstance(first_resource, dict) and isinstance(
                    first_resource.get("conversation_id"), str
                ):
                    conversation_id = first_resource["conversation_id"]
        scope_digest = None
        if scope_values:
            scope_digest = self.token_codec.private_digest("access-scope.v1", {
                "reader": self.reader.reader_id, "scope": sorted(scope_values),
            })
        receipt_id = opaque_id(
            "wxreceipt", self.reader.reader_id, tool_name, started_at, completed_at
        )
        self.repository.record_access_receipt(
            receipt_id=receipt_id,
            reader_id=self.reader.reader_id,
            tool_name=tool_name,
            conversation_id=conversation_id,
            scope_kind=scope_kind,
            scope_digest=scope_digest,
            message_count=message_count,
            resource_count=resource_count,
            bytes_returned=len(body.encode("utf-8")) + int(binary_bytes_returned),
            started_at=started_at,
            completed_at=completed_at,
            outcome=outcome,
            warning_codes=warning_codes,
        )

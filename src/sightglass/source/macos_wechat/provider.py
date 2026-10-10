from __future__ import annotations

import base64
import contextvars
import hashlib
import heapq
import importlib
import json
import os
import re
import secrets
import stat
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import zstandard

from sightglass.contracts.capture import CaptureRequest, ResourceCaptureBinding
from sightglass.contracts.common import SourceSortKey, parse_aware_datetime, utc_now
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.identity import (
    ConversationCandidate,
    LabelObservation,
    SourceAccount,
    SourceConversation,
    SourceIdentityKey,
    SourceParticipant,
    SourceParticipantFilter,
)
from sightglass.contracts.messages import (
    SourceDiscoveryPage,
    SourceMessage,
    SourceMessagePage,
)
from sightglass.contracts.resources import SourceResourcePayload
from sightglass.operations import check_operation_budget, operation_expired, wait_for_event
from sightglass.source.base import (
    SourceHealth,
    SourcePreparationStep,
    SourceProviderDescriptor,
    SourceScope,
    SourceSnapshot,
    _check_preparation,
    _PreparationWindow,
)
from sightglass.source.message_identity import native_message_token

from .config import MacOSWeChatSettings
from .discovery import (
    WeChatCandidate,
    discover_candidates,
    discover_configured_candidate,
)
from .image_index import NativeImageIndex
from .keys import (
    AUXILIARY_DATABASE,
    decode_key_map,
    parse_image_decoder_key,
    parse_image_xor_key,
    read_keychain_secret,
    verify_page1,
)
from .media_index import MEDIA_DATABASE, NativeVoiceIndex
from .resources import NativeResourceResolver
from .sticker_key import load_sticker_decoder_key

_MESSAGE_DB = re.compile(r"^message/(?:biz_)?message_\d+\.db$")
_MESSAGE_TABLE = re.compile(r"^Msg_[0-9a-f]{32}$")
_MESSAGE_COLUMNS = {
    "local_id",
    "server_id",
    "local_type",
    "sort_seq",
    "create_time",
    "status",
    "message_content",
    "WCDB_CT_message_content",
    "packed_info_data",
}

# The supported macOS WeChat build uses 0/1 for received rows and 2/3 for
# account-sent rows. A content-free live lookup confirmed that the exact
# owner-sent row reported by named-host acceptance has status 3, so testing
# equality against only 2 silently reattributed it to the direct-chat peer.
_OUTGOING_MESSAGE_STATUSES = frozenset({2, 3})
_RECEIVED_MESSAGE_STATUSES = frozenset({0, 1})
# System and recall rows are conversation events, not utterances. Their stored
# sender is derived from the conversation, so they must stay free of human
# sender evidence instead of becoming a peer's message through the fallback.
_SYSTEM_MESSAGE_TYPES = frozenset({10000, 10002})

# A shard position page and a shard payload page are two different costs. The
# position query selects only ``rowid, create_time, sort_seq`` but still has to
# sort the encrypted shard; the payload query is an exact ``rowid IN (...)``
# point lookup. Decoupling them lets a page fetch a bounded *position* batch
# (one shard sort per batch) while still resolving payloads and resources one
# bounded payload page at a time, so a page of ``N`` selected rows no longer
# repeats a full shard sort ``N`` times. Position buffering never loads bodies.
_POSITION_PAGE_LIMIT = 256
_POSITION_PAGE_MAX = 20_003

# Only idle shared catalog handles count against this limit. Narrow sessions own
# and close their pinned handles separately; active/waiting users never get evicted.
NATIVE_IDLE_CONNECTION_LIMIT = 16
NATIVE_IDLE_CONNECTION_SECONDS = 60.0

# One bounded physical discovery pass. The reader owns matching and canonical
# validation; the provider only walks its own physical rowid order (descending,
# so recent physical work is likely first) and never claims chronology. The
# schema tag makes a stale/reused cursor fail closed instead of being
# reinterpreted, and the per-call raw-row cap bounds work for a sparse or empty
# time window.
DISCOVERY_POSITION_SCHEMA = "sightglass.macos-wechat.discovery-position.v1"
DISCOVERY_SCAN_CAP = 256
DISCOVERY_DEFAULT_LIMIT = 100

# The scoped session a thread is currently reading through, if any. Native reads that
# need a database use this to route to a session-pinned read transaction instead of
# the shared process-wide connection cache. It is a ``ContextVar`` so concurrent
# threads never observe each other's scope, and it stays empty for the historical
# catalog-wide ``snapshot()`` path.
_ACTIVE_SCOPED_SESSION: contextvars.ContextVar[_ScopedSession | None] = contextvars.ContextVar(
    "sightglass_active_scoped_session", default=None
)


def _message_direction(status: Any) -> str:
    """Classify a native row as ``outgoing``, ``incoming``, or ``unknown``.

    Unrecognized states stay ``unknown`` so callers fail closed rather than
    asserting a human sender that the row's direction evidence cannot support.
    """

    value = int(status or 0)
    if value in _OUTGOING_MESSAGE_STATUSES:
        return "outgoing"
    if value in _RECEIVED_MESSAGE_STATUSES:
        return "incoming"
    return "unknown"


def _base_message_type(value: Any) -> int:
    """Return the public WeChat message type without native high-word flags.

    Current macOS rows may pack an app-message subtype or other source flags into
    the upper 32 bits while the lower word remains the canonical type used by the
    parser and resource resolver (for example ``(5 << 32) | 49``).  The raw value
    still remains part of local/fallback source identity and read-back predicates;
    only the semantic projection is normalized here.
    """

    return int(value or 0) & 0xFFFFFFFF


@dataclass(frozen=True)
class _FileIdentity:
    relative: str
    device: int
    inode: int
    size: int
    mtime_ns: int
    page1_digest: str
    wal_device: int | None
    wal_inode: int | None
    wal_size: int | None
    wal_mtime_ns: int | None

    def generation_id(self) -> str:
        return hashlib.sha256(
            json.dumps(self.__dict__, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def logical_generation_id(self, key: str) -> str:
        return hashlib.sha256(
            json.dumps(
                {
                    "relative": self.relative,
                    "device": self.device,
                    "inode": self.inode,
                    "key_digest": hashlib.sha256(key.encode()).hexdigest(),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()


@dataclass
class _SnapshotState:
    identities: tuple[_FileIdentity, ...]
    generation_by_shard: dict[str, str]
    contacts: dict[str, tuple[str, str]] | None = None
    conversations: dict[str, SourceConversation] | None = None
    known_conversations: dict[str, SourceConversation] = field(default_factory=dict)
    message_shards: dict[str, tuple[str, ...]] = field(default_factory=dict)
    catalog_unresolved_tables: int | None = None
    auxiliary_identities: tuple[_FileIdentity, ...] = ()
    # A dependency-scoped session carries the typed scope it serves plus the
    # identities of databases this session opened lazily. The catalog-wide snapshot
    # keeps ``scope=None`` and a full ``identities`` tuple.
    scope: SourceScope | None = None
    scoped_identities: dict[str, _FileIdentity] = field(default_factory=dict)


@dataclass(frozen=True)
class _ContactCatalog:
    """Contact metadata and the exact native view that produced it."""

    contacts: dict[str, tuple[str, str]]
    identity: tuple[Any, ...]
    revision: tuple[Any, ...]


@dataclass
class _MessageShardCatalog:
    """Generation-bound structural facts read from one native message shard."""

    message_tables: frozenset[str]
    has_name2id: bool
    name2id_source_ids: frozenset[str]
    identity: tuple[Any, ...]
    revision: tuple[Any, ...]
    columns_by_table: dict[str, frozenset[str]] = field(default_factory=dict)


@dataclass
class _NegativeMessageRouting:
    """Target tables proved absent without selecting this shard's message bodies."""

    identity: tuple[Any, ...]
    revision: tuple[Any, ...]
    tables: set[str] = field(default_factory=set)


@dataclass
class _CachedSQLCipherConnection:
    """One identity-bound SQLCipher handle serialized across daemon threads."""

    connection: Any
    lock: Any = field(default_factory=threading.Lock)
    retired: bool = False
    users: int = 0
    last_used: float = field(default_factory=time.monotonic)
    closed: bool = False


@dataclass
class _ScopedSession:
    """A dependency-scoped native read session.

    Each database the read actually opens gets a session-owned read-only SQLCipher
    handle pinned with an explicit ``BEGIN`` read transaction, so every statement in
    the session observes one coherent committed view. The session records the exact
    database and file dependencies it touched and re-validates only those at exit: a
    selected database replacement, a key/page-1 binding change, or a selected target
    file mutation fails closed, while unrelated shard/WAL growth is ignored.
    """

    provider: MacOSWeChatSourceProvider
    scope: SourceScope
    state: _SnapshotState | None = None
    connections: dict[str, Any] = field(default_factory=dict)
    # relative -> (_connection_identity tuple) captured when the handle was opened.
    identities: dict[str, tuple[Any, ...]] = field(default_factory=dict)
    auxiliary: dict[str, bool] = field(default_factory=dict)
    # relative -> dependency revision captured at open. A change means another
    # connection committed to this selected database during the session.
    revisions: dict[str, tuple[Any, ...]] = field(default_factory=dict)
    # absolute path -> (device, inode, size, mtime_ns) captured when the file was read.
    files: dict[str, tuple[int, int, int, int]] = field(default_factory=dict)
    message_candidates: tuple[str, ...] | None = None
    negative_message_routing: dict[str, _NegativeMessageRouting] = field(default_factory=dict)

    def open(self) -> None:
        warnings = self.provider._binding_warnings()
        if warnings:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                retryable=True,
                details={"warning_codes": list(warnings)},
            )

    def connection(self, relative: str, *, auxiliary: bool) -> Any:
        existing = self.connections.get(relative)
        if existing is not None:
            return existing
        connection, identity, revision = self.provider._open_scoped_connection(
            relative, auxiliary=auxiliary
        )
        self.connections[relative] = connection
        self.identities[relative] = identity
        self.auxiliary[relative] = auxiliary
        self.revisions[relative] = revision
        if self.state is not None:
            try:
                file_identity = self.provider._identity(relative)
            except SightglassError:
                file_identity = None
            if file_identity is not None:
                self.state.scoped_identities[relative] = file_identity
                self.state.generation_by_shard.setdefault(relative, file_identity.generation_id())
        return connection

    def record_file(self, path: Path, metadata: Any) -> None:
        self.files[str(path)] = (
            int(metadata.st_dev),
            int(metadata.st_ino),
            int(metadata.st_size),
            int(metadata.st_mtime_ns),
        )

    def candidate_message_relatives(self) -> tuple[str, ...]:
        relatives = self.provider._message_relatives()
        for relative in relatives:
            if not self.provider._keys.get(relative, {}).get("enc_key"):
                raise SightglassError(
                    ErrorCode.SOURCE_INCOMPLETE,
                    retryable=True,
                    details={"warning_codes": ["source_key_missing_or_invalid"]},
                )
        if self.message_candidates is None:
            self.message_candidates = relatives
        elif relatives != self.message_candidates:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        return relatives

    def record_negative_message_table(
        self, relative: str, table: str, catalog: _MessageShardCatalog
    ) -> None:
        dependency = self.negative_message_routing.get(relative)
        tables = {table} | (dependency.tables if dependency is not None else set())
        revision = self.provider._dependency_revision(relative)
        identity = self.provider._connection_identity(relative, auxiliary=False)[0]
        if identity != catalog.identity or (
            dependency is not None and identity != dependency.identity
        ):
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        if revision != catalog.revision or (
            dependency is not None and revision != dependency.revision
        ):
            identity, revision = self.provider._recheck_negative_message_tables(relative, tables)
            if identity != catalog.identity:
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        if self.provider._dependency_revision(relative) != revision:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        self.negative_message_routing[relative] = _NegativeMessageRouting(
            identity, revision, tables
        )

    def validate(self) -> None:
        if self.provider._binding_warnings():
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        if self.message_candidates is not None:
            self.candidate_message_relatives()
        for relative, captured in sorted(self.identities.items()):
            try:
                current = self.provider._connection_identity(
                    relative, auxiliary=self.auxiliary[relative]
                )[0]
            except SightglassError as exc:
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True) from exc
            if current != captured:
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
            if self.provider._dependency_revision(relative) != self.revisions.get(relative):
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        routing_revisions: dict[str, tuple[Any, ...]] = {}
        for relative, dependency in sorted(self.negative_message_routing.items()):
            check_operation_budget()
            if relative in self.identities:
                # Its pinned view already has the stricter selected-database fence.
                continue
            try:
                identity = self.provider._connection_identity(relative, auxiliary=False)[0]
                revision = self.provider._dependency_revision(relative)
                if identity != dependency.identity:
                    raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
                if revision != dependency.revision:
                    identity, revision = self.provider._recheck_negative_message_tables(
                        relative, dependency.tables
                    )
                    if identity != dependency.identity:
                        raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
            except SightglassError as exc:
                if exc.code == ErrorCode.SOURCE_INCOMPLETE:
                    raise SightglassError(
                        ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True
                    ) from exc
                raise
            routing_revisions[relative] = revision
        for path, captured_identity in self.files.items():
            try:
                metadata = os.lstat(path)
            except OSError as exc:
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True) from exc
            current_identity = (
                int(metadata.st_dev),
                int(metadata.st_ino),
                int(metadata.st_size),
                int(metadata.st_mtime_ns),
            )
            if current_identity != captured_identity:
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        for relative, revision in routing_revisions.items():
            if self.provider._dependency_revision(relative) != revision:
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        if self.message_candidates is not None:
            self.candidate_message_relatives()

    def close(self) -> None:
        for connection in self.connections.values():
            try:
                connection.execute("ROLLBACK")
            except Exception:  # pragma: no cover - transaction may already be closed
                pass
            try:
                connection.close()
            except Exception:  # pragma: no cover - best-effort handle cleanup
                pass
            finally:
                with self.provider._connection_cache_lock:
                    self.provider._scoped_connection_count -= 1
        self.connections.clear()


class MacOSWeChatSourceProvider:
    """Read current local WeChat databases through read-only SQLCipher handles."""

    def __init__(
        self,
        settings_path: str | os.PathLike[str],
        *,
        secret_loader: Callable[[str], str] | None = None,
        candidate_discovery: Callable[[], tuple[WeChatCandidate, ...]] | None = None,
        image_key_loader: Callable[[str], str] | None = None,
        sticker_key_loader: Callable[[MacOSWeChatSettings], bytes | None] | None = None,
    ) -> None:
        self.settings = MacOSWeChatSettings.load(settings_path)
        load_secret = secret_loader or read_keychain_secret
        self._keys = decode_key_map(load_secret(self.settings.keychain_account))
        self._discover_candidates = candidate_discovery or discover_candidates
        self._use_configured_candidate_probe = candidate_discovery is None
        self._snapshots: dict[str, _SnapshotState] = {}
        self._message_shard_catalogs: dict[tuple[str, str, str], _MessageShardCatalog] = {}
        self._message_shard_catalog_builds: dict[tuple[str, str, str], threading.Event] = {}
        self._contacts_by_generation: dict[tuple[str, str, str], _ContactCatalog] = {}
        self._message_shard_catalog_lock = threading.RLock()
        self._connection_cache: dict[
            tuple[str, int, int, str, str], _CachedSQLCipherConnection
        ] = {}
        self._connection_builds: dict[tuple[str, int, int, str, str], threading.Event] = {}
        self._connection_cache_lock = threading.RLock()
        self._retiring_connections: dict[int, _CachedSQLCipherConnection] = {}
        self._scoped_connection_count = 0
        self._closed = False
        self._image_key_loader = image_key_loader or load_secret
        self._sticker_key_loader = sticker_key_loader or load_sticker_decoder_key
        image_decoder_key, image_xor_key = self._load_image_decoder_material()
        self._resource_resolver = NativeResourceResolver(
            self.settings.source_root,
            self.settings.source_account_binding_id,
            reader_timezone="Asia/Singapore",
            image_decoder_key=image_decoder_key,
            image_xor_key=image_xor_key,
            sticker_decoder_key=self._sticker_key_loader(self.settings),
            image_index=NativeImageIndex(open_database=self._connect_auxiliary),
            voice_index=NativeVoiceIndex(
                self.settings.source_root,
                databases=tuple(
                    sorted(
                        relative for relative in self._keys if MEDIA_DATABASE.fullmatch(relative)
                    )
                ),
                open_database=self._connect_auxiliary,
            ),
            dependency_recorder=self._record_resource_dependency,
        )

    def _load_image_decoder_material(self) -> tuple[bytes | None, int | None]:
        """Read one account-bound Keychain item; malformed material fails closed."""
        account = self.settings.image_keychain_account
        if not account:
            return None, None
        try:
            material = self._image_key_loader(account)
            return parse_image_decoder_key(material), parse_image_xor_key(material)
        except (RuntimeError, ValueError):
            return None, None

    @property
    def descriptor(self) -> SourceProviderDescriptor:
        return SourceProviderDescriptor(
            kind="macos-wechat",
            implementation="sightglass.macos-wechat.sqlcipher.v6",
            source_mode="live",
            platform=("darwin",),
            supports_incremental=True,
            supports_resources=True,
            requires_running_app_for_key_refresh=True,
            message_sender_evidence_complete=True,
        )

    def _expected_databases(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        values = {"contact/contact.db", "session/session.db"}
        values.update(relative for relative in self._keys if _MESSAGE_DB.fullmatch(relative))
        warnings: list[str] = []
        message_directory = self.settings.source_root / "message"
        try:
            metadata = message_directory.lstat()
            if message_directory.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
                raise OSError("message_directory_not_regular")
            values.update(
                f"message/{path.name}"
                for path in message_directory.iterdir()
                if _MESSAGE_DB.fullmatch(f"message/{path.name}")
            )
        except OSError:
            warnings.append("source_message_inventory_unreadable")
        return tuple(sorted(values)), tuple(warnings)

    def _identity(self, relative: str) -> _FileIdentity:
        path = self.settings.source_root / relative
        try:
            metadata = path.lstat()
            if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
                raise OSError("not_regular")
            descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            try:
                page1 = os.read(descriptor, 4096)
            finally:
                os.close(descriptor)
        except OSError as exc:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                retryable=True,
                details={"warning_codes": ["source_database_unreadable"]},
            ) from exc
        key = self._keys.get(relative, {}).get("enc_key", "")
        try:
            key_bytes = bytes.fromhex(key)
        except ValueError:
            key_bytes = b""
        if not key or not verify_page1(key_bytes, page1):
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                details={"warning_codes": ["source_key_missing_or_invalid"]},
            )
        wal = Path(f"{path}-wal")
        wal_values: tuple[int | None, int | None, int | None, int | None]
        try:
            wal_metadata = wal.lstat()
            if wal.is_symlink() or not stat.S_ISREG(wal_metadata.st_mode):
                raise OSError("wal_not_regular")
            wal_values = (
                int(wal_metadata.st_dev),
                int(wal_metadata.st_ino),
                int(wal_metadata.st_size),
                int(wal_metadata.st_mtime_ns),
            )
        except FileNotFoundError:
            wal_values = (None, None, None, None)
        except OSError as exc:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                retryable=True,
                details={"warning_codes": ["source_wal_unreadable"]},
            ) from exc
        return _FileIdentity(
            relative=relative,
            device=int(metadata.st_dev),
            inode=int(metadata.st_ino),
            size=int(metadata.st_size),
            mtime_ns=int(metadata.st_mtime_ns),
            page1_digest=hashlib.sha256(page1).hexdigest(),
            wal_device=wal_values[0],
            wal_inode=wal_values[1],
            wal_size=wal_values[2],
            wal_mtime_ns=wal_values[3],
        )

    def _inventory(
        self, *, check_binding: bool = True
    ) -> tuple[SourceHealth, tuple[_FileIdentity, ...]]:
        expected, discovery_warnings = self._expected_databases()
        warnings = list(discovery_warnings)
        identities: list[_FileIdentity] = []
        if not expected or not any(_MESSAGE_DB.fullmatch(value) for value in expected):
            warnings.append("source_message_shards_unavailable")
        if check_binding:
            warnings.extend(self._binding_warnings())
        for relative in expected:
            check_operation_budget()
            try:
                identities.append(self._identity(relative))
            except SightglassError as exc:
                warnings.extend(str(value) for value in exc.details.get("warning_codes", ()))
        basis = {
            "schema": "sightglass.macos-wechat.logical-inventory.v1",
            "source_account_binding_id": self.settings.source_account_binding_id,
            "source_account_key": self.settings.source_account_key,
            "candidate": {
                "bundle_id": self.settings.bundle_id,
                "version": self.settings.version,
                "build": self.settings.build,
                "architecture": self.settings.architecture,
                "profile_id": self.settings.profile_id,
            },
            "expected_databases": list(expected),
        }
        inventory_digest = hashlib.sha256(
            json.dumps(basis, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        generations = tuple(
            sorted(
                (
                    identity.relative,
                    identity.logical_generation_id(
                        self._keys.get(identity.relative, {}).get("enc_key", "")
                    ),
                )
                for identity in identities
            )
        )
        generation_set_digest = hashlib.sha256(
            json.dumps(generations, separators=(",", ":")).encode()
        ).hexdigest()
        latest_ns = max(
            (max(identity.mtime_ns, identity.wal_mtime_ns or 0) for identity in identities),
            default=0,
        )
        complete = len(identities) == len(expected) and not warnings
        health = SourceHealth(
            configured=True,
            available=complete,
            account_count=1 if complete else 0,
            source_state="complete" if complete else "incomplete",
            fresh_as_of=(
                datetime.fromtimestamp(latest_ns / 1_000_000_000, UTC).isoformat()
                if latest_ns
                else utc_now().isoformat()
            ),
            inventory_digest=inventory_digest,
            generation_set_digest=generation_set_digest,
            shard_counts={
                "present": len(identities),
                "missing": max(0, len(expected) - len(identities)),
                "key_missing": int("source_key_missing_or_invalid" in warnings),
                "cache_only": 0,
                "unreadable": int(bool(warnings)),
            },
            warnings=tuple(sorted(set(warnings))),
        )
        return health, tuple(identities)

    def _binding_warnings(self) -> list[str]:
        """Report a mismatched app/profile binding without scanning the databases.

        Shared by the catalog-wide inventory and by dependency-scoped sessions: a
        scoped read still refuses to run when the configured source candidate no
        longer matches the running app it was bound to.
        """

        if self._use_configured_candidate_probe:
            configured_candidate = discover_configured_candidate(self.settings.source_root)
            candidates = (configured_candidate,) if configured_candidate is not None else ()
        else:
            candidates = self._discover_candidates()
        matching = [
            item
            for item in candidates
            if item.source_root == self.settings.source_root
            and item.bundle_id == self.settings.bundle_id
            and item.version == self.settings.version
            and item.build == self.settings.build
            and item.architecture == self.settings.architecture
            and (
                item.profile_id == self.settings.profile_id
                if self.settings.profile_id
                else item.profile_id is not None
            )
        ]
        if len(matching) != 1:
            return ["source_build_binding_changed"]
        return []

    def health(self) -> SourceHealth:
        return self._inventory()[0]

    def _assert_snapshot(self, snapshot: SourceSnapshot) -> _SnapshotState:
        state = self._snapshots.get(snapshot.token)
        if state is None:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        return state

    @staticmethod
    def _state_identity(state: _SnapshotState, relative: str) -> _FileIdentity | None:
        identity = state.scoped_identities.get(relative)
        if identity is not None:
            return identity
        return next((item for item in state.identities if item.relative == relative), None)

    @staticmethod
    def _enforce_scope(
        state: _SnapshotState,
        *,
        account_id: str | None = None,
        conversation_source_id: str | None = None,
        source_message_id: str | None = None,
        source_resource_key: str | None = None,
    ) -> None:
        """Reject a read whose target is outside the session's declared scope.

        A narrow session may only serve the exact target it was opened for. The
        catalog-wide ``snapshot()`` (``scope is None``) keeps its historical behavior.
        """

        scope = state.scope
        if scope is None:
            return
        if source_resource_key is not None:
            if scope.kind != "resource" or scope.source_resource_key != source_resource_key:
                raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
            return
        if scope.kind == "resource":
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
        if account_id is not None and scope.account_id is not None:
            if account_id != scope.account_id:
                raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)
        if conversation_source_id is not None and scope.conversation_source_id is not None:
            if conversation_source_id != scope.conversation_source_id:
                raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        if scope.kind == "conversations" and conversation_source_id is not None:
            if conversation_source_id not in scope.conversation_source_ids:
                raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        if scope.kind == "message":
            if source_message_id is None or scope.source_message_id != source_message_id:
                raise SightglassError(ErrorCode.MESSAGE_NOT_FOUND)

    def _validate_snapshot(self, snapshot: SourceSnapshot) -> None:
        state = self._assert_snapshot(snapshot)
        health, identities = self._inventory(check_binding=True)
        if (
            not health.complete
            or health.inventory_digest != snapshot.inventory_digest
            or health.generation_set_digest != snapshot.generation_set_digest
            or identities != state.identities
            or self._auxiliary_identities() != state.auxiliary_identities
        ):
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)

    @contextmanager
    def snapshot(self) -> Iterator[SourceSnapshot]:
        health, identities = self._inventory(check_binding=True)
        if not health.complete:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                retryable=True,
                details={"warning_codes": list(health.warnings)},
            )
        token = secrets.token_urlsafe(24)
        generation_by_shard = {item.relative: item.generation_id() for item in identities}
        state = _SnapshotState(
            identities,
            generation_by_shard,
            auxiliary_identities=self._auxiliary_identities(),
        )
        self._prune_message_shard_catalogs(state)
        self._snapshots[token] = state
        snapshot = SourceSnapshot(
            inventory_digest=health.inventory_digest,
            generation_set_digest=health.generation_set_digest,
            fresh_as_of=health.fresh_as_of,
            generation_by_shard=tuple(sorted(generation_by_shard.items())),
            token=token,
        )
        try:
            yield snapshot
            self._validate_snapshot(snapshot)
        finally:
            self._snapshots.pop(token, None)

    @staticmethod
    def _scope_fingerprint(scope: SourceScope) -> str:
        evidence = {
            "schema": "sightglass.macos-wechat.scope.v1",
            "kind": scope.kind,
            "account_id": scope.account_id,
            "conversation_source_id": scope.conversation_source_id,
            "source_message_id": scope.source_message_id,
            "source_resource_key_digest": (
                hashlib.sha256(scope.source_resource_key.encode()).hexdigest()
                if scope.source_resource_key else None
            ),
        }
        if scope.kind == "conversations":
            evidence["conversation_source_ids"] = scope.conversation_source_ids
        return hashlib.sha256(
            json.dumps(
                evidence,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()

    @contextmanager
    def session(self, scope: SourceScope) -> Iterator[SourceSnapshot]:
        """Open a dependency-scoped read session for one explicit target.

        ``catalog`` scopes keep the catalog-wide snapshot semantics; every narrower
        scope pins a coherent read-only view of only the databases and files the read
        actually depends on, so unrelated shard/WAL growth cannot reject a coherent
        read. The session validates its selected dependencies again before it closes.
        """

        if scope.kind == "catalog":
            with self.snapshot() as snapshot:
                yield snapshot
            return
        session = _ScopedSession(self, scope)
        session.open()
        token = secrets.token_urlsafe(24)
        state = _SnapshotState((), {}, scope=scope)
        session.state = state
        self._snapshots[token] = state
        fingerprint = self._scope_fingerprint(scope)
        snapshot = SourceSnapshot(
            inventory_digest=fingerprint,
            generation_set_digest=fingerprint,
            fresh_as_of=utc_now().isoformat(),
            generation_by_shard=(),
            token=token,
            scope=scope,
        )
        reset = _ACTIVE_SCOPED_SESSION.set(session)
        try:
            yield snapshot
            session.validate()
        finally:
            _ACTIVE_SCOPED_SESSION.reset(reset)
            self._snapshots.pop(token, None)
            session.close()

    @contextmanager
    def _connect(self, relative: str) -> Iterator[Any]:
        if relative not in self._keys:
            raise SightglassError(ErrorCode.SOURCE_INCOMPLETE)
        session = _ACTIVE_SCOPED_SESSION.get()
        if session is not None:
            sqlite = importlib.import_module("sqlcipher3.dbapi2")
            connection = session.connection(relative, auxiliary=False)
            broken = False
            try:
                yield connection
            except sqlite.DatabaseError as exc:
                broken = True
                check_operation_budget()
                raise SightglassError(
                    ErrorCode.SOURCE_INCOMPLETE,
                    retryable=True,
                    details={"warning_codes": ["source_query_failed"]},
                ) from exc
            return
        slot, sqlite = self._connection_slot(relative, auxiliary=False)
        self._acquire_connection(slot)
        broken = False
        try:
            yield slot.connection
        except sqlite.DatabaseError as exc:
            broken = True
            check_operation_budget()
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                retryable=True,
                details={"warning_codes": ["source_query_failed"]},
            ) from exc
        finally:
            self._release_connection(slot, broken=broken)

    def _connection_identity(
        self,
        relative: str,
        *,
        auxiliary: bool,
    ) -> tuple[tuple[str, int, int, str, str], Path, str, bytes]:
        path = self.settings.source_root / relative
        try:
            descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            try:
                metadata = os.fstat(descriptor)
                if not stat.S_ISREG(metadata.st_mode):
                    raise OSError("database_not_regular")
                page = os.read(descriptor, 4096)
            finally:
                os.close(descriptor)
        except OSError as exc:
            if auxiliary:
                raise SightglassError(
                    ErrorCode.RESOURCE_UNAVAILABLE,
                    details={"reason": "image_mapping_unavailable"},
                ) from exc
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                retryable=True,
                details={"warning_codes": ["source_database_unreadable"]},
            ) from exc
        entry = self._keys.get(relative)
        if entry is None:
            if auxiliary:
                raise SightglassError(
                    ErrorCode.RESOURCE_UNAVAILABLE,
                    details={"reason": "image_mapping_unavailable"},
                )
            raise SightglassError(ErrorCode.SOURCE_INCOMPLETE)
        key = entry["enc_key"]
        if auxiliary and not verify_page1(bytes.fromhex(key), page):
            raise SightglassError(
                ErrorCode.RESOURCE_UNAVAILABLE,
                details={"reason": "image_mapping_unavailable"},
            )
        salt = page[:16]
        identity = (
            relative,
            int(metadata.st_dev),
            int(metadata.st_ino),
            salt.hex(),
            hashlib.sha256(key.encode()).hexdigest(),
        )
        return identity, path, key, salt

    def _connection_slot(
        self,
        relative: str,
        *,
        auxiliary: bool,
    ) -> tuple[_CachedSQLCipherConnection, Any]:
        identity, path, key, salt = self._connection_identity(relative, auxiliary=auxiliary)
        sqlite: Any = importlib.import_module("sqlcipher3.dbapi2")
        while True:
            with self._connection_cache_lock:
                if self._closed:
                    raise SightglassError(
                        ErrorCode.RESOURCE_UNAVAILABLE if auxiliary else ErrorCode.SOURCE_INCOMPLETE
                    )
                cached = self._connection_cache.get(identity)
                if cached is not None:
                    cached.users += 1
                    return cached, sqlite
                build = self._connection_builds.get(identity)
                if build is None:
                    build = threading.Event()
                    self._connection_builds[identity] = build
                    break
            wait_for_event(build)

        connection = None
        try:
            connection = sqlite.connect(
                f"file:{quote(str(path), safe='/')}?mode=ro",
                uri=True,
                check_same_thread=False,
            )
            connection.row_factory = sqlite.Row
            connection.set_progress_handler(lambda: int(operation_expired()), 1_000)
            connection.execute(f'''PRAGMA key = "x'{key}{salt.hex()}'"''')
            connection.execute("PRAGMA query_only = ON")
            connection.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
            check_operation_budget()
            slot = _CachedSQLCipherConnection(connection)
            if self._connection_identity(relative, auxiliary=auxiliary)[0] != identity:
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        except sqlite.DatabaseError as exc:
            with self._connection_cache_lock:
                self._connection_builds.pop(identity, None)
                build.set()
            if connection is not None:
                connection.close()
            check_operation_budget()
            if auxiliary:
                raise SightglassError(
                    ErrorCode.RESOURCE_UNAVAILABLE,
                    details={"reason": "image_mapping_unavailable"},
                ) from exc
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                retryable=True,
                details={"warning_codes": ["source_query_failed"]},
            ) from exc
        except BaseException:
            with self._connection_cache_lock:
                self._connection_builds.pop(identity, None)
                build.set()
            if connection is not None:
                connection.close()
            raise
        closing = []
        with self._connection_cache_lock:
            rejected = self._closed
            if rejected:
                slot.retired = True
                closing.extend(self._close_idle_connection(slot))
            else:
                for stale_identity, old in tuple(self._connection_cache.items()):
                    if stale_identity[0] == relative and stale_identity != identity:
                        self._connection_cache.pop(stale_identity)
                        old.retired = True
                        closing.extend(self._close_idle_connection(old))
                slot.users = 1
                self._connection_cache[identity] = slot
                closing.extend(self._prune_idle_connections())
            self._connection_builds.pop(identity, None)
            build.set()
        try:
            self._close_connections(closing)
        except BaseException:
            if not rejected:
                # Publication reserved this caller before waking build waiters.
                # A failed retirement close must not strand that reservation.
                with self._connection_cache_lock:
                    slot.users -= 1
                    slot.last_used = time.monotonic()
            raise
        if rejected:
            raise SightglassError(
                ErrorCode.RESOURCE_UNAVAILABLE if auxiliary else ErrorCode.SOURCE_INCOMPLETE
            )
        return slot, sqlite

    def _acquire_connection(
        self, slot: _CachedSQLCipherConnection, *, auxiliary: bool = False
    ) -> None:
        acquired = False
        try:
            while not slot.lock.acquire(timeout=0.05):
                check_operation_budget()
            acquired = True
            check_operation_budget()
            with self._connection_cache_lock:
                if slot.retired or self._closed:
                    raise SightglassError(
                        (
                            ErrorCode.RESOURCE_UNAVAILABLE
                            if auxiliary else ErrorCode.SOURCE_INCOMPLETE
                        ),
                        retryable=not auxiliary,
                    )
        except BaseException:
            with self._connection_cache_lock:
                slot.users -= 1
                closing = self._close_idle_connection(slot)
            if acquired:
                slot.lock.release()
            self._close_connections(closing)
            raise

    def _close_idle_connection(self, slot: _CachedSQLCipherConnection) -> list[Any]:
        """Claim a retired zero-user close under the registry lock; close outside it."""
        if slot.retired and slot.users:
            self._retiring_connections[id(slot)] = slot
        if slot.retired and slot.users == 0 and not slot.closed:
            slot.closed = True
            self._retiring_connections.pop(id(slot), None)
            return [slot.connection]
        return []

    @staticmethod
    def _close_connections(connections: list[Any]) -> None:
        first_error: BaseException | None = None
        for connection in connections:
            try:
                connection.close()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def _prune_idle_connections(self) -> list[Any]:
        """Called with the cache lock held; retire idle age/LRU excess only."""
        now = time.monotonic()
        idle = sorted(
            ((key, slot) for key, slot in self._connection_cache.items() if not slot.users),
            key=lambda item: item[1].last_used,
        )
        excess = max(0, len(idle) - NATIVE_IDLE_CONNECTION_LIMIT)
        closing = []
        for ordinal, (key, slot) in enumerate(idle):
            if ordinal >= excess and now - slot.last_used < NATIVE_IDLE_CONNECTION_SECONDS:
                continue
            self._connection_cache.pop(key)
            slot.retired = True
            closing.extend(self._close_idle_connection(slot))
        return closing

    def maintain_idle_connections(self) -> int:
        """Content-free cache maintenance; never opens or validates a source file."""
        with self._connection_cache_lock:
            closing = self._prune_idle_connections()
        self._close_connections(closing)
        return len(closing)

    def connection_cache_status(self) -> dict[str, int]:
        with self._connection_cache_lock:
            slots = tuple(self._connection_cache.values())
            retiring = tuple(self._retiring_connections.values())
            return {
                "shared_handles": len(slots),
                "retiring_handles": len(retiring),
                "idle_handles": sum(slot.users == 0 for slot in slots),
                "active_or_waiting_users": sum(slot.users for slot in (*slots, *retiring)),
                "building_handles": len(self._connection_builds),
                "scoped_handles": self._scoped_connection_count,
                "idle_handle_limit": NATIVE_IDLE_CONNECTION_LIMIT,
            }

    def _release_connection(
        self,
        slot: _CachedSQLCipherConnection,
        *,
        broken: bool,
    ) -> None:
        try:
            with self._connection_cache_lock:
                if broken:
                    stale = [key for key, value in self._connection_cache.items() if value is slot]
                    for key in stale:
                        self._connection_cache.pop(key, None)
                    slot.retired = True
                slot.users -= 1
                slot.last_used = time.monotonic()
                closing = self._close_idle_connection(slot)
                closing.extend(self._prune_idle_connections())
        finally:
            slot.lock.release()
        self._close_connections(closing)

    def close(self) -> None:
        """Retire cached read-only handles without racing active source reads."""

        with self._connection_cache_lock:
            if self._closed:
                return
            self._closed = True
            slots = tuple({id(value): value for value in self._connection_cache.values()}.values())
            self._connection_cache.clear()
            closing = []
            for slot in slots:
                slot.retired = True
                closing.extend(self._close_idle_connection(slot))
        self._close_connections(closing)

    def _auxiliary_identities(self) -> tuple[_FileIdentity, ...]:
        """Present, page-1-verified optional resource databases for this snapshot.

        An absent database, an unenrolled key, or an unverified page 1 simply leaves
        the affected resource unavailable while the message source stays complete.
        """
        values: list[_FileIdentity] = []
        for relative in sorted(self._keys):
            if AUXILIARY_DATABASE.fullmatch(relative) is None:
                continue
            try:
                values.append(self._identity(relative))
            except SightglassError:
                continue
        return tuple(values)

    @contextmanager
    def _connect_auxiliary(self, relative: str) -> Iterator[Any]:
        """Open one optional auxiliary mapping database, or fail that mapping.

        The key must already be enrolled, the file must still be a regular file this
        process may open without following a link, and its page 1 must verify against
        that key before a read-only connection is handed out.
        """
        session = _ACTIVE_SCOPED_SESSION.get()
        if session is not None:
            sqlite = importlib.import_module("sqlcipher3.dbapi2")
            connection = session.connection(relative, auxiliary=True)
            try:
                yield connection
            except sqlite.DatabaseError as exc:
                check_operation_budget()
                raise SightglassError(
                    ErrorCode.RESOURCE_UNAVAILABLE,
                    details={"reason": "image_mapping_unavailable"},
                ) from exc
            return
        slot, sqlite = self._connection_slot(relative, auxiliary=True)
        self._acquire_connection(slot, auxiliary=True)
        broken = False
        try:
            yield slot.connection
        except sqlite.DatabaseError as exc:
            broken = True
            check_operation_budget()
            raise SightglassError(
                ErrorCode.RESOURCE_UNAVAILABLE,
                details={"reason": "image_mapping_unavailable"},
            ) from exc
        finally:
            self._release_connection(slot, broken=broken)

    def _dependency_revision(self, relative: str) -> tuple[Any, ...]:
        """One per-database revision for a selected dependency.

        Combines the main database identity with the ``-wal`` sidecar identity, so a
        selected database commit, checkpoint, or replacement is visible even though
        an unrelated shard's WAL growth stays invisible.
        A zero-byte WAL is equivalent to an absent WAL: opening a read-only handle
        can create that empty sidecar without changing any source content.
        """

        path = self.settings.source_root / relative
        values: dict[str, Any] = {"main": self._identity_stat(path)}
        wal = self._identity_stat(Path(f"{path}-wal"))
        values["wal"] = wal if wal is not None and wal[2] else None
        return (
            values["main"],
            values["wal"],
        )

    @staticmethod
    def _identity_stat(path: Path) -> tuple[int, int, int, int] | None:
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return None
        except OSError:
            return None
        if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
            return None
        return (
            int(metadata.st_dev),
            int(metadata.st_ino),
            int(metadata.st_size),
            int(metadata.st_mtime_ns),
        )

    def _open_scoped_connection(
        self, relative: str, *, auxiliary: bool
    ) -> tuple[Any, tuple[Any, ...], tuple[Any, ...]]:
        """Open one session-owned read-only SQLCipher handle and pin its view.

        The handle is a fresh connection outside the process-wide cache because the
        session holds an explicit read transaction for its whole lifetime. The
        ``mode=ro`` URI and ``query_only`` pragma stay in force, and the enrolled key
        plus verified page-1 salt are the only key material used.

        The path identity is captured as one fenced operation: a held ``O_NOFOLLOW``
        descriptor is compared against the path (and hence the handle) before and
        after the connection and its ``BEGIN`` read transaction are established. A
        replacement that swaps the file between opening the handle and binding the
        recorded identity therefore fails closed instead of binding a different file.
        The main/WAL revision is captured before the read view is pinned and checked
        again after opening, so a concurrent commit cannot bind an old view to a new
        revision. Session exit revalidates that same captured revision.
        """

        if self._closed:
            raise SightglassError(
                ErrorCode.RESOURCE_UNAVAILABLE if auxiliary else ErrorCode.SOURCE_INCOMPLETE
            )
        pre_identity, path, key, salt = self._connection_identity(relative, auxiliary=auxiliary)
        pre_revision = self._dependency_revision(relative)
        sqlite: Any = importlib.import_module("sqlcipher3.dbapi2")
        try:
            descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except OSError as exc:  # pragma: no cover - raced replacement/removal
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True) from exc
        connection = None
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
            connection = sqlite.connect(
                f"file:{quote(str(path), safe='/')}?mode=ro",
                uri=True,
                check_same_thread=False,
            )
            connection.row_factory = sqlite.Row
            connection.set_progress_handler(lambda: int(operation_expired()), 1_000)
            connection.execute(f'''PRAGMA key = "x'{key}{salt.hex()}'"''')
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")
            connection.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
            path_after = os.stat(path, follow_symlinks=False)
            if self._dependency_revision(relative) != pre_revision:
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        except SightglassError:
            if connection is not None:
                connection.close()
            raise
        except sqlite.DatabaseError as exc:
            if connection is not None:
                connection.close()
            check_operation_budget()
            if auxiliary:
                raise SightglassError(
                    ErrorCode.RESOURCE_UNAVAILABLE,
                    details={"reason": "image_mapping_unavailable"},
                ) from exc
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                retryable=True,
                details={"warning_codes": ["source_query_failed"]},
            ) from exc
        except BaseException:
            if connection is not None:
                connection.close()
            raise
        finally:
            os.close(descriptor)
        if (
            int(metadata.st_dev),
            int(metadata.st_ino),
        ) != (int(path_after.st_dev), int(path_after.st_ino)):
            if connection is not None:
                connection.close()
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        with self._connection_cache_lock:
            rejected = self._closed
            if not rejected:
                self._scoped_connection_count += 1
        if rejected:
            connection.close()
            raise SightglassError(
                ErrorCode.RESOURCE_UNAVAILABLE if auxiliary else ErrorCode.SOURCE_INCOMPLETE,
                retryable=not auxiliary,
            )
        return connection, pre_identity, pre_revision

    def _record_resource_dependency(self, path: Path, metadata: Any) -> None:
        session = _ACTIVE_SCOPED_SESSION.get()
        if session is not None:
            session.record_file(path, metadata)

    def _recheck_negative_message_tables(
        self, relative: str, tables: set[str]
    ) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
        """Recheck changed routing facts in a new read-only view, outside caches.

        This handle does not select message bodies or contribute a cursor logical
        generation. Unrelated WAL commits may leave all declared target tables
        absent, but a table appearance or a mutation while checking fails closed.
        """

        connection, identity, revision = self._open_scoped_connection(relative, auxiliary=False)
        sqlite: Any = importlib.import_module("sqlcipher3.dbapi2")
        try:
            for table in sorted(tables):
                check_operation_budget()
                if connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
                ).fetchone() is not None:
                    raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
            if (
                self._dependency_revision(relative) != revision
                or self._connection_identity(relative, auxiliary=False)[0] != identity
            ):
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
            return identity, revision
        except sqlite.DatabaseError as exc:
            check_operation_budget()
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True) from exc
        finally:
            try:
                connection.execute("ROLLBACK")
            finally:
                try:
                    connection.close()
                finally:
                    with self._connection_cache_lock:
                        self._scoped_connection_count -= 1

    def _require_account(self, account_id: str) -> SourceAccount:
        account = SourceAccount(
            source_namespace="macos-wechat",
            source_account_key=self.settings.source_account_key,
            self_principal_key=f"self:{self.settings.source_account_key}",
            display_name="Local WeChat",
            reader_timezone="Asia/Singapore",
            identity_confidence="strong",
            account_binding_id=self.settings.source_account_binding_id,
        )
        if account_id != account.source_account_key:
            raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)
        return account

    def list_accounts(self, snapshot: SourceSnapshot) -> list[SourceAccount]:
        self._assert_snapshot(snapshot)
        return [self._require_account(self.settings.source_account_key)]

    def _contacts(self, state: _SnapshotState) -> dict[str, tuple[str, str]]:
        if state.contacts is not None:
            return state.contacts
        relative = "contact/contact.db"
        # A cache hit still consumes this database's metadata. Pin and record its
        # current session view before comparing cache provenance, so a cached
        # contact/WAL correction cannot evade exit validation.
        with self._connect(relative) as connection:
            identity = self._connection_identity(relative, auxiliary=False)[0]
            revision = self._dependency_revision(relative)
            session = _ACTIVE_SCOPED_SESSION.get()
            if session is not None and (
                session.identities.get(relative) != identity
                or session.revisions.get(relative) != revision
            ):
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
            cache_key = self._message_shard_catalog_key(state, relative)
            with self._message_shard_catalog_lock:
                cached = self._contacts_by_generation.get(cache_key)
            if cached is not None and (
                cached.identity == identity and cached.revision == revision
            ):
                catalog = cached
            else:
                rows = connection.execute(
                    "SELECT username, nick_name, remark FROM contact"
                ).fetchall()
                contacts = {
                    str(row["username"]): (str(row["nick_name"] or ""), str(row["remark"] or ""))
                    for row in rows
                    if row["username"]
                }
                catalog = _ContactCatalog(contacts, identity, revision)
            if (
                self._dependency_revision(relative) != revision
                or self._connection_identity(relative, auxiliary=False)[0] != identity
            ):
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        with self._message_shard_catalog_lock:
            # An older in-flight cache entry must never win over newly read facts
            # merely because both lookups used the same pre-open generation key.
            self._contacts_by_generation = {cache_key: catalog}
        state.contacts = catalog.contacts
        return catalog.contacts

    def _message_shard_catalog_key(
        self, state: _SnapshotState, relative: str
    ) -> tuple[str, str, str]:
        identity = self._state_identity(state, relative)
        if identity is None:
            if state.scope is None:
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
            identity = self._identity(relative)
            state.scoped_identities[relative] = identity
        generation = state.generation_by_shard.get(relative) or identity.generation_id()
        if generation is None:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        key = self._keys.get(relative, {}).get("enc_key", "")
        return (
            relative,
            generation,
            identity.logical_generation_id(key),
        )

    def _prune_message_shard_catalogs(self, state: _SnapshotState) -> None:
        active = {
            self._message_shard_catalog_key(state, relative)
            for relative in state.generation_by_shard
            if _MESSAGE_DB.fullmatch(relative)
        }
        with self._message_shard_catalog_lock:
            self._message_shard_catalogs = {
                key: value for key, value in self._message_shard_catalogs.items() if key in active
            }
            contact_key = self._message_shard_catalog_key(state, "contact/contact.db")
            self._contacts_by_generation = {
                key: value
                for key, value in self._contacts_by_generation.items()
                if key == contact_key
            }

    def _message_shard_catalog(self, state: _SnapshotState, relative: str) -> _MessageShardCatalog:
        cache_key = self._message_shard_catalog_key(state, relative)
        while True:
            with self._message_shard_catalog_lock:
                cached = self._message_shard_catalogs.get(cache_key)
                if cached is not None:
                    return cached
                build = self._message_shard_catalog_builds.get(cache_key)
                if build is None:
                    build = threading.Event()
                    self._message_shard_catalog_builds[cache_key] = build
                    break
            wait_for_event(build)

        try:
            revision = self._dependency_revision(relative)
            identity = self._connection_identity(relative, auxiliary=False)[0]
            with self._connect(relative) as connection:
                message_tables = frozenset(
                    str(row[0])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Msg_%'"
                    )
                    if _MESSAGE_TABLE.fullmatch(str(row[0]))
                )
                has_name2id = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'Name2Id'"
                ).fetchone()
                name2id_source_ids = (
                    frozenset(
                        str(row[0])
                        for row in connection.execute(
                            "SELECT user_name FROM Name2Id WHERE user_name IS NOT NULL"
                        )
                        if str(row[0] or "")
                    )
                    if has_name2id is not None
                    else frozenset()
                )
            session = _ACTIVE_SCOPED_SESSION.get()
            if (
                self._dependency_revision(relative) != revision
                or self._connection_identity(relative, auxiliary=False)[0] != identity
                or (session is not None and session.revisions.get(relative) != revision)
            ):
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
            catalog = _MessageShardCatalog(
                message_tables=message_tables,
                has_name2id=has_name2id is not None,
                name2id_source_ids=name2id_source_ids,
                identity=identity,
                revision=revision,
            )
        except BaseException:
            with self._message_shard_catalog_lock:
                self._message_shard_catalog_builds.pop(cache_key, None)
                build.set()
            raise
        with self._message_shard_catalog_lock:
            catalog = self._message_shard_catalogs.setdefault(cache_key, catalog)
            self._message_shard_catalog_builds.pop(cache_key, None)
            build.set()
            return catalog

    def _message_table_columns(
        self,
        state: _SnapshotState,
        relative: str,
        table: str,
        catalog: _MessageShardCatalog,
    ) -> frozenset[str]:
        with self._message_shard_catalog_lock:
            cached = catalog.columns_by_table.get(table)
            if cached is not None:
                return cached
        # Reconfirm that this catalog still belongs to the requested snapshot;
        # an older in-flight snapshot may retain its object after cache pruning.
        self._message_shard_catalog_key(state, relative)
        with self._connect(relative) as connection:
            columns = frozenset(
                str(row["name"]) for row in connection.execute(f"PRAGMA table_info([{table}])")
            )
        with self._message_shard_catalog_lock:
            existing = catalog.columns_by_table.get(table)
            if existing is not None:
                return existing
            catalog.columns_by_table[table] = columns
            return columns

    def _real_sender_expression(
        self,
        state: _SnapshotState,
        relative: str,
        table: str,
        *,
        row_alias: str,
    ) -> str:
        """SQL expression for the stable group sender mapped by ``real_sender_id``.

        Older direct-chat fixtures and unsupported source shapes may lack ``Name2Id``;
        those rows deliberately project no mapped sender instead of guessing from text.
        Table and alias names come only from the provider's validated schema inventory.
        """

        catalog = self._message_shard_catalog(state, relative)
        columns = self._message_table_columns(state, relative, table, catalog)
        if not catalog.has_name2id or "real_sender_id" not in columns:
            return "NULL"
        return (
            "(SELECT sender_name.user_name FROM Name2Id AS sender_name "
            f"WHERE sender_name.rowid = {row_alias}.real_sender_id LIMIT 1)"
        )

    def _raw_sender_expression(
        self, state: _SnapshotState, relative: str, table: str,
    ) -> str:
        # Even an unmapped sender ID is evidence: two unknown mappings cannot
        # prove equivalent text copies when their raw source IDs differ.
        catalog = self._message_shard_catalog(state, relative)
        columns = self._message_table_columns(state, relative, table, catalog)
        return "message_row.real_sender_id" if "real_sender_id" in columns else "NULL"

    def _group_sender_sql_filter(
        self,
        state: _SnapshotState,
        relative: str,
        table: str,
        conversation: SourceConversation,
        filters: tuple[SourceParticipantFilter, ...],
        *,
        row_alias: str,
    ) -> tuple[str, tuple[str, ...]] | None:
        """Return a safe source-side superset for stable group-sender filters.

        A projected group sender can match an ``internal_username`` filter only when
        the row's ``real_sender_id`` maps to that username. The visible envelope is
        still verified in Python, so this SQL predicate narrows work without becoming
        the identity authority. Mixed, direct-chat, self, or message-id filters retain
        the general bounded traversal path.
        """

        if conversation.kind != "group" or not filters:
            return None
        catalog = self._message_shard_catalog(state, relative)
        columns = self._message_table_columns(state, relative, table, catalog)
        if not catalog.has_name2id or "real_sender_id" not in columns:
            return None
        values: set[str] = set()
        for participant_filter in filters:
            value = participant_filter.key_value
            if (
                participant_filter.source_message_id is not None
                or participant_filter.key_kind != "internal_username"
                or not participant_filter.principal_eligible
                or not value
                or value.startswith("self:")
                or participant_filter.scope_conversation_source_id
                not in {None, conversation.source_conversation_id}
            ):
                return None
            values.add(value)
        if not values:
            return None
        selected = tuple(sorted(values))
        placeholders = ",".join("?" for _value in selected)
        return (
            f"{row_alias}.real_sender_id IN ("
            f"SELECT rowid FROM Name2Id WHERE user_name IN ({placeholders})"
            ")",
            selected,
        )

    @staticmethod
    def _timestamp(value: Any) -> str | None:
        try:
            seconds = int(value or 0)
        except (TypeError, ValueError):
            return None
        if not seconds:
            return None
        return datetime.fromtimestamp(seconds, UTC).isoformat(timespec="microseconds")

    def _conversations(self, state: _SnapshotState) -> dict[str, SourceConversation]:
        if state.conversations is not None:
            return state.conversations
        contacts = self._contacts(state)
        with self._connect("session/session.db") as connection:
            rows = connection.execute(
                """
                SELECT username, last_timestamp, COALESCE(unread_count, 0) AS unread_count
                FROM SessionTable
                WHERE last_timestamp > 0
                ORDER BY last_timestamp DESC, username ASC
                """
            ).fetchall()
        sessions = {str(row["username"]): row for row in rows if str(row["username"] or "")}
        observed_tables: set[str] = set()
        name2id_source_ids: set[str] = set()
        for relative in self._candidate_message_relatives(state):
            check_operation_budget()
            catalog = self._message_shard_catalog(state, relative)
            observed_tables.update(catalog.message_tables)
            name2id_source_ids.update(catalog.name2id_source_ids)
        known_source_ids = set(contacts) | set(sessions) | name2id_source_ids
        table_to_source_id = {
            self._table_name(source_id): source_id for source_id in known_source_ids
        }
        history_source_ids = {
            table_to_source_id[table] for table in observed_tables if table in table_to_source_id
        }
        state.catalog_unresolved_tables = len(observed_tables - set(table_to_source_id))
        result: dict[str, SourceConversation] = {}
        # SessionTable also contains UI container rows (for example aggregate
        # folders) that have timestamps and contact labels but no Msg_* table.
        # Only a source id backed by an observed message table is a readable
        # conversation; history-only tables remain discoverable through the
        # contact/Name2Id mapping above.
        for username in sorted(history_source_ids):
            check_operation_budget()
            row = sessions.get(username)
            nickname, remark = contacts.get(username, ("", ""))
            group = "@chatroom" in username
            title = remark or nickname or ("未命名群聊" if group else "微信会话")
            aliases = tuple(value for value in (nickname, remark) if value and value != title)
            result[username] = SourceConversation(
                source_conversation_id=username,
                kind="group" if group else "direct",
                title=title,
                aliases=tuple(dict.fromkeys(aliases)),
                last_message_at_utc=(
                    self._timestamp(row["last_timestamp"]) if row is not None else None
                ),
                roster_complete=False,
                unread_count=max(0, int(row["unread_count"] or 0)) if row is not None else 0,
                catalog_state="active_session" if row is not None else "history_only",
            )
        state.conversations = result
        return result

    def _conversation(
        self,
        state: _SnapshotState,
        conversation_source_id: str,
    ) -> SourceConversation | None:
        """Resolve one known target without constructing the account-wide catalog."""

        if state.conversations is not None:
            return state.conversations.get(conversation_source_id)
        cached = state.known_conversations.get(conversation_source_id)
        if cached is not None:
            return cached
        with self._connect("session/session.db") as connection:
            rows = connection.execute(
                "SELECT username, last_timestamp, COALESCE(unread_count, 0) AS unread_count "
                "FROM SessionTable WHERE username = ? LIMIT 2",
                (conversation_source_id,),
            ).fetchall()
        if len(rows) > 1:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                details={"warning_codes": ["source_conversation_ambiguous"]},
            )
        table = self._table_name(conversation_source_id)
        if not self._message_shards(state, table):
            return None
        row = rows[0] if rows else None
        nickname, remark = self._contacts(state).get(conversation_source_id, ("", ""))
        group = "@chatroom" in conversation_source_id
        title = remark or nickname or ("未命名群聊" if group else "微信会话")
        aliases = tuple(value for value in (nickname, remark) if value and value != title)
        result = SourceConversation(
            source_conversation_id=conversation_source_id,
            kind="group" if group else "direct",
            title=title,
            aliases=tuple(dict.fromkeys(aliases)),
            last_message_at_utc=(
                self._timestamp(row["last_timestamp"]) if row is not None else None
            ),
            roster_complete=False,
            unread_count=max(0, int(row["unread_count"] or 0)) if row is not None else 0,
            catalog_state="active_session" if row is not None else "history_only",
        )
        state.known_conversations[conversation_source_id] = result
        return result

    def get_conversation(
        self,
        account_id: str,
        conversation_source_id: str,
        snapshot: SourceSnapshot,
    ) -> SourceConversation | None:
        """Resolve one target without opening unrelated conversation shards."""
        state = self._assert_snapshot(snapshot)
        self._require_account(account_id)
        self._enforce_scope(
            state,
            account_id=account_id,
            conversation_source_id=conversation_source_id,
        )
        return self._conversation(state, conversation_source_id)

    def list_conversations(
        self, account_id: str, snapshot: SourceSnapshot
    ) -> list[SourceConversation]:
        state = self._assert_snapshot(snapshot)
        self._require_account(account_id)
        return sorted(
            self._conversations(state).values(),
            key=lambda item: (item.last_message_at_utc or "", item.source_conversation_id),
            reverse=True,
        )

    def resolve_conversation(
        self, account_id: str, query: str, snapshot: SourceSnapshot
    ) -> list[ConversationCandidate]:
        folded = str(query or "").casefold()
        result: list[ConversationCandidate] = []
        for conversation in self.list_conversations(account_id, snapshot):
            if not folded:
                result.append(
                    ConversationCandidate(conversation, conversation.title, "recent_activity")
                )
                continue
            for value, kind in (
                (conversation.title, "title"),
                *((alias, "alias") for alias in conversation.aliases),
            ):
                if folded in value.casefold():
                    result.append(ConversationCandidate(conversation, value, kind))
                    break
        return result

    def _participant(
        self,
        conversation_id: str,
        source_id: str,
        label: str | None,
        observed_at: str,
        *,
        label_kind: str | None = None,
        is_self: bool = False,
        last_spoke_at: str | None = None,
    ) -> SourceParticipant:
        labels = ()
        if label:
            labels = (
                LabelObservation(
                    label=label,
                    label_kind=label_kind or "account_nickname",
                    scope="account",
                    provenance="macos-wechat.current-contact",
                    observed_at_utc=observed_at,
                    temporal_confidence="current_only",
                ),
            )
        return SourceParticipant(
            source_conversation_id=conversation_id,
            identity_keys=(
                SourceIdentityKey(
                    "internal_username",
                    source_id,
                    "stable",
                    True,
                    "macos-wechat.message-or-account",
                ),
            ),
            labels=labels,
            is_self=is_self,
            resolution_state="stable",
            identity_confidence="strong" if is_self else "exact",
            last_spoke_at_utc=last_spoke_at,
            account_labels_complete=False,
            membership_labels_complete=False,
        )

    def list_participants(
        self,
        account_id: str,
        conversation_source_id: str,
        snapshot: SourceSnapshot,
    ) -> list[SourceParticipant]:
        state = self._assert_snapshot(snapshot)
        account = self._require_account(account_id)
        self._enforce_scope(state, account_id=account_id,
                            conversation_source_id=conversation_source_id)
        conversation = self._conversation(state, conversation_source_id)
        if conversation is None:
            raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        participants: dict[str, SourceParticipant] = {
            account.self_principal_key: self._participant(
                conversation_source_id,
                account.self_principal_key,
                "我",
                snapshot.fresh_as_of,
                label_kind="account_nickname",
                is_self=True,
            )
        }
        contacts = self._contacts(state)
        if conversation.kind == "direct":
            nickname, remark = contacts.get(conversation_source_id, ("", ""))
            participants[conversation_source_id] = self._participant(
                conversation_source_id,
                conversation_source_id,
                remark or nickname or None,
                snapshot.fresh_as_of,
                label_kind="contact_remark" if remark else "account_nickname",
            )
        # The roster needs exact sender keys and current labels, not resources.
        # Reuse canonical message parsing without resolving each row's attachments.
        recent = self._messages(
            account_id,
            conversation_source_id,
            snapshot,
            direction="backward",
            limit=201,
            include_resources=False,
        )[-200:]
        for message in recent:
            for key in message.sender_keys:
                existing = participants.get(key.value)
                if existing is not None:
                    # ``recent`` is chronological. Keep the latest observed activity,
                    # including the self/direct participants seeded before this scan.
                    participants[key.value] = replace(
                        existing, last_spoke_at_utc=message.sent_at_utc
                    )
                    continue
                nickname, remark = contacts.get(key.value, ("", ""))
                participants[key.value] = self._participant(
                    conversation_source_id,
                    key.value,
                    remark or nickname or None,
                    snapshot.fresh_as_of,
                    label_kind="contact_remark" if remark else "account_nickname",
                    last_spoke_at=message.sent_at_utc,
                )
        return list(participants.values())

    def resolve_participant(
        self,
        account_id: str,
        conversation_source_id: str,
        query: str,
        snapshot: SourceSnapshot,
    ) -> list[SourceParticipant]:
        folded = str(query or "").casefold()
        participants = self.list_participants(account_id, conversation_source_id, snapshot)
        if not folded:
            return participants
        return [
            participant
            for participant in participants
            if any(folded in label.label.casefold() for label in participant.labels)
        ]

    @staticmethod
    def _payload_digest(row: Any) -> str:
        digest = hashlib.sha256()
        for name in ("message_content", "packed_info_data"):
            value = row[name]
            payload = value if isinstance(value, bytes) else str(value or "").encode("utf-8")
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
        return digest.hexdigest()

    @classmethod
    def _message_identity(cls, row: Any, relative: str) -> tuple[str, tuple[Any, ...]]:
        server_id = int(row["server_id"] or 0)
        if server_id > 0:
            return "server", (server_id,)
        local_id = int(row["local_id"] or 0)
        create_time = int(row["create_time"] or 0)
        local_type = int(row["local_type"] or 0)
        if local_id > 0:
            return "local", (relative, local_id, create_time, local_type)
        return (
            "fallback",
            (
                relative,
                int(row["source_rowid"]),
                create_time,
                int(row["sort_seq"] or 0),
                local_type,
                cls._payload_digest(row),
            ),
        )

    @classmethod
    def _message_token(cls, conversation: str, row: Any, relative: str) -> str:
        kind, identity = cls._message_identity(row, relative)
        return native_message_token(conversation, kind, identity)

    @staticmethod
    def _decode_message_token(token: str) -> tuple[str, str, tuple[Any, ...]] | None:
        if not token.startswith("nmsg_"):
            return None
        try:
            body = base64.urlsafe_b64decode(token[5:] + "===")
            value = json.loads(body)
            if not isinstance(value, list) or len(value) < 4 or value[0] != 2:
                return None
            conversation = str(value[1])
            kind = str(value[2])
            identity = tuple(value[3:])
            if not conversation:
                return None
            if kind == "server":
                if len(identity) != 1 or int(identity[0]) < 1:
                    return None
                identity = (int(identity[0]),)
            elif kind == "local":
                if (
                    len(identity) != 4
                    or not _MESSAGE_DB.fullmatch(str(identity[0]))
                    or int(identity[1]) < 1
                ):
                    return None
                identity = (
                    str(identity[0]),
                    int(identity[1]),
                    int(identity[2]),
                    int(identity[3]),
                )
            elif kind == "fallback":
                if (
                    len(identity) != 6
                    or not _MESSAGE_DB.fullmatch(str(identity[0]))
                    or int(identity[1]) < 1
                    or not re.fullmatch(r"[0-9a-f]{64}", str(identity[5]))
                ):
                    return None
                identity = (
                    str(identity[0]),
                    int(identity[1]),
                    int(identity[2]),
                    int(identity[3]),
                    int(identity[4]),
                    str(identity[5]),
                )
            else:
                return None
            return conversation, kind, identity
        except (ValueError, TypeError, json.JSONDecodeError):
            return None

    def _table_name(self, conversation_source_id: str) -> str:
        return "Msg_" + hashlib.md5(conversation_source_id.encode()).hexdigest()

    def _message_relatives(self) -> tuple[str, ...]:
        """Every message shard this installation currently exposes.

        A dependency-scoped session fences this eligible membership separately
        from selected body dependencies and negative target-table routing facts.
        """

        values, warnings = self._expected_databases()
        if warnings:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                retryable=True,
                details={"warning_codes": list(warnings)},
            )
        return tuple(relative for relative in values if _MESSAGE_DB.fullmatch(relative))

    def _candidate_message_relatives(self, state: _SnapshotState) -> tuple[str, ...]:
        if state.scope is not None:
            session = _ACTIVE_SCOPED_SESSION.get()
            if session is not None and session.state is state:
                return session.candidate_message_relatives()
            return self._message_relatives()
        return tuple(
            sorted(value for value in state.generation_by_shard if _MESSAGE_DB.fullmatch(value))
        )

    def _message_shards(self, state: _SnapshotState, table: str) -> tuple[str, ...]:
        cached = state.message_shards.get(table)
        if cached is not None:
            return cached
        found: list[str] = []
        for relative in self._candidate_message_relatives(state):
            check_operation_budget()
            catalog = self._message_shard_catalog(state, relative)
            if table not in catalog.message_tables:
                session = _ACTIVE_SCOPED_SESSION.get()
                if session is not None and session.state is state:
                    session.record_negative_message_table(relative, table, catalog)
                continue
            columns = self._message_table_columns(state, relative, table, catalog)
            if not _MESSAGE_COLUMNS.issubset(columns):
                raise SightglassError(
                    ErrorCode.SOURCE_INCOMPLETE,
                    details={"warning_codes": ["source_schema_invalid"]},
                )
            found.append(relative)
        result = tuple(found)
        state.message_shards[table] = result
        return result

    def _record_message_dependencies(
        self,
        state: _SnapshotState,
        snapshot: SourceSnapshot,
        relatives: tuple[str, ...],
    ) -> None:
        """Bind a read/cursor only to shards that can serve its conversation."""

        for relative in relatives:
            identity = self._state_identity(state, relative)
            if identity is None:
                identity = self._identity(relative)
                state.scoped_identities[relative] = identity
            snapshot.dependency_generation_by_shard[relative] = identity.logical_generation_id(
                self._keys.get(relative, {}).get("enc_key", "")
            )

    @staticmethod
    def _keyset_clause(
        boundary: tuple[int, int, int], *, direction: str, inclusive: bool
    ) -> tuple[str, tuple[int, ...]]:
        timestamp, sort_seq, rowid = boundary
        if direction == "forward":
            final = ">=" if inclusive else ">"
            clause = (
                "(create_time > ? OR "
                "(create_time = ? AND COALESCE(sort_seq, 0) > ?) OR "
                f"(create_time = ? AND COALESCE(sort_seq, 0) = ? AND rowid {final} ?))"
            )
        else:
            final = "<=" if inclusive else "<"
            clause = (
                "(create_time < ? OR "
                "(create_time = ? AND COALESCE(sort_seq, 0) < ?) OR "
                f"(create_time = ? AND COALESCE(sort_seq, 0) = ? AND rowid {final} ?))"
            )
        return clause, (timestamp, timestamp, sort_seq, timestamp, sort_seq, rowid)

    @staticmethod
    def _sql_boundary(value: SourceSortKey) -> tuple[int, int, int]:
        return (
            int(parse_aware_datetime(value.sent_at_utc).timestamp()),
            int(value.sort_seq),
            int(value.source_rowid),
        )

    def _iter_shard_rows(
        self,
        state: _SnapshotState,
        conversation: SourceConversation,
        table: str,
        relative: str,
        *,
        page_size: int,
        direction: str,
        after: SourceSortKey | None,
        before: SourceSortKey | None,
        participant_source_ids: tuple[SourceParticipantFilter, ...],
        time_after_utc: str | None,
        time_before_utc: str | None,
    ) -> Iterator[Any]:
        page_boundary: tuple[int, int, int] | None = None
        order = "ASC" if direction == "forward" else "DESC"
        while True:
            clauses: list[str] = []
            params: list[Any] = []
            if time_after_utc is not None:
                clauses.append("create_time >= ?")
                params.append(int(parse_aware_datetime(time_after_utc).timestamp()))
            if time_before_utc is not None:
                clauses.append("create_time < ?")
                params.append(int(parse_aware_datetime(time_before_utc).timestamp()))
            # Keep the exact timestamp/sequence/rowid predicates, but expose their
            # inclusive timestamp bounds separately. An existing create_time index
            # can then seek in timeline order instead of sorting the whole OR union.
            if after is not None:
                boundary = self._sql_boundary(after)
                clauses.append("create_time >= ?")
                params.append(boundary[0])
                clause, values = self._keyset_clause(
                    boundary, direction="forward", inclusive=True
                )
                clauses.append(clause)
                params.extend(values)
            if before is not None:
                boundary = self._sql_boundary(before)
                clauses.append("create_time <= ?")
                params.append(boundary[0])
                clause, values = self._keyset_clause(
                    boundary, direction="backward", inclusive=True
                )
                clauses.append(clause)
                params.extend(values)
            if page_boundary is not None:
                clauses.append("create_time >= ?" if direction == "forward" else "create_time <= ?")
                params.append(page_boundary[0])
                clause, values = self._keyset_clause(
                    page_boundary, direction=direction, inclusive=False
                )
                clauses.append(clause)
                params.extend(values)
            sender_filter = self._group_sender_sql_filter(
                state,
                relative,
                table,
                conversation,
                participant_source_ids,
                row_alias="message_row",
            )
            if sender_filter is not None:
                sender_clause, sender_values = sender_filter
                clauses.append(sender_clause)
                params.extend(sender_values)
            where = "WHERE " + " AND ".join(clauses) if clauses else ""
            payload_page_size = max(1, min(int(page_size), 256))
            # A participant-filtered traversal narrows rows in SQL and needs one
            # large window; the ordinary traversal keeps a bounded *position*
            # batch independent of the lazy *payload* page size.
            position_limit = (
                _POSITION_PAGE_MAX
                if participant_source_ids
                else max(payload_page_size, _POSITION_PAGE_LIMIT)
            )
            position_params = (*params, position_limit)
            with self._connect(relative) as connection:
                positions = connection.execute(
                    f"""
                    SELECT rowid AS source_rowid, create_time,
                           COALESCE(sort_seq, 0) AS sort_seq, server_id, local_type
                    FROM [{table}] AS message_row {where}
                    ORDER BY create_time {order}, COALESCE(sort_seq, 0) {order},
                             rowid {order}
                    LIMIT ?
                    """,
                    position_params,
                ).fetchall()
                if not positions:
                    return
            for position_offset in range(0, len(positions), _POSITION_PAGE_LIMIT):
                position_batch = positions[position_offset : position_offset + _POSITION_PAGE_LIMIT]
                with self._connect(relative) as connection:
                    representatives = self._text_position_representatives(
                        connection, table, position_batch
                    )
                selected_positions = [
                    row for row in position_batch
                    if self._is_representative_position(row, representatives)
                ]
                for offset in range(0, len(selected_positions), payload_page_size):
                    position_page = selected_positions[offset : offset + payload_page_size]
                    rowids = tuple(int(row["source_rowid"]) for row in position_page)
                    payload_by_rowid = self._message_rows_by_rowid(
                        state, relative, table, rowids, text_representatives=representatives
                    )
                    yield from (
                        payload_by_rowid[int(position["source_rowid"])]
                        for position in position_page
                    )
            last = positions[-1]
            page_boundary = (
                int(last["create_time"] or 0),
                int(last["sort_seq"] or 0),
                int(last["source_rowid"]),
            )
            if len(positions) < position_limit:
                return

    @staticmethod
    def _text_position_representatives(
        connection: Any, table: str, positions: list[Any],
    ) -> dict[int, tuple[int, int]]:
        """Resolve only candidate text identities; this is not payload evidence.

        Same-shard ordinary text copies may differ in local/physical IDs. Their
        timeline position is the smallest rowid only when every copy has the same
        time/sequence/type. Payload/sender/packed evidence is checked separately
        before returning a selected row. Other types and coordinate conflicts are
        never filtered here. The IN query is batched even without a server-ID index.
        """

        ids = tuple(sorted({
            int(row["server_id"]) for row in positions
            if int(row["server_id"] or 0) > 0 and int(row["local_type"] or 0) == 1
        }))
        if not ids:
            return {}
        placeholders = ",".join("?" for _value in ids)
        rows = connection.execute(
            f"""SELECT server_id, MIN(rowid) AS representative, COUNT(*) AS copies,
                       MIN(create_time) AS first_time, MAX(create_time) AS last_time,
                       MIN(COALESCE(sort_seq, 0)) AS first_seq,
                       MAX(COALESCE(sort_seq, 0)) AS last_seq,
                       MIN(local_type) AS first_type, MAX(local_type) AS last_type,
                       COUNT(create_time) AS known_times, COUNT(local_type) AS known_types
                FROM [{table}] WHERE server_id IN ({placeholders}) GROUP BY server_id""",
            ids,
        ).fetchall()
        check_operation_budget()
        return {
            int(row["server_id"]): (
                int(row["representative"]) if (
                    row["first_type"] == row["last_type"] == 1
                    and row["first_time"] == row["last_time"]
                    and row["first_seq"] == row["last_seq"]
                    and row["known_times"] == row["known_types"] == row["copies"]
                ) else 0,
                int(row["copies"]),
            )
            for row in rows
            if int(row["copies"]) > 1
        }

    @staticmethod
    def _is_representative_position(row: Any, representatives: dict[int, tuple[int, int]]) -> bool:
        representative = representatives.get(int(row["server_id"] or 0))
        return (
            representative is None or representative[0] == 0
            or int(row["source_rowid"]) == representative[0]
        )

    @staticmethod
    def _collapse_equivalent_text_rows(rows: list[Any]) -> list[Any]:
        """Prove exact raw equality before ignoring local IDs of ordinary text."""

        groups: dict[int, list[Any]] = {}
        other: list[Any] = []
        for row in rows:
            check_operation_budget()
            if int(row["server_id"] or 0) > 0:
                groups.setdefault(int(row["server_id"]), []).append(row)
            else:
                other.append(row)
        fields = (
            "server_id", "local_type", "sort_seq", "create_time", "status",
            "message_content", "content_type", "packed_info_data", "real_sender_username",
            "real_sender_source_id",
        )
        for copies in groups.values():
            check_operation_budget()
            if len(copies) > 1 and any(int(row["local_type"] or 0) == 1 for row in copies):
                first = copies[0]
                if not all(
                    int(row["local_type"] or 0) == 1
                    and all(row[key] == first[key] for key in fields)
                    for row in copies
                ):
                    raise SightglassError(
                        ErrorCode.SOURCE_INCOMPLETE,
                        details={"warning_codes": ["duplicate_message_identity_conflict"]},
                    )
                other.append(min(copies, key=lambda row: int(row["source_rowid"])))
            else:
                other.extend(copies)
        return other

    def _server_identity_rows(
        self, state: _SnapshotState, relative: str, table: str, server_ids: tuple[int, ...],
    ) -> list[Any]:
        if not server_ids:
            return []
        real_sender = self._real_sender_expression(
            state, relative, table, row_alias="message_row"
        )
        raw_sender = self._raw_sender_expression(state, relative, table)
        placeholders = ",".join("?" for _value in server_ids)
        with self._connect(relative) as connection:
            rows = connection.execute(
                f"""SELECT message_row.rowid AS source_rowid,
                           message_row.local_id, message_row.server_id, message_row.local_type,
                           COALESCE(message_row.sort_seq, 0) AS sort_seq,
                           message_row.create_time, message_row.status, message_row.message_content,
                           message_row.WCDB_CT_message_content AS content_type,
                           message_row.packed_info_data, {real_sender} AS real_sender_username,
                           {raw_sender} AS real_sender_source_id
                    FROM [{table}] AS message_row
                    WHERE message_row.server_id IN ({placeholders})
                    ORDER BY message_row.rowid ASC LIMIT 20003""",
                server_ids,
            ).fetchall()
        if len(rows) > 20_002:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                details={"warning_codes": ["native_traversal_limit_reached"]},
            )
        check_operation_budget()
        return self._collapse_equivalent_text_rows(rows)

    def _message_rows_by_rowid(
        self,
        state: _SnapshotState,
        relative: str,
        table: str,
        rowids: tuple[int, ...],
        *, canonical: bool = True,
        text_representatives: dict[int, tuple[int, int]] | None = None,
    ) -> dict[int, Any]:
        if not rowids:
            return {}
        real_sender = self._real_sender_expression(
            state, relative, table, row_alias="message_row"
        )
        raw_sender = self._raw_sender_expression(state, relative, table)
        placeholders = ",".join("?" for _value in rowids)
        with self._connect(relative) as connection:
            rows = connection.execute(
                f"""
                SELECT message_row.rowid AS source_rowid,
                       message_row.local_id, message_row.server_id,
                       message_row.local_type,
                       COALESCE(message_row.sort_seq, 0) AS sort_seq,
                       message_row.create_time, message_row.status,
                       message_row.message_content,
                       message_row.WCDB_CT_message_content AS content_type,
                       message_row.packed_info_data,
                       {real_sender} AS real_sender_username,
                       {raw_sender} AS real_sender_source_id
                FROM [{table}] AS message_row
                WHERE message_row.rowid IN ({placeholders})
                """,
                rowids,
            ).fetchall()
        result = {int(row["source_rowid"]): row for row in rows}
        if len(result) != len(rowids):
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        if canonical:
            if text_representatives is None:
                with self._connect(relative) as connection:
                    text_representatives = self._text_position_representatives(
                        connection, table, rows
                    )
            ids = tuple(sorted({
                int(row["server_id"]) for row in rows
                if int(row["server_id"] or 0) > 0 and int(row["local_type"] or 0) == 1
                and int(row["server_id"]) in text_representatives
            }))
            canonical_rows = {
                int(row["server_id"]): row
                for row in self._server_identity_rows(state, relative, table, ids)
            }
            for row in rows:
                canonical_row = canonical_rows.get(int(row["server_id"] or 0))
                if (
                    canonical_row is not None
                    and row["source_rowid"] != canonical_row["source_rowid"]
                ):
                    raise SightglassError(
                        ErrorCode.SOURCE_INCOMPLETE,
                        details={"warning_codes": ["duplicate_message_identity_conflict"]},
                    )
        return result

    def _message(
        self,
        state: _SnapshotState,
        conversation: SourceConversation,
        row: Any,
        relative: str,
        snapshot: SourceSnapshot,
        *,
        include_resources: bool = True,
    ) -> SourceMessage:
        content = row["message_content"]
        message_type = _base_message_type(row["local_type"])
        if int(row["content_type"] or 0) == 4 and isinstance(content, bytes):
            try:
                # A ZstdDecompressor instance is mutable native state and cannot be
                # shared by the daemon request thread and background source worker.
                # Keep each decode independent so concurrent live reads cannot race
                # inside the C backend.
                content = zstandard.ZstdDecompressor().decompress(content).decode("utf-8")
            except (UnicodeError, zstandard.ZstdError) as exc:
                raise SightglassError(ErrorCode.SOURCE_MESSAGE_DECODE_FAILED) from exc
        elif isinstance(content, bytes):
            content = content.decode("utf-8", errors="replace")
        content = str(content or "")
        direction = _message_direction(row["status"])
        mapped_sender = str(row["real_sender_username"] or "") or None
        group_prefix = (
            content.split(":\n", 1)[0]
            if conversation.kind == "group" and ":\n" in content
            else None
        )
        verified_group_sender = (
            mapped_sender
            if group_prefix and mapped_sender and group_prefix == mapped_sender
            else None
        )
        conflicting_group_envelope = bool(
            conversation.kind == "group" and group_prefix and verified_group_sender is None
        )
        # Direct and group rows do not share one status vocabulary. On the supported
        # native build, group status 3 is used both for account-sent rows without a
        # member envelope and for received rows whose real_sender_id mapping matches
        # the leading envelope. Verified member evidence therefore outranks status;
        # conflicting/unverifiable envelope evidence stays unresolved rather than
        # becoming a false self attribution.
        outgoing = (
            direction == "outgoing"
            and verified_group_sender is None
            and not conflicting_group_envelope
        )
        sender_id: str | None = None
        current_label: str | None = None
        current_label_kind: str | None = None
        if message_type in _SYSTEM_MESSAGE_TYPES:
            # A system/recall row stays system data: neither the peer fallback
            # nor a group prefix may turn it into a human participant.
            outgoing = False
        elif verified_group_sender is not None:
            sender_id = verified_group_sender
            nickname, remark = (state.contacts or {}).get(sender_id, ("", ""))
            current_label = remark or nickname or None
            current_label_kind = "contact_remark" if remark else "account_nickname"
        elif outgoing:
            sender_id = f"self:{self.settings.source_account_key}"
            current_label = "我"
            current_label_kind = "account_nickname"
        elif conversation.kind == "direct" and direction == "incoming":
            sender_id = conversation.source_conversation_id
            nickname, remark = (state.contacts or {}).get(sender_id, ("", ""))
            current_label = remark or nickname or None
            current_label_kind = "contact_remark" if remark else "account_nickname"
        rowid = int(row["source_rowid"])
        source_message_id = self._message_token(conversation.source_conversation_id, row, relative)
        sent_at = datetime.fromtimestamp(int(row["create_time"] or 0), UTC).isoformat(
            timespec="microseconds"
        )
        sender_keys = (
            (
                SourceIdentityKey(
                    "internal_username",
                    sender_id,
                    "stable",
                    True,
                    "macos-wechat.message-envelope",
                ),
            )
            if sender_id
            else ()
        )
        labels = ()
        if current_label:
            labels = (
                LabelObservation(
                    label=current_label,
                    label_kind=current_label_kind or "account_nickname",
                    scope="account",
                    provenance="macos-wechat.current-contact",
                    observed_at_utc=snapshot.fresh_as_of,
                    temporal_confidence="current_only",
                ),
            )
        packed_info_data = row["packed_info_data"]
        if isinstance(packed_info_data, memoryview):
            packed_info_data = packed_info_data.tobytes()
        if not isinstance(packed_info_data, bytes):
            packed_info_data = None
        resources = (
            self._resource_resolver.resources_for_message(
                source_message_id=source_message_id,
                conversation_source_id=conversation.source_conversation_id,
                local_id=int(row["local_id"] or 0),
                server_id=int(row["server_id"] or 0),
                create_time=int(row["create_time"] or 0),
                local_type=message_type,
                raw_content=content,
                packed_info_data=packed_info_data,
            )
            if include_resources
            else ()
        )
        return SourceMessage(
            source_message_id=source_message_id,
            source_conversation_id=conversation.source_conversation_id,
            conversation_kind=conversation.kind,
            source_time_raw=str(row["create_time"]),
            sent_at_utc=sent_at,
            observed_at_utc=snapshot.fresh_as_of,
            sort_seq=int(row["sort_seq"] or 0),
            source_rowid=rowid,
            wechat_type=message_type,
            raw_content=content,
            is_outgoing=outgoing,
            source_generation_id=state.generation_by_shard[relative],
            logical_shard_key=relative,
            sender_keys=sender_keys,
            sender_labels=labels,
            sender_surface_label=None,
            resources=resources,
        )

    @staticmethod
    def _matches_participant(
        message: SourceMessage, filters: tuple[SourceParticipantFilter, ...]
    ) -> bool:
        if not filters:
            return True
        for participant_filter in filters:
            if participant_filter.source_message_id == message.source_message_id:
                return True
            if any(
                participant_filter.key_kind == key.kind
                and participant_filter.key_value == key.value
                and participant_filter.principal_eligible == key.principal_eligible
                for key in message.sender_keys
            ):
                return True
        return False

    @staticmethod
    def _message_evidence_digest(message: SourceMessage) -> str:
        value = {
            "sent_at": message.sent_at_utc,
            "sort_seq": message.sort_seq,
            "source_rowid": message.source_rowid,
            "type": message.wechat_type,
            "raw_content": message.raw_content,
            "outgoing": message.is_outgoing,
            "sender_keys": [key.__dict__ for key in message.sender_keys],
        }
        return hashlib.sha256(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def _iter_shard_messages(
        self,
        state: _SnapshotState,
        conversation: SourceConversation,
        table: str,
        relative: str,
        snapshot: SourceSnapshot,
        *,
        page_size: int,
        direction: str,
        after: SourceSortKey | None,
        before: SourceSortKey | None,
        participant_source_ids: tuple[SourceParticipantFilter, ...],
        time_after_utc: str | None,
        time_before_utc: str | None,
        include_resources: bool = True,
    ) -> Iterator[SourceMessage]:
        for row in self._iter_shard_rows(
            state,
            conversation,
            table,
            relative,
            page_size=page_size,
            direction=direction,
            after=after,
            before=before,
            participant_source_ids=participant_source_ids,
            time_after_utc=time_after_utc,
            time_before_utc=time_before_utc,
        ):
            message = self._message(
                state,
                conversation,
                row,
                relative,
                snapshot,
                include_resources=include_resources and not participant_source_ids,
            )
            if participant_source_ids and not self._matches_participant(
                message, participant_source_ids
            ):
                continue
            if participant_source_ids and include_resources:
                message = self._message(state, conversation, row, relative, snapshot)
            yield message

    def _messages(
        self,
        account_id: str,
        conversation_source_id: str,
        snapshot: SourceSnapshot,
        *,
        direction: str,
        limit: int,
        after: SourceSortKey | None = None,
        before: SourceSortKey | None = None,
        participant_source_ids: tuple[SourceParticipantFilter, ...] = (),
        time_after_utc: str | None = None,
        time_before_utc: str | None = None,
        include_resources: bool = True,
    ) -> list[SourceMessage]:
        state = self._assert_snapshot(snapshot)
        self._require_account(account_id)
        self._enforce_scope(
            state,
            account_id=account_id,
            conversation_source_id=conversation_source_id,
        )
        conversation = self._conversation(state, conversation_source_id)
        if conversation is None:
            raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        self._contacts(state)
        table = self._table_name(conversation_source_id)
        shards = self._message_shards(state, table)
        self._record_message_dependencies(state, snapshot, shards)
        # A one-shard table keeps its full ``limit + 1`` prefetch. With more than one
        # shard, ``heapq.merge`` only needs each shard's earliest row to select the next
        # global winner, so an ordinary read must not eagerly materialize ``limit``
        # payload rows per shard (whose losing rows are then discarded). Fetching one
        # row per shard at a time bounds merge initialization to O(shards) payload/resource
        # work, while keyset pagination still walks every shard's full history.
        if participant_source_ids:
            page_size = 256
        elif len(shards) > 1:
            page_size = 1
        else:
            page_size = min(256, max(1, int(limit)))
        iterators = tuple(
            self._iter_shard_messages(
                state,
                conversation,
                table,
                relative,
                snapshot,
                page_size=page_size,
                direction=direction,
                after=after,
                before=before,
                participant_source_ids=participant_source_ids,
                time_after_utc=time_after_utc,
                time_before_utc=time_before_utc,
                include_resources=include_resources,
            )
            for relative in shards
        )
        merged = heapq.merge(
            *iterators,
            key=lambda item: item.sort_key.as_tuple(),
            reverse=direction == "backward",
        )
        messages: list[SourceMessage] = []
        seen: dict[str, str] = {}
        for scanned, message in enumerate(merged, start=1):
            if scanned > 20_002:
                raise SightglassError(
                    ErrorCode.SOURCE_INCOMPLETE,
                    details={"warning_codes": ["native_traversal_limit_reached"]},
                )
            evidence = self._message_evidence_digest(message)
            previous = seen.get(message.source_message_id)
            if previous is not None:
                if previous != evidence:
                    raise SightglassError(
                        ErrorCode.SOURCE_INCOMPLETE,
                        details={"warning_codes": ["duplicate_message_identity_conflict"]},
                    )
                continue
            seen[message.source_message_id] = evidence
            if after is not None and message.sort_key.as_tuple() <= after.as_tuple():
                continue
            if before is not None and message.sort_key.as_tuple() >= before.as_tuple():
                continue
            if not self._matches_participant(message, participant_source_ids):
                continue
            messages.append(message)
            if len(messages) >= max(1, int(limit)):
                break
        messages.sort(key=lambda item: item.sort_key.as_tuple())
        return messages

    def read_recent(
        self,
        account_id: str,
        conversation_source_id: str,
        limit: int,
        snapshot: SourceSnapshot,
    ) -> SourceMessagePage:
        bounded = max(1, int(limit))
        messages = self._messages(
            account_id,
            conversation_source_id,
            snapshot,
            direction="backward",
            limit=bounded + 1,
        )
        selected = messages[-bounded:]
        self._assert_snapshot(snapshot)
        return SourceMessagePage(tuple(selected), has_more_before=len(messages) > len(selected))

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
        if direction not in {"forward", "backward"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        bounded = max(1, int(limit))
        messages = self._messages(
            account_id,
            conversation_source_id,
            snapshot,
            direction=direction,
            limit=bounded + 1,
            after=after,
            before=before,
            participant_source_ids=participant_source_ids,
            time_after_utc=time_after_utc,
            time_before_utc=time_before_utc,
        )
        selected = messages[:bounded] if direction == "forward" else messages[-bounded:]
        self._assert_snapshot(snapshot)
        return SourceMessagePage(
            tuple(selected),
            has_more_before=direction == "backward" and len(messages) > len(selected),
            has_more_after=direction == "forward" and len(messages) > len(selected),
        )

    def search_generation_binding(
        self,
        account_id: str,
        conversation_source_id: str,
        *,
        snapshot: SourceSnapshot,
    ) -> tuple[tuple[str, str], ...]:
        """Bind only the target's selected logical shards, without reading Msg rows."""

        state = self._assert_snapshot(snapshot)
        self._require_account(account_id)
        self._enforce_scope(
            state, account_id=account_id, conversation_source_id=conversation_source_id,
        )
        shards = self._message_shards(state, self._table_name(conversation_source_id))
        if not shards:
            raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        self._record_message_dependencies(state, snapshot, shards)
        return tuple(
            (relative, snapshot.dependency_generation_by_shard[relative]) for relative in shards
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
        """Prepare one unfiltered page through bounded rowid scans in a pinned lease.

        Time predicates stay out of the scan SQL: even a sparse/absent time window
        must return after at most one raw-row batch. Chronological heaps retain only
        candidate positions. Payloads and resources are read after positions finish;
        no progress value is a cross-session checkpoint. The caller validates/closes
        this conversation session before committing the final page.
        """

        _check_preparation(check_authority)
        if direction not in {"forward", "backward"} or batch_size < 1:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        state = self._assert_snapshot(snapshot)
        if state.scope is None or state.scope.kind != "conversation":
            raise SightglassError(ErrorCode.QUERY_INVALID)
        binding = self.search_generation_binding(
            account_id, conversation_source_id, snapshot=snapshot,
        )
        conversation = self._conversation(state, conversation_source_id)
        if conversation is None:
            raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        self._contacts(state)
        table = self._table_name(conversation_source_id)
        shards = tuple(relative for relative, _generation in binding)
        batch = min(int(batch_size), 1024)
        bounded = max(1, int(limit))
        capacity = bounded + 1 + int(after is not None) + int(before is not None)
        after_position = self._sql_boundary(after) if after is not None else None
        before_position = self._sql_boundary(before) if before is not None else None
        minimum_time = (
            parse_aware_datetime(time_after_utc).timestamp() if time_after_utc is not None else None
        )
        maximum_time = (
            parse_aware_datetime(time_before_utc).timestamp()
            if time_before_utc is not None else None
        )
        scanned = 0
        completed = 0
        positions_by_shard: dict[str, list[tuple[int, int, int]]] = {}
        yield SourcePreparationStep("positions", scanned, completed, len(shards))
        for relative in shards:
            representatives: dict[int, tuple[int, int]] = {}
            with self._connect(relative) as connection:
                while True:
                    window = _PreparationWindow(capacity, forward=direction == "forward")
                    last_rowid: int | None = None
                    while True:
                        _check_preparation(check_authority)
                        where = "WHERE rowid > ?" if last_rowid is not None else ""
                        params = (last_rowid, batch) if last_rowid is not None else (batch,)
                        rows = connection.execute(
                            f"""SELECT rowid AS source_rowid, create_time,
                                       COALESCE(sort_seq, 0) AS sort_seq, server_id, local_type
                                FROM [{table}] {where} ORDER BY rowid LIMIT ?""",
                            params,
                        ).fetchall()
                        for row in rows:
                            if not self._is_representative_position(row, representatives):
                                continue
                            position = (
                                int(row["create_time"] or 0), int(row["sort_seq"]),
                                int(row["source_rowid"]),
                            )
                            if minimum_time is not None and position[0] < minimum_time:
                                continue
                            if maximum_time is not None and position[0] >= maximum_time:
                                continue
                            if after_position is not None and position < after_position:
                                continue
                            if before_position is not None and position > before_position:
                                continue
                            window.offer(position, position)
                        scanned += len(rows)
                        exhausted = len(rows) < batch
                        if rows:
                            last_rowid = int(rows[-1]["source_rowid"])
                        _check_preparation(check_authority)
                        if exhausted:
                            break
                        yield SourcePreparationStep("positions", scanned, completed, len(shards))
                    selected_positions = window.selected()
                    metadata = self._position_metadata(connection, table, tuple(selected_positions))
                    resolved = self._text_position_representatives(connection, table, metadata)
                    _check_preparation(check_authority)
                    if all(self._is_representative_position(row, resolved) for row in metadata):
                        completed += 1
                        yield SourcePreparationStep("positions", scanned, completed, len(shards))
                        positions_by_shard[relative] = selected_positions
                        break
                    # Candidate copies consumed the window: restart this shard's
                    # bounded position scan with just these exact identities filtered.
                    # No payload or progress is admitted until this lease completes.
                    representatives.update(resolved)
                    if len(representatives) > 20_002:
                        raise SightglassError(
                            ErrorCode.SOURCE_INCOMPLETE,
                            details={"warning_codes": ["native_traversal_limit_reached"]},
                        )
                    yield SourcePreparationStep("positions", scanned, completed, len(shards))

        rows_by_shard: dict[str, dict[int, Any]] = {}
        messages_by_shard: dict[str, list[SourceMessage]] = {}
        for relative, positions in positions_by_shard.items():
            payload_rows: dict[int, Any] = {}
            messages: list[SourceMessage] = []
            for offset in range(0, len(positions), batch):
                _check_preparation(check_authority)
                rowids = tuple(position[2] for position in positions[offset:offset + batch])
                rows = self._message_rows_by_rowid(state, relative, table, rowids)
                payload_rows.update(rows)
                for rowid in rowids:
                    check_operation_budget()
                    messages.append(self._message(
                        state, conversation, rows[rowid], relative, snapshot,
                        include_resources=False,
                    ))
                _check_preparation(check_authority)
                yield SourcePreparationStep("payload", scanned, completed, len(shards))
            rows_by_shard[relative] = payload_rows
            messages_by_shard[relative] = messages

        merged = heapq.merge(
            *messages_by_shard.values(), key=lambda item: item.sort_key.as_tuple(),
            reverse=direction == "backward",
        )
        selected: list[SourceMessage] = []
        seen: dict[str, str] = {}
        for consumed, message in enumerate(merged, start=1):
            check_operation_budget()
            if consumed > 20_002:
                raise SightglassError(
                    ErrorCode.SOURCE_INCOMPLETE,
                    details={"warning_codes": ["native_traversal_limit_reached"]},
                )
            evidence = self._message_evidence_digest(message)
            previous = seen.get(message.source_message_id)
            if previous is not None:
                if previous != evidence:
                    raise SightglassError(
                        ErrorCode.SOURCE_INCOMPLETE,
                        details={"warning_codes": ["duplicate_message_identity_conflict"]},
                    )
                continue
            seen[message.source_message_id] = evidence
            if after is not None and message.sort_key.as_tuple() <= after.as_tuple():
                continue
            if before is not None and message.sort_key.as_tuple() >= before.as_tuple():
                continue
            selected.append(message)
            if len(selected) == bounded + 1:
                break
        more = len(selected) > bounded
        hydrated: list[SourceMessage] = []
        for message in selected[:bounded]:
            _check_preparation(check_authority)
            hydrated.append(self._message(
                state, conversation,
                rows_by_shard[message.logical_shard_key][message.source_rowid],
                message.logical_shard_key, snapshot,
            ))
            _check_preparation(check_authority)
            yield SourcePreparationStep("payload", scanned, completed, len(shards))
        hydrated.sort(key=lambda item: item.sort_key.as_tuple())
        self._assert_snapshot(snapshot)
        _check_preparation(check_authority)
        yield SourcePreparationStep(
            "complete", scanned, completed, len(shards),
            SourceMessagePage(
                tuple(hydrated), has_more_before=direction == "backward" and more,
                has_more_after=direction == "forward" and more,
            ),
        )

    @staticmethod
    def _discovery_position(shard: str, rowid: int) -> dict[str, Any]:
        """Provider-owned physical continuation: resume the given shard below ``rowid``."""

        return {"schema": DISCOVERY_POSITION_SCHEMA, "shard": shard, "rowid": int(rowid)}

    def _discovery_start(
        self,
        position: dict[str, Any] | None,
        shards: tuple[str, ...],
    ) -> tuple[int, int | None]:
        """Resolve an incoming position to ``(shard_index, exclusive_rowid)``.

        ``None`` starts at the first shard with no bound. A malformed cursor or one
        naming a shard outside the currently selected set fails closed rather than
        silently restarting the scan or scanning an unrelated shard.
        """

        if position is None:
            return (0, None)
        if not isinstance(position, dict):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if position.get("schema") != DISCOVERY_POSITION_SCHEMA:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        shard = position.get("shard")
        rowid = position.get("rowid")
        if not isinstance(shard, str) or shard not in shards:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if isinstance(rowid, bool) or not isinstance(rowid, int) or rowid < 0:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        return (shards.index(shard), rowid)

    def _discovery_scan_rows(
        self,
        relative: str,
        table: str,
        *,
        after_rowid: int | None,
        page_size: int,
    ) -> list[Any]:
        """One bounded descending-``rowid`` physical page from a single shard."""

        bound = max(1, min(int(page_size), DISCOVERY_SCAN_CAP))
        if after_rowid is None:
            where = ""
            params: tuple[Any, ...] = (bound,)
        else:
            where = "WHERE rowid < ?"
            params = (int(after_rowid), bound)
        with self._connect(relative) as connection:
            return connection.execute(
                f"""
                SELECT rowid AS source_rowid, create_time,
                       COALESCE(sort_seq, 0) AS sort_seq
                FROM [{table}]
                {where}
                ORDER BY rowid DESC
                LIMIT ?
                """,
                params,
            ).fetchall()

    def scan_discovery_page(
        self,
        account_id: str,
        conversation_source_id: str,
        *,
        snapshot: SourceSnapshot,
        position: dict[str, Any] | None = None,
        limit: int = DISCOVERY_DEFAULT_LIMIT,
        time_after_utc: str | None = None,
        time_before_utc: str | None = None,
    ) -> SourceDiscoveryPage:
        """Walk bounded physical raw rows and return time-windowed candidates.

        One call inspects at most ``min(limit, DISCOVERY_SCAN_CAP)`` raw rows across
        the selected shards, regardless of how many satisfy the time window, so a
        sparse/empty window still advances by a bounded amount. Rows are visited in
        descending ``rowid`` order (physical, not chronological); the time filter is
        applied in Python and never becomes an unbounded SQL ``WHERE``/``ORDER BY``.

        Returned messages are discovery *candidates*, not authoritative results: the
        reader MUST call ``get_message`` for each selected identity before admission,
        recheck conversation/time/filter semantics on that canonical row, and dedupe
        by canonical ``source_message_id``. The same identity may repeat across shards
        or pages; that is expected, and only the canonical lookup may fail closed on a
        genuinely conflicting identity.

        ``has_more`` is conservative: it is ``True`` whenever a page filled exactly at
        the requested bound, even if that happened to be the last raw row. Then
        ``next_position`` is non-``None``; a following call resolves to
        ``has_more=False`` (with ``next_position=None``) without returning rows, so no
        row is ever skipped and the walk always terminates.
        """

        check_operation_budget()
        bounded = max(1, min(int(limit), DISCOVERY_SCAN_CAP))
        minimum_time = (
            parse_aware_datetime(time_after_utc).timestamp() if time_after_utc is not None else None
        )
        maximum_time = (
            parse_aware_datetime(time_before_utc).timestamp()
            if time_before_utc is not None
            else None
        )
        state = self._assert_snapshot(snapshot)
        self._require_account(account_id)
        self._enforce_scope(
            state,
            account_id=account_id,
            conversation_source_id=conversation_source_id,
        )
        conversation = self._conversation(state, conversation_source_id)
        if conversation is None:
            raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        self._contacts(state)
        table = self._table_name(conversation_source_id)
        shards = self._message_shards(state, table)
        self._record_message_dependencies(state, snapshot, shards)
        start_index, start_rowid = self._discovery_start(position, shards)

        inspected = 0
        selected: list[tuple[str, int]] = []
        last_shard: str | None = None
        last_rowid: int | None = None
        has_more = False

        for shard_index in range(start_index, len(shards)):
            check_operation_budget()
            relative = shards[shard_index]
            after_rowid = start_rowid if shard_index == start_index else None
            while inspected < bounded:
                remaining = bounded - inspected
                rows = self._discovery_scan_rows(
                    relative, table, after_rowid=after_rowid, page_size=remaining
                )
                if not rows:
                    break
                for row in rows:
                    check_operation_budget()
                    inspected += 1
                    last_shard = relative
                    last_rowid = int(row["source_rowid"])
                    timestamp = int(row["create_time"] or 0)
                    within_window = (
                        (minimum_time is None or timestamp >= minimum_time)
                        and (maximum_time is None or timestamp < maximum_time)
                    )
                    if not within_window:
                        continue
                    selected.append((relative, int(row["source_rowid"])))
                if inspected >= bounded:
                    # The page filled mid-shard; more raw rows may remain.
                    has_more = True
                    break
                after_rowid = last_rowid
                if len(rows) < remaining:
                    break
            if inspected >= bounded:
                break

        # Resolve every selected payload in one bounded batch per shard (at most
        # ``bounded`` point lookups total) using the same helper as ordinary reads,
        # then rebuild messages in the original physical order.
        payloads: dict[tuple[str, int], Any] = {}
        for relative in dict.fromkeys(shard for shard, _rowid in selected):
            check_operation_budget()
            rowids = tuple(rowid for shard, rowid in selected if shard == relative)
            fetched = self._message_rows_by_rowid(state, relative, table, rowids, canonical=False)
            for rowid in rowids:
                payloads[(relative, rowid)] = fetched[rowid]

        messages: list[SourceMessage] = []
        positions: list[dict[str, Any]] = []
        for relative, rowid in selected:
            check_operation_budget()
            messages.append(
                self._message(
                    state,
                    conversation,
                    payloads[(relative, rowid)],
                    relative,
                    snapshot,
                    include_resources=False,
                )
            )
            positions.append(self._discovery_position(relative, rowid))

        next_position = (
            self._discovery_position(last_shard, last_rowid)
            if has_more and last_shard is not None and last_rowid is not None
            else None
        )
        self._assert_snapshot(snapshot)
        check_operation_budget()
        return SourceDiscoveryPage(
            messages=tuple(messages),
            positions=tuple(positions),
            next_position=next_position,
            has_more=has_more,
            scanned_rows=inspected,
        )

    def _context_positions(
        self, connection: Any, table: str, boundary: tuple[int, int, int],
        before: int, after: int,
    ) -> tuple[list[tuple[int, int, int]], list[tuple[int, int, int]]]:
        # Unique timelines use the existing seek/one unordered position pass. Only
        # retained duplicate text identities trigger a refill; each refill excludes
        # their nonrepresentative rows before the bounded neighbor windows fill.
        representatives: dict[int, tuple[int, int]] = {}
        while True:
            check_operation_budget()
            left: list[tuple[int, int, int]] = []
            right: list[tuple[int, int, int]] = []
            ties: list[tuple[int, int, int]] = []
            overscan = sum(count - 1 for _rowid, count in representatives.values())
            seeks: list[tuple[str, tuple[int, ...]] | None] = []
            indexed = True
            for direction, radius in (("backward", before), ("forward", after)):
                if not radius:
                    seeks.append(None)
                    continue
                clause, values = self._keyset_clause(
                    boundary, direction=direction, inclusive=True
                )
                guard = "<=" if direction == "backward" else ">="
                order = "DESC" if direction == "backward" else "ASC"
                sql = f"""
                    SELECT rowid AS source_rowid, create_time,
                           COALESCE(sort_seq, 0) AS sort_seq, server_id, local_type
                    FROM [{table}]
                    WHERE create_time {guard} ? AND {clause}
                    ORDER BY create_time {order}, COALESCE(sort_seq, 0) {order},
                             rowid {order}
                    LIMIT ?
                    """
                # The prefix-equal row is inclusive. Keep radius+1 neighbors
                # plus that possible full-ID tie; it cannot displace a sentinel.
                params = (boundary[0], *values, min(20_003, radius + 2 + overscan))
                seeks.append((sql, params))
                if indexed:
                    plan = connection.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()
                    details = [str(row[3]) for row in plan]
                    indexed = any(
                        detail.startswith("SEARCH ") for detail in details
                    ) and not any(
                        detail == "USE TEMP B-TREE FOR ORDER BY" for detail in details
                    )
            if indexed:
                position_sides: list[list[tuple[int, int, int]]] = []
                for seek in seeks:
                    rows = connection.execute(*seek).fetchall() if seek is not None else ()
                    position_sides.append(
                        [
                            (
                                int(row["create_time"] or 0),
                                int(row["sort_seq"] or 0),
                                int(row["source_rowid"]),
                            )
                            for row in rows
                            if self._is_representative_position(row, representatives)
                        ]
                    )
                left_positions, right_positions = (
                    side[:radius + 2] for side, radius in zip(position_sides, (before, after))
                )
            else:
                cursor = connection.execute(
                    f"""
                    SELECT rowid AS source_rowid, create_time,
                           COALESCE(sort_seq, 0) AS sort_seq, server_id, local_type
                    FROM [{table}]
                    """
                )
                while batch := cursor.fetchmany(1024):
                    check_operation_budget()
                    for row in batch:
                        if not self._is_representative_position(row, representatives):
                            continue
                        position = (
                            int(row["create_time"] or 0),
                            int(row["sort_seq"] or 0),
                            int(row["source_rowid"]),
                        )
                        if position == boundary:
                            # rowid is unique within a shard, but another shard may
                            # share all three fields. Its full source ID breaks ties.
                            ties.append(position)
                        elif position < boundary and before:
                            if len(left) < before + 1:
                                heapq.heappush(left, position)
                            elif position > left[0]:
                                heapq.heapreplace(left, position)
                        elif position > boundary and after:
                            inverse = (-position[0], -position[1], -position[2])
                            if len(right) < after + 1:
                                heapq.heappush(right, inverse)
                            elif inverse > right[0]:
                                heapq.heapreplace(right, inverse)
                left_positions = sorted((*left, *ties), reverse=True) if before else []
                right_positions = (
                    sorted((*((-item[0], -item[1], -item[2]) for item in right), *ties))
                    if after
                    else []
                )
            selected = (*left_positions, *right_positions)
            metadata = self._position_metadata(connection, table, selected)
            resolved = self._text_position_representatives(connection, table, metadata)
            if all(self._is_representative_position(row, resolved) for row in metadata):
                return left_positions, right_positions
            representatives.update(resolved)
            if len(representatives) > 20_002 or overscan >= 20_002:
                raise SightglassError(
                    ErrorCode.SOURCE_INCOMPLETE,
                    details={"warning_codes": ["native_traversal_limit_reached"]},
                )

    @staticmethod
    def _position_metadata(
        connection: Any, table: str, positions: tuple[tuple[int, int, int], ...],
    ) -> list[Any]:
        rowids = tuple(sorted({position[2] for position in positions}))
        if not rowids:
            return []
        placeholders = ",".join("?" for _value in rowids)
        return connection.execute(
            f"""SELECT rowid AS source_rowid, server_id, local_type, create_time,
                       COALESCE(sort_seq, 0) AS sort_seq
                FROM [{table}] WHERE rowid IN ({placeholders})""", rowids,
        ).fetchall()

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
        """Read exact neighbors with one unordered position pass per shard.

        The caller obtained ``focus`` in this same session. Native installations
        may have no create_time-leading index, and sort_seq is not chronological.
        Seek positions when the query plan proves an existing timeline index;
        otherwise scan positions once and retain bounded candidates for both sides.
        Merge using the complete canonical key (including payload-dependent ID ties).
        Session exit still validates every opened dependency before admission.
        """

        state = self._assert_snapshot(snapshot)
        self._require_account(account_id)
        self._enforce_scope(
            state,
            account_id=account_id,
            conversation_source_id=conversation_source_id,
        )
        if before < 0 or after < 0:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if focus.source_conversation_id != conversation_source_id:
            raise SightglassError(ErrorCode.MESSAGE_NOT_FOUND)
        conversation = self._conversation(state, conversation_source_id)
        if conversation is None:
            raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        if not before and not after:
            return SourceMessagePage((focus,))
        self._contacts(state)
        table = self._table_name(conversation_source_id)
        shards = self._message_shards(state, table)
        self._record_message_dependencies(state, snapshot, shards)
        boundary = self._sql_boundary(focus.sort_key)
        positions_by_shard: dict[
            str, tuple[list[tuple[int, int, int]], list[tuple[int, int, int]]]
        ] = {}
        rows_by_shard: dict[str, dict[int, Any]] = {}
        for relative in shards:
            with self._connect(relative) as connection:
                left_positions, right_positions = self._context_positions(
                    connection, table, boundary, before, after
                )
            positions_by_shard[relative] = (left_positions, right_positions)
            rowids = tuple(
                sorted(
                    {
                        position[2]
                        for position in (*left_positions, *right_positions)
                        if relative != focus.logical_shard_key
                        or position[2] != focus.source_rowid
                    }
                )
            )
            rows_by_shard[relative] = self._message_rows_by_rowid(
                state, relative, table, rowids
            )

        def messages_for_positions(
            relative: str, positions: list[tuple[int, int, int]]
        ) -> Iterator[SourceMessage]:
            for position in positions:
                check_operation_budget()
                if relative == focus.logical_shard_key and position[2] == focus.source_rowid:
                    yield focus
                else:
                    yield self._message(
                        state,
                        conversation,
                        rows_by_shard[relative][position[2]],
                        relative,
                        snapshot,
                        include_resources=False,
                    )

        seen = {focus.source_message_id: self._message_evidence_digest(focus)}
        sides: list[list[SourceMessage]] = []
        for side, radius in enumerate((before, after)):
            selected: list[SourceMessage] = []
            if radius:
                merged = heapq.merge(
                    *(
                        messages_for_positions(relative, positions[side])
                        for relative, positions in positions_by_shard.items()
                    ),
                    key=lambda item: item.sort_key.as_tuple(),
                    reverse=side == 0,
                )
                for scanned, message in enumerate(merged, start=1):
                    if scanned > 20_002:
                        raise SightglassError(
                            ErrorCode.SOURCE_INCOMPLETE,
                            details={"warning_codes": ["native_traversal_limit_reached"]},
                        )
                    evidence = self._message_evidence_digest(message)
                    previous = seen.get(message.source_message_id)
                    if previous is not None:
                        if previous != evidence:
                            raise SightglassError(
                                ErrorCode.SOURCE_INCOMPLETE,
                                details={"warning_codes": ["duplicate_message_identity_conflict"]},
                            )
                        continue
                    # A three-field tie can fall on the opposite side of the focus
                    # once its exact source ID is known. Do not consume it here.
                    key = message.sort_key.as_tuple()
                    if (side == 0 and key >= focus.sort_key.as_tuple()) or (
                        side == 1 and key <= focus.sort_key.as_tuple()
                    ):
                        continue
                    seen[message.source_message_id] = evidence
                    selected.append(message)
                    if len(selected) == radius + 1:
                        break
            sides.append(selected)
        left_messages, right_messages = sides
        selected_neighbors = (*reversed(left_messages[:before]), *right_messages[:after])
        # A retained neighbor can carry a server ID that also appears in another
        # shard outside this neighbor radius. get_message already rejects such a
        # duplicate identity conflict; read_context must reconcile the exact
        # neighbors it is about to return under the same session/budget, and it
        # must do so before resource hydration so a conflict never triggers
        # resource work.
        self._reconcile_context_neighbors(state, conversation, table, selected_neighbors, snapshot)
        hydrated = [
            self._message(
                state,
                conversation,
                rows_by_shard[message.logical_shard_key][message.source_rowid],
                message.logical_shard_key,
                snapshot,
            )
            for message in selected_neighbors
        ]
        result = sorted((*hydrated, focus), key=lambda item: item.sort_key.as_tuple())
        self._assert_snapshot(snapshot)
        return SourceMessagePage(
            tuple(result),
            has_more_before=len(left_messages) > before,
            has_more_after=len(right_messages) > after,
        )

    def _reconcile_context_neighbors(
        self,
        state: _SnapshotState,
        conversation: SourceConversation,
        table: str,
        neighbors: tuple[SourceMessage, ...],
        snapshot: SourceSnapshot,
    ) -> None:
        """Reconcile retained neighbor identities across shards, batched and bounded.

        Only ``server`` identities can collide across shards without the shard being
        part of the token; ``local``/``fallback`` tokens already carry their shard.
        A single ``server_id IN (...)`` query per shard resolves every retained
        neighbor id at once, so the added cost is one bounded statement per shard
        rather than one lookup per neighbor. Cancellation/deadline budgets apply to
        every step, and a differing payload for one identity fails closed exactly as
        ``get_message`` does.
        """

        server_ids: set[int] = set()
        for message in neighbors:
            decoded = self._decode_message_token(message.source_message_id)
            if decoded is None:
                continue
            kind, identity = decoded[1], decoded[2]
            if kind == "server":
                server_ids.add(int(identity[0]))
        if not server_ids:
            return
        expected: dict[str, str] = {}
        for message in neighbors:
            decoded = self._decode_message_token(message.source_message_id)
            if decoded is not None and decoded[1] == "server":
                expected[message.source_message_id] = self._message_evidence_digest(message)
        if not expected:
            return
        ordered = tuple(sorted(server_ids))
        for relative in self._message_shards(state, table):
            check_operation_budget()
            rows = self._server_identity_rows(state, relative, table, ordered)
            for row in rows:
                check_operation_budget()
                message = self._message(
                    state, conversation, row, relative, snapshot, include_resources=False
                )
                previous = expected.get(message.source_message_id)
                if previous is None:
                    continue
                if previous != self._message_evidence_digest(message):
                    raise SightglassError(
                        ErrorCode.SOURCE_INCOMPLETE,
                        details={"warning_codes": ["duplicate_message_identity_conflict"]},
                    )

    def get_message(
        self,
        account_id: str,
        source_message_id: str,
        snapshot: SourceSnapshot,
    ) -> SourceMessage | None:
        decoded = self._decode_message_token(source_message_id)
        if decoded is None:
            return None
        conversation_id, kind, identity = decoded
        state = self._assert_snapshot(snapshot)
        self._require_account(account_id)
        self._enforce_scope(
            state,
            account_id=account_id,
            conversation_source_id=conversation_id,
            source_message_id=source_message_id,
        )
        conversation = self._conversation(state, conversation_id)
        if conversation is None:
            return None
        self._contacts(state)
        table = self._table_name(conversation_id)
        relatives = self._message_shards(state, table)
        self._record_message_dependencies(state, snapshot, relatives)
        if kind == "server":
            selected_relatives = relatives
            where = "server_id = ?"
            params: tuple[Any, ...] = (identity[0],)
        elif kind == "local":
            relative, local_id, create_time, local_type = identity
            selected_relatives = (relative,) if relative in relatives else ()
            where = "local_id = ? AND create_time = ? AND local_type = ?"
            params = (local_id, create_time, local_type)
        else:
            relative, rowid, create_time, sort_seq, local_type, _payload_digest = identity
            selected_relatives = (relative,) if relative in relatives else ()
            where = "rowid = ? AND create_time = ? AND COALESCE(sort_seq, 0) = ? AND local_type = ?"
            params = (rowid, create_time, sort_seq, local_type)
        matches: list[SourceMessage] = []
        rows_by_position: dict[tuple[str, int], Any] = {}
        for relative in selected_relatives:
            real_sender = self._real_sender_expression(
                state,
                relative,
                table,
                row_alias="message_row",
            )
            raw_sender = self._raw_sender_expression(state, relative, table)
            with self._connect(relative) as connection:
                rows = connection.execute(
                    f"""
                    SELECT message_row.rowid AS source_rowid,
                           message_row.local_id, message_row.server_id,
                           message_row.local_type,
                           COALESCE(message_row.sort_seq, 0) AS sort_seq,
                           message_row.create_time, message_row.status,
                           message_row.message_content,
                           message_row.WCDB_CT_message_content AS content_type,
                           message_row.packed_info_data,
                           {real_sender} AS real_sender_username,
                           {raw_sender} AS real_sender_source_id
                    FROM [{table}] AS message_row
                    WHERE {where}
                    ORDER BY message_row.rowid ASC
                    LIMIT 20003
                    """,
                    params,
                ).fetchall()
            if len(rows) > 20_002:
                raise SightglassError(
                    ErrorCode.SOURCE_INCOMPLETE,
                    details={"warning_codes": ["native_traversal_limit_reached"]},
                )
            if kind == "server":
                rows = self._collapse_equivalent_text_rows(rows)
            for row in rows:
                rows_by_position[(relative, int(row["source_rowid"]))] = row
                matches.append(self._message(
                    state, conversation, row, relative, snapshot, include_resources=False
                ))
            if len(matches) > 20_002:
                raise SightglassError(
                    ErrorCode.SOURCE_INCOMPLETE,
                    details={"warning_codes": ["native_traversal_limit_reached"]},
                )
        matches = [message for message in matches if message.source_message_id == source_message_id]
        if not matches:
            return None
        evidence = {self._message_evidence_digest(message) for message in matches}
        if len(evidence) != 1:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                details={"warning_codes": ["duplicate_message_identity_conflict"]},
            )
        selected = min(matches, key=lambda message: message.sort_key.as_tuple())
        return self._message(
            state, conversation,
            rows_by_position[(selected.logical_shard_key, selected.source_rowid)],
            selected.logical_shard_key, snapshot,
        )

    def capture_resource_binding(
        self, request: CaptureRequest, snapshot: SourceSnapshot,
    ) -> ResourceCaptureBinding:
        """Authenticate the exact opaque locator without hydrating its message."""
        from sightglass.contracts.capture import CaptureProtocolError
        from sightglass.source.capture.resource import request_resource_binding

        state = self._assert_snapshot(snapshot)
        self._require_account(request.account_id)
        self._enforce_scope(state, source_resource_key=request.resource_key)
        binding = request_resource_binding(request)
        locator = self._resource_resolver._decode_locator(request.resource_key or "")
        descriptor = request.resource_descriptor
        if descriptor is None or (
            locator.source_message_id != binding.source_message_id
            or locator.conversation_source_id != binding.conversation_source_id
            or locator.kind != descriptor.kind
            or locator.declared_size != descriptor.declared_size
            or locator.declared_hash != descriptor.declared_hash
            or locator.original_name != descriptor.original_name
        ):
            raise CaptureProtocolError("resource_locator_scope_mismatch")
        return binding

    def read_resource(
        self,
        source_resource_key: str,
        *,
        max_bytes: int,
        snapshot: SourceSnapshot,
    ) -> SourceResourcePayload:
        state = self._assert_snapshot(snapshot)
        self._enforce_scope(state, source_resource_key=source_resource_key)
        data, variant = self._resource_resolver.read(source_resource_key, max_bytes=max_bytes)
        self._assert_snapshot(snapshot)
        return SourceResourcePayload(
            source_resource_key=source_resource_key, data=data, variant=variant
        )

    def catalog_complete(self, snapshot: SourceSnapshot) -> bool:
        state = self._assert_snapshot(snapshot)
        self._conversations(state)
        return state.catalog_unresolved_tables == 0

    def active_conversations_only(self, snapshot: SourceSnapshot) -> bool:
        self._assert_snapshot(snapshot)
        return False

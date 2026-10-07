"""Durable Cloudflare semantic candidate lane over canonical admitted messages.

The lane owns a private sidecar (its own SQLite database) holding:

* a small content-free state/publication header, and
* a per-admitted-message manifest of the exact remote identity, the captured
  canonical version, the encoder input hash and the *real* float32 vector bytes.

A durable *send intent* is written before any network mutation. An interrupted or
ambiguous submission is only ever resolved by readback against the stored bytes;
it is never re-encoded or re-submitted. Every remote id is read back and fenced
against the *current* canonical window.db observation before it can enter a
candidate set.

Invariants:

* Disabled by default: constructing without ``enabled`` performs no network,
  credential or sidecar action.
* Only current-epoch admitted messages that intersect the configured
  conversations *and* the live reader policy may be captured.
* All network calls happen outside a window.db transaction; the sidecar uses its
  own short transactions and never pretends to be atomic with window.db.
* Query text and message bodies never enter logs, status or receipts.
* Hard account/conversation/time/sender/kind/watermark constraints are applied as
  a remote pre-filter *before* topK and re-fenced locally afterwards.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import struct
import threading
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.current_body import (
    card_search_values,
    current_body_text,
    current_search_document,
)
from sightglass.model.links import LinkRepository
from sightglass.model.repositories import WindowRepository
from sightglass.operations import check_operation_budget, operation_remaining_seconds
from sightglass.policy.readers import ReaderContext

from .cloudflare import CloudflareError
from .settings import (
    ACTIVE_DIMENSIONS,
    ACTIVE_METRIC,
    ACTIVE_MODEL,
    ACTIVE_RECIPE,
    SemanticSettings,
)

SEMANTIC_RECIPE = ACTIVE_RECIPE
RECIPE_VERSION = 2
MAX_KINDS = 8
MAX_CONCEPT_CHARS = 500
MAX_INPUT_CHARS = 16_000
ENCODE_BATCH_LIMIT = 16
_INDEX_ONCE_CEILING = 128
_TOP_K = 20
# Remote query sub-budget for one retrieval call, separate from the configured
# per-request timeout. Operator reload must be able to cancel between requests.
QUERY_BUDGET_SECONDS = 8.0

# Metadata indexes the operator must pre-provision on the dedicated index. Nothing
# here is auto-created; ``verify_index`` fails closed if any is missing/typed wrong.
# ``sent_at``/``watermark`` are numeric; ``sender``/``kind`` are strings; ``has_link``
# lets a link-kind query pre-filter without trusting a possibly-URL-bearing text body.
REQUIRED_METADATA_INDEXES: tuple[tuple[str, str], ...] = (
    ("sent_at", "number"),
    ("sender", "string"),
    ("kind", "string"),
    ("watermark", "number"),
    ("has_link", "boolean"),
    ("conversation", "string"),
)

_SIDECAR_SCHEMA = """
CREATE TABLE IF NOT EXISTS semantic_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    generation INTEGER NOT NULL,
    state TEXT NOT NULL,
    recipe TEXT NOT NULL,
    model TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    metric TEXT NOT NULL,
    indexed_count INTEGER NOT NULL DEFAULT 0,
    indexed_bytes INTEGER NOT NULL DEFAULT 0,
    coverage TEXT NOT NULL DEFAULT 'partial',
    last_error TEXT,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS semantic_publication (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    generation INTEGER NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS semantic_identity (
    id INTEGER PRIMARY KEY CHECK (id=1),
    store_fingerprint TEXT NOT NULL,
    projection_epoch TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS semantic_entries (
    message_id TEXT PRIMARY KEY,
    namespace TEXT NOT NULL,
    remote_id TEXT NOT NULL UNIQUE,
    unit_kind TEXT NOT NULL,
    observation_seq INTEGER NOT NULL,
    first_observation_seq INTEGER NOT NULL,
    projection_epoch TEXT NOT NULL,
    account_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    conversation_digest TEXT NOT NULL,
    sender_digest TEXT NOT NULL,
    kind TEXT NOT NULL,
    sent_at_utc TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    vector BLOB NOT NULL,
    generator INTEGER NOT NULL,
    published INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS semantic_entries_fence
    ON semantic_entries(account_id, conversation_id, generator, published);
CREATE TABLE IF NOT EXISTS semantic_intents (
    remote_id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    generation INTEGER NOT NULL,
    input_hash TEXT NOT NULL,
    vector BLOB NOT NULL,
    observation_seq INTEGER NOT NULL,
    conversation_id TEXT NOT NULL,
    conversation_digest TEXT NOT NULL,
    sender_digest TEXT NOT NULL,
    kind TEXT NOT NULL,
    sent_at_utc TEXT NOT NULL,
    has_link INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS semantic_intents_fence
    ON semantic_intents(generation, namespace);
CREATE TABLE IF NOT EXISTS semantic_capture_checkpoint (
    conversation_id TEXT PRIMARY KEY,
    last_message_id TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS semantic_scan_cursor (
    id INTEGER PRIMARY KEY CHECK (id=1),
    last_conversation_id TEXT NOT NULL
);
INSERT OR IGNORE INTO semantic_scan_cursor VALUES (1, '');
CREATE TABLE IF NOT EXISTS semantic_coverage (
    generation INTEGER NOT NULL,
    conversation_id TEXT NOT NULL,
    covered INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (generation, conversation_id)
);
"""

_MIB = 1024 * 1024


@dataclass(frozen=True)
class SemanticCandidates:
    message_ids: tuple[str, ...]
    receipt: dict[str, Any]


class SemanticBackend(Protocol):
    """The subset of :class:`CloudflareBackend` the service depends on."""

    def verify_index(self, *, required_metadata: tuple[tuple[str, str], ...]) -> Any: ...

    def encode(self, texts: tuple[str, ...]) -> tuple[tuple[float, ...], ...]: ...

    def upsert(self, rows: list[dict[str, Any]]) -> None: ...

    def get_by_ids(self, ids: list[str]) -> list[dict[str, Any]]: ...

    def query(
        self,
        vector: Sequence[float],
        namespace: str,
        filter: dict[str, Any],
        top_k: int,
    ) -> list[dict[str, Any]]: ...


# -- content-free derivations ------------------------------------------------
def _namespace(source_account_id: str, epoch: str, generation: int) -> str:
    """Namespace binds source account, projection epoch, model recipe and generation.

    Reusing the same Vectorize namespace across a different account, a different
    projection epoch or a superseded model recipe would silently mix incompatible
    vectors, so every dimension of the identity participates. Vectorize caps a
    namespace at 64 bytes, so the identity is a 64-character digest.
    """
    digest = hashlib.sha256(
        json.dumps(
            [source_account_id, epoch, ACTIVE_MODEL, SEMANTIC_RECIPE, int(generation)],
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return digest


def _sender_digest(source_account_id: str, sender_id: str | None) -> str:
    return hashlib.sha256(
        json.dumps([source_account_id, sender_id or ""], separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _conversation_digest(source_account_id: str, conversation_id: str) -> str:
    """Opaque, account-scoped conversation token; the raw id never leaves locally."""
    return hashlib.sha256(
        json.dumps([source_account_id, conversation_id], separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _remote_id(namespace: str, message_id: str, input_hash: str, observation_seq: int) -> str:
    """Opaque, version-bound remote vector id (not the canonical message id).

    The id binds the captured canonical observation and its encoder-input hash, so a
    correction publishes a *different* remote object. The superseded object remains
    on the remote index but can never be admitted (its manifest entry is replaced and
    its local identity no longer exists).
    """
    return hashlib.sha256(
        json.dumps(
            [namespace, message_id, input_hash, int(observation_seq)],
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _encoder_input(row: Any) -> str:
    """Stable, bounded encoder input from canonical text plus approved card fields.

    Only the canonical body/search text and an optional link-card title/description
    are used. Raw transport envelopes, resource paths and any non-text payload stay
    outside the encoder input.
    """
    # An unknown-message display placeholder is not source-authored evidence.
    # Empty resource messages remain available through deterministic neighbors,
    # but cannot become a semantic focus merely because an empty vector is a hub.
    if row["kind"] == "unknown":
        return ""
    parts: list[str] = []

    def append(value: Any) -> None:
        if isinstance(value, str) and value.strip() and value not in parts:
            parts.append(value)

    # Recipe v2 input order is fixed: the canonical recall document (card search
    # text, else the body), then the body column, then the independent card
    # title/description. The card document and the body are distinct strings, so
    # dropping a duplicate stored body representation is input-invariant while
    # the joined card document still contributes.
    append(current_search_document(row))
    append(current_body_text(row))
    with suppress(ValueError, TypeError):
        for value in card_search_values(row):
            append(value)
    text = "\n".join(parts)[:MAX_INPUT_CHARS]
    return text


def _input_hash(row: Any) -> str:
    """Hash of the exact canonical version *and* the actual encoder input."""
    return hashlib.sha256(
        json.dumps(
            [
                str(row["message_id"]),
                str(row["sent_at_utc"]),
                str(row["kind"]),
                row["sender_id"],
                _encoder_input(row),
                int(row["current_observation_seq"]),
                str(row["projection_epoch"]),
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _pack_vector(vector: Sequence[float]) -> bytes:
    values = tuple(float(value) for value in vector)
    if len(values) != ACTIVE_DIMENSIONS:
        raise SightglassError(ErrorCode.INTERNAL_ERROR, details={"reason": "vector_dimensions"})
    return struct.pack(f"<{len(values)}f", *values)


def _unpack_vector(blob: Any) -> tuple[float, ...] | None:
    if not isinstance(blob, (bytes, bytearray)) or len(blob) != ACTIVE_DIMENSIONS * 4:
        return None
    return struct.unpack(f"<{ACTIVE_DIMENSIONS}f", bytes(blob))


def _sent_at_number(value: str) -> float | None:
    """Cloudflare numeric metadata must be a finite UTC epoch number."""
    from datetime import UTC, datetime

    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    stamp = parsed.timestamp()
    if stamp != stamp or stamp in (float("inf"), float("-inf")):
        return None
    return stamp


class SemanticService:
    def __init__(
        self,
        repository: WindowRepository,
        reader: ReaderContext,
        *,
        settings: SemanticSettings,
        backend: SemanticBackend | None,
        epoch_factory: Callable[[], str],
        sidecar_path: Path,
    ) -> None:
        if not isinstance(settings, SemanticSettings):
            raise ValueError("semantic service requires SemanticSettings")
        self.repository = repository
        self.reader = reader
        self.settings = settings
        self.backend = backend
        self._epoch_factory = epoch_factory
        # Keep the path absolute *without* resolving the leaf, so a symlinked target
        # cannot be silently followed at construction time.
        self._sidecar_path = Path(os.path.abspath(str(Path(sidecar_path).expanduser())))
        self._storage = repository.database.storage
        self._links = LinkRepository(repository.database)
        self._lock = threading.RLock()
        self._connection: sqlite3.Connection | None = None
        if settings.enabled:
            try:
                self._open_sidecar()
            except BaseException:
                self.close()
                raise

    # -- lifecycle ---------------------------------------------------------
    @property
    def enabled(self) -> bool:
        return bool(self.settings.enabled and self.backend is not None)

    def _sidecar_files(self) -> tuple[Path, ...]:
        return tuple(Path(str(self._sidecar_path) + suffix) for suffix in ("", "-wal", "-shm"))

    def _open_sidecar(self) -> None:
        parent = self._sidecar_path.parent
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent_meta = parent.lstat()
        if (
            not stat.S_ISDIR(parent_meta.st_mode)
            or parent_meta.st_mode & 0o077
            or parent_meta.st_uid != os.geteuid()
        ):
            raise RuntimeError("semantic sidecar directory must be a private directory (0700)")
        with self._storage_reservation(256 * 1024):
            try:
                leaf = self._sidecar_path.lstat()
            except FileNotFoundError:
                leaf = None
            if leaf is not None and (
                not stat.S_ISREG(leaf.st_mode)
                or leaf.st_nlink != 1
                or leaf.st_mode & 0o077
                or leaf.st_uid != os.geteuid()
            ):
                raise RuntimeError("semantic sidecar must be an owner-private regular file")
            connection = sqlite3.connect(self._sidecar_path, check_same_thread=False)
            self._connection = connection
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(_SIDECAR_SCHEMA)
            os.chmod(self._sidecar_path, 0o600)
            with connection:
                connection.execute(
                    "INSERT OR IGNORE INTO semantic_identity VALUES (1, ?, ?)",
                    (self._store_fingerprint(), self._epoch_factory()),
                )
                connection.execute(
                    "INSERT OR IGNORE INTO semantic_state"
                    "(id, generation, state, recipe, model, dimensions, metric, updated_at)"
                    " VALUES (1, 1, 'unindexed', ?, ?, ?, ?, '')",
                    (SEMANTIC_RECIPE, ACTIVE_MODEL, ACTIVE_DIMENSIONS, ACTIVE_METRIC),
                )
                connection.execute(
                    "INSERT OR IGNORE INTO semantic_publication"
                    "(id, generation, revision, updated_at) VALUES (1, 1, 0, '')"
                )
            for suffix in ("", "-wal", "-shm"):
                with suppress(FileNotFoundError):
                    os.chmod(str(self._sidecar_path) + suffix, 0o600)
        self._track_sidecar()
        self._assert_recipe_identity()

    def _assert_recipe_identity(self) -> None:
        """Fail closed when the persisted recipe/model/dimensions differ."""
        row = self._state_row()
        if (
            str(row["recipe"]) != SEMANTIC_RECIPE
            or str(row["model"]) != ACTIVE_MODEL
            or int(row["dimensions"]) != ACTIVE_DIMENSIONS
            or str(row["metric"]) != ACTIVE_METRIC
        ):
            raise RuntimeError(
                "semantic sidecar recipe/model identity differs; run an explicit rebuild"
            )
        if (
            self._db()
            .execute("SELECT store_fingerprint FROM semantic_identity WHERE id=1")
            .fetchone()[0]
            != self._store_fingerprint()
        ):
            raise RuntimeError("semantic sidecar belongs to a different store or source account")

    def _store_fingerprint(self) -> str:
        return hashlib.sha256(
            json.dumps(
                [
                    self.settings.cf_account_id,
                    self.settings.index_name,
                    self.settings.source_account_id,
                ],
                separators=(",", ":"),
            ).encode()
        ).hexdigest()

    def _epoch_current(self) -> bool:
        with self._lock:
            return (
                self._db()
                .execute("SELECT projection_epoch FROM semantic_identity WHERE id=1")
                .fetchone()[0]
                == self._epoch_factory()
            )

    @contextmanager
    def _storage_reservation(self, amount: int, *, background: bool = True) -> Iterator[None]:
        """Real storage admission + verification for a sidecar mutation."""
        if self._storage is None:
            yield
            return
        with self._storage.reserve(amount, background=background) as lease:
            yield
            lease.verify()

    def close(self) -> None:
        with self._lock:
            if self._connection is not None:
                if self._storage is not None:
                    self._storage.track(*self._sidecar_files())
                self._connection.close()
                self._connection = None

    def _backend(self) -> SemanticBackend:
        backend = self.backend
        if backend is None:
            raise SightglassError(ErrorCode.SERVICE_UNAVAILABLE, details={"reason": "disabled"})
        return backend

    def _db(self) -> sqlite3.Connection:
        with self._lock:
            if self._connection is None:
                raise SightglassError(ErrorCode.SERVICE_UNAVAILABLE, details={"reason": "closed"})
            return self._connection

    def _linked_message_ids(self, message_ids: tuple[str, ...]) -> set[str]:
        """Authoritative materialized link evidence for the captured versions.

        Uses the versioned link store rather than guessing from body text, so a
        "link" kind filter never misses a URL-bearing card and never accepts a
        message whose links were not actually extracted.
        """
        if not message_ids:
            return set()
        return {str(row["message_id"]) for row in self._links.links_for_messages(message_ids)}

    def _track_sidecar(self) -> None:
        if self._storage is not None:
            self._storage.track(*self._sidecar_files())

    # -- remote scheduling -------------------------------------------------
    def _remaining(self) -> float:
        remaining = operation_remaining_seconds()
        if remaining is None:
            return float(self.settings.timeout_seconds)
        return max(0.0, min(float(self.settings.timeout_seconds), remaining))

    def _call(self, operation: Callable[[float], Any]) -> Any:
        """Run one remote operation under the operation budget and a bounded timeout."""
        check_operation_budget()
        budget = self._remaining()
        if budget <= 0:
            check_operation_budget()
            raise SightglassError(ErrorCode.SERVICE_TIMEOUT, retryable=True)
        result = operation(budget)
        check_operation_budget()
        return result

    # -- status ------------------------------------------------------------
    def _state_row(self) -> sqlite3.Row:
        row = self._db().execute("SELECT * FROM semantic_state WHERE id=1").fetchone()
        assert row is not None
        return row

    def state_token(self) -> str:
        if not self.enabled:
            return "0:0:0"
        with self._lock:
            generation = int(self._state_row()["generation"])
            revision = int(
                self._db()
                .execute("SELECT revision FROM semantic_publication WHERE id=1")
                .fetchone()[0]
            )
        return f"{RECIPE_VERSION}:{generation}:{revision}"

    def status(self) -> dict[str, Any]:
        """Content-free lane state; never scans message text or bodies."""
        common = {
            "schema": "sightglass.semantic-status.v1",
            "model": ACTIVE_MODEL,
            "recipe": SEMANTIC_RECIPE,
            "dimensions": ACTIVE_DIMENSIONS,
            "metric": ACTIVE_METRIC,
        }
        if not self.enabled:
            return {**common, "state": "disabled"}
        with self._lock:
            state = self._state_row()
            revision = int(
                self._db()
                .execute("SELECT revision FROM semantic_publication WHERE id=1")
                .fetchone()[0]
            )
            pending = int(self._db().execute("SELECT COUNT(*) FROM semantic_intents").fetchone()[0])
            configured = len(self.settings.conversation_ids)
            generation = int(state["generation"])
            namespace = _namespace(
                self.settings.source_account_id, self._epoch_factory(), generation
            )
            published = int(
                self._db()
                .execute(
                    "SELECT COUNT(*) FROM semantic_entries"
                    " WHERE published=1 AND generator=? AND namespace=?",
                    (generation, namespace),
                )
                .fetchone()[0]
            )
            bytes_ = int(state["indexed_bytes"])
        return {
            **common,
            "state": str(state["state"]) if self._epoch_current() else "rebuilding",
            "coverage": self._coverage_state(self._capable_conversations(), generation),
            "generation": generation,
            "revision": revision,
            "indexed": int(state["indexed_count"]),
            "indexed_scope": published,
            "pending": self._outstanding_intents(generation),
            "pending_total": pending,
            "bytes": bytes_,
            "failure": state["last_error"],
            "configured_conversations": configured,
        }

    # -- scope -------------------------------------------------------------
    def _configured_conversations(self) -> tuple[str, ...]:
        return tuple(self.settings.conversation_ids)

    def _capable_conversations(self) -> tuple[str, ...]:
        """Configured conversations that still exist, are permitted, and are intact."""
        account = self.settings.source_account_id
        if not account or self.reader.paused or not self.reader.policy.search:
            return ()
        available = {
            str(row["conversation_id"]) for row in self.repository.account_conversations(account)
        }
        degraded = self.repository.degraded_conversation_ids(
            account, error_code="duplicate_message_identity_conflict"
        )
        return tuple(
            value
            for value in self._configured_conversations()
            if value in available and value not in degraded and self.reader.policy.permits(value)
        )

    def _checkpoint(self, conversation_id: str) -> str | None:
        with self._lock:
            row = (
                self._db()
                .execute(
                    "SELECT last_message_id FROM semantic_capture_checkpoint"
                    " WHERE conversation_id=?",
                    (conversation_id,),
                )
                .fetchone()
            )
        if row is None or not str(row["last_message_id"]):
            return None
        return str(row["last_message_id"])

    def _capture(
        self,
        conversations: tuple[str, ...],
        *,
        limit: int,
        start_at: dict[str, str | None] | None = None,
    ) -> tuple[list[sqlite3.Row], dict[str, str]]:
        """Bounded, fair, checkpoint-resumable capture of canonical scope rows.

        Returns the captured rows and the new per-conversation checkpoint (the last
        examined message id) so a later call continues where this one stopped
        instead of re-reading the first page forever.
        """
        if not conversations:
            return [], {}
        with self._lock:
            last_conversation = (
                self._db()
                .execute("SELECT last_conversation_id FROM semantic_scan_cursor WHERE id=1")
                .fetchone()[0]
            )
        if last_conversation in conversations:
            offset = conversations.index(last_conversation) + 1
            conversations = conversations[offset:] + conversations[:offset]
        epoch = self._epoch_factory()
        watermark = self.repository.observation_watermark()
        per_conversation = max(1, -(-int(limit) // len(conversations)))
        rows: list[sqlite3.Row] = []
        checkpoint: dict[str, str] = {}
        with self.repository.database.read_snapshot():
            for conversation_id in conversations:
                if len(rows) >= limit:
                    break
                resume_id = (start_at or {}).get(conversation_id)
                after_key_value = None
                if resume_id:
                    anchor = self.repository.materialized_message_rows(
                        conversation_id,
                        projection_epoch=epoch,
                        observation_watermark=watermark,
                        limit=1,
                        direction="forward",
                        message_id=resume_id,
                    )
                    if anchor and str(anchor[0]["message_id"]) == resume_id:
                        after_key_value = _source_sort_key(anchor[0])
                batch = self.repository.materialized_message_rows(
                    conversation_id,
                    projection_epoch=epoch,
                    observation_watermark=watermark,
                    limit=min(per_conversation, limit - len(rows)),
                    direction="forward",
                    after=after_key_value,
                )
                if batch:
                    checkpoint[conversation_id] = str(batch[-1]["message_id"])
                else:
                    # The forward page is exhausted for this pass: clear the checkpoint
                    # so the next pass re-examines from the conversation start. This is
                    # what requeues a corrected row without a full rebuild.
                    checkpoint[conversation_id] = ""
                rows.extend(batch)
        rows.sort(key=lambda row: (str(row["conversation_id"]), str(row["message_id"])))
        return rows[:limit], checkpoint

    # -- indexing ----------------------------------------------------------
    def index_once(self, limit: int = 32) -> dict[str, Any]:
        if not self.enabled:
            raise SightglassError(ErrorCode.SERVICE_UNAVAILABLE, details={"reason": "disabled"})
        bounded = max(1, min(int(limit), _INDEX_ONCE_CEILING))
        if not self._epoch_current():
            self.request_rebuild()
        conversations = self._capable_conversations()
        if not conversations:
            return self._result("disabled", 0, 0, 0, None)
        generation = self._generation()
        # 1. Resolve any durable, previously-submitted intent by readback only.
        resumed = self._resume_intents(conversations, generation)
        outstanding = self._outstanding_intents(generation)
        # 2. Advance the capture cursor fairly across conversations.
        start = {
            conversation_id: self._checkpoint(conversation_id) for conversation_id in conversations
        }
        rows, checkpoint = self._capture(conversations, limit=bounded, start_at=start)
        failure: str | None = None
        published = 0
        if rows:
            pending = [row for row in rows if self._needs_publication(row, generation)]
            if pending:
                try:
                    published, failure = self._publish(pending, generation)
                except _DriftError:
                    with self._lock:
                        self._set_state("degraded", error="canonical_drift")
                    return self._result(
                        "degraded", resumed, len(pending), resumed, "canonical_drift"
                    )
        # A capture pass with no trailing rows means the forward cursor is exhausted
        # for this conversation this generation: mark it covered so a *complete* pass
        # over every capable conversation can advance the lane to ready.
        if failure is None or failure in {"readback_pending", "upsert_ambiguous"}:
            self._save_checkpoint(checkpoint)
            self._mark_coverage(conversations, checkpoint, generation)
        outstanding = self._outstanding_intents(generation)
        if failure == "upsert_ambiguous":
            failure = "readback_pending"
        if failure is None and outstanding:
            failure = "readback_pending"
        state = self._lane_state(conversations, generation, failure)
        with self._lock:
            self._set_state(state, error=failure)
        return self._result(
            state,
            resumed + published,
            outstanding,
            resumed + published,
            failure,
            scanned=len(rows),
        )

    @staticmethod
    def _result(
        state: str,
        indexed: int,
        pending: int,
        processed: int,
        failure: str | None,
        *,
        scanned: int = 0,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "state": state,
            "indexed": indexed,
            "pending": pending,
            "processed": processed,
            "scanned": scanned,
        }
        if failure:
            result["failure"] = failure
        return result

    def _mark_coverage(
        self, conversations: tuple[str, ...], checkpoint: dict[str, str], generation: int
    ) -> None:
        """Record content-free per-conversation coverage for this generation.

        A conversation is covered once a capture pass returns no trailing rows for it
        (``checkpoint[conversation] == ""``), meaning the forward cursor reached the
        end of the current scope. No message body is read here.
        """
        if not checkpoint:
            return
        with self._lock, self._storage_reservation(64 * 1024):
            connection = self._db()
            for conversation_id, last_message_id in checkpoint.items():
                covered = 1 if last_message_id == "" else 0
                if covered:
                    connection.execute(
                        "INSERT INTO semantic_coverage"
                        "(generation, conversation_id, covered, updated_at)"
                        " VALUES (?, ?, 1, ?) ON CONFLICT(generation, conversation_id)"
                        " DO UPDATE SET covered=1, updated_at=excluded.updated_at",
                        (generation, conversation_id, _now()),
                    )
            connection.commit()
        self._track_sidecar()

    def _lane_state(
        self, conversations: tuple[str, ...], generation: int, failure: str | None
    ) -> str:
        if failure:
            return "degraded"
        if self._fully_covered(conversations, generation):
            return "ready"
        return "building"

    def _fully_covered(self, conversations: tuple[str, ...], generation: int) -> bool:
        if not conversations:
            return False
        with self._lock:
            row = (
                self._db()
                .execute(
                    "SELECT COUNT(*) FROM semantic_coverage WHERE generation=? AND covered=1"
                    f" AND conversation_id IN ({','.join('?' for _ in conversations)})",
                    (generation, *conversations),
                )
                .fetchone()
            )
        return int(row[0]) == len(conversations)

    def _coverage_state(self, conversations: tuple[str, ...], generation: int) -> str:
        return (
            "complete"
            if (
                self._fully_covered(conversations, generation)
                and not self._outstanding_intents(generation)
                and self._epoch_current()
            )
            else "partial"
        )

    def coverage(self) -> str:
        """Content-free coverage for the configured scope in the current generation."""
        if not self.enabled:
            return "disabled"
        return self._coverage_state(self._capable_conversations(), self._generation())

    def _outstanding_intents(self, generation: int) -> int:
        with self._lock:
            namespace = _namespace(
                self.settings.source_account_id, self._epoch_factory(), generation
            )
            conversations = self._capable_conversations()
            if not conversations:
                return 0
            return int(
                self._db()
                .execute(
                    "SELECT COUNT(*) FROM semantic_intents WHERE generation=? AND namespace=?"
                    f" AND conversation_id IN ({','.join('?' for _ in conversations)})",
                    (generation, namespace, *conversations),
                )
                .fetchone()[0]
            )

    def _save_checkpoint(self, checkpoint: dict[str, str]) -> None:
        if not checkpoint:
            return
        with self._lock, self._storage_reservation(64 * 1024):
            connection = self._db()
            for conversation_id, message_id in checkpoint.items():
                connection.execute(
                    "INSERT INTO semantic_capture_checkpoint"
                    "(conversation_id, last_message_id, updated_at) VALUES (?, ?, ?)"
                    " ON CONFLICT(conversation_id) DO UPDATE SET"
                    " last_message_id=excluded.last_message_id, updated_at=excluded.updated_at",
                    (conversation_id, message_id, _now()),
                )
            connection.execute(
                "UPDATE semantic_scan_cursor SET last_conversation_id=? WHERE id=1",
                (next(reversed(checkpoint)),),
            )
            connection.commit()
        self._track_sidecar()

    def _needs_publication(self, row: sqlite3.Row, generation: int) -> bool:
        if not _encoder_input(row):
            return False
        message_id = str(row["message_id"])
        digest = _input_hash(row)
        namespace = _namespace(self.settings.source_account_id, self._epoch_factory(), generation)
        remote_id = _remote_id(namespace, message_id, digest, int(row["current_observation_seq"]))
        with self._lock:
            existing = (
                self._db()
                .execute("SELECT * FROM semantic_entries WHERE message_id=?", (message_id,))
                .fetchone()
            )
            if (
                existing is not None
                and existing["published"] == 1
                and existing["input_hash"] == digest
                and int(existing["observation_seq"]) == int(row["current_observation_seq"])
                and int(existing["generator"]) == generation
                and str(existing["namespace"]) == namespace
            ):
                return False
            intent = (
                self._db()
                .execute(
                    "SELECT generation, input_hash, vector, conversation_id FROM semantic_intents"
                    " WHERE remote_id=?",
                    (remote_id,),
                )
                .fetchone()
            )
        return not (
            intent is not None
            and int(intent["generation"]) == generation
            and str(intent["input_hash"]) == digest
        )

    def _publish(self, rows: list[sqlite3.Row], generation: int) -> tuple[int, str | None]:
        """Encode then durably intend, submit, and read back the exact stored bytes."""
        epoch = self._epoch_factory()
        try:
            self._backend().verify_index(required_metadata=REQUIRED_METADATA_INDEXES)
        except SightglassError as exc:
            return 0, exc.details["reason"] if isinstance(exc, CloudflareError) else exc.code.value
        records: list[dict[str, Any]] = []
        try:
            for start in range(0, len(rows), ENCODE_BATCH_LIMIT):
                batch = rows[start : start + ENCODE_BATCH_LIMIT]
                texts = tuple(_encoder_input(row) for row in batch)
                vectors = self._call(lambda _t, _texts=texts: self._backend().encode(_texts))
                if len(vectors) != len(batch):
                    return 0, "encode_mismatch"
                linked = self._linked_message_ids(tuple(str(row["message_id"]) for row in batch))
                for row, vector in zip(batch, vectors, strict=True):
                    blob = _pack_vector(vector)
                    message_id = str(row["message_id"])
                    namespace = _namespace(self.settings.source_account_id, epoch, generation)
                    digest = _input_hash(row)
                    records.append(
                        {
                            "row": row,
                            "message_id": message_id,
                            "conversation_id": str(row["conversation_id"]),
                            "conversation_digest": _conversation_digest(
                                self.settings.source_account_id, str(row["conversation_id"])
                            ),
                            "namespace": namespace,
                            "remote_id": _remote_id(
                                namespace,
                                message_id,
                                digest,
                                int(row["current_observation_seq"]),
                            ),
                            "input_hash": digest,
                            "vector_bytes": blob,
                            "kind": str(row["kind"]),
                            "sent_at": str(row["sent_at_utc"]),
                            "sender_digest": _sender_digest(
                                self.settings.source_account_id,
                                str(row["sender_id"]) if row["sender_id"] else None,
                            ),
                            "has_link": message_id in linked,
                        }
                    )
        except SightglassError as exc:
            return 0, exc.details["reason"] if isinstance(exc, CloudflareError) else exc.code.value
        # Canonical fence: refuse to publish a version that changed during encoding.
        with self.repository.database.transaction():
            live = {
                str(value["message_id"]): value
                for value in self.repository.frozen_message_rows(
                    tuple(record["message_id"] for record in records)
                )
            }
            for record in records:
                current = live.get(record["message_id"])
                if (
                    current is None
                    or current["current_state"] != "present"
                    or int(current["current_observation_seq"])
                    != int(record["row"]["current_observation_seq"])
                    or _input_hash(current) != record["input_hash"]
                ):
                    raise _DriftError(record["message_id"])
        # Durable intent (with the real vector bytes) before the network mutation.
        if not self._write_intents(records, generation):
            return 0, "generation_changed"
        remote_rows = [
            {
                "id": record["remote_id"],
                "namespace": record["namespace"],
                "values": list(_unpack_vector(record["vector_bytes"]) or ()),
                "metadata": _remote_metadata(record),
            }
            for record in records
        ]
        try:
            capable = set(self._capable_conversations())
            if any(record["conversation_id"] not in capable for record in records):
                return 0, "scope_changed"
            self._call(lambda _t: self._backend().upsert(remote_rows))
        except SightglassError:
            # Ambiguous/transport failure: intents stay durable and are only ever
            # resolved by readback, never re-encoded or re-submitted.
            return 0, "upsert_ambiguous"
        return self._readback(records, generation)

    def _write_intents(self, records: list[dict[str, Any]], generation: int) -> bool:
        with self._lock, self._storage_reservation(max(64 * 1024, len(records) * 8192)):
            connection = self._db()
            if self._generation() != generation:
                return False
            for record in records:
                connection.execute(
                    "INSERT OR REPLACE INTO semantic_intents"
                    "(remote_id, message_id, namespace, generation, input_hash, vector,"
                    " observation_seq, conversation_id, conversation_digest,"
                    " sender_digest, kind, sent_at_utc, has_link, created_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        record["remote_id"],
                        record["message_id"],
                        record["namespace"],
                        generation,
                        record["input_hash"],
                        record["vector_bytes"],
                        int(record["row"]["current_observation_seq"]),
                        record["conversation_id"],
                        record["conversation_digest"],
                        record["sender_digest"],
                        record["kind"],
                        record["sent_at"],
                        int(record["has_link"]),
                        _now(),
                    ),
                )
            connection.commit()
        self._track_sidecar()
        return True

    def _resume_intents(self, conversations: tuple[str, ...], generation: int) -> int:
        """Read back durable intents for the current generation; never re-submits."""
        if not conversations:
            return 0
        epoch = self._epoch_factory()
        namespaces = tuple(
            _namespace(self.settings.source_account_id, epoch, generation) for _ in conversations
        )
        with self._lock:
            intents = (
                self._db()
                .execute(
                    f"SELECT * FROM semantic_intents WHERE generation=?"
                    f" AND namespace IN ({','.join('?' for _ in namespaces)})"
                    f" AND conversation_id IN ({','.join('?' for _ in conversations)}) LIMIT 32",
                    (generation, *namespaces, *conversations),
                )
                .fetchall()
            )
        if not intents:
            return 0
        ids = tuple(str(intent["message_id"]) for intent in intents)
        live = {str(row["message_id"]): row for row in self.repository.frozen_message_rows(ids)}
        records: list[dict[str, Any]] = []
        drop: list[str] = []
        for intent in intents:
            message_id = str(intent["message_id"])
            row = live.get(message_id)
            if (
                row is None
                or row["current_state"] != "present"
                or _input_hash(row) != str(intent["input_hash"])
                or int(row["current_observation_seq"]) != int(intent["observation_seq"])
            ):
                # Canonical drift superseded this intent; drop it so the next capture
                # re-encodes the *current* version under a fresh fence.
                drop.append(str(intent["remote_id"]))
                continue
            records.append(
                {
                    "row": row,
                    "message_id": message_id,
                    "conversation_id": str(row["conversation_id"]),
                    "conversation_digest": str(intent["conversation_digest"]),
                    "namespace": str(intent["namespace"]),
                    "remote_id": str(intent["remote_id"]),
                    "input_hash": str(intent["input_hash"]),
                    "vector_bytes": bytes(intent["vector"]),
                    "kind": str(intent["kind"]),
                    "sent_at": str(intent["sent_at_utc"]),
                    "sender_digest": str(intent["sender_digest"]),
                    "has_link": bool(intent["has_link"]),
                }
            )
        if drop:
            with self._lock, self._storage_reservation(64 * 1024):
                self._db().executemany(
                    "DELETE FROM semantic_intents WHERE remote_id=?",
                    [(value,) for value in drop],
                )
                self._db().commit()
            self._track_sidecar()
        if not records:
            return 0
        try:
            actual = self._call(
                lambda _t: self._backend().get_by_ids([r["remote_id"] for r in records])
            )
        except SightglassError:
            return 0
        return self._accept_readback(records, actual, generation)

    def _readback(self, records: list[dict[str, Any]], generation: int) -> tuple[int, str | None]:
        try:
            actual = self._call(
                lambda _t: self._backend().get_by_ids([r["remote_id"] for r in records])
            )
        except SightglassError:
            return 0, "readback_failed"
        accepted = self._accept_readback(records, actual, generation)
        if not accepted:
            return 0, "readback_mismatch"
        return accepted, None

    def _accept_readback(
        self, records: list[dict[str, Any]], actual: list[dict[str, Any]], generation: int
    ) -> int:
        by_id = {str(row.get("id")): row for row in actual}
        accepted = [
            record
            for record in records
            if (remote := by_id.get(record["remote_id"])) is not None
            and self._matches(record, remote)
        ]
        if not accepted:
            # Partial/missing readback stays pending: intents are retained and the
            # next call resolves them read-only. It does not become "ready".
            return 0
        return self._commit_published(accepted, generation)

    @staticmethod
    def _matches(record: dict[str, Any], remote: dict[str, Any]) -> bool:
        if str(remote.get("namespace")) != record["namespace"]:
            return False
        if not _metadata_matches(record, remote.get("metadata")):
            return False
        values = remote.get("values")
        if not isinstance(values, list) or len(values) != ACTIVE_DIMENSIONS:
            return False
        try:
            return _pack_vector(values) == record["vector_bytes"]
        except (SightglassError, ValueError, TypeError, OverflowError, struct.error):
            return False

    def _commit_published(self, records: list[dict[str, Any]], generation: int) -> int:
        """Publish inside one sidecar tx, re-checking generation and canonical version."""
        epoch = self._epoch_factory()
        with self._storage_reservation(max(64 * 1024, len(records) * 16_384)):
            with self.repository.database.transaction():
                live = {
                    str(value["message_id"]): value
                    for value in self.repository.frozen_message_rows(
                        tuple(record["message_id"] for record in records)
                    )
                }
                with self._lock:
                    connection = self._db()
                    current_generation = int(
                        connection.execute(
                            "SELECT generation FROM semantic_state WHERE id=1"
                        ).fetchone()[0]
                    )
                    if current_generation != generation:
                        # A rebuild started after the readback captured this view.
                        return 0
                    published = 0
                    for record in records:
                        row = live.get(record["message_id"])
                        if (
                            row is None
                            or self.reader.paused
                            or not self.reader.policy.search
                            or not self.reader.policy.permits(str(row["conversation_id"]))
                            or row["current_state"] != "present"
                            or str(row["projection_epoch"]) != epoch
                            or int(row["current_observation_seq"])
                            != int(record["row"]["current_observation_seq"])
                            or _input_hash(row) != record["input_hash"]
                        ):
                            continue
                        connection.execute(
                            "INSERT INTO semantic_entries"
                            "(message_id, namespace, remote_id, unit_kind, observation_seq,"
                            " first_observation_seq, projection_epoch, account_id, conversation_id,"
                            " conversation_digest, sender_digest, kind, sent_at_utc, input_hash,"
                            " vector, generator, published, created_at)"
                            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?)"
                            " ON CONFLICT(message_id) DO UPDATE SET"
                            " namespace=excluded.namespace, remote_id=excluded.remote_id,"
                            " unit_kind=excluded.unit_kind,"
                            " observation_seq=excluded.observation_seq,"
                            " first_observation_seq=excluded.first_observation_seq,"
                            " projection_epoch=excluded.projection_epoch,"
                            " account_id=excluded.account_id,"
                            " conversation_id=excluded.conversation_id,"
                            " conversation_digest=excluded.conversation_digest,"
                            " sender_digest=excluded.sender_digest,"
                            " kind=excluded.kind, sent_at_utc=excluded.sent_at_utc,"
                            " input_hash=excluded.input_hash, vector=excluded.vector,"
                            " generator=excluded.generator, published=1,"
                            " created_at=excluded.created_at",
                            (
                                record["message_id"],
                                record["namespace"],
                                record["remote_id"],
                                "message",
                                int(record["row"]["current_observation_seq"]),
                                int(record["row"]["first_observation_seq"]),
                                str(record["row"]["projection_epoch"]),
                                str(record["row"]["account_id"]),
                                record["conversation_id"],
                                record["conversation_digest"],
                                record["sender_digest"],
                                record["kind"],
                                record["sent_at"],
                                record["input_hash"],
                                record["vector_bytes"],
                                generation,
                                _now(),
                            ),
                        )
                        published += 1
                        connection.execute(
                            "DELETE FROM semantic_intents WHERE remote_id=?", (record["remote_id"],)
                        )
                    if published:
                        connection.execute(
                            "UPDATE semantic_publication SET revision=revision+1,"
                            " updated_at=? WHERE id=1",
                            (_now(),),
                        )
                    count = connection.execute(
                        "SELECT COUNT(*), COALESCE(SUM(LENGTH(vector)),0) FROM semantic_entries"
                        " WHERE published=1 AND generator=?",
                        (generation,),
                    ).fetchone()
                    connection.execute(
                        "UPDATE semantic_state SET indexed_count=?, indexed_bytes=?, updated_at=?"
                        " WHERE id=1",
                        (int(count[0]), int(count[1]), _now()),
                    )
                    connection.commit()

        self._track_sidecar()
        return published

    def _set_state(self, state: str, *, error: str | None) -> None:
        with self._storage_reservation(32 * 1024):
            connection = self._db()
            connection.execute(
                "UPDATE semantic_state SET state=?, last_error=?, updated_at=? WHERE id=1",
                (state, error, _now()),
            )
            connection.commit()
        self._track_sidecar()

    # -- rebuild -----------------------------------------------------------
    def request_rebuild(self) -> None:
        """Start a new generation/namespace; never issues a remote delete."""
        if not self.enabled:
            raise SightglassError(ErrorCode.SERVICE_UNAVAILABLE, details={"reason": "disabled"})
        with self._lock, self._storage_reservation(128 * 1024):
            connection = self._db()
            connection.execute(
                "UPDATE semantic_state SET generation=generation+1, state='rebuilding',"
                " indexed_count=0, indexed_bytes=0, last_error=NULL, updated_at=? WHERE id=1",
                (_now(),),
            )
            connection.execute("DELETE FROM semantic_entries")
            connection.execute("DELETE FROM semantic_intents")
            connection.execute("DELETE FROM semantic_capture_checkpoint")
            connection.execute("DELETE FROM semantic_coverage")
            connection.execute("UPDATE semantic_scan_cursor SET last_conversation_id='' WHERE id=1")
            connection.execute(
                "UPDATE semantic_identity SET projection_epoch=? WHERE id=1",
                (self._epoch_factory(),),
            )
            connection.execute(
                "UPDATE semantic_publication SET generation=("
                "SELECT generation FROM semantic_state WHERE id=1),"
                " revision=revision+1, updated_at=? WHERE id=1",
                (_now(),),
            )
            connection.commit()
        self._track_sidecar()

    def _generation(self) -> int:
        with self._lock:
            return int(self._state_row()["generation"])

    # -- query -------------------------------------------------------------
    def query(
        self,
        concept: str,
        *,
        account_id: str,
        conversation_ids: tuple[str, ...],
        participant_ids: tuple[str, ...] = (),
        after: str | None = None,
        before: str | None = None,
        watermark: int,
        epoch: str,
        kinds: tuple[str, ...] = (),
    ) -> SemanticCandidates:
        if (
            not isinstance(concept, str)
            or not concept
            or len(concept) > MAX_CONCEPT_CHARS
            or len(kinds) > MAX_KINDS
        ):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if not self.enabled:
            return self._degraded("disabled", None)
        scoped = self._query_scope(account_id, conversation_ids)
        if not scoped:
            return self._degraded("out_of_config", None)
        generation = self._generation()
        if not self._epoch_current():
            return self._degraded("degraded", generation, error="projection_refresh_pending")
        if not self._scope_indexed(scoped, account_id, epoch, generation):
            return self._degraded("not_ready", generation)
        filters = self._remote_filters(
            account_id=account_id,
            conversation_ids=scoped,
            participant_ids=participant_ids,
            after=after,
            before=before,
            watermark=watermark,
            kinds=kinds,
        )
        matches: list[dict[str, Any]] = []
        try:
            # The whole query (encode + every pre-filter request) shares one sub-budget
            # so an operator reload/cancel is honoured between requests.
            with self._query_budget():
                vector = self._call(lambda _t: self._backend().encode((concept,)))[0]
                namespace = _namespace(account_id, epoch, generation)
                for filter_ in filters:
                    matches.extend(
                        self._call(
                            lambda _t, _f=filter_: self._backend().query(
                                vector, namespace, _f, _TOP_K
                            )
                        )
                    )
        except (SightglassError, IndexError) as exc:
            code = (
                exc.details["reason"]
                if isinstance(exc, CloudflareError)
                else exc.code.value
                if isinstance(exc, SightglassError)
                else "encode_unavailable"
            )
            return self._degraded("degraded", generation, error=code)
        ordered = self._merge_matches(
            matches,
            account_id=account_id,
            conversations=scoped,
            participant_ids=participant_ids,
            after=after,
            before=before,
            watermark=watermark,
            epoch=epoch,
            kinds=kinds,
            generation=generation,
        )
        receipt = self._receipt("ready", generation, candidates=len(ordered))
        if conversation_ids and set(scoped) != set(conversation_ids):
            receipt["coverage"] = "partial"
        return SemanticCandidates(message_ids=tuple(ordered), receipt=receipt)

    @contextmanager
    def _query_budget(self) -> Iterator[None]:
        from sightglass.operations import operation_budget

        remaining = operation_remaining_seconds()
        budget = QUERY_BUDGET_SECONDS if remaining is None else min(QUERY_BUDGET_SECONDS, remaining)
        with operation_budget(budget):
            yield

    def _query_scope(self, account_id: str, conversation_ids: tuple[str, ...]) -> tuple[str, ...]:
        if account_id != self.settings.source_account_id:
            return ()
        if self.reader.paused or not self.reader.policy.search:
            return ()
        capable = set(self._capable_conversations())
        requested = conversation_ids or self._configured_conversations()
        return tuple(sorted(value for value in set(requested) if value in capable))

    def _scope_indexed(
        self, conversations: tuple[str, ...], account_id: str, epoch: str, generation: int
    ) -> bool:
        if not conversations:
            return False
        namespace = _namespace(account_id, epoch, generation)
        with self._lock:
            row = (
                self._db()
                .execute(
                    "SELECT 1 FROM semantic_entries WHERE published=1 AND generator=?"
                    " AND namespace=? AND conversation_id IN"
                    f" ({','.join('?' for _ in conversations)}) LIMIT 1",
                    (generation, namespace, *conversations),
                )
                .fetchone()
            )
        return row is not None

    @staticmethod
    def _remote_filters(
        *,
        account_id: str,
        conversation_ids: tuple[str, ...],
        participant_ids: tuple[str, ...],
        after: str | None,
        before: str | None,
        watermark: int,
        kinds: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        """Vectorize metadata pre-filters applied before topK; canonical refence after.

        Cloudflare Vectorize filter keys cannot start with ``$`` and it has no logical
        ``and``/``or`` operators: several fields combine with an implicit AND. A
        "link + concrete kind" scope therefore needs at most two independent
        pre-filter queries whose scored matches are merged locally. ``message`` means
        any kind; ``link`` means materialized link evidence.
        """
        base: dict[str, Any] = {}
        if len(conversation_ids) == 1:
            base["conversation"] = _conversation_digest(account_id, conversation_ids[0])
        elif conversation_ids:
            base["conversation"] = {
                "$in": [_conversation_digest(account_id, value) for value in conversation_ids]
            }
        if participant_ids:
            base["sender"] = {
                "$in": [_sender_digest(account_id, value) for value in participant_ids]
            }
        if after is not None and (low := _sent_at_number(after)) is not None:
            base["sent_at"] = {"$gte": low}
        if before is not None and (high := _sent_at_number(before)) is not None:
            base.setdefault("sent_at", {})["$lt"] = high
        if watermark:
            base["watermark"] = {"$lte": int(watermark)}

        kind_values = {value for value in kinds if value}
        if not kind_values or "message" in kind_values:
            return [dict(base)]
        wants_link = "link" in kind_values
        concrete = sorted(value for value in kind_values if value not in {"link", "message"})
        if wants_link and not concrete:
            return [{**base, "has_link": True}]
        if concrete and not wants_link:
            return [{**base, "kind": {"$in": concrete}}]
        # Mixed "link + concrete kinds": two independent pre-filters, merged by score.
        return [
            {**base, "kind": {"$in": concrete}},
            {**base, "has_link": True},
        ]

    def _merge_matches(
        self,
        matches: list[dict[str, Any]],
        *,
        account_id: str,
        conversations: tuple[str, ...],
        participant_ids: tuple[str, ...],
        after: str | None,
        before: str | None,
        watermark: int,
        epoch: str,
        kinds: tuple[str, ...],
        generation: int,
    ) -> list[str]:
        remote_ids = [str(row.get("id")) for row in matches if row.get("id")]
        if not remote_ids:
            return []
        manifest = self._manifest(remote_ids, generation)
        canonical_ids = tuple(
            dict.fromkeys(str(entry["message_id"]) for entry in manifest.values())
        )
        rows = {
            str(row["message_id"]): row
            for row in self.repository.frozen_message_rows(canonical_ids)
        }
        allowed_conversations = set(conversations)
        kind_filter = {value for value in kinds if value}
        # Re-check current reader policy/pause at admission, not only at query start:
        # a policy or pause change during remote work must fence the captured view.
        if self.reader.paused:
            return []
        candidate_ids = tuple(
            dict.fromkeys(str(entry["message_id"]) for entry in manifest.values())
        )
        linked = self._linked_message_ids(candidate_ids)
        scored: list[tuple[float, int, str]] = []
        rank = 0
        for raw in matches:
            remote_id = str(raw.get("id"))
            entry = manifest.get(remote_id)
            if entry is None:
                continue
            if "namespace" in raw and raw["namespace"] != entry["namespace"]:
                continue
            rank += 1
            message_id = str(entry["message_id"])
            row = rows.get(message_id)
            if row is None or not self.reader.policy.permits(str(row["conversation_id"])):
                continue
            if "metadata" in raw and not _metadata_matches(
                {
                    "row": row,
                    "sent_at": entry["sent_at_utc"],
                    "sender_digest": entry["sender_digest"],
                    "kind": entry["kind"],
                    "has_link": message_id in linked,
                    "conversation_digest": entry["conversation_digest"],
                },
                raw["metadata"],
            ):
                continue
            if not self._canonical_admits(
                row,
                entry,
                account_id=account_id,
                conversations=allowed_conversations,
                participant_ids=participant_ids,
                after=after,
                before=before,
                watermark=watermark,
                epoch=epoch,
                kinds=kind_filter,
                has_materialized_link=message_id in linked,
            ):
                continue
            score = raw.get("score")
            weight = float(score) if isinstance(score, (int, float)) else 0.0
            scored.append((weight, rank, message_id))
        # Merge across conversations by similarity, not conversation order.
        ordered: list[str] = []
        seen: set[str] = set()
        for _weight, _rank, message_id in sorted(scored, key=lambda item: (-item[0], item[1])):
            if message_id in seen:
                continue
            seen.add(message_id)
            ordered.append(message_id)
        return ordered

    def _manifest(self, remote_ids: list[str], generation: int) -> dict[str, sqlite3.Row]:
        with self._lock:
            rows = (
                self._db()
                .execute(
                    "SELECT * FROM semantic_entries WHERE published=1 AND generator=?"
                    f" AND remote_id IN ({','.join('?' for _ in remote_ids)})",
                    (generation, *remote_ids),
                )
                .fetchall()
            )
        return {str(row["remote_id"]): row for row in rows}

    @staticmethod
    def _canonical_admits(
        row: Any,
        entry: sqlite3.Row,
        *,
        account_id: str,
        conversations: set[str],
        participant_ids: tuple[str, ...],
        after: str | None,
        before: str | None,
        watermark: int,
        epoch: str,
        kinds: set[str],
        has_materialized_link: bool,
    ) -> bool:
        if row is None:
            return False
        if row["current_state"] != "present":
            return False
        if not _encoder_input(row):
            return False
        if str(row["account_id"]) != account_id:
            return False
        if str(row["conversation_id"]) not in conversations:
            return False
        if str(entry["conversation_digest"]) != _conversation_digest(
            account_id, str(row["conversation_id"])
        ):
            return False
        if str(row["projection_epoch"]) != epoch:
            return False
        if row["first_observation_seq"] is None or row["current_observation_seq"] is None:
            return False
        if (
            int(row["first_observation_seq"]) > watermark
            or int(row["current_observation_seq"]) > watermark
        ):
            return False
        if int(entry["observation_seq"]) != int(row["current_observation_seq"]):
            return False
        if str(entry["input_hash"]) != _input_hash(row):
            return False
        if str(entry["sender_digest"]) != _sender_digest(
            account_id, str(row["sender_id"]) if row["sender_id"] else None
        ):
            return False
        if kinds and not _kinds_admit(str(row["kind"]), kinds, has_materialized_link):
            return False
        if participant_ids and (
            row["sender_id"] is None or str(row["sender_id"]) not in participant_ids
        ):
            return False
        if after is not None and str(row["sent_at_utc"]) < after:
            return False
        if before is not None and str(row["sent_at_utc"]) >= before:
            return False
        return True

    def _receipt(
        self,
        state: str,
        generation: int | None,
        *,
        candidates: int = 0,
        error: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema": "sightglass.semantic-receipt.v1",
            "lane": "semantic",
            "state": state,
            "coverage": (
                self._coverage_state(self._capable_conversations(), self._generation())
                if self.enabled
                else "disabled"
            ),
            "model": ACTIVE_MODEL,
            "recipe": SEMANTIC_RECIPE,
            "dimensions": ACTIVE_DIMENSIONS,
            "metric": ACTIVE_METRIC,
            "generation": generation,
            "candidates": candidates,
            "live_validation": False,
            "external_egress_authorized": bool(self.settings.external_data_authorized),
            "state_token": self.state_token() if self.enabled else "0:0:0",
        }
        if error is not None:
            payload["error"] = error
        return payload

    def _degraded(
        self, state: str, generation: int | None, *, error: str | None = None
    ) -> SemanticCandidates:
        return SemanticCandidates(
            message_ids=(),
            receipt=self._receipt(state, generation, error=error),
        )


class _DriftError(Exception):
    """Internal: a captured row changed before publication (caught by index_once)."""

    def __init__(self, message_id: str) -> None:
        super().__init__(message_id)
        self.message_id = message_id


def _kinds_admit(kind: str, kinds: set[str], has_materialized_link: bool) -> bool:
    """Apply reader-facing kind constraints with the canonical kind vocabulary.

    ``message`` is any-kind and ``link`` requires materialized link evidence; a
    concrete resource kind matches its own canonical kind. A message can satisfy a
    request if *any* requested scope matches it.
    """
    if "message" in kinds:
        return True
    if kind in kinds:
        return True
    if "link" in kinds and has_materialized_link:
        return True
    return False


def _remote_metadata(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "sent_at": _sent_at_number(record["sent_at"]) or 0.0,
        "sender": record["sender_digest"],
        "kind": record["kind"],
        "watermark": int(record["observation_seq"])
        if "observation_seq" in record
        else int(record["row"]["current_observation_seq"]),
        "has_link": bool(record["has_link"]),
        "conversation": record["conversation_digest"],
    }


def _metadata_matches(record: dict[str, Any], metadata: Any) -> bool:
    if not isinstance(metadata, dict):
        return False
    expected = _remote_metadata(record)
    return (
        _close(metadata.get("sent_at"), expected["sent_at"])
        and metadata.get("sender") == expected["sender"]
        and metadata.get("kind") == expected["kind"]
        and metadata.get("watermark") == expected["watermark"]
        and metadata.get("has_link") is expected["has_link"]
        and metadata.get("conversation") == expected["conversation"]
    )


def _close(value: Any, expected: float) -> bool:
    if not isinstance(value, (int, float)):
        return False
    return abs(float(value) - float(expected)) <= 1e-3


def _source_sort_key(row: Any) -> Any:
    from sightglass.contracts.common import SourceSortKey

    return SourceSortKey(
        str(row["sort_primary"]),
        int(row["sort_seq"]),
        int(row["sort_tie"]),
        str(row["source_message_id"]),
    )


def _now() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat(timespec="microseconds")

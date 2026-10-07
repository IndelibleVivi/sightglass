from __future__ import annotations

import os
import sqlite3
import stat
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any

from sightglass.operations import (
    check_operation_budget,
    operation_expired,
    operation_remaining_seconds,
)
from sightglass.storage import SQL_WRITE_RESERVE, StorageBudget, StorageLease

from .backups import recover_interrupted_restore
from .schema import SCHEMA_SQL, SCHEMA_VERSION

STORAGE_EXPLAIN_DEFAULT_SAMPLE_SIZE = 128


class WindowDB:
    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        storage: StorageBudget | None = None,
        write_guard: Callable[[], None] | None = None,
    ) -> None:
        self.path = Path(path).expanduser().resolve()
        parent = self.path.parent
        if not parent.exists():
            parent.mkdir(parents=True, mode=0o700)
            os.chmod(parent, 0o700)
        else:
            mode = stat.S_IMODE(parent.stat().st_mode)
            if mode & 0o077:
                raise RuntimeError("window.db parent directory must already be private (mode 0700)")
        self._active_connection: ContextVar[sqlite3.Connection | None] = ContextVar(
            f"sightglass_window_connection_{id(self)}", default=None
        )
        self._commit_wakes: ContextVar[list[Callable[[], None]] | None] = ContextVar(
            f"sightglass_commit_wakes_{id(self)}", default=None
        )
        self._writer_lock = threading.RLock()
        self._writer_state_lock = threading.Lock()
        self._writer_waiters = 0
        self._writer_active = False
        self._writer_wait_count = 0
        self._writer_wait_total_ms = 0
        self._writer_wait_max_ms = 0
        self._writer_wait_samples: deque[int] = deque(maxlen=256)
        self.storage = storage
        self.write_guard = write_guard
        if write_guard is not None:
            write_guard()
        self._storage_lease: ContextVar[StorageLease | None] = ContextVar(
            f"sightglass_storage_lease_{id(self)}", default=None
        )
        self.migration_backup_path: Path | None = None
        recover_interrupted_restore(self.path)
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.set_progress_handler(lambda: int(operation_expired()), 1_000)
        return connection

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        active = self._active_connection.get()
        if active is not None:
            yield active
            return
        connection = self.connect()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def read_snapshot(self) -> Iterator[sqlite3.Connection]:
        """Share one SQLite read view across pure query/projection calls."""
        active = self._active_connection.get()
        if active is not None:
            yield active
            return
        with self.connection() as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            token = self._active_connection.set(connection)
            try:
                yield connection
            finally:
                self._active_connection.reset(token)
                connection.rollback()

    @contextmanager
    def transaction(self, *, maintenance: bool = False) -> Iterator[sqlite3.Connection]:
        active = self._active_connection.get()
        if active is not None:
            yield active
            return
        with self._writer_state_lock:
            self._writer_waiters += 1
        wait_started = time.monotonic()
        try:
            remaining = operation_remaining_seconds()
            if remaining is None:
                self._writer_lock.acquire()
            else:
                while not self._writer_lock.acquire(timeout=min(0.05, remaining)):
                    check_operation_budget()
                    remaining = operation_remaining_seconds()
                    assert remaining is not None
        finally:
            waited_ms = max(0, round((time.monotonic() - wait_started) * 1_000))
            with self._writer_state_lock:
                self._writer_waiters -= 1
                self._writer_wait_count += 1
                self._writer_wait_total_ms += waited_ms
                self._writer_wait_max_ms = max(self._writer_wait_max_ms, waited_ms)
                self._writer_wait_samples.append(waited_ms)
        with self._writer_state_lock:
            self._writer_active = True
        wakes: list[Callable[[], None]] = []
        wake_token = self._commit_wakes.set(wakes)
        committed = False
        try:
            with ExitStack() as stack:
                lease = (
                    stack.enter_context(
                        self.storage.reserve(
                            SQL_WRITE_RESERVE,
                            maintenance=maintenance,
                        )
                    )
                    if self.storage is not None
                    else None
                )
                lease_token = self._storage_lease.set(lease)
                stack.callback(self._storage_lease.reset, lease_token)
                connection = stack.enter_context(self.connection())
                token = self._active_connection.set(connection)
                try:
                    if self.write_guard is not None:
                        self.write_guard()
                    connection.execute("BEGIN IMMEDIATE")
                    try:
                        yield connection
                        if self.write_guard is not None:
                            self.write_guard()
                        if lease is not None:
                            lease.verify()
                    except Exception as exc:
                        if operation_expired():
                            connection.set_progress_handler(None, 0)
                        connection.rollback()
                        if isinstance(exc, sqlite3.DatabaseError) and operation_expired():
                            check_operation_budget()
                        raise
                    else:
                        connection.commit()
                        committed = True
                finally:
                    self._active_connection.reset(token)
        finally:
            self._commit_wakes.reset(wake_token)
            with self._writer_state_lock:
                self._writer_active = False
            self._writer_lock.release()
        if committed:
            for wake in wakes:
                self._best_effort_wake(wake)

    def wake_after_commit(self, wake: Callable[[], None]) -> None:
        """Signal a worker after the owning writer commits and releases its lock.

        This is only a best-effort Event notification. Durable queue state and the
        worker's bounded fallback remain authoritative if a notification fails.
        Nested writers share the outer list; rollback discards their notifications.
        A caller outside a writer can signal immediately, including in a read view.
        """
        wakes = self._commit_wakes.get()
        if wakes is None:
            self._best_effort_wake(wake)
        elif not any(existing is wake for existing in wakes):
            wakes.append(wake)

    @staticmethod
    def _best_effort_wake(wake: Callable[[], None]) -> None:
        try:
            wake()
        except Exception:
            # A failed hint must not turn an already committed transaction into
            # an apparent rollback. Workers re-read durable work on fallback.
            pass

    def reserve_growth(self, amount: int) -> None:
        """Extend this admission's estimate before writing a prepared message batch."""
        lease = self._storage_lease.get()
        if lease is not None:
            lease.extend(amount)
        elif self.storage is not None:
            self.storage.require(amount)

    @property
    def maintenance_write_active(self) -> bool:
        lease = self._storage_lease.get()
        return lease is not None and lease.maintenance

    def storage_status(self, *, detailed: bool = False) -> dict:
        if self.storage is None:
            return {"schema": "sightglass.storage-status.v1", "state": "unmanaged"}
        result = self.storage.status(reconcile=detailed)
        if detailed:
            with self.connection() as connection:
                count = int(connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0])
                page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
                free_pages = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
            result["message_count"] = count
            result["database_bytes_per_message"] = (
                round(result["components"]["database"] / count, 2) if count else None
            )
            result["database_free_page_bytes"] = page_size * free_pages
        return result

    def storage_explain(
        self,
        *,
        offset: int = 0,
        limit: int = 500,
        sample_size: int = STORAGE_EXPLAIN_DEFAULT_SAMPLE_SIZE,
        deep: bool = False,
        phase: str = "all",
        after_object: str | None = None,
        deadline_seconds: float = 10.0,
    ) -> dict[str, Any]:
        from .storage_diagnostics import explain

        return explain(
            self, offset=offset, limit=limit, sample_size=sample_size,
            deep=deep, phase=phase, after_object=after_object,
            deadline_seconds=deadline_seconds,
        )

    @staticmethod
    def _storage_categories(
        objects: list[dict[str, Any]],
        *,
        available: bool,
    ) -> dict[str, Any]:
        totals = {
            name: 0
            for name in (
                "canonical",
                "observations",
                "links",
                "lexical",
                "semantic",
                "deliveries",
                "other",
            )
        }
        for item in objects:
            table = str(item["owner_table"])
            if table == "message_observations":
                category = "observations"
            elif table.startswith("message_link"):
                category = "links"
            elif table.startswith("message_lexical"):
                category = "lexical"
            elif table.startswith("reader_deliver"):
                category = "deliveries"
            elif table in {
                "messages",
                "accounts",
                "conversations",
                "participants",
                "conversation_members",
                "resources",
            }:
                category = "canonical"
            else:
                category = "other"
            totals[category] += int(item["physical_bytes"] or 0)
        return {
            "physical_bytes": totals if available else {name: None for name in totals},
            "semantic_state": "separate_sidecar",
            "external_bytes": (
                "see status.components for semantic sidecar, CAS, "
                "delivery spool and migration backups"
            ),
        }

    def reader_profile(self, reader_id: str) -> sqlite3.Row | None:
        with self.connection() as connection:
            return connection.execute(
                "SELECT * FROM reader_profiles WHERE reader_id = ?",
                (reader_id,),
            ).fetchone()

    def writer_status(self) -> dict[str, Any]:
        with self._writer_state_lock:
            samples = tuple(sorted(self._writer_wait_samples))
            p50 = samples[max(0, (len(samples) + 1) // 2 - 1)] if samples else None
            p95 = samples[max(0, (len(samples) * 95 + 99) // 100 - 1)] if samples else None
            return {
                "schema": "sightglass.window-writer-status.v1",
                "active": self._writer_active,
                "waiting_count": self._writer_waiters,
                "wait_count": self._writer_wait_count,
                "wait_total_ms": self._writer_wait_total_ms,
                "wait_p50_ms": p50,
                "wait_p95_ms": p95,
                "wait_max_ms": self._writer_wait_max_ms,
            }

    def _initialize(self) -> None:
        with self.connection() as connection:
            current = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if current < 0 or current > SCHEMA_VERSION:
                raise RuntimeError(f"unsupported window.db schema version: {current}")
            if current == 0:
                user_tables = int(
                    connection.execute(
                        """
                        SELECT count(*) FROM sqlite_schema
                        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                        """
                    ).fetchone()[0]
                )
                if user_tables:
                    raise RuntimeError("unsupported unversioned window.db with existing tables")
                if self.storage is not None:
                    self.storage.require(SQL_WRITE_RESERVE)
                connection.execute("PRAGMA journal_mode = WAL")
                try:
                    connection.executescript(
                        f"BEGIN IMMEDIATE;\n{SCHEMA_SQL}\n"
                        f"PRAGMA user_version = {SCHEMA_VERSION};\nCOMMIT;"
                    )
                except Exception:
                    connection.rollback()
                    raise
            elif current < SCHEMA_VERSION:
                raise RuntimeError(
                    "legacy window.db requires stopped-only storage compact build; "
                    "normal startup never converts the observation/FTS backend"
                )
            else:
                connection.execute("PRAGMA journal_mode = WAL")
        os.chmod(self.path, 0o600)

    @property
    def schema_version(self) -> int:
        with self.connection() as connection:
            return int(connection.execute("PRAGMA user_version").fetchone()[0])

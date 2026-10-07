"""Transactional ``window.db`` schema migrations with private pre-migration snapshots."""

from __future__ import annotations

import os
import sqlite3
from contextlib import nullcontext
from pathlib import Path
from typing import TYPE_CHECKING

from sightglass.storage import StorageBudget

from .backups import find_verified_migration_snapshot
from .schema import (
    LINK_SCHEMA_STATEMENTS,
    READING_COVERAGE_COLUMNS,
    READING_SCHEMA_STATEMENTS,
    RESIDENCY_SCHEMA_STATEMENTS,
    RESOURCE_DISCOVERY_SCHEMA_STATEMENTS,
    RESOURCE_INDEX_STATEMENTS,
    RESOURCE_JOB_SCHEMA_STATEMENTS,
    SCHEMA_VERSION,
    VOICE_SCHEMA_STATEMENTS,
)

if TYPE_CHECKING:
    from .db import WindowDB


# Headroom beyond the snapshot itself: the migrated schema's own growth and the WAL
# frames the DDL transaction writes while the snapshot is still adjacent on disk.
MIGRATION_SPACE_RESERVE_BYTES = 32 * 1024 * 1024


MIGRATION_STEPS: dict[int, tuple[str, ...]] = {
    1: (
        "ALTER TABLE accounts ADD COLUMN account_binding_id TEXT",
        "ALTER TABLE accounts ADD COLUMN source_inventory_epoch TEXT",
        """
        CREATE UNIQUE INDEX accounts_binding_id_unique
            ON accounts(account_binding_id) WHERE account_binding_id IS NOT NULL
        """,
        "ALTER TABLE conversations ADD COLUMN catalog_state TEXT NOT NULL DEFAULT 'unknown'",
        "ALTER TABLE conversations ADD COLUMN unread_count INTEGER",
        "ALTER TABLE conversations ADD COLUMN catalog_observed_at TEXT",
        """
        CREATE INDEX conversation_catalog
            ON conversations(account_id, catalog_state, last_message_at, conversation_id)
        """,
        "ALTER TABLE messages ADD COLUMN search_text TEXT",
        "ALTER TABLE reader_profiles ADD COLUMN policy_revision INTEGER NOT NULL DEFAULT 1",
        """
        CREATE TABLE source_catalog_state (
            account_id TEXT PRIMARY KEY REFERENCES accounts(account_id),
            source_inventory_epoch TEXT,
            coverage_state TEXT NOT NULL DEFAULT 'unknown',
            next_cursor_token TEXT,
            scan_started_at TEXT,
            scan_completed_at TEXT,
            last_observed_at TEXT,
            last_error_code TEXT,
            updated_at TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE source_conversation_state (
            conversation_id TEXT PRIMARY KEY REFERENCES conversations(conversation_id),
            source_inventory_epoch TEXT,
            tail_cursor_token TEXT,
            tail_generation_id TEXT,
            tail_sort_primary TEXT,
            tail_sort_seq INTEGER,
            tail_sort_tie INTEGER,
            tail_source_message_id TEXT,
            tail_observed_at TEXT,
            indexed_before TEXT,
            indexed_after TEXT,
            backfill_state TEXT NOT NULL DEFAULT 'not_started',
            last_error_code TEXT,
            updated_at TEXT NOT NULL
        )
        """,
        """
        CREATE INDEX source_conversation_backfill_state
            ON source_conversation_state(backfill_state, updated_at)
        """,
        """
        CREATE TABLE source_shard_state (
            account_id TEXT NOT NULL REFERENCES accounts(account_id),
            source_shard_key TEXT NOT NULL,
            source_inventory_epoch TEXT,
            source_generation_id TEXT,
            availability_state TEXT NOT NULL DEFAULT 'unknown',
            cursor_token TEXT,
            last_sort_primary TEXT,
            last_sort_seq INTEGER,
            last_sort_tie INTEGER,
            last_source_message_id TEXT,
            discovered_at TEXT NOT NULL,
            last_verified_at TEXT,
            last_error_code TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(account_id, source_shard_key)
        )
        """,
        """
        CREATE INDEX source_shard_availability
            ON source_shard_state(account_id, availability_state, updated_at)
        """,
        """
        CREATE TABLE source_backfill_jobs (
            job_id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL REFERENCES accounts(account_id),
            conversation_id TEXT REFERENCES conversations(conversation_id),
            source_inventory_epoch TEXT,
            requested_after TEXT,
            requested_before TEXT,
            max_messages INTEGER NOT NULL,
            processed_messages INTEGER NOT NULL DEFAULT 0,
            cursor_token TEXT,
            state TEXT NOT NULL DEFAULT 'queued',
            created_at TEXT NOT NULL,
            started_at TEXT,
            updated_at TEXT NOT NULL,
            completed_at TEXT,
            last_error_code TEXT
        )
        """,
        """
        CREATE INDEX source_backfill_jobs_schedule
            ON source_backfill_jobs(state, updated_at, created_at)
        """,
        """
        CREATE TABLE resource_derivations (
            derivation_id TEXT PRIMARY KEY,
            resource_id TEXT NOT NULL REFERENCES resources(resource_id),
            source_digest TEXT NOT NULL,
            variant TEXT NOT NULL,
            processor_name TEXT NOT NULL,
            processor_version TEXT NOT NULL,
            parameters_json TEXT NOT NULL,
            derived_digest TEXT NOT NULL REFERENCES resource_objects(object_digest),
            created_at TEXT NOT NULL
        )
        """,
        """
        CREATE UNIQUE INDEX resource_derivations_provenance_unique
            ON resource_derivations(
                resource_id,
                source_digest,
                variant,
                processor_name,
                processor_version,
                parameters_json,
                derived_digest
            )
        """,
        """
        CREATE INDEX resource_derivations_derived_object
            ON resource_derivations(derived_digest)
        """,
    ),
    2: VOICE_SCHEMA_STATEMENTS,
    3: RESOURCE_INDEX_STATEMENTS,
    4: (
        "ALTER TABLE messages ADD COLUMN projection_epoch TEXT",
        "ALTER TABLE messages ADD COLUMN first_observation_seq INTEGER",
        "ALTER TABLE messages ADD COLUMN current_observation_seq INTEGER",
        """
        CREATE INDEX message_materialized_timeline
            ON messages(
                conversation_id, projection_epoch, current_state,
                sort_primary, sort_seq, sort_tie, source_message_id
            )
        """,
    ),
    5: RESOURCE_JOB_SCHEMA_STATEMENTS,
    6: RESOURCE_DISCOVERY_SCHEMA_STATEMENTS,
    7: LINK_SCHEMA_STATEMENTS,
    8: tuple(
        f"ALTER TABLE source_conversation_state ADD COLUMN {column}"
        for column in READING_COVERAGE_COLUMNS
    )
    + READING_SCHEMA_STATEMENTS,
    9: (
        "ALTER TABLE messages ADD COLUMN body_available INTEGER NOT NULL DEFAULT 1",
    )
    + RESIDENCY_SCHEMA_STATEMENTS,
}


class MigrationDiskSpaceError(RuntimeError):
    """The pre-migration snapshot cannot be written; no migration was started."""


def _available_bytes(directory: Path) -> int:
    stats = os.statvfs(directory)
    return int(stats.f_bavail) * int(stats.f_frsize or stats.f_bsize)


def _backup_footprint(database_path: Path) -> int:
    """Bytes the pre-migration snapshot needs: the main file plus its committed WAL."""

    total = database_path.stat().st_size
    for suffix in ("-wal", "-journal"):
        sibling = database_path.with_name(database_path.name + suffix)
        try:
            total += sibling.stat().st_size
        except OSError:
            continue
    return total


def require_migration_space(database_path: Path) -> None:
    """Fail before the migration transaction opens when the snapshot cannot fit.

    A migration that starts and then runs out of space leaves the operator with a
    half-written snapshot and a database that still needs migrating, so the space
    requirement is checked up front instead of being discovered by a failed copy.
    """

    required = _backup_footprint(database_path) + MIGRATION_SPACE_RESERVE_BYTES
    available = _available_bytes(database_path.parent)
    if available >= required:
        return
    raise MigrationDiskSpaceError(
        "window.db migration needs "
        f"{required / 1048576:.0f} MiB free next to {database_path} for the pre-migration "
        f"snapshot, but only {available / 1048576:.0f} MiB is available; no migration was "
        "started. Free space on that volume (or move the database to a larger one) and retry."
    )


def _reserve_backup_path(database_path: Path, source_version: int) -> Path:
    stem = f"{database_path.name}.v{source_version}.backup"
    attempt = 0
    while True:
        name = stem if attempt == 0 else f"{stem}.{attempt}"
        candidate = database_path.with_name(name)
        try:
            descriptor = os.open(candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            attempt += 1
            continue
        os.close(descriptor)
        return candidate


def _discard_partial_backup(backup_path: Path) -> None:
    """Remove a snapshot that was not completed, plus any journal/WAL it left behind."""

    for candidate in (
        backup_path,
        backup_path.with_name(backup_path.name + "-journal"),
        backup_path.with_name(backup_path.name + "-wal"),
    ):
        candidate.unlink(missing_ok=True)


def _snapshot_database(
    database_path: Path,
    source_version: int,
) -> Path:
    backup_path = _reserve_backup_path(database_path, source_version)
    try:
        source = sqlite3.connect(f"{database_path.as_uri()}?mode=ro", uri=True)
        try:
            destination = sqlite3.connect(backup_path)
            try:
                source.backup(destination)
                destination.execute("PRAGMA journal_mode = DELETE")
                destination.commit()
            finally:
                destination.close()
        finally:
            source.close()
        os.chmod(backup_path, 0o600)
        descriptor = os.open(backup_path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except Exception:
        _discard_partial_backup(backup_path)
        raise
    return backup_path


def migrate_schema(
    connection: sqlite3.Connection,
    database_path: Path,
    current_version: int,
    *,
    storage: StorageBudget | None = None,
) -> Path | None:
    """Upgrade one existing database to ``SCHEMA_VERSION`` in one DDL transaction."""

    if current_version == SCHEMA_VERSION:
        return None
    if current_version <= 0 or current_version > SCHEMA_VERSION:
        raise RuntimeError(f"unsupported window.db schema version: {current_version}")
    if connection.in_transaction:
        raise RuntimeError("window.db migration requires an idle SQLite connection")

    migration_versions = tuple(range(current_version, SCHEMA_VERSION))
    missing = [version for version in migration_versions if version not in MIGRATION_STEPS]
    if missing:
        raise RuntimeError(f"missing window.db migration from schema version: {missing[0]}")

    # A verified adjacent compressed snapshot is an operator-created recovery point for
    # these exact pre-migration bytes.  It avoids making a second raw copy of a large
    # database; without it the longstanding full SQLite snapshot path remains mandatory.
    compressed_backup = find_verified_migration_snapshot(database_path, current_version)
    if compressed_backup is None:
        # Checked before the transaction opens: a snapshot that cannot fit must fail
        # here, with the database untouched and still at its old version.
        require_migration_space(database_path)
    elif _available_bytes(database_path.parent) < MIGRATION_SPACE_RESERVE_BYTES:
        raise MigrationDiskSpaceError(
            "window.db migration needs at least "
            f"{MIGRATION_SPACE_RESERVE_BYTES / 1048576:.0f} MiB free for schema growth, "
            "but the adjacent volume does not have it; no migration was started."
        )

    reservation = (
        storage.reserve(_backup_footprint(database_path) + MIGRATION_SPACE_RESERVE_BYTES)
        if storage is not None and compressed_backup is None
        else nullcontext()
    )
    with reservation as lease:
        try:
            connection.execute("BEGIN IMMEDIATE")
            # A second read connection can take an online backup while this connection holds
            # the writer reservation. Both retain the same committed pre-migration boundary.
            backup_path = compressed_backup or _snapshot_database(database_path, current_version)
            for version in migration_versions:
                for statement in MIGRATION_STEPS[version]:
                    connection.execute(statement)
                connection.execute(f"PRAGMA user_version = {version + 1}")
            if lease is not None:
                lease.verify()
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    return backup_path


def ensure_current_schema(path: str | os.PathLike[str]) -> WindowDB:
    """Open a database, creating or migrating it through the canonical ``WindowDB`` path."""

    from .db import WindowDB

    return WindowDB(path)

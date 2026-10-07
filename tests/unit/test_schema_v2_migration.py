from __future__ import annotations

import os
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model import migrations
from sightglass.model.schema import SCHEMA_VERSION
from sightglass.operations import operation_budget
from tests.fixtures.legacy_window import migrate_fixture as WindowDB

V2_NEW_TABLES = {
    "source_catalog_state",
    "source_conversation_state",
    "source_shard_state",
    "source_backfill_jobs",
    "resource_derivations",
}


def _create_v1_fixture(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        PRAGMA journal_mode = WAL;
        PRAGMA wal_autocheckpoint = 0;

        CREATE TABLE accounts (
            account_id TEXT PRIMARY KEY,
            source_namespace TEXT NOT NULL UNIQUE,
            source_account_key TEXT,
            identity_confidence TEXT NOT NULL,
            reader_timezone TEXT NOT NULL,
            current_display_name TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE conversations (
            conversation_id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL REFERENCES accounts(account_id),
            source_conversation_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            current_title TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_message_at TEXT,
            visibility_state TEXT NOT NULL DEFAULT 'active',
            roster_complete INTEGER NOT NULL DEFAULT 0,
            UNIQUE(account_id, source_conversation_id)
        );
        CREATE TABLE messages (
            message_id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL REFERENCES accounts(account_id),
            conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
            source_message_id TEXT NOT NULL,
            source_time_raw TEXT NOT NULL,
            sent_at_utc TEXT NOT NULL,
            sort_primary TEXT NOT NULL,
            sort_seq INTEGER NOT NULL,
            sort_tie INTEGER NOT NULL,
            sender_id TEXT,
            sender_membership_id TEXT,
            sender_label_snapshot_json TEXT NOT NULL,
            kind TEXT NOT NULL,
            text TEXT,
            structured_json TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            current_state TEXT NOT NULL,
            current_generation_id TEXT NOT NULL,
            UNIQUE(account_id, source_message_id)
        );
        CREATE TABLE resources (
            resource_id TEXT PRIMARY KEY,
            message_id TEXT NOT NULL REFERENCES messages(message_id),
            source_resource_key TEXT,
            source_ordinal INTEGER NOT NULL,
            kind TEXT NOT NULL,
            mime_type TEXT,
            original_name TEXT,
            declared_size INTEGER,
            declared_hash TEXT,
            availability TEXT NOT NULL,
            resolver_json TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        );
        CREATE TABLE resource_objects (
            object_digest TEXT PRIMARY KEY,
            local_path_internal TEXT NOT NULL,
            mime_type TEXT,
            byte_size INTEGER NOT NULL,
            origin TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_verified_at TEXT
        );
        CREATE TABLE reader_profiles (
            reader_id TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            auth_token_hash TEXT,
            policy_json TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        PRAGMA user_version = 1;
        """
    )
    now = "2026-01-01T00:00:00+00:00"
    connection.execute(
        "INSERT INTO accounts VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)",
        ("acct_fixture", "synthetic-v1", "source-self", "exact", "UTC", "Fixture", now, now),
    )
    connection.execute(
        "INSERT INTO conversations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "conv_fixture",
            "acct_fixture",
            "source-conv",
            "direct",
            "Synthetic conversation",
            now,
            now,
            now,
            "active",
            1,
        ),
    )
    connection.execute(
        """
        INSERT INTO messages VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            "msg_fixture",
            "acct_fixture",
            "conv_fixture",
            "source-message",
            "1",
            now,
            now,
            1,
            1,
            "{}",
            "text",
            "synthetic body",
            "{}",
            now,
            now,
            "present",
            "generation-1",
        ),
    )
    connection.execute(
        "INSERT INTO reader_profiles VALUES (?, ?, NULL, ?, 1, ?, ?)",
        ("reader_fixture", "Fixture reader", "{}", now, now),
    )
    connection.commit()
    os.chmod(path, 0o600)
    return connection


def _column_names(connection: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")]


def _column_signature(connection: sqlite3.Connection, table: str) -> list[tuple[object, ...]]:
    return [tuple(row[1:]) for row in connection.execute(f"PRAGMA table_info({table})")]


class SchemaV2MigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "private"
        self.root.mkdir(mode=0o700)
        self.path = self.root / "window.db"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_fresh_current_database_has_private_sqlite_contract_and_current_state(self) -> None:
        database = WindowDB(self.path)

        self.assertEqual(SCHEMA_VERSION, 10)
        self.assertEqual(database.schema_version, SCHEMA_VERSION)
        self.assertIsNone(database.migration_backup_path)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.root.stat().st_mode & 0o777, 0o700)
        with database.connection() as connection:
            self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
            self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(connection.execute("PRAGMA busy_timeout").fetchone()[0], 5000)
            tables = {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT name FROM sqlite_schema
                    WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                    """
                )
            }
            self.assertTrue(V2_NEW_TABLES <= tables)
            self.assertIn("account_binding_id", _column_names(connection, "accounts"))
            self.assertIn("source_inventory_epoch", _column_names(connection, "accounts"))
            self.assertIn("catalog_state", _column_names(connection, "conversations"))
            self.assertIn("unread_count", _column_names(connection, "conversations"))
            self.assertIn("catalog_observed_at", _column_names(connection, "conversations"))
            self.assertIn("search_text", _column_names(connection, "messages"))
            self.assertIn("policy_revision", _column_names(connection, "reader_profiles"))

    def test_in_process_writer_transactions_wait_without_spending_sqlite_busy_timeout(
        self,
    ) -> None:
        database = WindowDB(self.path)
        original_connect = database.connect
        first_entered = threading.Event()
        release_first = threading.Event()
        errors: list[Exception] = []

        def short_timeout_connect() -> sqlite3.Connection:
            connection = original_connect()
            connection.execute("PRAGMA busy_timeout = 50")
            return connection

        def first_writer() -> None:
            try:
                with database.transaction():
                    first_entered.set()
                    release_first.wait(timeout=1)
            except Exception as exc:
                errors.append(exc)

        def second_writer() -> None:
            try:
                with database.transaction():
                    pass
            except Exception as exc:
                errors.append(exc)

        with patch.object(database, "connect", side_effect=short_timeout_connect):
            first = threading.Thread(target=first_writer)
            second = threading.Thread(target=second_writer)
            first.start()
            self.assertTrue(first_entered.wait(timeout=1))
            second.start()
            time.sleep(0.1)
            release_first.set()
            first.join(timeout=1)
            second.join(timeout=1)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(errors, [])
        writer = database.writer_status()
        self.assertGreaterEqual(int(writer["wait_count"] or 0), 2)
        self.assertGreaterEqual(int(writer["wait_p95_ms"] or 0), 50)
        self.assertGreaterEqual(int(writer["wait_max_ms"] or 0), 50)

    def test_writer_wait_respects_operation_deadline_and_recovers(self) -> None:
        database = WindowDB(self.path)
        first_entered = threading.Event()
        release_first = threading.Event()

        def first_writer() -> None:
            with database.transaction():
                first_entered.set()
                release_first.wait(timeout=1)

        first = threading.Thread(target=first_writer)
        first.start()
        self.assertTrue(first_entered.wait(timeout=1))
        started = time.monotonic()
        try:
            with self.assertRaises(SightglassError) as raised:
                with operation_budget(0.05), database.transaction():
                    self.fail("deadline-bound writer unexpectedly acquired the lock")
        finally:
            release_first.set()
            first.join(timeout=1)

        self.assertEqual(raised.exception.code, ErrorCode.SERVICE_TIMEOUT)
        self.assertLess(time.monotonic() - started, 0.25)
        self.assertEqual(database.writer_status()["waiting_count"], 0)
        with database.transaction():
            pass

    def test_v1_upgrade_snapshots_wal_data_preserves_rows_and_is_idempotent(self) -> None:
        writer = _create_v1_fixture(self.path)
        existing_backup = self.path.with_name(f"{self.path.name}.v1.backup")
        existing_backup.write_bytes(b"do-not-overwrite")
        os.chmod(existing_backup, 0o600)
        try:
            self.assertTrue(self.path.with_name(f"{self.path.name}-wal").exists())
            database = WindowDB(self.path)
        finally:
            writer.close()

        backup = database.migration_backup_path
        self.assertIsNotNone(backup)
        assert backup is not None
        self.assertEqual(backup.name, "window.db.v1.backup.1")
        self.assertEqual(existing_backup.read_bytes(), b"do-not-overwrite")
        self.assertEqual(backup.stat().st_mode & 0o777, 0o600)

        with closing(sqlite3.connect(backup)) as snapshot:
            self.assertEqual(snapshot.execute("PRAGMA user_version").fetchone()[0], 1)
            message = snapshot.execute(
                "SELECT text FROM messages WHERE message_id = 'msg_fixture'"
            ).fetchone()
            self.assertEqual(
                message[0],
                "synthetic body",
            )
            self.assertNotIn("search_text", _column_names(snapshot, "messages"))

        with database.connection() as connection:
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION
            )
            account = connection.execute(
                """
                SELECT account_binding_id, source_inventory_epoch
                FROM accounts WHERE account_id = 'acct_fixture'
                """
            ).fetchone()
            self.assertEqual(tuple(account), (None, None))
            conversation = connection.execute(
                """
                SELECT catalog_state, unread_count, catalog_observed_at
                FROM conversations WHERE conversation_id = 'conv_fixture'
                """
            ).fetchone()
            self.assertEqual(tuple(conversation), ("unknown", None, None))
            message = connection.execute(
                "SELECT text, search_text FROM messages WHERE message_id = 'msg_fixture'"
            ).fetchone()
            self.assertEqual(tuple(message), ("synthetic body", None))
            self.assertEqual(
                connection.execute(
                    "SELECT policy_revision FROM reader_profiles WHERE reader_id = 'reader_fixture'"
                ).fetchone()[0],
                1,
            )

        fresh = WindowDB(self.root / "fresh.db")
        relevant_tables = (
            "accounts",
            "conversations",
            "messages",
            "reader_profiles",
            *sorted(V2_NEW_TABLES),
        )
        with database.connection() as migrated_connection:
            with fresh.connection() as fresh_connection:
                for table in relevant_tables:
                    self.assertEqual(
                        _column_signature(migrated_connection, table),
                        _column_signature(fresh_connection, table),
                        table,
                    )

        backups_before = sorted(self.root.glob("window.db.v1.backup*"))
        reopened = WindowDB(self.path)
        self.assertIsNone(reopened.migration_backup_path)
        self.assertEqual(sorted(self.root.glob("window.db.v1.backup*")), backups_before)

    def test_v2_state_supports_tail_backfill_recovery_and_derivative_provenance(self) -> None:
        database = WindowDB(self.path)
        now = "2026-01-01T00:00:00+00:00"
        with database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO accounts(
                    account_id, source_namespace, source_account_key, identity_confidence,
                    reader_timezone, current_display_name, first_seen_at, last_seen_at,
                    account_binding_id, source_inventory_epoch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "acct",
                    "synthetic-v2",
                    "source-self",
                    "exact",
                    "UTC",
                    "Fixture",
                    now,
                    now,
                    "binding-1",
                    "inventory-1",
                ),
            )
            connection.execute(
                """
                INSERT INTO conversations(
                    conversation_id, account_id, source_conversation_id, kind, current_title,
                    first_seen_at, last_seen_at, last_message_at, visibility_state,
                    roster_complete, catalog_state, unread_count, catalog_observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "conv",
                    "acct",
                    "source-conv",
                    "direct",
                    "Synthetic conversation",
                    now,
                    now,
                    now,
                    "active",
                    1,
                    "complete",
                    2,
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO source_catalog_state(
                    account_id, source_inventory_epoch, coverage_state, next_cursor_token,
                    scan_started_at, scan_completed_at, last_observed_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("acct", "inventory-1", "partial", "catalog-next", now, None, now, now),
            )
            connection.execute(
                """
                INSERT INTO source_conversation_state(
                    conversation_id, source_inventory_epoch, tail_cursor_token,
                    tail_generation_id, tail_sort_primary, tail_sort_seq, tail_sort_tie,
                    tail_source_message_id, tail_observed_at, indexed_before, indexed_after,
                    backfill_state, last_error_code, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "conv",
                    "inventory-1",
                    "tail-next",
                    "generation-1",
                    now,
                    5,
                    1,
                    "source-message-5",
                    now,
                    "2025-12-01T00:00:00+00:00",
                    now,
                    "running",
                    None,
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO source_shard_state(
                    account_id, source_shard_key, source_inventory_epoch,
                    source_generation_id, availability_state, cursor_token,
                    discovered_at, last_verified_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "acct",
                    "synthetic-shard",
                    "inventory-1",
                    "generation-1",
                    "available",
                    "shard-next",
                    now,
                    now,
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO source_backfill_jobs(
                    job_id, account_id, conversation_id, source_inventory_epoch,
                    requested_after, requested_before, max_messages, processed_messages,
                    cursor_token, state, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "job",
                    "acct",
                    "conv",
                    "inventory-1",
                    "2025-01-01T00:00:00+00:00",
                    "2025-12-01T00:00:00+00:00",
                    100,
                    25,
                    "resume-here",
                    "running",
                    now,
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO messages(
                    message_id, account_id, conversation_id, source_message_id,
                    source_time_raw, sent_at_utc, sort_primary, sort_seq, sort_tie,
                    sender_id, sender_membership_id, sender_label_snapshot_json,
                    kind, text, structured_json, first_seen_at, last_seen_at,
                    current_state, current_generation_id, search_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "msg",
                    "acct",
                    "conv",
                    "source-message",
                    "1",
                    now,
                    now,
                    1,
                    1,
                    "{}",
                    "file",
                    "source-visible text",
                    "{}",
                    now,
                    now,
                    "present",
                    "generation-1",
                    "derived searchable projection",
                ),
            )
            connection.execute(
                """
                INSERT INTO resources(
                    resource_id, message_id, source_resource_key, source_ordinal,
                    kind, mime_type, original_name, declared_size, declared_hash,
                    availability, resolver_json, first_seen_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "resource",
                    "msg",
                    "source-resource",
                    0,
                    "file",
                    "application/pdf",
                    "synthetic.pdf",
                    100,
                    "source-digest",
                    "available",
                    "{}",
                    now,
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO resource_objects(
                    object_digest, local_path_internal, mime_type, byte_size,
                    origin, created_at, last_verified_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "derived-digest",
                    str(self.root / "objects" / "derived-digest"),
                    "text/plain",
                    25,
                    "private_cache",
                    now,
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO resource_derivations(
                    derivation_id, resource_id, source_digest, variant,
                    processor_name, processor_version, parameters_json,
                    derived_digest, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "derivation",
                    "resource",
                    "source-digest",
                    "extracted_text",
                    "synthetic-processor",
                    "1",
                    '{"pages":[1]}',
                    "derived-digest",
                    now,
                ),
            )
        with database.connection() as connection:
            tail = connection.execute(
                """
                SELECT tail_cursor_token, indexed_before, indexed_after, backfill_state
                FROM source_conversation_state WHERE conversation_id = 'conv'
                """
            ).fetchone()
            self.assertEqual(
                tuple(tail),
                ("tail-next", "2025-12-01T00:00:00+00:00", now, "running"),
            )
            job = connection.execute(
                """
                SELECT max_messages, processed_messages, cursor_token, state
                FROM source_backfill_jobs WHERE job_id = 'job'
                """
            ).fetchone()
            self.assertEqual(tuple(job), (100, 25, "resume-here", "running"))
            message = connection.execute(
                "SELECT text, search_text FROM messages WHERE message_id = 'msg'"
            ).fetchone()
            self.assertEqual(
                tuple(message),
                ("source-visible text", "derived searchable projection"),
            )
            derivation = connection.execute(
                """
                SELECT source_digest, variant, processor_name, processor_version,
                       parameters_json, derived_digest
                FROM resource_derivations WHERE derivation_id = 'derivation'
                """
            ).fetchone()
            self.assertEqual(
                tuple(derivation),
                (
                    "source-digest",
                    "extracted_text",
                    "synthetic-processor",
                    "1",
                    '{"pages":[1]}',
                    "derived-digest",
                ),
            )

    def test_failed_migration_rolls_back_all_ddl_and_keeps_snapshot(self) -> None:
        writer = _create_v1_fixture(self.path)
        writer.close()
        failing_steps = {
            **migrations.MIGRATION_STEPS,
            1: (
                "ALTER TABLE accounts ADD COLUMN rollback_probe TEXT",
                "CREATE TABLE broken syntax",
            ),
            2: migrations.MIGRATION_STEPS[2],
            3: migrations.MIGRATION_STEPS[3],
            4: migrations.MIGRATION_STEPS[4],
            5: migrations.MIGRATION_STEPS[5],
            6: migrations.MIGRATION_STEPS[6],
            7: migrations.MIGRATION_STEPS[7],
        }

        with patch.object(migrations, "MIGRATION_STEPS", failing_steps):
            with self.assertRaises(sqlite3.OperationalError):
                WindowDB(self.path)

        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 1)
            self.assertNotIn("rollback_probe", _column_names(connection, "accounts"))
            message = connection.execute(
                "SELECT text FROM messages WHERE message_id = 'msg_fixture'"
            ).fetchone()
            self.assertEqual(
                message[0],
                "synthetic body",
            )
        backups = list(self.root.glob("window.db.v1.backup*"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].stat().st_mode & 0o777, 0o600)
        with closing(sqlite3.connect(backups[0])) as snapshot:
            self.assertEqual(snapshot.execute("PRAGMA user_version").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()

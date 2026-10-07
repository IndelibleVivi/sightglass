from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from sightglass.model import migrations
from sightglass.model.schema import SCHEMA_SQL, SCHEMA_VERSION
from tests.fixtures.legacy_window import migrate_fixture as WindowDB
from tests.unit.test_schema_v2_migration import _create_v1_fixture

INDEX_NAME = "resources_message_id"
LOOKUP = "SELECT * FROM resources WHERE message_id = ? ORDER BY resource_id"


def create_v3_fixture(connection: sqlite3.Connection) -> None:
    """A current-schema database with the v4 index removed and stamped as v3."""

    connection.executescript(SCHEMA_SQL)
    for column in (
        "coverage_version",
        "contiguous_floor_position",
        "history_complete",
        "forward_complete",
    ):
        connection.execute("ALTER TABLE source_conversation_state DROP COLUMN " + column)
    connection.execute("DROP TABLE source_read_windows")
    for table in (
        "read_lease_message",
        "read_lease",
        "body_release_jobs",
        "message_body_residency",
        "residency_totals",
        "residency_state",
        "conversation_residency",
        "residency_settings",
    ):
        connection.execute("DROP TABLE IF EXISTS " + table)
    connection.execute("DROP INDEX IF EXISTS message_resident_timeline")
    for column in ("body_available",):
        connection.execute("ALTER TABLE messages DROP COLUMN " + column)
    connection.execute("DROP TABLE observation_maintenance_state")
    connection.execute("DROP INDEX IF EXISTS message_resource_discovery_timeline")
    connection.execute("DROP INDEX IF EXISTS resources_discovery_timeline")
    connection.execute("DROP INDEX IF EXISTS resource_jobs_state_schedule")
    connection.execute("DROP INDEX IF EXISTS resource_jobs_one_active_recipe")
    connection.execute("DROP TABLE IF EXISTS resource_jobs")
    connection.execute("DROP INDEX IF EXISTS message_derived_backfill")
    connection.execute("DROP INDEX IF EXISTS message_materialized_timeline")
    connection.execute("ALTER TABLE messages DROP COLUMN current_observation_seq")
    connection.execute("ALTER TABLE messages DROP COLUMN first_observation_seq")
    connection.execute("ALTER TABLE messages DROP COLUMN projection_epoch")
    connection.execute(f"DROP INDEX IF EXISTS {INDEX_NAME}")
    connection.execute("PRAGMA user_version = 3")
    connection.commit()


def query_plan(connection: sqlite3.Connection, statement: str) -> str:
    rows = connection.execute("EXPLAIN QUERY PLAN " + statement, ("synthetic",)).fetchall()
    return " | ".join(str(row[-1]) for row in rows)


class ResourceLookupIndexTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "private"
        self.root.mkdir(mode=0o700)
        self.path = self.root / "window.db"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_fresh_database_creates_the_lookup_index(self) -> None:
        database = WindowDB(self.path)
        self.assertEqual(SCHEMA_VERSION, 10)
        self.assertEqual(database.schema_version, SCHEMA_VERSION)
        with database.connection() as connection:
            indexes = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'resources'"
                )
            }
            self.assertIn(INDEX_NAME, indexes)
            plan = query_plan(connection, LOOKUP)
            self.assertIn(INDEX_NAME, plan)
            self.assertNotIn("SCAN resources", plan)

    def test_v3_upgrade_adds_the_index_and_keeps_a_v3_snapshot(self) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            create_v3_fixture(connection)
            connection.execute(
                """
                INSERT INTO accounts(account_id, source_namespace, identity_confidence,
                    reader_timezone, current_display_name, first_seen_at, last_seen_at)
                VALUES ('acct', 'synthetic-v4', 'exact', 'UTC', 'Synthetic', 'now', 'now')
                """
            )
            connection.execute(
                """
                INSERT INTO conversations(conversation_id, account_id, source_conversation_id,
                    kind, current_title, first_seen_at, last_seen_at)
                VALUES ('conv', 'acct', 'synthetic-conv', 'direct', 'Synthetic', 'now', 'now')
                """
            )
            connection.execute(
                """
                INSERT INTO messages(message_id, account_id, conversation_id, source_message_id,
                    source_time_raw, sent_at_utc, sort_primary, sort_seq, sort_tie,
                    sender_label_snapshot_json, kind, structured_json, first_seen_at,
                    last_seen_at, current_state, current_generation_id)
                VALUES ('msg', 'acct', 'conv', 'synthetic-msg', '0', 'now', 'now', 0, 0,
                    '{}', 'text', '{}', 'now', 'now', 'present', 'generation-1')
                """
            )
            connection.execute(
                """
                INSERT INTO resources(resource_id, message_id, source_ordinal, kind,
                    availability, resolver_json, first_seen_at, last_seen_at)
                VALUES ('resource', 'msg', 0, 'file', 'available', '{}', 'now', 'now')
                """
            )
            connection.commit()
        os.chmod(self.path, 0o600)

        database = WindowDB(self.path)
        self.assertEqual(database.schema_version, SCHEMA_VERSION)
        backup = database.migration_backup_path
        self.assertIsNotNone(backup)
        assert backup is not None
        self.assertEqual(backup.name, "window.db.v3.backup")
        self.assertEqual(backup.stat().st_mode & 0o777, 0o600)
        with closing(sqlite3.connect(backup)) as snapshot:
            self.assertEqual(snapshot.execute("PRAGMA user_version").fetchone()[0], 3)
            self.assertEqual(
                snapshot.execute(
                    "SELECT count(*) FROM sqlite_schema WHERE name = ?", (INDEX_NAME,)
                ).fetchone()[0],
                0,
            )
            self.assertEqual(snapshot.execute("SELECT count(*) FROM resources").fetchone()[0], 1)
        with database.connection() as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM resources WHERE message_id = 'msg'"
                ).fetchone()[0],
                1,
            )
            plan = query_plan(connection, LOOKUP)
            self.assertIn(INDEX_NAME, plan)
            self.assertNotIn("SCAN resources", plan)

    def test_chained_v1_upgrade_reaches_the_index(self) -> None:
        _create_v1_fixture(self.path).close()
        database = WindowDB(self.path)
        self.assertEqual(database.schema_version, SCHEMA_VERSION)
        with database.connection() as connection:
            self.assertEqual(
                connection.execute("SELECT text FROM messages").fetchone()[0],
                "synthetic body",
            )
            plan = query_plan(connection, LOOKUP)
            self.assertNotIn("SCAN resources", plan)
            self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)

    def test_reopening_an_upgraded_database_is_a_no_op(self) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            create_v3_fixture(connection)
        os.chmod(self.path, 0o600)
        self.assertIsNotNone(WindowDB(self.path).migration_backup_path)
        self.assertIsNone(WindowDB(self.path).migration_backup_path)
        self.assertEqual(WindowDB(self.path).schema_version, SCHEMA_VERSION)


class MigrationSpaceGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "private"
        self.root.mkdir(mode=0o700)
        self.path = self.root / "window.db"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def make_v3(self) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            create_v3_fixture(connection)
        os.chmod(self.path, 0o600)

    def test_requirement_covers_the_snapshot_and_a_reserve(self) -> None:
        self.make_v3()
        size = self.path.stat().st_size
        exact = size + migrations.MIGRATION_SPACE_RESERVE_BYTES
        with patch.object(migrations, "_available_bytes", return_value=exact):
            migrations.require_migration_space(self.path)
        with patch.object(migrations, "_available_bytes", return_value=exact - 1):
            with self.assertRaises(migrations.MigrationDiskSpaceError):
                migrations.require_migration_space(self.path)

    def test_upgrade_refuses_to_start_without_snapshot_space(self) -> None:
        self.make_v3()
        with patch.object(migrations, "_available_bytes", return_value=1024):
            with self.assertRaises(migrations.MigrationDiskSpaceError) as caught:
                WindowDB(self.path)
        message = str(caught.exception)
        self.assertIn("no migration was started", message)
        self.assertIn("MiB free", message)

        # Nothing was written: still v3, no index, no stray snapshot.
        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 3)
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM sqlite_schema WHERE name = ?", (INDEX_NAME,)
                ).fetchone()[0],
                0,
            )
        self.assertEqual(list(self.root.glob("window.db.v3.backup*")), [])

    def test_database_still_opens_once_space_is_available(self) -> None:
        self.make_v3()
        with patch.object(migrations, "_available_bytes", return_value=1024):
            with self.assertRaises(migrations.MigrationDiskSpaceError):
                WindowDB(self.path)
        database = WindowDB(self.path)
        self.assertEqual(database.schema_version, SCHEMA_VERSION)
        backup = database.migration_backup_path
        assert backup is not None
        self.assertEqual(backup.name, "window.db.v3.backup")

    def test_partial_snapshot_artifacts_are_discarded(self) -> None:
        backup = self.root / "window.db.v3.backup"
        for suffix in ("", "-journal", "-wal"):
            Path(str(backup) + suffix).write_bytes(b"partial")
        unrelated = self.root / "keep-me"
        unrelated.write_bytes(b"keep")

        migrations._discard_partial_backup(backup)

        self.assertEqual(list(self.root.glob("window.db.v3.backup*")), [])
        self.assertTrue(unrelated.exists())

    def test_failed_snapshot_leaves_no_backup_file(self) -> None:
        self.make_v3()

        def failing_fsync(_descriptor: int) -> None:
            raise OSError(28, "No space left on device")

        with patch.object(migrations.os, "fsync", failing_fsync):
            with self.assertRaises(OSError):
                migrations._snapshot_database(self.path, 3)

        self.assertEqual(list(self.root.glob("window.db.v3.backup*")), [])
        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 3)

    def test_failed_step_rolls_back_and_keeps_the_snapshot(self) -> None:
        self.make_v3()
        steps = dict(migrations.MIGRATION_STEPS)
        steps[3] = (steps[3][0], "CREATE TABLE broken syntax")
        with patch.object(migrations, "MIGRATION_STEPS", steps):
            with self.assertRaises(sqlite3.OperationalError):
                WindowDB(self.path)
        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 3)
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM sqlite_schema WHERE name = ?", (INDEX_NAME,)
                ).fetchone()[0],
                0,
            )
        backups = list(self.root.glob("window.db.v3.backup*"))
        self.assertEqual([path.name for path in backups], ["window.db.v3.backup"])
        with closing(sqlite3.connect(backups[0])) as snapshot:
            self.assertEqual(snapshot.execute("PRAGMA user_version").fetchone()[0], 3)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from sightglass.model.schema import SCHEMA_SQL, SCHEMA_VERSION
from tests.fixtures.legacy_window import migrate_fixture as WindowDB


def create_v4_fixture(connection: sqlite3.Connection) -> None:
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
    connection.execute("PRAGMA user_version = 4")
    connection.commit()


class MaterializedProjectionMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "private"
        self.root.mkdir(mode=0o700)
        self.path = self.root / "window.db"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_fresh_database_has_projection_identity_columns(self) -> None:
        database = WindowDB(self.path)
        self.assertEqual(SCHEMA_VERSION, 10)
        self.assertEqual(database.schema_version, SCHEMA_VERSION)
        with database.connection() as connection:
            columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(messages)")}
            self.assertTrue(
                {
                    "projection_epoch",
                    "first_observation_seq",
                    "current_observation_seq",
                }
                <= columns
            )
            self.assertIsNotNone(
                connection.execute(
                    "SELECT 1 FROM sqlite_schema WHERE name = 'message_materialized_timeline'"
                ).fetchone()
            )

    def test_v4_upgrade_keeps_old_rows_ineligible_until_readmitted(self) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            create_v4_fixture(connection)
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
                VALUES ('conv', 'acct', 'source-conv', 'direct', 'Synthetic', 'now', 'now')
                """
            )
            connection.execute(
                """
                INSERT INTO messages(message_id, account_id, conversation_id, source_message_id,
                    source_time_raw, sent_at_utc, sort_primary, sort_seq, sort_tie,
                    sender_label_snapshot_json, kind, structured_json, first_seen_at,
                    last_seen_at, current_state, current_generation_id)
                VALUES ('msg', 'acct', 'conv', 'source-msg', '0', 'now', 'now', 0, 0,
                    '{}', 'text', '{}', 'now', 'now', 'present', 'generation-1')
                """
            )
            connection.commit()
        os.chmod(self.path, 0o600)

        database = WindowDB(self.path)
        self.assertEqual(database.schema_version, SCHEMA_VERSION)
        assert database.migration_backup_path is not None
        self.assertEqual(database.migration_backup_path.name, "window.db.v4.backup")
        with database.connection() as connection:
            row = connection.execute(
                """
                SELECT projection_epoch, first_observation_seq, current_observation_seq
                FROM messages WHERE message_id = 'msg'
                """
            ).fetchone()
            assert row is not None
            self.assertEqual(tuple(row), (None, None, None))


if __name__ == "__main__":
    unittest.main()

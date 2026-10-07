from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from sightglass.model.migrations import MIGRATION_STEPS
from sightglass.model.schema import SCHEMA_SQL, SCHEMA_VERSION
from tests.fixtures.legacy_window import migrate_fixture as WindowDB


class RetrievalMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "window.db"
        connection = sqlite3.connect(self.path)
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
        for table in (
            "message_links",
            "message_link_projection",
            "message_lexical",
            "message_lexical_projection",
            "derived_index_state",
        ):
            connection.execute("DROP TABLE " + table)
        for index in ("message_search_timeline", "message_derived_backfill"):
            connection.execute("DROP INDEX " + index)
        connection.execute("PRAGMA user_version=7")
        connection.commit()
        connection.close()
        self.path.chmod(0o600)

    def tearDown(self):
        self.temporary.cleanup()

    def test_schema7_upgrade_retains_private_backup_and_buildable_derivatives(self):
        database = WindowDB(self.path)
        self.assertEqual(database.schema_version, SCHEMA_VERSION)
        self.assertIsNotNone(database.migration_backup_path)
        with database.connection() as connection:
            names = {
                row[0]
                for row in connection.execute("SELECT name FROM sqlite_schema WHERE type='table'")
            }
            self.assertTrue(
                {"message_links", "message_lexical", "message_lexical_projection"} <= names
            )
            self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
        assert database.migration_backup_path is not None
        with closing(sqlite3.connect(database.migration_backup_path)) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 7)
            self.assertIsNone(
                connection.execute(
                    "SELECT name FROM sqlite_schema WHERE name='message_links'"
                ).fetchone()
            )

    def test_interrupted_schema8_ddl_rolls_back_version_and_tables(self):
        steps = {**MIGRATION_STEPS, 7: (*MIGRATION_STEPS[7], "INVALID MIGRATION SQL")}
        with patch("sightglass.model.migrations.MIGRATION_STEPS", steps):
            with self.assertRaises(sqlite3.OperationalError):
                WindowDB(self.path)
        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 7)
            self.assertIsNone(
                connection.execute(
                    "SELECT name FROM sqlite_schema WHERE name='message_links'"
                ).fetchone()
            )
        self.assertEqual(WindowDB(self.path).schema_version, SCHEMA_VERSION)

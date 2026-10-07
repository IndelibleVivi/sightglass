from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from contextlib import closing, contextmanager
from pathlib import Path
from unittest.mock import patch

from sightglass.model.repositories import WindowRepository
from sightglass.model.schema import SCHEMA_SQL, SCHEMA_VERSION
from tests.fixtures.legacy_window import migrate_fixture as WindowDB

INDEX_NAME = "message_resource_discovery_timeline"
FINDER_PLAN = """
SELECT r.resource_id
FROM messages AS m INDEXED BY message_resource_discovery_timeline
JOIN resources AS r USING(message_id)
JOIN conversations AS c USING(conversation_id)
JOIN accounts AS a USING(account_id)
LEFT JOIN participants AS p ON p.participant_id = m.sender_id
WHERE m.account_id = ?
  AND m.current_state = 'present'
  AND m.first_observation_seq IS NOT NULL
  AND m.current_observation_seq IS NOT NULL
  AND m.first_observation_seq <= ?
  AND m.current_observation_seq <= ?
  AND COALESCE(json_extract(r.resolver_json, '$.active'), 1) != 0
ORDER BY m.sent_at_utc DESC, m.sort_seq DESC, m.sort_tie DESC,
         r.source_ordinal DESC, r.resource_id DESC
LIMIT ?
"""


def create_v6_fixture(connection: sqlite3.Connection) -> None:
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
    connection.execute(f"DROP INDEX IF EXISTS {INDEX_NAME}")
    connection.execute("PRAGMA user_version = 6")
    connection.commit()


def finder_plan(connection: sqlite3.Connection) -> str:
    rows = connection.execute(
        "EXPLAIN QUERY PLAN " + FINDER_PLAN,
        ("acct", 10, 10, 2),
    ).fetchall()
    return " | ".join(str(row[-1]) for row in rows)


class ResourceDiscoveryMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "private"
        self.root.mkdir(mode=0o700)
        self.path = self.root / "window.db"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _seed(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            INSERT INTO accounts(account_id, source_namespace, identity_confidence,
                reader_timezone, current_display_name, first_seen_at, last_seen_at)
            VALUES ('acct', 'synthetic-v6', 'exact', 'UTC', 'Synthetic', 'now', 'now')
            """
        )
        connection.execute(
            """
            INSERT INTO conversations(conversation_id, account_id, source_conversation_id,
                kind, current_title, first_seen_at, last_seen_at)
            VALUES ('conv', 'acct', 'synthetic-conv', 'direct', 'Synthetic', 'now', 'now')
            """
        )
        for ordinal, sent_at in enumerate(("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")):
            connection.execute(
                """
                INSERT INTO messages(message_id, account_id, conversation_id,
                    source_message_id, source_time_raw, sent_at_utc, sort_primary,
                    sort_seq, sort_tie, sender_label_snapshot_json, kind,
                    structured_json, first_seen_at, last_seen_at, current_state,
                    current_generation_id, first_observation_seq, current_observation_seq)
                VALUES (?, 'acct', 'conv', ?, '0', ?, ?, ?, 0, '{}', 'file', '{}',
                    'now', 'now', 'present', 'generation', ?, ?)
                """,
                (
                    f"msg-{ordinal}",
                    f"source-msg-{ordinal}",
                    sent_at,
                    sent_at,
                    ordinal,
                    ordinal + 1,
                    ordinal + 1,
                ),
            )
            connection.execute(
                """
                INSERT INTO resources(resource_id, message_id, source_ordinal, kind,
                    availability, resolver_json, first_seen_at, last_seen_at)
                VALUES (?, ?, 0, 'file', 'local_available', '{"active":true}', 'now', 'now')
                """,
                (f"resource-{ordinal}", f"msg-{ordinal}"),
            )
        connection.commit()

    def test_fresh_schema_uses_account_resource_timeline(self) -> None:
        database = WindowDB(self.path)
        self.assertEqual(SCHEMA_VERSION, 10)
        self.assertEqual(database.schema_version, SCHEMA_VERSION)
        statements: list[str] = []
        open_connection = database.connection

        @contextmanager
        def traced_connection():
            with open_connection() as connection:
                connection.set_trace_callback(statements.append)
                yield connection

        with patch.object(database, "connection", traced_connection):
            WindowRepository(database).find_resources(
                account_id="acct",
                query="",
                conversation_ids=(),
                kinds=(),
                format_families=(),
                after=None,
                before=None,
                availability=(),
                permitted_conversations=None,
                denied_conversations=(),
                observation_watermark=10,
                position=None,
                limit=2,
            )
        statement = next(
            statement
            for statement in statements
            if "FROM messages AS m INDEXED BY message_resource_discovery_timeline" in statement
        )
        with open_connection() as connection:
            rows = connection.execute("EXPLAIN QUERY PLAN " + statement).fetchall()
        plan = " | ".join(str(row[-1]) for row in rows)
        self.assertIn(INDEX_NAME, plan)
        self.assertNotIn("sqlite_autoindex_messages_2", plan)

    def test_v6_upgrade_adds_index_and_preserves_discovery_order(self) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            create_v6_fixture(connection)
            self._seed(connection)
        os.chmod(self.path, 0o600)

        database = WindowDB(self.path)

        self.assertEqual(database.schema_version, SCHEMA_VERSION)
        assert database.migration_backup_path is not None
        self.assertEqual(database.migration_backup_path.name, "window.db.v6.backup")
        with database.connection() as connection:
            self.assertIn(INDEX_NAME, finder_plan(connection))
            rows = connection.execute(FINDER_PLAN, ("acct", 10, 10, 2)).fetchall()
        self.assertEqual([str(row[0]) for row in rows], ["resource-1", "resource-0"])


if __name__ == "__main__":
    unittest.main()

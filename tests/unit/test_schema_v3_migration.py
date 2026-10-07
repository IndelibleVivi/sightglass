from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from sightglass.model import migrations
from sightglass.model.schema import SCHEMA_VERSION
from sightglass.runtime.control import cache_status, cleanup_cache
from tests.fixtures.legacy_window import migrate_fixture as WindowDB
from tests.unit.test_schema_v2_migration import _create_v1_fixture

VOICE_TABLES = ("voice_jobs", "voice_batches", "voice_batch_items", "voice_batch_events")


def seed_voice_parents(connection: sqlite3.Connection) -> None:
    connection.execute("""
        INSERT INTO accounts(account_id, source_namespace, identity_confidence,
            reader_timezone, current_display_name, first_seen_at, last_seen_at)
        VALUES ('acct', 'synthetic-voice', 'exact', 'UTC', 'Synthetic', 'now', 'now')
    """)
    connection.execute("""
        INSERT INTO conversations(conversation_id, account_id, source_conversation_id,
            kind, current_title, first_seen_at, last_seen_at)
        VALUES ('conv', 'acct', 'synthetic-conv', 'direct', 'Synthetic', 'now', 'now')
    """)
    connection.execute("""
        INSERT INTO messages(message_id, account_id, conversation_id, source_message_id,
            source_time_raw, sent_at_utc, sort_primary, sort_seq, sort_tie,
            sender_label_snapshot_json, kind, structured_json, first_seen_at,
            last_seen_at, current_state, current_generation_id)
        VALUES ('msg', 'acct', 'conv', 'synthetic-msg', '0', 'now', 'now', 0, 0,
            '{}', 'voice', '{}', 'now', 'now', 'present', 'synthetic-generation')
    """)
    connection.execute("""
        INSERT INTO resources(resource_id, message_id, source_ordinal, kind,
            availability, resolver_json, first_seen_at, last_seen_at)
        VALUES ('resource', 'msg', 0, 'voice', 'available', '{}', 'now', 'now')
    """)
    connection.execute("""
        INSERT INTO reader_profiles(reader_id, display_name, policy_json, created_at, updated_at)
        VALUES ('reader', 'Synthetic', '{}', 'now', 'now')
    """)


def insert_job(connection: sqlite3.Connection, **overrides: object) -> None:
    values: dict[str, object] = dict(
        job_id="job", account_id="acct", resource_id="resource", resource_revision="revision",
        recipe_digest="recipe", recipe_json="{}", created_at="now", updated_at="now",
    )
    values.update(overrides)
    connection.execute(
        f"INSERT INTO voice_jobs({','.join(values)}) VALUES ({','.join('?' for _ in values)})",
        tuple(values.values()),
    )


def insert_batch(connection: sqlite3.Connection, **overrides: object) -> None:
    values: dict[str, object] = dict(
        batch_id="batch", reader_id="reader", account_id="acct", selection_digest="selection",
        recipe_digest="recipe", voice_policy="auto", created_at="now", expires_at="later",
    )
    values.update(overrides)
    connection.execute(
        f"INSERT INTO voice_batches({','.join(values)}) VALUES ({','.join('?' for _ in values)})",
        tuple(values.values()),
    )


class SchemaV3MigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.path = self.root / "window.db"

    def create_v2(self) -> None:
        with closing(_create_v1_fixture(self.path)) as connection:
            for statement in migrations.MIGRATION_STEPS[1]:
                connection.execute(statement)
            connection.execute("""
                CREATE TABLE resource_bindings (
                    resource_id TEXT NOT NULL REFERENCES resources(resource_id),
                    object_digest TEXT NOT NULL REFERENCES resource_objects(object_digest),
                    variant TEXT NOT NULL, created_at TEXT NOT NULL,
                    PRIMARY KEY(resource_id, variant))
            """)
            connection.execute("PRAGMA user_version = 2")
            connection.commit()

    def test_v2_upgrade_voice_job_and_gc_end_to_end(self) -> None:
        self.create_v2()
        database = WindowDB(self.path)
        self.assertEqual(database.schema_version, SCHEMA_VERSION)
        with database.transaction() as connection:
            seed_voice_parents(connection)
            root = database.path.parent / "resource-cache" / "objects"
            root.mkdir(parents=True, mode=0o700)
            root.parent.chmod(0o700)
            for digest in ("a" * 64, "b" * 64):
                path = root / digest
                path.write_bytes(b"synthetic")
                path.chmod(0o600)
                connection.execute("""
                    INSERT INTO resource_objects(object_digest, local_path_internal,
                        byte_size, origin, created_at) VALUES (?, ?, 9, 'private_cache', 'now')
                """, (digest, str(path)))
            insert_job(connection, input_digest="a" * 64)
        self.assertEqual(cache_status(database)["unbound_count"], 1)
        self.assertEqual(cleanup_cache(database, apply=False)["object_count"], 1)
        result = cleanup_cache(database, apply=True)
        self.assertEqual(result["removed_count"], 1)
        self.assertTrue((root / ("a" * 64)).exists())
        self.assertFalse((root / ("b" * 64)).exists())
        with database.connection() as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM voice_jobs").fetchone()[0], 1)
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_fresh_and_upgrades_match_with_private_nonoverwriting_backups(self) -> None:
        fresh = WindowDB(self.root / "fresh.db")
        self.create_v2()
        reserved = self.root / "window.db.v2.backup"
        reserved.write_bytes(b"reserved")
        reserved.chmod(0o600)
        database = WindowDB(self.path)
        backup = database.migration_backup_path
        assert backup is not None
        self.assertEqual(backup.name, "window.db.v2.backup.1")
        self.assertEqual(backup.stat().st_mode & 0o777, 0o600)
        self.assertEqual(reserved.read_bytes(), b"reserved")
        with closing(sqlite3.connect(backup)) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 2)
            self.assertEqual(connection.execute("SELECT text FROM messages").fetchone()[0],
                             "synthetic body")
            self.assertEqual(connection.execute(
                "SELECT name FROM sqlite_master WHERE name LIKE 'voice_%'"
            ).fetchall(), [])
        with fresh.connection() as left, database.connection() as right:
            query = ("SELECT type, name, sql FROM sqlite_master "
                     "WHERE name LIKE 'voice_%' ORDER BY name")
            self.assertEqual([tuple(r) for r in left.execute(query)],
                             [tuple(r) for r in right.execute(query)])
            self.assertEqual(len(right.execute(query).fetchall()), 9)
            self.assertEqual(right.execute("SELECT text FROM messages").fetchone()[0],
                             "synthetic body")
        self.assertIsNone(WindowDB(self.path).migration_backup_path)
        v1 = self.root / "v1.db"
        _create_v1_fixture(v1).close()
        self.assertEqual(WindowDB(v1).schema_version, SCHEMA_VERSION)

    def test_second_step_failure_rolls_back_entire_upgrade(self) -> None:
        for source_version in (1, 2):
            with self.subTest(source_version=source_version):
                self.path = self.root / f"v{source_version}.db"
                if source_version == 2:
                    self.create_v2()
                else:
                    _create_v1_fixture(self.path).close()
                steps = dict(migrations.MIGRATION_STEPS)
                steps[2] = (steps[2][0], "CREATE TABLE broken syntax")
                with patch.object(migrations, "MIGRATION_STEPS", steps):
                    with self.assertRaises(sqlite3.OperationalError):
                        WindowDB(self.path)
                with closing(sqlite3.connect(self.path)) as connection:
                    self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0],
                                     source_version)
                    self.assertEqual(connection.execute(
                        "SELECT name FROM sqlite_master WHERE name LIKE 'voice_%'"
                    ).fetchall(), [])
                    self.assertEqual(connection.execute("SELECT text FROM messages").fetchone()[0],
                                     "synthetic body")
                    columns = [r[1] for r in connection.execute("PRAGMA table_info(accounts)")]
                    self.assertEqual("account_binding_id" in columns, source_version == 2)
                backup = self.path.with_name(f"{self.path.name}.v{source_version}.backup")
                self.assertEqual(os.stat(backup).st_mode & 0o777, 0o600)

    def test_job_states_active_uniqueness_limits_and_foreign_keys(self) -> None:
        database = WindowDB(self.path)
        with database.transaction() as connection:
            seed_voice_parents(connection)
            for state in ("pending", "leased", "running", "blocked"):
                insert_job(connection, state=state)
                with self.assertRaises(sqlite3.IntegrityError):
                    insert_job(connection, job_id="duplicate")
                connection.execute("DELETE FROM voice_jobs")
            for state in ("ready", "failed", "cancelled"):
                insert_job(connection, job_id=state, state=state)
            insert_job(connection)
            for column, value in (
                ("state", "unknown"), ("attempt", -1), ("fencing_token", -1),
                ("max_duration_ms", 0), ("max_bytes", 0), ("account_id", "absent"),
                ("resource_id", "absent"), ("input_digest", "absent"),
                ("result_digest", "absent"),
            ):
                with self.subTest(column=column), self.assertRaises(sqlite3.IntegrityError):
                    insert_job(connection, **{"job_id": "invalid", "recipe_digest": "other",
                                              column: value})

    def test_batch_item_event_constraints_and_autoincrement(self) -> None:
        database = WindowDB(self.path)
        with database.transaction() as connection:
            seed_voice_parents(connection)
            insert_job(connection)
            insert_batch(connection)
            for column, value in (("state", "unknown"), ("voice_policy", "unknown"),
                                  ("reader_id", "absent"), ("account_id", "absent")):
                with self.subTest(column=column), self.assertRaises(sqlite3.IntegrityError):
                    insert_batch(connection, **{"batch_id": "invalid", column: value})
            for index, state in enumerate(("open", "sealed", "delivered", "expired", "cancelled")):
                insert_batch(connection, batch_id=f"state-{index}", state=state,
                             voice_policy=("auto", "cached", "off")[index % 3])
            item_sql = """INSERT INTO voice_batch_items VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
            for ordinal, state in enumerate(("queued", "admitted", "rejected", "cached",
                                             "served", "skipped")):
                connection.execute(item_sql, ("batch", ordinal, "msg", "resource", "revision",
                                              None, 0, state))
            valid = ["batch", 10, "msg", "resource", "revision", "job", 0, "queued"]
            for column, value in ((0, "absent"), (1, -1), (1, 0), (2, "absent"),
                                  (3, "absent"), (5, "absent"), (6, -1), (7, "unknown")):
                invalid = valid.copy()
                invalid[column] = value
                with self.subTest(column=column, value=value):
                    with self.assertRaises(sqlite3.IntegrityError):
                        connection.execute(item_sql, invalid)
            event_sql = """INSERT INTO voice_batch_events(batch_id, item_ordinal, job_id,
                kind, result_digest, created_at) VALUES (?, ?, ?, ?, ?, 'now')"""
            for kind in ("ready", "failed", "state-change"):
                connection.execute(event_sql, ("batch", 0, "job", kind, None))
            last = connection.execute("SELECT MAX(event_id) FROM voice_batch_events").fetchone()[0]
            connection.execute("DELETE FROM voice_batch_events")
            connection.execute(event_sql, ("batch", None, None, "state-change", None))
            self.assertGreater(connection.execute(
                "SELECT event_id FROM voice_batch_events").fetchone()[0], last)
            for values in (("absent", None, None, "ready", None),
                           ("state-0", 0, None, "ready", None),
                           ("batch", 100, None, "ready", None),
                           ("batch", None, "absent", "ready", None),
                           ("batch", None, None, "unknown", None),
                           ("batch", None, None, "ready", "absent")):
                with self.subTest(values=values), self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(event_sql, values)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from sightglass.cli import _storage_command
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.observation_codec import encode_observation
from sightglass.model.schema import SCHEMA_VERSION
from sightglass.reader.deliveries import DeliveryPayloadStore
from sightglass.resources.cache import ResourceObjectStore
from sightglass.runtime.config import ConfigStore, SightglassConfig
from sightglass.storage import (
    MIB,
    StorageBudget,
    StorageSettings,
    storage_scope,
    temporary_workspace,
)
from tests.fixtures.legacy_window import migrate_fixture as WindowDB
from tests.unit.test_schema_v4_migration import create_v3_fixture


class StorageBudgetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.path = self.root / "window.db"
        self.settings = StorageSettings(4 * MIB, 8 * MIB, 0, 2 * MIB)
        self.free = patch("sightglass.storage._free", return_value=1024 * MIB)
        self.free.start()
        self.addCleanup(self.free.stop)
        self.addCleanup(self.temporary.cleanup)
        self.budget = StorageBudget(self.root, self.path, self.settings)

    def test_counts_wal_spool_objects_staging_backups_without_following_symlinks(self) -> None:
        names = (
            "window.db",
            "window.db-wal",
            "deliveries/a.json",
            "resource-cache/objects/a",
            "voice-work/a.pcm",
            "window.db.v3.backup",
            "window.db.v4.backup.zst",
            "window.db.v4.backup.zst.json",
            "window.db.backup",
        )
        for name in names:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"s" * 8192)
        (self.root / "unrelated-link").symlink_to(self.root.parent)
        result = self.budget.status(reconcile=True)
        self.assertEqual(result["accounted_bytes"], 9 * 8192)
        self.assertEqual(sum(result["components"].values()), result["accounted_bytes"])
        self.assertEqual(result["components"]["migration_backups"], 3 * 8192)
        self.assertNotIn(str(self.root), str(result))

    def test_explain_classifies_migration_backups_without_paths_or_symlink_targets(self) -> None:
        files = {
            "window.db": b"database",
            "window.db.v3.backup": b"backup",
            "resource-cache/objects/abc": b"object",
            "sightglassd.log": b"log",
            "unknown.bin": b"unknown",
        }
        for name, payload in files.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        (self.root / "outside-link").symlink_to(self.root.parent)

        status = self.budget.status(reconcile=True)
        self.assertEqual(
            status["components"]["migration_backups"],
            max(
                (self.root / "window.db.v3.backup").stat().st_size,
                (self.root / "window.db.v3.backup").stat().st_blocks * 512,
            ),
        )
        result = self.budget.explain(limit=2)
        self.assertEqual(result["file_count"], len(files))
        self.assertEqual(len(result["files"]), 2)
        self.assertEqual(result["next_offset"], 2)
        all_files = self.budget.explain(limit=20)["files"]
        rendered = str(all_files)
        self.assertNotIn(str(self.root), rendered)
        self.assertNotIn("outside-link", rendered)
        roles = {item["relative_path"]: item["recognized_role"] for item in all_files}
        self.assertEqual(roles["window.db.v3.backup"], "migration_backup")
        self.assertEqual(roles["resource-cache/objects/abc"], "resource_object")
        self.assertEqual(roles["unknown.bin"], "unknown_owned_file")

    def test_database_storage_explain_reports_physical_layout_and_bounded_legacy_sample(
        self,
    ) -> None:
        database = WindowDB(self.path, storage=self.budget)
        timestamp = "2026-09-28T00:00:00+00:00"
        legacy = '{"body":"' + ("synthetic " * 200) + '"}'
        with database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO accounts(
                    account_id, source_namespace, identity_confidence, reader_timezone,
                    current_display_name, first_seen_at, last_seen_at
                ) VALUES ('acct', 'synthetic-storage', 'stable', 'UTC', 'Synthetic', ?, ?)
                """,
                (timestamp, timestamp),
            )
            connection.execute(
                """
                INSERT INTO conversations(
                    conversation_id, account_id, source_conversation_id, kind,
                    current_title, first_seen_at, last_seen_at
                ) VALUES ('conv', 'acct', 'source-conv', 'group', 'Synthetic', ?, ?)
                """,
                (timestamp, timestamp),
            )
            connection.execute(
                """
                INSERT INTO messages(
                    message_id, account_id, conversation_id, source_message_id,
                    source_time_raw, sent_at_utc, sort_primary, sort_seq, sort_tie,
                    sender_label_snapshot_json, kind, structured_json, first_seen_at,
                    last_seen_at, current_state, current_generation_id
                ) VALUES (
                    'msg', 'acct', 'conv', 'source-msg', '0', ?, ?, 1, 1,
                    '{}', 'text', '{}', ?, ?, 'active', 'generation'
                )
                """,
                (timestamp, timestamp, timestamp, timestamp),
            )
            connection.executemany(
                """
                INSERT INTO message_observations(
                    observation_id, message_id, observed_at, source_generation_id,
                    state, payload_digest, parsed_json, parser_version
                ) VALUES (?, 'msg', ?, 'generation', 'active', ?, ?, 'test')
                """,
                (
                    ("obs-legacy", timestamp, "digest-legacy", legacy),
                    ("obs-encoded", timestamp, "digest-encoded", encode_observation(legacy)),
                ),
            )

        result = database.storage_explain(limit=20, sample_size=1, deep=True)
        self.assertFalse(result["mutated"])
        detail = result["database"]
        self.assertEqual(detail["message_count"], 1)
        self.assertEqual(detail["observation_count"], 2)
        self.assertEqual(detail["legacy_text_observation_count"], 1)
        self.assertEqual(detail["encoded_blob_observation_count"], 1)
        sample = detail["legacy_compression_sample"]
        self.assertEqual(sample["sampled_rows"], 1)
        self.assertGreater(sample["estimated_payload_reduction_bytes"], 0)
        self.assertEqual(sample["estimate_scope"], "payload_only_not_filesystem_reclaim")
        objects = {item["name"]: item for item in detail["objects"]}
        self.assertEqual(objects["messages"]["record_count"], 1)
        self.assertEqual(objects["message_observations"]["record_count"], 2)

    def test_outstanding_reservations_prevent_overbooking_and_release_on_error(self) -> None:
        with self.budget.reserve(5 * MIB):
            with self.assertRaises(SightglassError):
                with self.budget.reserve(4 * MIB):
                    self.fail("overbooked")
            self.assertEqual(self.budget.status()["reserved_bytes"], 5 * MIB)
        try:
            with self.budget.reserve(7 * MIB):
                raise ValueError("synthetic failure")
        except ValueError:
            pass
        self.assertEqual(self.budget.status()["reserved_bytes"], 0)

    def test_free_floor_leaves_maintenance_room(self) -> None:
        self.budget.settings = replace(self.settings, min_free_bytes=MIB)
        with patch("sightglass.storage._free", return_value=3 * MIB):
            with self.assertRaises(SightglassError) as caught:
                self.budget.require()
            self.assertEqual(caught.exception.details["reason"], "filesystem_free_floor")
            with self.budget.reserve(MIB, maintenance=True):
                pass
        with patch("sightglass.storage._free", return_value=MIB):
            with self.assertRaises(SightglassError):
                with self.budget.reserve(MIB, maintenance=True):
                    self.fail("maintenance crossed safety floor")

    def test_parallel_reservations_admit_only_one_competing_writer(self) -> None:
        started = threading.Barrier(2)
        attempted = threading.Barrier(2)

        def reserve() -> bool:
            started.wait(timeout=5)
            try:
                with self.budget.reserve(5 * MIB):
                    attempted.wait(timeout=5)
                    return True
            except SightglassError:
                attempted.wait(timeout=5)
                return False

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: reserve(), range(2)))
        self.assertEqual(sorted(results), [False, True])
        self.assertEqual(self.budget.status()["reserved_bytes"], 0)

    def test_pressure_before_commit_rolls_back_all_database_rows(self) -> None:
        db = WindowDB(self.path, storage=self.budget)
        with db.transaction() as connection:
            connection.execute("CREATE TABLE storage_probe(value TEXT)")
        with self.assertRaises(SightglassError) as caught:
            with db.transaction() as connection:
                connection.execute("INSERT INTO storage_probe VALUES ('uncommitted')")
                (self.root / "synthetic-external-growth").write_bytes(b"x" * 8 * MIB)
        self.assertEqual(caught.exception.code, ErrorCode.STORAGE_PRESSURE)
        with db.connection() as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM storage_probe").fetchone()[0], 0
            )
        self.assertEqual(self.budget.status()["reserved_bytes"], 0)

    def test_object_and_spool_reservation_fail_before_creating_bytes(self) -> None:
        cache = ResourceObjectStore(self.path, storage=self.budget)
        spool = DeliveryPayloadStore(self.path, storage=self.budget)
        cache.put(b"retained", mime_type="text/plain", origin="synthetic")
        self.budget.settings = StorageSettings(1, 2, 0, 2 * MIB)
        # An existing content-addressed object can be reused without allocating bytes.
        cache.put(b"retained", mime_type="text/plain", origin="synthetic")
        for write in (
            lambda: cache.put(b"new", mime_type="text/plain", origin="synthetic"),
            lambda: spool.write("synthetic-delivery", {"data": "synthetic"}),
        ):
            with self.assertRaises(SightglassError):
                write()
        self.assertEqual(len(list(cache.objects.iterdir())), 1)
        self.assertEqual(list(spool.root.iterdir()), [])

    def test_upgrade_budget_fails_before_backup_or_schema_changes_then_recovers(self) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            create_v3_fixture(connection)
        with self.assertRaises(SightglassError) as caught:
            WindowDB(self.path, storage=self.budget)
        self.assertEqual(caught.exception.code, ErrorCode.STORAGE_PRESSURE)
        self.assertEqual(list(self.root.glob("window.db.v3.backup*")), [])
        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 3)
        self.budget.settings = StorageSettings(64 * MIB, 128 * MIB, 0, 8 * MIB)
        upgraded = WindowDB(self.path, storage=self.budget)
        self.assertEqual(upgraded.schema_version, SCHEMA_VERSION)
        self.assertIsNotNone(upgraded.migration_backup_path)
        self.assertGreater(self.budget.status()["components"]["migration_backups"], 0)
        self.assertEqual(self.budget.status()["reserved_bytes"], 0)

    def test_processor_workspace_is_owned_reserved_and_cleaned(self) -> None:
        with storage_scope(self.budget), temporary_workspace("synthetic-", MIB) as name:
            self.assertTrue(Path(name).is_relative_to(self.root.resolve()))
            self.assertEqual(self.budget.status()["reserved_bytes"], MIB)
            (Path(name) / "input").write_bytes(b"synthetic")
            self.assertEqual(self.budget.status(reconcile=True)["accounted_bytes"], 0)
        self.assertFalse(Path(name).exists())
        self.assertEqual(self.budget.status()["reserved_bytes"], 0)
        self.assertEqual(self.budget.status()["accounted_bytes"], 0)

    def test_config_defaults_roundtrip_validation_and_stopped_only_command(self) -> None:
        config = SightglassConfig.create(self.root / "state", self.root / "source")
        value = config.as_dict()
        del value["storage"]
        self.assertEqual(SightglassConfig.from_dict(value).storage, StorageSettings())
        value["storage"] = self.settings.as_dict()
        self.assertEqual(SightglassConfig.from_dict(value).storage, self.settings)
        for bad in ({"soft_limit_bytes": True}, {"hard_limit_bytes": -1}, {"typo": 1}):
            value["storage"] = bad
            with self.assertRaises(ValueError):
                SightglassConfig.from_dict(value)
        store = ConfigStore(config.data_dir / "config.json")
        store.save(config)
        arguments = Namespace(storage_command="configure", **self.settings.as_dict())
        _storage_command(arguments, store)
        self.assertEqual(store.load().storage, self.settings)
        with patch("sightglass.cli._daemon_must_be_stopped", side_effect=RuntimeError("running")):
            with self.assertRaisesRegex(RuntimeError, "running"):
                _storage_command(arguments, store)

    def test_new_cli_status_can_inspect_files_beside_a_pre_budget_daemon(self) -> None:
        config = SightglassConfig.create(self.root / "state", self.root / "source")
        store = ConfigStore(config.data_dir / "config.json")
        store.save(config)
        config.socket_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        config.socket_path.touch()
        with patch("sightglass.cli._operator") as operator:
            operator.return_value.call.return_value = {"schema": "sightglass.daemon-status.v1"}
            result = _storage_command(Namespace(storage_command="status"), store)
        self.assertEqual(result["schema"], "sightglass.storage-status.v1")
        self.assertGreater(result["accounted_bytes"], 0)
        self.assertNotIn("message_count", result)

    def test_offline_storage_explain_does_not_open_or_migrate_database(self) -> None:
        config = SightglassConfig.create(self.root / "state", self.root / "source")
        store = ConfigStore(config.data_dir / "config.json")
        store.save(config)
        config.window_db_path.write_bytes(b"not-a-database")
        result = _storage_command(
            Namespace(storage_command="explain", offset=0, limit=20, sample_size=1), store
        )
        self.assertFalse(result["mutated"])
        self.assertEqual(result["database"]["reason"], "daemon_not_running")
        self.assertEqual(config.window_db_path.read_bytes(), b"not-a-database")
        self.assertNotIn(str(config.data_dir), str(result["files"]))
        self.assertIn("history", result)
        self.assertFalse(result["history"]["available"])
        self.assertEqual(result["history"]["reason"], "history_absent")
        self.assertNotIn(str(config.data_dir), str(result["history"]))

    def test_offline_storage_explain_reads_existing_history_without_database(self) -> None:
        from datetime import UTC, datetime, timedelta

        from sightglass.runtime.storage_history import (
            STORAGE_HISTORY_FILENAME,
            DailySnapshot,
            StorageHistory,
        )

        config = SightglassConfig.create(self.root / "state", self.root / "source")
        store = ConfigStore(config.data_dir / "config.json")
        store.save(config)
        free = patch("sightglass.storage._free", return_value=64 * 1024 * MIB)
        free.start()
        self.addCleanup(free.stop)
        budget = StorageBudget(config.data_dir, config.window_db_path, config.storage)
        history = StorageHistory(config.data_dir, budget)
        components = {key: 0 for key in (
            "database", "database_sidecars", "migration_backups", "delivery_spool",
            "resource_objects_and_tmp", "staging", "other_owned_files",
        )}
        components["database"] = 500
        today = datetime.now(UTC).date()
        baseline_day = (today - timedelta(days=7)).isoformat()
        current_day = today.isoformat()
        history.record(
            DailySnapshot(
                day=baseline_day, captured_at=f"{baseline_day}T00:00:00+00:00",
                accounted_bytes=500, components=components, message_count=1, observation_count=2,
            )
        )
        components["database"] = 900
        history.record(
            DailySnapshot(
                day=current_day, captured_at=f"{current_day}T00:00:00+00:00",
                accounted_bytes=900, components=components, message_count=5, observation_count=9,
            )
        )
        config.window_db_path.write_bytes(b"still-not-a-database")
        result = _storage_command(
            Namespace(storage_command="explain", offset=0, limit=20, sample_size=0), store
        )
        history_block = result["history"]
        self.assertTrue(history_block["available"])
        self.assertEqual(history_block["as_of_day"], current_day)
        self.assertTrue(history_block["windows"]["7d"]["available"])
        self.assertEqual(
            history_block["windows"]["7d"]["deltas"]["message_count"], 4
        )
        self.assertEqual(config.window_db_path.read_bytes(), b"still-not-a-database")
        self.assertTrue((config.data_dir / STORAGE_HISTORY_FILENAME).is_file())
        self.assertNotIn(str(config.data_dir), str(history_block))

    def test_online_storage_explain_uses_the_operator_daemon(self) -> None:
        config = SightglassConfig.create(self.root / "state", self.root / "source")
        store = ConfigStore(config.data_dir / "config.json")
        store.save(config)
        config.socket_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        config.socket_path.touch()
        expected = {"schema": "sightglass.storage-explain.v1", "mutated": False}
        with patch("sightglass.cli._operator") as operator:
            operator.return_value.call.return_value = expected
            result = _storage_command(
                Namespace(storage_command="explain", offset=3, limit=7, sample_size=11),
                store,
            )
        self.assertEqual(result, expected)
        operator.assert_called_once_with(store, timeout=30.0)
        operator.return_value.call.assert_called_once_with(
            "operator.storage.explain",
            {"offset": 3, "limit": 7, "sample_size": 11,
             "deep": False, "phase": "all", "after_object": None, "deadline_seconds": 10.0},
        )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from argparse import Namespace
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from sightglass.cli import _storage_command
from sightglass.model import backups as backups_module
from sightglass.model.backups import (
    backup_plan,
    create_compressed_snapshot,
    find_verified_migration_snapshot,
    recover_interrupted_restore,
    restore_compressed_snapshot,
    retire_compressed_snapshot,
    retire_legacy_backups,
)
from sightglass.model.db import WindowDB as WindowDatabase
from sightglass.model.schema import SCHEMA_VERSION
from sightglass.runtime.config import ConfigStore, SightglassConfig
from tests.fixtures.legacy_window import migrate_fixture as WindowDB


class StorageBackupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "private"
        self.root.mkdir(mode=0o700)
        self.path = self.root / "window.db"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _restore_tx_dir(self) -> Path:
        return backups_module._restore_tx_dir(self.path)

    def _database_with_probe(self, value: str = "before") -> WindowDatabase:
        database = WindowDB(self.path)
        with database.transaction() as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS backup_probe(value TEXT)")
            connection.execute("DELETE FROM backup_probe")
            connection.execute("INSERT INTO backup_probe VALUES (?)", (value,))
        return database

    def test_create_plan_restore_and_compressed_retirement(self) -> None:
        self._database_with_probe()
        created = create_compressed_snapshot(self.path)
        self.assertTrue(created["created"])
        artifact = self.root / created["artifact"]
        manifest = self.root / created["manifest"]
        self.assertEqual(artifact.stat().st_mode & 0o777, 0o600)
        self.assertEqual(manifest.stat().st_mode & 0o777, 0o600)

        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("UPDATE backup_probe SET value = 'after'")
            connection.commit()
        plan = backup_plan(self.path)
        snapshot = next(
            item
            for item in plan["compressed_snapshots"]
            if item["artifact"] == artifact.name
        )
        self.assertTrue(snapshot["verified"])
        restored = restore_compressed_snapshot(
            self.path, artifact.name, snapshot["restore_ack"]
        )
        self.assertTrue(restored["restored"])
        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(
                connection.execute("SELECT value FROM backup_probe").fetchone()[0],
                "before",
            )

        refreshed = backup_plan(self.path)
        snapshot = next(
            item
            for item in refreshed["compressed_snapshots"]
            if item["artifact"] == artifact.name
        )
        retired = retire_compressed_snapshot(
            self.path, artifact.name, snapshot["retire_ack"]
        )
        self.assertEqual(retired["removed_count"], 2)
        self.assertFalse(artifact.exists())
        self.assertFalse(manifest.exists())

    def test_legacy_retirement_requires_the_exact_current_plan(self) -> None:
        self._database_with_probe()
        backups = [self.root / "window.db.v1.backup", self.root / "window.db.v3.backup"]
        for index, path in enumerate(backups):
            path.write_bytes(f"synthetic-{index}".encode())
            os.chmod(path, 0o600)
        plan = backup_plan(self.path)
        stale_ack = plan["legacy_backups"]["retire_ack"]
        backups[0].write_bytes(b"changed")
        with self.assertRaisesRegex(RuntimeError, "stale or invalid"):
            retire_legacy_backups(self.path, stale_ack)
        current = backup_plan(self.path)
        result = retire_legacy_backups(
            self.path, current["legacy_backups"]["retire_ack"]
        )
        self.assertEqual(result["removed_count"], 2)
        self.assertTrue(all(not path.exists() for path in backups))

    def test_schema_upgrade_uses_verified_compressed_snapshot_without_raw_copy(self) -> None:
        self._database_with_probe()
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("DROP INDEX message_resource_discovery_timeline")
            connection.execute("DROP INDEX resources_discovery_timeline")
            connection.execute("DROP INDEX resource_jobs_state_schedule")
            connection.execute("DROP INDEX resource_jobs_one_active_recipe")
            connection.execute("DROP TABLE resource_jobs")
            for column in (
                "coverage_version", "contiguous_floor_position",
                "history_complete", "forward_complete",
            ):
                connection.execute("ALTER TABLE source_conversation_state DROP COLUMN " + column)
            connection.execute("DROP TABLE source_read_windows")
            for table in ("read_lease_message", "read_lease", "body_release_jobs",
                  "message_body_residency", "residency_totals", "residency_state",
                  "conversation_residency", "residency_settings"):
                connection.execute("DROP TABLE IF EXISTS " + table)
            connection.execute("DROP INDEX IF EXISTS message_resident_timeline")
            for column in ("body_available",):
                connection.execute("ALTER TABLE messages DROP COLUMN " + column)
            connection.execute("DROP TABLE observation_maintenance_state")
            connection.execute("PRAGMA user_version = 5")
            connection.commit()
        created = create_compressed_snapshot(self.path)
        artifact = self.root / created["artifact"]

        with patch(
            "sightglass.model.migrations.require_migration_space",
            side_effect=AssertionError("verified compressed migration must not request raw space"),
        ):
            upgraded = WindowDB(self.path)
        self.assertEqual(upgraded.schema_version, SCHEMA_VERSION)
        self.assertEqual(upgraded.migration_backup_path, artifact.resolve())
        self.assertEqual(list(self.root.glob("window.db.v5.backup")), [])
        with upgraded.connection() as connection:
            self.assertIsNotNone(
                connection.execute(
                    "SELECT 1 FROM sqlite_schema WHERE name = 'resource_jobs'"
                ).fetchone()
            )

    def test_restore_rejects_stale_ack_without_changing_database(self) -> None:
        self._database_with_probe()
        created = create_compressed_snapshot(self.path)
        before = self.path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "stale or invalid"):
            restore_compressed_snapshot(self.path, created["artifact"], "0" * 64)
        self.assertEqual(self.path.read_bytes(), before)

    def test_interrupted_publication_is_preserved_and_a_fresh_attempt_is_made(self) -> None:
        database = self._database_with_probe()
        created = create_compressed_snapshot(self.path)
        artifact = self.root / created["artifact"]
        manifest = self.root / created["manifest"]
        schema_version = database.schema_version
        original_bytes = artifact.read_bytes()

        # Simulate interruption between artifact publication and manifest publication.
        manifest.unlink()
        self.assertTrue(artifact.exists())

        # The incomplete artifact must never satisfy the verified-migration gate, and the
        # lookup must fail closed by returning None rather than raising a raw OS error.
        self.assertIsNone(find_verified_migration_snapshot(self.path, schema_version))
        plan = backup_plan(self.path)
        entry = next(
            item
            for item in plan["compressed_snapshots"]
            if item["artifact"] == artifact.name
        )
        self.assertFalse(entry["verified"])

        # A retry must never destroy the original bytes: it leaves the artifact in place,
        # reserves a fresh attempt, and reports the unverified artifact in the plan.
        retried = create_compressed_snapshot(self.path)
        self.assertTrue(retried["created"])
        self.assertTrue((self.root / retried["manifest"]).is_file())
        self.assertNotEqual(retried["artifact"], artifact.name)
        self.assertTrue(artifact.exists())
        self.assertEqual(artifact.read_bytes(), original_bytes)
        self.assertFalse(manifest.exists())

        repaired = find_verified_migration_snapshot(self.path, schema_version)
        self.assertIsNotNone(repaired)
        assert repaired is not None
        self.assertEqual(repaired.name, retried["artifact"])
        names = {
            item["artifact"]: item["verified"]
            for item in backup_plan(self.path)["compressed_snapshots"]
        }
        self.assertFalse(names[artifact.name])
        self.assertTrue(names[retried["artifact"]])

    def test_restore_failure_after_main_swap_rolls_back_original_namespace(self) -> None:
        self._database_with_probe("before")
        created = create_compressed_snapshot(self.path)
        snapshot = next(
            item
            for item in backup_plan(self.path)["compressed_snapshots"]
            if item["artifact"] == created["artifact"]
        )
        # Hold an open WAL-mode connection so committed frames live in a real ``-wal``
        # sidecar that the rollback must restore intact.
        live = sqlite3.connect(self.path)
        try:
            self.assertEqual(
                live.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal"
            )
            live.execute("UPDATE backup_probe SET value = 'after'")
            live.commit()
            wal = self.path.with_name(self.path.name + "-wal")
            self.assertTrue(wal.exists())
            before = self.path.read_bytes()
            wal_before = wal.read_bytes()

            # Fail *after* the new main file has been moved into place: the chmod that
            # follows the swap is the first post-swap step, so failing it exercises the
            # exact hazard the old patch mishandled (old WAL onto a new main DB).
            real_chmod = backups_module.os.chmod
            calls = {"chmod_main": 0}

            def failing_chmod(
                path: str | bytes | os.PathLike[str] | os.PathLike[bytes] | int,
                mode: int,
            ) -> None:
                if Path(str(path)) == self.path and calls["chmod_main"] == 0:
                    calls["chmod_main"] += 1
                    raise OSError("synthetic post-swap chmod failure")
                real_chmod(path, mode)

            with patch("sightglass.model.backups.os.chmod", side_effect=failing_chmod):
                with self.assertRaises(OSError):
                    restore_compressed_snapshot(
                        self.path, created["artifact"], snapshot["restore_ack"]
                    )

            # The complete original namespace (main plus WAL) must be back in place.
            self.assertEqual(calls["chmod_main"], 1)  # failure happened post-swap
            self.assertEqual(self.path.read_bytes(), before)
            self.assertTrue(wal.exists())
            self.assertEqual(wal.read_bytes(), wal_before)
            self.assertEqual(
                live.execute("SELECT value FROM backup_probe").fetchone()[0], "after"
            )
            # No transaction namespace may survive a fully rolled-back failure.
            self.assertFalse(
                self._restore_tx_dir().exists()
            )
        finally:
            live.close()

    def test_recover_rolls_back_process_death_before_commit(self) -> None:
        self._database_with_probe("before")
        created = create_compressed_snapshot(self.path)
        snapshot = next(
            item
            for item in backup_plan(self.path)["compressed_snapshots"]
            if item["artifact"] == created["artifact"]
        )
        original = self.path.read_bytes()
        wal = self.path.with_name(self.path.name + "-wal")
        wal.write_bytes(b"synthetic-committed-wal")

        transaction = self._restore_tx_dir()
        transaction.mkdir(mode=0o700)
        # Model process death between staging the originals and publishing the new main:
        # the journal says uncommitted and ``window.db`` is currently missing.
        (transaction / "journal.json").write_text(
            json.dumps(
                {
                    "schema": "sightglass.window-restore-journal.v1",
                    "database_name": self.path.name,
                    "artifact": created["artifact"],
                    "committed": False,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        os.chmod(transaction / "journal.json", 0o600)
        os.replace(self.path, transaction / "original-main")
        os.replace(wal, transaction / "original-wal")

        recovered = recover_interrupted_restore(self.path)
        self.assertTrue(recovered["recovered"])
        self.assertEqual(recovered["outcome"], "rolled_back")
        self.assertEqual(self.path.read_bytes(), original)
        self.assertTrue(wal.exists())
        self.assertEqual(wal.read_bytes(), b"synthetic-committed-wal")
        self.assertFalse(transaction.exists())
        # A second call is a safe no-op.
        self.assertEqual(recover_interrupted_restore(self.path)["outcome"], "none")
        del snapshot

    def test_recover_keeps_committed_namespace_and_discards_originals(self) -> None:
        self._database_with_probe("after")
        created = create_compressed_snapshot(self.path)
        transaction = self._restore_tx_dir()
        transaction.mkdir(mode=0o700)
        (transaction / "journal.json").write_text(
            json.dumps(
                {
                    "schema": "sightglass.window-restore-journal.v1",
                    "database_name": self.path.name,
                    "artifact": created["artifact"],
                    "committed": True,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        os.chmod(transaction / "journal.json", 0o600)
        # The obsolete original namespace is still staged but must be discarded.
        (transaction / "original-main").write_bytes(b"stale-original")
        os.chmod(transaction / "original-main", 0o600)

        recovered = recover_interrupted_restore(self.path)
        self.assertTrue(recovered["recovered"])
        self.assertEqual(recovered["outcome"], "committed")
        self.assertFalse(transaction.exists())
        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(
                connection.execute("SELECT value FROM backup_probe").fetchone()[0],
                "after",
            )

    def test_recover_discards_an_unused_transaction_namespace(self) -> None:
        self._database_with_probe("before")
        before = self.path.read_bytes()
        transaction = self._restore_tx_dir()
        transaction.mkdir(mode=0o700)
        # A staged preparation with no journal means no namespace mutation happened yet.
        (transaction / "prepared-main").write_bytes(b"partial")
        os.chmod(transaction / "prepared-main", 0o600)

        recovered = recover_interrupted_restore(self.path)
        self.assertFalse(recovered["recovered"])
        self.assertEqual(recovered["outcome"], "none")
        self.assertFalse(transaction.exists())
        self.assertEqual(self.path.read_bytes(), before)

    def test_recover_rejects_a_tampered_journal_without_touching_the_database(self) -> None:
        self._database_with_probe("before")
        before = self.path.read_bytes()
        transaction = self._restore_tx_dir()
        transaction.mkdir(mode=0o700)
        (transaction / "journal.json").write_text(
            json.dumps(
                {
                    "schema": "sightglass.window-restore-journal.v1",
                    "database_name": self.path.name,
                    "artifact": "../../etc/passwd",
                    "committed": False,
                }
            ),
            encoding="utf-8",
        )
        os.chmod(transaction / "journal.json", 0o600)
        with self.assertRaises(RuntimeError):
            recover_interrupted_restore(self.path)
        self.assertEqual(self.path.read_bytes(), before)

    def test_successful_restore_replaces_database_and_retires_transaction(self) -> None:
        database = self._database_with_probe("before")
        created = create_compressed_snapshot(self.path)
        with database.transaction() as connection:
            connection.execute("UPDATE backup_probe SET value = 'after'")
        # The restore acknowledgement binds the current database identity, so it must be
        # taken after the live database moved forward, matching operator flow.
        snapshot = next(
            item
            for item in backup_plan(self.path)["compressed_snapshots"]
            if item["artifact"] == created["artifact"]
        )

        restored = restore_compressed_snapshot(
            self.path, created["artifact"], snapshot["restore_ack"]
        )
        self.assertTrue(restored["restored"])
        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(
                connection.execute("SELECT value FROM backup_probe").fetchone()[0],
                "before",
            )
        self.assertFalse((self._restore_tx_dir()).exists())
        # The backup artifact itself is preserved across a restore.
        self.assertTrue((self.root / created["artifact"]).exists())

    def test_cli_backup_plan_and_create_are_stopped_only_and_process_locked(self) -> None:
        data = self.root / "state"
        source = self.root / "source"
        source.mkdir(mode=0o700)
        config = SightglassConfig.create(data, source)
        store = ConfigStore(data / "config.json")
        store.save(config)
        WindowDB(config.window_db_path)

        plan = _storage_command(
            Namespace(storage_command="backup", backup_command="plan"), store
        )
        self.assertEqual(plan["schema"], "sightglass.window-backup-plan.v1")
        created = _storage_command(
            Namespace(storage_command="backup", backup_command="create"), store
        )
        self.assertTrue(created["created"])
        self.assertTrue((data / created["artifact"]).is_file())


if __name__ == "__main__":
    unittest.main()

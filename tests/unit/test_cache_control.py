from __future__ import annotations

import hashlib
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from unittest.mock import patch

from sightglass.model.db import WindowDB
from sightglass.model.repositories import WindowRepository
from sightglass.reader.deliveries import DeliveryPayloadStore
from sightglass.resources.cache import ResourceObjectStore
from sightglass.runtime.control import (
    ControlError,
    cache_status,
    cleanup_cache,
    cleanup_deliveries,
    volume_encryption_status,
)
from tests.unit.test_schema_v3_migration import insert_batch, insert_job, seed_voice_parents


class CacheControlTests(unittest.TestCase):
    def test_volume_encryption_is_explicitly_unknown_outside_macos(self) -> None:
        with (
            patch("sightglass.runtime.control.sys.platform", "linux"),
            patch("sightglass.runtime.control.subprocess.run") as command,
        ):
            self.assertEqual(
                volume_encryption_status(self.database.path),
                {"status": "unknown", "encrypted": None},
            )
        command.assert_not_called()

    def test_delivery_cleanup_preserves_pending_and_young_orphans(self) -> None:
        store = DeliveryPayloadStore(self.database.path)
        repository = WindowRepository(self.database)
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO reader_profiles(reader_id,display_name,policy_json,"
                "created_at,updated_at) VALUES ('reader','Synthetic','{}','now','now')"
            )
        references = {}
        for status in ("pending", "acknowledged", "expired"):
            ref, digest = store.write(status, {"synthetic": status})
            references[status] = Path(ref)
            repository.create_pending_delivery(
                delivery_id=status,
                reader_id="reader",
                conversation_id="conv",
                scope_kind="conversation",
                scope_key=status,
                from_observation_seq=1,
                to_observation_seq=1,
                payload_digest=digest,
                payload_ref=ref,
                projection_schema_version="synthetic.v1",
                created_at="now",
            )
            with self.database.transaction() as connection:
                connection.execute(
                    "UPDATE reader_deliveries SET status=? WHERE delivery_id=?", (status, status)
                )
        old, _ = store.write("old-orphan", {"synthetic": "old"})
        young, _ = store.write("young-orphan", {"synthetic": "young"})
        timestamp = time.time() - 25 * 3600
        os.utime(old, (timestamp, timestamp))
        plan = cleanup_deliveries(self.database, apply=False)
        self.assertEqual(plan["candidate_count"], 3)
        self.assertTrue(all(path.exists() for path in references.values()))
        applied = cleanup_cache(self.database, apply=True)["deliveries"]
        self.assertEqual(applied["removed_count"], 3)
        self.assertEqual(applied["pending_preserved"], 1)
        self.assertTrue(references["pending"].exists())
        self.assertTrue(Path(young).exists())
        self.assertFalse(Path(old).exists())
        pending = repository.delivery("pending")
        assert pending is not None
        self.assertEqual(
            store.read(str(pending["payload_ref"]), str(pending["payload_digest"])),
            {"synthetic": "pending"},
        )

    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.database = WindowDB(Path(temporary.name) / "window.db")
        self.root = self.database.path.parent / "resource-cache" / "objects"
        self.root.mkdir(parents=True, mode=0o700)
        self.root.parent.chmod(0o700)
        with self.database.transaction() as connection:
            seed_voice_parents(connection)

    def object(self, letter: str, *, registered: bool = True, old: bool = False) -> Path:
        path = self.root / (letter * 64)
        path.write_bytes(b"synthetic")
        path.chmod(0o600)
        if old:
            timestamp = time.time() - 25 * 60 * 60
            os.utime(path, (timestamp, timestamp))
        if registered:
            with self.database.transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO resource_objects(object_digest, local_path_internal,
                        byte_size, origin, created_at) VALUES (?, ?, 9, 'private_cache', 'now')
                """,
                    (path.name, str(path)),
                )
        return path

    def registered(self, path: Path) -> bool:
        with self.database.connection() as connection:
            return (
                connection.execute(
                    "SELECT 1 FROM resource_objects WHERE object_digest = ?", (path.name,)
                ).fetchone()
                is not None
            )

    def bind(self, connection: sqlite3.Connection, path: Path) -> None:
        connection.execute(
            """
            INSERT INTO resource_bindings(resource_id, object_digest, variant, created_at)
            VALUES ('resource', ?, 'original', 'now')
        """,
            (path.name,),
        )

    def cas_object(self, data: bytes = b"synthetic-voice") -> tuple[bytes, Path]:
        """Register an unbound object whose CAS name is the real digest of ``data``."""

        digest = hashlib.sha256(data).hexdigest()
        path = self.root / digest
        path.write_bytes(data)
        path.chmod(0o600)
        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO resource_objects(object_digest, local_path_internal, mime_type,
                    byte_size, origin, created_at)
                VALUES (?, ?, 'application/json', ?, 'derived', 'now')
            """,
                (digest, str(path), len(data)),
            )
        return data, path

    def register_voice_style(self, data: bytes) -> None:
        """Publish a CAS file and commit the row plus voice reference in one writer lock.

        This mirrors the voice/service.py ``_store``/``insert_object`` path, where the
        object write and the ``resource_objects`` insert both run inside a single
        ``database.transaction()`` and therefore share the writer lock with cleanup.
        """

        with self.database.transaction():
            cached, stored = ResourceObjectStore(self.database.path).put(
                data, mime_type="application/json", origin="derived"
            )
            with self.database.transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO resource_objects(object_digest, local_path_internal, mime_type,
                        byte_size, origin, created_at)
                    VALUES (?, ?, 'application/json', ?, 'derived', 'now')
                """,
                    (cached.digest, stored, len(data)),
                )
                insert_job(connection, job_id="voice-job", input_digest=cached.digest)

    def wait_for_waiting_writer(self) -> int:
        deadline = time.monotonic() + 5
        waiting = 0
        while waiting < 1:
            self.assertLess(time.monotonic(), deadline)
            waiting = int(self.database.writer_status()["waiting_count"])
            if waiting < 1:
                time.sleep(0.005)
        return waiting

    def test_all_five_object_foreign_keys_share_status_dry_run_and_cleanup(self) -> None:
        paths = [self.object(letter) for letter in "abcdef"]
        with self.database.transaction() as connection:
            self.bind(connection, paths[0])
            connection.execute(
                """
                INSERT INTO resource_derivations(derivation_id, resource_id, source_digest,
                    variant, processor_name, processor_version, parameters_json,
                    derived_digest, created_at)
                VALUES ('derivation', 'resource', 'synthetic-source', 'preview',
                    'synthetic-processor', '1', '{}', ?, 'now')
            """,
                (paths[1].name,),
            )
            insert_job(connection, input_digest=paths[2].name, result_digest=paths[3].name)
            insert_batch(connection)
            connection.execute(
                """
                INSERT INTO voice_batch_events(batch_id, kind, result_digest, created_at)
                VALUES ('batch', 'ready', ?, 'now')
            """,
                (paths[4].name,),
            )
        self.assertEqual(
            cache_status(self.database),
            {
                "schema": "sightglass.cache-status.v1",
                "object_count": 6,
                "byte_size": 54,
                "unbound_count": 1,
                "reclaimable_bytes": 9,
            },
        )
        dry = cleanup_cache(self.database, apply=False)
        self.assertEqual(
            (dry["object_count"], dry["reclaimable_bytes"], dry["removed_count"]), (1, 9, 0)
        )
        self.assertTrue(all(path.exists() for path in paths))
        result = cleanup_cache(self.database, apply=True)
        self.assertEqual((result["removed_count"], result["orphan_count"]), (1, 0))
        for path in paths[:5]:
            self.assertTrue(path.exists())
            self.assertTrue(self.registered(path))
        self.assertFalse(paths[5].exists())
        self.assertFalse(self.registered(paths[5]))
        self.assertEqual(cache_status(self.database)["unbound_count"], 0)

    def test_reference_restored_between_selection_and_delete_survives(self) -> None:
        path = self.object("a")
        transaction = self.database.transaction

        @contextmanager
        def restore_then_delete(*, maintenance: bool = False) -> Iterator[sqlite3.Connection]:
            with transaction() as connection:
                self.bind(connection, path)
            with transaction(maintenance=maintenance) as connection:
                yield connection

        with patch.object(self.database, "transaction", restore_then_delete):
            result = cleanup_cache(self.database, apply=True)
        self.assertEqual(result["object_count"], 1)
        self.assertEqual(result["removed_count"], 0)
        self.assertTrue(path.exists())
        self.assertTrue(self.registered(path))

    def test_nested_dry_run_and_apply_are_rejected(self) -> None:
        path = self.object("a")
        for apply in (False, True):
            with self.subTest(apply=apply), self.database.transaction():
                with self.assertRaisesRegex(ControlError, "nested transaction"):
                    cleanup_cache(self.database, apply=apply)
        self.assertTrue(path.exists())
        self.assertTrue(self.registered(path))

    def test_delete_failure_rolls_back_before_any_unlink(self) -> None:
        path = self.object("a")
        with self.database.transaction() as connection:
            connection.execute("""
                CREATE TRIGGER fail_delete BEFORE DELETE ON resource_objects
                BEGIN SELECT RAISE(ABORT, 'synthetic delete failure'); END
            """)
        with self.assertRaisesRegex(sqlite3.IntegrityError, "synthetic delete failure"):
            cleanup_cache(self.database, apply=True)
        self.assertTrue(path.exists())
        self.assertTrue(self.registered(path))

    def test_real_commit_failure_keeps_file_and_rolls_back_row(self) -> None:
        path = self.object("a")
        transaction = self.database.transaction

        @contextmanager
        def fail_commit(*, maintenance: bool = False) -> Iterator[sqlite3.Connection]:
            with transaction(maintenance=maintenance) as connection:
                connection.execute("PRAGMA defer_foreign_keys = ON")
                connection.execute("""
                    INSERT INTO voice_batch_events(batch_id, kind, created_at)
                    VALUES ('absent', 'ready', 'now')
                """)
                yield connection

        with patch.object(self.database, "transaction", fail_commit):
            with self.assertRaises(sqlite3.IntegrityError):
                cleanup_cache(self.database, apply=True)
        self.assertTrue(path.exists())
        self.assertTrue(self.registered(path))
        with self.database.connection() as connection:
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_unlink_fence_serializes_after_durable_delete(self) -> None:
        path = self.object("a")
        unlink = Path.unlink
        called = []

        def checked_unlink(target: Path, missing_ok: bool = False) -> None:
            if target == path:
                # The durable DELETE already committed before the fence; the fence now
                # holds the writer lock so no registration can slip in before unlink.
                self.assertTrue(self.database.writer_status()["active"])
                with closing(sqlite3.connect(self.database.path)) as independent:
                    self.assertIsNone(
                        independent.execute(
                            "SELECT 1 FROM resource_objects WHERE object_digest = ?",
                            (path.name,),
                        ).fetchone()
                    )
                called.append(target)
            unlink(target, missing_ok=missing_ok)

        with patch.object(Path, "unlink", checked_unlink):
            self.assertEqual(cleanup_cache(self.database, apply=True)["removed_count"], 1)
        self.assertEqual(called, [path])

    def test_unlink_failure_leaves_orphan_then_later_sweep_converges(self) -> None:
        path = self.object("a")
        with patch.object(Path, "unlink", side_effect=OSError("synthetic unlink failure")):
            result = cleanup_cache(self.database, apply=True)
        self.assertEqual(
            (result["removed_count"], result["orphan_count"], result["orphan_bytes"]), (0, 1, 9)
        )
        self.assertTrue(path.exists())
        self.assertFalse(self.registered(path))
        self.assertEqual(cleanup_cache(self.database, apply=True)["removed_count"], 0)
        old = time.time() - 25 * 60 * 60
        os.utime(path, (old, old))
        result = cleanup_cache(self.database, apply=True)
        self.assertEqual((result["removed_count"], result["orphan_count"]), (1, 0))
        self.assertFalse(path.exists())

    def test_orphan_grace_and_nonrecursive_no_follow_sweep(self) -> None:
        recent = self.object("a", registered=False)
        old = self.object("b", registered=False, old=True)
        temporary = self.root / "staged.tmp"
        temporary.write_bytes(b"synthetic")
        symlink = self.root / ("c" * 64)
        symlink.symlink_to(temporary)
        hardlink = self.root / ("d" * 64)
        os.link(temporary, hardlink)
        directory = self.root / ("e" * 64)
        directory.mkdir(mode=0o700)
        nested = directory / ("f" * 64)
        nested.write_bytes(b"synthetic")
        dry = cleanup_cache(self.database, apply=False)
        self.assertEqual(
            (dry["orphan_count"], dry["orphan_bytes"], dry["removed_count"]), (2, 18, 0)
        )
        self.assertTrue(old.exists())
        result = cleanup_cache(self.database, apply=True)
        self.assertEqual(
            (result["orphan_count"], result["orphan_bytes"], result["removed_count"]), (1, 9, 1)
        )
        self.assertFalse(old.exists())
        for path in (recent, temporary, symlink, hardlink, nested):
            self.assertTrue(path.exists())

    def test_missing_tracked_file_removes_only_database_row(self) -> None:
        path = self.object("a")
        path.unlink()
        result = cleanup_cache(self.database, apply=True)
        self.assertEqual((result["object_count"], result["removed_count"]), (1, 0))
        self.assertFalse(self.registered(path))

    def test_unsafe_tracked_path_fails_closed(self) -> None:
        path = self.object("a")
        with self.database.transaction() as connection:
            connection.execute(
                "UPDATE resource_objects SET local_path_internal = ?",
                (str(path.parent.parent / path.name),),
            )
        for apply in (False, True):
            with self.subTest(apply=apply), self.assertRaises(ControlError):
                cleanup_cache(self.database, apply=apply)
        self.assertTrue(path.exists())
        self.assertTrue(self.registered(path))

    def test_tracked_symlinks_and_hardlinks_fail_closed(self) -> None:
        path = self.object("a")
        target = self.root / "synthetic-target"
        path.rename(target)
        path.symlink_to(target)
        with self.assertRaises(ControlError):
            cleanup_cache(self.database, apply=True)
        path.unlink()
        os.link(target, path)
        with self.assertRaises(ControlError):
            cleanup_cache(self.database, apply=True)
        self.assertTrue(self.registered(path))
        self.assertTrue(target.exists())

    def test_symlink_object_root_fails_closed(self) -> None:
        path = self.object("a")
        moved = self.root.with_name("moved-objects")
        self.root.rename(moved)
        self.root.symlink_to(moved, target_is_directory=True)
        with self.assertRaises(ControlError):
            cleanup_cache(self.database, apply=True)
        self.assertTrue(self.registered(path))
        self.assertTrue((moved / path.name).exists())

    def test_registration_winning_before_fence_keeps_file_and_reference(self) -> None:
        data, path = self.cas_object()
        transaction = self.database.transaction
        delete_committed = threading.Event()
        allow_cleanup = threading.Event()
        paused_once = False

        @contextmanager
        def paused(*, maintenance: bool = False) -> Iterator[sqlite3.Connection]:
            nonlocal paused_once
            with transaction(maintenance=maintenance) as connection:
                yield connection
            if maintenance and not paused_once:
                # The first maintenance transaction is the already-committed row DELETE.
                # Hold cleanup before its unlink fence so a concurrent same-digest
                # registration can commit first and win the lock.
                paused_once = True
                delete_committed.set()
                self.assertTrue(allow_cleanup.wait(5))

        errors: list[BaseException] = []

        def registrar() -> None:
            self.assertTrue(delete_committed.wait(5))
            try:
                self.register_voice_style(data)
            except BaseException as exc:  # surfaced through ``errors``
                errors.append(exc)
            finally:
                allow_cleanup.set()

        thread = threading.Thread(target=registrar)
        try:
            with patch.object(self.database, "transaction", paused):
                thread.start()
                result = cleanup_cache(self.database, apply=True)
        finally:
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(result["removed_count"], 0)
        self.assertTrue(path.exists())
        self.assertTrue(self.registered(path))
        with self.database.connection() as connection:
            self.assertIsNotNone(
                connection.execute(
                    "SELECT 1 FROM voice_jobs WHERE input_digest = ?",
                    (path.name,),
                ).fetchone()
            )

    def test_fence_unlink_blocks_registration_then_registrar_recreates(self) -> None:
        data, path = self.cas_object()
        unlink = Path.unlink
        errors: list[BaseException] = []
        registrars: list[threading.Thread] = []

        def registrar() -> None:
            try:
                self.register_voice_style(data)
            except BaseException as exc:  # surfaced through ``errors``
                errors.append(exc)

        def checked_unlink(target: Path, missing_ok: bool = False) -> None:
            if target == path:
                # The final fence already holds the writer lock here, so the registrar
                # cannot run during selection or the DELETE. Start it only now and wait
                # until it is genuinely parked on the lock before the unlink removes the
                # old inode.
                thread = threading.Thread(target=registrar)
                registrars.append(thread)
                thread.start()
                self.assertGreaterEqual(self.wait_for_waiting_writer(), 1)
            unlink(target, missing_ok=missing_ok)

        try:
            with patch.object(Path, "unlink", checked_unlink):
                result = cleanup_cache(self.database, apply=True)
        finally:
            for thread in registrars:
                thread.join(5)
        for thread in registrars:
            self.assertFalse(thread.is_alive())
        self.assertEqual(len(registrars), 1)
        self.assertEqual(errors, [])
        self.assertEqual(result["removed_count"], 1)
        # The registrar's CAS put ran after the fence released and re-created the file.
        self.assertTrue(path.exists())
        self.assertTrue(self.registered(path))
        with self.database.connection() as connection:
            self.assertIsNotNone(
                connection.execute(
                    "SELECT 1 FROM voice_jobs WHERE input_digest = ?",
                    (path.name,),
                ).fetchone()
            )

    def test_fresh_reopen_reconciles_missing_tracked_file(self) -> None:
        path = self.object("a")
        path.unlink()
        reopened = WindowDB(self.database.path)
        result = cleanup_cache(reopened, apply=True)
        self.assertEqual((result["object_count"], result["removed_count"]), (1, 0))
        with reopened.connection() as connection:
            self.assertIsNone(
                connection.execute(
                    "SELECT 1 FROM resource_objects WHERE object_digest = ?",
                    (path.name,),
                ).fetchone()
            )

    def test_fresh_reopen_orphan_grace_recovers_after_unlink_failure(self) -> None:
        path = self.object("a")
        with patch.object(Path, "unlink", side_effect=OSError("synthetic unlink failure")):
            failed = cleanup_cache(self.database, apply=True)
        self.assertEqual((failed["removed_count"], failed["orphan_count"]), (0, 1))
        self.assertTrue(path.exists())
        reopened = WindowDB(self.database.path)
        with reopened.connection() as connection:
            self.assertIsNone(
                connection.execute(
                    "SELECT 1 FROM resource_objects WHERE object_digest = ?",
                    (path.name,),
                ).fetchone()
            )
        self.assertEqual(cleanup_cache(reopened, apply=True)["removed_count"], 0)
        old = time.time() - 25 * 60 * 60
        os.utime(path, (old, old))
        recovered = cleanup_cache(reopened, apply=True)
        self.assertEqual((recovered["removed_count"], recovered["orphan_count"]), (1, 0))
        self.assertFalse(path.exists())


if __name__ == "__main__":
    unittest.main()

"""Explicit legacy publication recovery, entirely generated CAS/SQLite fixtures."""

from __future__ import annotations

import errno
import fcntl
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from sightglass.cli import _legacy_cache_links, _parser
from sightglass.contracts.errors import SightglassError
from sightglass.model.cache_recovery import frozen_object_bindings
from sightglass.resources.cache import ResourceObjectStore


class LegacyCASRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "window.db"
        self.store = ResourceObjectStore(self.db)
        self.data = b"synthetic legacy double-link publication"
        self.digest = self.store.digest(self.data)
        self.object = self.store.objects / self.digest
        self.temporary = self.store.tmp / "object-legacy"
        self.temporary.write_bytes(self.data)
        self.temporary.chmod(0o600)
        os.link(self.temporary, self.object)
        self.binding = lambda digest: (str(self.object), len(self.data))

    def preview(self):
        return self.store.legacy_links(self.binding)["objects"][0]

    def test_preview_is_read_only_repair_preserves_inode_binding_and_bytes(self):
        before = self.object.stat()
        with self.assertRaises(SightglassError):
            self.store.put(self.data, mime_type="text/plain", origin="synthetic")
        # Startup/ordinary put must never repair old stock implicitly.
        self.assertTrue(self.temporary.exists())
        row = self.preview()
        self.assertEqual(row["status"], "repairable")
        self.assertEqual(self.object.stat().st_nlink, 2)
        repaired = self.store.legacy_links(self.binding, digest=self.digest, expect=row["expect"])
        self.assertEqual(repaired["objects"][0]["status"], "repaired")
        self.assertFalse(self.temporary.exists())
        self.assertEqual(self.object.stat().st_ino, before.st_ino)
        self.assertEqual(self.object.stat().st_nlink, 1)
        self.assertEqual(
            self.store.put(self.data, mime_type="text/plain", origin="synthetic")[0].data, self.data
        )

    def test_extra_or_only_external_hardlink_and_symlink_refuse(self):
        external = self.root / "external"
        os.link(self.object, external)
        self.assertEqual(self.preview()["status"], "blocked")
        self.temporary.unlink()
        self.assertEqual(self.preview()["status"], "blocked")
        external.unlink()
        self.object.unlink()
        self.object.symlink_to(self.root / "missing")
        self.assertEqual(self.preview()["status"], "blocked")

    def test_foreign_owner_size_digest_and_binding_mismatch_refuse(self):
        for binding in (
            lambda _: None,
            lambda _: (str(self.object), 1),
            lambda _: ("outside", len(self.data)),
        ):
            self.assertEqual(self.store.legacy_links(binding)["objects"][0]["status"], "blocked")
        # Same inode, wrong digest, and unchanged size must still fail content proof.
        self.temporary.write_bytes(b"x" * len(self.data))
        self.assertEqual(self.preview()["reason"], "object_proof_mismatch")
        self.assertTrue(self.temporary.exists())
        with patch("sightglass.resources.cache.os.getuid", return_value=os.getuid() + 1):
            with self.assertRaises(RuntimeError):
                self.preview()

    def test_replaced_inode_or_changed_binding_after_preview_is_rejected(self):
        row = self.preview()
        self.temporary.unlink()
        self.object.unlink()
        self.temporary.write_bytes(self.data)
        self.temporary.chmod(0o600)
        os.link(self.temporary, self.object)
        with self.assertRaises(RuntimeError):
            self.store.legacy_links(self.binding, digest=self.digest, expect=row["expect"])
        with self.assertRaises(RuntimeError):
            self.store.legacy_links(
                lambda _: None, digest=self.digest, expect=self.preview()["expect"]
            )
        self.assertTrue(self.temporary.exists())

    def test_directory_lock_is_held_and_failed_fsync_leaves_readable_object(self):
        row = self.preview()

        def bound(digest):
            fd = os.open(self.store.objects, os.O_RDONLY | os.O_DIRECTORY)
            try:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(fd)
            return self.binding(digest)

        with patch(
            "sightglass.resources.cache.os.fsync", side_effect=OSError(errno.ENOSPC, "synthetic")
        ):
            with self.assertRaises(OSError):
                self.store.legacy_links(bound, digest=self.digest, expect=row["expect"])
        self.assertEqual(self.object.stat().st_nlink, 1)
        self.assertEqual(
            self.store.put(self.data, mime_type="text/plain", origin="synthetic")[0].data, self.data
        )

    def seed_db(self):
        with closing(sqlite3.connect(self.db)) as connection, connection:
            connection.executescript("""
CREATE TABLE resource_objects(
  object_digest TEXT PRIMARY KEY, local_path_internal TEXT, byte_size INT);
CREATE TABLE resources(resource_id TEXT PRIMARY KEY);
CREATE TABLE resource_bindings(resource_id TEXT, object_digest TEXT);
CREATE TABLE resource_derivations(resource_id TEXT, derived_digest TEXT);
""")
            connection.execute(
                "INSERT INTO resource_objects VALUES(?,?,?)",
                (self.digest, str(self.object), len(self.data)),
            )

    def test_frozen_model_read_view_and_dangling_binding_rejection(self):
        self.seed_db()
        with frozen_object_bindings(self.db) as binding:
            self.assertEqual(binding(self.digest), self.binding(self.digest))
            self.assertIsNone(binding("missing"))
        with closing(sqlite3.connect(self.db)) as connection, connection:
            connection.execute("INSERT INTO resource_bindings VALUES('missing',?)", (self.digest,))
        with frozen_object_bindings(self.db) as binding:
            with self.assertRaises(RuntimeError):
                binding(self.digest)

    def test_cli_exact_preview_apply_and_stopped_gate(self):
        self.seed_db()
        store = Mock()
        store.load.return_value = SimpleNamespace(
            window_db_path=self.db, socket_path=self.root / "runtime" / "daemon.sock"
        )
        store.load.return_value.socket_path.parent.mkdir(mode=0o700)
        preview = _parser().parse_args(["cache", "legacy-links", "preview"])
        with patch("sightglass.cli._daemon_must_be_stopped") as stopped:
            result = _legacy_cache_links(preview, store)
            stopped.assert_called_once()
            repair = _parser().parse_args(
                [
                    "cache",
                    "legacy-links",
                    "repair",
                    "--digest",
                    self.digest,
                    "--expect",
                    result["objects"][0]["expect"],
                ]
            )
            result = _legacy_cache_links(repair, store)
            self.assertEqual(result["objects"][0]["status"], "repaired")
        invalid = _parser().parse_args(["cache", "legacy-links", "repair"])
        with self.assertRaises(RuntimeError):
            _legacy_cache_links(invalid, store)
        with patch("sightglass.cli._daemon_must_be_stopped", side_effect=RuntimeError("running")):
            with self.assertRaises(RuntimeError):
                _legacy_cache_links(preview, store)

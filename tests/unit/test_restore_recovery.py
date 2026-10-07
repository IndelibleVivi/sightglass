from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from sightglass.model.backups import create_compressed_snapshot, recover_interrupted_restore
from sightglass.model.db import WindowDB


class RestoreRecoveryTests(unittest.TestCase):
    def test_process_death_before_and_after_commit_recovers_before_sqlite_open(self):
        for phase, expected in (("uncommitted", "after"), ("committed", "before")):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "window.db"
                database = WindowDB(path)
                with database.transaction() as connection:
                    connection.execute("CREATE TABLE synthetic_restore(value TEXT)")
                    connection.execute("INSERT INTO synthetic_restore VALUES ('before')")
                create_compressed_snapshot(path)
                with database.transaction() as connection:
                    connection.execute("UPDATE synthetic_restore SET value='after'")
                code = """
import os, sys
from pathlib import Path
from sightglass.model import backups
path = Path(sys.argv[1])
phase = sys.argv[2]
write = backups._write_restore_journal
def crash(path, *, artifact_name, committed):
    if committed and phase == 'uncommitted':
        os._exit(71)
    write(path, artifact_name=artifact_name, committed=committed)
    if committed:
        os._exit(71)
backups._write_restore_journal = crash
selected = backups.backup_plan(path)['compressed_snapshots'][0]
backups.restore_compressed_snapshot(path, selected['artifact'], selected['restore_ack'])
"""
                completed = subprocess.run(
                    [sys.executable, "-c", code, str(path), phase],
                    capture_output=True,
                    check=False,
                    timeout=20,
                )
                self.assertEqual(completed.returncode, 71, completed.stderr.decode())
                reopened = WindowDB(path)
                with reopened.connection() as connection:
                    self.assertEqual(
                        connection.execute("SELECT value FROM synthetic_restore").fetchone()[0],
                        expected,
                    )
                self.assertFalse(path.with_name(".window.db.restore-tx").exists())

    def test_unknown_namespace_and_symlink_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "window.db"
            WindowDB(path)
            before = path.read_bytes()
            transaction = root / ".window.db.restore-tx"
            transaction.mkdir(mode=0o700)
            unknown = transaction / "unrecognized-user-file"
            unknown.write_bytes(b"synthetic-owned-by-another-operation")
            unknown.chmod(0o600)
            with self.assertRaises(RuntimeError):
                recover_interrupted_restore(path)
            self.assertTrue(unknown.exists())
            self.assertEqual(path.read_bytes(), before)
            other = root / "other-private"
            other.mkdir(mode=0o700)
            other_db = other / "window.db"
            WindowDB(other_db)
            linked = root / "linked.db"
            linked.write_bytes(b"synthetic")
            linked.chmod(0o600)
            os.symlink(other, root / ".linked.db.restore-tx")
            with self.assertRaises(RuntimeError):
                recover_interrupted_restore(linked)
            self.assertEqual(linked.read_bytes(), b"synthetic")

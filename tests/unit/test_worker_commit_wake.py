from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from sightglass.model.db import WindowDB


class WorkerCommitWakeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.database = WindowDB(Path(self.temporary.name) / "window.db")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_nested_writer_notifies_once_after_commit_and_writer_release(self) -> None:
        observed: list[tuple[bool, int]] = []

        def wake():
            with self.database.connection() as connection:
                value = int(connection.execute("PRAGMA user_version").fetchone()[0])
            observed.append((self.database.writer_status()["active"], value))

        with self.database.transaction() as connection:
            connection.execute("PRAGMA user_version=123")
            self.database.wake_after_commit(wake)
            with self.database.transaction():
                self.database.wake_after_commit(wake)
            self.assertEqual(observed, [])
        self.assertEqual(observed, [(False, 123)])

    def test_outer_rollback_discards_nested_notifications(self) -> None:
        wake = Mock()
        with self.assertRaises(ValueError):
            with self.database.transaction():
                with self.database.transaction():
                    self.database.wake_after_commit(wake)
                raise ValueError("synthetic rollback")
        wake.assert_not_called()
        with self.database.transaction():
            pass
        wake.assert_not_called()

    def test_outside_writer_and_read_snapshot_notify_immediately(self) -> None:
        wake = Mock()
        self.database.wake_after_commit(wake)
        with self.database.read_snapshot():
            self.database.wake_after_commit(wake)
            self.assertEqual(wake.call_count, 2)

    def test_notification_failure_cannot_misreport_a_committed_transaction(self) -> None:
        wake = Mock(side_effect=RuntimeError("synthetic wake unavailable"))
        with self.database.transaction() as connection:
            connection.execute("PRAGMA user_version=124")
            self.database.wake_after_commit(wake)
        with self.database.connection() as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 124)
        self.database.wake_after_commit(wake)  # best effort outside a writer as well

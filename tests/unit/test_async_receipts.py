from __future__ import annotations

import queue
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from sightglass.runtime.config import SightglassConfig
from sightglass.runtime.receipts import AsyncReceiptWriter
from sightglass.runtime.service import build_daemon_tools
from sightglass.source.synthetic import create_synthetic_source


class AsyncReceiptWriterTests(unittest.TestCase):
    def test_stop_drains_receipt_accepted_after_empty_read(self) -> None:
        empty_read, release = threading.Event(), threading.Event()
        persisted: list[int] = []
        writer = AsyncReceiptWriter(lambda *, sequence: persisted.append(sequence), autostart=False)
        self.addCleanup(writer.close)
        self.addCleanup(release.set)
        real_get = writer._queue.get

        def delayed_empty(*args, **kwargs):
            if not empty_read.is_set():
                try:
                    return real_get(block=False)
                except queue.Empty:
                    empty_read.set()
                    if not release.wait(2):
                        raise RuntimeError("test did not release empty receipt read") from None
                    raise
            return real_get(*args, **kwargs)

        with patch.object(writer._queue, "get", side_effect=delayed_empty):
            writer.start()
            self.assertTrue(empty_read.wait(1))
            writer(sequence=1)
            with self.assertRaisesRegex(RuntimeError, "did not stop"):
                writer.stop(timeout=0)
            release.set()
            writer.close(timeout=2)

        self.assertEqual(persisted, [1])
        self.assertEqual(writer.status()["pending_count"], 0)
        self.assertEqual(writer.status()["persisted_count"], 1)

    def test_queue_is_bounded_nonblocking_and_drains_on_close(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        persisted: list[int] = []

        def persist(*, sequence: int) -> None:
            if sequence == 1:
                entered.set()
                if not release.wait(timeout=2):
                    raise RuntimeError("test did not release receipt persistence")
            persisted.append(sequence)

        writer = AsyncReceiptWriter(persist, capacity=1)
        writer(sequence=1)
        self.assertTrue(entered.wait(timeout=1))
        writer(sequence=2)
        writer(sequence=3)

        blocked = writer.status()
        self.assertTrue(blocked["active"])
        self.assertEqual(blocked["pending_count"], 1)
        self.assertEqual(blocked["dropped_count"], 1)

        release.set()
        writer.close(timeout=2)

        final = writer.status()
        self.assertEqual(persisted, [1, 2])
        self.assertEqual(final["persisted_count"], 2)
        self.assertEqual(final["dropped_count"], 1)
        self.assertFalse(final["alive"])

    def test_failed_close_rejoins_same_owner_and_never_restarts_live_writer(self) -> None:
        entered, release = threading.Event(), threading.Event()
        persisted = []

        def persist(*, sequence):
            if sequence == 1:
                entered.set()
                release.wait()
            persisted.append(sequence)

        writer = AsyncReceiptWriter(persist, capacity=2)
        self.addCleanup(writer.close)
        self.addCleanup(release.set)
        writer(sequence=1)
        self.assertTrue(entered.wait(1))
        writer(sequence=2)
        owner = writer._thread
        for _ in range(2):
            with self.assertRaisesRegex(RuntimeError, "did not stop"):
                writer.close(timeout=0.01)
            self.assertIs(writer._thread, owner)
            self.assertTrue(writer.status()["alive"])
            self.assertEqual(writer.status()["pending_count"], 1)
            with self.assertRaisesRegex(RuntimeError, "did not stop"):
                writer.start()
        release.set()
        writer.close(timeout=2)
        self.assertEqual(persisted, [1, 2])
        self.assertFalse(writer.status()["alive"])
        self.assertEqual(writer.status()["persisted_count"], 2)
        writer.close(timeout=0)  # Confirmed close remains idempotent.
        with self.assertRaisesRegex(RuntimeError, "closed"):
            writer.start()

    def test_dormant_and_stopped_calls_are_counted_then_start_resumes_one_owner(self) -> None:
        persisted = []
        writer = AsyncReceiptWriter(lambda **values: persisted.append(values), autostart=False)
        self.addCleanup(writer.close)
        writer(sequence=0)  # A status call may reach a not-yet-started daemon generation.
        self.assertEqual(persisted, [])
        self.assertIsNone(writer._thread)
        self.assertEqual(writer.status()["dropped_count"], 1)
        self.assertEqual(writer.status()["pending_count"], 0)
        writer.start()
        owner = writer._thread
        writer.start()
        self.assertIs(writer._thread, owner)
        writer(sequence=1)
        writer.stop(timeout=2)
        writer(sequence=2)
        self.assertEqual(writer.status()["dropped_count"], 2)
        self.assertEqual(persisted, [{"sequence": 1}])
        writer.start()
        self.assertIsNot(writer._thread, owner)
        self.assertFalse(owner is not None and owner.is_alive())
        writer(sequence=3)
        writer.close(timeout=2)
        self.assertEqual(persisted, [{"sequence": 1}, {"sequence": 3}])
        self.assertEqual(writer.status()["persisted_count"], 2)

    def test_persist_failure_retains_counters_and_drains_later_receipts(self) -> None:
        def persist(*, sequence):
            if sequence == 1:
                raise ValueError("synthetic failure")

        writer = AsyncReceiptWriter(persist)
        writer(sequence=1)
        writer(sequence=2)
        writer.close(timeout=2)
        status = writer.status()
        self.assertEqual(status["failed_count"], 1)
        self.assertEqual(status["persisted_count"], 1)
        self.assertEqual(status["pending_count"], 0)
        self.assertFalse(status["alive"])

    def test_direct_tools_build_autostarts_and_persists_ordinary_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = create_synthetic_source(root / "source")
            tools = build_daemon_tools(SightglassConfig.create(root / "state", source))
            try:
                status = tools.receipt_writer_status()
                assert status is not None
                self.assertTrue(status["alive"])
                result = tools.wechat_find_conversations(query="Synthetic")
                self.assertIn("candidates", result)
            finally:
                tools.close()
            status = tools.receipt_writer_status()
            assert status is not None
            self.assertEqual(status["persisted_count"], 1)
            with tools.service.repository.database.connection() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM access_receipts").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()

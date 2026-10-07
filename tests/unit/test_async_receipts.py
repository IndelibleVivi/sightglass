from __future__ import annotations

import threading
import unittest

from sightglass.runtime.receipts import AsyncReceiptWriter


class AsyncReceiptWriterTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()

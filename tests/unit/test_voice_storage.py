from __future__ import annotations

import unittest
from dataclasses import replace
from unittest.mock import Mock, patch

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.runtime.voice_worker import VoiceWorker
from sightglass.storage import MIB, StorageBudget, StorageSettings
from tests.unit.test_voice_service import VoiceFixture


class VoiceStorageTests(VoiceFixture, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.settings = StorageSettings(32 * MIB, 64 * MIB, 0, 8 * MIB)
        self.storage = StorageBudget(self.path.parent, self.path, self.settings)
        self.db.storage = self.storage
        self.service.object_store.storage = self.storage
        free = patch("sightglass.storage._free", return_value=1024 * MIB)
        free.start()
        self.addCleanup(free.stop)

    def pressure(self) -> None:
        self.storage.settings = replace(self.settings, soft_limit_bytes=1)

    def test_soft_pressure_keeps_existing_events_without_new_batch_step_or_worker_lease(
        self,
    ) -> None:
        selection = self.selection(30)
        receipt = self.create(selection)
        token = self.token(receipt)
        first = self.poll(token)
        before = self.jobs()
        self.pressure()
        rejected = self.create(selection, recipe="synthetic-other-recipe")
        self.assertEqual(rejected.reason, "storage_pressure")
        self.assertIsNone(rejected.reading_token)
        self.assertEqual(rejected.coverage.not_scheduled, 30)
        self.assertEqual(self.poll(token).items, first.items)
        self.poll(token, first.next_cursor)
        self.assertEqual(self.jobs(), before)
        transcriber = Mock()
        worker = VoiceWorker(self.service, transcriber)
        with self.assertRaises(SightglassError) as caught:
            worker._work_one_pending()
        self.assertEqual(caught.exception.code, ErrorCode.STORAGE_PRESSURE)
        transcriber.transcribe.assert_not_called()
        self.assertEqual(self.jobs(), before)
        self.storage.settings = self.settings
        self.poll(token, first.next_cursor)
        self.assertGreater(len(self.jobs()), len(before))

    def test_pressure_during_capture_returns_lease_to_pending_and_recovers(self) -> None:
        self.create()
        job = self.jobs()[0]
        transcriber = Mock()

        def fill_storage(*args, **kwargs):
            used = self.storage.status()["accounted_bytes"]
            self.storage.settings = StorageSettings(1, used + 1, 0, 8 * MIB)
            raise SightglassError(ErrorCode.STORAGE_PRESSURE, retryable=True)

        transcriber.transcribe.side_effect = fill_storage
        worker = VoiceWorker(self.service, transcriber)
        worker._drive_job(job)
        self.assertEqual(self.jobs()[0]["state"], "pending")
        self.assertEqual(self.jobs()[0]["attempt"], 0)
        self.assertEqual(worker.status().failed_count, 0)
        self.storage.settings = self.settings
        self.assertFalse(worker._work_one_pending())
        self.assertEqual(transcriber.transcribe.call_count, 1)
        transcriber.transcribe.side_effect = None
        transcriber.transcribe.return_value = "Synthetic recovered transcript"
        worker._drive_job(self.jobs()[0])
        self.assertEqual(self.jobs()[0]["state"], "ready")
        self.assertEqual(worker.status().completed_count, 1)


if __name__ == "__main__":
    unittest.main()

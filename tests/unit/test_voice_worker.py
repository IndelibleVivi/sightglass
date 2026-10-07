from __future__ import annotations

import threading
import time
import unittest
from datetime import UTC, datetime, timedelta
from unittest import mock

from sightglass.contracts.voice import VoiceTranscriptPage
from sightglass.operations import wait_for_event
from sightglass.runtime import voice_worker
from sightglass.runtime.voice_worker import VoiceWorker
from tests.unit.test_voice_service import VoiceFixture

POLL_TIMEOUT_SECONDS = 5.0


class FakeTranscriber:
    """Test-only recognizer.  ``wait`` blocks cooperatively on a gate event."""

    def __init__(self, gate: threading.Event, *, text: str = "Synthetic transcript",
                 behavior: str = "wait") -> None:
        self.gate = gate
        self.text = text
        self.behavior = behavior
        self.release = threading.Event()
        self.entered = threading.Event()
        self.calls: list[tuple[str, int]] = []

    def transcribe(
        self, job: dict, *, duration_ms: int, deadline: float | None,
    ) -> str:
        self.calls.append((str(job["job_id"]), duration_ms))
        self.entered.set()
        if self.behavior == "raise":
            raise ValueError("synthetic recognizer failure")
        if self.behavior == "hang":
            while not self.release.wait(timeout=0.05):
                pass
            return self.text
        wait_for_event(self.gate)
        return self.text


class WorkerFixture(VoiceFixture, unittest.TestCase):
    def wait_until(self, predicate, *, timeout: float = POLL_TIMEOUT_SECONDS) -> None:
        limit = time.monotonic() + timeout
        while time.monotonic() < limit:
            if predicate():
                return
            time.sleep(0.02)
        self.fail("condition not reached before timeout")

    def job_state(self) -> str:
        return str(self.jobs()[0]["state"])

    def drain(self, token: str) -> tuple[VoiceTranscriptPage, VoiceTranscriptPage]:
        """Follow the event cursor to the end; return (last non-empty page, final page)."""
        cursor: str | None = None
        last = self.poll(token)
        for _ in range(32):
            page = self.poll(token, cursor)
            if page.items:
                last = page
            elif page.next_cursor == cursor:
                return last, page
            cursor = page.next_cursor
        self.fail("event stream did not terminate")


class VoiceWorkerTests(WorkerFixture, unittest.TestCase):
    def test_disabled_worker_never_starts(self) -> None:
        worker = VoiceWorker(self.service, None)
        worker.start()
        worker.stop()
        status = worker.status()
        self.assertFalse(status.enabled)
        self.assertFalse(status.running)
        self.assertEqual(status.completed_count, 0)
        self.assertEqual(self.jobs(), [])

    def test_empty_recovery_and_idle_worker_never_enter_writer_or_storage(self) -> None:
        fake = FakeTranscriber(threading.Event())
        worker = VoiceWorker(self.service, fake)
        storage = mock.Mock()
        self.db.storage = storage
        with mock.patch.object(self.db, "transaction", wraps=self.db.transaction) as writer:
            self.assertEqual(self.service.recover_outstanding_leases(), 0)
            worker.start()
            try:
                self.wait_until(lambda: worker.status().idle_cycle_count == 1)
                time.sleep(0.15)
                self.assertEqual(worker.status().idle_cycle_count, 1)
                writer.assert_not_called()
                storage.require.assert_not_called()
            finally:
                started = time.monotonic()
                worker.stop()
                self.assertLess(time.monotonic() - started, 0.5)
        self.db.storage = None

    def test_committed_voice_work_wakes_idle_worker_and_clears_current_error(self) -> None:
        gate = threading.Event()
        gate.set()
        worker = VoiceWorker(self.service, FakeTranscriber(gate))
        self.service.set_worker_wake(worker.wake)
        worker._record_error(ValueError("private synthetic sentinel"))
        worker.start()
        try:
            self.wait_until(lambda: worker.status().idle_cycle_count == 1)
            self.create()
            self.wait_until(lambda: worker.status().completed_count == 1, timeout=1)
            status = worker.status()
            self.assertIsNone(status.last_error_code)
            self.assertEqual(status.historical_error_type, "ValueError")
            self.assertEqual(status.historical_error_count, 1)
            self.assertNotIn("private synthetic sentinel", str(status.as_dict()))
        finally:
            worker.stop()

    def test_backoff_head_does_not_hide_later_pending_voice_job(self) -> None:
        receipt = self.create(self.selection(12))
        first = self.poll(self.token(receipt))
        self.poll(self.token(receipt), first.next_cursor)
        pending = self.repo.pending_jobs(12)
        self.assertGreater(len(pending), 8)
        gate = threading.Event()
        gate.set()
        worker = VoiceWorker(self.service, FakeTranscriber(gate))
        worker._retry_at = {str(job["job_id"]): time.monotonic() + 5 for job in pending[:8]}
        self.assertTrue(worker._work_one_pending())
        self.assertEqual(self.repo.job(str(pending[8]["job_id"]))["state"], "ready")  # type: ignore[index]

    def test_voice_held_lease_recovers_at_deadline_before_idle_fallback(self) -> None:
        self.now = datetime.now(UTC)
        self.service.clock = lambda: datetime.now(UTC)
        self.create()
        job_id = str(self.jobs()[0]["job_id"])
        self.service.lease(job_id, owner_id="synthetic-old", lease_seconds=1)
        gate = threading.Event()
        gate.set()
        worker = VoiceWorker(self.service, FakeTranscriber(gate))
        worker.start()
        try:
            self.wait_until(lambda: worker.status().completed_count == 1, timeout=2)
            self.assertEqual(worker.status().recovered_count, 1)
            self.assertEqual(self.job_state(), "ready")
        finally:
            worker.stop()

    def test_voice_recovery_rechecks_due_rows_inside_writer(self) -> None:
        self.create()
        self.service.lease(str(self.jobs()[0]["job_id"]), owner_id="synthetic-old", lease_seconds=1)
        self.now += timedelta(seconds=2)
        expired = self.repo.expired_leases(self.now.isoformat(timespec="microseconds"))
        with mock.patch.object(self.repo, "expired_leases", side_effect=[expired, []]):
            with mock.patch.object(self.repo, "update_job") as update:
                self.assertEqual(self.service.recover_expired_leases(), 0)
        update.assert_not_called()

    def test_normal_completion_marks_batch_ready(self) -> None:
        gate = threading.Event()
        fake = FakeTranscriber(gate)
        receipt = self.create()
        worker = VoiceWorker(self.service, fake, poll_interval_seconds=0.05)
        worker.start()
        try:
            gate.set()
            token = self.token(receipt)
            self.wait_until(lambda: self.poll(token).processing_complete)
            page, tail = self.drain(token)
            self.assertEqual(page.items[0].state, "ready")
            self.assertEqual(page.coverage.ready, 1)
            self.assertEqual(page.items[0].text, "Synthetic transcript")
            self.assertTrue(page.text_coverage_complete)
            self.assertTrue(tail.processing_complete)
            self.wait_until(lambda: worker.status().completed_count == 1)
            self.assertEqual(fake.calls, [(self.jobs()[0]["job_id"], 10000)])
            self.assertEqual(self.job_state(), "ready")
        finally:
            worker.stop()

    def test_transcriber_exception_fails_job(self) -> None:
        gate = threading.Event()
        fake = FakeTranscriber(gate, behavior="raise")
        receipt = self.create()
        worker = VoiceWorker(self.service, fake, poll_interval_seconds=0.05)
        worker.start()
        try:
            token = self.token(receipt)
            self.wait_until(lambda: self.poll(token).coverage.failed == 1)
            self.assertEqual(self.job_state(), "failed")
            self.assertEqual(worker.status().historical_error_code, "TRANSCRIBE_FAILED")
            self.assertEqual(worker.status().historical_error_type, "ValueError")
            self.assertEqual(worker.status().failed_count, 1)
        finally:
            worker.stop()

    def test_transcribe_deadline_times_out(self) -> None:
        gate = threading.Event()
        fake = FakeTranscriber(gate)
        receipt = self.create()
        worker = VoiceWorker(self.service, fake, poll_interval_seconds=0.05,
                             transcribe_timeout_seconds=0.2)
        worker.start()
        try:
            token = self.token(receipt)
            self.wait_until(lambda: self.poll(token).coverage.failed == 1)
            self.assertEqual(self.job_state(), "failed")
            self.assertEqual(worker.status().historical_error_code, "TRANSCRIBE_TIMEOUT")
            gate.set()
        finally:
            worker.stop()

    def test_cooperative_stop_leaves_job_leased(self) -> None:
        gate = threading.Event()
        fake = FakeTranscriber(gate)
        self.create()
        worker = VoiceWorker(self.service, fake, poll_interval_seconds=0.05)
        worker.start()
        self.wait_until(lambda: self.job_state() in {"leased", "running"})
        worker.stop()
        self.assertFalse(worker.status().running)
        self.assertEqual(self.job_state(), "leased")
        self.assertEqual(worker.status().completed_count, 0)
        gate.set()

    def test_noncooperative_hang_survives_bounded_stop(self) -> None:
        fake = FakeTranscriber(threading.Event(), behavior="hang")
        self.create()
        worker = VoiceWorker(self.service, fake, poll_interval_seconds=0.05)
        worker.start()
        self.wait_until(lambda: self.job_state() in {"leased", "running"})
        with mock.patch.object(voice_worker, "VOICE_WORKER_STOP_TIMEOUT_SECONDS", 0.3):
            stopped_at = time.monotonic()
            worker.stop()
            self.assertLess(time.monotonic() - stopped_at, 2.0)
        self.assertTrue(worker.status().running)
        thread = worker._thread
        self.assertIsNotNone(thread)
        assert thread is not None
        self.assertTrue(thread.is_alive())
        fake.release.set()
        thread.join(timeout=POLL_TIMEOUT_SECONDS)

    def test_lost_lease_fence_blocks_late_result_then_recovery(self) -> None:
        gate = threading.Event()
        fake = FakeTranscriber(gate)
        receipt = self.create()
        worker = VoiceWorker(self.service, fake, poll_interval_seconds=0.05,
                             lease_seconds=60)
        worker.start()
        try:
            self.wait_until(lambda: self.job_state() in {"leased", "running"})
            self.assertTrue(fake.entered.wait(timeout=5))
            job_id = self.jobs()[0]["job_id"]
            stale_job = self.service.repository.job(job_id)
            assert stale_job is not None
            stale_fence = stale_job["fencing_token"]
            # Expire the durable lease with the fixture clock while the recognizer
            # is still waiting. A one-second lease gives its wall-clock watchdog
            # only 100 ms and can test timeout instead of stale-result fencing.
            self.now += timedelta(seconds=61)
            self.assertEqual(self.service.recover_expired_leases(), 1)
            self.assertEqual(self.job_state(), "pending")
            gate.set()
            token = self.token(receipt)
            self.wait_until(lambda: self.poll(token).processing_complete)
            page, _tail = self.drain(token)
            self.assertEqual(page.items[0].state, "ready")
            self.assertEqual(page.coverage.ready, 1)
            self.assertEqual(page.items[0].text, "Synthetic transcript")
            job = self.service.repository.job(job_id)
            assert job is not None
            self.assertEqual(job["state"], "ready")
            self.assertGreater(int(job["fencing_token"]), int(stale_fence))
            self.assertEqual(worker.status().completed_count, 1)
            self.assertEqual(worker.status().failed_count, 0)
            self.assertIsNone(worker.status().last_error_code)
            self.assertEqual(worker.status().historical_error_code, "CURSOR_STALE")
        finally:
            worker.stop()


if __name__ == "__main__":
    unittest.main()

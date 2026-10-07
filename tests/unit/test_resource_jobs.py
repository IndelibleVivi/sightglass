from __future__ import annotations

import tempfile
import threading
import time
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock, patch

from sightglass.model.db import WindowDB
from sightglass.model.repositories import WindowRepository
from sightglass.resources.coalesce import DerivationRequest
from sightglass.resources.jobs import (
    RESOURCE_ATTEMPTS_EXHAUSTED,
    RESOURCE_JOB_MAX_ATTEMPTS,
    ResourceJobService,
)
from sightglass.runtime.resource_worker import ResourceWorker


class _Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 9, 30, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.value


class ResourceJobTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name) / "private"
        root.mkdir(mode=0o700)
        self.database = WindowDB(root / "window.db")
        self.repository = WindowRepository(self.database)
        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO accounts(account_id, source_namespace, identity_confidence,
                    reader_timezone, current_display_name, first_seen_at, last_seen_at)
                VALUES ('acct', 'synthetic', 'exact', 'UTC', 'Synthetic', 'now', 'now')
                """
            )
            connection.execute(
                """
                INSERT INTO conversations(conversation_id, account_id,
                    source_conversation_id, kind, current_title, first_seen_at, last_seen_at)
                VALUES ('conv', 'acct', 'source-conv', 'direct', 'Synthetic', 'now', 'now')
                """
            )
            connection.execute(
                """
                INSERT INTO messages(message_id, account_id, conversation_id,
                    source_message_id, source_time_raw, sent_at_utc, sort_primary,
                    sort_seq, sort_tie, sender_label_snapshot_json, kind,
                    structured_json, first_seen_at, last_seen_at, current_state,
                    current_generation_id)
                VALUES ('msg', 'acct', 'conv', 'source-msg', '0', 'now', 'now',
                    0, 0, '{}', 'file', '{}', 'now', 'now', 'present', 'generation')
                """
            )
            connection.execute(
                """
                INSERT INTO resources(resource_id, message_id, source_ordinal, kind,
                    availability, resolver_json, first_seen_at, last_seen_at)
                VALUES ('resource', 'msg', 0, 'file', 'local_available',
                    '{"active":true}', 'now', 'now')
                """
            )
        self.clock = _Clock()
        self.jobs = ResourceJobService(self.repository, clock=self.clock)
        self.request = DerivationRequest(
            resource_id="resource",
            resource_revision="revision",
            mode="page",
            page=1,
            start_line=None,
            end_line=None,
            member=None,
            sheet=None,
            cell_range=None,
            max_bytes=1024,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_active_enqueue_deduplicates_and_terminal_enqueue_gets_new_identity(self) -> None:
        first = self.jobs.enqueue(self.request)
        duplicate = self.jobs.enqueue(self.request)
        self.assertEqual(first["job_id"], duplicate["job_id"])
        fence = self.jobs.lease(str(first["job_id"]), owner_id="worker")
        self.jobs.mark_running(str(first["job_id"]), owner_id="worker", fencing_token=fence)
        self.jobs.complete(str(first["job_id"]), owner_id="worker", fencing_token=fence)
        replacement = self.jobs.enqueue(self.request)
        self.assertNotEqual(first["job_id"], replacement["job_id"])

    def test_takeover_requeues_and_fences_the_old_owner(self) -> None:
        job = self.jobs.enqueue(self.request)
        job_id = str(job["job_id"])
        fence = self.jobs.lease(job_id, owner_id="old", lease_seconds=1)
        self.clock.value += timedelta(seconds=2)
        self.assertEqual(self.jobs.recover_expired_leases(), 1)
        with self.assertRaises(Exception):
            self.jobs.complete(job_id, owner_id="old", fencing_token=fence)
        new_fence = self.jobs.lease(job_id, owner_id="new")
        self.assertGreater(new_fence, fence)

    def test_fresh_schema_has_durable_resource_jobs(self) -> None:
        # Resource jobs arrived in schema v7; later migrations only add unrelated
        # tables, so assert the floor rather than pinning a version another owner owns.
        self.assertGreaterEqual(self.database.schema_version, 7)
        with self.database.connection() as connection:
            columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(resource_jobs)")
            }
        self.assertTrue(
            {
                "job_id",
                "resource_revision",
                "recipe_digest",
                "lease_expires_at",
                "fencing_token",
                "completed_at",
            }
            <= columns
        )

    def test_idle_worker_does_not_check_background_storage_admission(self) -> None:
        storage = Mock()
        cast(Any, self.database).storage = storage
        worker = ResourceWorker(cast(Any, None), self.jobs)

        self.assertFalse(worker._work_one())

        storage.require.assert_not_called()

    def test_empty_recovery_never_enters_maintenance_writer(self) -> None:
        with patch.object(self.database, "transaction", wraps=self.database.transaction) as writer:
            self.assertEqual(self.jobs.recover_expired_leases(), 0)
            self.assertEqual(self.jobs.recover_outstanding_leases(), 0)
        writer.assert_not_called()

    def test_expired_recovery_rechecks_due_rows_inside_writer(self) -> None:
        job = self.jobs.enqueue(self.request)
        self.jobs.lease(str(job["job_id"]), owner_id="synthetic-worker", lease_seconds=1)
        self.clock.value += timedelta(seconds=2)
        expired = self.repository.expired_resource_job_leases(self.jobs._now())
        with patch.object(
            self.repository, "expired_resource_job_leases", side_effect=[expired, []]
        ):
            with patch.object(self.repository, "update_resource_job") as update:
                self.assertEqual(self.jobs.recover_expired_leases(), 0)
        update.assert_not_called()

    def test_default_idle_worker_is_quiet_and_stop_wakes_wait(self) -> None:
        worker = ResourceWorker(cast(Any, None), self.jobs)
        with patch.object(self.database, "transaction", wraps=self.database.transaction) as writer:
            worker.start()
            try:
                limit = time.monotonic() + 1
                while worker.status().idle_cycle_count < 1 and time.monotonic() < limit:
                    time.sleep(0.005)
                time.sleep(0.15)
                self.assertEqual(worker.status().idle_cycle_count, 1)
                writer.assert_not_called()
            finally:
                started = time.monotonic()
                worker.stop()
                self.assertLess(time.monotonic() - started, 0.5)

    def test_committed_new_work_wakes_idle_worker_immediately(self) -> None:
        service = Mock()
        service.read_resource.return_value.descriptor = {"state": "ready"}
        worker = ResourceWorker(service, self.jobs)
        worker.start()
        try:
            limit = time.monotonic() + 1
            while worker.status().idle_cycle_count < 1 and time.monotonic() < limit:
                time.sleep(0.005)
            job = self.jobs.enqueue(self.request)
            worker.wake()
            limit = time.monotonic() + 1
            while worker.status().completed_count < 1 and time.monotonic() < limit:
                time.sleep(0.005)
            self.assertEqual(worker.status().completed_count, 1)
            self.assertEqual(self.jobs.by_id(str(job["job_id"]))["state"], "ready")  # type: ignore[index]
            self.assertGreaterEqual(worker.status().wake_count, 1)
        finally:
            worker.stop()

    def test_commit_between_empty_read_and_wait_is_not_lost(self) -> None:
        empty_read = threading.Event()
        release_read = threading.Event()
        original_pending = self.jobs.pending_jobs
        first = True

        def pending(limit: int, *, exclude: tuple[str, ...] = ()):
            nonlocal first
            result = original_pending(limit, exclude=exclude)
            if first:
                first = False
                self.assertEqual(result, [])
                empty_read.set()
                self.assertTrue(release_read.wait(2))
            return result

        service = Mock()
        service.read_resource.return_value.descriptor = {"state": "ready"}
        worker = ResourceWorker(service, self.jobs)
        with patch.object(self.jobs, "pending_jobs", side_effect=pending):
            worker.start()
            try:
                self.assertTrue(empty_read.wait(1))
                self.jobs.enqueue(self.request)
                worker.wake()  # commit occurs after no-work evidence, before wait
                release_read.set()
                limit = time.monotonic() + 1
                while worker.status().completed_count < 1 and time.monotonic() < limit:
                    time.sleep(0.005)
                self.assertEqual(worker.status().completed_count, 1)
            finally:
                release_read.set()
                worker.stop()

    def test_missing_notification_has_bounded_fallback(self) -> None:
        service = Mock()
        service.read_resource.return_value.descriptor = {"state": "ready"}
        worker = ResourceWorker(service, self.jobs, poll_interval_seconds=0.05)
        worker.start()
        try:
            time.sleep(0.02)
            self.jobs.enqueue(self.request)  # deliberately omit wake after commit
            limit = time.monotonic() + 1
            while worker.status().completed_count < 1 and time.monotonic() < limit:
                time.sleep(0.005)
            self.assertEqual(worker.status().completed_count, 1)
        finally:
            worker.stop()

    def test_held_lease_recovers_at_deadline_before_idle_fallback(self) -> None:
        self.jobs.clock = lambda: datetime.now(UTC)
        job = self.jobs.enqueue(self.request)
        self.jobs.lease(str(job["job_id"]), owner_id="synthetic-old", lease_seconds=1)
        service = Mock()
        service.read_resource.return_value.descriptor = {"state": "ready"}
        worker = ResourceWorker(service, self.jobs)
        worker.start()
        try:
            limit = time.monotonic() + 2
            while worker.status().completed_count < 1 and time.monotonic() < limit:
                time.sleep(0.01)
            self.assertEqual(worker.status().recovered_count, 1)
            self.assertEqual(worker.status().completed_count, 1)
        finally:
            worker.stop()

    def test_backoff_head_does_not_hide_later_pending_job(self) -> None:
        for page in range(1, 10):
            self.jobs.enqueue(replace(self.request, page=page))
        pending = self.jobs.pending_jobs(9)
        service = Mock()
        service.read_resource.return_value.descriptor = {"state": "ready"}
        worker = ResourceWorker(service, self.jobs)
        worker._retry_at = {str(job["job_id"]): time.monotonic() + 5 for job in pending[:8]}
        self.assertTrue(worker._work_one())
        self.assertEqual(self.jobs.by_id(str(pending[8]["job_id"]))["state"], "ready")  # type: ignore[index]
        self.assertEqual(worker.status().completed_count, 1)

    def test_new_arrivals_cannot_starve_an_old_due_retry(self) -> None:
        from sightglass.contracts.errors import ErrorCode, SightglassError

        old = self.jobs.enqueue(self.request)
        service = Mock()
        ready = Mock(descriptor={"state": "ready"})
        service.read_resource.side_effect = [
            SightglassError(ErrorCode.SERVICE_TIMEOUT, retryable=True), ready, ready,
        ]
        worker = ResourceWorker(service, self.jobs)
        self.assertTrue(worker._work_one())
        old_id = str(old["job_id"])
        self.assertEqual(self.jobs.by_id(old_id)["attempt"], 1)  # type: ignore[index]
        self.clock.value += timedelta(seconds=1)
        self.jobs.enqueue(replace(self.request, page=2))
        self.assertTrue(worker._work_one())  # fresh work may run while retry waits
        self.clock.value += timedelta(seconds=1)
        newest = self.jobs.enqueue(replace(self.request, page=3))
        worker._retry_at[old_id] = time.monotonic() - 1
        self.assertTrue(worker._work_one())
        self.assertEqual(self.jobs.by_id(old_id)["state"], "ready")  # type: ignore[index]
        self.assertEqual(self.jobs.by_id(old_id)["attempt"], 2)  # type: ignore[index]
        self.assertEqual(self.jobs.by_id(str(newest["job_id"]))["state"], "pending")  # type: ignore[index]

    def test_wall_clock_jumps_recompute_lease_delay_and_bound_wait(self) -> None:
        from sightglass.runtime.worker_wait import next_wait

        job = self.jobs.enqueue(self.request)
        self.jobs.lease(str(job["job_id"]), owner_id="synthetic-old", lease_seconds=60)
        self.clock.value -= timedelta(days=1)
        backwards = self.jobs.next_lease_delay()
        self.assertIsNotNone(backwards)
        self.assertEqual(next_wait(30.0, {}, backwards), 30.0)
        self.clock.value += timedelta(days=2)
        forwards = self.jobs.next_lease_delay()
        self.assertIsNotNone(forwards)
        self.assertGreater(next_wait(30.0, {}, forwards), 0)
        self.assertLess(next_wait(30.0, {}, forwards), 0.1)
        self.assertEqual(self.jobs.recover_expired_leases(), 1)
        self.assertIsNone(self.jobs.next_lease_delay())
        self.assertEqual(next_wait(30.0, {}, None), 30.0)

    def test_retry_deadline_is_not_delayed_by_idle_fallback_and_budget_is_preserved(self) -> None:
        from sightglass.contracts.errors import ErrorCode, SightglassError

        self.jobs.enqueue(self.request)
        service = Mock()
        service.read_resource.side_effect = SightglassError(
            ErrorCode.SERVICE_TIMEOUT, retryable=True
        )
        worker = ResourceWorker(service, self.jobs)
        with patch(
            "sightglass.runtime.resource_worker.RESOURCE_WORKER_RETRY_BACKOFF_SECONDS", 0.05
        ):
            worker.start()
            try:
                limit = time.monotonic() + 1
                while worker.status().failed_count < 1 and time.monotonic() < limit:
                    time.sleep(0.005)
                self.assertEqual(worker.status().retry_count, 2)
                self.assertEqual(worker.status().failed_count, 1)
                self.assertEqual(service.read_resource.call_count, 3)
            finally:
                worker.stop()

    def test_crash_recovery_loop_is_capped_by_the_durable_attempt_ceiling(self) -> None:
        # A job whose worker keeps dying mid-attempt is returned to pending by crash
        # recovery and re-leased. Without a durable ceiling that loop never ends.
        job = self.jobs.enqueue(self.request)
        job_id = str(job["job_id"])

        for _ in range(RESOURCE_JOB_MAX_ATTEMPTS):
            self.jobs.lease(job_id, owner_id="crashing-worker")
            # Simulate a daemon restart: the outstanding lease is returned to pending.
            self.assertEqual(self.jobs.recover_outstanding_leases(), 1)

        row = self.jobs.by_id(job_id)
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(int(row["attempt"]), RESOURCE_JOB_MAX_ATTEMPTS)

        # The next lease attempt retires the row instead of driving it again.
        with self.assertRaises(Exception):
            self.jobs.lease(job_id, owner_id="crashing-worker")
        retired = self.jobs.by_id(job_id)
        assert retired is not None
        self.assertEqual(str(retired["state"]), "failed")
        self.assertEqual(str(retired["error_code"]), RESOURCE_ATTEMPTS_EXHAUSTED)

    def test_exhaust_pending_terminalizes_only_spent_rows(self) -> None:
        job = self.jobs.enqueue(self.request)
        job_id = str(job["job_id"])

        # A fresh pending job is never retired by the ceiling helper.
        self.assertFalse(self.jobs.exhaust_pending(job_id))
        pending = self.jobs.by_id(job_id)
        assert pending is not None
        self.assertEqual(str(pending["state"]), "pending")

        for _ in range(RESOURCE_JOB_MAX_ATTEMPTS):
            self.jobs.lease(job_id, owner_id="crashing-worker")
            self.jobs.recover_outstanding_leases()

        self.assertTrue(self.jobs.exhaust_pending(job_id))
        retired = self.jobs.by_id(job_id)
        assert retired is not None
        self.assertEqual(str(retired["state"]), "failed")
        self.assertEqual(str(retired["error_code"]), RESOURCE_ATTEMPTS_EXHAUSTED)
        # Idempotent: the second call has nothing left to retire.
        self.assertFalse(self.jobs.exhaust_pending(job_id))


if __name__ == "__main__":
    unittest.main()

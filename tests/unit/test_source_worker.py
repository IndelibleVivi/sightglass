from __future__ import annotations

import threading
import time
import unittest
from typing import Literal
from unittest import mock

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.operations import check_operation_budget
from sightglass.runtime import source_worker
from sightglass.runtime.source_worker import SourceWorker
from sightglass.source.base import SourceProviderDescriptor


class _Provider:
    def __init__(
        self,
        *,
        supports_incremental: bool = True,
        source_mode: Literal["synthetic", "live"] = "synthetic",
    ) -> None:
        self.descriptor = SourceProviderDescriptor(
            kind="synthetic",
            implementation="test.incremental",
            source_mode=source_mode,
            platform=(),
            supports_incremental=supports_incremental,
            supports_resources=False,
            requires_running_app_for_key_refresh=False,
        )


class _RecoveringService:
    def __init__(self) -> None:
        self.provider = _Provider()
        self.calls = 0
        self.succeeded = threading.Event()

    def sync_source_once(self):
        self.calls += 1
        if self.calls == 1:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        self.succeeded.set()
        return {
            "conversation_count": 3,
            "message_count": 2,
            "pending_conversation_count": 1,
        }

    def process_backfill_once(self):
        return {"state": "idle"}


class _BackfillFailingService:
    def __init__(self) -> None:
        self.provider = _Provider()
        self.sync_calls = 0
        self.backfill_calls = 0
        self.sync_after_backfill_failure = threading.Event()

    def sync_source_once(self):
        self.sync_calls += 1
        return {
            "conversation_count": 1,
            "message_count": 1,
            "pending_conversation_count": 0,
        }

    def process_backfill_once(self):
        self.backfill_calls += 1
        if self.backfill_calls == 1:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        self.sync_after_backfill_failure.set()
        return {"state": "idle"}


class _TransientLiveDriftService:
    def __init__(self) -> None:
        self.provider = _Provider(source_mode="live")
        self.calls = 0
        self.sync_arguments: list[dict[str, int]] = []
        self.succeeded = threading.Event()

    def sync_source_once(self, **kwargs: int):
        self.calls += 1
        self.sync_arguments.append(kwargs)
        if self.calls < 3:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        self.succeeded.set()
        return {
            "conversation_count": 1,
            "message_count": 1,
            "pending_conversation_count": 0,
        }

    def process_backfill_once(self):
        return {"state": "idle"}


class _TransientLiveBackfillDriftService:
    def __init__(self) -> None:
        self.provider = _Provider(source_mode="live")
        self.backfill_calls = 0
        self.backfill_arguments: list[dict[str, int]] = []
        self.succeeded = threading.Event()

    def sync_source_once(self, **_kwargs: int):
        return {
            "conversation_count": 1,
            "message_count": 0,
            "pending_conversation_count": 0,
        }

    def process_backfill_once(self, **kwargs: int):
        self.backfill_calls += 1
        self.backfill_arguments.append(kwargs)
        if self.backfill_calls < 3:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        self.succeeded.set()
        return {"state": "running"}

class _SlowBackfillService:
    """A live backfill whose slow batch sizes genuinely run out the attempt deadline.

    When the requested ``batch_limit`` is "slow", the service consumes its own enclosing
    ``operation_budget`` by polling the canonical ``check_operation_budget()`` primitive
    until it trips, exactly as a slow native range read would before its next budget
    check. Any smaller batch is truthful and advances immediately.
    """

    def __init__(self, *, slow_batches: set[int]) -> None:
        self.provider = _Provider(source_mode="live")
        self.backfill_calls = 0
        self.backfill_arguments: list[dict[str, int]] = []
        self.advanced = threading.Event()
        self._slow_batches = slow_batches

    def sync_source_once(self, **_kwargs: int):
        return {
            "conversation_count": 1,
            "message_count": 0,
            "pending_conversation_count": 0,
        }

    def process_backfill_once(self, **kwargs: int):
        self.backfill_calls += 1
        self.backfill_arguments.append(kwargs)
        if kwargs.get("batch_limit") in self._slow_batches:
            # Burn the attempt's fresh budget through the canonical primitive until it
            # raises SERVICE_TIMEOUT(operation_deadline); never fake the exception.
            while True:
                check_operation_budget()
                time.sleep(0.001)
        self.advanced.set()
        return {"state": "running"}


class _ForegroundYieldService:
    def __init__(self) -> None:
        self.provider = _Provider(source_mode="live")
        self.entered = threading.Event()
        self.cancelled = threading.Event()
        self.succeeded = threading.Event()

    def sync_source_once(self, **_kwargs: int):
        if self.cancelled.is_set():
            self.succeeded.set()
            return {
                "conversation_count": 1,
                "message_count": 0,
                "pending_conversation_count": 0,
            }
        self.entered.set()
        try:
            while True:
                check_operation_budget()
                time.sleep(0.005)
        except SightglassError as exc:
            if exc.details.get("reason") != "operation_cancelled":
                raise
            self.cancelled.set()
            raise

    def process_backfill_once(self, **_kwargs: int):
        self.succeeded.set()
        return {"state": "idle"}


class _ForegroundBackfillYieldService:
    def __init__(self) -> None:
        self.provider = _Provider(source_mode="live")
        self.entered = threading.Event()
        self.cancelled = threading.Event()

    def sync_source_once(self, **_kwargs: int):
        return {
            "conversation_count": 1,
            "message_count": 0,
            "pending_conversation_count": 0,
        }

    def process_backfill_once(self, **_kwargs: int):
        self.entered.set()
        try:
            while True:
                check_operation_budget()
                time.sleep(0.005)
        except SightglassError as exc:
            if exc.details.get("reason") != "operation_cancelled":
                raise
            self.cancelled.set()
            raise


class _ForegroundQuiescenceService:
    def __init__(self) -> None:
        self.provider = _Provider(source_mode="live")
        self.entered = threading.Event()
        self.cancel_seen = threading.Event()
        self.allow_release = threading.Event()
        self.released = threading.Event()

    def sync_source_once(self, **_kwargs: int):
        self.entered.set()
        try:
            while True:
                check_operation_budget()
                time.sleep(0.005)
        except SightglassError as exc:
            if exc.details.get("reason") != "operation_cancelled":
                raise
            self.cancel_seen.set()
            self.allow_release.wait(1)
            self.released.set()
            raise

    def process_backfill_once(self, **_kwargs: int):
        return {"state": "idle"}

class _SlowTailService:
    """A live sync whose full-depth refresh genuinely runs out the attempt deadline.

    When the requested tail depth (``slow_tails``) or incremental range batch
    (``slow_batches``) is "slow", the service consumes its own enclosing
    ``operation_budget`` by polling the canonical ``check_operation_budget()`` primitive
    until it trips, exactly as a slow native read would before its next budget check.
    Any shallower depth or batch is truthful and admits immediately.
    """

    DEFAULT_TAIL = 50
    DEFAULT_BATCH = 200

    def __init__(
        self,
        *,
        slow_tails: set[int] | None = None,
        slow_batches: set[int] | None = None,
    ) -> None:
        self.provider = _Provider(source_mode="live")
        self.calls = 0
        self.sync_arguments: list[dict[str, int]] = []
        self.admitted = threading.Event()
        self._slow_tails = slow_tails or set()
        self._slow_batches = slow_batches or set()

    def sync_source_once(self, **kwargs: int):
        self.calls += 1
        self.sync_arguments.append(kwargs)
        tail = kwargs.get("initial_tail", self.DEFAULT_TAIL)
        batch = kwargs.get("batch_limit", self.DEFAULT_BATCH)
        if tail in self._slow_tails or batch in self._slow_batches:
            # Burn the attempt's fresh budget through the canonical primitive until it
            # raises SERVICE_TIMEOUT(operation_deadline); never fake the exception.
            while True:
                check_operation_budget()
                time.sleep(0.001)
        self.admitted.set()
        return {
            "conversation_count": 1,
            "message_count": 4,
            "pending_conversation_count": 1,
        }

    def process_backfill_once(self, **_kwargs: int):
        return {"state": "idle"}

class SourceWorkerTests(unittest.TestCase):
    def test_failed_foreground_claim_restores_idle_ownership(self) -> None:
        worker = SourceWorker(_RecoveringService())  # type: ignore[arg-type]
        failure = SightglassError(ErrorCode.SERVICE_TIMEOUT)
        with mock.patch("sightglass.runtime.source_worker.wait_for_event", side_effect=failure):
            with self.assertRaises(SightglassError):
                worker.foreground_enter()
        self.assertEqual(worker.status()["foreground"]["active_count"], 0)
        self.assertTrue(worker._foreground_idle.is_set())
        worker.foreground_enter()
        self.assertEqual(worker.status()["foreground"]["active_count"], 1)
        worker.foreground_exit()
        self.assertEqual(worker.status()["foreground"]["active_count"], 0)

    def test_foreground_enter_waits_until_background_work_is_quiescent(self) -> None:
        service = _ForegroundQuiescenceService()
        worker = SourceWorker(service, poll_interval_seconds=0.01)  # type: ignore[arg-type]
        worker.start()
        self.assertTrue(service.entered.wait(1))
        foreground_entered = threading.Event()

        thread = threading.Thread(
            target=lambda: (worker.foreground_enter(), foreground_entered.set())
        )
        thread.start()
        self.assertTrue(service.cancel_seen.wait(1))
        self.assertFalse(foreground_entered.wait(0.05))
        service.allow_release.set()
        self.assertTrue(foreground_entered.wait(1))
        self.assertTrue(service.released.is_set())

        worker.foreground_exit()
        worker.stop()
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())
        foreground = worker.status()["foreground"]
        self.assertEqual(foreground["wait_count"], 1)
        self.assertGreaterEqual(foreground["wait_p95_ms"], 50)
        self.assertGreaterEqual(foreground["wait_max_ms"], 50)

    def test_foreground_reader_yield_is_not_reported_as_backfill_error(self) -> None:
        service = _ForegroundBackfillYieldService()
        worker = SourceWorker(service, poll_interval_seconds=0.01)  # type: ignore[arg-type]
        worker.start()
        self.assertTrue(service.entered.wait(1))

        worker.foreground_enter()
        try:
            self.assertTrue(service.cancelled.wait(1))
        finally:
            worker.foreground_exit()
            worker.stop()

        self.assertIsNone(worker.status()["backfill_error_code"])
        self.assertIsNone(worker.status()["backfill_error_reason"])
        self.assertIsNone(worker.status()["backfill_error_elapsed_ms"])

    def test_foreground_reader_cooperatively_yields_background_work(self) -> None:
        service = _ForegroundYieldService()
        worker = SourceWorker(service, poll_interval_seconds=0.01)  # type: ignore[arg-type]
        worker.start()
        self.assertTrue(service.entered.wait(1))

        worker.foreground_enter()
        try:
            self.assertTrue(service.cancelled.wait(1))
            self.assertTrue(worker.status()["running"])
        finally:
            worker.foreground_exit()
            self.assertTrue(service.succeeded.wait(1))
            self.assertIsNone(worker.status()["last_error_code"])
            self.assertGreaterEqual(worker.status()["foreground_yield_count"], 1)
            worker.stop()

    def test_live_worker_retries_generation_drift_within_one_poll_cycle(self) -> None:
        service = _TransientLiveDriftService()
        worker = SourceWorker(service, poll_interval_seconds=10)  # type: ignore[arg-type]
        worker.start()
        succeeded_without_waiting_for_the_next_poll = service.succeeded.wait(1)
        worker.stop()

        self.assertTrue(succeeded_without_waiting_for_the_next_poll)
        self.assertEqual(service.calls, 3)
        self.assertEqual(
            service.sync_arguments,
            [
                {"conversation_limit": 5},
                {"conversation_limit": 2, "initial_tail": 20, "batch_limit": 20},
                {"conversation_limit": 1, "initial_tail": 10, "batch_limit": 1},
            ],
        )
        self.assertEqual(worker.status()["poll_count"], 1)
        self.assertIsNone(worker.status()["last_error_code"])

    def test_live_worker_retries_a_slow_tail_with_a_shallower_depth(self) -> None:
        # The full 50-row tail lets the first attempt's real budget trip via
        # check_operation_budget(); a fresh, shallower 20-row attempt then admits.
        service = _SlowTailService(slow_tails={50})
        with mock.patch.object(source_worker, "LIVE_SYNC_ATTEMPT_TIMEOUTS", (0.2, 0.2, 0.2)):
            worker = SourceWorker(service, poll_interval_seconds=50)  # type: ignore[arg-type]
            worker.start()
            admitted_without_waiting_for_the_next_poll = service.admitted.wait(2)
            worker.stop()
        status = worker.status()

        self.assertTrue(admitted_without_waiting_for_the_next_poll)
        self.assertEqual(service.calls, 2)
        self.assertEqual(
            service.sync_arguments,
            [
                {"conversation_limit": 5},
                {"conversation_limit": 2, "initial_tail": 20, "batch_limit": 20},
            ],
        )
        self.assertEqual(status["poll_count"], 1)
        self.assertIsNone(status["last_error_code"])
        self.assertEqual(status["message_count"], 4)

    def test_live_worker_reports_failure_when_every_fresh_attempt_times_out(self) -> None:
        # Every bounded tail depth consumes its own fresh attempt deadline, so the
        # worker must fail the poll closed rather than fabricate success.
        service = _SlowTailService(slow_tails={50, 20, 10})
        with mock.patch.object(source_worker, "LIVE_SYNC_ATTEMPT_TIMEOUTS", (0.2, 0.2, 0.2)):
            worker = SourceWorker(service, poll_interval_seconds=50)  # type: ignore[arg-type]
            worker.start()
            deadline = time.monotonic() + 3
            status = worker.status()
            while status["poll_count"] < 1 and time.monotonic() < deadline:
                time.sleep(0.01)
                status = worker.status()
            worker.stop()
        status = worker.status()

        self.assertEqual(service.calls, 3)
        self.assertEqual(
            service.sync_arguments,
            [
                {"conversation_limit": 5},
                {"conversation_limit": 2, "initial_tail": 20, "batch_limit": 20},
                {"conversation_limit": 1, "initial_tail": 10, "batch_limit": 1},
            ],
        )
        self.assertEqual(status["last_error_code"], ErrorCode.SERVICE_TIMEOUT.value)
        self.assertEqual(status["last_error_reason"], "operation_deadline")
        self.assertIsNone(status["last_success_epoch"])

    def test_live_worker_degrades_the_incremental_batch_with_each_fresh_attempt(self) -> None:
        # The default 200-row incremental range consumes the first attempt's real budget;
        # a fresh 50-row attempt then admits. This is the second, independent way one
        # rotation entry (already carrying the projection epoch) can pin the poll.
        service = _SlowTailService(slow_batches={200})
        with mock.patch.object(source_worker, "LIVE_SYNC_ATTEMPT_TIMEOUTS", (0.2, 0.2, 0.2)):
            worker = SourceWorker(service, poll_interval_seconds=50)  # type: ignore[arg-type]
            worker.start()
            admitted_without_waiting_for_the_next_poll = service.admitted.wait(2)
            worker.stop()
        status = worker.status()

        self.assertTrue(admitted_without_waiting_for_the_next_poll)
        self.assertEqual(service.calls, 2)
        self.assertEqual(
            service.sync_arguments,
            [
                {"conversation_limit": 5},
                {"conversation_limit": 2, "initial_tail": 20, "batch_limit": 20},
            ],
        )
        self.assertEqual(status["poll_count"], 1)
        self.assertIsNone(status["last_error_code"])
        self.assertEqual(status["message_count"], 4)

    def test_live_worker_reports_failure_when_every_batch_depth_times_out(self) -> None:
        # Every bounded batch depth consumes its own fresh attempt deadline, so the
        # worker must fail the poll closed rather than fabricate success.
        service = _SlowTailService(slow_batches={200, 20, 1})
        with mock.patch.object(source_worker, "LIVE_SYNC_ATTEMPT_TIMEOUTS", (0.2, 0.2, 0.2)):
            worker = SourceWorker(service, poll_interval_seconds=50)  # type: ignore[arg-type]
            worker.start()
            deadline = time.monotonic() + 3
            status = worker.status()
            while status["poll_count"] < 1 and time.monotonic() < deadline:
                time.sleep(0.01)
                status = worker.status()
            worker.stop()
        status = worker.status()

        self.assertEqual(service.calls, 3)
        self.assertEqual(
            service.sync_arguments,
            [
                {"conversation_limit": 5},
                {"conversation_limit": 2, "initial_tail": 20, "batch_limit": 20},
                {"conversation_limit": 1, "initial_tail": 10, "batch_limit": 1},
            ],
        )
        self.assertEqual(status["last_error_code"], ErrorCode.SERVICE_TIMEOUT.value)
        self.assertEqual(status["last_error_reason"], "operation_deadline")
        self.assertIsNone(status["last_success_epoch"])

    def test_worker_recovers_after_a_failed_poll_and_reports_content_free_state(self) -> None:
        service = _RecoveringService()
        worker = SourceWorker(service, poll_interval_seconds=0.01)  # type: ignore[arg-type]
        worker.start()
        self.assertTrue(service.succeeded.wait(1))
        deadline = time.monotonic() + 1
        status = worker.status()
        while status["last_success_epoch"] is None and time.monotonic() < deadline:
            time.sleep(0.005)
            status = worker.status()
        worker.stop()

        self.assertGreaterEqual(status["poll_count"], 2)
        self.assertIsNone(status["last_error_code"])
        self.assertEqual(status["conversation_count"], 3)
        self.assertEqual(status["message_count"], 2)
        self.assertEqual(status["pending_conversation_count"], 1)
        self.assertFalse(worker.status()["running"])

    def test_live_worker_retries_backfill_drift_with_progressively_smaller_batches(
        self,
    ) -> None:
        service = _TransientLiveBackfillDriftService()
        worker = SourceWorker(service, poll_interval_seconds=10)  # type: ignore[arg-type]
        worker.start()
        succeeded_without_waiting_for_the_next_poll = service.succeeded.wait(1)
        worker.stop()

        self.assertTrue(succeeded_without_waiting_for_the_next_poll)
        self.assertEqual(service.backfill_calls, 3)
        self.assertEqual(
            service.backfill_arguments,
            [{"batch_limit": 50}, {"batch_limit": 20}, {"batch_limit": 1}],
        )
        self.assertEqual(worker.status()["backfill_state"], "running")
        self.assertIsNone(worker.status()["backfill_error_code"])

    def test_live_backfill_degrades_the_batch_after_deadline_expiry(self) -> None:
        # The default 50-row batch consumes the first attempt's real budget; the fresh
        # 20-row attempt then also expires; the one-message batch admits and reports
        # truthful progress rather than a retryable error.
        service = _SlowBackfillService(slow_batches={50, 20})
        with mock.patch.object(
            source_worker, "LIVE_BACKFILL_ATTEMPT_TIMEOUTS", (0.2, 0.2, 0.2)
        ):
            worker = SourceWorker(service, poll_interval_seconds=10)  # type: ignore[arg-type]
            worker.start()
            advanced_without_waiting_for_the_next_poll = service.advanced.wait(2)
            worker.stop()
        status = worker.status()

        self.assertTrue(advanced_without_waiting_for_the_next_poll)
        self.assertEqual(service.backfill_calls, 3)
        self.assertEqual(
            service.backfill_arguments,
            [{"batch_limit": 50}, {"batch_limit": 20}, {"batch_limit": 1}],
        )
        self.assertEqual(status["backfill_state"], "running")
        self.assertIsNone(status["backfill_error_code"])

    def test_live_backfill_reports_timeout_when_every_batch_expires(self) -> None:
        # Even the one-message batch consumes its own fresh deadline: the worker must
        # report a retryable timeout and must not fabricate progress.
        service = _SlowBackfillService(slow_batches={50, 20, 1})
        with mock.patch.object(
            source_worker, "LIVE_BACKFILL_ATTEMPT_TIMEOUTS", (0.2, 0.2, 0.2)
        ):
            worker = SourceWorker(service, poll_interval_seconds=10)  # type: ignore[arg-type]
            worker.start()
            deadline = time.monotonic() + 4
            status = worker.status()
            while status["backfill_state"] == "idle" and time.monotonic() < deadline:
                time.sleep(0.01)
                status = worker.status()
            worker.stop()
        status = worker.status()

        self.assertFalse(service.advanced.is_set())
        self.assertEqual(service.backfill_calls, 3)
        self.assertEqual(status["backfill_state"], "retryable_error")
        self.assertEqual(status["backfill_error_code"], ErrorCode.SERVICE_TIMEOUT.value)
        self.assertEqual(status["backfill_error_reason"], "operation_deadline")
        self.assertGreaterEqual(status["backfill_error_elapsed_ms"], 500)
        self.assertIsNotNone(status["last_success_epoch"])

    def test_live_backfill_unrelated_error_still_fails_closed(self) -> None:
        class _UnrelatedErrorService(_SlowBackfillService):
            def process_backfill_once(self, **kwargs: int):
                self.backfill_calls += 1
                self.backfill_arguments.append(kwargs)
                raise SightglassError(ErrorCode.SOURCE_INCOMPLETE, retryable=True)

        service = _UnrelatedErrorService(slow_batches=set())
        worker = SourceWorker(service, poll_interval_seconds=10)  # type: ignore[arg-type]
        worker.start()
        deadline = time.monotonic() + 4
        status = worker.status()
        while status["backfill_state"] == "idle" and time.monotonic() < deadline:
            time.sleep(0.01)
            status = worker.status()
        worker.stop()
        status = worker.status()

        # No degraded retry for an unrelated source error.
        self.assertEqual(service.backfill_calls, 1)
        self.assertEqual(status["backfill_state"], "retryable_error")
        self.assertEqual(status["backfill_error_code"], ErrorCode.SOURCE_INCOMPLETE.value)

    def test_worker_keeps_a_bounded_polling_loop_after_a_backfill_error(self) -> None:
        service = _BackfillFailingService()
        worker = SourceWorker(service, poll_interval_seconds=0.01)  # type: ignore[arg-type]
        worker.start()
        self.assertTrue(service.sync_after_backfill_failure.wait(2))
        worker.stop()
        status = worker.status()

        self.assertGreaterEqual(service.sync_calls, 2)
        self.assertGreaterEqual(service.backfill_calls, 2)
        self.assertIsNone(status["last_error_code"])
        self.assertEqual(status["backfill_state"], "idle")
        self.assertIsNone(status["backfill_error_code"])
        self.assertFalse(status["running"])

    def test_worker_does_not_start_without_incremental_source_support(self) -> None:
        service = _RecoveringService()
        service.provider = _Provider(supports_incremental=False)
        worker = SourceWorker(service, poll_interval_seconds=0.01)  # type: ignore[arg-type]
        worker.start()
        time.sleep(0.05)
        worker.stop()
        status = worker.status()

        self.assertFalse(status["enabled"])
        self.assertEqual(status["poll_count"], 0)
        self.assertEqual(service.calls, 0)

    def test_live_attempt_schedules_stay_within_one_poll(self) -> None:
        # Guard the production schedule: aligned attempts, budgets that cannot extend the
        # poll deadline, and no single attempt outliving the worker stop/join window.
        schedules = (
            source_worker.LIVE_SYNC_CONVERSATION_LIMITS,
            source_worker.LIVE_SYNC_INITIAL_TAILS,
            source_worker.LIVE_SYNC_BATCH_LIMITS,
            source_worker.LIVE_SYNC_ATTEMPT_TIMEOUTS,
        )
        self.assertTrue(
            all(len(schedule) == source_worker.LIVE_SYNC_ATTEMPTS for schedule in schedules)
        )
        self.assertLessEqual(
            sum(source_worker.LIVE_SYNC_ATTEMPT_TIMEOUTS),
            source_worker.SOURCE_WORKER_OPERATION_TIMEOUT_SECONDS,
        )
        self.assertLess(
            max(source_worker.LIVE_SYNC_ATTEMPT_TIMEOUTS),
            source_worker.SOURCE_WORKER_STOP_TIMEOUT_SECONDS,
        )
        # The first attempt keeps the service default batch; the final degraded attempt
        # asks for a one-message batch while the stale-tail path degrades in lockstep.
        self.assertIsNone(source_worker.LIVE_SYNC_BATCH_LIMITS[0])
        self.assertEqual(source_worker.LIVE_SYNC_BATCH_LIMITS[-1], 1)
        self.assertEqual(source_worker.LIVE_SYNC_INITIAL_TAILS[-1], 10)
        # The backfill schedule follows the same invariants, and its final attempt asks
        # for the one-message batch (``read_range`` probe limit 2).
        self.assertEqual(
            len(source_worker.LIVE_BACKFILL_BATCH_LIMITS),
            source_worker.LIVE_BACKFILL_ATTEMPTS,
        )
        self.assertEqual(
            len(source_worker.LIVE_BACKFILL_ATTEMPT_TIMEOUTS),
            source_worker.LIVE_BACKFILL_ATTEMPTS,
        )
        self.assertLessEqual(
            sum(source_worker.LIVE_BACKFILL_ATTEMPT_TIMEOUTS),
            source_worker.SOURCE_WORKER_OPERATION_TIMEOUT_SECONDS,
        )
        self.assertLess(
            max(source_worker.LIVE_BACKFILL_ATTEMPT_TIMEOUTS),
            source_worker.SOURCE_WORKER_STOP_TIMEOUT_SECONDS,
        )
        self.assertEqual(source_worker.LIVE_BACKFILL_BATCH_LIMITS[0], 50)
        self.assertEqual(source_worker.LIVE_BACKFILL_BATCH_LIMITS[-1], 1)


if __name__ == "__main__":
    unittest.main()

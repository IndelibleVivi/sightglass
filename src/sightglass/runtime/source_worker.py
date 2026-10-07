from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.worker_schedule import collecting_conversations
from sightglass.operations import operation_budget, wait_for_event
from sightglass.reader.service import ReaderService
from sightglass.runtime.worker_diagnostics import (
    clear_error,
    error_fields,
    public_error_reason,
    record_error,
)

SOURCE_WORKER_STOP_TIMEOUT_SECONDS = 10.0
LIVE_SYNC_ATTEMPTS = 3
LIVE_SYNC_CONVERSATION_LIMITS = (5, 2, 1)
# A single slow conversation's default refresh depth must not pin the whole semantic
# tail rotation. Each live attempt gets its own fresh bounded operation, paired with a
# narrower conversation window and requested tail depth, while the schedule's total
# stays within the existing poll budget so no attempt can extend the overall deadline.
LIVE_SYNC_INITIAL_TAILS: tuple[int | None, ...] = (None, 20, 10)
# A degraded attempt must also shrink the incremental range batch: a 201-row repaired
# range read is a second, independent way for one rotation entry to consume the whole
# attempt. The final attempt asks for a one-message batch (``read_range`` probe limit 2),
# which admits a truthful bounded tail even for the slow active conversation. The first
# live attempt keeps the service default (200) so the fast path is unchanged.
LIVE_SYNC_BATCH_LIMITS: tuple[int | None, ...] = (None, 20, 1)
# Budgets are repartitioned so the smallest, final incremental attempt has enough time to
# admit the slow conversation; the schedule still sums to exactly the poll budget, and no
# single attempt can exceed the worker's stop/join window.
LIVE_SYNC_ATTEMPT_TIMEOUTS: tuple[float, ...] = (6.0, 5.0, 9.0)
# Native live backfill repairs one queued job at a time. Like the sync schedule, each
# attempt gets its own fresh bounded operation: the first two batch sizes keep the
# existing fast/degraded behavior, and the final ``batch_limit=1`` issues
# ``read_range(limit=2)`` for a single message, which admits truthful progress even when
# the default 51-row range consumes the earlier attempts' entire budgets.
LIVE_BACKFILL_ATTEMPTS = 3
LIVE_BACKFILL_BATCH_LIMITS: tuple[int, ...] = (50, 20, 1)
LIVE_BACKFILL_ATTEMPT_TIMEOUTS: tuple[float, ...] = (6.0, 5.0, 9.0)
SOURCE_WORKER_OPERATION_TIMEOUT_SECONDS = 20.0
SOURCE_WORKER_METADATA_INTERVAL_SECONDS = 30.0

# The live attempt schedules must stay aligned, and their budgets must never extend the
# poll deadline or overrun the worker's stop/join window.
assert all(
    len(schedule) == LIVE_SYNC_ATTEMPTS
    for schedule in (
        LIVE_SYNC_CONVERSATION_LIMITS,
        LIVE_SYNC_INITIAL_TAILS,
        LIVE_SYNC_BATCH_LIMITS,
        LIVE_SYNC_ATTEMPT_TIMEOUTS,
    )
)
assert sum(LIVE_SYNC_ATTEMPT_TIMEOUTS) <= SOURCE_WORKER_OPERATION_TIMEOUT_SECONDS
assert max(LIVE_SYNC_ATTEMPT_TIMEOUTS) < SOURCE_WORKER_STOP_TIMEOUT_SECONDS

# The live backfill schedules follow the same invariants as the sync schedules.
assert len(LIVE_BACKFILL_BATCH_LIMITS) == LIVE_BACKFILL_ATTEMPTS
assert len(LIVE_BACKFILL_ATTEMPT_TIMEOUTS) == LIVE_BACKFILL_ATTEMPTS
assert sum(LIVE_BACKFILL_ATTEMPT_TIMEOUTS) <= SOURCE_WORKER_OPERATION_TIMEOUT_SECONDS
assert max(LIVE_BACKFILL_ATTEMPT_TIMEOUTS) < SOURCE_WORKER_STOP_TIMEOUT_SECONDS


class SourceWorker:
    """Bounded, content-free daemon loop that keeps the authorized live tail indexed."""

    def __init__(
        self,
        service: ReaderService,
        *,
        poll_interval_seconds: float = 2.0,
        metadata_interval_seconds: float = SOURCE_WORKER_METADATA_INTERVAL_SECONDS,
        notify_work: Callable[[], None] | None = None,
    ) -> None:
        self.service = service
        self.poll_interval_seconds = max(0.1, float(poll_interval_seconds))
        self.metadata_interval_seconds = max(0.1, float(metadata_interval_seconds))
        self.notify_work = notify_work
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._cancel_work = threading.Event()
        self._foreground_idle = threading.Event()
        self._foreground_idle.set()
        self._background_idle = threading.Event()
        self._background_idle.set()
        self._foreground_lock = threading.Lock()
        self._foreground_count = 0
        self._background_active = False
        self._foreground_cancel_pending = False
        self._foreground_revision = 0
        self._foreground_wait_samples: deque[int] = deque(maxlen=256)
        self._foreground_wait_count = 0
        self._foreground_wait_total_ms = 0
        self._foreground_wait_max_ms = 0
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._state: dict[str, Any] = {
            "enabled": service.provider.descriptor.supports_incremental,
            "running": False,
            "poll_count": 0,
            "last_success_epoch": None,
            "last_error_code": None,
            "last_error_reason": None,
            "last_error_elapsed_ms": None,
            "foreground_yield_count": 0,
            "conversation_count": 0,
            "message_count": 0,
            "pending_conversation_count": 0,
            "conflict_conversation_count": 0,
            "backfill_state": "idle",
            "backfill_error_code": None,
            "backfill_error_reason": None,
            "backfill_error_elapsed_ms": None,
            "backfill_error_type": None,
            "backfill_error_location": None,
            **error_fields(),
        }

    def start(self) -> None:
        if not self._state["enabled"] or self._thread is not None:
            return
        self._stop.clear()
        self._cancel_work.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="sightglass-source-worker",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._cancel_work.set()
        self._foreground_idle.set()
        self._wake.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=SOURCE_WORKER_STOP_TIMEOUT_SECONDS)

    def wake(self) -> None:
        self._wake.set()

    def foreground_enter(self) -> None:
        """Yield background source work while a reader operation owns priority."""

        with self._foreground_lock:
            self._foreground_count += 1
            self._foreground_revision += 1
            self._foreground_idle.clear()
            if self._background_active:
                self._foreground_cancel_pending = True
                self._cancel_work.set()
        # Cancellation is cooperative. Do not let the reader race the still-unwinding
        # worker for cached SQLCipher handles or the window writer: foreground begins
        # only after the current background slice has actually released its resources.
        wait_started = time.monotonic()
        try:
            wait_for_event(self._background_idle)
        except BaseException:
            self.foreground_exit()
            raise
        finally:
            waited_ms = max(0, round((time.monotonic() - wait_started) * 1_000))
            with self._foreground_lock:
                self._foreground_wait_count += 1
                self._foreground_wait_total_ms += waited_ms
                self._foreground_wait_max_ms = max(self._foreground_wait_max_ms, waited_ms)
                self._foreground_wait_samples.append(waited_ms)

    def foreground_exit(self) -> None:
        with self._foreground_lock:
            self._foreground_count = max(0, self._foreground_count - 1)
            if self._foreground_count == 0:
                self._foreground_idle.set()
                if not self._stop.is_set() and not self._foreground_cancel_pending:
                    self._cancel_work.clear()
                self._wake.set()

    def _next_source_delay(self) -> float:
        # Test doubles and providers without resident metadata retain the existing
        # freshness cadence. Real services read only local policy/residency here.
        residency = getattr(self.service, "residency", None)
        if residency is None:
            return self.poll_interval_seconds
        settings = residency.settings()
        if settings.default_mode in {"keep", "recent"}:
            return self.poll_interval_seconds
        conversations = collecting_conversations(
            self.service.repository.database, default_mode=str(settings.default_mode)
        )
        if any(self.service.reader.policy.permits(value) for value in conversations):
            return self.poll_interval_seconds
        return self.metadata_interval_seconds

    def _foreground_present(self) -> bool:
        with self._foreground_lock:
            return self._foreground_count > 0

    def _foreground_revision_value(self) -> int:
        with self._foreground_lock:
            return self._foreground_revision

    def _acknowledge_foreground_cancel(self) -> bool:
        with self._foreground_lock:
            if not self._foreground_cancel_pending:
                return False
            self._foreground_cancel_pending = False
            if self._foreground_count == 0 and not self._stop.is_set():
                self._cancel_work.clear()
            return True

    def _wait_for_foreground(self) -> bool:
        while not self._stop.is_set():
            if self._foreground_idle.wait(timeout=0.1):
                return True
        return False

    def _begin_background_work(self) -> bool:
        """Atomically claim one worker slice only when no foreground read exists."""

        with self._foreground_lock:
            if self._stop.is_set() or self._foreground_count:
                return False
            self._background_active = True
            self._background_idle.clear()
            self._cancel_work.clear()
            return True

    def _end_background_work(self) -> None:
        with self._foreground_lock:
            self._background_active = False
            self._background_idle.set()
            self._foreground_cancel_pending = False
            if not self._stop.is_set():
                self._cancel_work.clear()

    def _record_foreground_yield(self) -> None:
        with self._lock:
            self._state["foreground_yield_count"] = int(self._state["foreground_yield_count"]) + 1

    def status(self) -> dict[str, Any]:
        with self._foreground_lock:
            samples = tuple(sorted(self._foreground_wait_samples))
            foreground = {
                "active_count": self._foreground_count,
                "wait_count": self._foreground_wait_count,
                "wait_total_ms": self._foreground_wait_total_ms,
                "wait_p50_ms": (samples[max(0, (len(samples) + 1) // 2 - 1)] if samples else None),
                "wait_p95_ms": (
                    samples[max(0, (len(samples) * 95 + 99) // 100 - 1)] if samples else None
                ),
                "wait_max_ms": self._foreground_wait_max_ms,
            }
        with self._lock:
            return {
                "schema": "sightglass.source-worker-status.v1",
                **self._state,
                "foreground": foreground,
            }

    def _sync_source_once(self) -> dict[str, Any]:
        live = self.service.provider.descriptor.source_mode == "live"
        attempts = LIVE_SYNC_ATTEMPTS if live else 1
        for attempt in range(attempts):
            if self._stop.is_set() or self._cancel_work.is_set():
                # A stop or a foreground read claimed priority: open no further
                # attempt. The run loop classifies this cooperative yield.
                raise SightglassError(
                    ErrorCode.SERVICE_TIMEOUT,
                    retryable=True,
                    details={"reason": "operation_cancelled"},
                )
            timeout = (
                LIVE_SYNC_ATTEMPT_TIMEOUTS[attempt]
                if live
                else SOURCE_WORKER_OPERATION_TIMEOUT_SECONDS
            )
            try:
                # Each live attempt gets its own fresh bounded budget; the schedule's
                # sum stays within the existing poll budget.
                with operation_budget(timeout, cancelled=self._cancel_work):
                    kwargs: dict[str, Any] = {}
                    if live:
                        kwargs["conversation_limit"] = LIVE_SYNC_CONVERSATION_LIMITS[attempt]
                        tail = LIVE_SYNC_INITIAL_TAILS[attempt]
                        if tail is not None:
                            kwargs["initial_tail"] = tail
                        batch = LIVE_SYNC_BATCH_LIMITS[attempt]
                        if batch is not None:
                            kwargs["batch_limit"] = batch
                    return self.service.sync_source_once(**kwargs)
            except SightglassError as exc:
                if self._stop.is_set():
                    raise
                if exc.details.get("reason") == "operation_cancelled" or self._foreground_present():
                    # Do not start another attempt; let the run loop record the yield.
                    raise
                # Generation drift and this attempt's own deadline expiry both advance
                # to the next fresh, narrower attempt before failing closed.
                if attempt + 1 >= attempts:
                    raise
                deadline_expired = (
                    exc.code == ErrorCode.SERVICE_TIMEOUT
                    and exc.details.get("reason") == "operation_deadline"
                )
                if exc.code != ErrorCode.SOURCE_GENERATION_CHANGED and not deadline_expired:
                    raise
        raise RuntimeError("source sync retry loop did not return")

    def _process_backfill_once(self) -> dict[str, Any]:
        live = self.service.provider.descriptor.source_mode == "live"
        if not live:
            return self.service.process_backfill_once()
        for attempt in range(LIVE_BACKFILL_ATTEMPTS):
            if self._stop.is_set() or self._cancel_work.is_set():
                # A stop or a foreground read claimed priority: open no further attempt.
                # The run loop classifies this cooperative yield.
                raise SightglassError(
                    ErrorCode.SERVICE_TIMEOUT,
                    retryable=True,
                    details={"reason": "operation_cancelled"},
                )
            try:
                # Each live attempt gets its own fresh bounded budget; the schedule's sum
                # stays within the existing 20-second backfill budget.
                with operation_budget(
                    LIVE_BACKFILL_ATTEMPT_TIMEOUTS[attempt],
                    cancelled=self._cancel_work,
                ):
                    return self.service.process_backfill_once(
                        batch_limit=LIVE_BACKFILL_BATCH_LIMITS[attempt]
                    )
            except SightglassError as exc:
                if self._stop.is_set():
                    raise
                if exc.details.get("reason") == "operation_cancelled" or self._foreground_present():
                    # Do not start another attempt; let the run loop record the yield.
                    raise
                # Generation drift and this attempt's own deadline expiry both advance to
                # the next fresh, smaller-batch attempt before failing closed.
                if attempt + 1 >= LIVE_BACKFILL_ATTEMPTS:
                    raise
                deadline_expired = (
                    exc.code == ErrorCode.SERVICE_TIMEOUT
                    and exc.details.get("reason") == "operation_deadline"
                )
                if exc.code != ErrorCode.SOURCE_GENERATION_CHANGED and not deadline_expired:
                    raise
        raise RuntimeError("source backfill retry loop did not return")

    def _run(self) -> None:
        delay = self.poll_interval_seconds
        with self._lock:
            self._state["running"] = True
        try:
            while not self._stop.is_set():
                self._wake.clear()
                if not self._wait_for_foreground():
                    break
                if not self._begin_background_work():
                    continue
                try:
                    foreground_revision = self._foreground_revision_value()
                    poll_started = time.monotonic()
                    try:
                        maintain = getattr(self.service.provider, "maintain_idle_connections", None)
                        if callable(maintain):
                            maintain()
                        with operation_budget(
                            SOURCE_WORKER_OPERATION_TIMEOUT_SECONDS,
                            cancelled=self._cancel_work,
                        ):
                            result = self._sync_source_once()
                        backfill_started = time.monotonic()
                        backfill_error_reason = None
                        backfill_error_elapsed_ms = None
                        try:
                            with operation_budget(
                                SOURCE_WORKER_OPERATION_TIMEOUT_SECONDS,
                                cancelled=self._cancel_work,
                            ):
                                backfill = self._process_backfill_once()
                            backfill_error_code = None
                        except SightglassError as exc:
                            if (
                                exc.details.get("reason") == "operation_cancelled"
                                or self._foreground_present()
                                or self._foreground_revision_value() != foreground_revision
                            ):
                                raise
                            backfill = {"state": "retryable_error"}
                            backfill_error_code = exc.code.value
                            backfill_error_reason = public_error_reason(exc)
                            backfill_error_elapsed_ms = round(
                                (time.monotonic() - backfill_started) * 1_000
                            )
                            with self._lock:
                                record_error(self._state, exc, exc.code.value)
                        except Exception as exc:
                            if (
                                self._foreground_present()
                                or self._foreground_revision_value() != foreground_revision
                            ):
                                raise
                            backfill = {"state": "error"}
                            backfill_error_code = exc.__class__.__name__
                            backfill_error_elapsed_ms = round(
                                (time.monotonic() - backfill_started) * 1_000
                            )
                            with self._lock:
                                record_error(self._state, exc, exc.__class__.__name__)
                        with self._lock:
                            self._state.update(
                                poll_count=int(self._state["poll_count"]) + 1,
                                last_success_epoch=time.time(),
                                last_error_code=None,
                                last_error_reason=None,
                                last_error_elapsed_ms=None,
                                conversation_count=int(result["conversation_count"]),
                                message_count=int(result["message_count"]),
                                pending_conversation_count=int(
                                    result["pending_conversation_count"]
                                ),
                                conflict_conversation_count=int(
                                    result.get("conflict_conversation_count", 0)
                                ),
                                backfill_state=str(backfill["state"]),
                                backfill_error_code=backfill_error_code,
                                backfill_error_reason=backfill_error_reason,
                                backfill_error_elapsed_ms=backfill_error_elapsed_ms,
                                backfill_error_type=(
                                    self._state["last_error_type"] if backfill_error_code else None
                                ),
                                backfill_error_location=(
                                    self._state["last_error_location"]
                                    if backfill_error_code else None
                                ),
                            )
                            self._state["work_count"] += 1
                            clear_error(self._state)
                        if self.notify_work is not None and (
                            result["message_count"] or backfill.get("message_count", 0)
                        ):
                            self.notify_work()
                        delay = (
                            self.poll_interval_seconds if result["pending_conversation_count"]
                            else self._next_source_delay()
                        )
                    except SightglassError as exc:
                        foreground_cancel = self._acknowledge_foreground_cancel()
                        if (
                            exc.details.get("reason") == "operation_cancelled"
                            or foreground_cancel
                            or self._foreground_present()
                            or self._foreground_revision_value() != foreground_revision
                        ):
                            self._record_foreground_yield()
                            delay = self.poll_interval_seconds
                            continue
                        with self._lock:
                            record_error(self._state, exc, exc.code.value)
                            self._state.update(
                                poll_count=int(self._state["poll_count"]) + 1,
                                last_error_code=exc.code.value,
                                last_error_reason=public_error_reason(exc),
                                last_error_elapsed_ms=round(
                                    (time.monotonic() - poll_started) * 1_000
                                ),
                            )
                        delay = min(30.0, max(self.poll_interval_seconds, delay * 2))
                    except Exception as exc:
                        if (
                            self._acknowledge_foreground_cancel()
                            or self._foreground_present()
                            or self._foreground_revision_value() != foreground_revision
                        ):
                            self._record_foreground_yield()
                            delay = self.poll_interval_seconds
                            continue
                        with self._lock:
                            record_error(self._state, exc, exc.__class__.__name__)
                            self._state.update(
                                poll_count=int(self._state["poll_count"]) + 1,
                                last_error_code=exc.__class__.__name__,
                                last_error_reason=None,
                                last_error_elapsed_ms=round(
                                    (time.monotonic() - poll_started) * 1_000
                                ),
                            )
                        delay = min(30.0, max(self.poll_interval_seconds, delay * 2))
                finally:
                    self._end_background_work()
                with self._lock:
                    self._state["idle_cycle_count"] += 1
                if self._wake.wait(timeout=delay) and not self._stop.is_set():
                    with self._lock:
                        self._state["wake_count"] += 1
        finally:
            self._background_idle.set()
            with self._lock:
                self._state["running"] = False
                self._thread = None

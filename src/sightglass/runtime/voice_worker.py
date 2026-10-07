from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any, Protocol
from uuid import uuid4

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.voice import VoiceTranscription
from sightglass.operations import check_operation_budget, operation_budget
from sightglass.runtime.worker_diagnostics import (
    WorkerDiagnostics,
    clear_error,
    error_fields,
    record_error,
)
from sightglass.runtime.worker_wait import next_wait, retry_exclusions
from sightglass.voice.service import VoiceService

VOICE_WORKER_STOP_TIMEOUT_SECONDS = 10.0
VOICE_WORKER_POLL_INTERVAL_SECONDS = 30.0
VOICE_WORKER_MIN_LEASE_SECONDS = 60
VOICE_WORKER_LEASE_MARGIN_SECONDS = 5
VOICE_WORKER_LEASE_CANDIDATES = 8
VOICE_WORKER_MAX_ATTEMPTS = 3
VOICE_WORKER_RETRY_BACKOFF_SECONDS = 0.5
VOICE_WORKER_RETRY_MAX_BACKOFF_SECONDS = 5.0
VOICE_WORKER_RETRY_TRACKED_JOBS = 64


class Transcriber(Protocol):
    """Injected recognizer.  A job row is read-only evidence; SQL stays in voice/."""

    def transcribe(
        self, job: dict[str, Any], *, duration_ms: int, deadline: float | None,
    ) -> str | VoiceTranscription: ...


def transcription_parts(
    result: str | VoiceTranscription,
) -> tuple[str, dict[str, Any] | None]:
    """Normalize a recognizer result into text plus optional derivation provenance."""

    if isinstance(result, VoiceTranscription):
        return result.text, dict(result.provenance)
    if not isinstance(result, str):
        raise SightglassError(ErrorCode.QUERY_INVALID)
    return result, None


@dataclass(frozen=True)
class VoiceWorkerStatus(WorkerDiagnostics):
    schema: str
    enabled: bool
    running: bool
    completed_count: int
    failed_count: int
    blocked_count: int
    retry_count: int
    recovered_count: int
    last_error_code: str | None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class VoiceWorker:
    """Leases pending voice jobs and drives them through an injected transcriber.

    Production builds pass ``transcriber=None``: the worker stays disabled and never
    runs a fake recognizer.  Transcription runs under an operation budget bounded by
    the job lease, so a cooperative recognizer is interrupted before the lease can
    expire; a lost lease can never commit because fencing tokens are checked
    server-side on every finalize call.

    The worker never claims the ``running`` job state: it cannot observe recognizer
    progress, so the only claim it makes is the lease itself.  A cooperative stop
    therefore leaves the job ``leased``, to be recovered by lease expiry or by the
    takeover of a replacing daemon context.  A retryable recognizer failure returns
    the job to the queue with a bounded local backoff and a new fencing token;
    ``RESOURCE_BLOCKED`` is permanent and is never retried.
    """

    def __init__(
        self,
        service: VoiceService,
        transcriber: Transcriber | None,
        *,
        owner_id: str | None = None,
        poll_interval_seconds: float = VOICE_WORKER_POLL_INTERVAL_SECONDS,
        transcribe_timeout_seconds: float | None = None,
        lease_seconds: int | None = None,
        wake: threading.Event | None = None,
        notify_events: Callable[[], None] | None = None,
    ) -> None:
        self.service = service
        self.transcriber = transcriber
        self.owner_id = owner_id or f"voice-worker-{uuid4().hex}"
        self.poll_interval_seconds = max(0.05, float(poll_interval_seconds))
        self.transcribe_timeout_seconds = transcribe_timeout_seconds
        self.lease_seconds = lease_seconds
        self.notify_events = notify_events
        self._wake = wake if wake is not None else threading.Event()
        self._stop = threading.Event()
        self._cancel_work = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._retry_at: dict[str, float] = {}
        self._idle_confirmed = False
        self._state: dict[str, Any] = {
            "enabled": transcriber is not None,
            "running": False,
            "completed_count": 0,
            "failed_count": 0,
            "blocked_count": 0,
            "retry_count": 0,
            "recovered_count": 0,
            "last_error_code": None,
            **error_fields(),
        }

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        with self._lock:
            if not self._state["enabled"] or self._thread is not None:
                return
            self._stop.clear()
            self._cancel_work.clear()
            thread = threading.Thread(
                target=self._run, name="sightglass-voice-worker", daemon=True,
            )
            self._thread = thread
        thread.start()

    def stop(self) -> None:
        with self._lock:
            self._stop.set()
            self._cancel_work.set()
            self._wake.set()
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=VOICE_WORKER_STOP_TIMEOUT_SECONDS)

    def wake(self) -> None:
        self._wake.set()

    def status(self) -> VoiceWorkerStatus:
        with self._lock:
            return VoiceWorkerStatus(
                schema="sightglass.voice-worker-status.v1",
                enabled=bool(self._state["enabled"]),
                running=bool(self._state["running"]),
                completed_count=int(self._state["completed_count"]),
                failed_count=int(self._state["failed_count"]),
                blocked_count=int(self._state["blocked_count"]),
                retry_count=int(self._state["retry_count"]),
                recovered_count=int(self._state["recovered_count"]),
                last_error_code=self._state["last_error_code"],
                **{key: self._state[key] for key in error_fields()},
            )

    # -- loop --------------------------------------------------------------

    def _run(self) -> None:
        with self._lock:
            self._state["running"] = True
        try:
            while not self._stop.is_set():
                self._wake.clear()
                try:
                    self._recover_once()
                    worked = self._work_one_pending()
                    if self._idle_confirmed:
                        with self._lock:
                            clear_error(self._state)
                    timeout = next_wait(
                        self.poll_interval_seconds, self._retry_at, self.service.next_lease_delay()
                    )
                except Exception as exc:
                    self._record_error(exc)
                    worked = False
                    timeout = self.poll_interval_seconds
                if self._stop.is_set():
                    break
                if worked:
                    with self._lock:
                        self._state["work_count"] += 1
                    continue
                with self._lock:
                    self._state["idle_cycle_count"] += 1
                if self._wake.wait(timeout=timeout) and not self._stop.is_set():
                    with self._lock:
                        self._state["wake_count"] += 1
        finally:
            with self._lock:
                self._state["running"] = False
                self._thread = None

    def _recover_once(self) -> None:
        recovered = self.service.recover_expired_leases()
        if recovered:
            with self._lock:
                self._state["recovered_count"] = (
                    int(self._state["recovered_count"]) + recovered
                )
            self._notify()

    def _work_one_pending(self) -> bool:
        assert self.transcriber is not None
        self._idle_confirmed = False
        candidates = self.service.repository.pending_jobs(
            VOICE_WORKER_LEASE_CANDIDATES, exclude=retry_exclusions(self._retry_at)
        )
        if not candidates:
            self._idle_confirmed = not self._retry_at
            return False
        if self.service.repository.database.storage is not None:
            self.service.repository.database.storage.require(background=True)
        for job in candidates:
            if self._stop.is_set():
                return False
            try:
                self._drive_job(job)
            except SightglassError as exc:
                self._record_error(exc)
                continue
            return True
        return False

    def _drive_job(self, job: dict[str, Any]) -> None:
        """Lease and finalize one job.  Raises only while the lease is not yet held."""
        assert self.transcriber is not None
        job_id = str(job["job_id"])
        attempt = int(job["attempt"]) + 1
        reserved_ms = int(job["max_duration_ms"] or 0)
        lease_seconds = self.lease_seconds
        if lease_seconds is None:
            lease_seconds = max(
                VOICE_WORKER_MIN_LEASE_SECONDS,
                reserved_ms // 1000 + VOICE_WORKER_LEASE_MARGIN_SECONDS,
            )
        fence = self.service.lease(job_id, owner_id=self.owner_id, lease_seconds=lease_seconds)
        self._retry_at.pop(job_id, None)
        self._notify()
        ceiling = max(0.1, float(lease_seconds) - 1.0)
        timeout = self.transcribe_timeout_seconds
        timeout = ceiling if timeout is None else max(0.1, min(float(timeout), ceiling))
        deadline = time.monotonic() + timeout
        try:
            with operation_budget(timeout, cancelled=self._cancel_work):
                produced = self.transcriber.transcribe(
                    job, duration_ms=reserved_ms, deadline=deadline,
                )
                text, provenance = transcription_parts(produced)
                check_operation_budget()
        except SightglassError as exc:
            if self._stop.is_set():
                return
            if exc.details.get("reason") in {"operation_cancelled", "operation_deadline"}:
                self._record_error(exc, code="TRANSCRIBE_TIMEOUT")
                self._finalize_failure(job_id, fence, "TRANSCRIBE_TIMEOUT")
            else:
                self._record_error(exc)
                self._finalize_recognizer_error(job_id, fence, attempt, exc)
            return
        except Exception as exc:
            if self._stop.is_set():
                return
            self._record_error(exc, code="TRANSCRIBE_FAILED")
            self._finalize_failure(job_id, fence, "TRANSCRIBE_FAILED")
            return
        with self._lock:
            if self._stop.is_set():
                return
            try:
                self.service.complete(
                    job_id, owner_id=self.owner_id, fencing_token=fence, text=text,
                    provenance=provenance,
                )
            except SightglassError as exc:
                self._record_error(exc)
                if exc.code == ErrorCode.STORAGE_PRESSURE:
                    self._finalize_recognizer_error(job_id, fence, attempt, exc)
                return
            self._state["completed_count"] = int(self._state["completed_count"]) + 1
            clear_error(self._state)
        self._notify()

    def _finalize_recognizer_error(
        self, job_id: str, fence: int, attempt: int, exc: SightglassError,
    ) -> None:
        if exc.code == ErrorCode.STORAGE_PRESSURE:
            self.service.requeue(
                job_id, owner_id=self.owner_id, fencing_token=fence, storage_pressure=True,
            )
            self._retry_at[job_id] = time.monotonic() + VOICE_WORKER_RETRY_MAX_BACKOFF_SECONDS
            self._notify()
            return
        if exc.code == ErrorCode.RESOURCE_BLOCKED:
            self._finalize_failure(job_id, fence, exc.code.value, state="blocked")
            return
        if exc.retryable and attempt < VOICE_WORKER_MAX_ATTEMPTS:
            self._requeue(job_id, fence, attempt)
            return
        self._finalize_failure(job_id, fence, exc.code.value)

    def _finalize_failure(
        self, job_id: str, fence: int, error_code: str, *, state: str = "failed",
    ) -> None:
        try:
            self.service.fail(
                job_id, owner_id=self.owner_id, fencing_token=fence,
                error_code=error_code, state=state,
            )
        except SightglassError as exc:
            self._record_error(exc)
            return
        with self._lock:
            counter = "failed_count" if state == "failed" else "blocked_count"
            self._state[counter] = int(self._state[counter]) + 1
            self._state["last_error_code"] = error_code
        self._notify()

    def _requeue(self, job_id: str, fence: int, attempt: int) -> None:
        try:
            self.service.requeue(job_id, owner_id=self.owner_id, fencing_token=fence)
        except SightglassError as exc:
            self._record_error(exc)
            return
        backoff = min(
            VOICE_WORKER_RETRY_MAX_BACKOFF_SECONDS,
            VOICE_WORKER_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)),
        )
        self._retry_at[job_id] = time.monotonic() + backoff
        if len(self._retry_at) > VOICE_WORKER_RETRY_TRACKED_JOBS:
            now = time.monotonic()
            self._retry_at = {key: value for key, value in self._retry_at.items() if value > now}
        with self._lock:
            self._state["retry_count"] = int(self._state["retry_count"]) + 1
        self._notify()

    def _notify(self) -> None:
        if self.notify_events is not None:
            self.notify_events()

    def _record_error(self, exc: Exception, *, code: str | None = None) -> None:
        if code is None:
            code = (
                exc.code.value if isinstance(exc, SightglassError)
                else ErrorCode.INTERNAL_ERROR.value
            )
        with self._lock:
            record_error(self._state, exc, code)

from __future__ import annotations

import threading
import time
from dataclasses import asdict, dataclass
from typing import Any
from uuid import uuid4

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.operations import check_operation_budget, operation_budget
from sightglass.resources.jobs import (
    RESOURCE_ATTEMPTS_EXHAUSTED,
    ResourceJobService,
)
from sightglass.resources.service import ResourceService
from sightglass.runtime.worker_diagnostics import (
    WorkerDiagnostics,
    clear_error,
    error_fields,
    record_error,
)
from sightglass.runtime.worker_wait import next_wait, retry_exclusions

RESOURCE_WORKER_STOP_TIMEOUT_SECONDS = 10.0
RESOURCE_WORKER_POLL_INTERVAL_SECONDS = 30.0
RESOURCE_WORKER_LEASE_SECONDS = 120
RESOURCE_WORKER_OPERATION_TIMEOUT_SECONDS = 90.0
RESOURCE_WORKER_MAX_ATTEMPTS = 3
RESOURCE_WORKER_LEASE_CANDIDATES = 8
RESOURCE_WORKER_RETRY_BACKOFF_SECONDS = 0.5


@dataclass(frozen=True)
class ResourceWorkerStatus(WorkerDiagnostics):
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


class ResourceWorker:
    """Bounded durable worker for resource derivations that outlive one tool call."""

    def __init__(
        self,
        service: ResourceService,
        jobs: ResourceJobService,
        *,
        owner_id: str | None = None,
        wake: threading.Event | None = None,
        poll_interval_seconds: float = RESOURCE_WORKER_POLL_INTERVAL_SECONDS,
    ) -> None:
        self.service = service
        self.jobs = jobs
        self.owner_id = owner_id or f"resource-worker-{uuid4().hex}"
        self.poll_interval_seconds = max(0.05, float(poll_interval_seconds))
        self._wake = wake or threading.Event()
        self._stop = threading.Event()
        self._cancel_work = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._retry_at: dict[str, float] = {}
        self._idle_confirmed = False
        self._state: dict[str, Any] = {
            "enabled": True,
            "running": False,
            "completed_count": 0,
            "failed_count": 0,
            "blocked_count": 0,
            "retry_count": 0,
            "recovered_count": 0,
            "last_error_code": None,
            **error_fields(),
        }

    def start(self) -> None:
        with self._lock:
            if self._thread is not None:
                return
            self._stop.clear()
            self._cancel_work.clear()
            thread = threading.Thread(
                target=self._run,
                name="sightglass-resource-worker",
                daemon=True,
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
            thread.join(timeout=RESOURCE_WORKER_STOP_TIMEOUT_SECONDS)

    def wake(self) -> None:
        self._wake.set()

    def status(self) -> ResourceWorkerStatus:
        with self._lock:
            return ResourceWorkerStatus(
                schema="sightglass.resource-worker-status.v1",
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

    def _run(self) -> None:
        with self._lock:
            self._state["running"] = True
        try:
            while not self._stop.is_set():
                # Consume the prior wake before observing durable work. A commit
                # during either query/drive remains signalled through the wait.
                self._wake.clear()
                try:
                    recovered = self.jobs.recover_expired_leases()
                    if recovered:
                        with self._lock:
                            self._state["recovered_count"] += recovered
                    worked = self._work_one()
                    if self._idle_confirmed:
                        with self._lock:
                            clear_error(self._state)
                    timeout = next_wait(
                        self.poll_interval_seconds, self._retry_at, self.jobs.next_lease_delay()
                    )
                except Exception as exc:  # the loop must remain available after one bad row
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

    def _work_one(self) -> bool:
        database = self.jobs.repository.database
        self._idle_confirmed = False
        candidates = self.jobs.pending_jobs(
            RESOURCE_WORKER_LEASE_CANDIDATES, exclude=retry_exclusions(self._retry_at)
        )
        if not candidates:
            self._idle_confirmed = not self._retry_at
            return False
        if database.storage is not None:
            database.storage.require(background=True)
        for job in candidates:
            if self._stop.is_set():
                return False
            try:
                self._drive(job)
            except SightglassError as exc:
                self._record_error(exc)
                continue
            return True
        return False

    def _drive(self, job: dict[str, Any]) -> None:
        job_id = str(job["job_id"])
        if self.jobs.exhaust_pending(job_id):
            # Crash recovery returned a row whose durable attempt budget is already
            # spent: retire it here rather than leasing it into another doomed run.
            with self._lock:
                self._state["failed_count"] += 1
                self._state["last_error_code"] = RESOURCE_ATTEMPTS_EXHAUSTED
            return
        attempt = int(job["attempt"]) + 1
        fence = self.jobs.lease(
            job_id,
            owner_id=self.owner_id,
            lease_seconds=RESOURCE_WORKER_LEASE_SECONDS,
        )
        self._retry_at.pop(job_id, None)
        try:
            self.jobs.mark_running(
                job_id, owner_id=self.owner_id, fencing_token=fence
            )
            request = self.jobs.request_for(dict(self.jobs.by_id(job_id) or job))
            with operation_budget(
                RESOURCE_WORKER_OPERATION_TIMEOUT_SECONDS,
                cancelled=self._cancel_work,
            ):
                payload = self.service.read_resource(
                    resource_id=request.resource_id,
                    mode=request.mode,
                    page=request.page,
                    start_line=request.start_line,
                    end_line=request.end_line,
                    max_bytes=request.max_bytes,
                    member=request.member,
                    sheet=request.sheet,
                    cell_range=request.cell_range,
                    allow_async=False,
                    publish_guard=lambda: self.jobs.verify(
                        job_id,
                        owner_id=self.owner_id,
                        fencing_token=fence,
                    ),
                )
                check_operation_budget()
                if payload.descriptor.get("state") == "processing":
                    raise SightglassError(ErrorCode.INTERNAL_ERROR)
            self.jobs.complete(
                job_id, owner_id=self.owner_id, fencing_token=fence
            )
        except SightglassError as exc:
            if self._stop.is_set():
                return
            self._record_error(exc)
            self._finalize_error(job_id, fence, attempt, exc)
            return
        except Exception as exc:
            if self._stop.is_set():
                return
            self._record_error(exc)
            self._fail(job_id, fence, ErrorCode.INTERNAL_ERROR.value)
            return
        with self._lock:
            self._state["completed_count"] += 1
            clear_error(self._state)

    def _finalize_error(
        self, job_id: str, fence: int, attempt: int, exc: SightglassError
    ) -> None:
        if exc.code == ErrorCode.RESOURCE_BLOCKED:
            self._fail(job_id, fence, exc.code.value, state="blocked")
            return
        if exc.retryable and attempt < RESOURCE_WORKER_MAX_ATTEMPTS:
            try:
                self.jobs.requeue(
                    job_id,
                    owner_id=self.owner_id,
                    fencing_token=fence,
                    storage_pressure=exc.code == ErrorCode.STORAGE_PRESSURE,
                )
            except SightglassError as requeue_error:
                self._record_error(requeue_error)
                return
            self._retry_at[job_id] = time.monotonic() + RESOURCE_WORKER_RETRY_BACKOFF_SECONDS
            with self._lock:
                self._state["retry_count"] += 1
                self._state["last_error_code"] = exc.code.value
            return
        self._fail(job_id, fence, exc.code.value)

    def _fail(
        self, job_id: str, fence: int, error_code: str, *, state: str = "failed"
    ) -> None:
        try:
            self.jobs.fail(
                job_id,
                owner_id=self.owner_id,
                fencing_token=fence,
                error_code=error_code,
                state=state,
            )
        except SightglassError as exc:
            self._record_error(exc)
            return
        with self._lock:
            counter = "blocked_count" if state == "blocked" else "failed_count"
            self._state[counter] += 1
            self._state["last_error_code"] = error_code

    def _record_error(self, exc: Exception) -> None:
        code = exc.code.value if isinstance(exc, SightglassError) else exc.__class__.__name__
        with self._lock:
            record_error(self._state, exc, code)

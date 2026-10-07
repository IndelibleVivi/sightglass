"""Independent bounded semantic publication; never owns source or reader progress."""

from __future__ import annotations

import threading
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.operations import operation_budget
from sightglass.runtime.worker_diagnostics import clear_error, error_fields, record_error
from sightglass.semantic.service import SemanticService


class SemanticWorker:
    def __init__(self, service: SemanticService | None, *, poll_seconds: float = 30.0) -> None:
        self.service = service
        self.poll_seconds = max(0.05, float(poll_seconds))
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._state: dict[str, Any] = {
            "enabled": service is not None,
            "running": False,
            "completed_batches": 0,
            "last_error_code": None,
            "paused_for_storage": False,
            **error_fields(),
        }

    def start(self) -> None:
        if self.service is None or self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="sightglass-semantic", daemon=True)
        self._thread.start()

    def stop(self) -> bool:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            # Each remote request has the configured timeout and the next step checks cancellation.
            timeout = self.service.settings.timeout_seconds + 2 if self.service else 5
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                return False
            self._thread = None
        return True

    def wake(self) -> None:
        self._wake.set()

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {"schema": "sightglass.semantic-worker.v1", **self._state}

    def run_once(self) -> bool:
        if self.service is None or self.service.reader.paused:
            return False
        try:
            with operation_budget(60.0, cancelled=self._stop):
                result = self.service.index_once(limit=32)
            with self._lock:
                self._state.update(
                    completed_batches=self._state["completed_batches"] + 1,
                    last_error_code=None,
                    paused_for_storage=False,
                )
                clear_error(self._state)
                self._state["work_count"] += 1
            # Poll pending publication and failures with backoff; never hammer an async mutation.
            return result.get("state") == "building" and result.get("scanned", 0) > 0
        except SightglassError as exc:
            with self._lock:
                record_error(self._state, exc, exc.code.value)
                self._state["paused_for_storage"] = exc.code == ErrorCode.STORAGE_PRESSURE
            return False

    def _run(self) -> None:
        with self._lock:
            self._state["running"] = True
        try:
            while not self._stop.is_set():
                self._wake.clear()
                try:
                    more = self.run_once()
                except Exception as exc:
                    with self._lock:
                        record_error(self._state, exc, ErrorCode.INTERNAL_ERROR.value)
                    more = False
                if not more:
                    with self._lock:
                        self._state["idle_cycle_count"] += 1
                    if self._wake.wait(timeout=self.poll_seconds) and not self._stop.is_set():
                        with self._lock:
                            self._state["wake_count"] += 1
        finally:
            with self._lock:
                self._state["running"] = False

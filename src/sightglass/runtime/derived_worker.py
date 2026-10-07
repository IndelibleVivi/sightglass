"""Bounded local derivative backfill; never opens the source or advances reader state."""

from __future__ import annotations

import threading
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.links import LinkRepository
from sightglass.operations import operation_budget
from sightglass.policy.readers import ReaderContext
from sightglass.runtime.worker_diagnostics import clear_error, error_fields, record_error


class DerivedIndexWorker:
    def __init__(
        self, links: LinkRepository, reader: ReaderContext, *, poll_seconds: float = 30.0
    ) -> None:
        self.links = links
        self.reader = reader
        self.poll_seconds = max(0.05, float(poll_seconds))
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._state: dict[str, Any] = {
            "running": False,
            "completed_batches": 0,
            "processed_messages": 0,
            "last_error_code": None,
            "paused_for_storage": False,
            **error_fields(),
        }

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="sightglass-derived-index", daemon=True
        )
        self._thread.start()

    def stop(self) -> bool:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            if self._thread.is_alive():
                return False
            self._thread = None
        return True

    def wake(self) -> None:
        self._wake.set()

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "schema": "sightglass.derived-worker.v1",
                "paused": self.reader.paused,
                **self._state,
            }

    def run_once(self) -> bool:
        if self.reader.paused:
            return False
        if self.links.state()["state"] == "ready" and not self.links.has_pending_backfill():
            with self._lock:
                clear_error(self._state)
                self._state["paused_for_storage"] = False
            return False
        try:
            storage = self.links.database.storage
            if storage is not None:
                storage.require(256 * 1024, background=True)
            with operation_budget(3.0, cancelled=self._stop):
                result = self.links.backfill_batch(limit=100)
            with self._lock:
                self._state.update(
                    completed_batches=self._state["completed_batches"] + 1,
                    processed_messages=self._state["processed_messages"] + result["processed"],
                    last_error_code=None,
                    paused_for_storage=False,
                )
                clear_error(self._state)
                self._state["work_count"] += 1
            return result["state"] != "ready"
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

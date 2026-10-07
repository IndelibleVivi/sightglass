from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from typing import Any

RECEIPT_QUEUE_CAPACITY = 4096


class AsyncReceiptWriter:
    """Serialize private receipt writes without delaying reader responses."""

    def __init__(
        self,
        persist: Callable[..., None],
        *,
        capacity: int = RECEIPT_QUEUE_CAPACITY,
    ) -> None:
        self._persist = persist
        self._capacity = max(1, int(capacity))
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=self._capacity)
        self._lock = threading.Lock()
        self._stopping = False
        self._persisted = 0
        self._failed = 0
        self._dropped = 0
        self._active = False
        self._thread = threading.Thread(
            target=self._run,
            name="sightglass-receipts",
            daemon=True,
        )
        self._thread.start()

    def __call__(self, **values: Any) -> None:
        with self._lock:
            if self._stopping:
                return
            try:
                self._queue.put_nowait(dict(values))
            except queue.Full:
                self._dropped += 1

    def _run(self) -> None:
        while True:
            try:
                task = self._queue.get(timeout=0.1)
            except queue.Empty:
                with self._lock:
                    if self._stopping:
                        return
                continue
            try:
                with self._lock:
                    self._active = True
                try:
                    self._persist(**task)
                except Exception:
                    with self._lock:
                        self._failed += 1
                else:
                    with self._lock:
                        self._persisted += 1
                finally:
                    with self._lock:
                        self._active = False
            finally:
                self._queue.task_done()

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "schema": "sightglass.receipt-writer-status.v1",
                "pending_count": self._queue.qsize(),
                "capacity": self._capacity,
                "active": self._active,
                "alive": self._thread.is_alive(),
                "persisted_count": self._persisted,
                "failed_count": self._failed,
                "dropped_count": self._dropped,
            }

    def close(self, *, timeout: float = 10.0) -> None:
        with self._lock:
            if self._stopping:
                return
            self._stopping = True
        if self._thread is not threading.current_thread():
            self._thread.join(timeout=timeout)

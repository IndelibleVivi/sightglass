from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from typing import Any

RECEIPT_QUEUE_CAPACITY = 4096
RECEIPT_STOP_TIMEOUT_SECONDS = 10.0


class AsyncReceiptWriter:
    """Serialize private receipt writes without delaying reader responses."""

    def __init__(
        self,
        persist: Callable[..., None],
        *,
        capacity: int = RECEIPT_QUEUE_CAPACITY,
        autostart: bool = True,
    ) -> None:
        self._persist = persist
        self._capacity = max(1, int(capacity))
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=self._capacity)
        self._lock = threading.Lock()
        self._lifecycle_lock = threading.RLock()
        self._stopping = True
        self._closed = False
        self._persisted = 0
        self._failed = 0
        self._dropped = 0
        self._active = False
        self._thread: threading.Thread | None = None
        if autostart:
            self.start()

    def start(self) -> None:
        """Start one owner, or resume only after its predecessor has stopped."""
        with self._lifecycle_lock:
            with self._lock:
                if self._closed:
                    raise RuntimeError("receipt writer is closed")
                if self._thread is not None and self._thread.is_alive():
                    if self._stopping:
                        raise RuntimeError("receipt writer did not stop")
                    return
                self._stopping = False
                self._thread = threading.Thread(
                    target=self._run, name="sightglass-receipts", daemon=True,
                )
                thread = self._thread
            thread.start()

    def __call__(self, **values: Any) -> None:
        with self._lock:
            if self._stopping:
                self._dropped += 1
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
                    if self._stopping and self._queue.empty():
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
                "alive": self._thread is not None and self._thread.is_alive(),
                "persisted_count": self._persisted,
                "failed_count": self._failed,
                "dropped_count": self._dropped,
            }

    def stop(self, *, timeout: float | None = None) -> None:
        """Drain accepted receipts and confirm quiescence; failed joins are retryable."""
        with self._lifecycle_lock:
            with self._lock:
                self._stopping = True
                thread = self._thread
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=RECEIPT_STOP_TIMEOUT_SECONDS if timeout is None else timeout)
            if thread is not None and thread.is_alive():
                raise RuntimeError("receipt writer did not stop")
            if not self._queue.empty():
                raise RuntimeError("receipt writer has undrained receipts")

    def close(self, *, timeout: float | None = None) -> None:
        # Rejoin after a failed close; stopping alone never proves quiescence.
        with self._lifecycle_lock:
            self.stop(timeout=timeout)
            with self._lock:
                self._closed = True

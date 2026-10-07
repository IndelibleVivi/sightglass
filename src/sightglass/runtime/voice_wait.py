from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

VOICE_WAIT_DEFAULT_MS = 8_000
VOICE_WAIT_CEILING_MS = 15_000
VOICE_WAIT_POLL_SECONDS = 0.05
VOICE_MAX_WAITERS = 2


def bound_wait_ms(requested: Any, *, ceiling_ms: int = VOICE_WAIT_CEILING_MS) -> int | None:
    """Clamp a requested transcript wait; ``None`` means the request is invalid."""

    if requested is None:
        value = VOICE_WAIT_DEFAULT_MS
    elif type(requested) is int:
        value = requested
    else:
        return None
    if value < 0:
        return None
    return min(value, max(0, int(ceiling_ms)))


class VoiceWaiters:
    """Bounded park/notify channel between the voice worker and transcript readers.

    A reader parks only while holding one waiter slot and no state gate, so daemon
    status, other reader tools, and operator mutations stay responsive and can wake
    it; a caller that disconnects frees its slot without waiting out the deadline.
    """

    def __init__(self, max_waiters: int = VOICE_MAX_WAITERS) -> None:
        self.max_waiters = max(1, int(max_waiters))
        self._condition = threading.Condition()
        self._revision = 0
        self._active = 0
        self._counts: dict[str, int] = {
            "woken_count": 0,
            "timeout_count": 0,
            "aborted_count": 0,
            "rejected_count": 0,
        }

    def acquire(self) -> bool:
        with self._condition:
            if self._active >= self.max_waiters:
                self._counts["rejected_count"] += 1
                return False
            self._active += 1
            return True

    def release(self) -> None:
        with self._condition:
            self._active = max(0, self._active - 1)
            self._condition.notify_all()

    def notify(self) -> None:
        """Wake parked readers; they re-verify their own batch and current policy."""

        with self._condition:
            self._revision += 1
            self._condition.notify_all()

    def wait(
        self, timeout_seconds: float, *, probe: Callable[[], bool] | None = None,
    ) -> str:
        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        with self._condition:
            revision = self._revision
        while True:
            remaining = max(0.0, deadline - time.monotonic())
            with self._condition:
                notified = self._condition.wait(
                    timeout=min(VOICE_WAIT_POLL_SECONDS, remaining)
                )
                woken = bool(notified and self._revision != revision)
            if woken:
                outcome = "woken"
                break
            if probe is not None and probe():
                outcome = "aborted"
                break
            if time.monotonic() >= deadline:
                outcome = "timeout"
                break
        with self._condition:
            self._counts[f"{outcome}_count"] += 1
        return outcome

    def status(self) -> dict[str, Any]:
        with self._condition:
            active = self._active
            counts = dict(self._counts)
        return {
            "schema": "sightglass.voice-waiters-status.v1",
            "active_waiters": active,
            "max_waiters": self.max_waiters,
            "available": active < self.max_waiters,
            **counts,
        }

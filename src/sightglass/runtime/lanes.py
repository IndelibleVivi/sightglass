"""Explicit runtime work classes with independent bounded capacities.

The daemon classifies every operation into one content-free work lane before it runs.
Lane capacities are independent, so a saturated source read or resource derivation can
never consume the capacity reserved for materialized ``window.db`` reads, warm-CAS
reads, exact pending replay/ACK, and committed transcript reads. The window writer and
the transcript waiter budget are reported through the same surface even though their
capacity is owned elsewhere.

Lanes never carry arguments, paths, labels, digests, or content: status exposes only
capacity and counters.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from sightglass.operations import check_operation_budget


class WorkClass(StrEnum):
    """One bounded runtime work class."""

    LOCAL_READ = "local_read"
    SEMANTIC_READ = "semantic_read"
    SOURCE_READ = "source_read"
    RESOURCE_DERIVATION = "resource_derivation"
    WINDOW_WRITE = "window_write"
    WAIT_POLL = "wait_poll"


@dataclass(frozen=True)
class LaneLimits:
    """Process-wide capacity for each work class.

    ``local_read`` and ``source_read`` are deliberately separate: materialized rows and
    warm CAS bindings are answered without a provider, so they must never queue behind
    a slow live read. ``window_write`` and ``wait_poll`` are single-owner surfaces
    reported here, not acquired here.
    """

    local_read: int = 6
    semantic_read: int = 2
    source_read: int = 3
    resource_derivation: int = 2
    window_write: int = 1
    wait_poll: int = 2

    def capacity(self, lane: WorkClass) -> int:
        return max(1, int(getattr(self, lane.value)))

    def as_dict(self) -> dict[str, int]:
        return {lane.value: self.capacity(lane) for lane in WorkClass}


class LaneLease:
    """One held lane slot; releasing twice is a no-op."""

    def __init__(self, lanes: RuntimeLanes, lane: WorkClass) -> None:
        self._lanes = lanes
        self.lane = lane
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._lanes.release(self.lane)

    def __enter__(self) -> LaneLease:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()


class RuntimeLanes:
    """Content-free, process-wide capacity registry for the daemon work classes."""

    def __init__(self, limits: LaneLimits | None = None) -> None:
        self.limits = limits or LaneLimits()
        self._lock = threading.Lock()
        self._semaphores = {
            lane: threading.BoundedSemaphore(self.limits.capacity(lane)) for lane in WorkClass
        }
        self._active = dict.fromkeys(WorkClass, 0)
        self._waiting = dict.fromkeys(WorkClass, 0)
        self._completed = dict.fromkeys(WorkClass, 0)
        self._busy = dict.fromkeys(WorkClass, 0)
        self._deferred = dict.fromkeys(WorkClass, 0)

    # -- acquisition -------------------------------------------------------

    def try_acquire(self, lane: WorkClass) -> LaneLease | None:
        """Acquire when a slot is immediately free; never blocks."""

        with self._lock:
            acquired = self._semaphores[lane].acquire(blocking=False)
            if not acquired:
                self._busy[lane] += 1
                return None
            self._active[lane] += 1
        return LaneLease(self, lane)

    def acquire(self, lane: WorkClass) -> LaneLease:
        """Block for a slot while cooperating with the current operation deadline."""

        with self._lock:
            self._waiting[lane] += 1
        try:
            while not self._semaphores[lane].acquire(timeout=0.05):
                check_operation_budget()
        finally:
            with self._lock:
                self._waiting[lane] -= 1
        with self._lock:
            self._active[lane] += 1
        return LaneLease(self, lane)

    @contextmanager
    def held(self, lane: WorkClass, *, wait: bool) -> Iterator[LaneLease | None]:
        """Acquire a lane for the duration of the block.

        ``wait=False`` yields ``None`` instead of blocking when the lane is saturated,
        so a caller can return a retryable busy/deferred result without queueing.
        """

        lease = self.acquire(lane) if wait else self.try_acquire(lane)
        try:
            yield lease
        finally:
            if lease is not None:
                lease.release()

    def release(self, lane: WorkClass) -> None:
        with self._lock:
            self._active[lane] = max(0, self._active[lane] - 1)
            self._completed[lane] += 1
        self._semaphores[lane].release()

    # -- bookkeeping -------------------------------------------------------

    def note_deferred(self, lane: WorkClass) -> None:
        """Record a saturated lane that a caller deferred instead of failing."""

        with self._lock:
            self._deferred[lane] += 1

    def status(
        self, supplements: dict[WorkClass, dict[str, Any]] | None = None
    ) -> dict[str, Any]:
        """Return the content-free lane status.

        ``supplements`` let the owner of a lane whose capacity lives elsewhere
        (``window_write`` or ``wait_poll``) contribute live counters.
        """

        supplements = supplements or {}
        with self._lock:
            lanes: dict[str, dict[str, Any]] = {}
            for lane in WorkClass:
                capacity = self.limits.capacity(lane)
                entry: dict[str, Any] = {
                    "capacity": capacity,
                    "active": self._active[lane],
                    "waiting": self._waiting[lane],
                    "completed_count": self._completed[lane],
                    "busy_count": self._busy[lane],
                    "deferred_count": self._deferred[lane],
                }
                entry.update(supplements.get(lane, {}))
                entry["saturated"] = int(entry.get("active", 0)) >= int(
                    entry.get("capacity", capacity)
                )
                lanes[lane.value] = entry
            total_active = sum(int(entry.get("active", 0)) for entry in lanes.values())
            total_capacity = sum(int(entry.get("capacity", 0)) for entry in lanes.values())
        return {
            "schema": "sightglass.work-lanes.v1",
            "limits": self.limits.as_dict(),
            "lanes": lanes,
            "active_count": total_active,
            "capacity": total_capacity,
            "saturated_lanes": sorted(
                name for name, entry in lanes.items() if entry.get("saturated")
            ),
        }

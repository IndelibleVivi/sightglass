"""Small deadline calculation shared by the durable resource and voice lanes."""

from __future__ import annotations

import time


def retry_exclusions(retry_at: dict[str, float]) -> tuple[str, ...]:
    now = time.monotonic()
    for key in tuple(retry_at):
        if retry_at[key] <= now:
            del retry_at[key]
    return tuple(retry_at)


def next_wait(fallback: float, retry_at: dict[str, float], lease_delay: float | None) -> float:
    now = time.monotonic()
    deadlines = [fallback]
    deadlines.extend(max(0.0, deadline - now) for deadline in retry_at.values())
    if lease_delay is not None:
        deadlines.append(max(0.0, lease_delay))
    # A due lease is checked on the next cycle; avoid a CPU spin on wall-clock
    # precision boundaries while retaining prompt recovery.
    return max(0.01, min(deadlines))

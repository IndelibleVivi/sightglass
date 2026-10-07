from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from sightglass.contracts.errors import ErrorCode, SightglassError


@dataclass(frozen=True)
class OperationBudget:
    deadline: float | None
    cancelled: threading.Event | None = None


_CURRENT_BUDGET: ContextVar[OperationBudget | None] = ContextVar(
    "sightglass_operation_budget", default=None
)
_LOCAL_READ_ONLY: ContextVar[bool] = ContextVar(
    "sightglass_local_read_only", default=False
)


@contextmanager
def operation_budget(
    timeout_seconds: float | None,
    *,
    cancelled: threading.Event | None = None,
) -> Iterator[None]:
    """Bound cooperative work in the current thread and all nested call layers."""

    current = _CURRENT_BUDGET.get()
    deadline = (
        time.monotonic() + max(0.0, float(timeout_seconds)) if timeout_seconds is not None else None
    )
    if current is not None:
        if current.deadline is not None:
            deadline = current.deadline if deadline is None else min(current.deadline, deadline)
        if cancelled is None:
            cancelled = current.cancelled
    token = _CURRENT_BUDGET.set(OperationBudget(deadline, cancelled))
    try:
        check_operation_budget()
        yield
    finally:
        _CURRENT_BUDGET.reset(token)


@contextmanager
def local_read_only_scope() -> Iterator[None]:
    """Require nested reader services to stay on the materialized local plane.

    The daemon sets this only after classifying an exact tool call while holding the
    runtime state read gate.  If the local premise disappears before a service reaches
    its read, the service must fail closed instead of silently touching the live source
    without foreground ownership.
    """

    token = _LOCAL_READ_ONLY.set(True)
    try:
        yield
    finally:
        _LOCAL_READ_ONLY.reset(token)


def local_read_only_requested() -> bool:
    return _LOCAL_READ_ONLY.get()


def operation_expired() -> bool:
    budget = _CURRENT_BUDGET.get()
    if budget is None:
        return False
    return bool(
        (budget.cancelled is not None and budget.cancelled.is_set())
        or (budget.deadline is not None and time.monotonic() >= budget.deadline)
    )


def operation_cancelled() -> bool:
    budget = _CURRENT_BUDGET.get()
    return bool(budget is not None and budget.cancelled is not None and budget.cancelled.is_set())


def operation_remaining_seconds() -> float | None:
    budget = _CURRENT_BUDGET.get()
    if budget is None or budget.deadline is None:
        return None
    return max(0.0, budget.deadline - time.monotonic())


def check_operation_budget() -> None:
    if operation_expired():
        raise SightglassError(
            ErrorCode.SERVICE_TIMEOUT,
            retryable=True,
            details={
                "reason": "operation_cancelled" if operation_cancelled() else "operation_deadline"
            },
        )


def wait_for_event(event: threading.Event, *, interval_seconds: float = 0.05) -> None:
    while not event.wait(timeout=interval_seconds):
        check_operation_budget()
    check_operation_budget()

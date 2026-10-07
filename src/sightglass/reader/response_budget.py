"""Request-local transport budgets; page owners still choose their continuations."""
from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ResponseBudget:
    max_bytes: int
    project: Callable[[dict[str, Any]], dict[str, Any]]
    reserve_bytes: int = 1024

    def size(self, value: dict[str, Any]) -> int:
        return len(json.dumps(self.project(value), ensure_ascii=False,
                              separators=(",", ":"), sort_keys=True).encode("utf-8"))

    @property
    def available_bytes(self) -> int:
        return max(0, self.max_bytes - self.reserve_bytes)


_ACTIVE: ContextVar[ResponseBudget | None] = ContextVar("response_budget", default=None)


def active_response_budget() -> ResponseBudget | None:
    return _ACTIVE.get()


@contextmanager
def response_budget(budget: ResponseBudget | None) -> Iterator[None]:
    token = _ACTIVE.set(budget)
    try:
        yield
    finally:
        _ACTIVE.reset(token)

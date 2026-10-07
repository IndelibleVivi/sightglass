"""Content-free current and historical worker failures.

Only exception classes and public Python locations are retained. Never render an
exception, its arguments, traceback filenames, or frame data into worker status.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

_PUBLIC_MODULE = re.compile(r"sightglass(?:\.[A-Za-z_][A-Za-z_0-9]*)+\Z")
_FUNCTION = re.compile(r"[A-Za-z_][A-Za-z_0-9]*\Z")


def public_error_reason(exc: Exception) -> str | None:
    # These are the worker's own cooperative budget classifications. Provider or
    # row-specific detail strings remain private even if supplied as a "reason".
    details = getattr(exc, "details", {})
    reason = details.get("reason") if isinstance(details, dict) else None
    return (
        reason
        if isinstance(reason, str) and reason in {"operation_cancelled", "operation_deadline"}
        else None
    )


@dataclass(frozen=True)
class WorkerDiagnostics:
    last_error_type: str | None
    last_error_location: dict[str, Any] | None
    historical_error_count: int
    historical_error_code: str | None
    historical_error_type: str | None
    historical_error_location: dict[str, Any] | None
    idle_cycle_count: int
    work_count: int
    wake_count: int


def error_fields() -> dict[str, Any]:
    return {
        "last_error_type": None,
        "last_error_location": None,
        "historical_error_count": 0,
        "historical_error_code": None,
        "historical_error_type": None,
        "historical_error_location": None,
        "idle_cycle_count": 0,
        "work_count": 0,
        "wake_count": 0,
    }


def record_error(state: dict[str, Any], exc: Exception, code: str) -> None:
    # Preserve a wrapper's original cause, including INTERNAL_ERROR conversions.
    seen: set[int] = set()
    original: BaseException = exc
    while original.__cause__ is not None and id(original) not in seen:
        seen.add(id(original))
        original = original.__cause__
    location = None
    trace = original.__traceback__
    while trace is not None:
        module = trace.tb_frame.f_globals.get("__name__", "")
        function = trace.tb_frame.f_code.co_name
        if isinstance(module, str) and _PUBLIC_MODULE.fullmatch(module) and _FUNCTION.fullmatch(
            function
        ):
            location = {"module": module, "function": function, "line": trace.tb_lineno}
        trace = trace.tb_next
    exception_type = type(original).__name__
    state.update(
        last_error_code=code,
        last_error_type=exception_type,
        last_error_location=location,
        historical_error_count=int(state["historical_error_count"]) + 1,
        historical_error_code=code,
        historical_error_type=exception_type,
        historical_error_location=location,
    )


def clear_error(state: dict[str, Any]) -> None:
    state.update(last_error_code=None, last_error_type=None, last_error_location=None)

"""Bounded, content-free runtime metrics for local tool calls.

The registry deliberately never accepts arguments, identifiers, paths, labels, query
text, or payloads.  It retains only tool names, outcome codes, bounded duration samples,
and the small safe failure classification emitted by Sightglass errors.
"""

from __future__ import annotations

import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any

from sightglass.contracts.errors import ErrorCode


def _percentile(values: tuple[int, ...], percentile: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * percentile + 0.999999) - 1))
    return ordered[index]


@dataclass
class _ToolSamples:
    success_count: int = 0
    error_count: int = 0
    timeout_count: int = 0
    durations_ms: deque[int] = field(default_factory=lambda: deque(maxlen=256))


class ToolMetrics:
    """Thread-safe bounded metrics with no request or response content."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tools: dict[str, _ToolSamples] = defaultdict(_ToolSamples)

    def record(self, tool: str, *, elapsed_ms: int, result: Any) -> None:
        code = None
        if isinstance(result, dict) and result.get("ok") is False:
            value = result.get("code")
            code = value if isinstance(value, str) else ErrorCode.INTERNAL_ERROR.value
        with self._lock:
            samples = self._tools[tool]
            samples.durations_ms.append(max(0, int(elapsed_ms)))
            if code is None:
                samples.success_count += 1
            else:
                samples.error_count += 1
                if code == ErrorCode.SERVICE_TIMEOUT.value:
                    samples.timeout_count += 1

    def status(self) -> dict[str, Any]:
        with self._lock:
            tools = {}
            for name, samples in sorted(self._tools.items()):
                durations = tuple(samples.durations_ms)
                tools[name] = {
                    "success_count": samples.success_count,
                    "error_count": samples.error_count,
                    "timeout_count": samples.timeout_count,
                    "sample_count": len(durations),
                    "latency_ms": {
                        "p50": _percentile(durations, 0.50),
                        "p95": _percentile(durations, 0.95),
                        "max": max(durations) if durations else None,
                    },
                }
        return {
            "schema": "sightglass.tool-metrics.v1",
            "sample_limit_per_tool": 256,
            "tools": tools,
        }

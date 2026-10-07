from __future__ import annotations

import inspect
import json
from functools import wraps
from typing import Any

from mcp.types import CallToolResult, TextContent

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.runtime.ipc import (
    IPCClient,
    IPCError,
    IPCTimeoutError,
    IPCUnavailableError,
)

from .projection import project_result
from .tools import ReaderTools


class DaemonReaderTools(ReaderTools):
    """Thin MCP argument surface backed only by authenticated daemon IPC."""

    def __init__(self, client: IPCClient | None = None) -> None:
        self.client = client or IPCClient(role="reader")

    def _call(self, name: str, arguments: dict[str, Any]) -> Any:
        try:
            return self.client.call("tools.call", {"name": name, "arguments": arguments})
        except IPCTimeoutError:
            return SightglassError(ErrorCode.SERVICE_TIMEOUT, retryable=True).as_dict()
        except IPCUnavailableError:
            return SightglassError(ErrorCode.SERVICE_UNAVAILABLE, retryable=True).as_dict()
        except IPCError:
            return SightglassError(ErrorCode.INTERNAL_ERROR).as_dict()

    @staticmethod
    def _error_result(error: dict[str, Any]) -> CallToolResult:
        return CallToolResult(
            isError=True,
            structuredContent=error,
            content=[
                TextContent(
                    type="text",
                    text=json.dumps(
                        error,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )
            ],
        )


def _daemon_proxy(declaration):
    signature = inspect.signature(declaration)

    @wraps(declaration)
    def call(self, *args, **kwargs):
        try:
            bound = signature.bind(self, *args, **kwargs)
        except TypeError:
            return self._error_result(SightglassError(ErrorCode.QUERY_INVALID).as_dict())
        bound.apply_defaults()
        arguments = {key: value for key, value in bound.arguments.items() if key != "self"}
        result = self._call(declaration.__name__, arguments)
        if declaration.__name__ == "wechat_read_resource":
            if isinstance(result, dict) and result.get("schema") == "sightglass.error.v1":
                return self._error_result(result)
            if not isinstance(result, dict) or result.get("__pydantic__") != "CallToolResult":
                raise RuntimeError("sightglassd returned an invalid resource result")
            result = CallToolResult.model_validate(result["value"])
        return project_result(declaration.__name__, result, arguments,
                              arguments["response_profile"])

    return call


for _name, _declaration in vars(ReaderTools).items():
    if _name.startswith("wechat_"):
        setattr(DaemonReaderTools, _name, _daemon_proxy(_declaration))

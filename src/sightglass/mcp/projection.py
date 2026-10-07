"""Versioned, format-only MCP projections and explicit client continuation actions."""
from __future__ import annotations

import inspect
import json
from contextvars import ContextVar
from functools import wraps
from typing import Any, Literal

from mcp.types import CallToolResult, TextContent

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.reader.response_budget import ResponseBudget, response_budget

ResponseProfile = Literal["brief", "diagnostic"]
BRIEF_PROFILE = "sightglass.mcp.brief.v1"
_REQUEST: ContextVar[tuple[str, dict[str, Any], str] | None] = ContextVar(
    "mcp_response_request", default=None
)

# These are implementation receipts, not message truth or authorization evidence.
_DIAGNOSTIC = {
    "inventory_digest", "generation_set_digest", "dependency_generation_digest",
    "generation_id", "generation", "projection_epoch", "observation_watermark",
    "index_receipt", "lexical_index_receipt", "semantic_index_receipt",
    "projection_receipt", "parser_version", "processor_version", "recipe",
    "runtime", "build_identity", "config_identity", "statistics_collected",
    "raw_payload_available", "source_message_id", "topology", "ranking",
}
_RECEIPT_COUNTS = {"candidates_examined", "candidate_budget", "candidates_scanned"}


def brief(value: dict[str, Any]) -> dict[str, Any]:
    """Copy mappings, retaining array positions and every content-bearing value."""
    def visit(item: Any, key: str = "") -> Any:
        if isinstance(item, list):
            # Compact columns (including null who/id slots) remain aligned.
            return [visit(child) for child in item]
        if not isinstance(item, dict):
            return item
        result = {}
        for name, child in item.items():
            if name in _DIAGNOSTIC or name in _RECEIPT_COUNTS:
                continue
            if child is None or child == [] or child == {}:
                continue
            if (name == "focus" and isinstance(child, dict)
                    and not child.get("participant_ids")
                    and not child.get("context_message_count")):
                continue
            result[name] = visit(child, name)
        return result
    result = visit(value)
    # Empty result collections have meaning; preparation responses never acquire them.
    for name in ("messages", "hits", "items", "contexts", "candidates", "resources", "people"):
        if name in value:
            result.setdefault(name, [])
    if isinstance(result.get("page"), dict) and (
        value.get("page", {}).get("message_rows_complete") is False
        or value.get("markers", {}).get("body_truncated")
    ):
        result["page"]["truncated"] = True
    result["response_profile"] = BRIEF_PROFILE
    return result


def next_actions(
    name: str, value: dict[str, Any], arguments: dict[str, Any],
) -> list[dict[str, Any]]:
    """Describe separate poll, materialized pagination and source-scan slots."""
    actions = []
    page = value.get("page")
    page = page if isinstance(page, dict) else {}
    cursor = page.get("next_cursor") or value.get("next_cursor")
    if cursor and (name != "wechat_read_transcripts" or value.get("has_more_results_now")):
        token_path = "page.next_cursor" if page.get("next_cursor") else "next_cursor"
        actions.append({"kind": "result_page", "tool": name, "parameter": "cursor",
                        "token_path": token_path,
                        "reuse_arguments": True,
                        "clear_arguments": [] if name == "wechat_read_transcripts"
                        else ["reading_token"]})
    receipt = value.get("source_receipt", {})
    continuation = receipt.get("source_continuation", {}).get("reading_token")
    if continuation:
        actions.append({"kind": "source_scan", "tool": name,
                        "parameter": "reading_token",
                        "token_path": "source_receipt.source_continuation.reading_token",
                        "reuse_arguments": True,
                        "clear_arguments": ["cursor"]})
    schema = value.get("schema", "")
    if schema in {"sightglass.search-preparation.v1", "sightglass.retrieval-preparation.v1",
                  "sightglass.resource-processing.v1"} and value.get("state") in {
                      "preparing", "ready", "processing"} and value.get("reading_token"):
        actions.append({"kind": "poll", "tool": name,
                        "parameter": "reading_token", "token_path": "reading_token",
                        "reuse_arguments": True, "clear_arguments": ["cursor"],
                        "wait_ms": value.get("retry_after_ms", 1000)})
    if name == "wechat_read_transcripts" and not value.get("processing_complete") and not value.get(
        "has_more_results_now"
    ) and arguments.get("reading_token"):
        has_cursor = bool(value.get("next_cursor"))
        actions.append({"kind": "poll", "tool": name,
                        "parameter": "cursor" if has_cursor else "reading_token",
                        "token_path": "next_cursor" if has_cursor else "reading_token",
                        "reuse_arguments": True, "arguments": {"wait_ms": 8000},
                        "wait_ms": value.get("wait", {}).get("retry_after_ms", 1000)})
    # Compact bodies have an explicit bounded recovery entry, without exposing a source path.
    for rows_key in ("messages", "hits"):
        for index in value.get("markers", {}).get("body_truncated", {}):
            rows = value.get(rows_key, [])
            if int(index) < len(rows):
                actions.append({"kind": "message_detail", "tool": "wechat_read_messages",
                                "arguments": {"mode": "message", "message_id": rows[int(index)][0],
                                              "response_profile": "diagnostic"}})
                break
    for row in value.get("messages", []):
        if isinstance(row, dict) and row.get("body_truncated"):
            actions.append({"kind": "message_detail", "tool": "wechat_read_messages",
                            "arguments": {"mode": "message", "message_id": row["message_id"],
                                          "response_profile": "diagnostic"}})
            break
    if name == "wechat_read_messages" and arguments.get("mode", "recent") == "context":
        rows = value.get("messages", [])
        for edge, flag in ((0, "has_more_before"), (-1, "has_more_after")):
            if rows and page.get(flag):
                row = rows[edge]
                identity = row[0] if isinstance(row, list) else row["message_id"]
                actions.append({"kind": "context_edge", "tool": name,
                                "arguments": {"mode": "context", "message_id": identity},
                                "reuse_arguments": True, "clear_arguments": ["anchor", "cursor"]})
    return actions


def project_result(name: str, value: Any, arguments: dict[str, Any], profile: str) -> Any:
    # Delivery bytes are an immutable reader-domain contract, including old pending pages.
    if arguments.get("mode") == "updates" or profile == "diagnostic":
        return value
    if isinstance(value, CallToolResult):
        if value.isError or value.structuredContent is None:
            return value
        descriptor = project_result(name, value.structuredContent, arguments, profile)
        content = list(value.content)
        if content and isinstance(content[0], TextContent):
            content[0] = TextContent(type="text", text=json.dumps(
                descriptor, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        return value.model_copy(update={"structuredContent": descriptor, "content": content})
    if not isinstance(value, dict) or value.get("ok") is False:
        return value
    result = brief(value)
    actions = next_actions(name, value, arguments)
    if actions:
        result["next_actions"] = actions
    return result


def receipt_projection(name: str, value: dict[str, Any]) -> dict[str, Any]:
    """Count the projected JSON; request/query arguments remain in memory only."""
    request = _REQUEST.get()
    if request is None or request[0] != name:
        return value
    return project_result(name, value, request[1], request[2])


def mcp_response(method):
    """One catalog signature, profile and budget path for local and daemon tools."""
    original = inspect.signature(method)
    parameters = list(original.parameters.values())

    @wraps(method)
    def call(self, *args, **kwargs):
        profile = kwargs.pop("response_profile", "brief")
        if not isinstance(profile, str) or profile not in {"brief", "diagnostic"}:
            return SightglassError(ErrorCode.QUERY_INVALID).as_dict()
        bound = original.bind(self, *args, **kwargs)
        bound.apply_defaults()
        arguments = {key: value for key, value in bound.arguments.items() if key != "self"}
        name = method.__name__
        maximum = {"wechat_read_messages": 16384, "wechat_search_messages": 8192,
                   "wechat_find_links": 8192, "wechat_retrieve": 16384}.get(name)
        budget = ResponseBudget(maximum, brief) if maximum and profile == "brief" and arguments.get(
            "mode") != "updates" else None
        request = _REQUEST.set((name, arguments, profile))
        try:
            with response_budget(budget):
                result = method(self, *args, **kwargs)
        finally:
            _REQUEST.reset(request)
        return project_result(name, result, arguments, profile)

    setattr(call, "__signature__", original.replace(parameters=parameters))
    call.__annotations__ = method.__annotations__ | {"response_profile": ResponseProfile}
    return call

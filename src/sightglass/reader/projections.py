from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sightglass.contracts.common import render_in_timezone
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.current_body import current_body_text, message_view
from sightglass.model.repositories import WindowRepository
from sightglass.operations import check_operation_budget
from sightglass.source.identity import SignedTokenCodec, opaque_id
from sightglass.source.parser import PARSER_VERSION, public_forwarded_chat, public_link

from .response_budget import active_response_budget

COMPACT_FIELDS = ["id", "when", "who", "kind", "what", "resources"]

_COMPACT_MARKERS = {
    "image": "[图片]",
    "sticker": "[表情]",
    "voice": "[语音]",
    "video": "[视频]",
    "location": "[位置]",
    "recalled": "[已撤回]",
    "unknown": "[暂不支持的消息类型]",
    "contact_card": "[联系人名片]",
}

_COMPACT_TITLED_MARKERS = {
    "file": "[文件]",
    "link": "[链接]",
    "mini_program": "[小程序]",
    "forwarded_chat": "[聊天记录]",
}


def _serialized(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class CompactPreparedRow:
    message_id: str
    when: str
    participant_id: str | None
    label: str
    is_self: bool
    kind: str
    body: str
    resource_count: int
    focus: bool
    context_only: bool
    late_arrival: bool
    state: str


class DetailMessageProjector:
    def __init__(self, repository: WindowRepository, token_codec: SignedTokenCodec) -> None:
        self.repository = repository
        self.token_codec = token_codec

    def project(
        self,
        rows: list[Any],
        *,
        timezone_name: str,
        include_resources: bool,
        focus_ids: tuple[str, ...],
        context_only_ids: frozenset[str] = frozenset(),
        late_arrival_ids: frozenset[str] = frozenset(),
    ) -> list[dict[str, Any]]:
        pairs = tuple(
            dict.fromkeys(
                (
                    str(row["sender_id"]),
                    (str(row["sender_membership_id"]) if row["sender_membership_id"] else None),
                )
                for row in rows
                if row["sender_id"]
            )
        )
        labels = self.repository.preferred_labels_bulk(pairs)
        resources = (
            self.repository.resources_for_messages_bulk(
                tuple(str(row["message_id"]) for row in rows)
            )
            if include_resources
            else {}
        )
        projected: list[dict[str, Any]] = []
        for row in rows:
            check_operation_budget()
            participant_id = str(row["sender_id"]) if row["sender_id"] else None
            membership_id = (
                str(row["sender_membership_id"]) if row["sender_membership_id"] else None
            )
            if participant_id:
                label, label_source = labels[(participant_id, membership_id)]
            else:
                label, label_source = "未知发送者", "opaque_fallback"
            snapshot = json.loads(str(row["sender_label_snapshot_json"]))
            structured = message_view(row)
            message_id = str(row["message_id"])
            context_only = message_id in context_only_ids
            focus_match = (not focus_ids or participant_id in focus_ids) and not context_only
            anchor = self.token_codec.encode(
                {
                    "schema": 1,
                    "account_id": str(row["account_id"]),
                    "conversation_id": str(row["conversation_id"]),
                    "message_id": message_id,
                    "sort": [
                        str(row["sort_primary"]),
                        int(row["sort_seq"]),
                        int(row["sort_tie"]),
                        message_id,
                    ],
                }
            )
            projected.append(
                {
                    "schema": "sightglass.message-detail.v1",
                    "message_id": message_id,
                    "account_id": str(row["account_id"]),
                    "conversation_id": str(row["conversation_id"]),
                    "anchor": anchor,
                    "sent_at": render_in_timezone(str(row["sent_at_utc"]), timezone_name),
                    "sender": {
                        "participant_id": participant_id,
                        "membership_id": membership_id,
                        "label": label,
                        "label_source": label_source,
                        "shown_as": snapshot.get("shown_as"),
                        "shown_as_source": snapshot.get("shown_as_source"),
                        "shown_as_temporal_confidence": snapshot.get(
                            "shown_as_temporal_confidence"
                        ),
                        "identity_state": (
                            str(row["sender_resolution_state"])
                            if participant_id and row["sender_resolution_state"]
                            else "unresolved"
                        ),
                        "identity_confidence": (
                            str(row["sender_identity_confidence"])
                            if participant_id and row["sender_identity_confidence"]
                            else "unknown"
                        ),
                        "is_self": bool(snapshot.get("is_self")),
                    },
                    "kind": str(row["kind"]),
                    "text": current_body_text(row),
                    "reply": structured.get("reply"),
                    "link": public_link(structured.get("link")),
                    "forwarded_chat": public_forwarded_chat(structured.get("forwarded_chat")),
                    "resources": resources.get(message_id, []),
                    "state": str(row["current_state"]),
                    "derivation": {
                        "text_kind": (
                            "placeholder" if row["kind"] == "unknown" else "source_visible_text"
                        ),
                        "parser_version": PARSER_VERSION,
                    },
                    "source": {
                        "source_message_id": opaque_id(
                            "wxsource", row["account_id"], row["source_message_id"]
                        ),
                        "observed_at": str(row["last_seen_at"]),
                        "generation_id": opaque_id("wxgeneration", row["current_generation_id"]),
                        "raw_payload_available": True,
                    },
                    "retrieval": {
                        "focus_match": focus_match,
                        "context_only": context_only,
                        "late_arrival": message_id in late_arrival_ids,
                        "matched_participant_ids": (
                            [participant_id]
                            if focus_match and participant_id and participant_id in focus_ids
                            else []
                        ),
                    },
                }
            )
        return projected


class CompactMessageProjector:
    def __init__(self, repository: WindowRepository) -> None:
        self.repository = repository

    @staticmethod
    def _body(kind: str, text: str | None) -> str:
        if kind in {"text", "reply", "system"}:
            return text or ""
        if kind in _COMPACT_TITLED_MARKERS:
            marker = _COMPACT_TITLED_MARKERS[kind]
            return f"{marker} {text}" if text else marker
        return _COMPACT_MARKERS.get(kind, text or "[暂不支持的消息类型]")

    def prepare(
        self,
        rows: list[Any],
        *,
        timezone_name: str,
        include_resource_indicators: bool,
        focus_message_ids: frozenset[str],
        context_only_ids: frozenset[str],
        late_arrival_ids: frozenset[str],
    ) -> list[CompactPreparedRow]:
        pairs = tuple(
            dict.fromkeys(
                (
                    str(row["sender_id"]),
                    (str(row["sender_membership_id"]) if row["sender_membership_id"] else None),
                )
                for row in rows
                if row["sender_id"]
            )
        )
        labels = self.repository.preferred_labels_bulk(pairs)
        counts = (
            self.repository.resource_counts_bulk(tuple(str(row["message_id"]) for row in rows))
            if include_resource_indicators
            else {}
        )
        prepared: list[CompactPreparedRow] = []
        for row in rows:
            check_operation_budget()
            message_id = str(row["message_id"])
            participant_id = str(row["sender_id"]) if row["sender_id"] else None
            membership_id = (
                str(row["sender_membership_id"]) if row["sender_membership_id"] else None
            )
            label = labels[(participant_id, membership_id)][0] if participant_id else "未知发送者"
            snapshot = json.loads(str(row["sender_label_snapshot_json"]))
            prepared.append(
                CompactPreparedRow(
                    message_id=message_id,
                    when=render_in_timezone(str(row["sent_at_utc"]), timezone_name),
                    participant_id=participant_id,
                    label=label,
                    is_self=bool(snapshot.get("is_self")),
                    kind=str(row["kind"]),
                    body=self._body(str(row["kind"]), current_body_text(row)),
                    resource_count=counts.get(message_id, 0),
                    focus=message_id in focus_message_ids,
                    context_only=message_id in context_only_ids,
                    late_arrival=message_id in late_arrival_ids,
                    state=str(row["current_state"]),
                )
            )
        return prepared

    @staticmethod
    def render(prepared: list[CompactPreparedRow]) -> dict[str, Any]:
        people: list[dict[str, Any]] = []
        people_indices: dict[str | None, int] = {}
        messages: list[list[Any]] = []
        focus: list[int] = []
        context: list[int] = []
        late_arrival: list[int] = []
        state: dict[str, str] = {}
        for index, item in enumerate(prepared):
            check_operation_budget()
            if item.participant_id not in people_indices:
                people_indices[item.participant_id] = len(people)
                people.append(
                    {"id": item.participant_id, "label": item.label, "self": item.is_self}
                )
            messages.append(
                [
                    item.message_id,
                    item.when,
                    people_indices[item.participant_id],
                    item.kind,
                    item.body,
                    item.resource_count,
                ]
            )
            if item.focus:
                focus.append(index)
            if item.context_only:
                context.append(index)
            if item.late_arrival:
                late_arrival.append(index)
            if item.state != "present":
                state[str(index)] = item.state
        markers: dict[str, Any] = {"body_truncated": {}}
        if focus and len(focus) != len(prepared):
            markers["focus"] = focus
        if context:
            markers["context"] = context
        if late_arrival:
            markers["late_arrival"] = late_arrival
        if state:
            markers["state"] = state
        return {
            "people": people,
            "fields": COMPACT_FIELDS,
            "messages": messages,
            "markers": markers,
        }


class CompactBodyBudgetAllocator:
    def __init__(
        self, *, max_payload_chars: int, max_body_chars: int, reserve_chars: int = 0,
        max_payload_bytes: int | None = None,
    ) -> None:
        self.max_payload_chars = int(max_payload_chars)
        self.max_body_chars = int(max_body_chars)
        self.reserve_chars = max(0, int(reserve_chars))
        self.effective_budget = max(0, self.max_payload_chars - self.reserve_chars)
        self.max_payload_bytes = max_payload_bytes

    @staticmethod
    def _waterfill(lengths: list[int], budget: int) -> list[int]:
        allocations = [0] * len(lengths)
        pending = sorted(range(len(lengths)), key=lambda index: lengths[index])
        remaining = max(0, budget)
        while pending:
            check_operation_budget()
            share = remaining // len(pending)
            smallest = pending[0]
            if lengths[smallest] <= share:
                allocations[smallest] = lengths[smallest]
                remaining -= lengths[smallest]
                pending.pop(0)
                continue
            for index in pending:
                allocations[index] = share
            break
        return allocations

    @staticmethod
    def _truncate(body: str, allocation: int) -> str:
        if len(body) <= allocation:
            return body
        if allocation <= 0:
            return ""
        if allocation == 1:
            return "…"
        return f"{body[: allocation - 1]}…"

    def apply(self, page: dict[str, Any], *, rows_key: str = "messages") -> None:
        messages = page[rows_key]
        full_bodies = [str(row[4]) for row in messages]
        for row in messages:
            row[4] = ""
        page["markers"]["body_truncated"] = {}
        page["projection_receipt"] = {
            "returned_rows": len(messages),
            "body_complete_rows": len(messages),
            "body_truncated_rows": 0,
            "serialized_chars": 0,
        }
        fixed_size = len(_serialized(page))
        transport = active_response_budget()
        byte_budget = self.max_payload_bytes
        if byte_budget is None and transport is not None:
            byte_budget = transport.available_bytes
        if byte_budget is not None and transport is not None and transport.size(page) > byte_budget:
            raise SightglassError(ErrorCode.OUTPUT_BUDGET_EXCEEDED,
                                 details={"reason": "fixed_envelope", "max_bytes": byte_budget})
        if fixed_size > self.effective_budget:
            raise SightglassError(
                ErrorCode.OUTPUT_BUDGET_EXCEEDED,
                details={
                    "max_compact_payload_chars": self.max_payload_chars,
                    "reserved_chars": self.reserve_chars,
                    "reason": "fixed_envelope",
                },
            )
        capped_lengths = [min(len(body), self.max_body_chars) for body in full_bodies]
        allocations = self._waterfill(
            capped_lengths,
            self.effective_budget - fixed_size - 512,
        )
        truncated: dict[str, dict[str, int]] = {}
        for index, (row, body, allocation) in enumerate(
            zip(messages, full_bodies, allocations, strict=True)
        ):
            check_operation_budget()
            row[4] = self._truncate(body, allocation)
            if len(row[4]) < len(body):
                truncated[str(index)] = {"full_chars": len(body)}
        page["markers"]["body_truncated"] = truncated
        page["projection_receipt"]["body_truncated_rows"] = len(truncated)
        page["projection_receipt"]["body_complete_rows"] = len(messages) - len(truncated)

        for _ in range(4):
            check_operation_budget()
            rendered_size = len(_serialized(page))
            page["projection_receipt"]["serialized_chars"] = rendered_size
            corrected_size = len(_serialized(page))
            if corrected_size <= self.effective_budget and corrected_size == rendered_size:
                self._apply_bytes(page, full_bodies, rows_key=rows_key, byte_budget=byte_budget)
                return
            if corrected_size <= self.effective_budget:
                continue
            if not messages:
                break
            overflow = corrected_size - self.effective_budget
            longest = max(range(len(messages)), key=lambda index: len(messages[index][4]))
            current = str(messages[longest][4]).removesuffix("…")
            keep = max(0, len(current) - overflow - 1)
            messages[longest][4] = f"{current[:keep]}…" if keep else "…"
            truncated[str(longest)] = {"full_chars": len(full_bodies[longest])}
            page["projection_receipt"]["body_truncated_rows"] = len(truncated)
            page["projection_receipt"]["body_complete_rows"] = len(messages) - len(truncated)
        final_size = len(_serialized(page))
        page["projection_receipt"]["serialized_chars"] = final_size
        if len(_serialized(page)) > self.effective_budget:
            raise SightglassError(
                ErrorCode.OUTPUT_BUDGET_EXCEEDED,
                details={
                    "max_compact_payload_chars": self.max_payload_chars,
                    "reserved_chars": self.reserve_chars,
                    "reason": "fixed_envelope",
                },
            )
        self._apply_bytes(page, full_bodies, rows_key=rows_key, byte_budget=byte_budget)

    def _apply_bytes(
        self, page: dict[str, Any], full_bodies: list[str], *, rows_key: str,
        byte_budget: int | None,
    ) -> None:
        transport = active_response_budget()
        if transport is None or byte_budget is None or transport.size(page) <= byte_budget:
            return
        rows = page[rows_key]
        bodies = [str(row[4]) for row in rows]
        for row in rows:
            row[4] = ""
        # Reserve the per-row truncation markers before assigning text bytes.
        truncated = page["markers"]["body_truncated"]
        for index, body in enumerate(bodies):
            if body:
                truncated[str(index)] = {"full_chars": len(full_bodies[index])}
        fixed = transport.size(page)
        if fixed > byte_budget:
            raise SightglassError(ErrorCode.OUTPUT_BUDGET_EXCEEDED,
                                 details={"reason": "fixed_envelope", "max_bytes": byte_budget})
        lengths = [len(_serialized(body).encode("utf-8")) - 2 for body in bodies]
        allocations = self._waterfill(lengths, byte_budget - fixed - 32)
        for index, (row, body, allocation) in enumerate(
            zip(rows, bodies, allocations, strict=True)
        ):
            low, high = 0, len(body)
            while low < high:
                middle = (low + high + 1) // 2
                candidate = self._truncate(body, middle)
                if len(_serialized(candidate).encode("utf-8")) - 2 <= allocation:
                    low = middle
                else:
                    high = middle - 1
            row[4] = self._truncate(body, low)
            if len(row[4]) == len(full_bodies[index]):
                truncated.pop(str(index), None)
        page["projection_receipt"].update(
            body_truncated_rows=len(truncated), body_complete_rows=len(rows) - len(truncated),
            serialized_chars=len(_serialized(page)),
        )
        if transport.size(page) > byte_budget:
            raise SightglassError(ErrorCode.OUTPUT_BUDGET_EXCEEDED,
                                 details={"reason": "fixed_envelope", "max_bytes": byte_budget})


def trim_detail_projection(
    rows: list[Any],
    projected: list[dict[str, Any]],
    *,
    direction: str,
    max_payload_chars: int,
) -> tuple[list[Any], list[dict[str, Any]], bool]:
    transport = active_response_budget()
    if transport is not None:
        # Content remains attached to its ID; a bounded detail is explicitly marked.
        fixed = {"messages": [{**item, "text": ""} for item in projected]}
        per_body = max(0, (
            transport.available_bytes - transport.size(fixed) - 2048 - 128 * len(projected)
        ) // max(1, len(projected)))
        for item in projected:
            body = item.get("text")
            if not isinstance(body, str) or len(_serialized(body).encode("utf-8")) - 2 <= per_body:
                continue
            low, high = 0, len(body)
            while low < high:
                middle = (low + high + 1) // 2
                candidate = CompactBodyBudgetAllocator._truncate(body, middle)
                if len(_serialized(candidate).encode("utf-8")) - 2 <= per_body:
                    low = middle
                else:
                    high = middle - 1
            item["text"] = CompactBodyBudgetAllocator._truncate(body, low)
            item["body_truncated"] = {"full_chars": len(body)}
    sizes = [len(_serialized(item)) for item in projected]
    selected_indices: list[int] = []
    used = 2
    candidates = range(len(rows)) if direction == "forward" else range(len(rows) - 1, -1, -1)
    for index in candidates:
        next_size = used + sizes[index] + (1 if selected_indices else 0)
        if next_size > max_payload_chars:
            break
        if transport is not None:
            candidate = {"messages": [projected[value] for value in [*selected_indices, index]]}
            if transport.size(candidate) + 2048 > transport.available_bytes:
                break
        selected_indices.append(index)
        used = next_size
    if not selected_indices and rows:
        raise SightglassError(
            ErrorCode.OUTPUT_BUDGET_EXCEEDED,
            details={"max_detail_payload_chars": max_payload_chars},
        )
    selected_indices.sort()
    truncated = len(selected_indices) != len(rows)
    return (
        [rows[index] for index in selected_indices],
        [projected[index] for index in selected_indices],
        truncated,
    )

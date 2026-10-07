from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from sightglass.contracts.errors import ErrorCode, SightglassError


@dataclass(frozen=True)
class ReaderPolicy:
    mode: Literal["allowlist", "all_except_denylist"] = "allowlist"
    allowed_conversation_ids: frozenset[str] = field(default_factory=frozenset)
    denied_conversation_ids: frozenset[str] = field(default_factory=frozenset)
    messages: bool = True
    search: bool = True
    resource_metadata: bool = True
    resource_preview: bool = True
    resource_original: bool = True
    identity_debug: bool = False
    max_messages_per_call: int = 200
    max_text_chars_per_call: int = 120_000
    max_compact_messages_per_call: int = 500
    max_detail_messages_per_call: int = 50
    max_compact_payload_chars: int = 180_000
    max_detail_payload_chars: int = 120_000
    max_compact_body_chars_per_message: int = 4_000
    max_binary_bytes_per_call: int = 8 * 1024 * 1024

    def permits(self, conversation_id: str) -> bool:
        if not self.messages or conversation_id in self.denied_conversation_ids:
            return False
        if self.mode == "all_except_denylist":
            return True
        return conversation_id in self.allowed_conversation_ids

    def as_dict(self) -> dict[str, Any]:
        return {
            "conversation_policy": {
                "mode": self.mode,
                "allowed_conversation_ids": sorted(self.allowed_conversation_ids),
                "denied_conversation_ids": sorted(self.denied_conversation_ids),
            },
            "capabilities": {
                "messages": self.messages,
                "search": self.search,
                "resource_metadata": self.resource_metadata,
                "resource_preview": self.resource_preview,
                "resource_original": self.resource_original,
                "identity_debug": self.identity_debug,
            },
            "limits": {
                "max_messages_per_call": self.max_messages_per_call,
                "max_text_chars_per_call": self.max_text_chars_per_call,
                "max_compact_messages_per_call": self.max_compact_messages_per_call,
                "max_detail_messages_per_call": self.max_detail_messages_per_call,
                "max_compact_payload_chars": self.max_compact_payload_chars,
                "max_detail_payload_chars": self.max_detail_payload_chars,
                "max_compact_body_chars_per_message": (
                    self.max_compact_body_chars_per_message
                ),
                "max_binary_bytes_per_call": self.max_binary_bytes_per_call,
            },
        }


@dataclass
class ReaderContext:
    reader_id: str
    display_name: str
    policy: ReaderPolicy
    paused: bool = False
    timezone: str | None = None

    def require_active(self) -> None:
        if self.paused:
            raise SightglassError(ErrorCode.SERVICE_PAUSED)

    def authorize(self, conversation_id: str) -> None:
        self.require_active()
        if not self.policy.permits(conversation_id):
            raise SightglassError(ErrorCode.POLICY_DENIED)

    def require_identity_debug(self) -> None:
        self.require_active()
        if not self.policy.identity_debug:
            raise SightglassError(ErrorCode.POLICY_DENIED)

    def require_search(self) -> None:
        self.require_active()
        if not self.policy.search:
            raise SightglassError(ErrorCode.POLICY_DENIED)

    def require_resource(self, capability: str) -> None:
        self.require_active()
        allowed = {
            "metadata": self.policy.resource_metadata,
            "preview": self.policy.resource_preview,
            "original": self.policy.resource_original,
        }
        if capability not in allowed or not allowed[capability]:
            raise SightglassError(ErrorCode.POLICY_DENIED)

    def bound_binary_bytes(self, requested: int) -> int:
        value = int(requested)
        if value < 1 or value > self.policy.max_binary_bytes_per_call:
            raise SightglassError(
                ErrorCode.QUERY_INVALID,
                details={"max_binary_bytes_per_call": self.policy.max_binary_bytes_per_call},
            )
        return value

    def bound_limit(self, requested: int) -> int:
        value = int(requested)
        if value < 1 or value > self.policy.max_messages_per_call:
            raise SightglassError(
                ErrorCode.QUERY_INVALID,
                details={"max_messages_per_call": self.policy.max_messages_per_call},
            )
        return value

    def bound_message_limit(self, requested: int, *, projection: str, mode: str) -> int:
        value = int(requested)
        if mode == "message":
            maximum = 1
        elif projection == "compact":
            maximum = self.policy.max_compact_messages_per_call
        elif projection == "detail":
            maximum = self.policy.max_detail_messages_per_call
        else:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if value < 1 or value > maximum:
            raise SightglassError(
                ErrorCode.QUERY_INVALID,
                details={"max_messages_for_projection": maximum},
            )
        return value

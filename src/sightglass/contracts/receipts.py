from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AccessReceipt:
    receipt_id: str
    reader_id: str
    tool_name: str
    conversation_id: str | None
    scope_kind: str | None
    scope_digest: str | None
    message_count: int
    resource_count: int
    bytes_returned: int
    started_at_utc: str
    completed_at_utc: str
    outcome: str
    warning_codes: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "reader_id": self.reader_id,
            "tool_name": self.tool_name,
            "conversation_id": self.conversation_id,
            "scope_kind": self.scope_kind,
            "scope_digest": self.scope_digest,
            "message_count": self.message_count,
            "resource_count": self.resource_count,
            "bytes_returned": self.bytes_returned,
            "started_at": self.started_at_utc,
            "completed_at": self.completed_at_utc,
            "outcome": self.outcome,
            "warning_codes": list(self.warning_codes),
        }

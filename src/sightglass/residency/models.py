"""Value objects for the residency store."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .decisions import ResidencyMode, normalize_mode


@dataclass(frozen=True)
class ConversationResidency:
    """One explicit operator residency decision for a conversation."""

    conversation_id: str
    mode: ResidencyMode
    keep_backfill: bool = False
    recent_window_days: int | None = None
    recent_max_bytes: int | None = None
    requested_at: str = ""
    updated_at: str = ""
    reason: str | None = None

    @classmethod
    def from_row(cls, row: Any) -> ConversationResidency:
        return cls(
            conversation_id=str(row["conversation_id"]),
            mode=normalize_mode(row["mode"]),
            keep_backfill=bool(row["keep_backfill"]),
            recent_window_days=(
                int(row["recent_window_days"]) if row["recent_window_days"] is not None else None
            ),
            recent_max_bytes=(
                int(row["recent_max_bytes"]) if row["recent_max_bytes"] is not None else None
            ),
            requested_at=str(row["requested_at"]),
            updated_at=str(row["updated_at"]),
            reason=row["reason"],
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "conversation_id": self.conversation_id,
            "mode": str(self.mode),
            "keep_backfill": self.keep_backfill,
            "recent_window_days": self.recent_window_days,
            "recent_max_bytes": self.recent_max_bytes,
            "requested_at": self.requested_at,
            "updated_at": self.updated_at,
            "reason": self.reason,
        }

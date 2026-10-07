from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def utc_now() -> datetime:
    return datetime.now(UTC)


def parse_aware_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include an explicit UTC offset")
    return parsed


def to_utc_iso(value: str | datetime) -> str:
    parsed = parse_aware_datetime(value) if isinstance(value, str) else value
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include an explicit UTC offset")
    return parsed.astimezone(UTC).isoformat(timespec="microseconds")


def validate_timezone(value: str) -> str:
    try:
        ZoneInfo(value)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown IANA timezone: {value}") from exc
    return value


def render_in_timezone(utc_value: str, timezone_name: str) -> str:
    return (
        parse_aware_datetime(utc_value)
        .astimezone(ZoneInfo(validate_timezone(timezone_name)))
        .isoformat(timespec="seconds")
    )


@dataclass(frozen=True, order=True)
class SourceSortKey:
    sent_at_utc: str
    sort_seq: int
    source_rowid: int
    source_message_id: str

    def as_tuple(self) -> tuple[str, int, int, str]:
        return (self.sent_at_utc, self.sort_seq, self.source_rowid, self.source_message_id)


@dataclass(frozen=True)
class Coverage:
    catalog: str = "unknown"
    conversation: str = "unknown"
    roster: str = "unknown"
    observed_time_after: str | None = None
    observed_time_before: str | None = None
    active_conversations_only: bool = False
    notes: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "catalog": self.catalog,
            "conversation": self.conversation,
            "roster": self.roster,
            "observed_time_after": self.observed_time_after,
            "observed_time_before": self.observed_time_before,
            "active_conversations_only": self.active_conversations_only,
            "notes": list(self.notes),
        }


@dataclass(frozen=True)
class SourceReceipt:
    complete: bool
    fresh_as_of: str
    inventory_digest: str
    generation_set_digest: str
    returned_count: int = 0
    hidden_system_count: int = 0
    coverage: Coverage = field(default_factory=Coverage)
    warnings: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "fresh_as_of": self.fresh_as_of,
            "inventory_digest": self.inventory_digest,
            "generation_set_digest": self.generation_set_digest,
            "returned_count": self.returned_count,
            "hidden_system_count": self.hidden_system_count,
            "coverage": self.coverage.as_dict(),
            "warnings": list(self.warnings),
        }

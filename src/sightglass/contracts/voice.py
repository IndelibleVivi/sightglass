from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class VoiceSelectionItem:
    message_id: str
    resource_id: str
    resource_revision: str
    duration_ms: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class VoiceCoverage:
    selected: int = 0
    ready: int = 0
    pending: int = 0
    not_scheduled: int = 0
    blocked: int = 0
    failed: int = 0
    empty: int = 0
    cancelled: int = 0

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


@dataclass(frozen=True)
class VoiceBatchReceipt:
    reading_token: str | None
    created: bool
    reason: str
    expires_at: str | None
    coverage: VoiceCoverage

    def as_dict(self) -> dict[str, Any]:
        return {
            "reading_token": self.reading_token,
            "created": self.created,
            "reason": self.reason,
            "expires_at": self.expires_at,
            "coverage": self.coverage.as_dict(),
        }


@dataclass(frozen=True)
class VoiceTranscriptItem:
    message_id: str
    resource_id: str
    ordinal: int
    state: str
    text: str | None
    error_code: str | None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def as_row(self) -> list[str | int | None]:
        return [
            self.message_id,
            self.resource_id,
            self.ordinal,
            self.state,
            self.text,
            self.error_code,
        ]


@dataclass(frozen=True)
class VoiceTranscriptPage:
    reading_token: str
    items: tuple[VoiceTranscriptItem, ...]
    coverage: VoiceCoverage
    processing_complete: bool
    text_coverage_complete: bool
    has_more_results_now: bool
    next_cursor: str | None
    expires_at: str
    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": "sightglass.voice-page.v2",
            "reading_token": self.reading_token,
            "fields": [
                "message_id",
                "resource_id",
                "ordinal",
                "state",
                "text",
                "error_code",
            ],
            "items": [item.as_row() for item in self.items],
            "coverage": self.coverage.as_dict(),
            "processing_complete": self.processing_complete,
            "text_coverage_complete": self.text_coverage_complete,
            "has_more_results_now": self.has_more_results_now,
            "next_cursor": self.next_cursor,
            "expires_at": self.expires_at,
            "derivation": {"kind": "derived_transcript"},
        }


@dataclass(frozen=True)
class VoiceTranscription:
    """One recognizer result: derived text plus content-free derivation provenance."""

    text: str
    provenance: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {"text": self.text, "provenance": dict(self.provenance)}


@dataclass(frozen=True)
class VoiceReadSettings:
    """Local voice read policy: whether a reader path may prepare transcripts at all."""

    enabled: bool = False
    default_policy: str = "off"
    language: str = "auto"
    open_item_limit: int = 3
    open_duration_ms: int = 300_000

    def __post_init__(self) -> None:
        if self.default_policy not in {"auto", "cached", "off"}:
            raise ValueError("voice default policy must be auto, cached, or off")
        if not isinstance(self.language, str) or not self.language:
            raise ValueError("voice language must be a non-empty string")
        if type(self.open_item_limit) is not int or not 1 <= self.open_item_limit <= 3:
            raise ValueError("voice open item limit must be between 1 and 3")
        if (
            type(self.open_duration_ms) is not int
            or not 1 <= self.open_duration_ms <= 300_000
        ):
            raise ValueError("voice open duration must be positive and bounded by 300 seconds")

    def policy_for(self, requested: str | None = None) -> str:
        """Resolve one call's effective policy; a disabled installation always reads off."""

        if not self.enabled:
            return "off"
        return self.default_policy if requested is None else requested

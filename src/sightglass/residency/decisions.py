"""The one canonical residency decision, shared by every growth and read path.

A residency decision is intentionally a small, pure value: given a conversation's
effective mode plus the current global/per-conversation bounds, it answers the
questions every route needs.  Centralising it is the point of the work order:
background sync, queued backfill, search preparation, retrieve, refresh, context
expansion and message/resource/voice admission all consult this module rather
than each re-deriving policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

MI = 1024 * 1024

DEFAULT_RECENT_WINDOW_DAYS = 30
DEFAULT_LEASE_TTL_SECONDS = 86_400
DEFAULT_RECENT_MAX_BYTES = 512 * MI
DEFAULT_LEASE_MAX_BYTES = 256 * MI
DEFAULT_GLOBAL_MAX_BYTES = 1024 * MI

MIN_RECENT_WINDOW_DAYS = 1
MAX_RECENT_WINDOW_DAYS = 3650
MIN_LEASE_TTL_SECONDS = 60
MAX_LEASE_TTL_SECONDS = 90 * 86_400


class ResidencyMode(StrEnum):
    """The keep/recent/on-demand retention axis (never the access gate)."""

    KEEP = "keep"
    RECENT = "recent"
    ON_DEMAND = "on_demand"


def normalize_mode(value: Any) -> ResidencyMode:
    """Accept operator spellings, including ``on-demand``/``ondemand``."""

    if isinstance(value, ResidencyMode):
        return value
    text = str(value).strip().lower().replace("-", "_")
    if text in {"on_demand", "ondemand", "on demand", "demand"}:
        return ResidencyMode.ON_DEMAND
    if text == "keep":
        return ResidencyMode.KEEP
    if text == "recent":
        return ResidencyMode.RECENT
    raise ValueError(f"unknown residency mode: {value!r}")


def _positive_int(value: Any, *, field: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"residency {field} must be an integer")
    if value < minimum or value > maximum:
        raise ValueError(f"residency {field} must be between {minimum} and {maximum}")
    return int(value)


def _optional_bytes(value: Any, *, field: str) -> int | None:
    return _positive_int(value, field=field, minimum=1, maximum=1 << 62)


@dataclass(frozen=True)
class ResidencySettings:
    """Global retention-axis configuration (one ``residency_settings`` row)."""

    default_mode: ResidencyMode | str = ResidencyMode.ON_DEMAND
    recent_window_days: int = DEFAULT_RECENT_WINDOW_DAYS
    recent_max_bytes: int | None = DEFAULT_RECENT_MAX_BYTES
    global_max_bytes: int | None = DEFAULT_GLOBAL_MAX_BYTES
    lease_ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS
    lease_max_bytes: int | None = DEFAULT_LEASE_MAX_BYTES

    def __post_init__(self) -> None:
        object.__setattr__(self, "default_mode", normalize_mode(self.default_mode))
        object.__setattr__(
            self,
            "recent_window_days",
            _positive_int(
                self.recent_window_days,
                field="recent_window_days",
                minimum=MIN_RECENT_WINDOW_DAYS,
                maximum=MAX_RECENT_WINDOW_DAYS,
            ),
        )
        object.__setattr__(
            self,
            "lease_ttl_seconds",
            _positive_int(
                self.lease_ttl_seconds,
                field="lease_ttl_seconds",
                minimum=MIN_LEASE_TTL_SECONDS,
                maximum=MAX_LEASE_TTL_SECONDS,
            ),
        )
        object.__setattr__(
            self,
            "recent_max_bytes",
            _optional_bytes(self.recent_max_bytes, field="recent_max_bytes"),
        )
        object.__setattr__(
            self,
            "global_max_bytes",
            _optional_bytes(self.global_max_bytes, field="global_max_bytes"),
        )
        object.__setattr__(
            self,
            "lease_max_bytes",
            _optional_bytes(self.lease_max_bytes, field="lease_max_bytes"),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "default_mode": str(self.default_mode),
            "recent_window_days": self.recent_window_days,
            "recent_max_bytes": self.recent_max_bytes,
            "global_max_bytes": self.global_max_bytes,
            "lease_ttl_seconds": self.lease_ttl_seconds,
            "lease_max_bytes": self.lease_max_bytes,
        }


def parse_residency_settings(value: dict[str, Any] | None) -> ResidencySettings:
    if value is None:
        return ResidencySettings()
    if not isinstance(value, dict):
        raise ValueError("residency settings must be an object")
    unknown = set(value) - set(ResidencySettings.__dataclass_fields__)
    if unknown:
        raise ValueError(f"unknown residency setting: {sorted(unknown)[0]}")
    return ResidencySettings(**value)


@dataclass(frozen=True)
class ResidencyDecision:
    """The canonical answer every growth/read route consults."""

    conversation_id: str
    mode: ResidencyMode
    collect_bodies: bool
    allow_historical_backfill: bool
    recent_window_days: int | None
    max_bytes: int | None
    allow_read_lease: bool
    lease_ttl_seconds: int
    lease_max_bytes: int | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "conversation_id": self.conversation_id,
            "mode": str(self.mode),
            "collect_bodies": self.collect_bodies,
            "allow_historical_backfill": self.allow_historical_backfill,
            "recent_window_days": self.recent_window_days,
            "max_bytes": self.max_bytes,
            "allow_read_lease": self.allow_read_lease,
            "lease_ttl_seconds": self.lease_ttl_seconds,
            "lease_max_bytes": self.lease_max_bytes,
        }


def decide_residency(
    conversation_id: str,
    residency: Any,
    settings: ResidencySettings,
    *,
    authorized: bool,
) -> ResidencyDecision:
    """Resolve one conversation's effective residency.

    ``residency`` is a :class:`ConversationResidency` or ``None`` when the
    conversation has no explicit row (the common case for stock upgrade and for
    a freshly discovered conversation).  ``authorized`` is the access gate; an
    unauthorized conversation always resolves to on-demand/no-collection and can
    never be turned into a retention target by a residency row.
    """

    if not authorized:
        return ResidencyDecision(
            conversation_id=conversation_id,
            mode=ResidencyMode.ON_DEMAND,
            collect_bodies=False,
            allow_historical_backfill=False,
            recent_window_days=None,
            max_bytes=None,
            allow_read_lease=False,
            lease_ttl_seconds=settings.lease_ttl_seconds,
            lease_max_bytes=settings.lease_max_bytes,
        )

    mode = normalize_mode(settings.default_mode if residency is None else residency.mode)
    if mode is ResidencyMode.ON_DEMAND:
        return ResidencyDecision(
            conversation_id=conversation_id,
            mode=mode,
            collect_bodies=False,
            allow_historical_backfill=False,
            recent_window_days=None,
            max_bytes=None,
            allow_read_lease=True,
            lease_ttl_seconds=settings.lease_ttl_seconds,
            lease_max_bytes=settings.lease_max_bytes,
        )

    if mode is ResidencyMode.RECENT:
        window = (
            residency.recent_window_days
            if residency is not None and residency.recent_window_days is not None
            else settings.recent_window_days
        )
        cap = (
            residency.recent_max_bytes
            if residency is not None and residency.recent_max_bytes is not None
            else settings.recent_max_bytes
        )
        return ResidencyDecision(
            conversation_id=conversation_id,
            mode=mode,
            collect_bodies=True,
            allow_historical_backfill=False,
            recent_window_days=window,
            max_bytes=cap,
            allow_read_lease=True,
            lease_ttl_seconds=settings.lease_ttl_seconds,
            lease_max_bytes=settings.lease_max_bytes,
        )

    allow_historical = bool(residency is not None and residency.keep_backfill)
    return ResidencyDecision(
        conversation_id=conversation_id,
        mode=mode,
        collect_bodies=True,
        allow_historical_backfill=allow_historical,
        recent_window_days=None,
        max_bytes=None,
        allow_read_lease=True,
        lease_ttl_seconds=settings.lease_ttl_seconds,
        lease_max_bytes=settings.lease_max_bytes,
    )

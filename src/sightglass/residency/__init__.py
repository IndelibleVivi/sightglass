"""Selective conversation residency and bounded on-demand reading.

Access (``ReaderPolicy``) and residency are deliberately separate axes:

* Access answers *may this reader see this conversation at all?*  It never
  implies that Sightglass keeps the conversation's bodies on disk.
* Residency answers *how much of this authorized conversation do we retain
  locally, and for how long?*

``on_demand`` is the default for new installations and for a conversation that
has no explicit residency decision.  It disables continuous body collection:
bounded foreground reads may still return context, but that context enters an
expiring read lease rather than becoming permanent resident coverage.  ``keep``
and ``recent`` opt a conversation into continuous collection, and ``keep``
starts prospective unless a historical backfill is explicitly selected.

Everything in this package is content-free: decisions depend only on the
``conversation_id``, the residency mode, sizes and timestamps.  No observed
payload, label, URL or participant fact is stored here.
"""

from __future__ import annotations

from .decisions import (
    DEFAULT_LEASE_TTL_SECONDS,
    DEFAULT_RECENT_WINDOW_DAYS,
    ResidencyDecision,
    ResidencyMode,
    ResidencySettings,
    decide_residency,
    normalize_mode,
    parse_residency_settings,
)
from .models import ConversationResidency

__all__ = [
    "DEFAULT_LEASE_TTL_SECONDS",
    "DEFAULT_RECENT_WINDOW_DAYS",
    "ConversationResidency",
    "ResidencyDecision",
    "ResidencyMode",
    "ResidencySettings",
    "decide_residency",
    "normalize_mode",
    "parse_residency_settings",
]

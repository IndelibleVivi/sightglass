from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .common import SourceSortKey
from .identity import LabelObservation, SourceIdentityKey
from .resources import SourceResource


@dataclass(frozen=True)
class SourceMessage:
    source_message_id: str
    source_conversation_id: str
    conversation_kind: str
    source_time_raw: str
    sent_at_utc: str
    observed_at_utc: str
    sort_seq: int
    source_rowid: int
    wechat_type: int
    raw_content: str
    is_outgoing: bool
    source_generation_id: str
    logical_shard_key: str
    sender_keys: tuple[SourceIdentityKey, ...] = ()
    sender_labels: tuple[LabelObservation, ...] = ()
    sender_surface_label: str | None = None
    sender_local_token: str | None = None
    resources: tuple[SourceResource, ...] = ()

    @property
    def sort_key(self) -> SourceSortKey:
        return SourceSortKey(
            self.sent_at_utc,
            int(self.sort_seq),
            int(self.source_rowid),
            self.source_message_id,
        )


@dataclass(frozen=True)
class SourceMessagePage:
    messages: tuple[SourceMessage, ...]
    has_more_before: bool = False
    has_more_after: bool = False

@dataclass(frozen=True)
class SourceDiscoveryPage:
    """One bounded physical discovery page over a source's raw message rows.

    A discovery page is *candidate evidence*, never an authoritative result. The
    provider walks its own physical storage order (never a chronological promise)
    and returns every inspected row that satisfies the optional time window. The
    reader is responsible for the semantic gate: it MUST call the canonical
    ``get_message`` for each selected candidate before admission, re-check
    conversation/time/filter semantics against that canonical row, and dedupe by
    canonical ``source_message_id``. The same identity may therefore repeat across
    shards or continuation pages; that is expected, not a conflict.

    ``positions`` carries one provider-owned physical continuation *after each
    returned candidate*, in the same order as ``messages``, so a caller that has
    already resolved candidate ``i`` can resume scanning immediately past it.

    ``next_position`` resumes *after every raw row this call inspected*, whether or
    not it produced a candidate. It is ``None`` exactly when ``has_more`` is
    ``False``; whenever a page fills to its bound (``has_more=True``) it carries the
    continuation even if this call happened to consume the last raw row. ``has_more``
    is therefore conservative: a filled page may require one further terminating
    call that returns no candidates and ``has_more=False``. An empty ``messages``
    tuple with ``has_more=True`` is an ordinary time-filtered page, not completion --
    only ``has_more=False`` ends the scan.

    Positions are private/internal provider state. They are NOT portable across a
    generation change; the reader binds reuse of any returned position to its
    existing ``search_generation_binding`` before continuing.
    """

    messages: tuple[SourceMessage, ...]
    positions: tuple[dict[str, Any], ...]
    next_position: dict[str, Any] | None
    has_more: bool
    scanned_rows: int


@dataclass(frozen=True)
class ParsedMessage:
    kind: str
    text: str | None
    structured: dict[str, Any] = field(default_factory=dict)
    resources: tuple[SourceResource, ...] = ()
    derivation_text_kind: str = "source_visible_text"

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

ResolutionState = Literal["stable", "conversation_local", "alias_only", "unresolved"]
IdentityConfidence = Literal["exact", "strong", "weak", "unknown"]
TemporalConfidence = Literal["exact", "near", "current_only", "unknown"]


@dataclass(frozen=True)
class SourceIdentityKey:
    kind: str
    value: str
    stability: Literal["stable", "conversation_local"]
    principal_eligible: bool
    provenance: str
    scope_conversation_source_id: str | None = None


@dataclass(frozen=True)
class SourceParticipantFilter:
    """Lossless internal source-side identity filter used by speaker reads."""

    key_kind: str | None = None
    key_value: str | None = None
    principal_eligible: bool = False
    scope_conversation_source_id: str | None = None
    source_message_id: str | None = None


@dataclass(frozen=True)
class LabelObservation:
    label: str
    label_kind: str
    scope: Literal["account", "conversation", "message-surface", "reader"]
    provenance: str
    observed_at_utc: str
    temporal_confidence: TemporalConfidence
    valid_from_utc: str | None = None
    valid_to_utc: str | None = None
    observed_source_message_id: str | None = None


@dataclass(frozen=True)
class SourceAccount:
    source_namespace: str
    source_account_key: str
    self_principal_key: str
    display_name: str
    reader_timezone: str
    identity_confidence: IdentityConfidence = "exact"
    account_binding_id: str | None = None


@dataclass(frozen=True)
class SourceConversation:
    source_conversation_id: str
    kind: str
    title: str
    aliases: tuple[str, ...] = ()
    last_message_at_utc: str | None = None
    roster_complete: bool = False
    unread_count: int = 0
    catalog_state: str = "known"


@dataclass(frozen=True)
class SourceParticipant:
    source_conversation_id: str
    identity_keys: tuple[SourceIdentityKey, ...]
    labels: tuple[LabelObservation, ...]
    is_self: bool = False
    actor_kind: str = "person"
    resolution_state: ResolutionState = "stable"
    identity_confidence: IdentityConfidence = "exact"
    source_membership_id: str | None = None
    last_spoke_at_utc: str | None = None
    account_labels_complete: bool = False
    membership_labels_complete: bool = False


@dataclass(frozen=True)
class ConversationCandidate:
    conversation: SourceConversation
    matched_value: str
    matched_kind: str


@dataclass(frozen=True)
class ParticipantCandidate:
    participant_id: str
    membership_id: str
    label: str
    label_source: str
    labels: dict[str, str | None]
    matched: dict[str, Any]
    last_spoke_at: str | None
    resolution_state: str
    identity_confidence: str

    def as_dict(self, *, include_labels: bool = True) -> dict[str, Any]:
        value: dict[str, Any] = {
            "participant_id": self.participant_id,
            "membership_id": self.membership_id,
            "label": self.label,
            "label_source": self.label_source,
            "matched": dict(self.matched),
            "last_spoke_at": self.last_spoke_at,
            "resolution_state": self.resolution_state,
            "identity_confidence": self.identity_confidence,
        }
        if include_labels:
            value["labels"] = dict(self.labels)
        return value

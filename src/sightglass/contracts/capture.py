"""Private operation-capture contracts, independent of a provider's wire API.

These types are internal edge/core evidence. They are never MCP arguments or
public message projections. The account/conversation egress ceiling remains a
separate local operator decision from reader authorization on the core.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Literal

from .common import SourceSortKey, parse_aware_datetime
from .identity import SourceAccount, SourceConversation, SourceParticipant, SourceParticipantFilter
from .messages import SourceMessage
from .resources import SourceResource, SourceResourceVariant

CAPTURE_VERSION = "sightglass.capture.v1"
MAX_CAPTURE_MESSAGES = 200
MAX_CAPTURE_METADATA_BYTES = 4 * 1024 * 1024
MAX_CAPTURE_RESOURCE_BYTES = 32 * 1024 * 1024
MAX_EDGE_SPOOL_BYTES = 64 * 1024 * 1024
CaptureOperation = Literal[
    "catalog", "recent", "range", "context", "verify", "discovery", "resource"
]
CaptureTerminal = Literal["complete", "rejected", "cancelled", "epoch_loss"]


class CaptureProtocolError(RuntimeError):
    """Content-free protocol failure; never interpolate private payloads."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def _identifier(value: str, name: str) -> None:
    if not value or len(value.encode("utf-8")) > 65_536 or "\x00" in value:
        raise CaptureProtocolError(f"invalid_{name}")


def _position(value: str | None) -> None:
    if value is None:
        return
    if len(value.encode("utf-8")) > 8192:
        raise CaptureProtocolError("discovery_position_too_large")
    try:
        parsed = json.loads(value)
    except (ValueError, RecursionError) as exc:
        raise CaptureProtocolError("invalid_discovery_position") from exc
    if not isinstance(parsed, dict):
        raise CaptureProtocolError("invalid_discovery_position")


@dataclass(frozen=True)
class CaptureRequest:
    request_id: str
    operation: CaptureOperation
    account_id: str
    policy_revision: str
    conversation_source_id: str | None = None
    conversation_source_ids: tuple[str, ...] = ()
    limit: int = 100
    direction: Literal["forward", "backward"] = "backward"
    after: SourceSortKey | None = None
    before: SourceSortKey | None = None
    time_after_utc: str | None = None
    time_before_utc: str | None = None
    participant_filters: tuple[SourceParticipantFilter, ...] = ()
    focus_source_message_id: str | None = None
    context_before: int = 0
    context_after: int = 0
    message_ids: tuple[str, ...] = ()
    position_json: str | None = None
    expected_generations: tuple[tuple[str, str], ...] = ()
    resource_key: str | None = None
    resource_variant: SourceResourceVariant = "original"
    max_resource_bytes: int = MAX_CAPTURE_RESOURCE_BYTES
    resource_descriptor_digest: str | None = None
    expected_resource_revision: str | None = None
    resource_descriptor: SourceResource | None = None
    # Canonical private resolver evidence used by ResourceService._resource_revision.
    # No source path operation is accepted; these are exact comparison fields only.
    resource_revision_json: str | None = None

    def __post_init__(self) -> None:
        for name in ("request_id", "account_id", "policy_revision"):
            _identifier(getattr(self, name), name)
        if self.operation not in {
            "catalog",
            "recent",
            "range",
            "context",
            "verify",
            "discovery",
            "resource",
        }:
            raise CaptureProtocolError("unsupported_operation")
        if type(self.limit) is not int or not 1 <= self.limit <= MAX_CAPTURE_MESSAGES:
            raise CaptureProtocolError("invalid_message_limit")
        if self.direction not in {"forward", "backward"}:
            raise CaptureProtocolError("invalid_direction")
        if self.conversation_source_ids:
            if (
                self.operation != "verify"
                or self.conversation_source_id is not None
                or len(self.conversation_source_ids) > MAX_CAPTURE_MESSAGES
                or len(set(self.conversation_source_ids)) != len(self.conversation_source_ids)
            ):
                raise CaptureProtocolError("invalid_verification_conversation_scope")
            for conversation_id in self.conversation_source_ids:
                _identifier(conversation_id, "conversation_source_id")
        if self.operation == "catalog":
            if self.conversation_source_id is not None:
                raise CaptureProtocolError("catalog_target_mismatch")
        elif not self.conversation_source_ids:
            _identifier(self.conversation_source_id or "", "conversation_source_id")
        if self.operation in {"context", "range"}:
            if (
                type(self.context_before) is not int
                or type(self.context_after) is not int
                or min(self.context_before, self.context_after) < 0
                or self.context_before + self.context_after + 1 > MAX_CAPTURE_MESSAGES
            ):
                raise CaptureProtocolError("invalid_context_radius")
            if self.operation == "context":
                _identifier(self.focus_source_message_id or "", "focus_source_message_id")
            elif (
                self.limit * (self.context_before + self.context_after + 1)
                + len(self.message_ids)
                > MAX_CAPTURE_MESSAGES
            ):
                raise CaptureProtocolError("range_context_batch_limit")
        if self.operation in {"verify", "range"}:
            if len(self.message_ids) > MAX_CAPTURE_MESSAGES:
                raise CaptureProtocolError("invalid_verification_batch")
            if len(set(self.message_ids)) != len(self.message_ids):
                raise CaptureProtocolError("duplicate_verification_id")
        elif self.message_ids:
            raise CaptureProtocolError("unexpected_message_targets")
        for message_id in self.message_ids:
            _identifier(message_id, "source_message_id")
        if len(self.participant_filters) > MAX_CAPTURE_MESSAGES:
            raise CaptureProtocolError("participant_filter_limit")
        for timestamp in (self.time_after_utc, self.time_before_utc):
            if timestamp is not None:
                try:
                    parse_aware_datetime(timestamp)
                except ValueError as exc:
                    raise CaptureProtocolError("invalid_time_bound") from exc
        if self.operation == "resource":
            for name in (
                "focus_source_message_id",
                "resource_key",
                "resource_descriptor_digest",
                "expected_resource_revision",
            ):
                _identifier(getattr(self, name) or "", name)
            if self.resource_descriptor is None or self.resource_revision_json is None:
                raise CaptureProtocolError("resource_binding_evidence_required")
            if (
                type(self.max_resource_bytes) is not int
                or not 1 <= self.max_resource_bytes <= MAX_CAPTURE_RESOURCE_BYTES
                or self.resource_variant not in {"original", "thumbnail"}
            ):
                raise CaptureProtocolError("invalid_resource_bound")
        elif self.resource_key is not None:
            raise CaptureProtocolError("unexpected_resource_target")
        _position(self.position_json)
        if len(set(key for key, _ in self.expected_generations)) != len(self.expected_generations):
            raise CaptureProtocolError("duplicate_generation_key")

    @property
    def conversations(self) -> tuple[str, ...]:
        return self.conversation_source_ids or (
            (self.conversation_source_id,) if self.conversation_source_id is not None else ()
        )


@dataclass(frozen=True)
class CaptureCeiling:
    account_id: str
    conversations: frozenset[str]
    revision: str

    def __post_init__(self) -> None:
        _identifier(self.account_id, "account_id")
        _identifier(self.revision, "egress_revision")
        for conversation in self.conversations:
            _identifier(conversation, "conversation_source_id")

    def authorize(self, request: CaptureRequest) -> None:
        if request.account_id != self.account_id:
            raise CaptureProtocolError("egress_account_denied")
        if request.operation != "catalog" and any(
            conversation not in self.conversations for conversation in request.conversations
        ):
            raise CaptureProtocolError("egress_conversation_denied")


@dataclass(frozen=True)
class CaptureCoverage:
    kind: Literal["none", "catalog", "page", "context", "verification", "discovery", "resource"]
    catalog_complete: bool = False
    active_conversations_only: bool = False
    has_more_before: bool = False
    has_more_after: bool = False
    scanned_rows: int = 0
    # Discovery traversal is never chronological continuity or result admission.
    discovery_has_more: bool = False


@dataclass(frozen=True)
class ResourceCaptureBinding:
    account_id: str
    conversation_source_id: str
    source_message_id: str
    source_resource_key: str
    descriptor_digest: str
    resolver_revision: str


@dataclass(frozen=True)
class CaptureContextWindow:
    focus_source_message_id: str
    before_message_ids: tuple[str, ...]
    after_message_ids: tuple[str, ...]
    has_more_before: bool = False
    has_more_after: bool = False


@dataclass(frozen=True)
class CaptureEvidence:
    accounts: tuple[SourceAccount, ...] = ()
    conversations: tuple[SourceConversation, ...] = ()
    participants: tuple[SourceParticipant, ...] = ()
    messages: tuple[SourceMessage, ...] = ()
    missing_message_ids: tuple[str, ...] = ()
    discovery_positions_json: tuple[str, ...] = ()
    discovery_next_position_json: str | None = None
    resource_binding: ResourceCaptureBinding | None = None
    resource_variant: SourceResourceVariant | None = None
    range_page_message_ids: tuple[str, ...] = ()
    context_windows: tuple[CaptureContextWindow, ...] = ()


@dataclass(frozen=True)
class CaptureOrigin:
    source_instance_id: str
    origin_epoch: str
    stream_epoch: str
    sequence: int
    batch_id: str
    egress_revision: str
    provider_kind: str
    provider_implementation: str
    provider_mode: Literal["synthetic", "live"]
    message_sender_evidence_complete: bool
    supports_resources: bool
    inventory_digest: str
    generation_set_digest: str
    source_fresh_as_of: str
    selected_generations: tuple[tuple[str, str], ...]
    captured_at: str

    def __post_init__(self) -> None:
        for name in (
            "source_instance_id",
            "origin_epoch",
            "stream_epoch",
            "batch_id",
            "egress_revision",
            "provider_kind",
            "provider_implementation",
            "inventory_digest",
            "generation_set_digest",
        ):
            _identifier(getattr(self, name), name)
        if type(self.sequence) is not int or not 1 <= self.sequence <= (1 << 63) - 1:
            raise CaptureProtocolError("invalid_sequence")
        if len(set(key for key, _ in self.selected_generations)) != len(self.selected_generations):
            raise CaptureProtocolError("duplicate_generation_key")
        for timestamp in (self.source_fresh_as_of, self.captured_at):
            try:
                parse_aware_datetime(timestamp)
            except ValueError as exc:
                raise CaptureProtocolError("invalid_capture_time") from exc


@dataclass(frozen=True)
class CaptureReceipt:
    terminal: CaptureTerminal
    sealed_at: str
    fresh_until: str
    coverage: CaptureCoverage = field(default_factory=lambda: CaptureCoverage("none"))
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.terminal not in {"complete", "rejected", "cancelled", "epoch_loss"}:
            raise CaptureProtocolError("invalid_terminal_state")
        if self.terminal != "complete" and self.coverage != CaptureCoverage("none"):
            raise CaptureProtocolError("terminal_error_claims_coverage")
        for timestamp in (self.sealed_at, self.fresh_until):
            try:
                parse_aware_datetime(timestamp)
            except ValueError as exc:
                raise CaptureProtocolError("invalid_receipt_time") from exc


@dataclass(frozen=True)
class CaptureDocument:
    request: CaptureRequest
    origin: CaptureOrigin
    receipt: CaptureReceipt
    evidence: CaptureEvidence


@dataclass(frozen=True)
class CaptureExpectation:
    account_id: str
    source_instance_id: str
    origin_epoch: str
    policy_revision: str
    egress_revision: str
    conversations: frozenset[str]


@dataclass(frozen=True)
class CaptureAck:
    """Issued only after the core's terminal receive ledger commit."""

    stream_epoch: str
    sequence: int
    batch_id: str
    envelope_digest: str
    request_id: str
    terminal: Literal["accepted", "rejected", "cancelled", "epoch_loss"]
    receipt_id: str


@dataclass(frozen=True)
class CaptureStreamPosition:
    stream_epoch: str
    next_sequence: int

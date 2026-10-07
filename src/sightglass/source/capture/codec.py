"""Strict bounded capture codec. Binary resources never become JSON/base64."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import struct
import types
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal, Union, get_args, get_origin, get_type_hints

from sightglass.contracts.capture import (
    CAPTURE_VERSION,
    MAX_CAPTURE_MESSAGES,
    MAX_CAPTURE_METADATA_BYTES,
    MAX_CAPTURE_RESOURCE_BYTES,
    CaptureCeiling,
    CaptureDocument,
    CaptureExpectation,
    CaptureProtocolError,
    CaptureRequest,
)
from sightglass.contracts.common import parse_aware_datetime, utc_now

_MAGIC = b"SGCP\x00\x01"
_HEADER = struct.Struct("!6sII")


def json_value(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return {
            item.name: json_value(getattr(value, item.name)) for item in dataclasses.fields(value)
        }
    if isinstance(value, (tuple, list)):
        return [json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: json_value(item) for key, item in value.items()}
    return value


def canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            json_value(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (ValueError, TypeError, RecursionError) as exc:
        raise CaptureProtocolError("invalid_json_value") from exc


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CaptureProtocolError("duplicate_json_key")
        result[key] = value
    return result


def strict_json(payload: bytes, *, max_bytes: int = MAX_CAPTURE_METADATA_BYTES) -> Any:
    if not payload or len(payload) > max_bytes:
        raise CaptureProtocolError("metadata_size_invalid")
    try:
        return json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                CaptureProtocolError("nonfinite_json_value")
            ),
        )
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise CaptureProtocolError("invalid_capture_json") from exc


def typed_value(annotation: Any, value: Any, *, depth: int = 0) -> Any:
    """Decode only declared dataclasses/types; no class names come from the wire."""
    if depth > 20:
        raise CaptureProtocolError("capture_nesting_limit")
    origin = get_origin(annotation)
    arguments = get_args(annotation)
    if origin in (Union, types.UnionType):
        for choice in arguments:
            try:
                return typed_value(choice, value, depth=depth + 1)
            except CaptureProtocolError:
                pass
        raise CaptureProtocolError("capture_type_mismatch")
    if origin is Literal:
        if value not in arguments or any(
            type(value) is not type(item) for item in arguments if value == item
        ):
            raise CaptureProtocolError("capture_literal_mismatch")
        return value
    if annotation is type(None):
        if value is not None:
            raise CaptureProtocolError("capture_type_mismatch")
        return None
    if annotation in (str, int, bool):
        if type(value) is not annotation:
            raise CaptureProtocolError("capture_type_mismatch")
        return value
    if origin is tuple:
        if not isinstance(value, list):
            raise CaptureProtocolError("capture_type_mismatch")
        if len(arguments) == 2 and arguments[1] is Ellipsis:
            return tuple(typed_value(arguments[0], item, depth=depth + 1) for item in value)
        if len(arguments) != len(value):
            raise CaptureProtocolError("capture_tuple_size")
        return tuple(
            typed_value(kind, item, depth=depth + 1)
            for kind, item in zip(arguments, value, strict=True)
        )
    if dataclasses.is_dataclass(annotation):
        if not isinstance(value, dict):
            raise CaptureProtocolError("capture_type_mismatch")
        fields = dataclasses.fields(annotation)
        if set(value) != {item.name for item in fields}:
            raise CaptureProtocolError("capture_fields_mismatch")
        hints = get_type_hints(annotation)
        factory: Any = annotation
        try:
            return factory(
                **{
                    item.name: typed_value(hints[item.name], value[item.name], depth=depth + 1)
                    for item in fields
                }
            )
        except (TypeError, ValueError) as exc:
            raise CaptureProtocolError("capture_value_invalid") from exc
    raise CaptureProtocolError("capture_type_unsupported")


def request_from_bytes(payload: bytes) -> CaptureRequest:
    return typed_value(CaptureRequest, strict_json(payload))


def _seal_digest(document_bytes: bytes, resource: bytes) -> str:
    digest = hashlib.sha256(b"sightglass-sealed-capture\x00")
    digest.update(struct.pack("!II", len(document_bytes), len(resource)))
    digest.update(document_bytes)
    digest.update(resource)
    return digest.hexdigest()


@dataclass(frozen=True)
class SealedCapture:
    """Immutable transfer unit. Decoded objects are fresh private copies."""

    metadata: bytes
    resource: bytes = b""

    def __post_init__(self) -> None:
        if type(self.metadata) is not bytes or type(self.resource) is not bytes:
            raise CaptureProtocolError("capture_requires_immutable_bytes")
        if len(self.resource) > MAX_CAPTURE_RESOURCE_BYTES:
            raise CaptureProtocolError("resource_size_invalid")
        self.document()  # structural/digest validation, including terminal closure

    @classmethod
    def seal(cls, document: CaptureDocument, resource: bytes = b"") -> SealedCapture:
        document_bytes = canonical_json(document)
        metadata = canonical_json(
            {
                "schema": CAPTURE_VERSION,
                "document": json_value(document),
                "seal": {
                    "document_digest": hashlib.sha256(document_bytes).hexdigest(),
                    "resource_digest": hashlib.sha256(resource).hexdigest(),
                    "resource_size": len(resource),
                    "digest": _seal_digest(document_bytes, resource),
                },
            }
        )
        return cls(metadata, resource)

    @property
    def digest(self) -> str:
        return str(strict_json(self.metadata)["seal"]["digest"])

    def document(self) -> CaptureDocument:
        value = strict_json(self.metadata)
        if not isinstance(value, dict) or set(value) != {"schema", "document", "seal"}:
            raise CaptureProtocolError("unsealed_capture")
        if value["schema"] != CAPTURE_VERSION:
            raise CaptureProtocolError("capture_version_mismatch")
        if canonical_json(value) != self.metadata:
            raise CaptureProtocolError("capture_metadata_not_canonical")
        seal = value["seal"]
        if not isinstance(seal, dict) or set(seal) != {
            "document_digest",
            "resource_digest",
            "resource_size",
            "digest",
        }:
            raise CaptureProtocolError("unsealed_capture")
        encoded = canonical_json(value["document"])
        if (
            type(seal["resource_size"]) is not int
            or seal["resource_size"] != len(self.resource)
            or seal["document_digest"] != hashlib.sha256(encoded).hexdigest()
            or seal["resource_digest"] != hashlib.sha256(self.resource).hexdigest()
            or seal["digest"] != _seal_digest(encoded, self.resource)
        ):
            raise CaptureProtocolError("capture_digest_mismatch")
        document: CaptureDocument = typed_value(CaptureDocument, value["document"])
        _validate_document(document, self.resource)
        return document

    def to_bytes(self) -> bytes:
        return (
            _HEADER.pack(_MAGIC, len(self.metadata), len(self.resource))
            + self.metadata
            + self.resource
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> SealedCapture:
        if len(data) < _HEADER.size:
            raise CaptureProtocolError("capture_header_incomplete")
        magic, metadata_size, resource_size = _HEADER.unpack_from(data)
        if (
            magic != _MAGIC
            or not 1 <= metadata_size <= MAX_CAPTURE_METADATA_BYTES
            or resource_size > MAX_CAPTURE_RESOURCE_BYTES
            or len(data) != _HEADER.size + metadata_size + resource_size
        ):
            raise CaptureProtocolError("capture_size_invalid")
        offset = _HEADER.size + metadata_size
        return cls(data[_HEADER.size : offset], data[offset:])


def _validate_document(document: CaptureDocument, resource: bytes) -> None:
    request, origin, receipt, evidence = (
        document.request,
        document.origin,
        document.receipt,
        document.evidence,
    )
    if receipt.terminal != "complete":
        if evidence != type(evidence)() or resource:
            raise CaptureProtocolError("terminal_error_carries_evidence")
        return
    if len(evidence.messages) > MAX_CAPTURE_MESSAGES:
        raise CaptureProtocolError("message_batch_limit")
    if len(set(item.source_message_id for item in evidence.messages)) != len(evidence.messages):
        raise CaptureProtocolError("duplicate_canonical_message")
    if len(evidence.accounts) != 1 or evidence.accounts[0].source_account_key != request.account_id:
        raise CaptureProtocolError("capture_account_mismatch")
    conversations = {item.source_conversation_id for item in evidence.conversations}
    if request.operation != "catalog" and request.operation != "resource":
        if conversations != set(request.conversations):
            raise CaptureProtocolError("capture_conversation_mismatch")
    if any(item.source_conversation_id not in conversations for item in evidence.participants):
        raise CaptureProtocolError("capture_participant_scope_mismatch")
    if any(item.source_conversation_id not in request.conversations for item in evidence.messages):
        raise CaptureProtocolError("capture_message_scope_mismatch")
    present = {item.source_message_id for item in evidence.messages}
    if request.operation in {"verify", "range"}:
        missing = set(evidence.missing_message_ids)
        if (
            len(missing) != len(evidence.missing_message_ids)
            or present & missing
            or not missing <= set(request.message_ids)
            or not set(request.message_ids) <= present | missing
            or (request.operation == "verify" and present | missing != set(request.message_ids))
        ):
            raise CaptureProtocolError("verification_scope_mismatch")
    elif evidence.missing_message_ids:
        raise CaptureProtocolError("unexpected_missing_message_targets")
    if request.operation == "range":
        captured = {item.source_message_id: item for item in evidence.messages}
        if (
            len(set(evidence.range_page_message_ids)) != len(evidence.range_page_message_ids)
            or len(evidence.range_page_message_ids) > request.limit
            or any(value not in captured for value in evidence.range_page_message_ids)
        ):
            raise CaptureProtocolError("context_window_scope_mismatch")
        if request.context_before or request.context_after:
            if (
                tuple(window.focus_source_message_id for window in evidence.context_windows)
                != evidence.range_page_message_ids
            ):
                raise CaptureProtocolError("context_window_scope_mismatch")
        elif evidence.context_windows:
            raise CaptureProtocolError("unexpected_context_windows")
        for window in evidence.context_windows:
            focus = captured[window.focus_source_message_id]
            if (
                len(window.before_message_ids) > request.context_before
                or len(window.after_message_ids) > request.context_after
                or any(
                    value not in captured or captured[value].sort_key >= focus.sort_key
                    for value in window.before_message_ids
                )
                or any(
                    value not in captured or captured[value].sort_key <= focus.sort_key
                    for value in window.after_message_ids
                )
            ):
                raise CaptureProtocolError("context_window_position_mismatch")
        declared_messages = set(evidence.range_page_message_ids) | set(request.message_ids)
        for window in evidence.context_windows:
            declared_messages.update(window.before_message_ids)
            declared_messages.update(window.after_message_ids)
        if not present <= declared_messages:
            raise CaptureProtocolError("range_message_scope_mismatch")
    elif evidence.context_windows or evidence.range_page_message_ids:
        raise CaptureProtocolError("unexpected_context_windows")
    if request.operation == "discovery":
        if len(evidence.discovery_positions_json) != len(evidence.messages):
            raise CaptureProtocolError("discovery_position_mismatch")
        if receipt.coverage.discovery_has_more != (
            evidence.discovery_next_position_json is not None
        ):
            raise CaptureProtocolError("discovery_terminal_mismatch")
    if request.operation == "resource":
        binding = evidence.resource_binding
        if binding is None or (
            binding.account_id != request.account_id
            or binding.conversation_source_id != request.conversation_source_id
            or binding.source_message_id != request.focus_source_message_id
            or binding.source_resource_key != request.resource_key
            or binding.descriptor_digest != request.resource_descriptor_digest
            or binding.resolver_revision != request.expected_resource_revision
            or evidence.resource_variant != request.resource_variant
            or len(resource) > request.max_resource_bytes
        ):
            raise CaptureProtocolError("resource_binding_mismatch")
    elif resource or evidence.resource_binding is not None or evidence.resource_variant is not None:
        raise CaptureProtocolError("unexpected_resource_payload")
    kinds = {
        "catalog": "catalog",
        "recent": "page",
        "range": "page",
        "context": "context",
        "verify": "verification",
        "discovery": "discovery",
        "resource": "resource",
    }
    if receipt.coverage.kind != kinds[request.operation]:
        raise CaptureProtocolError("coverage_scope_mismatch")
    if request.operation != "catalog" and receipt.coverage.catalog_complete:
        raise CaptureProtocolError("target_claims_catalog_coverage")
    selected = dict(origin.selected_generations)
    if any(selected.get(item.logical_shard_key) is None for item in evidence.messages):
        raise CaptureProtocolError("selected_generation_evidence_missing")


def validate_capture(
    envelope: SealedCapture,
    expected: CaptureExpectation,
    *,
    request: CaptureRequest | None = None,
    require_fresh: bool = True,
    now: str | None = None,
) -> CaptureDocument:
    document = envelope.document()
    origin = document.origin
    if (
        document.request.account_id != expected.account_id
        or origin.source_instance_id != expected.source_instance_id
        or origin.origin_epoch != expected.origin_epoch
        or document.request.policy_revision != expected.policy_revision
        or origin.egress_revision != expected.egress_revision
    ):
        raise CaptureProtocolError("capture_expectation_mismatch")
    CaptureCeiling(expected.account_id, expected.conversations, expected.egress_revision).authorize(
        document.request
    )
    if any(
        item.source_conversation_id not in expected.conversations
        for item in document.evidence.conversations
    ):
        raise CaptureProtocolError("capture_egress_ceiling_mismatch")
    if request is not None and document.request != request:
        raise CaptureProtocolError("capture_request_mismatch")
    if require_fresh:
        current = parse_aware_datetime(now) if now is not None else utc_now()
        captured = parse_aware_datetime(origin.captured_at)
        sealed = parse_aware_datetime(document.receipt.sealed_at)
        fresh_until = parse_aware_datetime(document.receipt.fresh_until)
        if (
            captured > current + timedelta(seconds=30)
            or sealed < captured
            or sealed > current + timedelta(seconds=30)
            or fresh_until < sealed
            or current > fresh_until
            or fresh_until - captured > timedelta(minutes=5)
        ):
            raise CaptureProtocolError("fresh_receipt_expired")
    return document

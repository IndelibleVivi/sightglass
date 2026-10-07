"""Exact private resolver evidence comparison; no owning-message hydration."""

from __future__ import annotations

import hashlib
import json

from sightglass.contracts.capture import (
    CaptureProtocolError,
    CaptureRequest,
    ResourceCaptureBinding,
)
from sightglass.contracts.resources import SourceResource
from sightglass.source.identity import opaque_id

from .codec import canonical_json, strict_json


def resource_descriptor_digest(descriptor: SourceResource) -> str:
    return hashlib.sha256(canonical_json(descriptor)).hexdigest()


def request_resource_binding(request: CaptureRequest) -> ResourceCaptureBinding:
    descriptor = request.resource_descriptor
    if descriptor is None or request.resource_revision_json is None:
        raise CaptureProtocolError("resource_binding_evidence_required")
    revision = strict_json(request.resource_revision_json.encode(), max_bytes=65_536)
    keys = {
        "resource_id",
        "message_id",
        "source_resource_key",
        "availability",
        "resolver_json",
        "kind",
        "mime_type",
        "declared_size",
        "declared_hash",
    }
    if not isinstance(revision, dict) or set(revision) != keys:
        raise CaptureProtocolError("resource_revision_fields_mismatch")
    # Preserve ResourceService's declared canonical hash, including resolver_json's
    # exact string. This is core state evidence, not a claim about source freshness.
    encoded = json.dumps(revision, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    account_id = opaque_id("wxacct", request.account_id)
    if (
        hashlib.sha256(encoded).hexdigest() != request.expected_resource_revision
        or resource_descriptor_digest(descriptor) != request.resource_descriptor_digest
        or descriptor.source_resource_key != request.resource_key
        or revision["source_resource_key"] != request.resource_key
        or revision["message_id"] != opaque_id("wxmsg", account_id, request.focus_source_message_id)
        or any(
            revision[name] != getattr(descriptor, name)
            for name in (
                "kind",
                "mime_type",
                "declared_size",
                "declared_hash",
                "availability",
            )
        )
    ):
        raise CaptureProtocolError("resource_revision_mismatch")
    resolver = revision["resolver_json"]
    if not isinstance(resolver, str):
        raise CaptureProtocolError("resource_resolver_invalid")
    parsed = strict_json(resolver.encode(), max_bytes=8192)
    if not isinstance(parsed, dict) or parsed.get("active", True) is not True:
        raise CaptureProtocolError("resource_resolver_inactive")
    return ResourceCaptureBinding(
        request.account_id,
        request.conversation_source_id or "",
        request.focus_source_message_id or "",
        request.resource_key or "",
        request.resource_descriptor_digest or "",
        request.expected_resource_revision or "",
    )

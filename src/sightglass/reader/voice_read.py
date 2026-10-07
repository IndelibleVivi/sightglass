"""Page-level voice preparation for reader responses.

The reader path prepares at most one bounded voice batch per response, after the
delivered row set is fixed.  This module owns the page sidecar, the derived
transcript read of a single resource, and the manifest rules that stay outside the
voice domain service.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.voice import (
    VoiceBatchReceipt,
    VoiceCoverage,
    VoiceReadSettings,
    VoiceSelectionItem,
)
from sightglass.resources.rich import bound_text
from sightglass.resources.types import ResourceReadPayload
from sightglass.voice.service import VoiceService, transcript_recipe

VOICE_SIDECAR_SCHEMA = "sightglass.voice-sidecar.v1"
VOICE_SIDECAR_FIELDS = ("message_id", "resource_id", "state", "text")
VOICE_SIDECAR_RESERVE_CHARS = 768
VOICE_SIDECAR_MAX_ITEMS = 32
VOICE_SIDECAR_INLINE_CHARS = 4_000
VOICE_SIDECAR_ITEM_OVERHEAD_CHARS = 8
VOICE_READABLE_AVAILABILITY = frozenset({"available", "local_available"})
VOICE_READ_GUIDE = (
    "read committed transcripts with wechat_read_transcripts(reading_token=..., cursor=...)"
)
VOICE_READ_BUDGET_GUIDE = (
    "read the remaining transcripts with wechat_read_transcripts(reading_token=..., cursor=...)"
)
VOICE_READ_UNREADABLE_GUIDE = (
    "no local voice payload is readable for these messages, so no transcript was prepared"
)
VOICE_TEXT_STATES = frozenset({"ready", "empty"})
# The derived transcript read shares the ordinary text-mode line window: an
# explicit window is capped at 500 lines and an open end defaults to 200.
VOICE_TRANSCRIPT_WINDOW_LINES = 500
VOICE_TRANSCRIPT_DEFAULT_WINDOW_LINES = 200


def _size(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def _window_lines(
    text: str, *, start_line: int | None, end_line: int | None
) -> tuple[str, dict[str, int] | None]:
    """Select one explicit transcript line window using the shared text rules."""

    if start_line is None and end_line is None:
        return text, None
    lines = text.splitlines()
    start = 1 if start_line is None else int(start_line)
    end = (
        min(len(lines), start + VOICE_TRANSCRIPT_DEFAULT_WINDOW_LINES - 1)
        if end_line is None
        else int(end_line)
    )
    if start < 1 or end < start or end - start + 1 > VOICE_TRANSCRIPT_WINDOW_LINES:
        raise SightglassError(ErrorCode.QUERY_INVALID)
    selected = lines[start - 1 : min(end, len(lines))]
    return "\n".join(selected), {"start": start, "end": start - 1 + len(selected)}


@dataclass(frozen=True)
class VoiceCandidate:
    """One message-bound voice resource in page order."""

    message_id: str
    resource_id: str
    account_id: str
    revision: str | None
    readable: bool


class VoiceReadPreparation:
    """Prepare or reuse the voice batch for one delivered message set."""

    def __init__(
        self,
        service: VoiceService,
        settings: VoiceReadSettings,
        repository: Any,
    ) -> None:
        self.service = service
        self.settings = settings
        self.repository = repository

    def policy_for(self, requested: str | None) -> str:
        if requested is not None and requested not in {"auto", "cached", "off"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        return self.settings.policy_for(requested)

    def _prepare_batch(
        self,
        *,
        reader_id: str,
        account_id: str,
        account_binding_id: str | None,
        selection: tuple[VoiceSelectionItem, ...],
        policy: str,
    ) -> VoiceBatchReceipt:
        """A failed optional write must not discard the already admitted page."""

        try:
            return self.service.create_batch(
                reader_id=reader_id,
                account_id=account_id,
                account_binding_id=account_binding_id,
                selection=selection,
                **_recipe(self.settings.language),
                voice_policy=policy,
            )
        except SightglassError as error:
            if error.code != ErrorCode.STORAGE_PRESSURE:
                raise
            return VoiceBatchReceipt(
                None, False, "storage_pressure", None,
                VoiceCoverage(selected=len(selection), not_scheduled=len(selection)),
            )

    def account_binding_id(self, account_id: str) -> str | None:
        """Resolve the installation binding of one active account, or fail closed."""

        for row in self.repository.active_accounts():
            if str(row["account_id"]) == account_id:
                binding = row["account_binding_id"]
                return None if binding is None else str(binding)
        raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)

    def collect(self, message_ids: tuple[str, ...]) -> tuple[VoiceCandidate, ...]:
        """Collect the voice-bound rows of one candidate page, in message order."""

        return tuple(
            VoiceCandidate(
                message_id=str(row["message_id"]),
                resource_id=str(row["resource_id"]),
                account_id=str(row["account_id"]),
                revision=(str(row["binding_fingerprint"]) if row["binding_fingerprint"] else None),
                readable=bool(row["bound"])
                or str(row["availability"]) in VOICE_READABLE_AVAILABILITY,
            )
            for row in self.repository.voice_resources_for_messages(message_ids)
        )

    def candidate_for_resource(self, row: Any) -> VoiceCandidate | None:
        """Project one resource row as a voice candidate, or ``None`` when it is not voice."""

        if str(row["kind"]) != "voice":
            return None
        try:
            resolver: Any = json.loads(str(row["resolver_json"]))
        except (KeyError, TypeError, json.JSONDecodeError):
            resolver = {}
        fingerprint = resolver.get("binding_fingerprint") if isinstance(resolver, dict) else None
        resource_id = str(row["resource_id"])
        return VoiceCandidate(
            message_id=str(row["message_id"]),
            resource_id=resource_id,
            account_id=str(row["account_id"]),
            revision=(str(fingerprint) if isinstance(fingerprint, str) and fingerprint else None),
            readable=(
                self.repository.resource_binding(resource_id, "original") is not None
                or self.repository.resource_binding(resource_id, "thumbnail") is not None
                or str(row["availability"]) in VOICE_READABLE_AVAILABILITY
            ),
        )

    @staticmethod
    def reserve_chars(policy: str, candidates: tuple[VoiceCandidate, ...]) -> int:
        """Reserve room for the smallest sidecar, so a prepared batch stays reachable."""

        if policy == "off" or not candidates:
            return 0
        return VOICE_SIDECAR_RESERVE_CHARS

    def attach(
        self,
        page: dict[str, Any],
        *,
        reader_id: str,
        message_ids: tuple[str, ...],
        candidates: tuple[VoiceCandidate, ...],
        account_binding_id: str | None,
        policy: str,
        budget_chars: int,
        focus_message_ids: frozenset[str] = frozenset(),
    ) -> dict[str, Any]:
        """Attach the page-level sidecar; called once, after the delivered rows are fixed."""

        if policy == "off" or not candidates:
            return page
        order = {message_id: index for index, message_id in enumerate(message_ids)}
        delivered = tuple(
            sorted(
                (item for item in candidates if item.message_id in order),
                key=lambda item: (
                    item.message_id not in focus_message_ids,
                    order[item.message_id],
                ),
            )
        )
        if not delivered:
            return page
        eligible = (
            delivered if policy == "cached" else tuple(item for item in delivered if item.readable)
        )
        selection = tuple(
            VoiceSelectionItem(item.message_id, item.resource_id, str(item.revision), None)
            for item in eligible
            if item.revision
        )
        batch: dict[str, Any] | None = None
        storage_blocked = False
        if selection:
            receipt = self._prepare_batch(
                reader_id=reader_id,
                account_id=delivered[0].account_id,
                account_binding_id=account_binding_id,
                selection=selection,
                policy=policy,
            )
            storage_blocked = receipt.reason == "storage_pressure"
            if receipt.reading_token is not None:
                batch = self.service.read_batch(
                    receipt.reading_token, limit=VOICE_SIDECAR_MAX_ITEMS
                )
        sidecar = self._sidecar(
            policy=policy,
            batch=batch,
            selected=len(selection),
            unreadable=sum(1 for item in delivered if not item.readable),
            unknown_revision=sum(1 for item in eligible if not item.revision),
        )
        remaining = max(
            0,
            min(VOICE_SIDECAR_INLINE_CHARS, budget_chars - _size(page) - _size(sidecar)),
        )
        sidecar["items"] = _item_rows(batch, remaining=remaining)
        sidecar["text_inline"] = any(row[3] for row in sidecar["items"])
        if batch is None:
            sidecar["guide"] = VOICE_READ_UNREADABLE_GUIDE
        elif sidecar["items"] and not sidecar["text_inline"]:
            sidecar["guide"] = VOICE_READ_BUDGET_GUIDE
        if storage_blocked:
            sidecar["state"] = "storage_pressure"
            sidecar["coverage"] = VoiceCoverage(
                selected=len(selection),
                not_scheduled=len(selection),
            ).as_dict()
            sidecar["guide"] = (
                "voice preparation paused by storage budget; retry after capacity recovery"
            )
        page["voice"] = sidecar
        if _size(page) > budget_chars:
            sidecar["items"] = _item_rows(batch, remaining=0)
            sidecar["text_inline"] = False
            if batch is not None:
                sidecar["guide"] = VOICE_READ_BUDGET_GUIDE
        if "projection_receipt" in page:
            page["projection_receipt"]["serialized_chars"] = _size(page)
        return page

    def resource_text(
        self,
        *,
        reader_id: str,
        candidate: VoiceCandidate,
        account_binding_id: str | None,
        policy: str,
        start_line: int | None,
        end_line: int | None,
        max_chars: int,
        max_bytes: int,
    ) -> ResourceReadPayload:
        """Derive one resource's transcript text from the same batch cache.

        The transcript is plain derived text, so it shares the ordinary text-mode
        line window and character/byte budgets instead of returning an unbounded
        blob; ``returned`` always reports the text that was actually emitted.
        """

        selection = (
            (
                VoiceSelectionItem(
                    candidate.message_id,
                    candidate.resource_id,
                    str(candidate.revision),
                    None,
                ),
            )
            if candidate.revision and (policy == "cached" or candidate.readable)
            else ()
        )
        batch: dict[str, Any] | None = None
        storage_blocked = False
        if selection:
            receipt = self._prepare_batch(
                reader_id=reader_id,
                account_id=candidate.account_id,
                account_binding_id=account_binding_id,
                selection=selection,
                policy=policy,
            )
            storage_blocked = receipt.reason == "storage_pressure"
            if receipt.reading_token is not None:
                batch = self.service.read_batch(receipt.reading_token, limit=1)
        item = batch["items"][0] if batch is not None and batch["items"] else None
        if item is not None:
            state = str(item["state"])
        elif not candidate.revision:
            state = "unsupported"
        elif not candidate.readable:
            state = "blocked"
        else:
            state = "not_scheduled"
        descriptor: dict[str, Any] = {
            "schema": "sightglass.resource-read.v1",
            "mode": "text",
            "resource": {
                "schema": "sightglass.resource.v1",
                "resource_id": candidate.resource_id,
                "source_message_id": candidate.message_id,
                "kind": "voice",
            },
            "page": None,
            "member": None,
            "sheet": None,
            "cell_range": None,
            "line_range": None,
            "returned": {"bytes": 0, "chars": 0, "truncated": False},
            "media": None,
            "resolution": {"path": "derived", "variant": "derived_transcript"},
            "derivation": {"kind": "derived_transcript", "tool": None},
            "warnings": ["derived_transcript_source_not_reread"],
            "transcript": {
                "schema": "sightglass.voice-transcript.v1",
                "state": state,
                "error_code": (
                    str(item["error_code"]) if item is not None and item.get("error_code") else None
                ),
                "reading_token": batch["reading_token"] if batch is not None else None,
                "expires_at": batch["expires_at"] if batch is not None else None,
                "coverage": (batch["coverage"] if batch is not None else VoiceCoverage().as_dict()),
                "guide": VOICE_READ_GUIDE,
            },
        }
        if not candidate.readable:
            descriptor["warnings"].append("voice_payload_unavailable")
        if storage_blocked:
            descriptor["warnings"].append("storage_pressure")
            descriptor["transcript"]["error_code"] = "STORAGE_PRESSURE"
        if state in VOICE_TEXT_STATES:
            text = str(item["text"]) if item is not None and item["text"] else ""
            window, line_range = _window_lines(text, start_line=start_line, end_line=end_line)
            bounded, truncated = bound_text(window, max_chars=max_chars, max_bytes=max_bytes)
            descriptor["line_range"] = line_range
            descriptor["returned"] = {
                "bytes": len(bounded.encode("utf-8")),
                "chars": len(bounded),
                "truncated": truncated,
            }
            descriptor["media"] = {"mime_type": "text/plain", "content_block_type": "text"}
            descriptor["transcript"]["state"] = "ready" if text else "empty"
            return ResourceReadPayload(
                descriptor,
                mime_type="text/plain",
                content_kind="text",
                text=bounded,
            )
        if state in {"blocked", "failed", "unsupported"}:
            descriptor["warnings"].append(f"transcript_{state}")
        return ResourceReadPayload(descriptor)

    def _sidecar(
        self,
        *,
        policy: str,
        batch: dict[str, Any] | None,
        selected: int,
        unreadable: int,
        unknown_revision: int,
    ) -> dict[str, Any]:
        coverage = batch["coverage"] if batch is not None else VoiceCoverage().as_dict()
        return {
            "schema": VOICE_SIDECAR_SCHEMA,
            "policy": policy,
            "state": self._state(coverage, policy, selected),
            "derivation": {"kind": "derived_transcript"},
            "reading_token": batch["reading_token"] if batch is not None else None,
            "expires_at": batch["expires_at"] if batch is not None else None,
            "fields": list(VOICE_SIDECAR_FIELDS),
            "items": [],
            "items_complete": selected <= VOICE_SIDECAR_MAX_ITEMS,
            "text_inline": False,
            "selected_count": selected,
            "coverage": coverage,
            "excluded": {"unreadable": unreadable, "unknown_revision": unknown_revision},
            "guide": VOICE_READ_GUIDE,
        }

    @staticmethod
    def _state(coverage: dict[str, int], policy: str, selected: int) -> str:
        if not selected:
            return "unreadable"
        if coverage["ready"] + coverage["empty"] >= selected:
            return "cached"
        if coverage["pending"] > 0:
            return "prepared"
        if policy == "cached":
            return "cached_only"
        return "terminal"


def _item_rows(batch: dict[str, Any] | None, *, remaining: int) -> list[list[Any]]:
    """Sparse row-level mapping: only rows that already have a committed transcript."""

    if batch is None:
        return []
    rows: list[list[Any]] = []
    used = 0
    for item in batch["items"]:
        if item["state"] not in VOICE_TEXT_STATES:
            continue
        text = str(item["text"]) if item["text"] else ""
        inline = ""
        if text:
            cost = len(text) + VOICE_SIDECAR_ITEM_OVERHEAD_CHARS
            if used + cost <= remaining:
                inline = text
                used += cost
        rows.append([item["message_id"], item["resource_id"], item["state"], inline or None])
    return rows


def _recipe(language: str) -> dict[str, str]:
    digest, recipe_json = transcript_recipe(language=language)
    return {"recipe_digest": digest, "recipe_json": recipe_json}

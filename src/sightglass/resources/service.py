from __future__ import annotations

import hashlib
import json
import re
import stat
import sys
import threading
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager, nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from sightglass.contracts.common import utc_now
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.messages import ParsedMessage, SourceMessage
from sightglass.contracts.resources import SourceResourceVariant
from sightglass.model.repositories import WindowRepository
from sightglass.operations import local_read_only_requested
from sightglass.policy.readers import ReaderContext
from sightglass.runtime.lanes import RuntimeLanes, WorkClass
from sightglass.source.base import SourceScope, SourceSnapshot, WeChatSourceProvider
from sightglass.source.parser import parse_message
from sightglass.storage import storage_scope

from .cache import ResourceObjectStore
from .coalesce import DerivationRequest, StageCoalescer
from .jobs import ResourceJobService
from .processors import (
    MAX_EXTRACTED_TEXT_BYTES,
    MAX_SOURCE_BYTES,
    MAX_SOURCE_ENVELOPE_BYTES,
    WXGF_MIME,
    decode_text,
    decode_text_with_encoding,
    extract_pdf_text,
    image_preview,
    image_preview_processor_version,
    inspect_image,
    inspect_media,
    inspect_pdf,
    render_pdf_page,
    render_video_preview,
    sniff_mime,
)
from .rich import (
    CSV_MIMES,
    OFFICE_MIMES,
    PPTX_MIME,
    TEXTUAL_RICH_MIMES,
    TSV_MIMES,
    XLSX_MIME,
    ZIP_MIME,
    extract_rich_text,
    extract_slide,
    extract_table,
    inspect_rich_metadata,
    is_rich_mime,
    list_archive_members,
    rich_mime_from_name,
)
from .types import CachedObject, ResourceReadPayload


@dataclass(frozen=True)
class _StagedBinding:
    """A verified object store entry waiting for the short window.db admission."""

    resource_id: str
    variant: str
    object_digest: str
    local_path_internal: str
    mime_type: str
    byte_size: int
    origin: str
    # The immutable staged bytes (a second reference, never a copy). ``_persist_staged``
    # republishes them under the admission writer lock so a cleanup that reaped the
    # unregistered CAS file after ``_stage`` cannot leave a committed row pointing at a
    # path that no longer exists.
    data: bytes
    device: int | None
    inode: int | None


@dataclass(frozen=True)
class _ResolvedSource:
    original: CachedObject
    warnings: tuple[str, ...]
    source_variant: SourceResourceVariant
    source_receipt: dict[str, Any]
    # The resource revision captured *before* acquisition. Publication compares this
    # exact captured value against the row at admission, so bytes read under one
    # resolver state can never bind as a different revision that appeared mid-read.
    resource_revision: str = ""
    prior_source_bindings: tuple[tuple[str, str], ...] = ()
    staged: tuple[_StagedBinding, ...] = ()


@dataclass(frozen=True)
class _StagedDerivation:
    """One verified derivation binding waiting for the short window.db admission.

    This is the provenance proof for a generated artifact: it records exactly which
    source object digest, source variant, processor recipe, and parameters produced the
    derived object. It is persisted in the existing ``resource_derivations`` table in
    the same transaction as the derived object row, so a cache hit can later confirm
    that the bytes it would serve really derive from the input this read holds.
    """

    resource_id: str
    source_digest: str
    variant: str
    processor_name: str
    processor_version: str
    parameters_json: str
    derived_digest: str


@dataclass(frozen=True)
class _PreviewProvenance:
    source_digest: str
    source_variant: SourceResourceVariant
    processor_name: str
    processor_version: str
    parameters_json: str


_OriginalKind = Literal["image", "audio", "blob", "text"]
# Source entries this service may serve, in preference order: the original first,
# then a source-kept derived preview.
_SOURCE_VARIANTS: tuple[SourceResourceVariant, ...] = ("original", "thumbnail")
_GENERATED_IMAGE_PREVIEW_VARIANT = "preview:v2"
_ASYNC_PDF_BYTES = 1024 * 1024
# Recipe identity for generated image/document previews. ``processor_version`` is part
# of the provenance key, so a change to the rendering recipe invalidates every cached
# preview that was produced by an older recipe. ``scale`` mirrors the bounded preview
# dimension in ``image_preview`` and is recorded so a later change to that bound is a
# different recipe rather than a silent reuse. The image version is derived from the
# active platform backend (``sips`` on macOS, ``libvips`` on Linux) so a cached preview
# produced by the other backend is not trusted as this platform's provenance.
_IMAGE_PREVIEW_PROCESSOR = "image-preview"
_IMAGE_PREVIEW_PROCESSOR_VERSION = image_preview_processor_version()
_PDF_PAGE_PROCESSOR = "pdf-page"
_PDF_PAGE_PROCESSOR_VERSION = "pdftoppm-v1"
_VIDEO_PREVIEW_PROCESSOR = "video-preview"
_VIDEO_PREVIEW_PROCESSOR_VERSION = "ffmpeg-v1"
_PREVIEW_SCALE_BOUND = 2048


class ResourceService:
    """Message-bound resource resolution, processing, caching, and projection."""

    def __init__(
        self,
        provider: WeChatSourceProvider,
        repository: WindowRepository,
        reader: ReaderContext,
        *,
        projection_epoch: Callable[[], str],
    ) -> None:
        self.provider = provider
        self.repository = repository
        self.reader = reader
        self.projection_epoch = projection_epoch
        self.storage = repository.database.storage
        self.cache = ResourceObjectStore(repository.database.path, storage=self.storage)
        self.coalescer = StageCoalescer()
        self.runtime_lanes: RuntimeLanes | None = None
        self.jobs: ResourceJobService | None = None
        self._wake_resource_worker: Callable[[], None] | None = None
        self._source_foreground_enter: Callable[[], None] | None = None
        self._source_foreground_exit: Callable[[], None] | None = None
        self.remote_acquire: Callable[[Any, str], _ResolvedSource] | None = None
        self._runtime_lock = threading.Lock()
        self._runtime_counts = {
            "cache_hit": 0,
            "cache_miss": 0,
            "source_acquisition": 0,
            "derivation": 0,
            "deferred": 0,
            "failures": 0,
        }
        self._runtime_failures: deque[dict[str, Any]] = deque(maxlen=32)

    def configure_runtime(
        self,
        *,
        lanes: RuntimeLanes,
        jobs: ResourceJobService,
        wake_resource_worker: Callable[[], None],
        source_foreground_enter: Callable[[], None],
        source_foreground_exit: Callable[[], None],
    ) -> None:
        """Attach daemon-owned scheduling without changing direct in-process callers."""

        self.runtime_lanes = lanes
        self.jobs = jobs
        self._wake_resource_worker = wake_resource_worker
        self._source_foreground_enter = source_foreground_enter
        self._source_foreground_exit = source_foreground_exit

    def _note_runtime(self, key: str) -> None:
        with self._runtime_lock:
            self._runtime_counts[key] += 1

    def runtime_status(self) -> dict[str, Any]:
        with self._runtime_lock:
            counts = dict(self._runtime_counts)
            failures = list(self._runtime_failures)
        return {
            "schema": "sightglass.resource-runtime-status.v1",
            "routes": counts,
            "coalescing": self.coalescer.status(),
            "jobs": self.jobs.status() if self.jobs is not None else None,
            "recent_failures": failures,
            "failure_limit": 32,
        }

    def _record_failure(self, resource_id: str, exc: SightglassError) -> None:
        format_family = None
        try:
            row = self.repository.resource_context(resource_id)
            if row is not None:
                declared = str(row["mime_type"]) if row["mime_type"] else None
                format_family = self._format_family(declared, str(row["kind"]))
        except Exception:
            format_family = None
        failure = {
            "tool": "wechat_read_resource",
            "code": exc.code.value,
            "phase": exc.details.get("phase"),
            "reason": exc.details.get("reason"),
            "format_family": format_family,
            "retryable": exc.retryable,
        }
        with self._runtime_lock:
            self._runtime_counts["failures"] += 1
            self._runtime_failures.append(failure)

    @contextmanager
    def _lane(self, lane: WorkClass, *, wait: bool) -> Iterator[bool]:
        lanes = self.runtime_lanes
        if lanes is None:
            yield True
            return
        with lanes.held(lane, wait=wait) as lease:
            yield lease is not None

    @contextmanager
    def _foreground_source(self) -> Iterator[None]:
        enter = self._source_foreground_enter
        exit_ = self._source_foreground_exit
        if enter is None or exit_ is None:
            yield
            return
        enter()
        try:
            yield
        finally:
            exit_()

    @contextmanager
    def _source_read(self) -> Iterator[tuple[ExitStack, SourceSnapshot]]:
        """Open a provider snapshot outside any window.db writer transaction."""

        stack = ExitStack()
        try:
            if self.storage is not None:
                self.storage.require()
            stack.enter_context(storage_scope(self.storage))
            snapshot = stack.enter_context(self.provider.snapshot())
            yield stack, snapshot
        except BaseException:
            stack.__exit__(*sys.exc_info())
            raise
        finally:
            stack.close()

    @contextmanager
    def _resource_scope(self, source_resource_key: str) -> Iterator[SourceSnapshot]:
        """Open a resource-scoped source lease outside any window.db writer transaction.

        The scope names the exact binding-authenticated locator this acquisition may
        depend on, so the provider validates and reads only that locator's auxiliary
        databases and files. The context manager releases the source lease on exit,
        which is where the provider re-validates its selected dependencies.
        """

        scope = SourceScope.resource(source_resource_key)
        if self.storage is not None:
            self.storage.require()
        with storage_scope(self.storage):
            with self.provider.session(scope) as snapshot:
                yield snapshot

    @contextmanager
    def _admission(self, snapshot_stack: ExitStack) -> Iterator[None]:
        """Run the final window.db admission, validated before commit.

        The source read phase must already be finished: this transaction only performs
        local window.db work, and its commit happens only after the still-open provider
        snapshot validates itself. A generation change therefore rolls back every
        staged resource object and binding.
        """

        with self.repository.database.transaction():
            yield
            snapshot_stack.close()

    @staticmethod
    def _resolver(row: Any) -> dict[str, Any]:
        try:
            value = json.loads(str(row["resolver_json"]))
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise SightglassError(ErrorCode.INTERNAL_ERROR) from exc
        if not isinstance(value, dict):
            raise SightglassError(ErrorCode.INTERNAL_ERROR)
        return value

    def _message_row(self, message_id: str) -> Any:
        row = self.repository.message_position_row(message_id)
        if row is None:
            raise SightglassError(ErrorCode.MESSAGE_NOT_FOUND)
        self.reader.authorize(str(row["conversation_id"]))
        return row

    def _resource_row(self, resource_id: str) -> Any:
        row = self.repository.resource_context(resource_id)
        if row is None:
            raise SightglassError(ErrorCode.RESOURCE_NOT_FOUND)
        self.reader.authorize(str(row["conversation_id"]))
        if self._resolver(row).get("active", True) is False:
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
        return row

    def _source_message(
        self, row: Any, snapshot: SourceSnapshot
    ) -> tuple[SourceMessage, ParsedMessage]:
        """Read the message that owns ``row`` from the current source snapshot."""

        context = self.repository.conversation_context(str(row["conversation_id"]))
        if context is None:
            raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        source = self.provider.get_message(
            str(context["source_account_key"]),
            str(row["source_message_id"]),
            snapshot,
        )
        if source is None:
            raise SightglassError(ErrorCode.MESSAGE_NOT_FOUND)
        return source, parse_message(source)

    def _admit_message(self, row: Any, source: SourceMessage, parsed: ParsedMessage) -> None:
        """Persist one source-read message and reconcile its resource binding."""
        self.repository.database.reserve_growth(
            16_384 + 4 * len(source.raw_content.encode("utf-8"))
        )

        self.repository.upsert_message(
            str(row["account_id"]),
            str(row["conversation_id"]),
            str(row["sender_id"]) if row["sender_id"] else None,
            str(row["sender_membership_id"]) if row["sender_membership_id"] else None,
            source,
            parsed,
            projection_epoch=self.projection_epoch(),
        )

    def _binding_available(self, resource_id: str, variant: str) -> bool:
        return self.repository.resource_binding(resource_id, variant) is not None

    def _source_binding_current(self, row: Any, variant: str) -> bool:
        binding = self.repository.resource_binding(str(row["resource_id"]), variant)
        if binding is None:
            return False
        if self._origin_kind() != "macos-wechat" or row["kind"] != "image":
            return True
        return self._derivation_matches(
            resource_id=str(row["resource_id"]),
            source_digest=str(binding["object_digest"]),
            variant="source:" + variant,
            processor_name="native-image-decoder",
            processor_version=self.provider.descriptor.implementation,
            parameters_json=self._source_provenance_parameters(row),
            derived_digest=str(binding["object_digest"]),
        )

    def _source_provenance_parameters(self, row: Any) -> str:
        return json.dumps(
            {"resource_revision": self._resource_revision(row)},
            sort_keys=True,
            separators=(",", ":"),
        )

    def _cache_ready(self, row: Any) -> bool:
        """True when the private CAS already holds the bytes this read needs.

        Mirrors the source-variant preference in ``_ensure_source_payload``: a source
        that declares a local original is answered only by its ``original`` binding,
        otherwise an ``original`` or source-kept ``thumbnail`` binding will do. A ready
        row means no provider snapshot is needed; integrity is still verified when the
        binding is actually read.
        """

        declares_original = str(row["availability"]) in {"local_available", "archive_available"}
        for variant in _SOURCE_VARIANTS:
            if variant == "thumbnail" and declares_original:
                break
            if self._source_binding_current(row, variant):
                return True
        return False

    def _origin_kind(self) -> str:
        origin = getattr(self.provider, "origin_descriptor", self.provider.descriptor)
        return str(origin.kind)

    @staticmethod
    def resource_revision_fields(row: Any) -> dict[str, Any]:
        resource_id = str(row["resource_id"])
        value = {
            "resource_id": resource_id,
            "message_id": str(row["message_id"]),
            "source_resource_key": (
                str(row["source_resource_key"]) if row["source_resource_key"] else None
            ),
            "availability": str(row["availability"]),
            "resolver_json": str(row["resolver_json"]),
            "kind": str(row["kind"]),
            "mime_type": str(row["mime_type"]) if row["mime_type"] else None,
            "declared_size": (
                int(row["declared_size"]) if row["declared_size"] is not None else None
            ),
            "declared_hash": str(row["declared_hash"]) if row["declared_hash"] else None,
        }
        return value

    def _resource_revision(self, row: Any) -> str:
        encoded = json.dumps(self.resource_revision_fields(row), sort_keys=True,
                             separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _verify_resolved_revision(
        self,
        resource_id: str,
        resolved: _ResolvedSource,
    ) -> None:
        # Compare against the revision captured at acquisition, never against a value
        # recomputed after the source bytes were read: the resolver may have advanced
        # while those bytes were in flight, so a fresh recomputation would silently
        # admit revision-B bytes under revision A's read.
        captured_revision = resolved.resource_revision
        current = self._resource_row(resource_id)
        binding = self.repository.resource_binding(resource_id, resolved.source_variant)
        if self._resource_revision(current) != captured_revision or (
            binding is not None
            and str(binding["object_digest"]) != resolved.original.digest
            and str(binding["object_digest"])
            != dict(resolved.prior_source_bindings).get(resolved.source_variant)
        ):
            raise SightglassError(
                ErrorCode.SOURCE_GENERATION_CHANGED,
                retryable=True,
                details={"phase": "admit", "reason": "resource_resolver_changed"},
            )

    def local_read_ready(self, resource_id: str, mode: str) -> bool:
        """Conservative local-only classification for the daemon routing decision.

        Returns ``True`` only when the resource row exists, the current reader is
        authorized to reach it, the canonical resolver is active, and the private CAS
        already holds the required source bytes. Any missing precondition, policy
        denial, revocation, or lookup failure reports ``False`` so the caller keeps the
        existing source-backed path, which then produces the correct error itself.
        """

        if mode not in self._MODE_SELECTORS:
            return False
        try:
            row = self._resource_row(resource_id)
        except SightglassError:
            return False
        return self._cache_ready(row)

    def _descriptor(self, row: Any) -> dict[str, Any]:
        resource_id = str(row["resource_id"])
        availability = str(row["availability"])
        original_available = self._binding_available(resource_id, "original") or availability in {
            "local_available",
            "archive_available",
        }
        declared_mime = str(row["mime_type"]) if row["mime_type"] else None
        detected_mime = None
        for variant in _SOURCE_VARIANTS:
            binding = self.repository.resource_binding(resource_id, variant)
            if binding is not None and binding["mime_type"]:
                detected_mime = str(binding["mime_type"])
                break
        named_mime = rich_mime_from_name(
            str(row["original_name"]) if row["original_name"] else None
        )
        effective_hint = detected_mime or declared_mime or named_mime
        format_family = self._format_family(effective_hint, str(row["kind"]))
        preview_supported = (
            str(row["kind"]) in {"image", "sticker"}
            or bool(declared_mime and declared_mime.startswith("image/"))
            or declared_mime == "application/pdf"
            or bool(effective_hint and effective_hint.startswith("video/"))
            or format_family == "video"
        )
        # A source-kept derived preview still supports a preview without asserting a
        # source original; availability stays the source-declared state.
        preview_available = bool(
            preview_supported
            and (
                original_available
                or availability == "preview_only"
                or self._binding_available(resource_id, "thumbnail")
            )
        )
        return {
            "schema": "sightglass.resource.v2",
            "resource_id": resource_id,
            "source_message_id": str(row["message_id"]),
            "conversation_id": str(row["conversation_id"]),
            "kind": str(row["kind"]),
            "mime_type": declared_mime,
            "declared_mime": declared_mime,
            "detected_mime": detected_mime,
            "format_family": format_family,
            "original_name": str(row["original_name"]) if row["original_name"] else None,
            "declared_size": (
                int(row["declared_size"]) if row["declared_size"] is not None else None
            ),
            "declared_hash": str(row["declared_hash"]) if row["declared_hash"] else None,
            "availability": ("local_available" if original_available else availability),
            "preview_available": preview_available,
            "original_available": original_available,
            "available_views": self._available_views(
                effective_hint,
                format_family,
                original_available=original_available,
                preview_available=preview_available,
            ),
        }

    @staticmethod
    def _format_family(mime_type: str | None, kind: str | None = None) -> str:
        if mime_type is not None:
            if mime_type.startswith("image/"):
                return "image"
            if mime_type.startswith("audio/"):
                return "audio"
            if mime_type.startswith("video/"):
                return "video"
            if mime_type == "application/pdf":
                return "pdf"
            if mime_type == XLSX_MIME or mime_type in CSV_MIMES | TSV_MIMES:
                return "workbook"
            if mime_type == PPTX_MIME:
                return "presentation"
            if mime_type == ZIP_MIME:
                return "archive"
            if mime_type.startswith("text/") or mime_type in TEXTUAL_RICH_MIMES:
                return "text"
            if mime_type in OFFICE_MIMES:
                return "office"
        if kind in {"image", "sticker"}:
            return "image"
        if kind == "voice":
            return "audio"
        if kind == "video":
            return "video"
        return "binary"

    @staticmethod
    def _available_views(
        mime_type: str | None,
        format_family: str,
        *,
        original_available: bool,
        preview_available: bool,
    ) -> dict[str, bool]:
        return {
            "metadata": True,
            "original": original_available,
            "preview": preview_available,
            "text": format_family in {"text", "pdf", "office", "presentation", "archive"},
            "page": mime_type == "application/pdf",
            "members": mime_type == ZIP_MIME,
            "table": bool(mime_type in CSV_MIMES | TSV_MIMES | {XLSX_MIME}),
            "slide": mime_type == PPTX_MIME,
        }

    @staticmethod
    def _source_receipt(snapshot: SourceSnapshot | None, *, local: bool = False) -> dict[str, Any]:
        """Receipt distinguishing a live-source read from a local-cache read.

        A local read is materialized, digest-verified content that the source is not
        re-queried for, so it can never be presented as a fresh live confirmation: it
        reports ``complete=False`` and a ``freshness`` block whose ``mode`` is
        ``local_cache``. The live path keeps its ``complete=True`` snapshot binding.
        """

        if local or snapshot is None:
            return {
                "complete": False,
                "fresh_as_of": None,
                "inventory_digest": None,
                "generation_set_digest": None,
                "warnings": ["served_from_local_cache"],
                "freshness": {"mode": "local_cache", "live_refresh_confirmed": False},
            }
        return {
            "complete": True,
            "fresh_as_of": snapshot.fresh_as_of,
            "inventory_digest": snapshot.inventory_digest,
            "generation_set_digest": snapshot.generation_set_digest,
            "warnings": [],
            "freshness": {"mode": "live_source", "live_refresh_confirmed": True},
        }

    def _resource_list(
        self, message_id: str, source_receipt: dict[str, Any],
    ) -> dict[str, Any]:
        """Publish descriptors inside the caller's validated admission/read snapshot."""

        self.reader.require_resource("metadata")
        self._message_row(message_id)
        resources = []
        for item in self.repository.resources_for_message(message_id):
            context = self.repository.resource_context(str(item["resource_id"]))
            if context is None:
                raise SightglassError(ErrorCode.INTERNAL_ERROR)
            resources.append(self._descriptor(context))
        return {
            "schema": "sightglass.resource-list.v1",
            "message_id": message_id,
            "resources": resources,
            "source_receipt": source_receipt,
        }

    def list_resources(self, message_id: str) -> dict[str, Any]:
        self.reader.require_resource("metadata")
        with self._source_read() as (stack, snapshot):
            row = self._message_row(message_id)
            source, parsed = self._source_message(row, snapshot)
            with self._admission(stack):
                self._admit_message(row, source, parsed)
                return self._resource_list(message_id, self._source_receipt(snapshot))

    @staticmethod
    def _effective_mime(
        data: bytes,
        declared_mime: str | None,
        original_name: str | None = None,
    ) -> tuple[str, list[str]]:
        sniffed = sniff_mime(data)
        named_mime = rich_mime_from_name(original_name)
        hinted_mime = (
            named_mime
            if named_mime is not None
            and declared_mime in {None, "application/octet-stream", "text/plain"}
            else declared_mime or named_mime
        )
        if (
            sniffed == "application/octet-stream"
            and hinted_mime
            and (hinted_mime.startswith("text/") or hinted_mime in TEXTUAL_RICH_MIMES)
        ):
            raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
        if (
            sniffed == "text/plain"
            and hinted_mime
            and (hinted_mime.startswith("text/") or hinted_mime in TEXTUAL_RICH_MIMES)
        ):
            effective = hinted_mime
        else:
            effective = sniffed
        warnings: list[str] = []
        if declared_mime and declared_mime != effective:
            warnings.append("declared_mime_mismatch")
        return effective, warnings

    @staticmethod
    def _declared_hash_matches(data: bytes, declared_hash: str) -> bool:
        normalized = declared_hash.casefold()
        if re.fullmatch(r"[0-9a-f]{32}", normalized):
            return hashlib.md5(data).hexdigest() == normalized
        if re.fullmatch(r"[0-9a-f]{64}", normalized):
            return hashlib.sha256(data).hexdigest() == normalized
        raise SightglassError(
            ErrorCode.RESOURCE_BLOCKED,
            details={"reason": "declared_hash_invalid"},
        )

    def _stage(
        self,
        staged: list[_StagedBinding],
        resource_id: str,
        variant: str,
        data: bytes,
        *,
        mime_type: str,
        origin: str,
    ) -> CachedObject:
        """Publish bytes into the private object store and queue their admission.

        The object store write is a local filesystem operation; the matching
        ``resource_objects`` / ``resource_bindings`` rows are persisted later by
        ``_persist_staged`` inside the short final admission.
        """

        cached, path = self.cache.put(data, mime_type=mime_type, origin=origin)
        try:
            metadata = Path(path).lstat()
        except FileNotFoundError:
            # Cleanup may have reaped a reused old orphan immediately after ``put``.
            # Admission will recreate and reverify it under the writer fence.
            metadata = None
        staged.append(
            _StagedBinding(
                resource_id=resource_id,
                variant=variant,
                object_digest=cached.digest,
                local_path_internal=path,
                mime_type=mime_type,
                byte_size=len(data),
                origin=origin,
                data=data,
                device=int(metadata.st_dev) if metadata is not None else None,
                inode=int(metadata.st_ino) if metadata is not None else None,
            )
        )
        return cached

    @staticmethod
    def _staged_file_identity_matches(item: _StagedBinding) -> bool:
        if item.device is None or item.inode is None:
            return False
        try:
            metadata = Path(item.local_path_internal).lstat()
        except FileNotFoundError:
            return False
        return (
            stat.S_ISREG(metadata.st_mode)
            and metadata.st_nlink == 1
            and stat.S_IMODE(metadata.st_mode) & 0o077 == 0
            and metadata.st_size == item.byte_size
            and (int(metadata.st_dev), int(metadata.st_ino)) == (item.device, item.inode)
        )

    def _persist_staged(self, staged: list[_StagedBinding]) -> None:
        if not staged:
            return
        now = utc_now().isoformat(timespec="microseconds")
        for item in staged:
            # The initial ``put`` already verified the digest and content. Under the
            # admission writer fence, the same private inode can be admitted without a
            # second full-file read. If cleanup reaped or replaced it after staging, use
            # the retained immutable bytes to recreate and fully verify it before the
            # row/binding commit.
            if not self._staged_file_identity_matches(item):
                cached, path = self.cache.put(
                    item.data,
                    mime_type=item.mime_type,
                    origin=item.origin,
                    maintenance=self.repository.database.maintenance_write_active,
                )
                if (
                    cached.digest != item.object_digest
                    or path != item.local_path_internal
                    or len(item.data) != item.byte_size
                ):
                    raise SightglassError(ErrorCode.RESOURCE_BLOCKED)
            self.repository.upsert_resource_object(
                object_digest=item.object_digest,
                local_path_internal=item.local_path_internal,
                mime_type=item.mime_type,
                byte_size=item.byte_size,
                origin=item.origin,
                observed_at=now,
            )
            self.repository.bind_resource_object(
                resource_id=item.resource_id,
                object_digest=item.object_digest,
                variant=item.variant,
                created_at=now,
            )
            if item.variant in _SOURCE_VARIANTS and self._origin_kind() == "macos-wechat":
                row = self._resource_row(item.resource_id)
                if row["kind"] == "image":
                    self._persist_derivations(
                        [
                            _StagedDerivation(
                                item.resource_id,
                                item.object_digest,
                                "source:" + item.variant,
                                "native-image-decoder",
                                self.provider.descriptor.implementation,
                                self._source_provenance_parameters(row),
                                item.object_digest,
                            )
                        ]
                    )

    def _ensure_source_payload(
        self,
        row: Any,
        snapshot: SourceSnapshot | None,
        staged: list[_StagedBinding],
        *,
        local_only: bool = False,
        acquired: tuple[bytes, SourceResourceVariant] | None = None,
    ) -> tuple[CachedObject, list[str], SourceResourceVariant]:
        """Resolve the source bytes for ``row`` and the source variant they came from.

        A source original is preferred. When the source only kept a derived preview
        entry, the payload resolves as ``thumbnail``; callers must not present those
        bytes as a source original.

        With ``local_only`` the private CAS bindings are the only permitted source: a
        cache hit never opens a provider read, and a row that is not fully cached fails
        closed instead of falling back to the source.

        ``acquired`` carries immutable source bytes the caller already read through a
        dependency-scoped source lease. Those bytes are used verbatim when no usable
        binding exists, so a cold read acquires the source exactly once and processes
        it outside the source session.
        """

        resource_id = str(row["resource_id"])
        declares_original = str(row["availability"]) in {"local_available", "archive_available"}
        for variant in _SOURCE_VARIANTS:
            if variant == "thumbnail" and declares_original:
                # The source now declares a local original, so a cached derived preview
                # must not answer for it. Fall through to the source read below.
                break
            binding = self.repository.resource_binding(resource_id, variant)
            if binding is None or not self._source_binding_current(row, variant):
                continue
            cached = self.cache.read_binding(binding)
            effective_mime, warnings = self._effective_mime(
                cached.data,
                str(row["mime_type"]) if row["mime_type"] else None,
                str(row["original_name"]) if row["original_name"] else None,
            )
            if effective_mime != cached.mime_type:
                raise SightglassError(ErrorCode.RESOURCE_BLOCKED)
            return cached, warnings, variant
        source_resource_key = (
            str(row["source_resource_key"]) if row["source_resource_key"] else None
        )
        if local_only:
            # The caller classified this read as a cache hit, so no binding means the
            # cache was reaped between classification and resolution. Never fall back
            # to the provider here.
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
        if source_resource_key is None:
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
        if acquired is None:
            assert snapshot is not None
            payload = self.provider.read_resource(
                source_resource_key,
                max_bytes=MAX_SOURCE_ENVELOPE_BYTES,
                snapshot=snapshot,
            )
            data = payload.data
            variant = payload.variant
        else:
            data, variant = acquired
        if len(data) > MAX_SOURCE_BYTES:
            raise SightglassError(
                ErrorCode.RESOURCE_TOO_LARGE,
                details={"max_bytes": MAX_SOURCE_BYTES},
            )
        effective_mime, warnings = self._effective_mime(
            data,
            str(row["mime_type"]) if row["mime_type"] else None,
            str(row["original_name"]) if row["original_name"] else None,
        )
        if variant == "original":
            # Declared size and digest describe the source original; a derived preview
            # entry is not an integrity claim about them.
            declared_size = int(row["declared_size"]) if row["declared_size"] is not None else None
            if declared_size is not None and declared_size != len(data):
                warnings.append("declared_size_mismatch")
            declared_hash = str(row["declared_hash"]) if row["declared_hash"] else None
            if declared_hash and not self._declared_hash_matches(data, declared_hash):
                raise SightglassError(
                    ErrorCode.RESOURCE_BLOCKED,
                    details={"reason": "declared_hash_mismatch"},
                )
        cached = self._stage(
            staged,
            resource_id,
            variant,
            data,
            mime_type=effective_mime,
            origin="private_cache",
        )
        return cached, warnings, variant

    @staticmethod
    def _release_original(
        data: bytes, effective_mime: str
    ) -> tuple[_OriginalKind, str, str | None]:
        """Transport kind, block MIME, and text for one provider-verified original.

        ``mode="original"`` returns the exact bytes the provider read and
        digest-verified, so the release form follows what those bytes are rather than
        what the source declared. Validation stays only where the released block
        asserts an interpreted type: an ``image/*`` or ``application/pdf`` block must
        still decode as that media, and a text block must still be decodable text.
        Content the accepted contract withholds (active Office content, encrypted or
        unsafe archive members, bombs, integrity mismatch) is rejected before or
        during this call by the provider read, ``_effective_mime``, or the strict
        validator kept for each parsed surface; bytes that merely resemble a
        container are inert data and still cross as an opaque blob.
        """

        if effective_mime == WXGF_MIME:
            inspect_image(data)
            return "blob", effective_mime, None
        if effective_mime.startswith("image/"):
            inspect_image(data)
            return "image", effective_mime, None
        if effective_mime.startswith("audio/"):
            return "audio", effective_mime, None
        if effective_mime == "application/pdf":
            inspect_pdf(data)
            return "blob", effective_mime, None
        if effective_mime in TEXTUAL_RICH_MIMES:
            try:
                inspect_rich_metadata(data, effective_mime)
            except SightglassError as exc:
                if exc.code is not ErrorCode.RESOURCE_DECODE_FAILED:
                    raise
                # A declared or name-derived structured type the bytes do not satisfy
                # is reported as the sniffed text. Metadata/text/search keep their own
                # strict structured validators and still fail closed on these bytes.
                return "text", "text/plain", decode_text(data)
            return "text", effective_mime, decode_text(data)
        if effective_mime in OFFICE_MIMES or effective_mime == ZIP_MIME:
            try:
                inspect_rich_metadata(data, effective_mime)
            except SightglassError as exc:
                if exc.code is not ErrorCode.RESOURCE_DECODE_FAILED:
                    raise
            return "blob", effective_mime, None
        if effective_mime.startswith("text/"):
            return "text", effective_mime, decode_text(data)
        # Inert bytes the runtime has no parsed surface for, including legacy Office
        # and archive containers and opaque binary, cross as an opaque blob.
        return "blob", effective_mime, None

    @staticmethod
    def _resolver_digest(row: Any) -> str:
        value = str(row["resolver_json"]) if row["resolver_json"] else ""
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _preview_provenance(
        self,
        row: Any,
        original: CachedObject,
        *,
        variant: str,
        page: int | None,
        max_bytes: int,
        source_variant: SourceResourceVariant,
    ) -> _PreviewProvenance:
        """The exact recipe identity a generated preview must have been produced by.

        Every field participates in the provenance key: the input object digest, the
        source variant the bytes came from (so a thumbnail-to-original upgrade is a
        different recipe), the processor recipe and its version, the bounded read
        parameters, and the applicable resolver revision. A cache hit proves nothing
        about provenance unless all of these still match.
        """

        parameters: dict[str, Any]
        if variant == "preview" and original.mime_type.startswith(("image/", WXGF_MIME)):
            processor_name = _IMAGE_PREVIEW_PROCESSOR
            processor_version = _IMAGE_PREVIEW_PROCESSOR_VERSION
            parameters = {"max_bytes": int(max_bytes), "scale": _PREVIEW_SCALE_BOUND}
        elif original.mime_type == "application/pdf":
            processor_name = _PDF_PAGE_PROCESSOR
            processor_version = _PDF_PAGE_PROCESSOR_VERSION
            parameters = {"page": int(page or 1), "max_bytes": int(max_bytes)}
        elif original.mime_type.startswith("video/"):
            processor_name = _VIDEO_PREVIEW_PROCESSOR
            processor_version = _VIDEO_PREVIEW_PROCESSOR_VERSION
            parameters = {"max_bytes": int(max_bytes), "scale": _PREVIEW_SCALE_BOUND}
        else:
            # Unsupported input never reaches a generation step; keep a stable recipe so
            # the provenance key is still well defined if it is ever consulted.
            processor_name = _IMAGE_PREVIEW_PROCESSOR
            processor_version = _IMAGE_PREVIEW_PROCESSOR_VERSION
            parameters = {"max_bytes": int(max_bytes)}
        parameters["source_variant"] = str(source_variant)
        parameters["resolver"] = self._resolver_digest(row)
        return _PreviewProvenance(
            source_digest=original.digest,
            source_variant=source_variant,
            processor_name=processor_name,
            processor_version=processor_version,
            parameters_json=json.dumps(parameters, sort_keys=True, separators=(",", ":")),
        )

    def _derivation_matches(
        self,
        *,
        resource_id: str,
        source_digest: str,
        variant: str,
        processor_name: str,
        processor_version: str,
        parameters_json: str,
        derived_digest: str,
    ) -> bool:
        """Whether a recorded derivation proves exactly this provenance."""

        with self.repository.database.connection() as connection:
            row = connection.execute(
                """
                SELECT 1 FROM resource_derivations
                WHERE resource_id = ? AND source_digest = ? AND variant = ?
                  AND processor_name = ? AND processor_version = ?
                  AND parameters_json = ? AND derived_digest = ?
                LIMIT 1
                """,
                (
                    resource_id,
                    source_digest,
                    variant,
                    processor_name,
                    processor_version,
                    parameters_json,
                    derived_digest,
                ),
            ).fetchone()
        return row is not None

    def _persist_derivations(self, derivations: list[_StagedDerivation]) -> None:
        """Record derivation provenance inside the caller's open admission transaction.

        The derived object row must already exist (``_persist_staged`` runs first), so
        the ``derived_digest`` foreign key is satisfied. Insert is idempotent per the
        unique provenance index.
        """

        if not derivations:
            return
        now = utc_now().isoformat(timespec="microseconds")
        with self.repository.database.connection() as connection:
            for item in derivations:
                identity = "|".join(
                    (
                        item.resource_id,
                        item.source_digest,
                        item.variant,
                        item.processor_name,
                        item.processor_version,
                        item.parameters_json,
                        item.derived_digest,
                    )
                )
                derivation_id = (
                    "wxresder_" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]
                )
                connection.execute(
                    """
                    INSERT INTO resource_derivations(
                        derivation_id, resource_id, source_digest, variant,
                        processor_name, processor_version, parameters_json,
                        derived_digest, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        derivation_id,
                        item.resource_id,
                        item.source_digest,
                        item.variant,
                        item.processor_name,
                        item.processor_version,
                        item.parameters_json,
                        item.derived_digest,
                        now,
                    ),
                )

    def _cached_or_store_preview(
        self,
        row: Any,
        original: CachedObject,
        *,
        variant: str,
        page: int | None,
        max_bytes: int,
        staged: list[_StagedBinding],
        source_variant: SourceResourceVariant,
        derivations: list[_StagedDerivation],
    ) -> tuple[CachedObject, dict[str, Any]]:
        resource_id = str(row["resource_id"])
        binding_variant = _GENERATED_IMAGE_PREVIEW_VARIANT if variant == "preview" else variant
        provenance = self._preview_provenance(
            row,
            original,
            variant=variant,
            page=page,
            max_bytes=max_bytes,
            source_variant=source_variant,
        )
        binding = self.repository.resource_binding(resource_id, binding_variant)
        if binding is not None:
            cached = self.cache.read_binding(binding)
            if cached.mime_type != "image/png":
                raise SightglassError(ErrorCode.RESOURCE_BLOCKED)
            # Serve the cached preview only when its recorded provenance still matches
            # this input digest, source variant, and recipe. A binding produced from a
            # different source original (for example a pre-upgrade thumbnail) or an
            # older recipe/parameters is not a proof of this read's provenance, so it is
            # invalidated and regenerated below.
            if self._derivation_matches(
                resource_id=resource_id,
                source_digest=provenance.source_digest,
                variant=binding_variant,
                processor_name=provenance.processor_name,
                processor_version=provenance.processor_version,
                parameters_json=provenance.parameters_json,
                derived_digest=str(binding["object_digest"]),
            ):
                return cached, {}
        if original.mime_type.startswith("image/"):
            data, info = image_preview(original.data, max_bytes=max_bytes)
            extra = {
                "image": {
                    "width": info.width,
                    "height": info.height,
                    "animated_source": info.animated,
                }
            }
        elif original.mime_type == "application/pdf":
            assert page is not None
            data, info = render_pdf_page(original.data, page=page, max_bytes=max_bytes)
            extra = {
                "document": {
                    "page_count": info.page_count,
                    "page_size": info.page_size,
                    "version": info.version,
                }
            }
        elif original.mime_type.startswith("video/"):
            data, info = render_video_preview(original.data, max_bytes=max_bytes)
            extra = {
                "media_info": {
                    "format": info.format,
                    "duration_seconds": info.duration_seconds,
                    "width": info.width,
                    "height": info.height,
                    "has_audio": info.has_audio,
                    "has_video": info.has_video,
                }
            }
        else:
            raise SightglassError(ErrorCode.RESOURCE_UNSUPPORTED)
        cached = self._stage(
            staged,
            resource_id,
            binding_variant,
            data,
            mime_type="image/png",
            origin="generated_preview",
        )
        derivations.append(
            _StagedDerivation(
                resource_id=resource_id,
                source_digest=provenance.source_digest,
                variant=binding_variant,
                processor_name=provenance.processor_name,
                processor_version=provenance.processor_version,
                parameters_json=provenance.parameters_json,
                derived_digest=cached.digest,
            )
        )
        return cached, extra

    def _pdf_text(
        self, row: Any, original: CachedObject, staged: list[_StagedBinding]
    ) -> tuple[str, Any]:
        resource_id = str(row["resource_id"])
        info = inspect_pdf(original.data)
        binding = self.repository.resource_binding(resource_id, "extracted_text")
        if binding is not None:
            cached = self.cache.read_binding(binding)
            return decode_text(cached.data), info
        text, info = extract_pdf_text(original.data)
        self._stage(
            staged,
            resource_id,
            "extracted_text",
            text.encode(),
            mime_type="text/plain",
            origin="private_cache",
        )
        return text, info

    def _bounded_lines(
        self,
        text: str,
        *,
        start_line: int | None,
        end_line: int | None,
    ) -> tuple[str, int, int, bool]:
        lines = text.splitlines()
        start = 1 if start_line is None else int(start_line)
        end = min(len(lines), start + 199) if end_line is None else int(end_line)
        if start < 1 or end < start or end - start + 1 > 500:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        selected: list[str] = []
        returned_end = start - 1
        for number in range(start, min(end, len(lines)) + 1):
            proposal = "\n".join([*selected, lines[number - 1]])
            if len(proposal) > self.reader.policy.max_text_chars_per_call:
                break
            selected.append(lines[number - 1])
            returned_end = number
        if start <= len(lines) and not selected:
            raise SightglassError(ErrorCode.OUTPUT_BUDGET_EXCEEDED)
        return "\n".join(selected), start, returned_end, returned_end < min(end, len(lines))

    def _ensure_structured_budget(self, descriptor: dict[str, Any]) -> None:
        rendered = json.dumps(descriptor, ensure_ascii=False, separators=(",", ":"))
        if len(rendered) > self.reader.policy.max_text_chars_per_call:
            raise SightglassError(ErrorCode.OUTPUT_BUDGET_EXCEEDED)

    _MODE_SELECTORS: dict[str, frozenset[str]] = {
        "metadata": frozenset(),
        "original": frozenset(),
        # A document preview keeps only an optional page selector.
        "preview": frozenset({"page"}),
        "page": frozenset({"page"}),
        "members": frozenset(),
        "table": frozenset({"sheet", "cell_range"}),
        "slide": frozenset({"page"}),
        # Text combines a line window, an optional archive member, and an optional
        # PDF page; any other selector is meaningless for text extraction.
        "text": frozenset({"page", "start_line", "end_line", "member"}),
    }
    _MODE_REQUIRED_SELECTORS: dict[str, frozenset[str]] = {
        "page": frozenset({"page"}),
        "slide": frozenset({"page"}),
    }
    _MODE_INCOMPATIBLE_SELECTORS: dict[str, frozenset[frozenset[str]]] = {
        # A page text read is a full-page window; line bounds and archive members are
        # separate text surfaces and never combine with a page selector.
        "text": frozenset(
            {
                frozenset({"page", "start_line"}),
                frozenset({"page", "end_line"}),
                frozenset({"page", "member"}),
            }
        ),
    }

    @classmethod
    def _validate_selectors(
        cls,
        mode: str,
        *,
        page: int | None,
        start_line: int | None,
        end_line: int | None,
        member: str | None,
        sheet: str | None,
        cell_range: str | None,
    ) -> None:
        provided = {
            name
            for name, value in (
                ("page", page),
                ("start_line", start_line),
                ("end_line", end_line),
                ("member", member),
                ("sheet", sheet),
                ("cell_range", cell_range),
            )
            if value is not None
        }
        allowed = cls._MODE_SELECTORS[mode]
        if provided - allowed:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        required = cls._MODE_REQUIRED_SELECTORS.get(mode, frozenset())
        if not required.issubset(provided):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if any(pair.issubset(provided) for pair in cls._MODE_INCOMPATIBLE_SELECTORS.get(mode, ())):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if page is not None and page < 1:
            raise SightglassError(ErrorCode.QUERY_INVALID)

    def validate_text_read(
        self,
        *,
        page: int | None,
        start_line: int | None,
        end_line: int | None,
        member: str | None,
        sheet: str | None,
        cell_range: str | None,
        max_bytes: int,
    ) -> int:
        """Validate one text-mode argument set and return its bounded byte budget.

        The derived voice transcript read shares this with the ordinary text reader,
        so a selector or byte budget that a text resource rejects can never be
        silently ignored by the transcript branch.
        """

        self._validate_selectors(
            "text",
            page=page,
            start_line=start_line,
            end_line=end_line,
            member=member,
            sheet=sheet,
            cell_range=cell_range,
        )
        return self.reader.bound_binary_bytes(max_bytes)

    def _resolved_source(self, row: Any, captured_revision: str) -> _ResolvedSource:
        """Resolve and admit immutable source bytes once per resource revision."""

        resource_id = str(row["resource_id"])
        prior = tuple(
            (variant, str(binding["object_digest"]))
            for variant in _SOURCE_VARIANTS
            if (binding := self.repository.resource_binding(resource_id, variant)) is not None
        )
        if self._cache_ready(row):
            with self._lane(WorkClass.LOCAL_READ, wait=True):
                original, warnings, source_variant = self._ensure_source_payload(
                    row, None, [], local_only=True
                )
            self._note_runtime("cache_hit")
            return _ResolvedSource(
                original,
                tuple(warnings),
                source_variant,
                self._source_receipt(None, local=True),
                resource_revision=captured_revision,
                prior_source_bindings=prior,
                staged=(),
            )
        if local_read_only_requested():
            raise SightglassError(
                ErrorCode.RESOURCE_UNAVAILABLE,
                retryable=True,
                details={"phase": "resolve", "reason": "local_cache_changed"},
            )
        source_resource_key = (
            str(row["source_resource_key"]) if row["source_resource_key"] else None
        )
        if source_resource_key is None:
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)

        def acquire() -> _ResolvedSource:
            # Re-check the resolver *before* the source read: the coalescing key was
            # built from ``captured_revision`` but the source bytes we are about to
            # acquire must belong to that same revision. A resolver that advanced while
            # we waited on the stage makes these bytes un-admittable, so fail closed and
            # let the caller retry against the new revision.
            current = self.repository.resource_context(resource_id)
            if current is None or self._resource_revision(current) != captured_revision:
                raise SightglassError(
                    ErrorCode.SOURCE_GENERATION_CHANGED,
                    retryable=True,
                    details={"phase": "acquire", "reason": "resource_resolver_changed"},
                )
            row = current
            source_resource_key = (
                str(row["source_resource_key"]) if row["source_resource_key"] else None
            )
            if source_resource_key is None:
                raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
            if self.remote_acquire is not None:
                with self._lane(WorkClass.SOURCE_READ, wait=True):
                    with self._foreground_source():
                        return self.remote_acquire(row, captured_revision)
            with self._lane(WorkClass.SOURCE_READ, wait=True):
                with self._foreground_source():
                    with self._resource_scope(source_resource_key) as snapshot:
                        acquired = self.provider.read_resource(
                            source_resource_key,
                            max_bytes=MAX_SOURCE_ENVELOPE_BYTES,
                            snapshot=snapshot,
                        )
            staged: list[_StagedBinding] = []
            original, warnings, source_variant = self._ensure_source_payload(
                row,
                snapshot,
                staged,
                acquired=(acquired.data, acquired.variant),
            )
            self._note_runtime("cache_miss")
            self._note_runtime("source_acquisition")
            return _ResolvedSource(
                original,
                tuple(warnings),
                source_variant,
                self._source_receipt(snapshot),
                resource_revision=captured_revision,
                prior_source_bindings=prior,
                staged=tuple(staged),
            )

        return self.coalescer.run(
            self.coalescer.acquisition_key(resource_id, captured_revision), acquire
        )

    @staticmethod
    def _processing_payload(row: Any, job: dict[str, Any]) -> ResourceReadPayload:
        return ResourceReadPayload(
            {
                "schema": "sightglass.resource-processing.v1",
                "ok": True,
                "state": "processing",
                "resource_id": str(row["resource_id"]),
                "reading_token": str(job["job_id"]),
                "retry_after_ms": 500,
                "attempt": int(job["attempt"]),
            }
        )

    @staticmethod
    def _job_failure(job: dict[str, Any]) -> SightglassError:
        state = str(job["state"])
        code = str(job["error_code"] or ErrorCode.RESOURCE_DECODE_FAILED.value)
        if state == "blocked":
            error = ErrorCode.RESOURCE_BLOCKED
        else:
            try:
                error = ErrorCode(code)
            except ValueError:
                error = ErrorCode.RESOURCE_DECODE_FAILED
        return SightglassError(
            error,
            retryable=False,
            details={"phase": "derivation", "reason": "resource_job_failed"},
        )

    def _defer_derivation(
        self,
        row: Any,
        request: DerivationRequest,
        resolved: _ResolvedSource,
    ) -> ResourceReadPayload:
        if self.jobs is None:
            raise SightglassError(ErrorCode.SERVICE_BUSY, retryable=True)
        if resolved.staged:
            with self.repository.database.transaction():
                self._verify_resolved_revision(request.resource_id, resolved)
                self._persist_staged(list(resolved.staged))
        job = self.jobs.enqueue(request)
        self._note_runtime("deferred")
        if self._wake_resource_worker is not None:
            self.repository.database.wake_after_commit(self._wake_resource_worker)
        return self._processing_payload(row, job)

    def _job_gate(
        self,
        row: Any,
        request: DerivationRequest,
        *,
        reading_token: str | None,
        allow_async: bool,
    ) -> ResourceReadPayload | None:
        jobs = self.jobs
        if jobs is None or not allow_async:
            return None
        if reading_token is not None:
            job = jobs.by_id(reading_token)
            if job is None:
                raise SightglassError(ErrorCode.CURSOR_INVALID)
            recorded = jobs.request_for(job)
            if recorded != request:
                raise SightglassError(ErrorCode.CURSOR_INVALID)
            state = str(job["state"])
            if state in {"pending", "leased", "running"}:
                return self._processing_payload(row, job)
            if state in {"failed", "blocked", "cancelled"}:
                raise self._job_failure(job)
            if state != "ready":
                raise SightglassError(ErrorCode.INTERNAL_ERROR)
            return None
        active = jobs.active(request)
        if active is not None:
            return self._processing_payload(row, active)
        return None

    def _derivation_ready(
        self,
        request: DerivationRequest,
        original: CachedObject,
        source_variant: SourceResourceVariant,
    ) -> bool:
        if request.mode == "preview" and original.mime_type.startswith(
            ("image/", "video/", WXGF_MIME)
        ):
            return self._preview_binding_matches(request, original, source_variant)
        if original.mime_type != "application/pdf":
            return False
        if request.mode in {"preview", "page"}:
            return self._preview_binding_matches(request, original, source_variant)
        if request.mode == "text":
            return self._binding_available(request.resource_id, "extracted_text")
        return False

    def _preview_binding_matches(
        self,
        request: DerivationRequest,
        original: CachedObject,
        source_variant: SourceResourceVariant,
    ) -> bool:
        """Whether the cached preview's provenance still matches this exact input.

        A binding produced from a different source digest, source variant, recipe, or
        resolver revision is not ready, so the caller regenerates it instead of
        trusting a stale artifact.
        """

        binding_variant = (
            f"page:{request.page or 1}"
            if original.mime_type == "application/pdf"
            else _GENERATED_IMAGE_PREVIEW_VARIANT
        )
        binding = self.repository.resource_binding(request.resource_id, binding_variant)
        if binding is None:
            return False
        row = self.repository.resource_context(request.resource_id)
        if row is None:
            return False
        provenance = self._preview_provenance(
            row,
            original,
            variant="page" if original.mime_type == "application/pdf" else "preview",
            page=request.page,
            max_bytes=request.max_bytes,
            source_variant=source_variant,
        )
        return self._derivation_matches(
            resource_id=request.resource_id,
            source_digest=provenance.source_digest,
            variant=binding_variant,
            processor_name=provenance.processor_name,
            processor_version=provenance.processor_version,
            parameters_json=provenance.parameters_json,
            derived_digest=str(binding["object_digest"]),
        )

    def _long_derivation(
        self,
        request: DerivationRequest,
        original: CachedObject,
        source_variant: SourceResourceVariant,
    ) -> bool:
        return bool(
            original.mime_type == "application/pdf"
            and request.mode in {"preview", "page", "text"}
            and len(original.data) >= _ASYNC_PDF_BYTES
            and not self._derivation_ready(request, original, source_variant)
        )

    def read_resource(
        self,
        *,
        resource_id: str,
        mode: str,
        page: int | None,
        start_line: int | None,
        end_line: int | None,
        max_bytes: int,
        member: str | None = None,
        sheet: str | None = None,
        cell_range: str | None = None,
        reading_token: str | None = None,
        allow_async: bool = True,
        publish_guard: Callable[[], None] | None = None,
    ) -> ResourceReadPayload:
        """Read one resource and retain only a bounded content-free failure class."""

        try:
            return self._read_resource(
                resource_id=resource_id,
                mode=mode,
                page=page,
                start_line=start_line,
                end_line=end_line,
                max_bytes=max_bytes,
                member=member,
                sheet=sheet,
                cell_range=cell_range,
                reading_token=reading_token,
                allow_async=allow_async,
                publish_guard=publish_guard,
            )
        except SightglassError as exc:
            self._record_failure(resource_id, exc)
            raise

    def _read_resource(
        self,
        *,
        resource_id: str,
        mode: str,
        page: int | None,
        start_line: int | None,
        end_line: int | None,
        max_bytes: int,
        member: str | None = None,
        sheet: str | None = None,
        cell_range: str | None = None,
        reading_token: str | None = None,
        allow_async: bool = True,
        publish_guard: Callable[[], None] | None = None,
    ) -> ResourceReadPayload:
        if mode not in self._MODE_SELECTORS:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        self._validate_selectors(
            mode,
            page=page,
            start_line=start_line,
            end_line=end_line,
            member=member,
            sheet=sheet,
            cell_range=cell_range,
        )
        capability = (
            "original" if mode == "original" else "metadata" if mode == "metadata" else "preview"
        )
        self.reader.require_resource(capability)
        bounded_bytes = self.reader.bound_binary_bytes(max_bytes)
        row = self._resource_row(resource_id)
        source_revision = self._resource_revision(row)
        resolved = self._resolved_source(row, source_revision)

        row = self._resource_row(resource_id)
        derivation_revision = self._resource_revision(row)
        request = DerivationRequest(
            resource_id=resource_id,
            resource_revision=derivation_revision,
            mode=mode,
            page=page,
            start_line=start_line,
            end_line=end_line,
            member=member,
            sheet=sheet,
            cell_range=cell_range,
            max_bytes=bounded_bytes,
        )
        gated = self._job_gate(
            row,
            request,
            reading_token=reading_token,
            allow_async=allow_async,
        )
        if gated is not None:
            return gated

        def derive() -> ResourceReadPayload:
            cached_derivation = self._derivation_ready(
                request, resolved.original, resolved.source_variant
            )
            if allow_async and self._long_derivation(
                request, resolved.original, resolved.source_variant
            ):
                return self._defer_derivation(row, request, resolved)
            lane_context = (
                self._lane(
                    (WorkClass.LOCAL_READ if cached_derivation else WorkClass.RESOURCE_DERIVATION),
                    wait=cached_derivation or not allow_async,
                )
                if self.runtime_lanes is not None
                else nullcontext(True)
            )
            with lane_context as acquired_lane:
                if not acquired_lane:
                    return self._defer_derivation(row, request, resolved)
                staged: list[_StagedBinding] = list(resolved.staged)
                derivations: list[_StagedDerivation] = []
                payload = self._resolve_payload(
                    row,
                    None,
                    mode=mode,
                    page=page,
                    start_line=start_line,
                    end_line=end_line,
                    bounded_bytes=bounded_bytes,
                    staged=staged,
                    derivations=derivations,
                    member=member,
                    sheet=sheet,
                    cell_range=cell_range,
                    resolved=resolved,
                )
                with self.repository.database.transaction():
                    if publish_guard is not None:
                        publish_guard()
                    self._verify_resolved_revision(resource_id, resolved)
                    self._persist_staged(staged)
                    self._persist_derivations(derivations)
                self._note_runtime("derivation")
                return payload

        return self.coalescer.run(
            self.coalescer.derivation_key(resolved.original.digest, request.recipe_key()),
            derive,
        )

    def _resolve_payload(
        self,
        row: Any,
        snapshot: SourceSnapshot | None,
        *,
        mode: str,
        page: int | None,
        start_line: int | None,
        end_line: int | None,
        bounded_bytes: int,
        staged: list[_StagedBinding],
        derivations: list[_StagedDerivation] | None = None,
        member: str | None = None,
        sheet: str | None = None,
        cell_range: str | None = None,
        local_only: bool = False,
        acquired: tuple[bytes, SourceResourceVariant] | None = None,
        resolved: _ResolvedSource | None = None,
    ) -> ResourceReadPayload:
        """Read one provider-verified resource outside any window.db writer lock."""

        if derivations is None:
            derivations = []
        descriptor = {
            "schema": "sightglass.resource-read.v1",
            "mode": mode,
            "resource": self._descriptor(row),
            "page": page,
            "member": member,
            "sheet": sheet,
            "cell_range": cell_range,
            "line_range": None,
            "returned": {"bytes": 0, "chars": 0, "truncated": False},
            "media": None,
            "resolution": {"path": "metadata", "variant": None},
            "derivation": {"kind": "source_metadata", "tool": None},
            "warnings": [],
            "source_receipt": (
                dict(resolved.source_receipt)
                if resolved is not None
                else self._source_receipt(snapshot, local=local_only)
            ),
        }
        if resolved is None:
            original, warnings, source_variant = self._ensure_source_payload(
                row, snapshot, staged, local_only=local_only, acquired=acquired
            )
        else:
            original = resolved.original
            warnings = list(resolved.warnings)
            source_variant = resolved.source_variant
        descriptor["warnings"] = warnings
        descriptor["sniffed_mime_type"] = original.mime_type
        resource_descriptor = descriptor["resource"]
        format_family = self._format_family(original.mime_type, str(row["kind"]))
        resource_descriptor["detected_mime"] = original.mime_type
        resource_descriptor["format_family"] = format_family
        resource_descriptor["available_views"] = self._available_views(
            original.mime_type,
            format_family,
            original_available=bool(resource_descriptor["original_available"]),
            preview_available=bool(
                resource_descriptor["preview_available"] or original.mime_type.startswith("video/")
            ),
        )
        if mode == "text" and member is not None and original.mime_type != ZIP_MIME:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if mode == "text" and page is not None and original.mime_type != "application/pdf":
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if mode == "metadata":
            if original.mime_type.startswith("image/"):
                info = inspect_image(original.data)
                descriptor["image"] = {
                    "width": info.width,
                    "height": info.height,
                    "format": info.format,
                    "animated": info.animated,
                }
            elif original.mime_type == "application/pdf":
                info = inspect_pdf(original.data)
                descriptor["document"] = {
                    "page_count": info.page_count,
                    "page_size": info.page_size,
                    "version": info.version,
                }
            elif is_rich_mime(original.mime_type):
                descriptor["structured"] = inspect_rich_metadata(
                    original.data,
                    original.mime_type,
                )
                self._ensure_structured_budget(descriptor)
            elif original.mime_type.startswith("text/"):
                decoded, encoding = decode_text_with_encoding(original.data)
                descriptor["text"] = {
                    "line_count": len(decoded.splitlines()),
                    "encoding": encoding,
                }
            elif original.mime_type == "audio/silk":
                # SILK is a verified source envelope, not a general media container.
                # Metadata must not route through ffmpeg: decoding/transcription stays
                # behind the separately bounded local voice pipeline.
                descriptor["audio"] = {
                    "format": "silk",
                    "mime_type": original.mime_type,
                }
            elif original.mime_type.startswith(("audio/", "video/")):
                info = inspect_media(original.data, original.mime_type)
                descriptor["media_info"] = {
                    "format": info.format,
                    "duration_seconds": info.duration_seconds,
                    "width": info.width,
                    "height": info.height,
                    "has_audio": info.has_audio,
                    "has_video": info.has_video,
                }
            else:
                descriptor["binary"] = {
                    "byte_size": len(original.data),
                    "mime_type": original.mime_type,
                }
            descriptor["resolution"] = {"path": "private_cache", "variant": source_variant}
            return ResourceReadPayload(descriptor)
        if mode == "original":
            if source_variant != "original":
                # A source-kept derived preview is never egress as the source original.
                raise SightglassError(
                    ErrorCode.RESOURCE_UNAVAILABLE,
                    details={"reason": "source_original_unavailable"},
                )
            if len(original.data) > bounded_bytes:
                raise SightglassError(
                    ErrorCode.RESOURCE_TOO_LARGE,
                    details={"max_bytes": bounded_bytes},
                )
            content_kind, released_mime, text = self._release_original(
                original.data, original.mime_type
            )
            descriptor["returned"]["bytes"] = len(original.data)
            descriptor["media"] = {
                "mime_type": released_mime,
                "content_block_type": content_kind,
            }
            descriptor["resolution"] = {"path": "private_cache", "variant": source_variant}
            descriptor["derivation"] = {"kind": "source_original", "tool": None}
            return ResourceReadPayload(
                descriptor,
                data=original.data if content_kind != "text" else None,
                mime_type=released_mime,
                content_kind=content_kind,
                text=text,
            )
        if mode == "members":
            if original.mime_type != ZIP_MIME:
                raise SightglassError(ErrorCode.RESOURCE_UNSUPPORTED)
            archive = list_archive_members(
                original.data,
                max_chars=self.reader.policy.max_text_chars_per_call,
                max_bytes=bounded_bytes,
            )
            descriptor["archive"] = archive
            rendered = json.dumps(archive, ensure_ascii=False, separators=(",", ":"))
            descriptor["returned"] = {
                "bytes": len(rendered.encode("utf-8")),
                "chars": len(rendered),
                "truncated": bool(archive["truncated"]),
            }
            descriptor["resolution"] = {"path": "private_cache", "variant": source_variant}
            descriptor["derivation"] = {"kind": "safe_archive_index", "tool": None}
            self._ensure_structured_budget(descriptor)
            return ResourceReadPayload(descriptor)
        if mode == "table":
            if original.mime_type not in CSV_MIMES | TSV_MIMES | {XLSX_MIME}:
                raise SightglassError(ErrorCode.RESOURCE_UNSUPPORTED)
            table = extract_table(
                original.data,
                original.mime_type,
                sheet=sheet,
                cell_range=cell_range,
                max_chars=self.reader.policy.max_text_chars_per_call,
                max_bytes=bounded_bytes,
            )
            descriptor["table"] = table
            descriptor["sheet"] = table["sheet"]
            descriptor["cell_range"] = table["cell_range"]
            rendered = json.dumps(table, ensure_ascii=False, separators=(",", ":"))
            descriptor["returned"] = {
                "bytes": len(rendered.encode("utf-8")),
                "chars": len(rendered),
                "truncated": bool(table["truncated"]),
            }
            descriptor["resolution"] = {"path": "private_cache", "variant": source_variant}
            descriptor["derivation"] = {
                "kind": (
                    "safe_xlsx_xml_table"
                    if original.mime_type == XLSX_MIME
                    else "validated_delimited_table"
                ),
                "tool": None,
            }
            self._ensure_structured_budget(descriptor)
            return ResourceReadPayload(descriptor)
        if mode == "slide":
            if original.mime_type != PPTX_MIME:
                raise SightglassError(ErrorCode.RESOURCE_UNSUPPORTED)
            assert page is not None
            slide = extract_slide(
                original.data,
                page=page,
                max_chars=self.reader.policy.max_text_chars_per_call,
                max_bytes=bounded_bytes,
            )
            descriptor["slide"] = slide
            rendered = json.dumps(slide, ensure_ascii=False, separators=(",", ":"))
            descriptor["returned"] = {
                "bytes": len(rendered.encode("utf-8")),
                "chars": len(rendered),
                "truncated": bool(slide["truncated"]),
            }
            descriptor["resolution"] = {"path": "private_cache", "variant": source_variant}
            descriptor["derivation"] = {"kind": "safe_pptx_xml_slide", "tool": None}
            self._ensure_structured_budget(descriptor)
            return ResourceReadPayload(descriptor)
        if mode in {"preview", "page"}:
            if mode == "page" and original.mime_type != "application/pdf":
                raise SightglassError(ErrorCode.RESOURCE_UNSUPPORTED)
            selected_page = page or 1
            if original.mime_type.startswith(("image/", "video/")) and page is not None:
                raise SightglassError(ErrorCode.QUERY_INVALID)
            if page is None and original.mime_type == "application/pdf":
                descriptor["warnings"].append("defaulted_to_page_1")
            variant = (
                f"page:{selected_page}" if original.mime_type == "application/pdf" else "preview"
            )
            preview, extra = self._cached_or_store_preview(
                row,
                original,
                variant=variant,
                page=selected_page,
                max_bytes=bounded_bytes,
                staged=staged,
                source_variant=source_variant,
                derivations=derivations,
            )
            if len(preview.data) > bounded_bytes:
                raise SightglassError(ErrorCode.RESOURCE_TOO_LARGE)
            descriptor.update(extra)
            descriptor["page"] = selected_page if original.mime_type == "application/pdf" else None
            descriptor["returned"]["bytes"] = len(preview.data)
            descriptor["media"] = {
                "mime_type": "image/png",
                "content_block_type": "image",
            }
            descriptor["resolution"] = {"path": "private_cache", "variant": variant}
            descriptor["derivation"] = {
                "kind": (
                    "rendered_pdf_page"
                    if original.mime_type == "application/pdf"
                    else "generated_video_preview"
                    if original.mime_type.startswith("video/")
                    else "generated_image_preview"
                ),
                "tool": (
                    "pdftoppm"
                    if original.mime_type == "application/pdf"
                    else "ffmpeg+sips"
                    if original.mime_type == WXGF_MIME
                    else "ffmpeg"
                    if original.mime_type.startswith("video/")
                    else "sips"
                ),
            }
            return ResourceReadPayload(
                descriptor,
                data=preview.data,
                mime_type="image/png",
                content_kind="image",
            )
        if mode == "text":
            if original.mime_type == "application/pdf":
                full_text, info = self._pdf_text(row, original, staged)
                selected_page = page or 1
                if not 1 <= selected_page <= info.page_count:
                    raise SightglassError(ErrorCode.QUERY_INVALID)
                pages = full_text.split("\f")
                text = pages[selected_page - 1].strip() if selected_page <= len(pages) else ""
                if page is None:
                    descriptor["warnings"].append("defaulted_to_page_1")
                descriptor["page"] = selected_page
                descriptor["document"] = {
                    "page_count": info.page_count,
                    "page_size": info.page_size,
                    "version": info.version,
                    "text_state": "available" if text else "empty",
                }
                if len(text) > self.reader.policy.max_text_chars_per_call:
                    raise SightglassError(ErrorCode.OUTPUT_BUDGET_EXCEEDED)
                descriptor["resolution"] = {
                    "path": "private_cache",
                    "variant": "extracted_text",
                }
                descriptor["derivation"] = {
                    "kind": "extracted_pdf_text",
                    "tool": "pdftotext",
                }
            elif is_rich_mime(original.mime_type):
                extraction_chars = (
                    MAX_EXTRACTED_TEXT_BYTES
                    if start_line is not None or end_line is not None
                    else self.reader.policy.max_text_chars_per_call
                )
                full_text, detail = extract_rich_text(
                    original.data,
                    original.mime_type,
                    max_chars=extraction_chars,
                    max_bytes=bounded_bytes if member is not None else None,
                    member=member,
                )
                if start_line is not None or end_line is not None:
                    text, start, end, line_truncated = self._bounded_lines(
                        full_text,
                        start_line=start_line,
                        end_line=end_line,
                    )
                    descriptor["line_range"] = {"start": start, "end": end}
                    descriptor["returned"]["truncated"] = bool(
                        detail.get("truncated") or line_truncated
                    )
                else:
                    text = full_text
                    descriptor["returned"]["truncated"] = bool(detail.get("truncated"))
                descriptor["resolution"] = {
                    "path": "private_cache",
                    "variant": source_variant,
                }
                descriptor["derivation"] = {
                    "kind": str(detail.get("kind", "safe_archive_member_text")),
                    "tool": None,
                }
                if member is not None:
                    descriptor["archive_member"] = {
                        "name": detail["member"],
                        "mime_type": detail["mime_type"],
                        "archive_depth": detail["archive_depth"],
                    }
            elif original.mime_type.startswith("text/"):
                if page is not None:
                    raise SightglassError(ErrorCode.QUERY_INVALID)
                decoded, encoding = decode_text_with_encoding(original.data)
                text, start, end, truncated = self._bounded_lines(
                    decoded,
                    start_line=start_line,
                    end_line=end_line,
                )
                descriptor["line_range"] = {"start": start, "end": end}
                descriptor["returned"]["truncated"] = truncated
                descriptor["resolution"] = {
                    "path": "private_cache",
                    "variant": source_variant,
                }
                descriptor["derivation"] = {
                    "kind": f"decoded_{encoding.replace('-', '_')}_text",
                    "tool": None,
                }
                descriptor["text_encoding"] = encoding
            else:
                raise SightglassError(ErrorCode.RESOURCE_UNSUPPORTED)
            encoded_size = len(text.encode("utf-8"))
            if encoded_size > bounded_bytes:
                raise SightglassError(
                    ErrorCode.RESOURCE_TOO_LARGE,
                    details={"max_bytes": bounded_bytes},
                )
            descriptor["returned"]["bytes"] = encoded_size
            descriptor["returned"]["chars"] = len(text)
            descriptor["media"] = {
                "mime_type": "text/plain",
                "content_block_type": "text",
            }
            return ResourceReadPayload(
                descriptor,
                mime_type="text/plain",
                content_kind="text",
                text=text,
            )
        raise SightglassError(ErrorCode.INTERNAL_ERROR)

    @staticmethod
    def _query_parts(query: str) -> tuple[str, ...]:
        if query.count('"') % 2:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        return tuple(
            (quoted or term).casefold()
            for quoted, term in re.findall(r'"([^"]+)"|(\S+)', query)
            if quoted or term
        )

    def search_resource_text(
        self,
        *,
        resource_id: str,
        query: str,
        limit: int,
    ) -> dict[str, Any]:
        self.reader.require_resource("preview")
        parts = self._query_parts(query)
        if not parts:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        bounded = self.reader.bound_limit(limit)
        row = self._resource_row(resource_id)
        captured_revision = self._resource_revision(row)
        staged: list[_StagedBinding] = []
        snapshot: SourceSnapshot | None
        if self._cache_ready(row):
            # Warm CAS fast path: resolve from the private store without any provider
            # session, still re-checking authorization and resolver state above.
            snapshot = None
            original, warnings, _source_variant = self._ensure_source_payload(
                row, None, staged, local_only=True
            )
        else:
            if local_read_only_requested():
                raise SightglassError(
                    ErrorCode.RESOURCE_UNAVAILABLE,
                    retryable=True,
                    details={"phase": "resolve", "reason": "local_cache_changed"},
                )
            source_resource_key = (
                str(row["source_resource_key"]) if row["source_resource_key"] else None
            )
            if source_resource_key is None:
                raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
            with self._resource_scope(source_resource_key) as snapshot:
                acquired = self.provider.read_resource(
                    source_resource_key,
                    max_bytes=MAX_SOURCE_ENVELOPE_BYTES,
                    snapshot=snapshot,
                )
            original, warnings, _source_variant = self._ensure_source_payload(
                row,
                snapshot,
                staged,
                acquired=(acquired.data, acquired.variant),
            )
        # Extraction runs outside any source session; the short admission below
        # re-authorizes and re-checks the resolver revision before binding.
        hits, truncated, derivation_kind = self._search_hits(
            row, original, parts=parts, bounded=bounded, staged=staged
        )
        with self.repository.database.transaction():
            current = self._resource_row(resource_id)
            if self._resource_revision(current) != captured_revision:
                raise SightglassError(
                    ErrorCode.SOURCE_GENERATION_CHANGED,
                    retryable=True,
                    details={
                        "phase": "admit",
                        "reason": "resource_resolver_changed",
                    },
                )
            if staged:
                self._persist_staged(staged)
        return {
            "schema": "sightglass.resource-search-results.v1",
            "resource": self._descriptor(row),
            "query": query,
            "hits": hits,
            "page": {"truncated": truncated},
            "derivation": {"kind": derivation_kind},
            "warnings": warnings,
            "source_receipt": self._source_receipt(snapshot),
        }

    def _search_hits(
        self,
        row: Any,
        original: CachedObject,
        *,
        parts: tuple[str, ...],
        bounded: int,
        staged: list[_StagedBinding],
    ) -> tuple[list[dict[str, Any]], bool, str]:
        """Return bounded match hits, truncation, and the derivation kind.

        Search candidate recall is never evidence: these hits are only the text
        lines of one already-authorized resource, and the caller still re-checks the
        resolver revision before any extracted-text binding is admitted.
        """

        hits: list[dict[str, Any]] = []
        stop = False
        derivation_kind = "decoded_utf8_text"
        if original.mime_type == "application/pdf":
            text, _info = self._pdf_text(row, original, staged)
            derivation_kind = "extracted_pdf_text"
            pages = text.split("\f")
            for page_number, value in enumerate(pages, start=1):
                for line_number, line in enumerate(value.splitlines(), start=1):
                    folded = line.casefold()
                    if all(part in folded for part in parts):
                        hits.append(
                            {"page": page_number, "line": line_number, "snippet": line[:500]}
                        )
                        if len(hits) > bounded:
                            stop = True
                            break
                if stop:
                    break
        elif is_rich_mime(original.mime_type) and original.mime_type != ZIP_MIME:
            text, rich_detail = extract_rich_text(
                original.data,
                original.mime_type,
                max_chars=MAX_EXTRACTED_TEXT_BYTES,
            )
            derivation_kind = str(rich_detail.get("kind", "decoded_utf8_text"))
            for line_number, line in enumerate(text.splitlines(), start=1):
                folded = line.casefold()
                if all(part in folded for part in parts):
                    hits.append({"page": None, "line": line_number, "snippet": line[:500]})
                    if len(hits) > bounded:
                        break
            stop = bool(rich_detail.get("truncated"))
        elif original.mime_type.startswith("text/"):
            for line_number, line in enumerate(decode_text(original.data).splitlines(), start=1):
                folded = line.casefold()
                if all(part in folded for part in parts):
                    hits.append({"page": None, "line": line_number, "snippet": line[:500]})
                    if len(hits) > bounded:
                        break
        else:
            raise SightglassError(ErrorCode.RESOURCE_UNSUPPORTED)
        truncated = len(hits) > bounded or stop
        hits = hits[:bounded]
        while (
            hits
            and len(json.dumps(hits, ensure_ascii=False, separators=(",", ":")))
            > self.reader.policy.max_text_chars_per_call
        ):
            hits.pop()
            truncated = True
        return hits, truncated, derivation_kind

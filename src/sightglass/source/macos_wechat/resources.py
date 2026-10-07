from __future__ import annotations

import base64
import binascii
import hashlib
import json
import mimetypes
import os
import re
import stat
import struct
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.resources import SourceResource, SourceResourceVariant
from sightglass.resources.sticker import (
    decrypt_wechat_sticker,
    decrypt_wechat_sticker_prefix,
    is_wechat_sticker_image,
)
from sightglass.resources.v2 import V2_MAGIC, decode_wechat_v2_image

from .image_index import (
    IMAGE_STEM,
    NativeImageEntry,
    NativeImageMapping,
    safe_component,
)
from .media_index import MEDIA_DATABASE, NativeVoiceEntry, NativeVoiceIndex

_MAX_METADATA_BYTES = 512 * 1024
_MAX_PROTOBUF_FIELDS = 4096
_HEX_DIGEST = re.compile(r"(?:[0-9a-fA-F]{32}|[0-9a-fA-F]{64})")
# Native image payloads are named by the WeChat-assigned file hash the mapping
# databases carry, never by the digest in the message XML. Within one such hash the
# native store keeps ``<hash>_h.dat`` as the high-resolution (full-size) entry,
# ``<hash>.dat`` as the mid-resolution entry and ``<hash>_t.dat`` as a JPEG
# thumbnail. ``_M``/``_t_M`` have no evidenced meaning in the native store, so they
# stay preview-class and are never presented as a source original.
_IMAGE_ORIGINAL_SUFFIXES = ("_h.dat", ".dat")
_IMAGE_PREVIEW_SUFFIXES = ("_t.dat", "_M.dat", "_t_M.dat")
_IMAGE_ATTACH_STORE = ("msg", "attach")
_IMAGE_ENTRY_DIRECTORY_SUFFIX = "Img"
# WeChat's bounded thumbnail cache beside ``db_storage`` still names a preview after
# the exact message position, so it stays usable when no mapping database is enrolled.
_IMAGE_THUMBNAIL_ROOT = "cache"
_IMAGE_THUMBNAIL_STORE = "Message"
_IMAGE_THUMBNAIL_DIRECTORY = "Thumb"
# Native video messages keep their downloaded payload unencrypted in the account's
# own `msg/video/<YYYY-MM>/` store, named by the media digest carried in the message
# metadata. The `_raw` variant is the pre-compression original when WeChat kept both.
_VIDEO_SUFFIXES = ("_raw.mp4", ".mp4")
_VIDEO_NAME = re.compile(r"[0-9a-f]{32}(?:_raw)?\.mp4")
_MAX_VIDEO_SCAN_ENTRIES = 8192
_STICKER_STORE = ("business", "emoticon", "Persist")
_STICKER_THUMBNAIL_STORE = ("business", "emoticon", "Thumb")
_LOCATOR_KINDS = frozenset({"file", "image", "video", "voice", "sticker"})


@dataclass(frozen=True)
class NativeResourceLocator:
    kind: str
    source_message_id: str
    conversation_source_id: str
    month: str
    original_name: str | None
    declared_size: int | None
    declared_hash: str | None
    attach_id: str | None
    # Image evidence. ``local_id``/``create_time`` address the bounded thumbnail cache;
    # ``image_directory``/``image_stem`` carry the mapped attach-store entry when the
    # optional mapping databases named one.
    local_id: int | None = None
    create_time: int | None = None
    image_directory: tuple[str, ...] | None = None
    image_stem: str | None = None
    # Voice evidence. These values stay inside the binding-authenticated opaque
    # locator and are never projected into reader-visible resource metadata.
    voice_database: str | None = None
    voice_chat_name_id: int | None = None
    server_id: int | None = None


@dataclass(frozen=True)
class _ImageSelection:
    """One locally selected image entry and the source variant it belongs to.

    ``locked`` is descriptor-only evidence: a read always re-checks the decoded
    payload, so it never probes the entry twice for the same call.
    """

    directory: tuple[str, ...]
    name: str
    variant: SourceResourceVariant
    locked: bool
    deferred: bool = False


def _valid_attach_directory(parts: tuple[str, ...]) -> bool:
    return (
        len(parts) == 5
        and tuple(parts[:2]) == _IMAGE_ATTACH_STORE
        and parts[4] == _IMAGE_ENTRY_DIRECTORY_SUFFIX
        and all(safe_component(part) is not None for part in parts)
    )


def _encoded(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _decoded(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _safe_basename(value: str | None) -> str | None:
    if value is None:
        return None
    selected = value.replace("\\", "/").rsplit("/", 1)[-1].replace("\x00", "").strip()
    if not selected or selected in {".", ".."} or len(selected.encode("utf-8")) > 255:
        return None
    return selected


def _safe_extension(value: str | None) -> str | None:
    selected = str(value or "").strip().lstrip(".").casefold()
    if not selected or not re.fullmatch(r"[a-z0-9][a-z0-9._+-]{0,31}", selected):
        return None
    return selected


def _normalized_digest(value: str | None) -> str | None:
    selected = str(value or "").strip()
    if not _HEX_DIGEST.fullmatch(selected):
        return None
    return selected.casefold()


def _bounded_nonnegative_int(value: str | None) -> int | None:
    try:
        selected = int(str(value or "").strip())
    except ValueError:
        return None
    return selected if 0 <= selected <= (1 << 63) - 1 else None


def _xml_root(content: str) -> ET.Element | None:
    encoded = content.encode("utf-8", errors="ignore")
    if (
        len(encoded) > _MAX_METADATA_BYTES
        or "<!DOCTYPE" in content.upper()
        or "<!ENTITY" in content.upper()
    ):
        return None
    starts = [position for marker in ("<msg", "<appmsg") if (position := content.find(marker)) >= 0]
    if not starts:
        return None
    try:
        return ET.fromstring(content[min(starts) :])
    except ET.ParseError:
        return None


def _first_text(root: ET.Element, paths: tuple[str, ...]) -> str | None:
    for path in paths:
        value = root.findtext(path)
        if value is not None and value.strip():
            return value.strip()
    return None


def _read_varint(data: bytes, offset: int) -> tuple[int, int] | None:
    value = 0
    shift = 0
    for index in range(offset, min(len(data), offset + 10)):
        byte = data[index]
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, index + 1
        shift += 7
    return None


def _protobuf_digests(data: bytes) -> tuple[str, ...]:
    if not data or len(data) > _MAX_METADATA_BYTES:
        return ()
    found: list[str] = []
    seen_nodes: set[tuple[int, int]] = set()
    fields = 0

    def walk(node: bytes, depth: int) -> None:
        nonlocal fields
        if depth > 4 or fields >= _MAX_PROTOBUF_FIELDS:
            return
        identity = (id(node), len(node))
        if identity in seen_nodes:
            return
        seen_nodes.add(identity)
        offset = 0
        while offset < len(node) and fields < _MAX_PROTOBUF_FIELDS:
            key = _read_varint(node, offset)
            if key is None:
                return
            raw_key, offset = key
            wire_type = raw_key & 0x07
            if raw_key >> 3 == 0:
                return
            fields += 1
            if wire_type == 0:
                value = _read_varint(node, offset)
                if value is None:
                    return
                _, offset = value
            elif wire_type == 1:
                offset += 8
            elif wire_type == 2:
                length_value = _read_varint(node, offset)
                if length_value is None:
                    return
                length, offset = length_value
                end = offset + length
                if length < 0 or end > len(node):
                    return
                payload = node[offset:end]
                try:
                    text = payload.decode("ascii")
                except UnicodeDecodeError:
                    text = ""
                digest = _normalized_digest(text)
                if digest is not None and digest not in found:
                    found.append(digest)
                if payload and depth < 4:
                    walk(payload, depth + 1)
                offset = end
            elif wire_type == 5:
                offset += 4
            else:
                return
            if offset > len(node):
                return

    walk(data, 0)
    return tuple(found)


def _xml_digests(content: str) -> tuple[str, ...]:
    root = _xml_root(content)
    if root is None:
        return ()
    found: list[str] = []
    for element in root.iter():
        for name, raw_value in element.attrib.items():
            if "md5" not in name.casefold() and "hash" not in name.casefold():
                continue
            digest = _normalized_digest(raw_value)
            if digest is not None and digest not in found:
                found.append(digest)
        if "md5" in element.tag.casefold() or "hash" in element.tag.casefold():
            digest = _normalized_digest(element.text)
            if digest is not None and digest not in found:
                found.append(digest)
    return tuple(found)


def _month(create_time: int, timezone: str) -> str:
    return (
        datetime.fromtimestamp(int(create_time), UTC)
        .astimezone(ZoneInfo(timezone))
        .strftime("%Y-%m")
    )


class NativeResourceResolver:
    """Native WeChat metadata extraction and read-only local resource resolution."""

    def __init__(
        self,
        source_root: Path,
        source_account_binding_id: str,
        *,
        reader_timezone: str,
        image_decoder_key: bytes | None = None,
        image_xor_key: int | None = None,
        sticker_decoder_key: bytes | None = None,
        image_index: NativeImageMapping | None = None,
        voice_index: NativeVoiceIndex | None = None,
        dependency_recorder: Callable[[Path, Any], None] | None = None,
    ) -> None:
        self.source_root = source_root.resolve()
        self.source_account_binding_id = source_account_binding_id
        self.reader_timezone = reader_timezone
        self.image_decoder_key = image_decoder_key
        self._image_xor_key = image_xor_key
        self.sticker_decoder_key = sticker_decoder_key
        self.image_index = image_index
        self.voice_index = voice_index
        # Called with the exact absolute path and its ``fstat``-verified metadata for
        # every payload file a read returns, so a dependency-scoped session can
        # re-validate only those files before its read transaction is released.
        self._dependency_recorder = dependency_recorder

    def _note_dependency(self, path: Path | None, metadata: Any) -> None:
        if path is not None and self._dependency_recorder is not None:
            self._dependency_recorder(path, metadata)

    def _absolute(self, parts: tuple[str, ...], name: str) -> Path:
        return self.source_root.parent.joinpath(*parts, name)

    def _encode_locator(self, locator: NativeResourceLocator) -> str:
        payload = json.dumps(
            {"schema": "sightglass.native-resource.v1", **locator.__dict__},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        digest = hashlib.sha256(
            b"sightglass-native-resource\0"
            + self.source_account_binding_id.encode()
            + b"\0"
            + payload
        ).digest()[:16]
        return f"nres1.{_encoded(payload)}.{_encoded(digest)}"

    def _decode_locator(self, value: str) -> NativeResourceLocator:
        try:
            prefix, encoded_payload, encoded_digest = value.split(".", 2)
            payload = _decoded(encoded_payload)
            supplied_digest = _decoded(encoded_digest)
            parsed: Any = json.loads(payload)
        except (binascii.Error, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE) from exc
        expected_digest = hashlib.sha256(
            b"sightglass-native-resource\0"
            + self.source_account_binding_id.encode()
            + b"\0"
            + payload
        ).digest()[:16]
        if (
            prefix != "nres1"
            or supplied_digest != expected_digest
            or not isinstance(parsed, dict)
            or parsed.get("schema") != "sightglass.native-resource.v1"
        ):
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
        try:
            locator = NativeResourceLocator(
                kind=str(parsed["kind"]),
                source_message_id=str(parsed["source_message_id"]),
                conversation_source_id=str(parsed["conversation_source_id"]),
                month=str(parsed["month"]),
                original_name=(
                    str(parsed["original_name"])
                    if parsed.get("original_name") is not None
                    else None
                ),
                declared_size=(
                    int(parsed["declared_size"])
                    if parsed.get("declared_size") is not None
                    else None
                ),
                declared_hash=(
                    str(parsed["declared_hash"])
                    if parsed.get("declared_hash") is not None
                    else None
                ),
                attach_id=(
                    str(parsed["attach_id"]) if parsed.get("attach_id") is not None else None
                ),
                local_id=(int(parsed["local_id"]) if parsed.get("local_id") is not None else None),
                create_time=(
                    int(parsed["create_time"]) if parsed.get("create_time") is not None else None
                ),
                image_directory=(
                    tuple(str(part) for part in parsed["image_directory"])
                    if parsed.get("image_directory") is not None
                    else None
                ),
                image_stem=(
                    str(parsed["image_stem"]) if parsed.get("image_stem") is not None else None
                ),
                voice_database=(
                    str(parsed["voice_database"])
                    if parsed.get("voice_database") is not None
                    else None
                ),
                voice_chat_name_id=(
                    int(parsed["voice_chat_name_id"])
                    if parsed.get("voice_chat_name_id") is not None
                    else None
                ),
                server_id=(
                    int(parsed["server_id"]) if parsed.get("server_id") is not None else None
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE) from exc
        if (
            locator.kind not in _LOCATOR_KINDS
            or not locator.source_message_id
            or not locator.conversation_source_id
            or not re.fullmatch(r"\d{4}-\d{2}", locator.month)
            or locator.original_name != _safe_basename(locator.original_name)
            or locator.declared_hash != _normalized_digest(locator.declared_hash)
            or (
                locator.declared_size is not None
                and not 0 <= locator.declared_size <= (1 << 63) - 1
            )
            or (locator.local_id is not None and not 0 <= locator.local_id <= (1 << 63) - 1)
            or (locator.create_time is not None and not 0 <= locator.create_time <= (1 << 63) - 1)
            or (locator.server_id is not None and not 0 <= locator.server_id <= (1 << 63) - 1)
            or (
                locator.voice_chat_name_id is not None
                and not 0 <= locator.voice_chat_name_id <= (1 << 63) - 1
            )
            or (
                locator.voice_database is not None
                and MEDIA_DATABASE.fullmatch(locator.voice_database) is None
            )
            or (locator.image_stem is None) != (locator.image_directory is None)
            or (locator.image_stem is not None and not IMAGE_STEM.fullmatch(locator.image_stem))
            or (
                locator.image_directory is not None
                and not _valid_attach_directory(locator.image_directory)
            )
        ):
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
        if locator.kind == "video" and locator.declared_hash is None:
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
        if locator.kind == "sticker" and (
            locator.declared_hash is None or len(locator.declared_hash) != 32
        ):
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
        if locator.kind == "voice" and (
            locator.voice_database is None
            or locator.voice_chat_name_id is None
            or locator.local_id is None
            or locator.server_id is None
            or locator.create_time is None
        ):
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
        return locator

    @contextmanager
    def _directory(self, parts: tuple[str, ...]) -> Iterator[int]:
        descriptors: list[int] = []
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            source_root = os.open(self.source_root, flags)
            descriptors.append(source_root)
            current = os.open("..", flags, dir_fd=source_root)
            descriptors.append(current)
            for part in parts:
                if not part or part in {".", ".."} or "/" in part or "\\" in part:
                    raise SightglassError(ErrorCode.RESOURCE_BLOCKED)
                current = os.open(part, flags, dir_fd=current)
                descriptors.append(current)
            yield current
        except FileNotFoundError as exc:
            raise SightglassError(
                ErrorCode.RESOURCE_UNAVAILABLE,
                details={"reason": "resource_missing"},
            ) from exc
        except SightglassError:
            raise
        except OSError as exc:
            raise SightglassError(
                ErrorCode.RESOURCE_BLOCKED,
                details={"reason": "resource_path_blocked"},
            ) from exc
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)

    @staticmethod
    def _file_candidate_names(directory_fd: int, name: str) -> tuple[str, ...]:
        suffix = Path(name).suffix
        stem = name[: -len(suffix)] if suffix else name
        numbered = re.compile(rf"^{re.escape(stem)} \(([1-9]\d*)\){re.escape(suffix)}$")
        values: list[tuple[int, str]] = []
        for candidate in os.listdir(directory_fd):
            if candidate == name:
                values.append((0, candidate))
                continue
            match = numbered.fullmatch(candidate)
            if match is not None:
                values.append((int(match.group(1)), candidate))
        return tuple(name for _, name in sorted(values))

    def _read_file(
        self,
        directory_fd: int,
        name: str,
        max_bytes: int,
        *,
        dependency_path: Path | None = None,
    ) -> bytes:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        try:
            descriptor = os.open(name, flags, dir_fd=directory_fd)
        except FileNotFoundError as exc:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True) from exc
        except OSError as exc:
            raise SightglassError(
                ErrorCode.RESOURCE_BLOCKED,
                details={"reason": "resource_path_blocked"},
            ) from exc
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                raise SightglassError(
                    ErrorCode.RESOURCE_BLOCKED,
                    details={"reason": "resource_not_single_regular_file"},
                )
            if before.st_size > max_bytes:
                raise SightglassError(
                    ErrorCode.RESOURCE_TOO_LARGE,
                    details={"max_bytes": max_bytes},
                )
            chunks: list[bytes] = []
            returned = 0
            while returned <= max_bytes:
                chunk = os.read(descriptor, min(1024 * 1024, max_bytes + 1 - returned))
                if not chunk:
                    break
                chunks.append(chunk)
                returned += len(chunk)
            after = os.fstat(descriptor)
            path_after = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except SightglassError:
            raise
        except OSError as exc:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True) from exc
        finally:
            os.close(descriptor)
        identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        path_identity = (
            path_after.st_dev,
            path_after.st_ino,
            path_after.st_size,
            path_after.st_mtime_ns,
        )
        if identity_before != identity_after or identity_after != path_identity:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        data = b"".join(chunks)
        if len(data) > max_bytes:
            raise SightglassError(
                ErrorCode.RESOURCE_TOO_LARGE,
                details={"max_bytes": max_bytes},
            )
        self._note_dependency(dependency_path, path_after)
        return data

    @staticmethod
    def _validate_declarations(data: bytes, locator: NativeResourceLocator) -> None:
        if locator.declared_size is not None and len(data) != locator.declared_size:
            raise SightglassError(
                ErrorCode.RESOURCE_BLOCKED,
                details={"reason": "declared_size_mismatch"},
            )
        if locator.declared_hash is None:
            return
        algorithm = hashlib.md5 if len(locator.declared_hash) == 32 else hashlib.sha256
        if algorithm(data).hexdigest() != locator.declared_hash:
            raise SightglassError(
                ErrorCode.RESOURCE_BLOCKED,
                details={"reason": "declared_hash_mismatch"},
            )

    def _file_directory_parts(self, locator: NativeResourceLocator) -> tuple[str, ...]:
        return ("msg", "file", locator.month)

    def _video_directory_parts(self, locator: NativeResourceLocator) -> tuple[str, ...]:
        return ("msg", "video", locator.month)

    def _sticker_directory_parts(
        self, locator: NativeResourceLocator, *, thumbnail: bool = False
    ) -> tuple[str, ...]:
        assert locator.declared_hash is not None
        store = _STICKER_THUMBNAIL_STORE if thumbnail else _STICKER_STORE
        return (*store, locator.declared_hash[:2])

    def _read_file_prefix(
        self,
        directory_fd: int,
        name: str,
        size: int,
        *,
        dependency_path: Path | None = None,
    ) -> bytes:
        """Read a stable prefix without following or accepting multiply-linked files."""

        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        try:
            descriptor = os.open(name, flags, dir_fd=directory_fd)
        except FileNotFoundError as exc:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True) from exc
        except OSError as exc:
            raise SightglassError(
                ErrorCode.RESOURCE_BLOCKED,
                details={"reason": "resource_path_blocked"},
            ) from exc
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                raise SightglassError(
                    ErrorCode.RESOURCE_BLOCKED,
                    details={"reason": "resource_not_single_regular_file"},
                )
            data = os.read(descriptor, size)
            after = os.fstat(descriptor)
            path_after = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except SightglassError:
            raise
        except OSError as exc:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True) from exc
        finally:
            os.close(descriptor)
        identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        path_identity = (
            path_after.st_dev,
            path_after.st_ino,
            path_after.st_size,
            path_after.st_mtime_ns,
        )
        if identity_before != identity_after or identity_after != path_identity:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        self._note_dependency(dependency_path, path_after)
        return data

    def _sticker_entry_state(self, directory_fd: int, name: str) -> str:
        """Classify one encrypted cache entry as missing, blocked, image, or opaque."""

        state = self._image_entry_state(directory_fd, name)
        if state != "present":
            return state
        if self.sticker_decoder_key is None:
            return "encrypted"
        prefix = self._read_file_prefix(directory_fd, name, 16)
        try:
            plaintext = decrypt_wechat_sticker_prefix(prefix, self.sticker_decoder_key)
        except SightglassError:
            return "opaque"
        return "image" if is_wechat_sticker_image(plaintext) else "opaque"

    def _sticker_variant_state(self, locator: NativeResourceLocator, *, thumbnail: bool) -> str:
        assert locator.declared_hash is not None
        name = f"{locator.declared_hash}.thumb" if thumbnail else locator.declared_hash
        parts = self._sticker_directory_parts(locator, thumbnail=thumbnail)
        try:
            with self._directory(parts) as directory_fd:
                return self._sticker_entry_state(directory_fd, name)
        except SightglassError as exc:
            if exc.code == ErrorCode.RESOURCE_UNAVAILABLE:
                return "missing"
            raise

    def _read_sticker_variant(
        self,
        locator: NativeResourceLocator,
        *,
        thumbnail: bool,
        max_bytes: int,
        record: bool = False,
    ) -> bytes | None:
        assert locator.declared_hash is not None
        if self.sticker_decoder_key is None:
            raise SightglassError(
                ErrorCode.RESOURCE_UNAVAILABLE,
                details={"reason": "sticker_decoder_key_missing"},
            )
        name = f"{locator.declared_hash}.thumb" if thumbnail else locator.declared_hash
        parts = self._sticker_directory_parts(locator, thumbnail=thumbnail)
        try:
            with self._directory(parts) as directory_fd:
                state = self._image_entry_state(directory_fd, name)
                if state == "blocked":
                    raise SightglassError(
                        ErrorCode.RESOURCE_BLOCKED,
                        details={"reason": "resource_not_single_regular_file"},
                    )
                if state == "missing":
                    return None
                ciphertext = self._read_file(
                    directory_fd,
                    name,
                    max_bytes + 16,
                    dependency_path=(self._absolute(parts, name) if record else None),
                )
        except SightglassError as exc:
            if exc.code == ErrorCode.RESOURCE_UNAVAILABLE:
                return None
            raise
        data = decrypt_wechat_sticker(ciphertext, self.sticker_decoder_key)
        if len(data) > max_bytes:
            raise SightglassError(
                ErrorCode.RESOURCE_TOO_LARGE,
                details={"max_bytes": max_bytes},
            )
        return data

    def _video_layout_observed(self) -> bool:
        """Report whether this installation actually keeps a native video store.

        A missing payload inside an observed store is a bounded local absence. When
        the store itself is absent the layout is unverified for this installation, so
        absence there must not be presented as a missing original.
        """
        try:
            with self._directory(("msg", "video")):
                return True
        except SightglassError:
            return False

    @staticmethod
    def _video_name_convention_observed(directory_fd: int) -> bool:
        """Report whether this directory stores payloads under the digest-named convention.

        Bounded to one scan of at most ``_MAX_VIDEO_SCAN_ENTRIES`` entries. Absence of
        the convention leaves the local layout unverified, which must not be reported
        as a missing original.
        """
        try:
            with os.scandir(directory_fd) as entries:
                for index, entry in enumerate(entries):
                    if index >= _MAX_VIDEO_SCAN_ENTRIES:
                        break
                    if _VIDEO_NAME.fullmatch(entry.name):
                        return True
        except OSError:
            return False
        return False

    @staticmethod
    def _video_variant_names(directory_fd: int, declared_hash: str) -> tuple[str, ...]:
        """Existing digest-named variants, rejecting anything but a single-link regular file."""
        present: list[str] = []
        for suffix in _VIDEO_SUFFIXES:
            name = f"{declared_hash}{suffix}"
            try:
                metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise SightglassError(
                    ErrorCode.RESOURCE_BLOCKED,
                    details={"reason": "resource_not_single_regular_file"},
                )
            present.append(name)
        return tuple(present)

    @staticmethod
    def _image_entry_state(directory_fd: int, name: str) -> str:
        """``missing``, ``blocked``, or ``present`` for one digest-named image entry."""
        try:
            metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            return "missing"
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            return "blocked"
        return "present"

    @staticmethod
    def _image_entry_is_v2(directory_fd: int, name: str) -> bool:
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory_fd,
        )
        try:
            return os.read(descriptor, len(V2_MAGIC)) == V2_MAGIC
        finally:
            os.close(descriptor)

    def _image_entry_locked(self, directory_fd: int, name: str) -> bool:
        """Report whether one entry still needs an unenrolled V2 decoder key."""
        return self._image_entry_is_v2(directory_fd, name) and self.image_decoder_key is None

    def _image_attach_store_observed(self) -> bool:
        """Report whether this installation keeps a native attach store.

        A mapped entry missing inside an observed store is a bounded local absence.
        When the store itself is absent, the layout is unverified for this
        installation, so nothing there is presented as a missing original.
        """
        try:
            with self._directory(_IMAGE_ATTACH_STORE):
                return True
        except SightglassError:
            return False

    def _image_thumbnail_selection(
        self, locator: NativeResourceLocator, *, probe: bool
    ) -> _ImageSelection | None:
        """The bounded thumbnail-cache preview beside ``db_storage``, when present."""
        if locator.local_id is None or locator.create_time is None:
            return None
        directory = (
            _IMAGE_THUMBNAIL_ROOT,
            locator.month,
            _IMAGE_THUMBNAIL_STORE,
            hashlib.md5(locator.conversation_source_id.encode()).hexdigest(),
            _IMAGE_THUMBNAIL_DIRECTORY,
        )
        name = f"{locator.local_id}_{locator.create_time}_thumb.jpg"
        try:
            with self._directory(directory) as directory_fd:
                state = self._image_entry_state(directory_fd, name)
                if state == "blocked":
                    raise SightglassError(
                        ErrorCode.RESOURCE_BLOCKED,
                        details={"reason": "resource_not_single_regular_file"},
                    )
                if state != "present":
                    return None
                return _ImageSelection(
                    directory,
                    name,
                    "thumbnail",
                    self._image_entry_locked(directory_fd, name) if probe else False,
                )
        except SightglassError as exc:
            if exc.code == ErrorCode.RESOURCE_UNAVAILABLE:
                return None
            raise

    def _locked_original_preview(self, locator: NativeResourceLocator) -> _ImageSelection | None:
        """The message-positioned preview a V2-locked mapped original falls back to.

        Reading a mapped attach-store entry that still needs its unenrolled V2
        decoder key would serve ciphertext, so the bounded thumbnail cache beside
        ``db_storage`` answers instead when it holds the exact message position.
        That entry is separate local evidence the contract lets satisfy preview, and
        it stays ``thumbnail``-class. A preview that is itself V2-locked, absent, or
        unsafe leaves the mapped entry's explicit ``key_missing`` state intact.
        """
        selection = self._image_thumbnail_selection(locator, probe=True)
        if selection is None or selection.locked:
            return None
        return selection

    def _mapped_image_selection(
        self, locator: NativeResourceLocator, *, max_bytes: int | None
    ) -> _ImageSelection | None:
        """Select within the mapped attach-store entry family for one file hash."""

        directory = locator.image_directory
        stem = locator.image_stem
        if directory is None or stem is None:
            return None
        with self._directory(directory) as directory_fd:
            for suffix in _IMAGE_ORIGINAL_SUFFIXES:
                name = f"{stem}{suffix}"
                state = self._image_entry_state(directory_fd, name)
                if state == "blocked":
                    raise SightglassError(
                        ErrorCode.RESOURCE_BLOCKED,
                        details={"reason": "resource_not_single_regular_file"},
                    )
                if state == "present":
                    return _ImageSelection(
                        directory,
                        name,
                        "original",
                        self._image_entry_locked(directory_fd, name)
                        if max_bytes is None
                        else False,
                    )
            present: list[str] = []
            for suffix in _IMAGE_PREVIEW_SUFFIXES:
                name = f"{stem}{suffix}"
                state = self._image_entry_state(directory_fd, name)
                if state == "blocked":
                    raise SightglassError(
                        ErrorCode.RESOURCE_BLOCKED,
                        details={"reason": "resource_not_single_regular_file"},
                    )
                if state == "present":
                    present.append(name)
            if not present:
                return None
            if len(present) > 1:
                if max_bytes is None:
                    # Descriptor calls never read payload bytes, so several
                    # unclassifiable preview entries make no local claim while a read
                    # reconciles their bytes exactly like native file duplicates.
                    return _ImageSelection(directory, present[0], "thumbnail", False, deferred=True)
                values = [self._read_file(directory_fd, name, max_bytes) for name in present]
                if len({hashlib.sha256(value).digest() for value in values}) != 1:
                    raise SightglassError(
                        ErrorCode.RESOURCE_BLOCKED,
                        details={"reason": "resource_candidates_ambiguous"},
                    )
            return _ImageSelection(
                directory,
                present[0],
                "thumbnail",
                self._image_entry_locked(directory_fd, present[0]) if max_bytes is None else False,
            )

    def _image_selection(
        self, locator: NativeResourceLocator, *, max_bytes: int | None
    ) -> _ImageSelection | None:
        """The richest locally present image entry, or ``None`` when none is local.

        A mapped attach-store entry always wins over the bounded thumbnail cache. A
        read passes its byte bound so several unclassifiable preview entries can be
        reconciled exactly like native file duplicates; a descriptor passes ``None``
        and makes no claim about them.
        """

        if locator.image_directory is not None and locator.image_stem is not None:
            try:
                selection = self._mapped_image_selection(locator, max_bytes=max_bytes)
            except SightglassError as exc:
                if exc.code != ErrorCode.RESOURCE_UNAVAILABLE:
                    raise
                selection = None
            if selection is not None:
                return selection
        return self._image_thumbnail_selection(locator, probe=max_bytes is None)

    @staticmethod
    def _validated_image_entry(entry: NativeImageEntry | None) -> NativeImageEntry | None:
        """Accept only a digest-named entry inside the native attach store."""
        if entry is None:
            return None
        if not _valid_attach_directory(entry.directory) or not IMAGE_STEM.fullmatch(entry.stem):
            return None
        return entry

    def _availability(self, locator: NativeResourceLocator) -> str:
        try:
            if locator.kind == "file":
                if locator.original_name is None:
                    return "metadata_only"
                with self._directory(self._file_directory_parts(locator)) as directory_fd:
                    names = self._file_candidate_names(directory_fd, locator.original_name)
                    if len(names) != 1:
                        return "metadata_only" if names else "missing"
                    metadata = os.stat(names[0], dir_fd=directory_fd, follow_symlinks=False)
                    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                        return "blocked_by_policy"
                    if (
                        locator.declared_size is not None
                        and metadata.st_size != locator.declared_size
                    ):
                        return "metadata_only"
                    return "local_available"
            if locator.kind == "video":
                if locator.declared_hash is None or not self._video_layout_observed():
                    return "metadata_only"
                with self._directory(self._video_directory_parts(locator)) as directory_fd:
                    names = self._video_variant_names(directory_fd, locator.declared_hash)
                    if names:
                        # Compressed and pre-compression originals are distinct payloads;
                        # a caller cannot be told which one a single descriptor means.
                        return "local_available" if len(names) == 1 else "metadata_only"
                    if not self._video_name_convention_observed(directory_fd):
                        return "metadata_only"
                return "missing"
            if locator.kind == "voice":
                return "local_available"
            if locator.kind == "sticker":
                assert locator.declared_hash is not None
                original = self._sticker_variant_state(locator, thumbnail=False)
                if original == "blocked":
                    return "blocked_by_policy"
                if original == "image":
                    return "local_available"
                preview = self._sticker_variant_state(locator, thumbnail=True)
                if preview == "blocked":
                    return "blocked_by_policy"
                if self.sticker_decoder_key is None and (
                    original == "encrypted" or preview == "encrypted"
                ):
                    return "key_missing"
                if preview == "image":
                    return "preview_only"
                if original == "missing" and preview == "missing":
                    return "missing"
                return "metadata_only"
            selection = self._image_selection(locator, max_bytes=None)
            if selection is None:
                if locator.image_directory is not None and self._image_attach_store_observed():
                    # The mapping named an entry inside an observed attach store, so
                    # its absence is a bounded local absence.
                    return "missing"
                return "metadata_only"
            if selection.deferred:
                # Several unclassifiable preview entries are local, but one descriptor
                # cannot say which entry a preview call would serve.
                return "metadata_only"
            if selection.locked:
                # The richer mapped entry cannot be decoded here, so a decodable exact
                # message-positioned preview is what a read can actually serve.
                if self._locked_original_preview(locator) is not None:
                    return "preview_only"
                return "key_missing"
            # A preview keeps ``preview_only`` so no caller reads it as a source
            # original or as the declared payload of the message.
            return "local_available" if selection.variant == "original" else "preview_only"
        except SightglassError as exc:
            if exc.code == ErrorCode.RESOURCE_UNAVAILABLE:
                return "missing"
            return "blocked_by_policy"
        except OSError:
            return "blocked_by_policy"
        return "missing"

    def resources_for_message(
        self,
        *,
        source_message_id: str,
        conversation_source_id: str,
        local_id: int,
        create_time: int,
        local_type: int,
        raw_content: str,
        packed_info_data: bytes | None,
        server_id: int = 0,
    ) -> tuple[SourceResource, ...]:
        month = _month(create_time, self.reader_timezone)
        if local_type == 49:
            root = _xml_root(raw_content)
            if root is None:
                return ()
            app_type = _first_text(root, (".//appmsg/type",))
            if app_type != "6":
                # Non-file app messages may still have one exact, message-positioned
                # cache thumbnail.  That is preview evidence only: it neither names
                # nor proves a source original, and an absent cache entry is not a
                # resource descriptor.
                locator = NativeResourceLocator(
                    kind="image",
                    source_message_id=source_message_id,
                    conversation_source_id=conversation_source_id,
                    month=month,
                    original_name=None,
                    declared_size=None,
                    declared_hash=None,
                    attach_id=None,
                    local_id=local_id,
                    create_time=create_time,
                )
                availability = self._availability(locator)
                if availability not in {
                    "preview_only",
                    "key_missing",
                    "blocked_by_policy",
                }:
                    return ()
                return (
                    SourceResource(
                        source_ordinal=0,
                        kind="image",
                        source_resource_key=self._encode_locator(locator),
                        mime_type=None,
                        availability=availability,
                    ),
                )
            original_name = _safe_basename(_first_text(root, (".//appmsg/title",)))
            extension = _safe_extension(_first_text(root, (".//appmsg/appattach/fileext",)))
            if original_name and extension and not Path(original_name).suffix:
                original_name = f"{original_name}.{extension}"
            declared_size = _bounded_nonnegative_int(
                _first_text(
                    root,
                    (
                        ".//appmsg/appattach/totallen",
                        ".//appmsg/appattach/filesize",
                        ".//appmsg/appattach/size",
                    ),
                )
            )
            declared_hash = _normalized_digest(
                _first_text(
                    root,
                    (
                        ".//appmsg/appattach/md5",
                        ".//appmsg/appattach/filemd5",
                    ),
                )
            )
            raw_attach_id = _first_text(root, (".//appmsg/appattach/attachid",))
            attach_id = raw_attach_id if raw_attach_id and len(raw_attach_id) <= 2048 else None
            locator = NativeResourceLocator(
                kind="file",
                source_message_id=source_message_id,
                conversation_source_id=conversation_source_id,
                month=month,
                original_name=original_name,
                declared_size=declared_size,
                declared_hash=declared_hash,
                attach_id=attach_id,
            )
            source_key = self._encode_locator(locator) if original_name else None
            availability = self._availability(locator) if source_key else "metadata_only"
            return (
                SourceResource(
                    source_ordinal=0,
                    kind="file",
                    source_resource_key=source_key,
                    mime_type=(mimetypes.guess_type(original_name)[0] if original_name else None),
                    original_name=original_name,
                    declared_size=declared_size,
                    declared_hash=declared_hash,
                    availability=availability,
                ),
            )
        if local_type == 3:
            # The message XML carries a WeChat-assigned hash, not the payload digest
            # and not the stored file name. It only keys the fallback mapping database.
            xml_digest = next(iter(_xml_digests(raw_content)), None)
            entry = self._validated_image_entry(
                self.image_index.entry_for(
                    conversation_source_id=conversation_source_id,
                    local_id=local_id,
                    month=month,
                    xml_digest=xml_digest,
                )
                if self.image_index is not None
                else None
            )
            locator = NativeResourceLocator(
                kind="image",
                source_message_id=source_message_id,
                conversation_source_id=conversation_source_id,
                month=month,
                original_name=None,
                declared_size=None,
                # Neither the message digest nor the stored file hash is a claim about
                # the payload bytes, so no image descriptor publishes one.
                declared_hash=None,
                attach_id=None,
                local_id=local_id,
                create_time=create_time,
                image_directory=entry.directory if entry is not None else None,
                image_stem=entry.stem if entry is not None else None,
            )
            source_key = self._encode_locator(locator)
            return (
                SourceResource(
                    source_ordinal=0,
                    kind="image",
                    source_resource_key=source_key,
                    mime_type=None,
                    availability=self._availability(locator),
                ),
            )
        if local_type == 43:
            # The downloaded payload is named by the media digest the message carries
            # in its resource metadata; two distinct digests cannot both name it.
            digests = (
                _protobuf_digests(bytes(packed_info_data)) if packed_info_data is not None else ()
            )
            locator_hash = digests[0] if len(digests) == 1 else None
            locator = NativeResourceLocator(
                kind="video",
                source_message_id=source_message_id,
                conversation_source_id=conversation_source_id,
                month=month,
                original_name=None,
                declared_size=None,
                # Locator-only digest: it names the file, it is not an integrity claim
                # about the payload bytes, so it is never published as declared_hash.
                declared_hash=locator_hash,
                attach_id=None,
            )
            source_key = self._encode_locator(locator) if locator_hash else None
            availability = self._availability(locator) if source_key else "metadata_only"
            return (
                SourceResource(
                    source_ordinal=0,
                    kind="video",
                    source_resource_key=source_key,
                    mime_type=None,
                    availability=availability,
                ),
            )
        if local_type == 34:
            if self.voice_index is None:
                return (SourceResource(source_ordinal=0, kind="voice"),)
            resolution = self.voice_index.resolve(
                conversation_source_id=conversation_source_id,
                local_id=local_id,
                server_id=server_id,
                create_time=create_time,
            )
            entry = resolution.entry
            if entry is None:
                return (
                    SourceResource(
                        source_ordinal=0,
                        kind="voice",
                        mime_type="audio/silk",
                        availability=resolution.availability,
                    ),
                )
            locator = NativeResourceLocator(
                kind="voice",
                source_message_id=source_message_id,
                conversation_source_id=conversation_source_id,
                month=month,
                original_name=None,
                declared_size=None,
                declared_hash=None,
                attach_id=None,
                local_id=entry.local_id,
                create_time=entry.create_time,
                voice_database=entry.database,
                voice_chat_name_id=entry.chat_name_id,
                server_id=entry.server_id,
            )
            return (
                SourceResource(
                    source_ordinal=0,
                    kind="voice",
                    source_resource_key=self._encode_locator(locator),
                    mime_type="audio/silk",
                    availability="local_available",
                ),
            )
        if local_type == 47:
            root = _xml_root(raw_content)
            emoji = root.find(".//emoji") if root is not None else None
            digest = _normalized_digest(emoji.attrib.get("md5")) if emoji is not None else None
            if digest is None or len(digest) != 32:
                return (SourceResource(source_ordinal=0, kind="sticker"),)
            locator = NativeResourceLocator(
                kind="sticker",
                source_message_id=source_message_id,
                conversation_source_id=conversation_source_id,
                month=month,
                original_name=None,
                declared_size=None,
                # Locator-only digest: live cache evidence shows that WeChat's
                # emoji@md5 names the Persist entry but is not uniformly the MD5
                # of the decrypted payload, so it is never published as a content
                # integrity assertion.
                declared_hash=digest,
                attach_id=None,
            )
            return (
                SourceResource(
                    source_ordinal=0,
                    kind="sticker",
                    source_resource_key=self._encode_locator(locator),
                    availability=self._availability(locator),
                ),
            )
        return ()

    def read(
        self, source_resource_key: str, *, max_bytes: int
    ) -> tuple[bytes, SourceResourceVariant]:
        """Read one source resource and report which source variant it came from."""

        if max_bytes < 1:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        locator = self._decode_locator(source_resource_key)
        if locator.kind == "file":
            if locator.original_name is None:
                raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
            parts = self._file_directory_parts(locator)
            with self._directory(parts) as directory_fd:
                names = self._file_candidate_names(directory_fd, locator.original_name)
                if not names:
                    raise SightglassError(
                        ErrorCode.RESOURCE_UNAVAILABLE,
                        details={"reason": "resource_missing"},
                    )
                values = [
                    self._read_file(
                        directory_fd,
                        name,
                        max_bytes,
                        dependency_path=self._absolute(parts, name),
                    )
                    for name in names
                ]
            if len({hashlib.sha256(value).digest() for value in values}) != 1:
                raise SightglassError(
                    ErrorCode.RESOURCE_BLOCKED,
                    details={"reason": "resource_candidates_ambiguous"},
                )
            data = values[0]
            self._validate_declarations(data, locator)
            return data, "original"
        if locator.kind == "video":
            if locator.declared_hash is None:
                raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
            if not self._video_layout_observed():
                raise SightglassError(
                    ErrorCode.RESOURCE_UNAVAILABLE,
                    details={"reason": "resource_layout_unobserved"},
                )
            parts = self._video_directory_parts(locator)
            with self._directory(parts) as directory_fd:
                names = self._video_variant_names(directory_fd, locator.declared_hash)
                if not names:
                    if not self._video_name_convention_observed(directory_fd):
                        raise SightglassError(
                            ErrorCode.RESOURCE_UNAVAILABLE,
                            details={"reason": "resource_layout_unobserved"},
                        )
                    raise SightglassError(
                        ErrorCode.RESOURCE_UNAVAILABLE,
                        details={"reason": "resource_missing"},
                    )
                if len(names) != 1:
                    raise SightglassError(
                        ErrorCode.RESOURCE_BLOCKED,
                        details={"reason": "resource_candidates_ambiguous"},
                    )
                return (
                    self._read_file(
                        directory_fd,
                        names[0],
                        max_bytes,
                        dependency_path=self._absolute(parts, names[0]),
                    ),
                    "original",
                )
        if locator.kind == "voice":
            if self.voice_index is None:
                raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
            assert locator.voice_database is not None
            assert locator.voice_chat_name_id is not None
            assert locator.local_id is not None
            assert locator.server_id is not None
            assert locator.create_time is not None
            data = self.voice_index.read(
                NativeVoiceEntry(
                    database=locator.voice_database,
                    conversation_source_id=locator.conversation_source_id,
                    chat_name_id=locator.voice_chat_name_id,
                    local_id=locator.local_id,
                    server_id=locator.server_id,
                    create_time=locator.create_time,
                ),
                max_bytes=max_bytes,
            )
            return data, "original"
        if locator.kind == "sticker":
            assert locator.declared_hash is not None
            original = self._read_sticker_variant(
                locator,
                thumbnail=False,
                max_bytes=max_bytes,
                record=True,
            )
            if original is not None and is_wechat_sticker_image(original):
                return original, "original"
            preview = self._read_sticker_variant(
                locator,
                thumbnail=True,
                max_bytes=max_bytes,
                record=True,
            )
            if preview is not None and is_wechat_sticker_image(preview):
                return preview, "thumbnail"
            if original is None and preview is None:
                raise SightglassError(
                    ErrorCode.RESOURCE_UNAVAILABLE,
                    details={"reason": "resource_missing"},
                )
            raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
        selection = self._image_selection(locator, max_bytes=max_bytes)
        if selection is None:
            raise SightglassError(
                ErrorCode.RESOURCE_UNAVAILABLE,
                details={"reason": "resource_missing"},
            )
        with self._directory(selection.directory) as directory_fd:
            data = self._read_file(
                directory_fd,
                selection.name,
                max_bytes,
                dependency_path=self._absolute(selection.directory, selection.name),
            )
        variant = selection.variant
        if data.startswith(V2_MAGIC):
            if self.image_decoder_key is None:
                preview = self._locked_original_preview(locator)
                if preview is None:
                    raise SightglassError(
                        ErrorCode.RESOURCE_UNAVAILABLE,
                        details={"reason": "image_decoder_key_missing"},
                    )
                with self._directory(preview.directory) as directory_fd:
                    data = self._read_file(
                        directory_fd,
                        preview.name,
                        max_bytes,
                        dependency_path=self._absolute(preview.directory, preview.name),
                    )
                if data.startswith(V2_MAGIC):
                    # The preview needs the same unenrolled key, so no local entry is
                    # decodable and the explicit key state still owns this resource.
                    raise SightglassError(
                        ErrorCode.RESOURCE_UNAVAILABLE,
                        details={"reason": "image_decoder_key_missing"},
                    )
                return data, preview.variant
            encrypted = data
            data = decode_wechat_v2_image(data, self.image_decoder_key, xor_key=self._image_xor_key)
            xor_size = struct.unpack_from("<I", encrypted, 10)[0]
            if xor_size:
                # Transient material remains inside this account-scoped resolver.
                self._image_xor_key = encrypted[-1] ^ data[-1]
            if len(data) > max_bytes:
                raise SightglassError(
                    ErrorCode.RESOURCE_TOO_LARGE,
                    details={"max_bytes": max_bytes},
                )
        return data, variant

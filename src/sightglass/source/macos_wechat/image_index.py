"""Optional native mapping databases that name WeChat image payloads.

The native 4.1.13 store does not name image payloads after the digest carried in the
message XML. Two auxiliary SQLCipher databases carry the WeChat-assigned file hash:

- ``message/message_resource.db`` is authoritative and is populated as soon as the
  image arrives: ``ChatName2Id.user_name`` resolves the integer chat id and
  ``MessageResourceInfo.packed_info`` carries the file hash for
  ``(chat_id, message_local_id)``.
- ``hardlink/hardlink.db`` is a fallback with an indexing delay:
  ``image_hardlink_info_v4.md5`` holds the message XML digest and resolves through
  ``dir2id`` to the stored file name and the two attach directory names.

Both databases are optional. An unenrolled key, an absent database, an unverified
page 1, an unexpected schema, or ambiguous rows degrade image resolution to the
bounded thumbnail cache or to an explicit metadata-only descriptor. They never fail
the message source, and unverified metadata never becomes a file name.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any, Protocol

from sightglass.contracts.errors import SightglassError

IMAGE_RESOURCE_DATABASE = "message/message_resource.db"
IMAGE_HARDLINK_DATABASE = "hardlink/hardlink.db"
IMAGE_AUXILIARY_DATABASES: tuple[str, ...] = (
    IMAGE_RESOURCE_DATABASE,
    IMAGE_HARDLINK_DATABASE,
)

# The stored payload is always a digest-named ``.dat`` entry. ``_h``/``_M``/``_t``/
# ``_t_M`` are the only suffixes observed beside it; anything else is not used as a
# file name.
IMAGE_FILE_NAME = re.compile(r"^([0-9a-f]{32})(?:_h|_M|_t|_t_M)?\.dat$")
IMAGE_STEM = re.compile(r"[0-9a-f]{32}")
_LOWER_HEX_32 = re.compile(r"[0-9a-f]{32}")
_MAX_PACKED_INFO_BYTES = 512 * 1024
_MAX_PROTOBUF_FIELDS = 4096
_MAX_MAPPING_ROWS = 2


def safe_component(value: str) -> str | None:
    """Return one bounded single-component path segment, or ``None``."""
    if (
        not value
        or value in {".", ".."}
        or "/" in value
        or "\\" in value
        or "\x00" in value
        or len(value.encode("utf-8")) > 255
    ):
        return None
    return value


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


def packed_file_stem(payload: Any) -> str | None:
    """The single 32-character lowercase file hash carried by one ``packed_info`` blob.

    The blob is bounded and non-recursive beyond two nesting levels. Zero matches and
    several distinct matches both yield ``None`` so an unreadable envelope can never
    become a guessed file name.
    """

    if isinstance(payload, memoryview):
        payload = payload.tobytes()
    if not isinstance(payload, bytes) or not payload or len(payload) > _MAX_PACKED_INFO_BYTES:
        return None
    found: list[str] = []
    fields = 0

    def walk(node: bytes, depth: int) -> None:
        nonlocal fields
        if depth > 2 or fields >= _MAX_PROTOBUF_FIELDS:
            return
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
                chunk = node[offset:end]
                if len(chunk) == 32 and _LOWER_HEX_32.fullmatch(chunk.decode("ascii", "ignore")):
                    text = chunk.decode("ascii")
                    if text not in found:
                        found.append(text)
                elif chunk and depth < 2:
                    walk(chunk, depth + 1)
                offset = end
            elif wire_type == 5:
                offset += 4
            else:
                return
            if offset > len(node):
                return

    walk(payload, 0)
    return found[0] if len(found) == 1 else None


def _row_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


@dataclass(frozen=True)
class NativeImageEntry:
    """One mapped image payload family, relative to the configured source root."""

    directory: tuple[str, ...]
    stem: str


class NativeImageMapping(Protocol):
    """Capability the resolver needs from one image mapping source."""

    def entry_for(
        self,
        *,
        conversation_source_id: str,
        local_id: int,
        month: str,
        xml_digest: str | None,
    ) -> NativeImageEntry | None: ...


class NativeImageIndex:
    """Best-effort mapping evidence for native image payloads.

    ``open_database`` yields a read-only, page-1-verified auxiliary connection and
    raises when the database is absent or not enrolled. Every failure inside this
    class degrades the affected mapping instead of failing the message source.
    """

    def __init__(
        self, open_database: Callable[[str], AbstractContextManager[Any]]
    ) -> None:
        self._open_database = open_database

    def entry_for(
        self,
        *,
        conversation_source_id: str,
        local_id: int,
        month: str,
        xml_digest: str | None,
    ) -> NativeImageEntry | None:
        """The mapped payload entry for one image message, or ``None``."""

        try:
            entry = self._resource_entry(conversation_source_id, local_id, month)
            if entry is not None:
                return entry
            if xml_digest is None:
                return None
            return self._hardlink_entry(conversation_source_id, xml_digest)
        except (SightglassError, OSError):
            # An unenrolled key, an absent database, or a failed query degrades this
            # mapping; it is never evidence about the message's local payload.
            return None

    def _resource_entry(
        self, conversation_source_id: str, local_id: int, month: str
    ) -> NativeImageEntry | None:
        """``MessageResourceInfo.packed_info`` names the stored payload."""

        try:
            with self._open_database(IMAGE_RESOURCE_DATABASE) as connection:
                chat = connection.execute(
                    "SELECT rowid FROM ChatName2Id WHERE user_name = ? LIMIT 1",
                    (conversation_source_id,),
                ).fetchone()
                if chat is None:
                    return None
                chat_id = _row_int(chat[0])
                if chat_id is None:
                    return None
                rows = connection.execute(
                    "SELECT packed_info FROM MessageResourceInfo"
                    " WHERE chat_id = ? AND message_local_id = ? LIMIT ?",
                    (chat_id, local_id, _MAX_MAPPING_ROWS),
                ).fetchall()
        except (TypeError, ValueError, IndexError):
            return None
        stems = {
            stem for stem in (packed_file_stem(row[0]) for row in rows) if stem is not None
        }
        if len(stems) != 1:
            return None
        return NativeImageEntry(
            directory=(
                "msg",
                "attach",
                hashlib.md5(conversation_source_id.encode()).hexdigest(),
                month,
                "Img",
            ),
            stem=stems.pop(),
        )

    def _hardlink_entry(
        self, conversation_source_id: str, xml_digest: str
    ) -> NativeImageEntry | None:
        """``image_hardlink_info_v4`` resolves the stored payload through ``dir2id``."""

        try:
            with self._open_database(IMAGE_HARDLINK_DATABASE) as connection:
                rows = connection.execute(
                    "SELECT file_name, dir1, dir2 FROM image_hardlink_info_v4"
                    " WHERE md5 = ? LIMIT ?",
                    (xml_digest, _MAX_MAPPING_ROWS),
                ).fetchall()
                triples: set[tuple[str, int, int]] = set()
                for row in rows:
                    match = IMAGE_FILE_NAME.fullmatch(str(row[0] or ""))
                    first = _row_int(row[1])
                    second = _row_int(row[2])
                    if match is None or first is None or second is None:
                        continue
                    triples.add((match.group(1), first, second))
                if len(triples) != 1:
                    return None
                stem, dir1, dir2 = triples.pop()
                names: dict[int, str] = {}
                for row in connection.execute(
                    "SELECT rowid, username FROM dir2id WHERE rowid IN (?, ?)",
                    (dir1, dir2),
                ).fetchall():
                    identifier = _row_int(row[0])
                    if identifier is not None and isinstance(row[1], str):
                        names[identifier] = row[1]
        except (TypeError, ValueError, IndexError):
            return None
        chat_dir = safe_component(names.get(dir1, ""))
        date_dir = safe_component(names.get(dir2, ""))
        if chat_dir is None or date_dir is None:
            return None
        if chat_dir != hashlib.md5(conversation_source_id.encode()).hexdigest():
            # The resolved attach directory must agree with the conversation the
            # message belongs to; a disagreeing fallback row never serves bytes.
            return None
        return NativeImageEntry(
            directory=("msg", "attach", chat_dir, date_dir, "Img"),
            stem=stem,
        )

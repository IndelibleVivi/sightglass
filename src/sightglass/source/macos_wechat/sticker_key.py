from __future__ import annotations

import os
import re
import stat
import struct
import zlib
from collections.abc import Iterator
from contextlib import ExitStack
from pathlib import Path

from cryptography.hazmat.decrepit.ciphers.modes import CFB
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms

from sightglass.resources.sticker import (
    decrypt_wechat_sticker_prefix,
    derive_wechat_sticker_key,
    is_wechat_sticker_image,
)

from .config import MacOSWeChatSettings
from .discovery import DEFAULT_WECHAT_APP

_FAT_MAGIC = 0xCAFEBABE
_CPU_TYPE_ARM64 = 0x0100000C
_MACHO_64_MAGIC = 0xFEEDFACF
_MAX_APP_BINARY_BYTES = 512 * 1024 * 1024
_MAX_MMKV_BYTES = 16 * 1024 * 1024
_MAX_MMKV_META_BYTES = 64 * 1024
_MAX_MMKV_ENTRIES = 8192
_MAX_MMKV_KEY_BYTES = 512
_MAX_ACCOUNT_STRING_BYTES = 1024
_MAX_STICKER_SCAN_ENTRIES = 8192
_STICKER_FILE = re.compile(r"[0-9a-f]{32}")
_TARGET_KEYS = frozenset(
    {
        b"mmkv_key_latest_login_uin",
        b"mmkv_key_latest_login_username",
        b"mmkv_key_user_name",
    }
)

# This is an address inside the arm64 slice, not key material. The installed
# WeChat build is the authority for the bytes; Sightglass never persists them.
_PROFILE_MMKV_KEY_OFFSETS = {
    "wechat-macos-4.1.13-269602-arm64-v1": 0x84C6830,
}


class _StickerKeyUnavailable(RuntimeError):
    pass


def _read_regular(path: Path, *, max_bytes: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size < 1
            or before.st_size > max_bytes
        ):
            raise _StickerKeyUnavailable("invalid local input")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                raise _StickerKeyUnavailable("truncated local input")
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise _StickerKeyUnavailable("local input changed")
        return b"".join(chunks)
    except OSError as exc:
        raise _StickerKeyUnavailable("local input unavailable") from exc
    finally:
        os.close(descriptor)


def _read_arm64_static_bytes(path: Path, *, offset: int, size: int) -> bytes:
    try:
        metadata = path.lstat()
        if (
            path.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size < 8
            or metadata.st_size > _MAX_APP_BINARY_BYTES
        ):
            raise _StickerKeyUnavailable("invalid app binary")
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise _StickerKeyUnavailable("app binary unavailable") from exc
    try:
        header = os.pread(descriptor, 8, 0)
        if len(header) != 8:
            raise _StickerKeyUnavailable("truncated app binary")
        magic, count = struct.unpack(">II", header)
        if magic != _FAT_MAGIC or count < 1 or count > 8:
            raise _StickerKeyUnavailable("unsupported app binary")
        records = os.pread(descriptor, count * 20, 8)
        if len(records) != count * 20:
            raise _StickerKeyUnavailable("truncated app binary")
        arm64_slice: tuple[int, int] | None = None
        for index in range(count):
            cpu_type, _, slice_offset, slice_size, _ = struct.unpack_from(
                ">iiIII", records, index * 20
            )
            if cpu_type == _CPU_TYPE_ARM64:
                if arm64_slice is not None:
                    raise _StickerKeyUnavailable("ambiguous app binary")
                arm64_slice = (slice_offset, slice_size)
        if arm64_slice is None:
            raise _StickerKeyUnavailable("arm64 app slice unavailable")
        slice_offset, slice_size = arm64_slice
        if (
            slice_offset + slice_size > metadata.st_size
            or offset < 0
            or size < 1
            or offset + size > slice_size
        ):
            raise _StickerKeyUnavailable("invalid app slice")
        slice_magic = os.pread(descriptor, 4, slice_offset)
        if len(slice_magic) != 4 or struct.unpack("<I", slice_magic)[0] != _MACHO_64_MAGIC:
            raise _StickerKeyUnavailable("invalid arm64 app slice")
        value = os.pread(descriptor, size, slice_offset + offset)
        if len(value) != size:
            raise _StickerKeyUnavailable("truncated app slice")
        return value
    except OSError as exc:
        raise _StickerKeyUnavailable("app binary unreadable") from exc
    finally:
        os.close(descriptor)


def _read_varint(data: bytes | bytearray, cursor: int, *, max_bytes: int) -> tuple[int, int]:
    value = 0
    for index in range(max_bytes):
        if cursor >= len(data):
            raise _StickerKeyUnavailable("truncated MMKV varint")
        byte = data[cursor]
        cursor += 1
        value |= (byte & 0x7F) << (index * 7)
        if byte < 0x80:
            return value, cursor
    raise _StickerKeyUnavailable("oversized MMKV varint")


def _selected_mmkv_values(body: bytearray) -> dict[bytes, bytes]:
    _, cursor = _read_varint(body, 0, max_bytes=5)
    values: dict[bytes, bytes] = {}
    entries = 0
    while cursor < len(body):
        entries += 1
        if entries > _MAX_MMKV_ENTRIES:
            raise _StickerKeyUnavailable("too many MMKV entries")
        key_size, cursor = _read_varint(body, cursor, max_bytes=5)
        if key_size > _MAX_MMKV_KEY_BYTES or cursor + key_size > len(body):
            raise _StickerKeyUnavailable("invalid MMKV key")
        key = bytes(body[cursor : cursor + key_size])
        cursor += key_size
        if not key:
            continue
        value_size, cursor = _read_varint(body, cursor, max_bytes=5)
        if cursor + value_size > len(body):
            raise _StickerKeyUnavailable("invalid MMKV value")
        if key in _TARGET_KEYS:
            if value_size:
                values[key] = bytes(body[cursor : cursor + value_size])
            else:
                values.pop(key, None)
        cursor += value_size
    return values


def _decode_mmkv_snapshot(data: bytes, meta: bytes, key: bytes) -> dict[bytes, bytes]:
    if len(key) != 16 or len(data) < 4 or len(meta) < 32:
        raise _StickerKeyUnavailable("invalid MMKV snapshot")
    crc_digest, version, _, vector, meta_actual_size = struct.unpack_from(
        "<III16sI", meta
    )
    if version > 6:
        raise _StickerKeyUnavailable("unsupported MMKV metadata")
    actual_size = (
        meta_actual_size if version >= 3 else struct.unpack_from("<I", data)[0]
    )
    if actual_size < 1 or actual_size > len(data) - 4:
        raise _StickerKeyUnavailable("invalid MMKV size")
    ciphertext = data[4 : actual_size + 4]
    if zlib.crc32(ciphertext) & 0xFFFFFFFF != crc_digest:
        raise _StickerKeyUnavailable("MMKV CRC mismatch")
    iv = vector if version >= 2 else key
    decryptor = Cipher(algorithms.AES(key), CFB(iv)).decryptor()
    plaintext = bytearray(decryptor.update(ciphertext) + decryptor.finalize())
    try:
        return _selected_mmkv_values(plaintext)
    finally:
        plaintext[:] = bytes(len(plaintext))


def _load_mmkv_values(path: Path, key: bytes) -> dict[bytes, bytes]:
    meta_path = Path(f"{path}.crc")
    for _ in range(3):
        meta_before = _read_regular(meta_path, max_bytes=_MAX_MMKV_META_BYTES)
        data = _read_regular(path, max_bytes=_MAX_MMKV_BYTES)
        meta_after = _read_regular(meta_path, max_bytes=_MAX_MMKV_META_BYTES)
        if meta_before == meta_after:
            return _decode_mmkv_snapshot(data, meta_after, key)
    raise _StickerKeyUnavailable("MMKV snapshot changed")


def _decode_uint64(value: bytes) -> int:
    decoded, cursor = _read_varint(value, 0, max_bytes=10)
    if cursor != len(value) or decoded < 1 or decoded > 0xFFFFFFFFFFFFFFFF:
        raise _StickerKeyUnavailable("invalid account integer")
    return decoded


def _decode_string(value: bytes) -> str:
    size, cursor = _read_varint(value, 0, max_bytes=5)
    if (
        size < 1
        or size > _MAX_ACCOUNT_STRING_BYTES
        or cursor + size != len(value)
    ):
        raise _StickerKeyUnavailable("invalid account string")
    try:
        decoded = value[cursor:].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _StickerKeyUnavailable("invalid account string") from exc
    if "\x00" in decoded:
        raise _StickerKeyUnavailable("invalid account string")
    return decoded


def _sticker_ciphertext_prefixes(source_root: Path) -> Iterator[bytes]:
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        account_descriptor = os.open(source_root.parent, directory_flags)
    except OSError:
        return

    with ExitStack() as stack:
        stack.callback(os.close, account_descriptor)
        parent_descriptor = account_descriptor
        try:
            for name in ("business", "emoticon", "Persist"):
                parent_descriptor = os.open(
                    name,
                    directory_flags,
                    dir_fd=parent_descriptor,
                )
                stack.callback(os.close, parent_descriptor)
        except OSError:
            return

        inspected = 0
        for prefix in (f"{value:02x}" for value in range(256)):
            try:
                prefix_descriptor = os.open(
                    prefix,
                    directory_flags,
                    dir_fd=parent_descriptor,
                )
            except OSError:
                continue
            try:
                entries = os.scandir(prefix_descriptor)
                with entries:
                    for child in entries:
                        if inspected >= _MAX_STICKER_SCAN_ENTRIES:
                            return
                        inspected += 1
                        if not _STICKER_FILE.fullmatch(
                            child.name
                        ) or not child.name.startswith(prefix):
                            continue
                        try:
                            descriptor = os.open(
                                child.name,
                                os.O_RDONLY
                                | getattr(os, "O_NOFOLLOW", 0)
                                | getattr(os, "O_NONBLOCK", 0),
                                dir_fd=prefix_descriptor,
                            )
                            try:
                                before = os.fstat(descriptor)
                                if (
                                    not stat.S_ISREG(before.st_mode)
                                    or before.st_nlink != 1
                                    or before.st_size < 16
                                    or before.st_size % 16
                                ):
                                    continue
                                ciphertext_prefix = os.read(descriptor, 16)
                                after = os.fstat(descriptor)
                                path_after = os.stat(
                                    child.name,
                                    dir_fd=prefix_descriptor,
                                    follow_symlinks=False,
                                )
                            finally:
                                os.close(descriptor)
                        except OSError:
                            continue
                        identity_before = (
                            before.st_dev,
                            before.st_ino,
                            before.st_size,
                            before.st_mtime_ns,
                        )
                        identity_after = (
                            after.st_dev,
                            after.st_ino,
                            after.st_size,
                            after.st_mtime_ns,
                        )
                        path_identity = (
                            path_after.st_dev,
                            path_after.st_ino,
                            path_after.st_size,
                            path_after.st_mtime_ns,
                        )
                        if (
                            identity_before != identity_after
                            or identity_after != path_identity
                        ):
                            continue
                        if len(ciphertext_prefix) == 16:
                            yield ciphertext_prefix
            except OSError:
                continue
            finally:
                os.close(prefix_descriptor)


def _validated_candidate_key(source_root: Path, candidates: set[bytes]) -> bytes | None:
    remaining = {value for value in candidates if len(value) == 16}
    validated: set[bytes] = set()
    for prefix in _sticker_ciphertext_prefixes(source_root):
        for candidate in tuple(remaining):
            plaintext_prefix = decrypt_wechat_sticker_prefix(prefix, candidate)
            if is_wechat_sticker_image(plaintext_prefix):
                validated.add(candidate)
                remaining.remove(candidate)
        if len(validated) > 1 or not remaining:
            break
    return next(iter(validated)) if len(validated) == 1 else None


def load_sticker_decoder_key(
    settings: MacOSWeChatSettings,
    *,
    app_path: Path = DEFAULT_WECHAT_APP,
) -> bytes | None:
    """Derive the selected account's FileXorKey without persisting key material.

    The supported WeChat profile supplies only a binary offset. The MMKV key,
    account fields, derived FileXorKey, and decrypted validation prefixes remain
    in process memory and are never written to config, receipts, or logs.
    """

    try:
        if settings.architecture != "arm64":
            raise _StickerKeyUnavailable("unsupported source architecture")
        key_offset = _PROFILE_MMKV_KEY_OFFSETS[settings.profile_id]
        binary = app_path / "Contents" / "Resources" / "wechat.dylib"
        mmkv_key = _read_arm64_static_bytes(binary, offset=key_offset, size=16)
        if not all(0x20 <= value < 0x7F for value in mmkv_key):
            raise _StickerKeyUnavailable("invalid MMKV key material")
        global_config = (
            settings.source_root.parents[1]
            / "all_users"
            / "config"
            / "global_config"
        )
        values = _load_mmkv_values(global_config, mmkv_key)
        uin = _decode_uint64(values[b"mmkv_key_latest_login_uin"])
        names = {
            _decode_string(values[key])
            for key in (
                b"mmkv_key_latest_login_username",
                b"mmkv_key_user_name",
            )
            if key in values
        }
        if not names:
            raise _StickerKeyUnavailable("account name unavailable")
        candidates = {
            derive_wechat_sticker_key(str(uin), name)
            for name in names
        }
        return _validated_candidate_key(settings.source_root, candidates)
    except (KeyError, IndexError, OSError, _StickerKeyUnavailable):
        return None

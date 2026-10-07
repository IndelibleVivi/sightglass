from __future__ import annotations

import hashlib
import hmac
import importlib
import json
import os
import re
import secrets
import stat
import struct
from pathlib import Path
from typing import Any

PAGE_SIZE = 4096
IMAGE_DECODER_KEY_BYTES = 16
MAX_IMAGE_KEY_TEXT_BYTES = 4096
REQUIRED_DATABASE = re.compile(
    r"^(?:contact/contact\.db|session/session\.db|message/(?:biz_)?message_\d+\.db)$"
)
# Optional auxiliary databases carry resource mapping or payload evidence. They are
# not required for a complete message source and are enrolled only when the operator
# supplies a key that verifies against the current page 1 of that exact database.
AUXILIARY_DATABASE = re.compile(
    r"^(?:message/message_resource\.db|hardlink/hardlink\.db|"
    r"message/media_\d+\.db)$"
)


def verify_page1(key: bytes, page: bytes) -> bool:
    if len(key) != 32 or len(page) != PAGE_SIZE:
        return False
    salt = page[:16]
    mac_salt = bytes(value ^ 0x3A for value in salt)
    mac_key = hashlib.pbkdf2_hmac("sha512", key, mac_salt, 2, dklen=32)
    verifier = hmac.new(mac_key, page[16 : PAGE_SIZE - 80 + 16], hashlib.sha512)
    verifier.update(struct.pack("<I", 1))
    return hmac.compare_digest(verifier.digest(), page[PAGE_SIZE - 64 :])


def _source_databases(source_root: Path) -> tuple[str, ...]:
    values: list[str] = []
    for path in source_root.rglob("*.db"):
        if path.is_symlink() or not path.is_file():
            continue
        relative = path.relative_to(source_root).as_posix().lower()
        if REQUIRED_DATABASE.fullmatch(relative):
            values.append(relative)
    return tuple(sorted(values))


def _read_key_file(key_file: Path) -> bytes:
    maximum = 4 * 1024 * 1024
    try:
        descriptor = os.open(key_file, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise RuntimeError("key import file must be a private regular file") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_uid != os.getuid():
            raise RuntimeError("key import file must be a private regular file")
        if before.st_size > maximum:
            raise RuntimeError("key import file is too large")
        chunks: list[bytes] = []
        size = 0
        while size <= maximum:
            chunk = os.read(descriptor, min(1024 * 1024, maximum + 1 - size))
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
        after = os.fstat(descriptor)
        path_after = os.stat(key_file, follow_symlinks=False)
    except OSError as exc:
        raise RuntimeError("key import file changed during import") from exc
    finally:
        os.close(descriptor)
    data = b"".join(chunks)
    if len(data) > maximum:
        raise RuntimeError("key import file is too large")
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    path_identity = (
        path_after.st_dev,
        path_after.st_ino,
        path_after.st_size,
        path_after.st_mtime_ns,
    )
    if before_identity != after_identity or after_identity != path_identity:
        raise RuntimeError("key import file changed during import")
    return data


def _page1_matches(source_root: Path, relative: str, key: str) -> bool:
    path = source_root / relative
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError:
        return False
    try:
        page = os.read(descriptor, PAGE_SIZE)
    except OSError:
        return False
    finally:
        os.close(descriptor)
    return verify_page1(bytes.fromhex(key), page)


def verify_key_map(source_root: Path, value: dict[str, Any]) -> dict[str, dict[str, str]]:
    if not isinstance(value, dict):
        raise RuntimeError("native source key map is invalid")
    candidates: dict[str, str] = {}
    for name, item in value.items():
        normalized = str(name).replace("\\", "/").lower()
        key = str(item.get("enc_key") or "") if isinstance(item, dict) else ""
        if (
            REQUIRED_DATABASE.fullmatch(normalized) or AUXILIARY_DATABASE.fullmatch(normalized)
        ) and re.fullmatch(r"[0-9a-fA-F]{64}", key):
            candidates[normalized] = key.lower()
    expected = _source_databases(source_root)
    if not expected:
        raise RuntimeError("no supported WeChat databases were discovered")
    verified: dict[str, dict[str, str]] = {}
    for relative in expected:
        candidate = candidates.get(relative)
        if candidate is None:
            continue
        if _page1_matches(source_root, relative, candidate):
            verified[relative] = {"enc_key": candidate}
    if set(verified) != set(expected):
        raise RuntimeError("verified WeChat key set is incomplete")
    for relative, candidate in sorted(candidates.items()):
        if AUXILIARY_DATABASE.fullmatch(relative) and _page1_matches(
            source_root, relative, candidate
        ):
            verified[relative] = {"enc_key": candidate}
    return verified


def import_verified_key_file(source_root: Path, key_file: Path) -> dict[str, dict[str, str]]:
    try:
        value: Any = json.loads(_read_key_file(key_file))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("key import file is invalid") from exc
    if not isinstance(value, dict):
        raise RuntimeError("key import file is invalid")
    return verify_key_map(source_root, value)


def new_source_account_binding_id() -> str:
    return "wxbind_" + secrets.token_urlsafe(24)


def image_decoder_keychain_account(binding_id: str) -> str:
    """Account-bound Keychain identity for this installation's V2 image decoder key.

    Current bindings can be used directly. Existing v1 installations retain their
    account identity as ``legacy-<source_account_key>``; hash that private legacy
    value into a stable Keychain scope instead of forcing an identity migration or
    exposing it in the Keychain account name.
    """
    if re.fullmatch(r"wxbind_[A-Za-z0-9_-]{24,}", binding_id):
        scope = binding_id
    elif binding_id.startswith("legacy-") and len(binding_id) > len("legacy-"):
        scope = "wxlegacy_" + hashlib.sha256(binding_id.encode("utf-8")).hexdigest()
    else:
        raise RuntimeError("native source account binding is invalid")
    return f"source.{scope}.image-decoder-key"


def parse_image_decoder_key(value: bytes | str) -> bytes:
    """Strict V2 image decoder key: exactly 32 hexadecimal characters (16 bytes).

    The transient hexadecimal text and the intermediate buffer are overwritten once
    the sixteen retained key bytes are derived.
    """
    if isinstance(value, bytes):
        try:
            text = value.decode("ascii")
        except UnicodeDecodeError as exc:
            raise RuntimeError("image decoder key must be ASCII hexadecimal") from exc
    else:
        text = value
    text = text.strip()
    if text.startswith("{"):
        try:
            material = json.loads(text)
            text = material["aes_key"]
            if not isinstance(text, str):
                raise ValueError
        except (ValueError, KeyError, TypeError) as exc:
            raise RuntimeError("invalid image decoder material") from exc
    if len(text) != IMAGE_DECODER_KEY_BYTES * 2 or re.fullmatch(r"[0-9a-fA-F]+", text) is None:
        raise RuntimeError("image decoder key must be exactly 32 hexadecimal characters")
    raw = bytearray(bytes.fromhex(text))
    try:
        return bytes(raw)
    finally:
        for index in range(len(raw)):
            raw[index] = 0


def parse_image_xor_key(value: str) -> int | None:
    if not value.strip().startswith("{"):
        return None
    try:
        material = json.loads(value)
        encoded = material.get("xor_key")
        if not isinstance(encoded, str) or re.fullmatch(r"[0-9a-fA-F]{2}", encoded) is None:
            raise ValueError
        return int(encoded, 16)
    except (ValueError, TypeError, AttributeError) as exc:
        raise RuntimeError("invalid image decoder material") from exc


def read_image_xor_key_file(path: Path) -> bytes:
    metadata = os.stat(path, follow_symlinks=False)
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RuntimeError("image decoder material must be owner-private")
    data = _read_key_file(path).strip()
    if re.fullmatch(rb"[0-9a-fA-F]{2}", data) is None:
        raise RuntimeError("image XOR material must contain two hexadecimal characters")
    return bytes.fromhex(data.decode("ascii"))


def read_image_decoder_key_file(key_file: Path) -> bytes:
    """Read one owner-only, single-link, bounded, no-follow image key file.

    Stricter than the database key map: an image decoder key file must not be
    group or world readable either.
    """
    try:
        metadata = os.stat(key_file, follow_symlinks=False)
    except OSError as exc:
        raise RuntimeError("image decoder key file must be a private regular file") from exc
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RuntimeError("image decoder key file must not be group or world readable")
    return parse_image_decoder_key(_read_key_file(key_file))


def read_image_decoder_key_stream(stream: Any) -> bytes:
    """Read a bounded image key from a non-argv stream such as stdin."""
    data = stream.read(MAX_IMAGE_KEY_TEXT_BYTES + 1)
    if isinstance(data, str):
        data = data.encode("utf-8")
    if not isinstance(data, bytes) or len(data) > MAX_IMAGE_KEY_TEXT_BYTES:
        raise RuntimeError("image decoder key input is not a bounded byte stream")
    return parse_image_decoder_key(data)


def source_account_key(binding_id: str) -> str:
    if not re.fullmatch(r"wxbind_[A-Za-z0-9_-]{24,}", binding_id):
        raise RuntimeError("native source account binding is invalid")
    return hashlib.sha256(f"sightglass-macos-wechat-account-v2\0{binding_id}".encode()).hexdigest()


def write_keychain_secret(account: str, value: str) -> None:
    Security: Any = importlib.import_module("Security")

    query = {
        Security.kSecClass: Security.kSecClassGenericPassword,
        Security.kSecAttrService: "com.indeliblevivi.sightglass",
        Security.kSecAttrAccount: account,
    }
    payload = value.encode("utf-8")
    status, _existing = Security.SecItemCopyMatching(
        {
            **query,
            Security.kSecReturnData: True,
            Security.kSecMatchLimit: Security.kSecMatchLimitOne,
        },
        None,
    )
    if int(status) == int(Security.errSecSuccess):
        updated = Security.SecItemUpdate(query, {Security.kSecValueData: payload})
        if int(updated) != int(Security.errSecSuccess):
            raise RuntimeError("could not update native source keys in Keychain")
        return
    if int(status) != int(Security.errSecItemNotFound):
        raise RuntimeError("could not inspect native source keys in Keychain")
    added, _result = Security.SecItemAdd({**query, Security.kSecValueData: payload}, None)
    if int(added) != int(Security.errSecSuccess):
        raise RuntimeError("could not store native source keys in Keychain")


def read_keychain_secret(account: str) -> str:
    Security: Any = importlib.import_module("Security")

    status, data = Security.SecItemCopyMatching(
        {
            Security.kSecClass: Security.kSecClassGenericPassword,
            Security.kSecAttrService: "com.indeliblevivi.sightglass",
            Security.kSecAttrAccount: account,
            Security.kSecReturnData: True,
            Security.kSecMatchLimit: Security.kSecMatchLimitOne,
        },
        None,
    )
    if int(status) != int(Security.errSecSuccess) or data is None:
        raise RuntimeError("native source keys are unavailable in Keychain")
    return bytes(data).decode("utf-8")


def delete_keychain_secret(account: str) -> None:
    Security: Any = importlib.import_module("Security")

    status, _result = Security.SecItemDelete(
        {
            Security.kSecClass: Security.kSecClassGenericPassword,
            Security.kSecAttrService: "com.indeliblevivi.sightglass",
            Security.kSecAttrAccount: account,
        }
    )
    if int(status) not in {
        int(Security.errSecSuccess),
        int(Security.errSecItemNotFound),
    }:
        raise RuntimeError("could not remove the native source image key from Keychain")


def encode_key_map(keys: dict[str, dict[str, str]]) -> str:
    return json.dumps(keys, sort_keys=True, separators=(",", ":"))


def decode_key_map(value: str) -> dict[str, dict[str, str]]:
    parsed: Any = json.loads(value)
    if not isinstance(parsed, dict):
        raise RuntimeError("native source key map is invalid")
    result: dict[str, dict[str, str]] = {}
    for name, item in parsed.items():
        key = str(item.get("enc_key") or "") if isinstance(item, dict) else ""
        if not (
            REQUIRED_DATABASE.fullmatch(str(name)) or AUXILIARY_DATABASE.fullmatch(str(name))
        ) or not re.fullmatch(r"[0-9a-f]{64}", key):
            raise RuntimeError("native source key map is invalid")
        result[str(name)] = {"enc_key": key}
    return result

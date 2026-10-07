from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import stat
from pathlib import Path
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError


def opaque_id(prefix: str, *parts: object) -> str:
    basis = "\0".join(["sightglass-id-v1", *[str(part) for part in parts]])
    digest = hashlib.sha256(basis.encode("utf-8")).hexdigest()[:32]
    return f"{prefix}_{digest}"


class SignedTokenCodec:
    """Small HMAC codec for opaque anchors and reader-scoped cursors."""

    def __init__(self, secret: bytes) -> None:
        if len(secret) < 16:
            raise ValueError("token secret must be at least 16 bytes")
        self._secret = bytes(secret)

    @staticmethod
    def _encode_bytes(value: bytes) -> str:
        return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")

    @staticmethod
    def _decode_bytes(value: str) -> bytes:
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))

    def encode(self, payload: dict[str, Any]) -> str:
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        signature = hmac.new(self._secret, body, hashlib.sha256).digest()
        return f"{self._encode_bytes(body)}.{self._encode_bytes(signature)}"

    def decode(self, token: str) -> dict[str, Any]:
        try:
            body_text, signature_text = token.split(".", 1)
            body = self._decode_bytes(body_text)
            signature = self._decode_bytes(signature_text)
            expected = hmac.new(self._secret, body, hashlib.sha256).digest()
            if not hmac.compare_digest(signature, expected):
                raise ValueError("signature mismatch")
            value = json.loads(body)
            if not isinstance(value, dict):
                raise ValueError("payload is not an object")
            return value
        except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
            raise SightglassError(ErrorCode.CURSOR_INVALID) from exc


def load_or_create_token_secret(window_db_path: str | os.PathLike[str]) -> bytes:
    """Load a private per-install token secret next to ``window.db``.

    The parent directory is already required to be mode 0700 by ``WindowDB``.
    Existing secrets fail closed if they are symlinks, non-regular files, too
    short, or readable by group/other.
    """

    path = Path(window_db_path).expanduser().resolve().with_name("token-secret")
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, os.O_RDONLY | nofollow)
    except FileNotFoundError:
        value = secrets.token_bytes(32)
        try:
            descriptor = os.open(
                path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | nofollow,
                0o600,
            )
        except FileExistsError:
            return load_or_create_token_secret(window_db_path)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        return value
    with os.fdopen(descriptor, "rb", closefd=True) as handle:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise RuntimeError("token secret must be a private regular file (mode 0600)")
        value = handle.read(4096)
    if len(value) < 32:
        raise RuntimeError("token secret is invalid")
    return value

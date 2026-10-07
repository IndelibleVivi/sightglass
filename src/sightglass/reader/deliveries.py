from __future__ import annotations

import hashlib
import json
import os
import stat
from contextlib import nullcontext
from pathlib import Path
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.storage import StorageBudget


class DeliveryPayloadStore:
    """Private immutable JSON spool used for byte-stable pending replay."""

    def __init__(
        self, window_db_path: str | os.PathLike[str], *, storage: StorageBudget | None = None,
    ) -> None:
        parent = Path(window_db_path).expanduser().resolve().parent
        self.root = parent / "deliveries"
        self.storage = storage
        if self.root.is_symlink():
            raise RuntimeError("delivery spool directory cannot be a symlink")
        self.root.mkdir(mode=0o700, exist_ok=True)
        if self.root.is_symlink() or stat.S_IMODE(self.root.stat().st_mode) & 0o077:
            raise RuntimeError("delivery spool directory must be private (mode 0700)")

    @staticmethod
    def serialize(payload: dict[str, Any]) -> bytes:
        return json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    def write(self, delivery_id: str, payload: dict[str, Any]) -> tuple[str, str]:
        body = self.serialize(payload)
        digest = hashlib.sha256(body).hexdigest()
        path = self.root / f"{delivery_id}.json"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        reservation = self.storage.reserve(len(body) + 4096) if self.storage else nullcontext()
        with reservation:
            try:
                descriptor = os.open(path, flags, 0o600)
            except FileExistsError as exc:
                raise SightglassError(ErrorCode.INTERNAL_ERROR) from exc
            try:
                with os.fdopen(descriptor, "wb", closefd=True) as handle:
                    handle.write(body)
                    handle.flush()
                    os.fsync(handle.fileno())
            finally:
                if self.storage is not None:
                    self.storage.track(path)
        return str(path), digest

    def read(self, payload_ref: str, expected_digest: str) -> dict[str, Any]:
        path = Path(payload_ref)
        try:
            if path.parent != self.root or path.suffix != ".json":
                raise ValueError("payload reference is outside the private spool")
            descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            with os.fdopen(descriptor, "rb", closefd=True) as handle:
                metadata = os.fstat(descriptor)
                if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
                    raise ValueError("payload is not a private regular file")
                body = handle.read()
            if hashlib.sha256(body).hexdigest() != expected_digest:
                raise ValueError("payload digest mismatch")
            value = json.loads(body)
            if not isinstance(value, dict):
                raise ValueError("payload must be an object")
            return value
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise SightglassError(ErrorCode.INTERNAL_ERROR) from exc

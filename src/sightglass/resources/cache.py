from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from contextlib import nullcontext
from pathlib import Path
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.storage import StorageBudget

from .types import CachedObject


class ResourceObjectStore:
    """Private content-addressed store next to ``window.db``."""

    def __init__(
        self, window_db_path: str | os.PathLike[str], *, storage: StorageBudget | None = None,
    ) -> None:
        parent = Path(window_db_path).expanduser().resolve().parent
        self.root = parent / "resource-cache"
        self.objects = self.root / "objects"
        self.tmp = self.root / "tmp"
        self.storage = storage
        for directory in (self.root, self.objects, self.tmp):
            if directory.is_symlink():
                raise RuntimeError("resource cache directories cannot be symlinks")
            directory.mkdir(mode=0o700, exist_ok=True)
            metadata = directory.stat()
            if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
                raise RuntimeError("resource cache directories must be private (mode 0700)")

    @staticmethod
    def digest(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def put(
        self, data: bytes, *, mime_type: str, origin: str, maintenance: bool = False,
    ) -> tuple[CachedObject, str]:
        digest = self.digest(data)
        destination = self.objects / digest
        if not destination.exists():
            reservation = (
                self.storage.reserve(len(data) + 4096, maintenance=maintenance)
                if self.storage else nullcontext()
            )
            with reservation:
                descriptor, temporary_name = tempfile.mkstemp(prefix="object-", dir=self.tmp)
                temporary = Path(temporary_name)
                try:
                    os.fchmod(descriptor, 0o600)
                    with os.fdopen(descriptor, "wb", closefd=True) as handle:
                        handle.write(data)
                        handle.flush()
                        os.fsync(handle.fileno())
                    try:
                        os.link(temporary, destination, follow_symlinks=False)
                    except FileExistsError:
                        pass
                finally:
                    temporary.unlink(missing_ok=True)
                    if self.storage is not None:
                        self.storage.track(destination, temporary)
        value = self.read_path(
            destination,
            expected_digest=digest,
            expected_size=len(data),
            mime_type=mime_type,
            origin=origin,
        )
        return value, str(destination)

    def read_binding(self, row: Any) -> CachedObject:
        return self.read_path(
            Path(str(row["local_path_internal"])),
            expected_digest=str(row["object_digest"]),
            expected_size=int(row["byte_size"]),
            mime_type=str(row["mime_type"]),
            origin=str(row["origin"]),
        )

    def read_path(
        self,
        path: Path,
        *,
        expected_digest: str,
        expected_size: int,
        mime_type: str,
        origin: str,
    ) -> CachedObject:
        if path.parent != self.objects or path.name != expected_digest:
            raise SightglassError(ErrorCode.RESOURCE_BLOCKED)
        try:
            descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            with os.fdopen(descriptor, "rb", closefd=True) as handle:
                metadata = os.fstat(handle.fileno())
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_nlink != 1
                    or stat.S_IMODE(metadata.st_mode) & 0o077
                    or metadata.st_size != expected_size
                ):
                    raise SightglassError(ErrorCode.RESOURCE_BLOCKED)
                data = handle.read(expected_size + 1)
        except SightglassError:
            raise
        except OSError as exc:
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE) from exc
        if len(data) != expected_size or self.digest(data) != expected_digest:
            raise SightglassError(ErrorCode.RESOURCE_BLOCKED)
        return CachedObject(expected_digest, mime_type, data, origin)

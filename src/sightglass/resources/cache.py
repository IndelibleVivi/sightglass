from __future__ import annotations

import fcntl
import hashlib
import os
import re
import stat
import tempfile
from collections.abc import Callable
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

    def legacy_links(
        self, binding: Callable[[str], tuple[str, int] | None], *,
        digest: str | None = None, expect: str | None = None,
    ) -> dict[str, Any]:
        """Explicit stopped-owner inventory/repair; never called by startup or put.

        Only a registered object with exactly one internal old publication alias is
        recoverable. The caller holds the runtime lock and a frozen DB read view.
        ``expect`` binds apply to the exact previewed inode and temporary name.
        """
        if digest is not None and re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ValueError("invalid object digest")
        if expect is not None and digest is None:
            raise ValueError("repair requires one previewed digest")
        locks: list[int] = []
        try:
            for directory in (self.root, self.objects, self.tmp):
                fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                locks.append(fd)
                metadata = os.fstat(fd)
                if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
                    raise RuntimeError("unsafe resource cache directory")
                fcntl.flock(fd, fcntl.LOCK_EX)
            aliases: dict[tuple[int, int], list[Path]] = {}
            for path in self.tmp.iterdir():
                metadata = path.lstat()
                aliases.setdefault((metadata.st_dev, metadata.st_ino), []).append(path)
            candidates = [self.objects / digest] if digest else sorted(self.objects.iterdir())
            rows: list[dict[str, Any]] = []
            for path in candidates:
                metadata = path.lstat()
                if digest is None and stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1:
                    continue
                row: dict[str, Any] = {"digest": path.name, "status": "blocked"}
                rows.append(row)
                peers = aliases.get((metadata.st_dev, metadata.st_ino), [])
                if (
                    re.fullmatch(r"[0-9a-f]{64}", path.name) is None
                    or not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_uid != os.getuid()
                    or stat.S_IMODE(metadata.st_mode) & 0o077
                    or metadata.st_nlink != 2 or len(peers) != 1
                    or re.fullmatch(r"object-[A-Za-z0-9_-]+", peers[0].name) is None
                ):
                    row["reason"] = "unproved_internal_pair"
                    continue
                temporary = peers[0]
                registered = binding(path.name)
                if registered != (str(path), metadata.st_size):
                    row["reason"] = "object_binding_mismatch"
                    continue
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                try:
                    before = os.fstat(fd)
                    hasher = hashlib.sha256()
                    with os.fdopen(os.dup(fd), "rb") as handle:
                        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                            hasher.update(chunk)
                    # Recheck inode, size and content-change stamps after streaming.
                    def identity(value: os.stat_result) -> tuple[int, ...]:
                        return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns,
                                value.st_ctime_ns, value.st_nlink, value.st_uid, value.st_mode)
                    if (
                        identity(metadata) != identity(before)
                        or identity(before) != identity(os.fstat(fd))
                        or identity(before) != identity(path.lstat())
                        or identity(before) != identity(temporary.lstat())
                        or hasher.hexdigest() != path.name
                    ):
                        row["reason"] = "object_proof_mismatch"
                        continue
                    token = ":".join(map(str, identity(before))) + ":" + temporary.name
                    row.update(status="repairable", byte_size=before.st_size, expect=token)
                    if expect is not None:
                        if token != expect:
                            raise RuntimeError("legacy link preview changed; preview again")
                        temporary.unlink()
                        os.fsync(locks[2])
                        if os.fstat(fd).st_nlink != 1:
                            raise RuntimeError("legacy link repair changed unexpectedly")
                        row["status"] = "repaired"
                        if self.storage is not None:
                            self.storage.track(path, temporary)
                finally:
                    os.close(fd)
            if expect is not None and not any(row["status"] == "repaired" for row in rows):
                raise RuntimeError("legacy link repair proof rejected")
            return {"schema": "sightglass.legacy-cas-links.v1", "objects": rows}
        finally:
            for fd in reversed(locks):
                os.close(fd)

    @staticmethod
    def digest(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def put(
        self, data: bytes, *, mime_type: str, origin: str, maintenance: bool = False,
    ) -> tuple[CachedObject, str]:
        digest = self.digest(data)
        destination = self.objects / digest
        # A directory lock coordinates threads, processes and store instances without
        # persistent per-digest lock files. Rename never publishes a two-link inode.
        lock = os.open(self.objects, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX)
            return self._put_locked(data, digest, destination, mime_type, origin, maintenance)
        finally:
            os.close(lock)

    def _put_locked(
        self, data: bytes, digest: str, destination: Path, mime_type: str,
        origin: str, maintenance: bool,
    ) -> tuple[CachedObject, str]:
        if not os.path.lexists(destination):
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
                    os.rename(temporary, destination)
                    # Persist both the destination entry and removal from the source
                    # directory: this rename crosses two directory namespaces.
                    for directory in (self.objects, self.tmp):
                        directory_fd = os.open(
                            directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                        )
                        try:
                            os.fsync(directory_fd)
                        finally:
                            os.close(directory_fd)
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

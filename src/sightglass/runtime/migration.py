"""Frozen, resumable state transfer without a second source-host database copy.

Only explicit installation-owned regular files are accepted. This module transfers
bytes into an invisible destination namespace; it does not stop services, choose a
writer, rewrite source locators, activate a release, or retire the original files.
The operator must hold the stopped installation lock for the complete transfer.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import stat
import struct
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO

TRANSFER_SCHEMA = "sightglass.frozen-transfer.v1"
CHUNK_BYTES = 1024 * 1024
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
MAX_FILES = 10_000
MIN_FREE_BYTES = 2 * 1024**3
_CONTROL_FILES = {"transfer-manifest.json", "transfer-complete.json", ".transfer.lock"}
_DIGEST = re.compile(r"[a-f0-9]{64}\Z")


def _relative(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or path.is_absolute()
        or ".." in path.parts
        or "\\" in value
        or "\x00" in value
        or path.as_posix() != value
        or value.endswith(".sgpartial")
        or path.parts[0] in _CONTROL_FILES
    ):
        raise RuntimeError("unsafe transfer member")
    return value


def _private_directory(path: Path, *, create: bool = False) -> None:
    if create:
        path.mkdir(mode=0o700, exist_ok=True)
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise RuntimeError("transfer directory must be owner-private and not a symlink")


def _member(root: Path, relative: str, *, create: bool = False) -> Path:
    relative = _relative(relative)
    _private_directory(root)
    parent = root
    for part in PurePosixPath(relative).parts[:-1]:
        parent /= part
        _private_directory(parent, create=create)
    return root / relative


def _regular(path: Path) -> os.stat_result:
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise RuntimeError("transfer input must be a private single-linked regular file")
    return metadata


def _revision(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
        stat.S_IMODE(metadata.st_mode),
    )


def _open_read(path: Path) -> BinaryIO:
    before = _regular(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    if _revision(os.fstat(descriptor)) != _revision(before):
        os.close(descriptor)
        raise RuntimeError("transfer input changed before opening")
    return os.fdopen(descriptor, "rb")


def _hash(path: Path) -> str:
    before = _revision(_regular(path))
    digest = hashlib.sha256()
    with _open_read(path) as handle:
        while chunk := handle.read(CHUNK_BYTES):
            digest.update(chunk)
    if _revision(_regular(path)) != before:
        raise RuntimeError("destination changed during verification")
    return digest.hexdigest()


@dataclass(frozen=True)
class FrozenFile:
    relative: str
    size: int
    digest: str
    chunks: tuple[str, ...]
    revision: tuple[int, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "relative": self.relative,
            "size": self.size,
            "digest": self.digest,
            "chunks": list(self.chunks),
            "revision": list(self.revision),
        }


@dataclass(frozen=True)
class FrozenTransfer:
    files: tuple[FrozenFile, ...]
    logical_cut: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": TRANSFER_SCHEMA,
            "chunk_bytes": CHUNK_BYTES,
            "files": [item.as_dict() for item in self.files],
            "logical_cut": self.logical_cut,
        }

    @classmethod
    def parse(cls, value: Any) -> FrozenTransfer:
        if (
            not isinstance(value, dict)
            or value.get("schema") != TRANSFER_SCHEMA
            or set(value) != {"schema", "chunk_bytes", "files", "logical_cut"}
            or value.get("chunk_bytes") != CHUNK_BYTES
            or not isinstance(value.get("files"), list)
            or not 0 < len(value["files"]) <= MAX_FILES
        ):
            raise RuntimeError("invalid frozen transfer manifest")
        cut = value["logical_cut"]
        if cut is not None and (
            not isinstance(cut, dict)
            or cut.get("schema") != "sightglass.installation-cut.v1"
            or set(cut)
            != {
                "schema",
                "window_schema",
                "window_member",
                "owner_generation",
                "mode",
                "committed_state",
                "remote",
            }
        ):
            raise RuntimeError("invalid logical installation cut")
        if cut is not None:
            _relative(cut.get("window_member", ""))
        result = []
        seen: set[str] = set()
        for item in value["files"]:
            if not isinstance(item, dict):
                raise RuntimeError("invalid transfer member")
            relative = _relative(item.get("relative", ""))
            size, digest, chunks, revision = (
                item.get("size"),
                item.get("digest"),
                item.get("chunks"),
                item.get("revision"),
            )
            if (
                relative in seen
                or type(size) is not int
                or size < 0
                or not isinstance(digest, str)
                or not _DIGEST.fullmatch(digest)
                or not isinstance(chunks, list)
                or len(chunks) != (size + CHUNK_BYTES - 1) // CHUNK_BYTES
                or any(not isinstance(part, str) or not _DIGEST.fullmatch(part) for part in chunks)
                or not isinstance(revision, list)
                or len(revision) != 6
                or any(type(part) is not int for part in revision)
                or revision[-1] not in {0o600, 0o700}
            ):
                raise RuntimeError("invalid transfer member evidence")
            seen.add(relative)
            result.append(FrozenFile(relative, size, digest, tuple(chunks), tuple(revision)))
        names = sorted(seen)
        if any(right.startswith(left + "/") for left, right in zip(names, names[1:])):
            raise RuntimeError("transfer members overlap")
        if cut is not None and cut["window_member"] not in seen:
            raise RuntimeError("installation cut omits its frozen WindowDB")
        return cls(tuple(result), cut)


def freeze_files(root: Path, members: tuple[str, ...]) -> FrozenTransfer:
    """Read one explicitly stopped file set; never copy its bodies locally."""
    if not members or len(set(members)) != len(members) or len(members) > MAX_FILES:
        raise RuntimeError("invalid frozen transfer file set")
    files = []
    for relative in sorted(members):
        path = _member(root, relative)
        before = _revision(_regular(path))
        digest = hashlib.sha256()
        chunks = []
        with _open_read(path) as handle:
            while chunk := handle.read(CHUNK_BYTES):
                digest.update(chunk)
                chunks.append(hashlib.sha256(chunk).hexdigest())
        if _revision(_regular(path)) != before:
            raise RuntimeError("input changed while freezing transfer")
        files.append(FrozenFile(relative, before[2], digest.hexdigest(), tuple(chunks), before))
    plan = FrozenTransfer(tuple(files))
    FrozenTransfer.parse(plan.as_dict())
    _encoded(plan.as_dict())
    return plan


def _encoded(value: Any) -> bytes:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    if len(payload) > MAX_MANIFEST_BYTES:
        raise RuntimeError("transfer manifest exceeds its bound")
    return payload


def _write_frame(handle: BinaryIO, value: Any) -> None:
    payload = _encoded(value)
    handle.write(struct.pack("!I", len(payload)))
    handle.write(payload)
    handle.flush()


def _read_exact(handle: BinaryIO, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = handle.read(size - len(data))
        if not chunk:
            raise RuntimeError("frozen transfer interrupted")
        data.extend(chunk)
    return bytes(data)


def _read_frame(handle: BinaryIO) -> Any:
    size = struct.unpack("!I", _read_exact(handle, 4))[0]
    if size > MAX_MANIFEST_BYTES:
        raise RuntimeError("transfer frame exceeds its bound")
    return json.loads(_read_exact(handle, size))


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _resume(root: Path, item: FrozenFile) -> int:
    final = _member(root, item.relative, create=True)
    if final.exists() or final.is_symlink():
        if _regular(final).st_size == item.size and _hash(final) == item.digest:
            return item.size
        raise RuntimeError("completed destination does not match frozen input")
    partial = final.with_name(final.name + ".sgpartial")
    if not partial.exists() and not partial.is_symlink():
        return 0
    _regular(partial)
    offset = 0
    with _open_read(partial) as handle:
        for expected in item.chunks:
            chunk = handle.read(min(CHUNK_BYTES, item.size - offset))
            if not chunk or hashlib.sha256(chunk).hexdigest() != expected:
                break
            offset += len(chunk)
    # Only this unpublished partial is repaired, never an active or completed file.
    descriptor = os.open(partial, os.O_WRONLY | os.O_NOFOLLOW)
    try:
        os.ftruncate(descriptor, offset)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return offset


def send_frozen(root: Path, plan: FrozenTransfer, incoming: BinaryIO, outgoing: BinaryIO) -> None:
    for item in plan.files:
        if _revision(_regular(_member(root, item.relative))) != item.revision:
            raise RuntimeError("frozen source changed before transfer")
    _write_frame(outgoing, plan.as_dict())
    offsets = _read_frame(incoming)
    if not isinstance(offsets, list) or len(offsets) != len(plan.files):
        raise RuntimeError("invalid transfer resumption")
    for item, offset in zip(plan.files, offsets, strict=True):
        if (
            type(offset) is not int
            or not 0 <= offset <= item.size
            or offset != item.size
            and offset % CHUNK_BYTES
        ):
            raise RuntimeError("invalid transfer resumption")
        path = _member(root, item.relative)
        if _revision(_regular(path)) != item.revision:
            raise RuntimeError("frozen source changed before member transfer")
        with _open_read(path) as handle:
            handle.seek(offset)
            while offset < item.size:
                chunk = handle.read(min(CHUNK_BYTES, item.size - offset))
                if hashlib.sha256(chunk).hexdigest() != item.chunks[offset // CHUNK_BYTES]:
                    raise RuntimeError("frozen source bytes changed")
                outgoing.write(chunk)
                offset += len(chunk)
            outgoing.flush()
        if _read_frame(incoming) != {"received": item.digest}:
            raise RuntimeError("destination verification failed")
    for item in plan.files:
        if _revision(_regular(_member(root, item.relative))) != item.revision:
            raise RuntimeError("frozen source changed during transfer")
    _write_frame(outgoing, {"source_stable": True})
    if _read_frame(incoming) != {"complete": True}:
        raise RuntimeError("destination transfer did not complete")


def _capacity(root: Path, growth: int, min_free_bytes: int) -> None:
    available = os.statvfs(root)
    if available.f_bavail * available.f_frsize < min_free_bytes + growth:
        raise RuntimeError("frozen transfer would violate the physical free floor")


def receive_frozen(
    root: Path,
    incoming: BinaryIO,
    outgoing: BinaryIO,
    *,
    min_free_bytes: int = MIN_FREE_BYTES,
) -> FrozenTransfer:
    """One receiver owns staging; a partial can never race another publication."""
    _private_directory(root, create=True)
    descriptor = os.open(root / ".transfer.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        _regular(root / ".transfer.lock")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("frozen destination already has a receiver") from exc
        return _receive_locked(root, incoming, outgoing, min_free_bytes=min_free_bytes)
    finally:
        os.close(descriptor)


def _receive_locked(
    root: Path,
    incoming: BinaryIO,
    outgoing: BinaryIO,
    *,
    min_free_bytes: int,
) -> FrozenTransfer:
    plan = FrozenTransfer.parse(_read_frame(incoming))
    manifest_path = root / "transfer-manifest.json"
    manifest = _encoded(plan.as_dict())
    if manifest_path.exists() or manifest_path.is_symlink():
        with _open_read(manifest_path) as handle:
            if handle.read(MAX_MANIFEST_BYTES + 1) != manifest:
                raise RuntimeError("destination belongs to another frozen input")
    else:
        descriptor = os.open(manifest_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(manifest)
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_directory(root)
    offsets = [_resume(root, item) for item in plan.files]
    remaining = sum(item.size - offset for item, offset in zip(plan.files, offsets, strict=True))
    _capacity(root, remaining, min_free_bytes)
    _write_frame(outgoing, offsets)
    for item, offset in zip(plan.files, offsets, strict=True):
        final = _member(root, item.relative)
        partial = final.with_name(final.name + ".sgpartial")
        if not final.exists():
            descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.seek(offset)
                while offset < item.size:
                    chunk = _read_exact(incoming, min(CHUNK_BYTES, item.size - offset))
                    if hashlib.sha256(chunk).hexdigest() != item.chunks[offset // CHUNK_BYTES]:
                        raise RuntimeError("transfer chunk digest mismatch")
                    _capacity(root, len(chunk), min_free_bytes)
                    handle.write(chunk)
                    offset += len(chunk)
                handle.flush()
                os.fchmod(handle.fileno(), item.revision[-1])
                os.fsync(handle.fileno())
            if _regular(partial).st_size != item.size or _hash(partial) != item.digest:
                raise RuntimeError("destination member digest mismatch")
            os.replace(partial, final)
            _fsync_directory(final.parent)
        _write_frame(outgoing, {"received": item.digest})
    if _read_frame(incoming) != {"source_stable": True}:
        raise RuntimeError("source did not confirm its frozen revision")
    if plan.logical_cut is not None:
        from .migration_cut import verify_committed_cut

        verify_committed_cut(_member(root, plan.logical_cut["window_member"]), plan.logical_cut)
    complete = root / "transfer-complete.json"
    completion = _encoded(
        {
            "schema": TRANSFER_SCHEMA,
            "files": len(plan.files),
            "bytes": sum(item.size for item in plan.files),
        }
    )
    if complete.exists() or complete.is_symlink():
        with _open_read(complete) as handle:
            if handle.read(MAX_MANIFEST_BYTES + 1) != completion:
                raise RuntimeError("invalid frozen completion evidence")
    else:
        descriptor = os.open(complete, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(completion)
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_directory(root)
    _write_frame(outgoing, {"complete": True})
    return plan


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Receive frozen installation state into staging")
    parser.add_argument("--destination", type=Path, required=True)
    arguments = parser.parse_args(argv)
    receive_frozen(arguments.destination, sys.stdin.buffer, sys.stdout.buffer)


if __name__ == "__main__":
    main()

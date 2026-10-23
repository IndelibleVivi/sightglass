"""Parent-owned Linux attempt directories with inherited, crash-safe leases."""

from __future__ import annotations

import fcntl
import json
import os
import re
import shutil
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

_NAME = re.compile(r"sightglass-whisper-[0-9a-f]{32}\Z")


def _directory(path: Path) -> None:
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise RuntimeError("unsafe voice workspace directory")


def _file(fd: int) -> None:
    metadata = os.fstat(fd)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise RuntimeError("unsafe voice workspace file")


def _remove(path: Path) -> None:
    # Validate before deleting anything. Never follow links, nor accept outside aliases.
    _directory(path)
    for directory, dirs, files in os.walk(path, followlinks=False):
        for name in dirs:
            _directory(Path(directory) / name)
        for name in files:
            fd = os.open(Path(directory) / name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                _file(fd)
            finally:
                os.close(fd)
    shutil.rmtree(path)


@contextmanager
def _root(root: Path) -> Iterator[None]:
    _directory(root.parent)
    root.mkdir(mode=0o700, exist_ok=True)
    _directory(root)
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def reap(root: Path, *, manager: str | None, stop: Callable[[str], None] | None) -> int:
    count = 0
    if root.parent.exists() or root.parent.is_symlink():
        _directory(root.parent)
    if not root.exists() and not root.is_symlink():
        return count
    with _root(root):
        for path in root.iterdir():
            if not _NAME.fullmatch(path.name):
                continue
            _directory(path)
            lease = path / "lease"
            if not lease.exists() and not lease.is_symlink():
                # A crash between mkdir and lease creation owns no processes/files.
                if not any(path.iterdir()):
                    path.rmdir()
                    count += 1
                continue
            fd = os.open(lease, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                _file(fd)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    continue
                raw = os.read(fd, 4097)
                if not raw:
                    # Lease creation precedes launch and its durable marker.
                    _remove(path)
                    count += 1
                    continue
                marker = json.loads(raw)
                if marker != {"manager": manager}:
                    raise RuntimeError("voice workspace manager mismatch")
                if manager is not None:
                    if stop is None:
                        raise RuntimeError("voice workspace scope cleanup unavailable")
                    stop(path.name + ".scope")
                _remove(path)
                count += 1
            finally:
                os.close(fd)
    return count


@contextmanager
def attempt(root: Path, name: str, *, manager: str | None) -> Iterator[tuple[Path, int]]:
    if not _NAME.fullmatch(name):
        raise RuntimeError("invalid voice workspace name")
    path = root / name
    with _root(root):
        path.mkdir(mode=0o700)
        fd = os.open(path / "lease", os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        os.write(fd, json.dumps({"manager": manager}).encode())
        os.fsync(fd)
    try:
        yield path, fd
    finally:
        # Cleanup is driven by reap only after runner confirms subtree termination.
        os.close(fd)


def validate_child(path: Path, lease_fd: int) -> None:
    _directory(path.parent)
    _directory(path)
    _file(lease_fd)
    metadata = (path / "lease").lstat()
    inherited = os.fstat(lease_fd)
    if (metadata.st_dev, metadata.st_ino) != (inherited.st_dev, inherited.st_ino):
        raise RuntimeError("voice workspace lease mismatch")


def inventory(root: Path) -> list[Path]:
    """Accounting paths only; do not follow untrusted directory aliases."""
    result: list[Path] = []
    if not root.exists():
        return result
    _directory(root)
    for directory, dirs, files in os.walk(root, followlinks=False):
        dirs[:] = [name for name in dirs if not (Path(directory) / name).is_symlink()]
        result.extend(Path(directory) / name for name in files)
    return result

"""Owner-private process lock shared by daemon startup and offline maintenance."""

from __future__ import annotations

import fcntl
import os
from pathlib import Path
from typing import TextIO


def acquire_runtime_lock(lock_path: Path) -> TextIO:
    handle = lock_path.open("a+", encoding="utf-8")
    os.chmod(lock_path, 0o600)
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise RuntimeError("another sightglassd process already owns this runtime") from exc
    return handle


def release_runtime_lock(handle: TextIO) -> None:
    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    handle.close()

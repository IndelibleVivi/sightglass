"""Accounted local storage and admission backpressure (not a filesystem quota).

Immutable object directories are inventoried once, then updated at their write/delete
sites. Database/WAL and mutable top-level files are stat'ed on every admission. Operator
status can reconcile the full inventory. Reservations cover concurrent admitted work;
SQLite growth is estimated, so a maintenance reserve remains outside the hard boundary.
"""

from __future__ import annotations

import os
import re
import stat
import tempfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError

MIB = 1024 * 1024
SQL_WRITE_RESERVE = MIB
STORAGE_EXPLAIN_DEFAULT_LIMIT = 500
STORAGE_EXPLAIN_MAX_LIMIT = 2_000


@dataclass(frozen=True)
class StorageSettings:
    soft_limit_bytes: int = 4 * 1024 * MIB
    hard_limit_bytes: int = 6 * 1024 * MIB
    min_free_bytes: int = 2 * 1024 * MIB
    maintenance_reserve_bytes: int = 256 * MIB

    def __post_init__(self) -> None:
        values = asdict(self)
        if any(type(value) is not int or value < 0 for value in values.values()):
            raise ValueError("storage limits must be non-negative integer bytes")
        if not 0 < self.soft_limit_bytes < self.hard_limit_bytes:
            raise ValueError("storage requires 0 < soft_limit_bytes < hard_limit_bytes")
        if self.maintenance_reserve_bytes < SQL_WRITE_RESERVE:
            raise ValueError("storage maintenance reserve must be at least 1 MiB")

    @classmethod
    def from_dict(cls, value: Any) -> StorageSettings:
        if not isinstance(value, dict):
            raise ValueError("storage settings must be an object")
        if set(value) - set(cls.__dataclass_fields__):
            raise ValueError("unknown storage setting")
        return cls(**value)

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


def _regular_file_usage(path: Path) -> tuple[os.stat_result, int, int, int] | None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(metadata.st_mode):
        return None
    logical = int(metadata.st_size)
    allocated = int(metadata.st_blocks) * 512
    # Account for both logical growth and filesystem allocation, including tiny files.
    return metadata, logical, allocated, max(logical, allocated)


def _free(path: Path) -> int:
    while not path.exists():
        path = path.parent
    value = os.statvfs(path)
    return int(value.f_bavail) * int(value.f_frsize or value.f_bsize)


class StorageLease:
    def __init__(self, budget: StorageBudget, amount: int, maintenance: bool) -> None:
        self.budget = budget
        self.amount = amount
        self.maintenance = maintenance

    def extend(self, amount: int) -> None:
        with self.budget._lock:
            self.budget._require(amount, maintenance=self.maintenance)
            self.amount += amount
            self.budget._reserved += amount

    def verify(self) -> None:
        with self.budget._lock:
            self.budget._require(0, maintenance=self.maintenance, exclude=self.amount)


class StorageBudget:
    def __init__(
        self,
        data_dir: Path,
        database_path: Path,
        settings: StorageSettings,
    ) -> None:
        self.data_dir = data_dir.resolve()
        self.database_path = database_path.resolve()
        roots = {self.data_dir, self.database_path.parent}
        self.roots = tuple(
            sorted(
                (root for root in roots if not any(root.is_relative_to(p) for p in roots - {root})),
                key=str,
            )
        )
        self.settings = settings
        self.temporary_root = data_dir.resolve() / "storage-tmp"
        self._lock = threading.RLock()
        self._files: dict[Path, int] = {}
        self._totals: dict[str, int] = {}
        self._reserved = 0
        self._mutable_paths: set[Path] = set()
        self._active_workspaces: set[Path] = set()
        self._migration_backup_pattern = re.compile(
            rf"^{re.escape(self.database_path.name)}\.v\d+\.backup(?:\.\d+)?"
            rf"(?:(?:-(?:journal|wal|shm))|(?:\.zst(?:\.json)?))?$"
        )
        self.reconcile()

    def reconcile(self) -> None:
        """Refresh owned files only; never follow symlinks into source/account data."""
        with self._lock:
            found: dict[Path, int] = {}
            for root in self.roots:
                for directory, dirs, files in os.walk(root, followlinks=False):
                    base = Path(directory)
                    dirs[:] = [
                        name
                        for name in dirs
                        if not (base / name).is_symlink()
                        and base / name not in self._active_workspaces
                    ]
                    for name in files:
                        path = base / name
                        usage = _regular_file_usage(path)
                        if usage is not None:
                            found[path] = usage[3]
            self._files = found
            self._mutable_paths = {path for path in found if path.parent in self.roots}
            self._totals = {}
            for path, size in found.items():
                category = self._component(path)
                self._totals[category] = self._totals.get(category, 0) + size

    def track(self, *paths: Path) -> None:
        with self._lock:
            for path in paths:
                path = path.absolute()
                usage = _regular_file_usage(path)
                size = usage[3] if usage is not None else 0
                category = self._component(path)
                self._totals[category] = (
                    self._totals.get(category, 0) + size - self._files.get(path, 0)
                )
                if usage is not None:
                    self._files[path] = size
                else:
                    self._files.pop(path, None)

    def _refresh_mutable(self) -> None:
        paths = [
            self.database_path.with_name(self.database_path.name + suffix)
            for suffix in ("", "-wal", "-shm", "-journal")
        ]
        for root in self.roots:
            if root.is_dir():
                paths.extend(path for path in root.iterdir() if not path.is_dir())
        self._mutable_paths.update(paths)
        paths.extend(self._mutable_paths)
        self.track(*paths)

    def _component(self, path: Path) -> str:
        if path == self.database_path:
            return "database"
        if path.name in {
            self.database_path.name + suffix for suffix in ("-wal", "-shm", "-journal")
        }:
            return "database_sidecars"
        if path.parent == self.database_path.parent and self._migration_backup_pattern.fullmatch(
            path.name
        ):
            return "migration_backups"
        if "deliveries" in path.parts:
            return "delivery_spool"
        if "semantic" in path.parts:
            return "semantic_sidecar"
        if "resource-cache" in path.parts:
            return "resource_objects_and_tmp"
        if "voice-work" in path.parts or "storage-tmp" in path.parts:
            return "staging"
        return "other_owned_files"

    def _snapshot(self) -> dict[str, Any]:
        self._refresh_mutable()
        components: dict[str, int] = dict.fromkeys(
            (
                "database",
                "database_sidecars",
                "migration_backups",
                "delivery_spool",
                "semantic_sidecar",
                "resource_objects_and_tmp",
                "staging",
                "other_owned_files",
            ),
            0,
        )
        components.update(self._totals)
        used = sum(components.values())
        free = min(_free(root) for root in self.roots)
        settings = self.settings
        hard = (
            used + self._reserved + SQL_WRITE_RESERVE > settings.hard_limit_bytes
            or free - self._reserved - SQL_WRITE_RESERVE
            < settings.min_free_bytes + settings.maintenance_reserve_bytes
        )
        soft = used + self._reserved >= settings.soft_limit_bytes
        return {
            "schema": "sightglass.storage-status.v1",
            "state": "hard_limit" if hard else "soft_limit" if soft else "ok",
            "accounted_bytes": used,
            "components": components,
            "reserved_bytes": self._reserved,
            "available_bytes": free,
            "remaining_budget_bytes": max(0, settings.hard_limit_bytes - used - self._reserved),
            "limits": settings.as_dict(),
            "background_growth_allowed": not hard and not soft,
            "admission_allowed": not hard,
            "accounting": "max_logical_or_allocated_per_file",
        }

    def status(self, *, reconcile: bool = False) -> dict[str, Any]:
        with self._lock:
            if reconcile:
                self.reconcile()
            return self._snapshot()

    def _display_path(self, path: Path) -> tuple[str, str]:
        if path.is_relative_to(self.data_dir):
            return "data", path.relative_to(self.data_dir).as_posix()
        return "database", path.relative_to(self.database_path.parent).as_posix()

    def _role(self, path: Path) -> tuple[str, str]:
        category = self._component(path)
        if path == self.database_path:
            return "primary_database", "retain"
        if category == "database_sidecars":
            suffix = path.name.removeprefix(self.database_path.name).removeprefix("-")
            return f"database_{suffix}", "managed_by_sqlite"
        if category == "migration_backups":
            return "migration_backup", "review_offline_only"
        if "deliveries" in path.parts:
            return "delivery_payload", "managed_by_delivery_store"
        if "resource-cache" in path.parts:
            if "objects" in path.parts:
                return "resource_object", "use_cache_cleanup"
            return "resource_temporary", "managed_temporary"
        if "voice-work" in path.parts:
            return "voice_staging", "managed_temporary"
        if "storage-tmp" in path.parts:
            return "processor_staging", "managed_temporary"
        if path.name == "config.json":
            return "runtime_config", "retain"
        if path.name == "storage-history.json":
            return "storage_history", "retain"
        if path.name == "config.v1.backup.json":
            return "config_backup", "review_offline_only"
        if path.name == "sightglassd.log":
            return "daemon_log", "review_retention"
        if path.name == "source.json" and "sources" in path.parts:
            return "source_settings", "retain"
        return "unknown_owned_file", "investigate"

    def explain(
        self,
        *,
        offset: int = 0,
        limit: int = STORAGE_EXPLAIN_DEFAULT_LIMIT,
        reconcile: bool = True,
    ) -> dict[str, Any]:
        """Return a bounded, content-free inventory of owned regular files."""

        if type(offset) is not int or offset < 0:
            raise ValueError("storage explain offset must be a non-negative integer")
        if type(limit) is not int or not 1 <= limit <= STORAGE_EXPLAIN_MAX_LIMIT:
            raise ValueError(
                f"storage explain limit must be between 1 and {STORAGE_EXPLAIN_MAX_LIMIT}"
            )
        with self._lock:
            if reconcile:
                self.reconcile()
            selected = sorted(self._files, key=lambda item: self._display_path(item))
            entries: list[dict[str, Any]] = []
            for path in selected[offset : offset + limit]:
                usage = _regular_file_usage(path)
                if usage is None:
                    continue
                metadata, logical, allocated, accounted = usage
                root, relative_path = self._display_path(path)
                role, safe_action = self._role(path)
                entries.append(
                    {
                        "root": root,
                        "relative_path": relative_path,
                        "category": self._component(path),
                        "logical_bytes": logical,
                        "allocated_bytes": allocated,
                        "accounted_bytes": accounted,
                        "mtime_utc": datetime.fromtimestamp(
                            metadata.st_mtime, tz=UTC
                        ).isoformat(timespec="seconds"),
                        "recognized_role": role,
                        "safe_action": safe_action,
                    }
                )
            page = entries
            next_offset = offset + min(limit, max(0, len(selected) - offset))
            return {
                "schema": "sightglass.storage-files.v1",
                "file_count": len(selected),
                "inventory_source": "reconciled" if reconcile else "tracked_files",
                "offset": offset,
                "limit": limit,
                "next_offset": next_offset if next_offset < len(selected) else None,
                "files": page,
                "accounting": "max_logical_or_allocated_per_file",
            }

    def _require(
        self,
        amount: int,
        *,
        maintenance: bool = False,
        background: bool = False,
        exclude: int = 0,
    ) -> None:
        snapshot = self._snapshot()
        settings = self.settings
        projected = snapshot["accounted_bytes"] + self._reserved - exclude + amount
        reserve = settings.maintenance_reserve_bytes
        ceiling = settings.hard_limit_bytes + (reserve if maintenance else 0)
        floor = settings.min_free_bytes + (0 if maintenance else reserve)
        reason = None
        if snapshot["available_bytes"] - self._reserved + exclude - amount < floor:
            reason = "filesystem_free_floor"
        elif projected > ceiling:
            reason = "hard_limit"
        elif background and projected > settings.soft_limit_bytes:
            reason = "soft_limit"
        if reason is not None:
            raise SightglassError(
                ErrorCode.STORAGE_PRESSURE,
                retryable=True,
                details={
                    "reason": reason,
                    "requested_bytes": amount,
                    "accounted_bytes": snapshot["accounted_bytes"],
                    "remaining_budget_bytes": snapshot["remaining_budget_bytes"],
                },
            )

    def require(self, amount: int = SQL_WRITE_RESERVE, *, background: bool = False) -> None:
        with self._lock:
            self._require(amount, background=background)

    @contextmanager
    def reserve(
        self,
        amount: int,
        *,
        maintenance: bool = False,
        background: bool = False,
    ) -> Iterator[StorageLease]:
        with self._lock:
            self._require(amount, maintenance=maintenance, background=background)
            lease = StorageLease(self, amount, maintenance)
            self._reserved += amount
        try:
            yield lease
        finally:
            with self._lock:
                self._reserved -= lease.amount
                self._refresh_mutable()


_CURRENT_STORAGE: ContextVar[StorageBudget | None] = ContextVar("sightglass_storage", default=None)


@contextmanager
def storage_scope(budget: StorageBudget | None) -> Iterator[None]:
    token = _CURRENT_STORAGE.set(budget)
    try:
        yield
    finally:
        _CURRENT_STORAGE.reset(token)


@contextmanager
def temporary_workspace(prefix: str, max_bytes: int) -> Iterator[str]:
    """Reserve bounded processor workspace on the owned volume, including failure residue."""
    budget = _CURRENT_STORAGE.get()
    if budget is None:
        with tempfile.TemporaryDirectory(prefix=prefix) as name:
            yield name
        return
    with budget.reserve(max_bytes):
        root = budget.temporary_root
        if root.is_symlink():
            raise RuntimeError("storage staging root cannot be a symlink")
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if stat.S_IMODE(root.stat().st_mode) & 0o077:
            raise RuntimeError("storage staging root must be private")
        temporary = tempfile.TemporaryDirectory(prefix=prefix, dir=root)
        with budget._lock:
            budget._active_workspaces.add(Path(temporary.name))
        try:
            with temporary as name:
                yield name
        finally:
            with budget._lock:
                budget._active_workspaces.discard(Path(temporary.name))
                for directory, dirs, files in os.walk(temporary.name, followlinks=False):
                    dirs[:] = [name for name in dirs if not (Path(directory) / name).is_symlink()]
                    budget.track(*(Path(directory) / name for name in files))

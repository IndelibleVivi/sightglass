"""Content-free daily storage history sidecar.

The daemon records one bounded, content-free snapshot per UTC day into a dedicated
owner-private atomic sidecar. ``storage explain`` reads those snapshots to report exact
7-day and 30-day growth deltas without ever touching ``window.db`` schema or content.
"""

from __future__ import annotations

import errno
import json
import os
import stat
import tempfile
import threading
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sightglass.contracts.errors import SightglassError

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids a model/db import cycle
    from sightglass.model.db import WindowDB
    from sightglass.storage import StorageBudget

STORAGE_HISTORY_SCHEMA = "sightglass.storage-history.v1"
STORAGE_HISTORY_STATE_SCHEMA = "sightglass.storage-history-state.v1"
STORAGE_HISTORY_FILE_VERSION = 1
STORAGE_HISTORY_FILENAME = "storage-history.json"
STORAGE_HISTORY_MAX_DAYS = 64
STORAGE_HISTORY_MAX_BYTES = 128 * 1024
STORAGE_HISTORY_RESERVE_BYTES = 128 * 1024
STORAGE_HISTORY_WINDOWS = (7, 30)

_COMPONENT_KEYS = (
    "database",
    "database_sidecars",
    "migration_backups",
    "delivery_spool",
    "resource_objects_and_tmp",
    "staging",
    "other_owned_files",
)
_DELTA_SOURCES = {
    "accounted_bytes": "accounted_bytes",
    "database_bytes": "database",
    "other_owned_bytes": "other_owned_files",
    "resource_bytes": "resource_objects_and_tmp",
}
_DELTA_COUNTS = ("message_count", "observation_count")
_ENTRY_KEYS = frozenset(
    {"day", "captured_at", "accounted_bytes", "components", "message_count", "observation_count"}
)
_TOP_LEVEL_KEYS = frozenset({"schema", "version", "days"})


class StorageHistoryError(RuntimeError):
    """Content-free failure raised when the history sidecar is unsafe or unreadable."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _non_negative_int(value: Any) -> int | None:
    if type(value) is not int or value < 0:
        return None
    return value


@dataclass(frozen=True)
class DailySnapshot:
    """One UTC day of content-free storage metrics."""

    day: str
    captured_at: str
    accounted_bytes: int
    components: dict[str, int]
    message_count: int
    observation_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.components, dict) or set(self.components) != set(_COMPONENT_KEYS):
            raise ValueError("history components must be the fixed content-free set")
        for key in _COMPONENT_KEYS:
            if _non_negative_int(self.components[key]) is None:
                raise ValueError("history components must be non-negative integers")
        for field in ("accounted_bytes", "message_count", "observation_count"):
            if _non_negative_int(getattr(self, field)) is None:
                raise ValueError(f"history {field} must be a non-negative integer")
        if sum(self.components.values()) != self.accounted_bytes:
            raise ValueError("history component totals must equal accounted_bytes")
        self._validate_day()

    def _validate_day(self) -> None:
        try:
            parsed_day = date.fromisoformat(self.day)
        except ValueError as exc:
            raise ValueError("history day is not an ISO date") from exc
        try:
            captured = datetime.fromisoformat(self.captured_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("history captured_at is not an ISO timestamp") from exc
        if captured.tzinfo is None or captured.utcoffset() is None:
            raise ValueError("history captured_at must be timezone-aware")
        if captured.astimezone(UTC).date() != parsed_day:
            raise ValueError("history captured_at must fall on the entry's UTC day")

    def as_dict(self) -> dict[str, Any]:
        return {
            "day": self.day,
            "captured_at": self.captured_at,
            "accounted_bytes": self.accounted_bytes,
            "components": dict(self.components),
            "message_count": self.message_count,
            "observation_count": self.observation_count,
        }

    @classmethod
    def from_dict(cls, value: Any) -> DailySnapshot:
        if not isinstance(value, dict):
            raise ValueError("history day entry must be an object")
        if set(value) != _ENTRY_KEYS:
            raise ValueError("history day entry has unexpected keys")
        day = value.get("day")
        captured_at = value.get("captured_at")
        accounting = _non_negative_int(value.get("accounted_bytes"))
        message_count = _non_negative_int(value.get("message_count"))
        observation_count = _non_negative_int(value.get("observation_count"))
        components = value.get("components")
        if (
            not isinstance(day, str)
            or not isinstance(captured_at, str)
            or accounting is None
            or message_count is None
            or observation_count is None
            or not isinstance(components, dict)
        ):
            raise ValueError("history day entry is incomplete")
        if set(components) != set(_COMPONENT_KEYS):
            raise ValueError("history components have unexpected keys")
        parsed_components: dict[str, int] = {}
        for key in _COMPONENT_KEYS:
            component = _non_negative_int(components.get(key))
            if component is None:
                raise ValueError("history components are incomplete")
            parsed_components[key] = component
        return cls(
            day=day,
            captured_at=captured_at,
            accounted_bytes=accounting,
            components=parsed_components,
            message_count=message_count,
            observation_count=observation_count,
        )


def capture_daily_snapshot(database: WindowDB, *, now: datetime | None = None) -> DailySnapshot:
    """Read one content-free daily snapshot from the owned budget and the database."""

    budget = database.storage
    status: dict[str, Any] = budget.status(reconcile=True) if budget is not None else {}
    components = status.get("components")
    if not isinstance(components, dict):
        components = {}
    counted: dict[str, int] = {key: int(components.get(key, 0)) for key in _COMPONENT_KEYS}
    with database.connection() as connection:
        connection.execute("BEGIN")
        row = connection.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM messages) AS message_count,
                (SELECT COUNT(*) FROM message_observations) AS observation_count
            """
        ).fetchone()
        connection.rollback()
    timestamp = (now or datetime.now(UTC)).astimezone(UTC)
    accounted = status.get("accounted_bytes")
    return DailySnapshot(
        day=timestamp.date().isoformat(),
        captured_at=timestamp.isoformat(timespec="seconds"),
        accounted_bytes=(
            accounted
            if type(accounted) is int
            else sum(counted.values())
        ),
        components=counted,
        message_count=int(row["message_count"]),
        observation_count=int(row["observation_count"]),
    )


class StorageHistory:
    """Bounded atomic sidecar of daily content-free storage metrics."""

    def __init__(
        self,
        data_dir: Path,
        budget: StorageBudget | None,
        *,
        filename: str = STORAGE_HISTORY_FILENAME,
        max_days: int = STORAGE_HISTORY_MAX_DAYS,
        max_bytes: int = STORAGE_HISTORY_MAX_BYTES,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.path = self.data_dir / filename
        self.budget = budget
        self.max_days = max_days
        self.max_bytes = max_bytes
        self._lock = threading.RLock()

    # -- storage ---------------------------------------------------------------

    def record(self, snapshot: DailySnapshot) -> None:
        """Replace this UTC day's snapshot, then trim to the bounded retention window."""

        with self._lock:
            by_day = {item.day: item for item in self._read_locked()}
            by_day[snapshot.day] = snapshot
            ordered = [by_day[day] for day in sorted(by_day)][-self.max_days :]
            self._write_locked(ordered)

    def status(self) -> dict[str, Any]:
        """Content-free observability for the daemon status surface."""

        try:
            with self._lock:
                entries = self._read_locked()
        except StorageHistoryError as exc:
            return {
                "schema": STORAGE_HISTORY_STATE_SCHEMA,
                "available": False,
                "reason": exc.reason,
                "day_count": 0,
                "latest_day": None,
                "latest_captured_at": None,
            }
        latest = entries[-1] if entries else None
        return {
            "schema": STORAGE_HISTORY_STATE_SCHEMA,
            "available": bool(entries),
            "reason": None if entries else "history_absent",
            "day_count": len(entries),
            "latest_day": latest.day if latest is not None else None,
            "latest_captured_at": latest.captured_at if latest is not None else None,
        }

    def history(
        self,
        *,
        current: DailySnapshot | None = None,
        limits: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Report exact 7/30-day deltas; never stand a shorter interval in for them."""

        moment = (now or datetime.now(UTC)).astimezone(UTC)
        today = moment.date()
        selected_limits = dict(limits or {})
        try:
            with self._lock:
                stored = self._read_locked()
        except StorageHistoryError as exc:
            return self._unavailable(exc.reason, selected_limits, today)
        by_day = {item.day: item for item in stored}
        if current is not None:
            by_day[current.day] = current
        if not by_day:
            return self._unavailable("history_absent", selected_limits, today)
        reference_day = today if today.isoformat() in by_day else date.fromisoformat(max(by_day))
        return {
            "schema": STORAGE_HISTORY_SCHEMA,
            "available": True,
            "reason": None,
            "retention_days": self.max_days,
            "as_of_day": reference_day.isoformat(),
            "as_of_is_current_day": reference_day == today,
            "day_count": len(by_day),
            "captured_days": sorted(by_day)[-self.max_days :],
            "limits": {
                "soft_limit_bytes": selected_limits.get("soft_limit_bytes"),
                "hard_limit_bytes": selected_limits.get("hard_limit_bytes"),
            },
            "windows": {
                f"{days}d": self._window(by_day, reference_day, days, selected_limits)
                for days in STORAGE_HISTORY_WINDOWS
            },
        }

    # -- internals -------------------------------------------------------------

    def _unavailable(
        self, reason: str, limits: dict[str, Any], today: date
    ) -> dict[str, Any]:
        return {
            "schema": STORAGE_HISTORY_SCHEMA,
            "available": False,
            "reason": reason,
            "retention_days": self.max_days,
            "as_of_day": None,
            "as_of_is_current_day": None,
            "day_count": 0,
            "captured_days": [],
            "limits": {
                "soft_limit_bytes": limits.get("soft_limit_bytes"),
                "hard_limit_bytes": limits.get("hard_limit_bytes"),
            },
            "windows": {
                f"{days}d": {
                    "days": days,
                    "available": False,
                    "reason": reason,
                    "baseline_day": (today - timedelta(days=days)).isoformat(),
                }
                for days in STORAGE_HISTORY_WINDOWS
            },
        }

    def _window(
        self,
        by_day: dict[str, DailySnapshot],
        reference_day: date,
        days: int,
        limits: dict[str, Any],
    ) -> dict[str, Any]:
        baseline_day = reference_day - timedelta(days=days)
        baseline = by_day.get(baseline_day.isoformat())
        reference = by_day.get(reference_day.isoformat())
        block: dict[str, Any] = {
            "days": days,
            "available": False,
            "reason": "baseline_day_absent",
            "baseline_day": baseline_day.isoformat(),
        }
        if baseline is None or reference is None:
            return block
        deltas = {
            key: self._delta(reference, baseline, source)
            for key, source in _DELTA_SOURCES.items()
        }
        for key in _DELTA_COUNTS:
            deltas[key] = getattr(reference, key) - getattr(baseline, key)
        growth = deltas["accounted_bytes"]
        growing = growth > 0
        per_day = growth / days
        block.update(
            {
                "available": True,
                "reason": None,
                "baseline_captured_at": baseline.captured_at,
                "interval_days": days,
                "deltas": deltas,
                "growing": growing,
                "average_accounted_bytes_per_day": round(per_day, 2),
                "bytes_per_new_message": (
                    round(growth / deltas["message_count"], 2)
                    if deltas["message_count"] > 0
                    else None
                ),
                "estimated_days_to_soft_limit": self._days_to_limit(
                    limits.get("soft_limit_bytes"), reference.accounted_bytes, per_day, growing
                ),
                "estimated_days_to_hard_limit": self._days_to_limit(
                    limits.get("hard_limit_bytes"), reference.accounted_bytes, per_day, growing
                ),
            }
        )
        return block

    @staticmethod
    def _delta(reference: DailySnapshot, baseline: DailySnapshot, source: str) -> int:
        if source == "accounted_bytes":
            return reference.accounted_bytes - baseline.accounted_bytes
        return reference.components.get(source, 0) - baseline.components.get(source, 0)

    @staticmethod
    def _days_to_limit(limit: Any, used: int, per_day: float, growing: bool) -> float | None:
        if not growing or type(limit) is not int:
            return None
        remaining = limit - used
        if remaining <= 0:
            return 0.0
        return round(remaining / per_day, 2)

    def _read_locked(self) -> list[DailySnapshot]:
        try:
            descriptor = os.open(self.path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except FileNotFoundError:
            return []
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise StorageHistoryError("history_symlink") from exc
            raise StorageHistoryError("history_unreadable") from exc
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise StorageHistoryError("history_not_regular")
            if stat.S_IMODE(metadata.st_mode) & 0o077:
                raise StorageHistoryError("history_insecure_permissions")
            if metadata.st_nlink != 1:
                raise StorageHistoryError("history_hardlinked")
            if metadata.st_size > self.max_bytes:
                raise StorageHistoryError("history_too_large")
            chunks: list[bytes] = []
            total = 0
            while True:
                chunk = os.read(descriptor, 65536)
                if not chunk:
                    break
                total += len(chunk)
                if total > self.max_bytes:
                    raise StorageHistoryError("history_too_large")
                chunks.append(chunk)
        finally:
            os.close(descriptor)
        try:
            decoded = json.loads(b"".join(chunks).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise StorageHistoryError("history_corrupt") from exc
        if (
            not isinstance(decoded, dict)
            or set(decoded) != _TOP_LEVEL_KEYS
            or decoded.get("schema") != STORAGE_HISTORY_SCHEMA
            or decoded.get("version") != STORAGE_HISTORY_FILE_VERSION
            or not isinstance(decoded.get("days"), list)
        ):
            raise StorageHistoryError("history_corrupt")
        entries: dict[str, DailySnapshot] = {}
        try:
            for item in decoded["days"]:
                snapshot = DailySnapshot.from_dict(item)
                if snapshot.day in entries:
                    raise ValueError("duplicate history day entry")
                entries[snapshot.day] = snapshot
        except ValueError as exc:
            raise StorageHistoryError("history_corrupt") from exc
        return [entries[day] for day in sorted(entries)][-self.max_days :]

    def _write_locked(self, snapshots: list[DailySnapshot]) -> None:
        payload = json.dumps(
            {
                "schema": STORAGE_HISTORY_SCHEMA,
                "version": STORAGE_HISTORY_FILE_VERSION,
                "days": [snapshot.as_dict() for snapshot in snapshots],
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
        if len(payload) > self.max_bytes:
            raise StorageHistoryError("history_too_large")
        if self.budget is not None:
            try:
                with self.budget.reserve(STORAGE_HISTORY_RESERVE_BYTES, maintenance=True):
                    self._atomic_replace(payload)
            except SightglassError as exc:
                raise StorageHistoryError("history_write_failed") from exc
            self.budget.track(self.path)
        else:
            self._atomic_replace(payload)

    def _atomic_replace(self, payload: bytes) -> None:
        parent = self.path.parent
        descriptor: int | None = None
        temporary: Path | None = None
        try:
            descriptor, name = tempfile.mkstemp(prefix=".storage-history-", dir=parent)
            temporary = Path(name)
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                descriptor = None
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            temporary = None
            os.chmod(self.path, 0o600)
        except OSError as exc:
            raise StorageHistoryError("history_write_failed") from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        try:
            directory = os.open(parent, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(directory)
        except OSError:
            pass
        finally:
            os.close(directory)


def next_utc_midnight_delay(now: datetime | None = None) -> float:
    """Seconds until the next UTC day boundary, always strictly positive."""

    moment = (now or datetime.now(UTC)).astimezone(UTC)
    tomorrow = moment.date() + timedelta(days=1)
    boundary = datetime(tomorrow.year, tomorrow.month, tomorrow.day, tzinfo=UTC)
    return max(1.0, (boundary - moment).total_seconds())

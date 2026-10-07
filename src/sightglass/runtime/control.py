from __future__ import annotations

import os
import plistlib
import stat
import subprocess
import sys
import time
from itertools import islice
from pathlib import Path
from typing import Any

from sightglass.contracts.errors import SightglassError
from sightglass.model.db import WindowDB
from sightglass.residency.decisions import parse_residency_settings
from sightglass.residency.repository import ResidencyRepository
from sightglass.source.registry import create_provider

from .config import SightglassConfig


class ControlError(RuntimeError):
    pass


_OBJECT_LIVENESS_PREDICATE = """
    NOT EXISTS (SELECT 1 FROM resource_bindings AS rb
                WHERE rb.object_digest = {alias}.object_digest)
    AND NOT EXISTS (SELECT 1 FROM resource_derivations AS rd
                    WHERE rd.derived_digest = {alias}.object_digest)
    AND NOT EXISTS (SELECT 1 FROM voice_jobs AS vj
                    WHERE vj.input_digest = {alias}.object_digest
                       OR vj.result_digest = {alias}.object_digest)
    AND NOT EXISTS (SELECT 1 FROM voice_batch_events AS ve
                    WHERE ve.result_digest = {alias}.object_digest)
"""


def cache_status(database: WindowDB) -> dict[str, Any]:
    with database.connection() as connection:
        row = connection.execute(
            f"""
            SELECT COUNT(*) AS object_count,
                   COALESCE(SUM(byte_size), 0) AS byte_size,
                   COALESCE(SUM(CASE WHEN {_OBJECT_LIVENESS_PREDICATE.format(alias="ro")}
                   THEN 1 ELSE 0 END), 0) AS unbound_count,
                   COALESCE(SUM(CASE WHEN {_OBJECT_LIVENESS_PREDICATE.format(alias="ro")}
                   THEN byte_size ELSE 0 END), 0) AS reclaimable_bytes
            FROM resource_objects AS ro
            """
        ).fetchone()
    return {
        "schema": "sightglass.cache-status.v1",
        "object_count": int(row["object_count"]),
        "byte_size": int(row["byte_size"]),
        "unbound_count": int(row["unbound_count"]),
        "reclaimable_bytes": int(row["reclaimable_bytes"]),
    }


_ORPHAN_GRACE_SECONDS = 24 * 60 * 60


def _cache_root_exists(root: Path) -> bool:
    for directory in (root.parent, root):
        try:
            metadata = directory.lstat()
        except FileNotFoundError:
            return False
        if not stat.S_ISDIR(metadata.st_mode):
            raise ControlError("cache object directory must not be a symlink or non-directory")
    return True


def _object_metadata(path: Path) -> os.stat_result | None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise ControlError("cache object is not a private single-linked file")
    return metadata


def _digest_name(name: str) -> bool:
    return len(name) == 64 and all(character in "0123456789abcdef" for character in name)


def _unlink_unregistered_object(database: WindowDB, path: Path, selected: os.stat_result) -> bool:
    try:
        # A registration commits its resource_objects/resource_bindings rows inside a
        # WindowDB writer transaction, and the voice-style path also performs its CAS write
        # under that same lock. The durable row DELETE already committed before this call,
        # so reuse that writer lock as a short final registration fence: revalidate the
        # root, path and captured dev/inode and confirm the digest row is still absent on
        # this connection, then unlink -- all serialized against registration. A
        # registration that committed first is seen by the absence check and the file is
        # left intact; a registration that arrives later blocks on the fence and re-creates
        # the CAS file only after this unlink. This fence never repeats or owns the earlier
        # delete.
        with database.transaction(maintenance=True) as connection:
            if not _cache_root_exists(path.parent):
                return False
            current = _object_metadata(path)
            if current is None or (current.st_dev, current.st_ino) != (
                selected.st_dev,
                selected.st_ino,
            ):
                return False
            if connection.execute(
                "SELECT 1 FROM resource_objects WHERE object_digest = ?", (path.name,)
            ).fetchone():
                return False
            path.unlink()
        if database.storage is not None:
            database.storage.track(path)
    except (OSError, ControlError, SightglassError):
        return False
    return True


def cleanup_cache(database: WindowDB, *, apply: bool) -> dict[str, Any]:
    expected_root = database.path.parent / "resource-cache" / "objects"
    with database.connection() as connection:
        if connection.in_transaction:
            raise ControlError("cache cleanup requires an idle database, not a nested transaction")
        rows = connection.execute(
            f"""
            SELECT ro.* FROM resource_objects AS ro
            WHERE {_OBJECT_LIVENESS_PREDICATE.format(alias="ro")}
            ORDER BY ro.object_digest
            """
        ).fetchall()
    root_exists = _cache_root_exists(expected_root)
    candidates: dict[str, tuple[Path, os.stat_result | None]] = {}
    for row in rows:
        digest = str(row["object_digest"])
        path = Path(str(row["local_path_internal"]))
        if path.parent != expected_root or path.name != digest or not _digest_name(digest):
            raise ControlError("cache object path does not match the private object store")
        candidates[digest] = (path, _object_metadata(path) if root_exists else None)
    deleted: list[str] = []
    removed_count = 0
    if apply:
        with database.transaction(maintenance=True) as connection:
            for digest in candidates:
                result = connection.execute(
                    f"""
                    DELETE FROM resource_objects AS ro
                    WHERE ro.object_digest = ? AND {_OBJECT_LIVENESS_PREDICATE.format(alias="ro")}
                    """,
                    (digest,),
                )
                if result.rowcount:
                    deleted.append(digest)
        for digest in deleted:
            path, metadata = candidates[digest]
            if metadata is not None:
                removed_count += int(_unlink_unregistered_object(database, path, metadata))
    orphan_count = 0
    orphan_bytes = 0
    if _cache_root_exists(expected_root):
        cutoff = time.time() - _ORPHAN_GRACE_SECONDS
        for path in expected_root.iterdir():
            if not _digest_name(path.name):
                continue
            try:
                metadata = _object_metadata(path)
            except (OSError, ControlError):
                continue
            if metadata is None:
                continue
            with database.connection() as connection:
                if connection.execute(
                    "SELECT 1 FROM resource_objects WHERE object_digest = ?", (path.name,)
                ).fetchone():
                    continue
            if (
                apply
                and path.name not in deleted
                and metadata.st_mtime < cutoff
                and _unlink_unregistered_object(database, path, metadata)
            ):
                removed_count += 1
                continue
            orphan_count += 1
            orphan_bytes += metadata.st_size
    return {
        "schema": "sightglass.cache-cleanup.v1",
        "applied": apply,
        "object_count": len(rows),
        "reclaimable_bytes": sum(int(row["byte_size"]) for row in rows),
        "removed_count": removed_count,
        "orphan_count": orphan_count,
        "orphan_bytes": orphan_bytes,
        "deliveries": cleanup_deliveries(database, apply=apply),
        "residency": reclaim_expired_cache(database) if apply else cache_preview(database),
    }


def cleanup_deliveries(database: WindowDB, *, apply: bool, limit: int = 500) -> dict[str, Any]:
    """Retire terminal spools and aged orphans under the pending-publication writer fence."""
    root = database.path.parent / "deliveries"
    result = {
        "candidate_count": 0,
        "reclaimable_bytes": 0,
        "removed_count": 0,
        "pending_preserved": 0,
        "orphan_grace_seconds": _ORPHAN_GRACE_SECONDS,
        "batch_limit": limit,
        "has_more": False,
    }
    if not root.exists():
        return result
    metadata = root.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise ControlError("delivery spool directory must be private and not a symlink")
    paths = list(islice(root.iterdir(), limit + 1))
    result["has_more"] = len(paths) > limit
    cutoff = time.time() - _ORPHAN_GRACE_SECONDS
    # Pending spools are written and registered inside this same writer lock. Recheck
    # every reference and inode while holding it; an in-flight delivery cannot be GC'd.
    context = database.transaction(maintenance=True) if apply else database.read_snapshot()
    with context as connection:
        for path in paths[:limit]:
            if path.suffix != ".json":
                continue
            try:
                selected = _object_metadata(path)
            except (OSError, ControlError):
                continue
            if selected is None or stat.S_IMODE(selected.st_mode) & 0o077:
                continue
            rows = connection.execute(
                "SELECT status FROM reader_deliveries WHERE payload_ref=?", (str(path),)
            ).fetchall()
            if any(row[0] == "pending" for row in rows):
                result["pending_preserved"] += 1
                continue
            terminal = bool(rows) and all(row[0] in {"acknowledged", "expired"} for row in rows)
            if not terminal and (rows or selected.st_mtime >= cutoff):
                continue
            result["candidate_count"] += 1
            result["reclaimable_bytes"] += selected.st_size
            if apply:
                current = path.lstat()
                if (current.st_dev, current.st_ino, current.st_mtime_ns, current.st_size) != (
                    selected.st_dev,
                    selected.st_ino,
                    selected.st_mtime_ns,
                    selected.st_size,
                ):
                    continue
                path.unlink()
                result["removed_count"] += 1
                if database.storage is not None:
                    database.storage.track(path)
    if apply and result["removed_count"]:
        descriptor = os.open(root, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return result


def pending_delivery_count(database: WindowDB) -> int:
    with database.connection() as connection:
        return int(
            connection.execute(
                "SELECT COUNT(*) FROM reader_deliveries WHERE status = 'pending'"
            ).fetchone()[0]
        )


def volume_encryption_status(path: Path) -> dict[str, Any]:
    if sys.platform != "darwin":
        return {"status": "unknown", "encrypted": None}
    resolved = subprocess.run(
        ["/bin/df", "-P", str(path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    lines = resolved.stdout.splitlines()
    if resolved.returncode != 0 or len(lines) < 2:
        return {"status": "unknown", "encrypted": None}
    device = lines[-1].split(maxsplit=1)[0]
    if not device.startswith("/dev/"):
        return {"status": "unknown", "encrypted": None}
    completed = subprocess.run(
        ["/usr/sbin/diskutil", "info", "-plist", device],
        check=False,
        capture_output=True,
        timeout=15,
    )
    if completed.returncode != 0:
        return {"status": "unknown", "encrypted": None}
    try:
        value = plistlib.loads(completed.stdout)
    except plistlib.InvalidFileException:
        return {"status": "unknown", "encrypted": None}
    encrypted = value.get("Encrypted")
    if not isinstance(encrypted, bool):
        encrypted = value.get("FileVault")
    return {
        "status": "verified" if isinstance(encrypted, bool) else "unknown",
        "encrypted": encrypted if isinstance(encrypted, bool) else None,
        "filesystem": value.get("FilesystemType"),
    }


def doctor(config: SightglassConfig, *, keychain_ready: bool) -> dict[str, Any]:
    descriptor = create_provider(config).descriptor
    settings_path = config.source_settings_path
    checks: dict[str, Any] = {
        "config_directory_private": stat.S_IMODE(config.data_dir.stat().st_mode) == 0o700,
        "socket_directory_private": stat.S_IMODE(config.socket_path.parent.stat().st_mode) == 0o700,
        "window_db_private": (
            config.window_db_path.exists()
            and stat.S_IMODE(config.window_db_path.stat().st_mode) == 0o600
        ),
        "keychain_secrets_available": keychain_ready,
        "source_settings_available": bool(settings_path is not None and settings_path.is_file()),
    }
    return {
        "schema": "sightglass.doctor.v1",
        "ready": all(checks.values()),
        "checks": checks,
        "volume_encryption": volume_encryption_status(config.window_db_path.parent),
        "source_kind": descriptor.kind,
        "source_mode": descriptor.source_mode,
        "synthetic_only": descriptor.source_mode == "synthetic",
    }


def process_is_running(pid: int) -> bool:
    if pid < 1:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


# -- selective residency operator surface ---------------------------------


def residency_status(database: WindowDB) -> dict[str, Any]:
    """Content-free residency settings and occupancy summary."""

    return ResidencyRepository(database).preview()


def residency_list(
    database: WindowDB,
    *,
    mode: str | None = None,
    limit: int = 200,
    cursor: str | None = None,
    sort: str = "bytes",
) -> dict[str, Any]:
    entries, next_cursor = ResidencyRepository(database).list(
        mode=mode, limit=limit, cursor=cursor, sort=sort
    )
    return {
        "schema": "sightglass.residency-list.v1",
        "entries": entries,
        "next_cursor": next_cursor,
    }


def residency_set(
    database: WindowDB,
    *,
    conversation_ids: list[str] | tuple[str, ...],
    mode: str,
    keep_backfill: bool = False,
    recent_window_days: int | None = None,
    recent_max_bytes: int | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    """Batch set the future-collection residency for authorized conversations.

    Setting a mode only governs *future* collection.  Releasing already-retained
    copies is a separate :func:`residency_release` preview/apply action.
    """

    store = ResidencyRepository(database)
    updated = store.batch_set(
        tuple(conversation_ids),
        mode=mode,
        keep_backfill=keep_backfill,
        recent_window_days=recent_window_days,
        recent_max_bytes=recent_max_bytes,
        reason=reason,
    )
    return {
        "schema": "sightglass.residency-set.v1",
        "updated_count": len(updated),
        "entries": [entry.as_dict() for entry in updated],
    }


def residency_configure(
    database: WindowDB,
    *,
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    store = ResidencyRepository(database)
    if settings:
        store.set_settings(parse_residency_settings(store.settings().as_dict() | settings))
    return {"schema": "sightglass.residency-settings.v1", "settings": store.settings().as_dict()}


def residency_release(
    database: WindowDB,
    *,
    conversation_id: str,
    apply: bool = False,
    plan: dict[str, Any] | None = None,
    cursor: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    """Preview (default) or apply explicit release of disposable resident copies.

    Existing stock is never released implicitly; ``apply=False`` only estimates.
    """

    if type(apply) is not bool:
        raise ValueError("release apply must be a boolean")
    store = ResidencyRepository(database)
    if not apply:
        preview = store.stock_preview(conversation_id, after=cursor, limit=limit)
        preview.update(
            {
                "schema": "sightglass.residency-release-preview.v1",
                "applied": False,
            }
        )
        return preview
    if plan is None:
        raise ValueError("release --apply requires the exact preview plan")
    result = store.release_stock(conversation_id, plan=plan, limit=limit)
    result["applied"] = True
    return result


def cache_preview(database: WindowDB) -> dict[str, Any]:
    """Content-free reclaimable-cache preview (read leases + expired work)."""

    store = ResidencyRepository(database)
    preview = store.preview()
    return {
        "schema": "sightglass.cache-preview.v1",
        "read_lease_live_count": preview["lease_live_count"],
        "read_lease_live_bytes": preview["lease_live_bytes"],
        "read_lease_expired_count": preview["lease_expired_count"],
        "read_lease_expired_bytes": preview["lease_expired_bytes"],
    }


def reclaim_expired_cache(database: WindowDB, *, limit: int = 500) -> dict[str, Any]:
    """Bounded reclaim of expired read leases (disposable foreground cache)."""

    return ResidencyRepository(database).reclaim_expired(limit=limit)


def residency_rebaseline(
    database: WindowDB,
    *,
    conversation_id: str,
    reason: str | None = None,
) -> dict[str, Any]:
    """Explicitly rebaseline resident update evidence for one conversation."""

    return ResidencyRepository(database).rebaseline(conversation_id, reason=reason)

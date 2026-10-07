"""Offline compressed snapshots and explicit migration-backup lifecycle."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import tempfile
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

BACKUP_MANIFEST_SCHEMA = "sightglass.window-backup.v1"
BACKUP_PLAN_SCHEMA = "sightglass.window-backup-plan.v1"
BACKUP_RESULT_SCHEMA = "sightglass.window-backup-result.v1"
RESTORE_JOURNAL_SCHEMA = "sightglass.window-restore-journal.v1"
_CHUNK_BYTES = 4 * 1024 * 1024
# Owner-private transaction namespace beside window.db, derived only from the database
# name plus these fixed suffixes. No recovery path is ever taken from a value recorded in
# the journal, so a corrupt journal can never redirect the restore at an arbitrary path.
_RESTORE_TX_SUFFIX = ".restore-tx"
_RESTORE_JOURNAL_NAME = "journal.json"
_RESTORE_PREPARED_NAME = "prepared-main"
_RESTORE_ORIGINAL_MAIN_NAME = "original-main"
_RESTORE_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")
_RESTORE_ORIGINAL_SIDECAR_NAMES = {
    "-wal": "original-wal",
    "-shm": "original-shm",
    "-journal": "original-journal",
}


def _zstandard():
    try:
        import zstandard
    except ImportError as exc:  # pragma: no cover - optional production extra
        raise RuntimeError(
            "compressed window.db backups require the macos-wechat extra (zstandard)"
        ) from exc
    return zstandard


def _private_regular_file(path: Path) -> os.stat_result:
    if path.is_symlink():
        raise RuntimeError("backup input must not be a symlink")
    metadata = path.stat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise RuntimeError("backup input must be a private single-linked regular file")
    return metadata


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with os.fdopen(
        os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)),
        "rb",
        closefd=True,
    ) as handle:
        while chunk := handle.read(_CHUNK_BYTES):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _database_state(database_path: Path) -> dict[str, Any]:
    metadata = _private_regular_file(database_path)
    with closing(sqlite3.connect(f"{database_path.as_uri()}?mode=ro", uri=True)) as connection:
        schema_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
    if quick_check != "ok":
        raise RuntimeError("window.db failed SQLite quick_check")
    return {
        "database_name": database_path.name,
        "schema_version": schema_version,
        "byte_size": int(metadata.st_size),
        "mtime_ns": int(metadata.st_mtime_ns),
        "inode": int(metadata.st_ino),
    }


def checkpoint_database(database_path: Path) -> dict[str, Any]:
    """Checkpoint committed WAL frames before an offline byte snapshot."""

    _private_regular_file(database_path)
    with closing(sqlite3.connect(database_path)) as connection:
        row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if row is None or int(row[0]) != 0:
            raise RuntimeError("window.db WAL checkpoint did not complete")
    return _database_state(database_path)


def _artifact_name(database_path: Path, schema_version: int, attempt: int) -> str:
    suffix = "" if attempt == 0 else f".{attempt}"
    return f"{database_path.name}.v{schema_version}.backup{suffix}.zst"


def _reserve_artifact_path(database_path: Path, schema_version: int) -> Path:
    attempt = 0
    while True:
        candidate = database_path.with_name(_artifact_name(database_path, schema_version, attempt))
        if not candidate.exists() and not candidate.is_symlink():
            return candidate
        attempt += 1


def _manifest_path(artifact: Path) -> Path:
    return artifact.with_name(artifact.name + ".json")


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8"))
            handle.write(b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _read_manifest(artifact: Path) -> dict[str, Any]:
    manifest_path = _manifest_path(artifact)
    # An artifact and its manifest are published as two separate steps, so any state
    # between them (artifact without a manifest, a half-written manifest, a replaced or
    # unreadable path) is an *incomplete publication* and must be treated as unverified
    # rather than surfacing a raw filesystem error. Callers rely on a single RuntimeError
    # signal to decide that a compressed snapshot cannot satisfy the verified-backup
    # requirement; leaking FileNotFoundError here would abort a migration that should
    # have fallen back to the raw snapshot path.
    try:
        _private_regular_file(artifact)
        _private_regular_file(manifest_path)
    except FileNotFoundError as exc:
        raise RuntimeError("compressed backup publication is incomplete") from exc
    except RuntimeError as exc:
        raise RuntimeError(f"compressed backup is not a verified snapshot: {exc}") from exc
    try:
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("compressed backup manifest is invalid") from exc
    required = {
        "schema",
        "database_name",
        "source_schema_version",
        "created_at",
        "compression",
        "raw_size",
        "raw_sha256",
        "compressed_size",
        "compressed_sha256",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise RuntimeError("compressed backup manifest has an invalid contract")
    if (
        value.get("schema") != BACKUP_MANIFEST_SCHEMA
        or value.get("compression") != "zstd-level-1"
        or not isinstance(value.get("database_name"), str)
        or type(value.get("source_schema_version")) is not int
        or type(value.get("raw_size")) is not int
        or type(value.get("compressed_size")) is not int
        or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("raw_sha256")))
        or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("compressed_sha256")))
    ):
        raise RuntimeError("compressed backup manifest has invalid values")
    return value


def create_compressed_snapshot(database_path: Path) -> dict[str, Any]:
    state = checkpoint_database(database_path)
    schema_version = int(state["schema_version"])
    existing = find_verified_migration_snapshot(database_path, schema_version)
    if existing is not None:
        manifest = _read_manifest(existing)
        return {
            "schema": BACKUP_RESULT_SCHEMA,
            "action": "create",
            "created": False,
            "reused": True,
            "artifact": existing.name,
            "manifest": _manifest_path(existing).name,
            "source_schema_version": schema_version,
            "raw_size": manifest["raw_size"],
            "compressed_size": manifest["compressed_size"],
        }
    # A previous create may have been interrupted between artifact publication and
    # manifest publication. Such an artifact can never verify, but unverified is not
    # permission to destroy possibly recoverable bytes: it is left in place and excluded
    # from verified-migration selection (``find_verified_migration_snapshot`` only returns
    # artifacts that verify). A fresh attempt is reserved alongside it, and the plan
    # surface reports the unverified artifact rather than hiding or removing it.
    artifact = _reserve_artifact_path(database_path, schema_version)
    zstandard = _zstandard()
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{artifact.name}.", suffix=".tmp", dir=artifact.parent
    )
    temporary = Path(temporary_name)
    raw_digest = hashlib.sha256()
    raw_size = 0
    descriptor_open = True
    try:
        os.fchmod(descriptor, 0o600)
        source_descriptor = os.open(database_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with (
            os.fdopen(source_descriptor, "rb", closefd=True) as source,
            os.fdopen(descriptor, "wb", closefd=True) as destination,
            zstandard.ZstdCompressor(level=1).stream_writer(
                destination, closefd=False
            ) as compressor,
        ):
            descriptor_open = False
            while chunk := source.read(_CHUNK_BYTES):
                raw_digest.update(chunk)
                raw_size += len(chunk)
                compressor.write(chunk)
            compressor.flush(zstandard.FLUSH_FRAME)
            destination.flush()
            os.fsync(destination.fileno())
        if raw_size != int(state["byte_size"]):
            raise RuntimeError("window.db changed while the offline snapshot was created")
        compressed_digest, compressed_size = _hash_file(temporary)
        os.replace(temporary, artifact)
        os.chmod(artifact, 0o600)
        manifest = {
            "schema": BACKUP_MANIFEST_SCHEMA,
            "database_name": database_path.name,
            "source_schema_version": schema_version,
            "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "compression": "zstd-level-1",
            "raw_size": raw_size,
            "raw_sha256": raw_digest.hexdigest(),
            "compressed_size": compressed_size,
            "compressed_sha256": compressed_digest,
        }
        _write_json_atomic(_manifest_path(artifact), manifest)
        _fsync_directory(artifact.parent)
    except Exception:
        if descriptor_open:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        artifact.unlink(missing_ok=True)
        _manifest_path(artifact).unlink(missing_ok=True)
        raise
    return {
        "schema": BACKUP_RESULT_SCHEMA,
        "action": "create",
        "created": True,
        "reused": False,
        "artifact": artifact.name,
        "manifest": _manifest_path(artifact).name,
        "source_schema_version": schema_version,
        "raw_size": raw_size,
        "compressed_size": compressed_size,
    }


def verify_compressed_snapshot(
    database_path: Path,
    artifact: Path,
    *,
    expected_schema_version: int | None = None,
    expected_raw_sha256: str | None = None,
) -> dict[str, Any]:
    if artifact.parent.resolve() != database_path.parent.resolve():
        raise RuntimeError("compressed backup must be adjacent to window.db")
    manifest = _read_manifest(artifact)
    if manifest["database_name"] != database_path.name:
        raise RuntimeError("compressed backup belongs to a different database")
    if (
        expected_schema_version is not None
        and manifest["source_schema_version"] != expected_schema_version
    ):
        raise RuntimeError("compressed backup schema version does not match")
    compressed_digest, compressed_size = _hash_file(artifact)
    if (
        compressed_digest != manifest["compressed_sha256"]
        or compressed_size != manifest["compressed_size"]
    ):
        raise RuntimeError("compressed backup digest does not match its manifest")
    raw_digest = hashlib.sha256()
    raw_size = 0
    zstandard = _zstandard()
    try:
        with (
            artifact.open("rb") as source,
            zstandard.ZstdDecompressor().stream_reader(source) as reader,
        ):
            while chunk := reader.read(_CHUNK_BYTES):
                raw_digest.update(chunk)
                raw_size += len(chunk)
    except (OSError, zstandard.ZstdError) as exc:
        # A truncated or unreadable artifact is an unverified snapshot, never a crash
        # that escapes the RuntimeError contract its callers rely on.
        raise RuntimeError("compressed backup payload is unreadable") from exc
    if raw_size != manifest["raw_size"] or raw_digest.hexdigest() != manifest["raw_sha256"]:
        raise RuntimeError("compressed backup payload does not match its manifest")
    if expected_raw_sha256 is not None and raw_digest.hexdigest() != expected_raw_sha256:
        raise RuntimeError("compressed backup does not match the current window.db")
    return manifest


def _compressed_candidates(database_path: Path, schema_version: int) -> list[Path]:
    pattern = re.compile(
        rf"^{re.escape(database_path.name)}\.v{schema_version}\.backup(?:\.\d+)?\.zst$"
    )
    return sorted(
        path
        for path in database_path.parent.iterdir()
        if pattern.fullmatch(path.name) and not path.is_symlink()
    )


def find_verified_migration_snapshot(database_path: Path, source_version: int) -> Path | None:
    candidates = _compressed_candidates(database_path, source_version)
    if not candidates:
        return None
    raw_sha256, _raw_size = _hash_file(database_path)
    for artifact in candidates:
        try:
            verify_compressed_snapshot(
                database_path,
                artifact,
                expected_schema_version=source_version,
                expected_raw_sha256=raw_sha256,
            )
        except RuntimeError:
            continue
        return artifact
    return None


def _legacy_backup_paths(database_path: Path) -> list[Path]:
    pattern = re.compile(
        rf"^{re.escape(database_path.name)}\.v\d+\.backup(?:\.\d+)?"
        rf"(?:-(?:journal|wal|shm))?$"
    )
    return sorted(
        path
        for path in database_path.parent.iterdir()
        if pattern.fullmatch(path.name) and not path.is_symlink()
    )


def _identity(path: Path) -> dict[str, Any]:
    metadata = _private_regular_file(path)
    return {
        "name": path.name,
        "byte_size": int(metadata.st_size),
        "mtime_ns": int(metadata.st_mtime_ns),
        "inode": int(metadata.st_ino),
    }


def _restore_tx_dir(database_path: Path) -> Path:
    return database_path.with_name(f".{database_path.name}{_RESTORE_TX_SUFFIX}")


def _restore_original(database_path: Path, suffix: str) -> Path:
    return _restore_tx_dir(database_path) / _RESTORE_ORIGINAL_SIDECAR_NAMES[suffix]


def _ensure_restore_tx_dir(database_path: Path) -> Path:
    transaction = _restore_tx_dir(database_path)
    if transaction.is_symlink():
        raise RuntimeError("restore transaction directory must not be a symlink")
    transaction.mkdir(mode=0o700, exist_ok=True)
    _fsync_directory(database_path.parent)
    metadata = transaction.stat()
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RuntimeError("restore transaction directory must be private (mode 0700)")
    return transaction


def _write_restore_journal(database_path: Path, *, artifact_name: str, committed: bool) -> None:
    """Durably record restore intent/commit before the next namespace mutation.

    Owner-private and fixed-name: only the adjacent artifact basename, a schema tag, and
    the commit flag are stored. Recovery never trusts a path from this file.
    """

    if Path(artifact_name).name != artifact_name:
        raise RuntimeError("restore artifact must be an adjacent filename")
    transaction = _ensure_restore_tx_dir(database_path)
    value = {
        "schema": RESTORE_JOURNAL_SCHEMA,
        "database_name": database_path.name,
        "artifact": artifact_name,
        "committed": bool(committed),
    }
    _write_json_atomic(transaction / _RESTORE_JOURNAL_NAME, value)


def _read_restore_journal(database_path: Path) -> dict[str, Any]:
    journal = _restore_tx_dir(database_path) / _RESTORE_JOURNAL_NAME
    try:
        _private_regular_file(journal)
    except FileNotFoundError as exc:
        raise RuntimeError("restore journal is missing") from exc
    except RuntimeError as exc:
        raise RuntimeError(f"restore journal is not a private file: {exc}") from exc
    try:
        value = json.loads(journal.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("restore journal is invalid") from exc
    required = {"schema", "database_name", "artifact", "committed"}
    if (
        not isinstance(value, dict)
        or set(value) != required
        or value.get("schema") != RESTORE_JOURNAL_SCHEMA
        or value.get("database_name") != database_path.name
        or not isinstance(value.get("artifact"), str)
        or Path(str(value.get("artifact"))).name != value.get("artifact")
        or not isinstance(value.get("committed"), bool)
    ):
        raise RuntimeError("restore journal has an invalid contract")
    return value


def _remove_restore_tx_dir(database_path: Path) -> None:
    transaction = _restore_tx_dir(database_path)
    if not transaction.exists():
        return
    if not transaction.is_dir() or transaction.is_symlink():
        raise RuntimeError("restore transaction namespace is malformed")
    children = _restore_namespace_files(database_path)
    # Keep the recovery decision durable until every retired file is gone. A crash
    # halfway through cleanup must still see the committed/rollback journal.
    for child in sorted(children, key=lambda path: path.name == _RESTORE_JOURNAL_NAME):
        child.unlink()
    transaction.rmdir()
    _fsync_directory(database_path.parent)


def _restore_namespace_files(database_path: Path) -> list[Path]:
    transaction = _restore_tx_dir(database_path)
    metadata = transaction.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RuntimeError("restore transaction namespace must be a private directory")
    names = {
        _RESTORE_JOURNAL_NAME,
        _RESTORE_PREPARED_NAME,
        _RESTORE_ORIGINAL_MAIN_NAME,
        *_RESTORE_ORIGINAL_SIDECAR_NAMES.values(),
    }
    children = list(transaction.iterdir())
    for child in children:
        temporary = re.fullmatch(
            r"\.(?:prepared|journal\.json)\.[A-Za-z0-9_-]+\.tmp(?:-(?:wal|shm|journal))?",
            child.name,
        )
        if child.name not in names and temporary is None:
            raise RuntimeError("restore transaction namespace contains an unknown file")
        _private_regular_file(child)
    return children


def recover_interrupted_restore(database_path: Path) -> dict[str, Any]:
    """Finish or roll back a durable restore interrupted by process death.

    Called before any code opens or creates ``window.db`` and before CLI backup
    operations. A committed transaction keeps the newly published database and discards
    the staged originals; an uncommitted transaction restores the original main file plus
    its WAL/SHM/journal so the pre-restore namespace is complete and usable. Returns a
    content-free summary; it is a no-op (``recovered=False``) when no transaction exists.
    """

    transaction = _restore_tx_dir(database_path)
    if not transaction.exists() and not transaction.is_symlink():
        return {
            "schema": BACKUP_RESULT_SCHEMA,
            "action": "recover",
            "recovered": False,
            "outcome": "none",
        }
    _restore_namespace_files(database_path)
    if not (transaction / _RESTORE_JOURNAL_NAME).exists():
        # The journal is written before any namespace mutation, so its absence means no
        # swap started: only an unused staged preparation remains. Discard it safely.
        _remove_restore_tx_dir(database_path)
        return {
            "schema": BACKUP_RESULT_SCHEMA,
            "action": "recover",
            "recovered": False,
            "outcome": "none",
        }
    journal = _read_restore_journal(database_path)
    committed = bool(journal["committed"])
    if committed:
        # The new main file was durably published; the staged originals are the obsolete
        # namespace. Keep the new database and retire the transaction namespace.
        _remove_restore_tx_dir(database_path)
        return {
            "schema": BACKUP_RESULT_SCHEMA,
            "action": "recover",
            "recovered": True,
            "outcome": "committed",
        }
    # Uncommitted: rebuild the original namespace. The original main and sidecars were
    # renamed aside (never deleted), so the complete pre-restore database is restored.
    original_main = transaction / _RESTORE_ORIGINAL_MAIN_NAME
    if original_main.exists():
        database_path.unlink(missing_ok=True)
        os.replace(original_main, database_path)
        os.chmod(database_path, 0o600)
    for suffix in _RESTORE_SIDECAR_SUFFIXES:
        staged = _restore_original(database_path, suffix)
        if staged.exists():
            sidecar = database_path.with_name(database_path.name + suffix)
            sidecar.unlink(missing_ok=True)
            os.replace(staged, sidecar)
    _fsync_directory(database_path.parent)
    _remove_restore_tx_dir(database_path)
    return {
        "schema": BACKUP_RESULT_SCHEMA,
        "action": "recover",
        "recovered": True,
        "outcome": "rolled_back",
    }


def _ack(kind: str, value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(kind.encode("ascii") + b"\0" + encoded).hexdigest()


def backup_plan(database_path: Path) -> dict[str, Any]:
    database = _database_state(database_path)
    legacy = [_identity(path) for path in _legacy_backup_paths(database_path)]
    snapshots = []
    for version in range(1, max(1, int(database["schema_version"])) + 1):
        for artifact in _compressed_candidates(database_path, version):
            try:
                manifest = verify_compressed_snapshot(database_path, artifact)
            except RuntimeError as exc:
                snapshots.append({"artifact": artifact.name, "verified": False, "reason": str(exc)})
                continue
            restore_value = {
                "database": database,
                "artifact": artifact.name,
                "compressed_sha256": manifest["compressed_sha256"],
            }
            retire_value = {
                "database_name": database_path.name,
                "artifact": _identity(artifact),
                "manifest": _identity(_manifest_path(artifact)),
                "compressed_sha256": manifest["compressed_sha256"],
            }
            snapshots.append(
                {
                    "artifact": artifact.name,
                    "manifest": _manifest_path(artifact).name,
                    "verified": True,
                    "source_schema_version": manifest["source_schema_version"],
                    "raw_size": manifest["raw_size"],
                    "compressed_size": manifest["compressed_size"],
                    "restore_ack": _ack("restore-v1", restore_value),
                    "retire_ack": _ack("retire-compressed-v1", retire_value),
                }
            )
    retire_value = {"database_name": database_path.name, "targets": legacy}
    return {
        "schema": BACKUP_PLAN_SCHEMA,
        "database": database,
        "legacy_backups": {
            "count": len(legacy),
            "total_bytes": sum(int(item["byte_size"]) for item in legacy),
            "targets": legacy,
            "retire_ack": _ack("retire-v1", retire_value) if legacy else None,
        },
        "compressed_snapshots": snapshots,
        "mutated": False,
    }


def retire_legacy_backups(database_path: Path, acknowledgement: str) -> dict[str, Any]:
    plan = backup_plan(database_path)
    legacy = plan["legacy_backups"]
    expected = legacy["retire_ack"]
    if not expected or acknowledgement != expected:
        raise RuntimeError("legacy backup retirement acknowledgement is stale or invalid")
    targets = [database_path.parent / item["name"] for item in legacy["targets"]]
    current = [_identity(path) for path in targets]
    if current != legacy["targets"]:
        raise RuntimeError("legacy backup set changed after the retirement plan")
    removed_bytes = 0
    removed = []
    for path, identity in zip(targets, current, strict=True):
        path.unlink()
        removed.append(path.name)
        removed_bytes += int(identity["byte_size"])
    _fsync_directory(database_path.parent)
    return {
        "schema": BACKUP_RESULT_SCHEMA,
        "action": "retire",
        "removed_count": len(removed),
        "removed_bytes": removed_bytes,
        "removed": removed,
    }


def retire_compressed_snapshot(
    database_path: Path, artifact_name: str, acknowledgement: str
) -> dict[str, Any]:
    if Path(artifact_name).name != artifact_name:
        raise RuntimeError("backup artifact must be an adjacent filename")
    plan = backup_plan(database_path)
    selected = next(
        (
            item
            for item in plan["compressed_snapshots"]
            if item.get("artifact") == artifact_name and item.get("verified") is True
        ),
        None,
    )
    if selected is None or acknowledgement != selected.get("retire_ack"):
        raise RuntimeError("compressed backup retirement acknowledgement is stale or invalid")
    artifact = database_path.parent / artifact_name
    manifest = _manifest_path(artifact)
    removed_bytes = artifact.stat().st_size + manifest.stat().st_size
    artifact.unlink()
    manifest.unlink()
    _fsync_directory(database_path.parent)
    return {
        "schema": BACKUP_RESULT_SCHEMA,
        "action": "retire",
        "removed_count": 2,
        "removed_bytes": removed_bytes,
        "removed": [artifact.name, manifest.name],
    }


def restore_compressed_snapshot(
    database_path: Path, artifact_name: str, acknowledgement: str
) -> dict[str, Any]:
    if Path(artifact_name).name != artifact_name:
        raise RuntimeError("backup artifact must be an adjacent filename")
    plan = backup_plan(database_path)
    selected = next(
        (
            item
            for item in plan["compressed_snapshots"]
            if item.get("artifact") == artifact_name and item.get("verified") is True
        ),
        None,
    )
    if selected is None or acknowledgement != selected.get("restore_ack"):
        raise RuntimeError("restore acknowledgement is stale or invalid")
    artifact = database_path.parent / artifact_name
    manifest = verify_compressed_snapshot(database_path, artifact)
    zstandard = _zstandard()
    transaction = _ensure_restore_tx_dir(database_path)
    prepared = transaction / _RESTORE_PREPARED_NAME
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".prepared.", suffix=".tmp", dir=transaction
    )
    temporary = Path(temporary_name)
    descriptor_open = True
    try:
        # Phase 1: prepare and fully validate the replacement database inside the private
        # transaction directory before any current namespace entry is touched.
        os.fchmod(descriptor, 0o600)
        raw_digest = hashlib.sha256()
        raw_size = 0
        with (
            artifact.open("rb") as source,
            zstandard.ZstdDecompressor().stream_reader(source) as reader,
            os.fdopen(descriptor, "wb", closefd=True) as destination,
        ):
            descriptor_open = False
            while chunk := reader.read(_CHUNK_BYTES):
                raw_digest.update(chunk)
                raw_size += len(chunk)
                destination.write(chunk)
            destination.flush()
            os.fsync(destination.fileno())
        if raw_size != manifest["raw_size"] or raw_digest.hexdigest() != manifest["raw_sha256"]:
            raise RuntimeError("restored database bytes do not match the backup manifest")
        with closing(sqlite3.connect(f"{temporary.as_uri()}?mode=ro", uri=True)) as connection:
            schema_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        if schema_version != manifest["source_schema_version"] or quick_check != "ok":
            raise RuntimeError("restored database failed SQLite validation")
        os.replace(temporary, prepared)
        os.chmod(prepared, 0o600)
        _fsync_directory(transaction)
        # Phase 2: durable intent before the first namespace mutation, so a crash at any
        # later point is recoverable to exactly one complete namespace.
        _write_restore_journal(database_path, artifact_name=artifact_name, committed=False)
        # Phase 3: move the original main and sidecars aside (never delete), then publish
        # the prepared database. A crash before the committed marker rolls the complete
        # original namespace back; the new main is never mixed with old WAL frames.
        os.replace(database_path, transaction / _RESTORE_ORIGINAL_MAIN_NAME)
        for suffix in _RESTORE_SIDECAR_SUFFIXES:
            sidecar = database_path.with_name(database_path.name + suffix)
            if sidecar.exists():
                os.replace(sidecar, _restore_original(database_path, suffix))
        os.replace(prepared, database_path)
        os.chmod(database_path, 0o600)
        _fsync_directory(database_path.parent)
        _fsync_directory(transaction)
        # Phase 4: record the committed state only after the new main is durably
        # published. Recovery keeps the new namespace from here on.
        _write_restore_journal(database_path, artifact_name=artifact_name, committed=True)
        _remove_restore_tx_dir(database_path)
    except BaseException:
        if descriptor_open:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        # Roll the *complete* namespace back (main plus sidecars) so a mid-swap failure
        # never leaves old WAL frames attached to a partially published new main.
        try:
            recover_interrupted_restore(database_path)
        except RuntimeError:
            # Leave the transaction namespace in place for the next startup recovery
            # rather than masking the original failure with a recovery error.
            pass
        raise
    finally:
        if descriptor_open:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
    return {
        "schema": BACKUP_RESULT_SCHEMA,
        "action": "restore",
        "restored": True,
        "artifact": artifact.name,
        "schema_version": manifest["source_schema_version"],
        "raw_size": manifest["raw_size"],
    }

"""Clone the mutable private namespace so ACK/cleanup cannot damage rollback."""

from __future__ import annotations

import os
import sqlite3
import stat
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any

from sightglass.model.backups import _fsync_directory, _hash_file, _private_regular_file


def _helper_state(path: Path, *, executable: bool = True) -> dict[str, Any]:
    """Bind the default executable and its private two-level namespace without running it."""
    root = path.parent.parent
    for directory in (root, path.parent):
        try:
            metadata = directory.lstat()
        except FileNotFoundError:
            continue
        if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) & 0o077):
            raise RuntimeError("unsafe private voice helper directory")
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return {"present": False}
    if (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1
            or metadata.st_uid != os.getuid() or metadata.st_size <= 0
            or stat.S_IMODE(metadata.st_mode) & 0o7077
            or executable and not metadata.st_mode & stat.S_IXUSR):
        raise RuntimeError("voice helper must be a private owned nonempty executable")
    digest, size = _hash_file(path)
    after = path.lstat()
    def revision(item: os.stat_result) -> list[int]:
        return [item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns,
                item.st_ctime_ns, stat.S_IMODE(item.st_mode), item.st_uid]
    if revision(after) != revision(metadata) or size != metadata.st_size:
        raise RuntimeError("voice helper changed while staging")
    # Recheck parent safety after the no-follow read as well as before it.
    for directory in (root, path.parent):
        current = directory.lstat()
        if (not stat.S_ISDIR(current.st_mode) or current.st_uid != os.getuid()
                or stat.S_IMODE(current.st_mode) & 0o077):
            raise RuntimeError("unsafe private voice helper directory")
    return {"present": True, "digest": digest, "bytes": size, "revision": revision(after)}


def _linux_helper_bundle(path: Path) -> dict[str, dict[str, Any]]:
    """Bind only the default helper's declared model/manifest and build receipt."""
    from sightglass.voice.linux import resolve_model_binding

    manifest = path.parent / "models" / "model.json"
    manifest_state = _helper_state(manifest, executable=False)
    model = resolve_model_binding(path).path
    if not manifest_state["present"] or model is None:
        raise RuntimeError("voice helper model binding is unavailable")
    model_state = _helper_state(model, executable=False)
    if not model_state["present"]:
        raise RuntimeError("voice helper model is unavailable")
    return {
        "models/model.json": manifest_state,
        f"models/{model.name}": model_state,
        "helper-build.json": _helper_state(path.parent / "helper-build.json", executable=False),
    }


def relocated_path(value: str, old: Path, new: Path) -> str:
    path = Path(value)
    relative = path.relative_to(old)
    if ".." in relative.parts:
        raise RuntimeError("private state reference escapes its namespace")
    return str(new / relative)


def clone_state(
    database: Path, old: Path, old_data: Path, new: Path, *, capacity: Callable[[int], Any],
    default_voice_helper: bool = False,
    voice_helper_name: str = "sightglass-transcribe",
) -> list[dict[str, Any]]:
    """Clone referenced payloads/CAS, known sidecars and the optional default helper.

    The caller holds the stopped installation lock. File copies are bounded,
    digest-verified, single-linked and fsynced; the old namespace is never mutated.
    """
    files: list[dict[str, Any]] = []

    def copy(
        source: Path, destination: Path, expected: str | None = None, *, mode: int = 0o600,
    ) -> None:
        parent = source.parent
        while parent != (old if source.is_relative_to(old) else old_data):
            if parent.is_symlink() or parent.stat().st_mode & 0o077:
                raise RuntimeError("unsafe private source directory")
            parent = parent.parent
        metadata = _private_regular_file(source)
        capacity(metadata.st_size + 4096)
        relative = destination.relative_to(new)
        parent = new
        for part in relative.parts[:-1]:
            parent /= part
            parent.mkdir(mode=0o700, exist_ok=True)
            if parent.is_symlink() or parent.stat().st_mode & 0o077:
                raise RuntimeError("unsafe private state directory")
        before = _hash_file(source)
        if expected is not None and before[0] != expected:
            raise RuntimeError("private state digest mismatch")
        if not destination.exists():
            with os.fdopen(os.open(source, os.O_RDONLY | os.O_NOFOLLOW), "rb") as reader:
                with destination.open("xb") as writer:
                    os.fchmod(writer.fileno(), mode)
                    while chunk := reader.read(1024**2):
                        capacity(len(chunk))
                        writer.write(chunk)
                    writer.flush()
                    os.fsync(writer.fileno())
        if _hash_file(destination) != before or _hash_file(source) != before:
            raise RuntimeError("private state changed while staging")
        _private_regular_file(destination)
        _fsync_directory(destination.parent)
        files.append({"path": str(destination), "digest": before[0], "bytes": before[1]})

    if default_voice_helper:
        if voice_helper_name not in {"sightglass-transcribe", "sightglass-whisper"}:
            raise RuntimeError("unknown default voice helper")
        source = old_data / "voice" / voice_helper_name
        target = new / "voice" / voice_helper_name
        initial = _helper_state(source)
        item: dict[str, Any] = {"path": str(target)}
        if initial["present"]:
            copy(source, target, initial["digest"], mode=initial["revision"][-2])
            if _helper_state(source) != initial:
                raise RuntimeError("voice helper changed while staging")
            item = files[-1]
        else:
            files.append(item)
        item.update(kind="default_voice_helper", source_path=str(source),
                    source_state=initial, target_state=_helper_state(target))
        if initial["present"] and voice_helper_name == "sightglass-whisper":
            bundle = _linux_helper_bundle(source)
            for relative, evidence in bundle.items():
                if evidence["present"]:
                    copy(source.parent / relative, target.parent / relative, evidence["digest"])
            if _linux_helper_bundle(source) != bundle:
                raise RuntimeError("voice helper model changed while staging")
            item.update(
                bundle_source_state=bundle, bundle_target_state=_linux_helper_bundle(target),
            )

    secret = old / "token-secret"
    if secret.exists():
        copy(secret, new / "token-secret")
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        for identity, reference, digest, status in connection.execute(
            "SELECT delivery_id,payload_ref,payload_digest,status FROM reader_deliveries"
        ):
            source = Path(reference)
            if source.parent != old / "deliveries" or source.name != f"{identity}.json":
                raise RuntimeError("delivery reference is outside its private spool")
            target = Path(relocated_path(reference, old, new))
            if status == "pending" or source.exists():
                copy(source, target, digest)
            connection.execute(
                "UPDATE reader_deliveries SET payload_ref=? WHERE delivery_id=?",
                (str(target), identity),
            )
        for digest, reference, size in connection.execute(
            "SELECT object_digest,local_path_internal,byte_size FROM resource_objects"
        ):
            source = Path(reference)
            if source.parent != old / "resource-cache" / "objects" or source.name != digest:
                raise RuntimeError("resource reference is outside its CAS namespace")
            if _private_regular_file(source).st_size != size:
                raise RuntimeError("private resource size mismatch")
            target = Path(relocated_path(reference, old, new))
            copy(source, target, digest)
            connection.execute(
                "UPDATE resource_objects SET local_path_internal=? WHERE object_digest=?",
                (str(target), digest),
            )
        # Completed request outcomes may outlive reader ACK and also include empty
        # pages. Their opaque spool IDs relocate through the same independent root.
        from sightglass.reader.replica import UPDATE_REQUEST_TOOL, request_spool_ids

        retained_requests = request_spool_ids(connection)
        for row in connection.execute(
            "SELECT warning_codes_json FROM access_receipts WHERE tool_name=?",
            (UPDATE_REQUEST_TOOL,),
        ):
            import json

            metadata = json.loads(row[0])
            identity = metadata.get("payload_id")
            if identity not in retained_requests:
                continue
            source = old / "deliveries" / f"{identity}.json"
            target = new / "deliveries" / source.name
            copy(source, target, metadata["payload_digest"])
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    # These JSON sidecars contain no query text and have independent validation.
    for source, target in (
        (old / "search-preparation.json", new / "search-preparation.json"),
        (old_data / "storage-history.json", new / "storage-history.json"),
    ):
        if source.exists():
            copy(source, target)
    semantic = old_data / "semantic" / "index.db"
    if semantic.exists():
        from sightglass.model.compact_candidate import file_revision

        before = file_revision(semantic)
        capacity(_private_regular_file(semantic).st_size + 1024**2)
        parent = new / "semantic"
        parent.mkdir(mode=0o700)
        target = parent / "index.db"
        with (
            closing(sqlite3.connect(semantic.as_uri() + "?mode=ro", uri=True)) as reader,
            closing(sqlite3.connect(target)) as writer,
        ):
            os.chmod(target, 0o600)
            reader.execute("PRAGMA query_only=ON")
            reader.backup(writer, pages=256, progress=lambda *args: capacity(1024**2))
            if writer.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise RuntimeError("semantic sidecar consistency failure")
        if file_revision(semantic) != before:
            raise RuntimeError("semantic sidecar changed while staging")
        with target.open("rb") as handle:
            os.fsync(handle.fileno())
        digest, size = _hash_file(target)
        files.append({"path": str(target), "digest": digest, "bytes": size})
        _fsync_directory(parent)
    _fsync_directory(new)
    return files


def verify_initial_state(files: list[dict[str, Any]]) -> None:
    for item in files:
        path = Path(item["path"])
        if item.get("kind") == "default_voice_helper":
            if (_helper_state(Path(item["source_path"])) != item["source_state"]
                    or _helper_state(path) != item["target_state"]):
                raise RuntimeError("voice helper private state changed before activation")
            if "bundle_source_state" in item and (
                _linux_helper_bundle(Path(item["source_path"])) != item["bundle_source_state"]
                or _linux_helper_bundle(path) != item["bundle_target_state"]
            ):
                raise RuntimeError("voice helper model state changed before activation")
            continue
        _private_regular_file(path)
        if _hash_file(path) != (item["digest"], item["bytes"]):
            raise RuntimeError("staged private state changed before activation")

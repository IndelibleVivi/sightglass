"""Stopped installation inventory and exact relocation of received private state.

The only DB transformations are owned delivery/CAS paths. Source resolver JSON,
account keys, identities, observations, FTS and ACK/cursor state remain exact.
The frozen recovery stays untouched. This module has no activation or deletion.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import shlex
import sqlite3
import subprocess
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import replace
from itertools import zip_longest
from pathlib import Path
from typing import Any, BinaryIO, cast

from sightglass.model.backups import checkpoint_database
from sightglass.reader.replica import UPDATE_REQUEST_TOOL, request_spool_ids

from .config import SightglassConfig
from .migration import (
    FrozenTransfer,
    _fsync_directory,
    _hash,
    _member,
    _private_directory,
    _regular,
    freeze_files,
    send_frozen,
)
from .paired_state import relocated_path


@contextmanager
def stopped_installation(config: SightglassConfig) -> Iterator[None]:
    """Own the very same lock as daemon startup, before checkpoint/inventory/freeze."""
    directory = config.socket_path.parent
    _private_directory(directory)
    path = directory / "sightglassd.lock"
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        _regular(path)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("stop the full runtime before freezing installation state") from exc
        yield
    finally:
        os.close(descriptor)


def _readonly(path: Path) -> sqlite3.Connection:
    _regular(path)
    connection = sqlite3.connect(path.absolute().as_uri() + "?mode=ro", uri=True)
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA cache_size=-8192")
    return connection


def installation_members(config: SightglassConfig) -> tuple[str, ...]:
    """Enumerate canonical owned references; never recursively export a runtime root.

    Must run under stopped_installation. The data directory must contain its DB
    namespace. Native enrollment files, Keychain material, configs, logs, stale
    caches, lock/socket and arbitrary files are deliberately not input members.
    """
    root, namespace = config.data_dir, config.window_db_path.parent
    _private_directory(root)
    if not namespace.is_relative_to(root):
        raise RuntimeError("migration requires the DB namespace inside its configured data root")
    members = {config.window_db_path.relative_to(root).as_posix()}

    def include(path: Path, expected: str | None = None, size: int | None = None) -> None:
        if not path.is_relative_to(root):
            raise RuntimeError("owned reference escapes the installation root")
        relative = path.relative_to(root).as_posix()
        metadata = _regular(_member(root, relative))
        if size is not None and metadata.st_size != size:
            raise RuntimeError("owned payload size does not match its durable reference")
        if expected is not None and _hash(path) != expected:
            raise RuntimeError("owned payload digest does not match its durable reference")
        members.add(relative)

    include(namespace / "token-secret")
    with closing(_readonly(config.window_db_path)) as connection:
        connection.execute("BEGIN")
        for identity, reference, digest, status in connection.execute(
            "SELECT delivery_id,payload_ref,payload_digest,status FROM reader_deliveries"
        ):
            source = Path(reference)
            if source.parent != namespace / "deliveries" or source.name != f"{identity}.json":
                raise RuntimeError("delivery reference is outside its canonical namespace")
            if status == "pending" or source.exists() or source.is_symlink():
                include(source, digest)
        retained = request_spool_ids(connection)
        for (metadata,) in connection.execute(
            "SELECT warning_codes_json FROM access_receipts WHERE tool_name=?",
            (UPDATE_REQUEST_TOOL,),
        ):
            record = json.loads(metadata)
            identity = record.get("payload_id")
            if identity in retained:
                include(namespace / "deliveries" / f"{identity}.json", record["payload_digest"])
        for digest, reference, size in connection.execute(
            "SELECT object_digest,local_path_internal,byte_size FROM resource_objects"
        ):
            source = Path(reference)
            if source.parent != namespace / "resource-cache" / "objects" or source.name != digest:
                raise RuntimeError("resource reference is outside its canonical CAS namespace")
            include(source, digest, size)
    for path in (
        namespace / "search-preparation.json",
        root / "storage-history.json",
        root / "voice" / "sightglass-transcribe",
    ):
        if path.exists() or path.is_symlink():
            include(path)
    if not config.voice_helper_path.strip():
        from .paired_state import _linux_helper_bundle
        from .voice_setup import resolve_helper_path

        helper = resolve_helper_path(config)
        if helper.name == "sightglass-whisper" and (helper.exists() or helper.is_symlink()):
            include(helper)
            for relative, evidence in _linux_helper_bundle(helper).items():
                if evidence["present"]:
                    include(helper.parent / relative, evidence["digest"], evidence["bytes"])
    semantic = root / "semantic" / "index.db"
    if semantic.exists() or semantic.is_symlink():
        checkpoint_database(semantic)
        include(semantic)
    return tuple(sorted(members))


@contextmanager
def frozen_installation(
    config: SightglassConfig, *, edge_state: dict[str, Any] | None = None
) -> Iterator[FrozenTransfer]:
    from .activation import require_core_activation
    from .migration_cut import logical_installation_cut

    with stopped_installation(config):
        require_core_activation(config)
        checkpoint_database(config.window_db_path)
        members = installation_members(config)
        cut = logical_installation_cut(config, edge_state=edge_state)
        plan = replace(freeze_files(config.data_dir, members), logical_cut=cut)
        if logical_installation_cut(config, edge_state=edge_state) != cut:
            raise RuntimeError("installation logical cut changed during its file freeze")
        yield plan


def send_installation(
    config: SightglassConfig,
    *,
    host: str,
    identity_file: Path,
    remote_python: str,
    destination: str,
    edge_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Operator-only direct SSH transfer while holding the existing stopped lock.

    No second full source copy is made. An interrupted destination resumes only this
    exact frozen input; source changes require another destination. Encryption and old
    owner revocation are explicit operator preconditions, never inferred from SSH.
    """
    from .edge_relay import SSHRelayConnector

    for path in (remote_python, destination):
        if not path or "\x00" in path or not Path(path).is_absolute() or ".." in Path(path).parts:
            raise RuntimeError("migration remote paths must be explicit absolute paths")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,255}", host):
        raise RuntimeError("invalid migration SSH host alias")
    _regular(identity_file)
    argv = SSHRelayConnector(host, identity_file).argv()
    argv[-1] = shlex.join(
        [remote_python, "-m", "sightglass.runtime.migration", "--destination", destination]
    )
    with frozen_installation(config, edge_state=edge_state) as plan:
        process = subprocess.Popen(
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )
        assert process.stdin is not None and process.stdout is not None
        try:
            send_frozen(
                config.data_dir,
                plan,
                cast(BinaryIO, process.stdout),
                cast(BinaryIO, process.stdin),
            )
            process.stdin.close()
            if process.wait(timeout=15) != 0:
                raise RuntimeError("frozen remote receiver did not exit successfully")
        finally:
            process.stdin.close()
            process.stdout.close()
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        return {
            "schema": "sightglass.frozen-installation-receipt.v1",
            "verified": True,
            "activated": False,
            "manifest": plan.as_dict(),
            "files": len(plan.files),
            "bytes": sum(item.size for item in plan.files),
        }


def relocate_received(database: Path, *, old_namespace: Path, new_namespace: Path) -> None:
    """Apply the two declared path relocations in a received candidate only."""
    _regular(database)
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("BEGIN IMMEDIATE")
        for table, identity_field, path_field, directory in (
            ("reader_deliveries", "delivery_id", "payload_ref", "deliveries"),
            ("resource_objects", "object_digest", "local_path_internal", "resource-cache/objects"),
        ):
            for identity, reference in connection.execute(
                f'SELECT "{identity_field}","{path_field}" FROM "{table}"'
            ):
                source = Path(reference)
                expected_name = f"{identity}.json" if table == "reader_deliveries" else identity
                if source.parent != old_namespace / directory or source.name != expected_name:
                    raise RuntimeError("received state contains a noncanonical owned reference")
                destination = relocated_path(reference, old_namespace, new_namespace)
                connection.execute(
                    f'UPDATE "{table}" SET "{path_field}"=? WHERE "{identity_field}"=?',
                    (destination, identity),
                )
        if connection.execute("PRAGMA foreign_key_check").fetchone():
            raise RuntimeError("relocated state has a foreign key violation")
        connection.commit()
        row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if row is None or row[0] != 0:
            raise RuntimeError("relocated state checkpoint did not complete")
    with database.open("rb") as handle:
        os.fsync(handle.fileno())
    _fsync_directory(database.parent)


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def verify_relocated(
    frozen_database: Path,
    candidate_database: Path,
    *,
    old_namespace: Path,
    new_namespace: Path,
) -> dict[str, Any]:
    """Stream every table/row/byte, allowing only declared owned path substitutions."""
    counts = {}
    with (
        closing(_readonly(frozen_database)) as source,
        closing(_readonly(candidate_database)) as target,
    ):
        source.execute("BEGIN")
        target.execute("BEGIN")
        schema = list(
            source.execute("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY name")
        )
        if (
            schema
            != list(
                target.execute("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY name")
            )
            or source.execute("PRAGMA user_version").fetchone()
            != target.execute("PRAGMA user_version").fetchone()
        ):
            raise RuntimeError("relocation changed the database schema")
        for kind, table, _owner, sql in schema:
            if kind != "table":
                continue
            columns = list(source.execute(f"PRAGMA table_info({_quoted(table)})"))
            names = [row[1] for row in columns]
            without_rowid = bool(sql and "WITHOUT ROWID" in sql.upper())
            keys = [row[1] for row in sorted(columns, key=lambda row: row[5]) if row[5]]
            projection = ",".join(_quoted(name) for name in names)
            if not without_rowid:
                projection = "rowid," + projection
            order = ",".join(_quoted(name) for name in keys) if without_rowid else "rowid"
            statement = f"SELECT {projection} FROM {_quoted(table)} ORDER BY {order}"
            count = 0
            for before, after in zip_longest(source.execute(statement), target.execute(statement)):
                if before is None or after is None:
                    raise RuntimeError("relocation changed durable row cardinality")
                expected = list(before)
                path_field = (
                    "payload_ref"
                    if table == "reader_deliveries"
                    else "local_path_internal"
                    if table == "resource_objects"
                    else None
                )
                if path_field is not None:
                    index = names.index(path_field) + (0 if without_rowid else 1)
                    expected[index] = relocated_path(expected[index], old_namespace, new_namespace)
                if tuple(expected) != tuple(after):
                    raise RuntimeError("relocation changed a durable row outside owned path fields")
                count += 1
            counts[table] = count
        if target.execute("PRAGMA foreign_key_check").fetchone():
            raise RuntimeError("relocated state has a foreign key violation")
        if target.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("relocated state consistency failure")
    return {
        "schema": "sightglass.relocation-verify.v1",
        "verified": True,
        "table_row_counts": counts,
        "allowed_transformations": [
            "reader_deliveries.payload_ref",
            "resource_objects.local_path_internal",
        ],
    }

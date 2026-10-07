"""Host/namespace-bound ownership, outside the exported WindowDB namespace.

An activation is a local write guard, not a distributed lease. Cross-host transfer
still stops/revokes the previous supervisor, tunnel and credentials first. Recovery
copies receive no writer credential; rollback needs a new generation and the latest
authoritative reader state. The old installed release must be revoked separately.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import plistlib
import re
import secrets
import stat
import subprocess
import sys
import tempfile
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .config import SightglassConfig

ACTIVATION_SCHEMA = "sightglass.activation.v2"
MAX_ACTIVATION_BYTES = 8192


@lru_cache(maxsize=1)
def host_identity() -> str:
    """A machine identity comparison, never projected to MCP or public receipts."""
    if sys.platform == "darwin":
        result = subprocess.run(
            ["/usr/sbin/ioreg", "-rd1", "-c", "IOPlatformExpertDevice", "-a"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
        value = plistlib.loads(result.stdout)
        identity = value[0].get("IOPlatformUUID") if isinstance(value, list) and value else None
    elif sys.platform.startswith("linux"):
        identity = Path("/etc/machine-id").read_text().strip()
    else:
        raise RuntimeError("installation host identity is unsupported")
    if not isinstance(identity, str) or not identity or len(identity) > 256:
        raise RuntimeError("installation host identity is unavailable")
    return hashlib.sha256(b"sightglass-host-owner-v1\x00" + identity.encode()).hexdigest()


def _namespace(path: Path) -> str:
    absolute = path.expanduser().absolute()
    if ".." in absolute.parts or absolute.resolve() != absolute:
        raise RuntimeError("activation namespace must not traverse a symlink")
    return str(absolute)


def _private_bytes(path: Path, *, maximum: int) -> bytes:
    parent = path.parent.lstat()
    if (
        not stat.S_ISDIR(parent.st_mode)
        or parent.st_uid != os.getuid()
        or stat.S_IMODE(parent.st_mode) & 0o077
    ):
        raise RuntimeError("unsafe installation activation directory")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != os.getuid()
            or stat.S_IMODE(before.st_mode) & 0o077
            or not 0 < before.st_size <= maximum
        ):
            raise RuntimeError("unsafe installation activation record")
        data = os.read(descriptor, maximum + 1)
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ) or len(data) > maximum:
            raise RuntimeError("installation activation changed during read")
        return data
    finally:
        os.close(descriptor)


def _credential(path: Path) -> Path:
    return path.with_name(path.name + ".credential")


def read_activation(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(_private_bytes(path, maximum=MAX_ACTIVATION_BYTES))
    except (OSError, ValueError) as exc:
        raise RuntimeError("installation activation record is unavailable") from exc
    keys = {
        "schema",
        "generation",
        "state",
        "role",
        "predecessor",
        "counter",
        "host_id",
        "namespace",
        "credential_digest",
    }
    if (
        not isinstance(value, dict)
        or set(value) != keys
        or value.get("schema") != ACTIVATION_SCHEMA
        or not isinstance(value.get("generation"), str)
        or not value["generation"]
        or value.get("state") not in {"active", "revoked"}
        or value.get("role") not in {"core", "edge"}
        or type(value.get("counter")) is not int
        or value["counter"] < 1
        or not isinstance(value.get("namespace"), str)
        or not Path(value["namespace"]).is_absolute()
        or not isinstance(value.get("host_id"), str)
        or not re.fullmatch(r"[a-f0-9]{64}", value["host_id"])
        or not isinstance(value.get("credential_digest"), str)
        or not re.fullmatch(r"[a-f0-9]{64}", value["credential_digest"])
        or value["predecessor"] is not None
        and not isinstance(value["predecessor"], str)
    ):
        raise RuntimeError("invalid installation activation record")
    return value


def require_activation(path: Path, *, generation: str, role: str, namespace: Path) -> None:
    value = read_activation(path)
    if value["generation"] != generation or value["state"] != "active" or value["role"] != role:
        raise RuntimeError(f"this installation has no active {role} ownership")
    if value["host_id"] != host_identity() or value["namespace"] != _namespace(namespace):
        raise RuntimeError("installation ownership does not cover this host/namespace")
    try:
        credential = _private_bytes(_credential(path), maximum=64)
    except OSError as exc:
        raise RuntimeError("installation writer credential is unavailable") from exc
    if len(credential) != 32 or not hmac.compare_digest(
        hashlib.sha256(credential).hexdigest(), value["credential_digest"]
    ):
        raise RuntimeError("installation writer credential does not match its grant")


def require_core_activation(config: SightglassConfig) -> None:
    if not config.activation_generation and config.activation_path is None:
        if config.source_kind == "remote-capture":
            raise RuntimeError("remote core requires an explicit activation generation")
        return
    if config.activation_path is None:
        raise RuntimeError("installation activation binding is incomplete")
    if config.activation_path.is_relative_to(config.data_dir):
        raise RuntimeError("core activation must remain outside the exported data namespace")
    require_activation(
        config.activation_path,
        generation=config.activation_generation,
        role="core",
        namespace=config.window_db_path,
    )


def write_activation(
    path: Path,
    *,
    generation: str,
    state: str,
    role: str,
    namespace: Path,
    expected_generation: str | None = None,
    predecessor: str | None = None,
    counter: int | None = None,
) -> None:
    """Publish authorized ownership under the stopped installation lock.

    A new generation increases the counter and requires its exact predecessor.
    Revocation cannot be undone by reactivating the same generation.
    """
    if not generation or state not in {"active", "revoked"} or role not in {"core", "edge"}:
        raise RuntimeError("invalid installation activation transition")
    target = _namespace(namespace)
    previous = None
    if expected_generation is not None:
        previous = read_activation(path)
        if previous["generation"] != expected_generation:
            raise RuntimeError("installation ownership changed before transition")
        if previous["host_id"] != host_identity():
            raise RuntimeError("installation ownership belongs to another host")
        if generation == previous["generation"]:
            if (
                state == "active"
                and previous["state"] == "revoked"
                or target != previous["namespace"]
                or role != previous["role"]
            ):
                raise RuntimeError("reactivation requires a new generation and predecessor")
        elif predecessor != previous["generation"]:
            raise RuntimeError("new generation requires its exact predecessor")
    elif path.exists() or path.is_symlink():
        raise RuntimeError("existing activation needs an exact expected generation")
    expected_counter = (
        (previous["counter"] + int(generation != previous["generation"])) if previous else 1
    )
    if previous is None and predecessor is not None:
        if type(counter) is not int or counter < 2:
            raise RuntimeError("cross-host successor requires its operator ownership counter")
        expected_counter = counter
    if counter is not None and (type(counter) is not int or counter != expected_counter):
        raise RuntimeError("installation ownership counter cannot reset")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    parent = path.parent.lstat()
    if (
        not stat.S_ISDIR(parent.st_mode)
        or parent.st_uid != os.getuid()
        or stat.S_IMODE(parent.st_mode) & 0o077
    ):
        raise RuntimeError("unsafe installation activation directory")
    credential_path = _credential(path)
    if previous is None:
        descriptor = os.open(
            credential_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(secrets.token_bytes(32))
            handle.flush()
            os.fsync(handle.fileno())
    credential = _private_bytes(credential_path, maximum=64)
    if (
        len(credential) != 32
        or previous is not None
        and not hmac.compare_digest(
            hashlib.sha256(credential).hexdigest(), previous["credential_digest"]
        )
    ):
        raise RuntimeError("installation writer credential changed before transition")
    payload = (
        json.dumps(
            {
                "schema": ACTIVATION_SCHEMA,
                "generation": generation,
                "state": state,
                "role": role,
                "predecessor": predecessor,
                "counter": expected_counter,
                "host_id": host_identity(),
                "namespace": target,
                "credential_digest": hashlib.sha256(credential).hexdigest(),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        + b"\n"
    )
    descriptor, name = tempfile.mkstemp(prefix=".activation-", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        Path(name).unlink(missing_ok=True)

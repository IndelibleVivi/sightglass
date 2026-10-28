"""Fixed SSH capability for one enrolled Linux vault; no shell or generic RPC.

Deployment requires an operator-owned private configuration and forced SSH command.
The legacy 32-byte stdin unlock protocol is retained. Status never reads a key.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

STATUS_FRAME = b"sightglass-vault-status-v1\n"
STATUS_SCHEMA = "sightglass.vault-status.v1"


def private_file(path: Path) -> None:
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o077 or info.st_nlink != 1):
        raise ValueError("unsafe private file")


def load_config(path: Path) -> dict[str, Any]:
    private_file(path)
    config = json.loads(path.read_bytes())
    if set(config) != {"image", "uuid", "mapping", "mount", "marker", "services", "lock"}:
        raise ValueError("invalid configuration")
    for name in ("image", "mount", "marker", "lock"):
        if not isinstance(config[name], str) or not Path(config[name]).is_absolute():
            raise ValueError("invalid path")
    paths = [Path(config[n]).resolve() for n in ("image", "mount", "marker", "lock")]
    if len(set(paths + [path.resolve()])) != 5:
        raise ValueError("overlapping paths")
    if (not paths[2].is_relative_to(paths[1]) or paths[0].is_relative_to(paths[1])
            or paths[3].is_relative_to(paths[1]) or path.resolve().is_relative_to(paths[1])):
        raise ValueError("invalid mount boundary")
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", config["mapping"]):
        raise ValueError("invalid mapping")
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", config["uuid"]):
        raise ValueError("invalid UUID")
    if (not isinstance(config["services"], list) or not config["services"]
            or any(not isinstance(s, str) or not re.fullmatch(r"[\w@.-]+\.service", s)
                   for s in config["services"])):
        raise ValueError("invalid services")
    return config


class Gateway:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.deadline = time.monotonic() + 115

    def run(self, *args: str, key: bytes | None = None) -> subprocess.CompletedProcess[bytes]:
        # Never project command output, arguments, exceptions or key bytes to SSH.
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("operation deadline")
        with subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, start_new_session=True,
                              env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL": "C"}) as p:
            try:
                out, err = p.communicate(key, timeout=min(40, remaining))
            except BaseException:
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                p.wait()
                raise
            return subprocess.CompletedProcess(args, p.returncode, out, err)

    def checked(self, *args: str, key: bytes | None = None) -> bytes:
        result = self.run(*args, key=key)
        if result.returncode:
            raise ValueError("vault operation failed")
        return result.stdout.strip()

    def status(self) -> dict[str, Any]:
        c = self.config
        private_file(Path(c["image"]))
        if self.checked("cryptsetup", "luksUUID", c["image"]).decode() != c["uuid"]:
            raise ValueError("wrong image")
        node = Path("/dev/mapper") / c["mapping"]
        mapped = node.exists()
        if mapped:
            details = self.checked("cryptsetup", "status", c["mapping"]).decode()
            devices = re.findall(r"^\s*device:\s*(/\S+)\s*$", details, re.MULTILINE)
            if len(devices) != 1:
                raise ValueError("unproved mapping")
            if self.checked("cryptsetup", "luksUUID", devices[0]).decode() != c["uuid"]:
                raise ValueError("wrong mapping")
            loops = re.findall(r"^\s*loop:\s*(/\S+)\s*$", details, re.MULTILINE)
            if len(loops) != 1 or os.path.realpath(loops[0]) != os.path.realpath(c["image"]):
                raise ValueError("wrong backing image")
        mounted = self.run("findmnt", "-n", "-o", "SOURCE", "--mountpoint", c["mount"])
        if mounted.returncode not in (0, 1):
            raise ValueError("mount probe failed")
        present = mounted.returncode == 0
        if present and (not mapped or os.path.realpath(mounted.stdout.strip().decode())
                        != os.path.realpath(node)):
            raise ValueError("foreign mount")
        active = all(self.run("systemctl", "is-active", "--quiet", s).returncode == 0
                     for s in c["services"])
        return {"schema": STATUS_SCHEMA, "mapped": mapped, "mounted": present,
                "deployment_ready": present and Path(c["marker"]).is_file(),
                "services_active": active}

    def unlock(self, key: bytes, *, automatic: bool = False) -> dict[str, bool]:
        c = self.config
        state = self.status()
        if automatic and state["mounted"]:
            # Another owner mounted/stopped it after the Mac probe. Preserve that state.
            return {"unlocked": True, "deployment_ready": state["deployment_ready"]}
        # Test even when already mounted, retaining the old manual protocol guarantee.
        self.checked("cryptsetup", "open", "--test-passphrase", "--key-file", "-",
                     c["image"], key=key)
        if not state["mapped"]:
            self.checked("cryptsetup", "open", "--type", "luks", "--key-file", "-",
                         c["image"], c["mapping"], key=key)
        if not state["mounted"]:
            self.checked("mount", "-o", "nodev,nosuid", "/dev/mapper/" + c["mapping"],
                         c["mount"])
        state = self.status()
        if not state["mounted"]:
            raise ValueError("mount not confirmed")
        if state["deployment_ready"]:
            self.checked("systemctl", "start", *c["services"])
        return {"unlocked": True, "deployment_ready": state["deployment_ready"]}


def serve(config: dict[str, Any], frame: bytes) -> dict[str, Any]:
    automatic = len(frame) == 33 and frame[:1] == b"R"
    if frame != STATUS_FRAME and len(frame) != 32 and not automatic:
        raise ValueError("invalid frame")
    fd = os.open(config["lock"], os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_nlink != 1 or info.st_mode & 0o077):
            raise ValueError("unsafe lock")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        gateway = Gateway(config)
        if frame == STATUS_FRAME:
            return gateway.status()
        return gateway.unlock(frame[1:], automatic=True) if automatic else gateway.unlock(frame)
    finally:
        os.close(fd)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    # Bound incomplete stdin, flock, cryptsetup and systemd as one SSH operation.
    def expired(_signum: int, _frame: Any) -> None:
        # Raising lets subprocess.run kill/wait its child before serve releases flock.
        raise TimeoutError("operation deadline")
    signal.signal(signal.SIGALRM, expired)
    signal.signal(signal.SIGTERM, expired)
    signal.alarm(120)
    try:
        result = serve(load_config(args.config), sys.stdin.buffer.read(34))
    except Exception:
        print('{"error":"vault_unavailable"}')
        return 1
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

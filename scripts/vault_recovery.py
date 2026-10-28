"""One-shot Mac vault recovery for launchd; requires explicit installation consent.

Uses an existing trusted noninteractive Keychain helper and a fixed SSH capability.
No key file, interactive authorization, source read, migration or data initialization.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import stat
import subprocess
import tempfile
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


def load_config(path: Path) -> dict[str, str]:
    private_file(path)
    config = json.loads(path.read_bytes())
    if set(config) != {"ssh_alias", "keychain_helper", "keychain_account",
                       "receipt", "lock", "inhibit"}:
        raise ValueError("invalid configuration")
    if any(not isinstance(v, str) or not v for v in config.values()):
        raise ValueError("invalid configuration")
    if config["ssh_alias"].startswith("-") or any(c.isspace() for c in config["ssh_alias"]):
        raise ValueError("invalid SSH alias")
    for name in ("keychain_helper", "receipt", "lock", "inhibit"):
        if not Path(config[name]).is_absolute():
            raise ValueError("invalid path")
    parent = Path(config["receipt"]).parent
    info = parent.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise ValueError("unsafe receipt directory")
    if any(Path(config[n]).parent != parent for n in ("receipt", "lock", "inhibit")):
        raise ValueError("state must share private directory")
    paths = [Path(config[n]).resolve() for n in ("receipt", "lock", "inhibit")]
    if len(set(paths + [path.resolve(), Path(config["keychain_helper"]).resolve()])) != 5:
        raise ValueError("overlapping paths")
    return config


class Recovery:
    def __init__(self, config: dict[str, str]) -> None:
        self.config = config

    def run(self, args: list[str], data: bytes | None = None) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(args, input=data, capture_output=True, timeout=125, check=False)

    def remote(self, frame: bytes) -> dict[str, Any]:
        result = self.run(["/usr/bin/ssh", "-T", "-o", "BatchMode=yes", "-o",
                           "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes", "-o",
                           "ConnectTimeout=10", "-o", "ConnectionAttempts=1", "-o",
                           "ClearAllForwardings=yes", "-o", "PermitLocalCommand=no", "-o",
                           "ControlMaster=no", "-o", "ControlPath=none", "-o",
                           "ControlPersist=no", self.config["ssh_alias"]], frame)
        if result.returncode or len(result.stdout) > 4096:
            raise ValueError("SSH unavailable")
        return json.loads(result.stdout)

    def status(self) -> dict[str, Any]:
        state = self.remote(STATUS_FRAME)
        if (not isinstance(state, dict) or set(state) != {"schema", "mapped", "mounted",
                "deployment_ready", "services_active"} or state["schema"] != STATUS_SCHEMA
                or any(type(state[k]) is not bool for k in state if k != "schema")
                or state["mounted"] and not state["mapped"]
                or state["deployment_ready"] and not state["mounted"]):
            raise ValueError("unproved status")
        return state

    def attempt(self) -> str:
        if Path(self.config["inhibit"]).exists():
            return "inhibited"
        state = self.status()
        if state["mounted"]:
            if not state["deployment_ready"]:
                return "deployment_not_ready"
            return "healthy" if state["services_active"] else "services_stopped"
        if Path(self.config["inhibit"]).exists():
            return "inhibited"
        key = self.run([self.config["keychain_helper"], "emit", self.config["keychain_account"]])
        if key.returncode or len(key.stdout) != 32:
            return "keychain_unavailable"
        if Path(self.config["inhibit"]).exists():
            return "inhibited"
        # Never put key bytes in argv, output, state or a temporary file.
        opened = self.remote(b"R" + key.stdout)
        if opened != {"unlocked": True, "deployment_ready": True}:
            return "recovery_unconfirmed"
        state = self.status()
        return ("recovered" if state["mounted"] and state["deployment_ready"]
                and state["services_active"] else "recovery_unconfirmed")

    def tick(self, now: float | None = None) -> dict[str, Any]:
        now = time.time() if now is None else now
        started = time.monotonic()
        c = self.config
        fd = os.open(c["lock"], os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or info.st_nlink != 1 or info.st_mode & 0o077):
                raise ValueError("unsafe lock")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return {"state": "already_running"}
            previous: dict[str, Any] = {}
            receipt = Path(c["receipt"])
            if receipt.exists():
                private_file(receipt)
                previous = json.loads(receipt.read_bytes())
            if previous.get("retry_after", 0) > now and not Path(c["inhibit"]).exists():
                return {"state": "backoff", "retry_after": previous["retry_after"]}
            try:
                state = self.attempt()
            except Exception:
                state = "probe_or_recovery_unavailable"
            failed = state not in {"healthy", "recovered", "inhibited", "services_stopped"}
            failures = min(int(previous.get("failures", 0)) + 1, 5) if failed else 0
            completed = now + time.monotonic() - started
            result = {"schema": "sightglass.vault-recovery.v1", "checked_at": completed,
                      "state": state, "failures": failures,
                      "retry_after": completed + min(60 * 2 ** (failures - 1), 900)
                      if failed else 0}
            temp_fd, temporary = tempfile.mkstemp(prefix=".vault-receipt-", dir=receipt.parent)
            try:
                with os.fdopen(temp_fd, "wb") as stream:
                    stream.write(json.dumps(result, separators=(",", ":")).encode())
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, receipt)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
            return result
        finally:
            os.close(fd)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        print(json.dumps(Recovery(load_config(args.config)).tick(), separators=(",", ":")))
    except Exception:
        print('{"state":"controller_unavailable"}')
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

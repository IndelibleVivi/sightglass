from __future__ import annotations

import json
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .keys import image_decoder_keychain_account, source_account_key

SETTINGS_SCHEMA_V1 = "sightglass.source.macos-wechat.v1"
SETTINGS_SCHEMA = "sightglass.source.macos-wechat.v2"
MAX_SETTINGS_BYTES = 512 * 1024


def read_settings_bytes(path: str | os.PathLike[str]) -> bytes:
    """Read one bounded owner-private settings before-image without following it."""
    target = Path(path).expanduser().absolute()
    try:
        descriptor = os.open(target, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as exc:
        raise RuntimeError("macos-wechat settings are unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
            or before.st_uid != os.getuid() or stat.S_IMODE(before.st_mode) & 0o077
            or not 0 < before.st_size <= MAX_SETTINGS_BYTES
        ):
            raise RuntimeError("macos-wechat settings must be a bounded private regular file")
        parts = bytearray()
        while len(parts) <= MAX_SETTINGS_BYTES:
            part = os.read(descriptor, min(65_536, MAX_SETTINGS_BYTES + 1 - len(parts)))
            if not part:
                break
            parts.extend(part)
        after = os.fstat(descriptor)
        current = target.stat(follow_symlinks=False)
        fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if (len(parts) > MAX_SETTINGS_BYTES
                or any(getattr(before, name) != getattr(after, name) for name in fields)
                or any(getattr(before, name) != getattr(current, name) for name in fields)):
            raise RuntimeError("macos-wechat settings changed during read")
        return bytes(parts)
    except OSError as exc:
        raise RuntimeError("macos-wechat settings changed or became unavailable") from exc
    finally:
        os.close(descriptor)


def _save_settings(path: str | os.PathLike[str], value: dict[str, Any],
                   *, expected: bytes | None = None) -> None:
    target = Path(path).expanduser().absolute()
    target.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    os.chmod(target.parent, 0o700)
    descriptor, temporary_name = tempfile.mkstemp(prefix="source-", dir=target.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8", closefd=True) as handle:
            json.dump(value, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        if expected is not None and read_settings_bytes(target) != expected:
            raise RuntimeError("macos-wechat settings changed before rebind")
        os.replace(temporary, target)
        directory = os.open(target.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


@dataclass(frozen=True)
class MacOSWeChatSettings:
    instance_id: str
    source_root: Path
    keychain_account: str
    source_account_binding_id: str
    source_account_key: str
    bundle_id: str
    version: str
    build: str
    architecture: str
    profile_id: str
    image_keychain_account: str | None = None

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> MacOSWeChatSettings:
        return cls.from_dict(json.loads(read_settings_bytes(path)))

    @classmethod
    def from_dict(cls, value: Any) -> MacOSWeChatSettings:
        binding = value.get("candidate_binding") if isinstance(value, dict) else None
        schema = value.get("schema") if isinstance(value, dict) else None
        if schema not in {SETTINGS_SCHEMA_V1, SETTINGS_SCHEMA} or not isinstance(
            binding, dict
        ):
            raise RuntimeError("unsupported macos-wechat settings")
        source_root = Path(str(value.get("source_root") or "")).expanduser().resolve()
        if not source_root.is_dir() or source_root.is_symlink():
            raise RuntimeError("macos-wechat source root is unavailable")
        fields = {
            "instance_id": str(value.get("instance_id") or ""),
            "keychain_account": str(value.get("keychain_account") or ""),
            "source_account_key": str(value.get("source_account_key") or ""),
            "bundle_id": str(binding.get("bundle_id") or ""),
            "version": str(binding.get("version") or ""),
            "build": str(binding.get("build") or ""),
            "architecture": str(binding.get("architecture") or ""),
        }
        if any(not item for item in fields.values()):
            raise RuntimeError("macos-wechat source binding is incomplete")
        source_account_binding_id = str(value.get("source_account_binding_id") or "")
        profile_id = str(binding.get("profile_id") or "")
        if schema == SETTINGS_SCHEMA and (
            not source_account_binding_id or not profile_id
        ):
            raise RuntimeError("macos-wechat source binding is incomplete")
        if (
            schema == SETTINGS_SCHEMA
            and source_account_binding_id.startswith("wxbind_")
            and fields["source_account_key"] != source_account_key(source_account_binding_id)
        ):
            raise RuntimeError("macos-wechat source account binding is inconsistent")
        image_keychain_account = value.get("image_keychain_account")
        if image_keychain_account is not None:
            if schema != SETTINGS_SCHEMA or str(image_keychain_account) != (
                image_decoder_keychain_account(source_account_binding_id)
            ):
                raise RuntimeError("macos-wechat image key binding is inconsistent")
        if schema == SETTINGS_SCHEMA_V1:
            source_account_binding_id = f"legacy-{fields['source_account_key']}"
        return cls(
            source_root=source_root,
            source_account_binding_id=source_account_binding_id,
            profile_id=profile_id,
            image_keychain_account=(
                str(image_keychain_account)
                if image_keychain_account is not None
                else None
            ),
            **fields,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": SETTINGS_SCHEMA,
            "instance_id": self.instance_id,
            "candidate_binding": {
                "bundle_id": self.bundle_id,
                "version": self.version,
                "build": self.build,
                "architecture": self.architecture,
                "profile_id": self.profile_id,
            },
            "source_root": str(self.source_root),
            "keychain_account": self.keychain_account,
            "source_account_binding_id": self.source_account_binding_id,
            "source_account_key": self.source_account_key,
            "image_keychain_account": self.image_keychain_account,
            "poll_interval_seconds": 2,
            "initial_index_mode": "from_now",
            "auto_key_refresh": False,
        }

    def save(self, path: str | os.PathLike[str]) -> None:
        _save_settings(path, self.as_dict())

    def save_profile_binding(self, path: str | os.PathLike[str], *, expected: bytes) -> None:
        """Replace only the supported app binding of an exact existing v2 file."""
        value = json.loads(expected)
        if not isinstance(value, dict) or value.get("schema") != SETTINGS_SCHEMA:
            raise RuntimeError("edge rebind requires an existing v2 native binding")
        value["candidate_binding"] = {
            **value["candidate_binding"], **self.as_dict()["candidate_binding"],
        }
        _save_settings(path, value, expected=expected)

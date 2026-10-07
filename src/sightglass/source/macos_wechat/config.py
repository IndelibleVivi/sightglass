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
        target = Path(path).expanduser().resolve()
        metadata = target.lstat()
        if (
            target.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) & 0o077
        ):
            raise RuntimeError("macos-wechat settings must be a private regular file")
        value = json.loads(target.read_text(encoding="utf-8"))
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
        target = Path(path).expanduser().resolve()
        target.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        os.chmod(target.parent, 0o700)
        descriptor, temporary_name = tempfile.mkstemp(prefix="source-", dir=target.parent)
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8", closefd=True) as handle:
                json.dump(self.as_dict(), handle, ensure_ascii=False, sort_keys=True, indent=2)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
            os.chmod(target, 0o600)
        finally:
            temporary.unlink(missing_ok=True)

from __future__ import annotations

import json
import os
import stat
import sys
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from sightglass.contracts.common import validate_timezone
from sightglass.contracts.voice import VoiceReadSettings
from sightglass.semantic.settings import SemanticSettings
from sightglass.storage import StorageSettings

CONFIG_SCHEMA_V1 = "sightglass.config.v1"
CONFIG_SCHEMA = "sightglass.config.v2"
DEFAULT_CONFIG_PATH = (
    Path(os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config")))
    / "sightglass" / "config.json"
    if sys.platform.startswith("linux")
    else Path.home() / "Library" / "Application Support" / "Sightglass" / "config.json"
)


def _private_directory(path: Path) -> None:
    if path.is_symlink():
        raise RuntimeError("Sightglass private directories cannot be symlinks")
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    metadata = path.stat()
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RuntimeError(f"Sightglass private directory must use mode 0700: {path}")


def _private_file(path: Path) -> None:
    if path.is_symlink():
        raise RuntimeError("Sightglass private files cannot be symlinks")
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RuntimeError(f"Sightglass private file must use mode 0600: {path}")

def _fsync_directory(directory: Path) -> None:
    """Persist a completed rename in its parent directory.

    ``os.replace`` is atomic against a crash only once the directory entry itself is
    durable; without this fsync a power loss can roll back to the previous config file.
    """

    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

def _policy_id_list(policy: dict[str, Any], key: str) -> tuple[str, ...]:
    """Parse one strict conversation-id collection.

    The stored contract is a JSON array of non-empty strings. A missing field defaults to
    empty, but an explicit ``null`` is malformed (it is not the accepted collection
    shape), and a bare string/object/number must never be silently iterated into
    character keys or dict keys. Rejecting these shapes keeps a malformed policy from
    widening or corrupting the admitted conversation set.
    """

    if key not in policy:
        return ()
    value = policy[key]
    if not isinstance(value, list):
        raise RuntimeError(f"Sightglass policy {key} must be a list of strings")
    selected: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item:
            raise RuntimeError(f"Sightglass policy {key} must contain non-empty strings")
        selected.append(item)
    return tuple(sorted(set(selected)))


def default_config_path() -> Path:
    configured = os.environ.get("SIGHTGLASS_CONFIG")
    return Path(configured).expanduser().resolve() if configured else DEFAULT_CONFIG_PATH


def _bounded_voice_timeout(value: Any) -> int:
    """Bound the helper watchdog: at least one second, never more than ten minutes."""

    if value is None:
        return 120
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("invalid Sightglass voice helper timeout") from exc
    if parsed < 1 or parsed > 600:
        raise RuntimeError("Sightglass voice helper timeout must be between 1 and 600 seconds")
    return parsed


@dataclass(frozen=True)
class SightglassConfig:
    data_dir: Path
    source_root: Path | None
    window_db_path: Path
    socket_path: Path
    reader_id: str = "reader"
    reader_display_name: str = "Reader"
    policy_mode: str = "all_except_denylist"
    allowed_conversation_ids: tuple[str, ...] = ()
    denied_conversation_ids: tuple[str, ...] = ()
    paused: bool = False
    reader_token_hash: str = ""
    operator_token_hash: str = ""
    source_kind: str = "synthetic"
    source_instance_id: str = "synthetic-default"
    source_settings_path: Path | None = None
    voice_enabled: bool = False
    voice_policy: str = "off"
    voice_language: str = "auto"
    voice_open_item_limit: int = 3
    voice_open_duration_ms: int = 300_000
    voice_helper_path: str = ""
    voice_helper_timeout_seconds: int = 120
    reader_timezone: str = "Asia/Singapore"
    storage: StorageSettings = StorageSettings()
    semantic: SemanticSettings = SemanticSettings()
    reader_default_view: str = "auto"
    activation_generation: str = ""
    activation_path: Path | None = None

    def voice_settings(self) -> VoiceReadSettings:
        return VoiceReadSettings(
            enabled=bool(self.voice_enabled),
            default_policy=self.voice_policy,
            language=self.voice_language,
            open_item_limit=self.voice_open_item_limit,
            open_duration_ms=self.voice_open_duration_ms,
        )

    @classmethod
    def create(cls, data_dir: str | os.PathLike[str], source_root: str | os.PathLike[str]):
        root = Path(data_dir).expanduser().resolve()
        source = Path(source_root).expanduser().resolve()
        return cls(
            data_dir=root,
            source_root=source,
            window_db_path=root / "window.db",
            socket_path=root / "run" / "sightglassd.sock",
            source_settings_path=source / "source.json",
        )

    @classmethod
    def from_dict(cls, value: dict[str, Any]):
        schema = value.get("schema")
        if schema not in {CONFIG_SCHEMA_V1, CONFIG_SCHEMA}:
            raise RuntimeError("unsupported Sightglass config schema")
        policy = value.get("policy")
        if not isinstance(policy, dict):
            raise RuntimeError("Sightglass config policy is missing")
        mode = str(policy.get("mode", ""))
        if mode not in {"allowlist", "all_except_denylist"}:
            raise RuntimeError("invalid Sightglass conversation policy mode")
        paths = value.get("paths")
        reader = value.get("reader")
        auth = value.get("auth")
        if not all(isinstance(item, dict) for item in (paths, reader, auth)):
            raise RuntimeError("Sightglass config is incomplete")
        assert isinstance(paths, dict)
        assert isinstance(reader, dict)
        assert isinstance(auth, dict)
        source_root: Path | None
        source_settings_path: Path | None
        if schema == CONFIG_SCHEMA_V1:
            source_kind = str(value.get("source_kind") or "synthetic")
            if source_kind != "synthetic":
                raise RuntimeError("v1 Sightglass config admits only the synthetic source")
            source_root = Path(str(paths["source_root"])).expanduser().resolve()
            source_settings_path = source_root / "source.json"
            source_instance_id = "synthetic-default"
        else:
            source = value.get("source")
            if not isinstance(source, dict):
                raise RuntimeError("Sightglass config source is missing")
            source_kind = str(source.get("kind") or "")
            if source_kind not in {"synthetic", "macos-wechat", "remote-capture"}:
                raise RuntimeError("unsupported Sightglass source provider")
            source_instance_id = str(source.get("instance_id") or "")
            settings_value = str(source.get("settings_path") or "")
            if not source_instance_id or not settings_value:
                raise RuntimeError("Sightglass source binding is incomplete")
            source_settings_path = Path(settings_value).expanduser().resolve()
            source_root = (
                source_settings_path.parent if source_kind == "synthetic" else None
            )
        voice = value.get("voice", {})
        if not isinstance(voice, dict):
            raise RuntimeError("Sightglass config voice section must be an object")
        try:
            voice_settings = VoiceReadSettings(
                enabled=bool(voice.get("enabled", False)),
                default_policy=str(voice.get("policy", "off")),
                language=str(voice.get("language", "auto")),
                open_item_limit=int(voice.get("open_item_limit", 3)),
                open_duration_ms=int(voice.get("open_duration_ms", 300_000)),
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"invalid Sightglass voice config: {exc}") from exc
        default_view = str(reader.get(
            "default_view", "replica" if source_kind == "remote-capture" else "auto"
        ))
        if default_view not in {"auto", "replica", "fresh"}:
            raise RuntimeError("invalid Sightglass reader default view")
        if source_kind == "remote-capture" and default_view == "auto":
            raise RuntimeError("remote capture requires an explicit replica or fresh default")
        activation = value.get("activation", {})
        if not isinstance(activation, dict):
            raise RuntimeError("invalid Sightglass activation binding")
        activation_generation = activation.get("generation", "")
        activation_path = activation.get("path", "")
        if (
            not isinstance(activation_generation, str)
            or not isinstance(activation_path, str)
            or bool(activation_generation) != bool(activation_path)
        ):
            raise RuntimeError("Sightglass activation binding is incomplete")
        return cls(
            data_dir=Path(str(paths["data_dir"])).expanduser().resolve(),
            source_root=source_root,
            window_db_path=Path(str(paths["window_db"])).expanduser().resolve(),
            socket_path=Path(str(paths["socket"])).expanduser().resolve(),
            reader_id=str(reader["reader_id"]),
            reader_display_name=str(reader["display_name"]),
            reader_timezone=validate_timezone(str(reader.get("timezone", "Asia/Singapore"))),
            policy_mode=mode,
            allowed_conversation_ids=_policy_id_list(policy, "allowed"),
            denied_conversation_ids=_policy_id_list(policy, "denied"),
            paused=bool(value.get("paused", False)),
            reader_token_hash=str(auth.get("reader_token_hash", "")),
            operator_token_hash=str(auth.get("operator_token_hash", "")),
            source_kind=source_kind,
            source_instance_id=source_instance_id,
            source_settings_path=source_settings_path,
            voice_enabled=voice_settings.enabled,
            voice_policy=voice_settings.default_policy,
            voice_language=voice_settings.language,
            voice_open_item_limit=voice_settings.open_item_limit,
            voice_open_duration_ms=voice_settings.open_duration_ms,
            voice_helper_path=str(voice.get("helper_path", "") or ""),
            voice_helper_timeout_seconds=_bounded_voice_timeout(voice.get("helper_timeout_seconds")),
            storage=StorageSettings.from_dict(value.get("storage", {})),
            semantic=SemanticSettings.from_dict(value.get("semantic", {})),
            reader_default_view=default_view,
            activation_generation=activation_generation,
            activation_path=(
                Path(activation_path).expanduser().absolute() if activation_path else None
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": CONFIG_SCHEMA,
            "storage": self.storage.as_dict(),
            "semantic": self.semantic.as_dict(),
            "paths": {
                "data_dir": str(self.data_dir),
                "window_db": str(self.window_db_path),
                "socket": str(self.socket_path),
            },
            "source": {
                "kind": self.source_kind,
                "instance_id": self.source_instance_id,
                "settings_path": str(self.source_settings_path or ""),
            },
            "reader": {
                "reader_id": self.reader_id,
                "display_name": self.reader_display_name,
                "timezone": self.reader_timezone,
                "default_view": self.reader_default_view,
            },
            "policy": {
                "mode": self.policy_mode,
                "allowed": list(self.allowed_conversation_ids),
                "denied": list(self.denied_conversation_ids),
            },
            "paused": self.paused,
            "auth": {
                "reader_token_hash": self.reader_token_hash,
                "operator_token_hash": self.operator_token_hash,
            },
            "voice": {
                "enabled": bool(self.voice_enabled),
                "policy": self.voice_policy,
                "language": self.voice_language,
                "open_item_limit": self.voice_open_item_limit,
                "open_duration_ms": self.voice_open_duration_ms,
                "helper_path": self.voice_helper_path,
                "helper_timeout_seconds": self.voice_helper_timeout_seconds,
            },
            "activation": {
                "generation": self.activation_generation,
                "path": str(self.activation_path or ""),
            },
        }

    def with_pause(self, paused: bool):
        return replace(self, paused=paused)

    def with_policy(
        self,
        *,
        mode: str | None = None,
        allowed: tuple[str, ...] | None = None,
        denied: tuple[str, ...] | None = None,
    ):
        selected_mode = mode or self.policy_mode
        if selected_mode not in {"allowlist", "all_except_denylist"}:
            raise RuntimeError("invalid Sightglass conversation policy mode")
        return replace(
            self,
            policy_mode=selected_mode,
            allowed_conversation_ids=(
                tuple(sorted(set(allowed)))
                if allowed is not None
                else self.allowed_conversation_ids
            ),
            denied_conversation_ids=(
                tuple(sorted(set(denied))) if denied is not None else self.denied_conversation_ids
            ),
        )

    def with_source(
        self,
        *,
        kind: str,
        instance_id: str,
        settings_path: Path,
        window_db_path: Path,
        paused: bool,
        policy_mode: str,
        allowed: tuple[str, ...] = (),
        denied: tuple[str, ...] = (),
    ):
        return replace(
            self,
            source_root=(settings_path.parent if kind == "synthetic" else None),
            source_kind=kind,
            source_instance_id=instance_id,
            source_settings_path=settings_path,
            window_db_path=window_db_path,
            paused=paused,
            policy_mode=policy_mode,
            allowed_conversation_ids=tuple(sorted(set(allowed))),
            denied_conversation_ids=tuple(sorted(set(denied))),
        )


class ConfigStore:
    def __init__(self, path: str | os.PathLike[str] | None = None) -> None:
        self.path = Path(path).expanduser().resolve() if path else default_config_path()

    def load(self) -> SightglassConfig:
        _private_file(self.path)
        value = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise RuntimeError("Sightglass config must be a JSON object")
        config = SightglassConfig.from_dict(value)
        if value.get("schema") == CONFIG_SCHEMA_V1:
            backup = self.path.with_name("config.v1.backup.json")
            if not backup.exists():
                descriptor = os.open(
                    backup,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
                with os.fdopen(descriptor, "wb", closefd=True) as handle:
                    rendered = json.dumps(
                        value, ensure_ascii=False, sort_keys=True, indent=2
                    ).encode()
                    handle.write(rendered)
                    handle.write(b"\n")
                    handle.flush()
                    os.fsync(handle.fileno())
            self.save(config)
        _private_directory(config.data_dir)
        _private_directory(config.socket_path.parent)
        return config

    def save(self, config: SightglassConfig) -> None:
        _private_directory(self.path.parent)
        _private_directory(config.data_dir)
        _private_directory(config.socket_path.parent)
        descriptor, temporary_name = tempfile.mkstemp(prefix="config-", dir=self.path.parent)
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            payload = json.dumps(
                config.as_dict(), ensure_ascii=False, sort_keys=True, indent=2
            ).encode("utf-8")
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(payload)
                handle.write(b"\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            os.chmod(self.path, 0o600)
            _fsync_directory(self.path.parent)
        finally:
            temporary.unlink(missing_ok=True)

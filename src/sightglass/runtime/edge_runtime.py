"""Explicit thin-edge assembly: native source, bounded spool and outgoing relay.

This entry never constructs WindowDB, reader services, processors or MCP. Native
settings and Keychain keys remain on the source host. Enrollment initializes the
stream once; ordinary startup refuses missing or changed stream state.
"""

from __future__ import annotations

import json
import os
import signal
import stat
import threading
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from sightglass.contracts.capture import CaptureCeiling, CaptureProtocolError, CaptureRequest
from sightglass.source.base import WeChatSourceProvider
from sightglass.source.capture import CaptureExecutor
from sightglass.source.capture.codec import SealedCapture
from sightglass.source.direct_wechat import SyntheticSourceProvider

from .activation import require_activation
from .capture_recovery import read_private_json, write_private_json
from .edge import EdgeSpool
from .edge_relay import EdgeSession, SSHRelayConnector
from .secrets import SecretStore, default_secret_store

EDGE_CONFIG_SCHEMA = "sightglass.edge-config.v1"
MAX_EDGE_CONFIG_BYTES = 512 * 1024


class EdgeConnectionStatus:
    """One bounded private snapshot beside, never inside, the pending spool."""

    def __init__(self, spool_directory: Path) -> None:
        self.path = spool_directory.with_name(spool_directory.name + ".connection.json")

    def _previous(self) -> dict[str, Any] | None:
        previous = read_private_json(self.path) if self.path.exists() else None
        if previous is not None and previous.get("schema") != (
            "sightglass.edge-connection.v1"
        ):
            raise CaptureProtocolError("edge_connection_status_invalid")
        return previous

    def __call__(self, value: dict[str, Any]) -> None:
        from sightglass.contracts.common import utc_now

        value = {**value, "pid": os.getpid(), "observed_at": utc_now().isoformat()}
        try:
            previous = self._previous()
            write_private_json(self.path, value, expected=previous)
        except (OSError, ValueError, RuntimeError, CaptureProtocolError):
            # A damaged/foreign diagnostic cannot block startup or pending replay.
            # Preserve it for the operator; this owner grants no repair authority.
            return


@dataclass(frozen=True)
class EdgeSettings:
    source_kind: str
    source_settings_path: Path
    source_instance_id: str
    account_id: str
    conversations: frozenset[str]
    egress_revision: str
    origin_epoch: str
    stream_epoch: str
    spool_directory: Path
    relay_host: str
    relay_identity_file: Path
    capability_secret_account: str
    activation_path: Path
    activation_generation: str
    core_generation: str

    @classmethod
    def load(cls, path: Path) -> EdgeSettings:
        parent = path.parent.lstat()
        if (
            not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != os.getuid()
            or stat.S_IMODE(parent.st_mode) & 0o077
        ):
            raise RuntimeError("edge configuration directory must be owner-private")
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
                or before.st_uid != os.getuid()
                or stat.S_IMODE(before.st_mode) & 0o077
                or before.st_size > MAX_EDGE_CONFIG_BYTES
            ):
                raise RuntimeError("edge configuration must be a bounded private regular file")
            payload = bytearray()
            while len(payload) <= MAX_EDGE_CONFIG_BYTES:
                part = os.read(descriptor, min(65_536, MAX_EDGE_CONFIG_BYTES + 1 - len(payload)))
                if not part:
                    break
                payload.extend(part)
            after = os.fstat(descriptor)
            if (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
                before.st_ctime_ns,
            ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                raise RuntimeError("edge configuration changed during read")
        finally:
            os.close(descriptor)
        if len(payload) > MAX_EDGE_CONFIG_BYTES:
            raise RuntimeError("edge configuration exceeds its bound")
        return cls.from_dict(json.loads(payload))

    @classmethod
    def from_dict(cls, value: Any) -> EdgeSettings:
        if not isinstance(value, dict) or value.get("schema") != EDGE_CONFIG_SCHEMA:
            raise RuntimeError("invalid edge configuration schema")
        names = set(cls.__dataclass_fields__)
        if set(value) != names | {"schema"}:
            raise RuntimeError("invalid edge configuration fields")
        if value["source_kind"] not in {"synthetic", "macos-wechat"}:
            raise RuntimeError("edge requires a supported local source")
        conversations = value["conversations"]
        if (
            not isinstance(conversations, list)
            or len(conversations) > 10_000
            or any(not isinstance(item, str) or not item for item in conversations)
            or len(set(conversations)) != len(conversations)
        ):
            raise RuntimeError("invalid edge conversation ceiling")
        paths = {
            "source_settings_path",
            "spool_directory",
            "relay_identity_file",
            "activation_path",
        }
        converted = {}
        for name in names - {"conversations"}:
            item = value[name]
            if not isinstance(item, str) or not item or "\x00" in item:
                raise RuntimeError("invalid edge configuration value")
            if name in paths:
                path = Path(item)
                if not path.is_absolute() or ".." in path.parts:
                    raise RuntimeError("edge configuration paths must be absolute")
                converted[name] = path
            else:
                converted[name] = item
        settings = cls(conversations=frozenset(conversations), **converted)
        CaptureCeiling(settings.account_id, settings.conversations, settings.egress_revision)
        SSHRelayConnector(settings.relay_host, settings.relay_identity_file).argv()
        return settings

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"schema": EDGE_CONFIG_SCHEMA}
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            result[name] = (
                str(value)
                if isinstance(value, Path)
                else sorted(value)
                if isinstance(value, frozenset)
                else value
            )
        return result

    def require_active(self) -> None:
        require_activation(
            self.activation_path,
            generation=self.activation_generation,
            role="edge",
            namespace=self.spool_directory,
        )


class _OwnedExecutor(CaptureExecutor):
    def __init__(self, provider: WeChatSourceProvider, settings: EdgeSettings) -> None:
        self.settings = settings
        super().__init__(
            provider,
            CaptureCeiling(settings.account_id, settings.conversations, settings.egress_revision),
            source_instance_id=settings.source_instance_id,
        )
        if self.origin_epoch != settings.origin_epoch:
            raise CaptureProtocolError("edge_interpretation_epoch_changed")

    def capture(self, request: CaptureRequest, **kwargs: Any) -> SealedCapture:
        self.settings.require_active()
        envelope = super().capture(request, **kwargs)
        self.settings.require_active()
        return envelope


def build_executor(settings: EdgeSettings) -> CaptureExecutor:
    settings.require_active()
    if settings.source_kind == "synthetic":
        provider: WeChatSourceProvider = SyntheticSourceProvider(
            settings.source_settings_path.parent
        )
    else:
        from sightglass.source.macos_wechat import MacOSWeChatSourceProvider

        native = MacOSWeChatSourceProvider(settings.source_settings_path)
        if (
            native.settings.instance_id != settings.source_instance_id
            or native.settings.source_account_key != settings.account_id
        ):
            raise CaptureProtocolError("edge_native_account_binding_changed")
        provider = native
    return _OwnedExecutor(provider, settings)


def enroll_edge(settings: EdgeSettings) -> dict[str, Any]:
    executor = build_executor(settings)
    spool = EdgeSpool.initialize(
        settings.spool_directory,
        source_instance_id=settings.source_instance_id,
        account_id=settings.account_id,
        origin_epoch=executor.origin_epoch,
        stream_epoch=settings.stream_epoch,
    )
    spool.close()
    return {
        "schema": "sightglass.edge-enrollment.v1",
        "initialized": True,
        "next_sequence": 1,
        "window_db_created": False,
    }


def run_edge(
    settings: EdgeSettings,
    *,
    secret_store: SecretStore | None = None,
    stop: threading.Event | None = None,
) -> None:
    executor = build_executor(settings)
    token = (secret_store or default_secret_store()).get(settings.capability_secret_account)
    identity = settings.relay_identity_file.lstat()
    if (
        not stat.S_ISREG(identity.st_mode)
        or identity.st_nlink != 1
        or identity.st_uid != os.getuid()
        or stat.S_IMODE(identity.st_mode) & 0o077
    ):
        raise RuntimeError("edge relay identity must be an owner-private regular file")
    spool = EdgeSpool(
        settings.spool_directory,
        source_instance_id=settings.source_instance_id,
        account_id=settings.account_id,
        origin_epoch=executor.origin_epoch,
    )
    stop = stop or threading.Event()
    previous_handlers = {}
    if threading.current_thread() is threading.main_thread():
        for number in (signal.SIGTERM, signal.SIGINT):
            previous_handlers[number] = signal.signal(number, lambda *_args: stop.set())
    try:
        if spool.stream_epoch != settings.stream_epoch:
            raise CaptureProtocolError("edge_stream_epoch_binding_changed")
        connector = SSHRelayConnector(settings.relay_host, settings.relay_identity_file)
        EdgeSession(
            executor,
            spool,
            token=token,
            core_generation=settings.core_generation,
            ownership_guard=settings.require_active,
            connection_observer=EdgeConnectionStatus(settings.spool_directory),
        ).reconnect(connector.connect, stop=stop)
    finally:
        for number, handler in previous_handlers.items():
            signal.signal(number, handler)
        spool.close()

def rebind_edge(
    settings_path: Path,
    *,
    same_account: bool,
    secret_store: SecretStore | None = None,
) -> dict[str, Any]:
    """Explicitly move one existing thin edge to a verified supported app profile."""

    if not same_account:
        raise RuntimeError("edge rebind requires --same-account")
    settings = EdgeSettings.load(settings_path)
    if settings.source_kind != "macos-wechat":
        raise RuntimeError("edge rebind requires a macos-wechat edge")
    settings.require_active()
    from ..source.macos_wechat.config import MacOSWeChatSettings, read_settings_bytes
    from ..source.macos_wechat.discovery import discover_configured_candidate
    from ..source.macos_wechat.keys import (
        decode_key_map,
        read_keychain_secret,
        verify_key_map,
    )

    previous = read_settings_bytes(settings.source_settings_path)
    existing = MacOSWeChatSettings.from_dict(json.loads(previous))
    if (
        existing.instance_id != settings.source_instance_id
        or existing.source_account_key != settings.account_id
    ):
        raise RuntimeError("edge native settings do not match the edge binding")

    # Taking the existing spool lock proves the managed edge is stopped. The
    # rebind never creates, initializes, or opens a WindowDB.
    spool = EdgeSpool(
        settings.spool_directory,
        source_instance_id=settings.source_instance_id,
        account_id=settings.account_id,
        origin_epoch=settings.origin_epoch,
    )
    try:
        if spool.stream_epoch != settings.stream_epoch:
            raise CaptureProtocolError("edge_stream_epoch_binding_changed")
        candidate = discover_configured_candidate(existing.source_root)
        if candidate is None or not candidate.running or candidate.profile_id is None:
            raise RuntimeError("no exact supported running candidate for the configured source")
        if (
            candidate.source_root != existing.source_root
            or candidate.bundle_id != existing.bundle_id
            or candidate.architecture != existing.architecture
        ):
            raise RuntimeError("edge rebind source identity changed")
        secret = (
            secret_store.get(existing.keychain_account)
            if secret_store is not None else read_keychain_secret(existing.keychain_account)
        )
        secret_text = secret.decode("utf-8") if isinstance(secret, bytes) else secret
        rebound = replace(
            existing,
            bundle_id=candidate.bundle_id,
            version=candidate.version,
            build=candidate.build,
            architecture=candidate.architecture,
            profile_id=str(candidate.profile_id),
        )
        if discover_configured_candidate(existing.source_root) != candidate:
            raise RuntimeError("running source candidate changed during rebind")
        keys = verify_key_map(candidate.source_root, decode_key_map(secret_text))
        settings.require_active()
        if EdgeSettings.load(settings_path) != settings:
            raise RuntimeError("edge configuration changed during rebind")
        rebound.save_profile_binding(settings.source_settings_path, expected=previous)
        return {
            "schema": "sightglass.edge-rebind.v1",
            "rebound": True,
            "version": rebound.version,
            "build": rebound.build,
            "profile_id": rebound.profile_id,
            "architecture": rebound.architecture,
            "verified_database_count": len(keys),
            "verified_required": True,
            "same_account_attested": True,
            "activated": False,
        }
    finally:
        spool.close()

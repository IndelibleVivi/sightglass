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
from typing import Any, cast

from sightglass.contracts.capture import CaptureCeiling, CaptureProtocolError, CaptureRequest
from sightglass.source.base import WeChatSourceProvider
from sightglass.source.capture import CaptureExecutor
from sightglass.source.capture.codec import SealedCapture
from sightglass.source.direct_wechat import SyntheticSourceProvider
from sightglass.source.macos_wechat import MacOSWeChatSourceProvider

from .activation import require_activation
from .capture_recovery import read_private_json, write_private_json
from .edge import EdgeSpool
from .edge_relay import EdgeSession, SSHRelayConnector
from .secrets import SecretStore, default_secret_store
from .source_access import (
    HELPER_VERSION,
    ReceiverContext,
    SourceAccessError,
    SourceAccessLease,
    account_identity_from_source_root,
    derive_receiver_proof,
    enable_in_settings,
    shared_config_identities_from_source_root,
    validate_helper,
)
from .source_access import (
    authorize_source_access as helper_authorize,
)

EDGE_CONFIG_SCHEMA = "sightglass.edge-config.v1"
MAX_EDGE_CONFIG_BYTES = 512 * 1024
SOURCE_ACCESS_MODE = "macos-wechat.account-broker.v1"


class EdgeConnectionStatus:
    """One bounded private snapshot beside, never inside, the pending spool."""

    def __init__(self, spool_directory: Path) -> None:
        self.path = spool_directory.with_name(spool_directory.name + ".connection.json")

    def _previous(self) -> dict[str, Any] | None:
        previous = read_private_json(self.path) if self.path.exists() else None
        if previous is not None and previous.get("schema") != ("sightglass.edge-connection.v1"):
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
    source_access_helper_path: Path | None = None
    source_access_helper_digest: str | None = None
    source_access_shared_config: bool = False
    source_access_mode: str | None = None

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
        required = names - {
            "source_access_helper_path",
            "source_access_helper_digest",
            "source_access_shared_config",
            "source_access_mode",
        }
        allowed = names | {"schema"}
        if not required <= set(value) or not set(value) <= allowed:
            raise RuntimeError("invalid edge configuration fields")
        access_path = value.get("source_access_helper_path")
        access_digest = value.get("source_access_helper_digest")
        if (access_path is None) != (access_digest is None):
            raise RuntimeError("edge source-access binding is incomplete")
        if access_path is not None and (
            not isinstance(access_path, str)
            or not isinstance(access_digest, str)
            or not access_path
            or not access_digest
        ):
            raise RuntimeError("invalid edge source-access binding")
        shared_config = value.get("source_access_shared_config", False)
        if type(shared_config) is not bool:
            raise RuntimeError("invalid edge source-access shared-config binding")
        if shared_config and access_path is None:
            raise RuntimeError("edge source-access helper is required for shared config")
        access_mode = value.get("source_access_mode")
        if access_path is None:
            if access_mode is not None:
                raise RuntimeError("edge source-access mode requires a helper")
        elif access_mode != SOURCE_ACCESS_MODE:
            raise RuntimeError("edge source-access mode is unsupported")
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
            "source_access_helper_path",
        }
        converted = {}
        for name in names - {"conversations"}:
            if name not in value:
                converted[name] = False if name == "source_access_shared_config" else None
                continue
            item = value[name]
            if name == "source_access_shared_config":
                converted[name] = item
            else:
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
            if value is None or (name == "source_access_shared_config" and value is False):
                continue
            result[name] = (
                str(value)
                if isinstance(value, Path)
                else sorted(value)
                if isinstance(value, frozenset)
                else value
            )
        return result

    def with_source_access(
        self, path: Path, digest: str, *, shared_config: bool = False
    ) -> EdgeSettings:
        if self.source_kind != "macos-wechat":
            raise RuntimeError("source access requires a macos-wechat edge")
        if not path.is_absolute() or not digest:
            raise RuntimeError("invalid source-access helper binding")
        return replace(
            self,
            source_access_helper_path=path,
            source_access_helper_digest=digest,
            source_access_shared_config=shared_config,
            source_access_mode=SOURCE_ACCESS_MODE,
        )

    def receiver_context(self, expected_account: Any) -> ReceiverContext:
        return ReceiverContext(
            source_instance_id=self.source_instance_id,
            account_id=self.account_id,
            account_path=expected_account.path,
        )

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


class _LeasedNativeProvider:
    """Delegate the native provider while owning its process-lifetime URL lease."""

    def __init__(self, provider: Any, lease: Any) -> None:
        self._provider = provider
        self._lease = lease
        self._closed = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._provider.close()
        finally:
            self._lease.close()


def _close_provider(provider: Any) -> None:
    close = getattr(provider, "close", None)
    if close is not None:
        close()


class _FixedSecretStore:
    """Use the already-read relay capability for derived source-access authority."""

    def __init__(self, token: str) -> None:
        self._token = token

    def get(self, account: str) -> str:
        del account
        return self._token

    def set(self, account: str, value: str) -> None:
        del account, value
        raise RuntimeError("derived source-access credential is read-only")


def build_executor(
    settings: EdgeSettings,
    *,
    secret_store: SecretStore | None = None,
) -> CaptureExecutor:
    settings.require_active()
    if settings.source_kind == "synthetic":
        if settings.source_access_helper_path is not None:
            raise CaptureProtocolError("synthetic_edge_cannot_use_source_access")
        provider: WeChatSourceProvider = SyntheticSourceProvider(
            settings.source_settings_path.parent
        )
        return _OwnedExecutor(provider, settings)
    elif settings.source_access_helper_path is None:
        native = MacOSWeChatSourceProvider(settings.source_settings_path)
    else:
        from sightglass.source.macos_wechat.config import read_settings_bytes

        source_before = read_settings_bytes(settings.source_settings_path)
        native_settings = json.loads(source_before)
        root = Path(str(native_settings.get("source_root") or ""))
        artifact = validate_helper(settings.source_access_helper_path)
        if artifact.digest != settings.source_access_helper_digest:
            raise CaptureProtocolError("source_access_helper_binding_changed")
        try:
            expected_account = account_identity_from_source_root(root)
            expected_shared = (
                shared_config_identities_from_source_root(root)
                if settings.source_access_shared_config
                else None
            )
        except SourceAccessError as exc:
            raise CaptureProtocolError(exc.code) from exc
        try:
            edge_secret = (secret_store or default_secret_store()).get(
                settings.capability_secret_account
            )
            receiver_proof = derive_receiver_proof(
                edge_secret,
                settings.receiver_context(expected_account),
            )
            lease = SourceAccessLease(
                artifact.app_path,
                expected_account,
                expected_shared_config=expected_shared,
                receiver_proof=receiver_proof,
            )
        except (SourceAccessError, RuntimeError) as exc:
            code = (
                exc.code
                if isinstance(exc, SourceAccessError)
                else ("source_access_receiver_credential_missing")
            )
            raise CaptureProtocolError(code) from exc
        try:
            sticker_loader = (
                __import__(
                    "sightglass.source.macos_wechat.sticker_key",
                    fromlist=["load_sticker_decoder_key"],
                ).load_sticker_decoder_key
                if settings.source_access_shared_config
                else (lambda _settings: None)
            )
            native = MacOSWeChatSourceProvider(
                settings.source_settings_path,
                sticker_key_loader=sticker_loader,
            )
            if read_settings_bytes(settings.source_settings_path) != source_before:
                raise CaptureProtocolError("edge_native_source_changed_during_access")
        except BaseException:
            lease.close()
            raise
        provider = cast(WeChatSourceProvider, _LeasedNativeProvider(native, lease))
    if (
        native.settings.instance_id != settings.source_instance_id
        or native.settings.source_account_key != settings.account_id
    ):
        _close_provider(provider)
        raise CaptureProtocolError("edge_native_account_binding_changed")
    return _OwnedExecutor(cast(WeChatSourceProvider, provider), settings)


def enroll_edge(settings: EdgeSettings) -> dict[str, Any]:
    secret_store = default_secret_store()
    executor = build_executor(settings, secret_store=secret_store)
    try:
        spool = EdgeSpool.initialize(
            settings.spool_directory,
            source_instance_id=settings.source_instance_id,
            account_id=settings.account_id,
            origin_epoch=executor.origin_epoch,
            stream_epoch=settings.stream_epoch,
        )
        spool.close()
    finally:
        _close_provider(executor.provider)
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
    secret_store = secret_store or default_secret_store()
    token = secret_store.get(settings.capability_secret_account)
    executor = build_executor(settings, secret_store=_FixedSecretStore(token))
    try:
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
    finally:
        _close_provider(executor.provider)


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
            if secret_store is not None
            else read_keychain_secret(existing.keychain_account)
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


def _read_edge_config_bytes(path: Path) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != os.getuid()
            or stat.S_IMODE(before.st_mode) & 0o077
            or not 0 < before.st_size <= MAX_EDGE_CONFIG_BYTES
        ):
            raise RuntimeError("edge configuration must be a bounded private regular file")
        payload = bytearray()
        while len(payload) <= MAX_EDGE_CONFIG_BYTES:
            part = os.read(
                descriptor,
                min(65_536, MAX_EDGE_CONFIG_BYTES + 1 - len(payload)),
            )
            if not part:
                break
            payload.extend(part)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if len(payload) > MAX_EDGE_CONFIG_BYTES or (
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
    ):
        raise RuntimeError("edge configuration changed during read")
    return bytes(payload)


def source_access_scope_preview(
    settings: EdgeSettings, *, include_shared_config: bool = False
) -> dict[str, Any]:
    """Validate exact grant scopes from native settings without opening a picker."""
    if settings.source_kind != "macos-wechat":
        raise RuntimeError("source access requires a macos-wechat edge")
    from sightglass.source.macos_wechat.config import (
        MacOSWeChatSettings,
        read_settings_bytes,
    )

    native = MacOSWeChatSettings.from_dict(
        json.loads(read_settings_bytes(settings.source_settings_path))
    )
    if (
        native.instance_id != settings.source_instance_id
        or native.source_account_key != settings.account_id
    ):
        raise RuntimeError("edge native settings do not match the edge binding")
    try:
        account = account_identity_from_source_root(native.source_root)
        shared = (
            shared_config_identities_from_source_root(native.source_root)
            if include_shared_config
            else None
        )
    except SourceAccessError as exc:
        raise RuntimeError(exc.code) from exc
    return {
        "schema": "sightglass.source-access-preview.v1",
        "account_scope": "configured_account",
        "database_subdirectory": "db_storage",
        "shared_config": [
            {"path_class": "all_users_config_data", "state": "configured"},
            {"path_class": "all_users_config_meta", "state": "configured"},
        ]
        if shared
        else [],
        "source_root_unchanged": True,
        "egress_unchanged": True,
        "authorized": False,
        "account_digest": account.digest,
    }


def authorize_source_access(
    settings_path: Path,
    *,
    helper_path: Path,
    include_shared_config: bool = False,
    output_path: Path | None = None,
    bind: bool = False,
    secret_store: SecretStore | None = None,
) -> dict[str, Any]:
    """Explicit picker flow; grant-only by default and shadow-bind optional."""
    from sightglass.source.macos_wechat.config import (
        MacOSWeChatSettings,
        read_settings_bytes,
    )

    if output_path is not None and not bind:
        raise RuntimeError("source-access --output requires --bind")
    target = None
    if bind:
        if output_path is None:
            raise RuntimeError("source-access binding requires a new --output shadow file")
        target = output_path.expanduser().absolute()
        if target.resolve() == settings_path.resolve() or target.exists() or target.is_symlink():
            raise RuntimeError("shadow edge configuration must be a new file")
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent = target.parent.lstat()
        if (
            not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != os.getuid()
            or stat.S_IMODE(parent.st_mode) & 0o077
        ):
            raise RuntimeError("shadow edge configuration parent must be owner-private")
    settings = EdgeSettings.load(settings_path)
    if settings.source_kind != "macos-wechat":
        raise RuntimeError("source access requires a macos-wechat edge")
    settings.require_active()
    settings_before = _read_edge_config_bytes(settings_path)
    native_before = read_settings_bytes(settings.source_settings_path)
    native = MacOSWeChatSettings.from_dict(json.loads(native_before))
    if (
        native.instance_id != settings.source_instance_id
        or native.source_account_key != settings.account_id
    ):
        raise RuntimeError("edge native settings do not match the edge binding")
    try:
        expected_account = account_identity_from_source_root(native.source_root)
        expected_shared = (
            shared_config_identities_from_source_root(native.source_root)
            if include_shared_config
            else None
        )
        artifact = validate_helper(helper_path)
    except SourceAccessError as exc:
        raise RuntimeError(exc.code) from exc
    edge_secret = (secret_store or default_secret_store()).get(settings.capability_secret_account)
    receiver_proof = derive_receiver_proof(
        edge_secret,
        settings.receiver_context(expected_account),
    )
    _report, artifact = helper_authorize(
        artifact.app_path,
        expected_account,
        expected_shared_config=expected_shared,
        receiver_proof=receiver_proof,
    )
    if (
        _read_edge_config_bytes(settings_path) != settings_before
        or read_settings_bytes(settings.source_settings_path) != native_before
    ):
        raise RuntimeError("source changed during source-access authorization")
    value = settings.as_dict()
    value = enable_in_settings(value, artifact)
    value.update(
        {
            "source_access_shared_config": include_shared_config,
            "source_access_mode": SOURCE_ACCESS_MODE,
        }
    )
    rebound = EdgeSettings.from_dict(value)
    assert rebound.source_access_mode == SOURCE_ACCESS_MODE
    result: dict[str, Any] = {
        "schema": "sightglass.source-access-authorization.v1",
        "granted": True,
        "bound": False,
        "shared_config": include_shared_config,
        "account_scope": "configured_account",
        "helper_version": HELPER_VERSION,
        "activated": False,
        "settings_unchanged": True,
    }
    if bind:
        assert target is not None
        if _read_edge_config_bytes(settings_path) != settings_before:
            raise RuntimeError("edge configuration changed during binding")
        if read_settings_bytes(settings.source_settings_path) != native_before:
            raise RuntimeError("native settings changed during binding")
        write_private_json(target, rebound.as_dict())
        result.update(
            {
                "bound": True,
                "shadow": True,
                "settings_unchanged": True,
            }
        )
    return result

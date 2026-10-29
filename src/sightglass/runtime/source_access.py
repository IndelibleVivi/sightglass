"""Narrow macOS account-folder capability bridge.

The sandboxed helper owns user-selected app-scoped bookmarks. Runtime resolution
is noninteractive: it returns implicit bookmark bytes over private pipes and the
Python Edge resolves and holds those URLs for the complete source lifetime.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import selectors
import stat
import subprocess
import time
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

HELPER_SCHEMA = "sightglass.source-access.v3"
HELPER_BUNDLE_ID = "dev.sightglass.source-access"
HELPER_EXECUTABLE = "SightglassSourceAccess"
HELPER_VERSION = "3"
MAX_REQUEST_BYTES = 24 * 1024
MAX_RESPONSE_BYTES = 768 * 1024
DEFAULT_HELPER_TIMEOUT_SECONDS = 8.0
_NONCE = re.compile(r"[A-Za-z0-9_-]{16,128}")
RECEIVER_PROOF_DOMAIN = b"sightglass.source-access.receiver.v1\0"


class SourceAccessError(RuntimeError):
    """A finite, content-free source access failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class ReceiverContext:
    source_instance_id: str
    account_id: str
    account_path: str

    def __post_init__(self) -> None:
        if (
            not self.source_instance_id
            or not self.account_id
            or not Path(self.account_path).is_absolute()
        ):
            raise SourceAccessError("source_access_receiver_context_invalid")

    def binding(self) -> bytes:
        return json.dumps(
            {
                "account_id": self.account_id,
                "account_path": self.account_path,
                "schema": "sightglass.source-access.receiver-context.v1",
                "source_instance_id": self.source_instance_id,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()


def derive_receiver_proof(secret: str, context: ReceiverContext) -> str:
    """Purpose-separated, HMAC-bound proof; the relay credential never leaves Python."""
    if not secret:
        raise SourceAccessError("source_access_receiver_credential_missing")
    return hmac.new(
        secret.encode(),
        RECEIVER_PROOF_DOMAIN + context.binding(),
        hashlib.sha256,
    ).hexdigest()


@dataclass(frozen=True)
class SourceAccessIdentity:
    path: str
    inode: int
    device: int
    kind: str

    def __post_init__(self) -> None:
        if (
            not self.path
            or not Path(self.path).is_absolute()
            or "\x00" in self.path
            or type(self.inode) is not int
            or self.inode < 0
            or type(self.device) is not int
            or self.device < 0
            or self.kind
            not in {
                "account_directory",
                "shared_config_file",
                "shared_config_meta_file",
            }
        ):
            raise SourceAccessError("source_access_root_identity_invalid")

    def as_request(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "inode": self.inode,
            "device": self.device,
            "kind": self.kind,
        }

    @property
    def digest(self) -> str:
        payload = (
            f"sightglass.source-access.item.v3\x00{self.kind}\x00"
            f"{self.path}\x00{self.device}\x00{self.inode}"
        ).encode()
        return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class HelperArtifact:
    app_path: Path
    executable_path: Path
    digest: str
    size: int
    revision: tuple[int, int, int, int, int, int, int]


@dataclass(frozen=True)
class SourceAccessReport:
    mode: str
    capability_present: bool
    grants_access: bool
    account: SourceAccessIdentity | None
    account_digest: str | None
    shared_config: SourceAccessIdentity | None
    shared_config_digest: str | None
    shared_config_meta: SourceAccessIdentity | None
    shared_config_meta_digest: str | None
    account_bookmark: str | None = field(repr=False)
    shared_config_bookmark: str | None = field(repr=False)
    shared_config_meta_bookmark: str | None = field(repr=False)
    helper_digest: str
    entitlement_ready: bool | None
    receiver_proof_accepted: bool | None


def _private_directory(path: Path) -> None:
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise SourceAccessError("source_access_helper_directory_unsafe")


def _artifact_revision(metadata: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
        stat.S_IMODE(metadata.st_mode),
        metadata.st_uid,
    )


def _hash_fd(descriptor: int, expected: os.stat_result) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    offset = 0
    while True:
        chunk = os.pread(descriptor, 1024 * 1024, offset)
        if not chunk:
            break
        total += len(chunk)
        if total > expected.st_size:
            raise SourceAccessError("source_access_helper_changed")
        digest.update(chunk)
        offset += len(chunk)
    if total != expected.st_size:
        raise SourceAccessError("source_access_helper_changed")
    return digest.hexdigest(), total


def validate_helper(path: str | os.PathLike[str]) -> HelperArtifact:
    """Validate one fixed app bundle without executing it."""
    requested = Path(path).expanduser()
    if not requested.is_absolute():
        raise SourceAccessError("source_access_helper_path_invalid")
    try:
        app = requested.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise SourceAccessError("source_access_helper_missing") from exc
    if app.suffix.casefold() != ".app" or requested != app:
        raise SourceAccessError("source_access_helper_path_invalid")
    for directory in (app.parent, app):
        _private_directory(directory)
    contents = app / "Contents"
    _private_directory(contents)
    executable = contents / "MacOS" / HELPER_EXECUTABLE
    try:
        metadata = executable.lstat()
    except OSError as exc:
        raise SourceAccessError("source_access_helper_missing") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o7077
        or not stat.S_IMODE(metadata.st_mode) & stat.S_IXUSR
        or metadata.st_size <= 0
    ):
        raise SourceAccessError("source_access_helper_unsafe")
    info_path = contents / "Info.plist"
    try:
        info_metadata = info_path.lstat()
        info_payload = info_path.read_bytes()
    except OSError as exc:
        raise SourceAccessError("source_access_helper_invalid") from exc
    if (
        not stat.S_ISREG(info_metadata.st_mode)
        or info_metadata.st_nlink != 1
        or info_metadata.st_uid != os.geteuid()
        or stat.S_IMODE(info_metadata.st_mode) & 0o077
        or not 0 < len(info_payload) <= 64 * 1024
    ):
        raise SourceAccessError("source_access_helper_invalid")
    try:
        descriptor = os.open(executable, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise SourceAccessError("source_access_helper_missing") from exc
    try:
        before = os.fstat(descriptor)
        if _artifact_revision(before) != _artifact_revision(metadata):
            raise SourceAccessError("source_access_helper_changed")
        digest, size = _hash_fd(descriptor, before)
        after = os.fstat(descriptor)
        current = executable.lstat()
        if (
            _artifact_revision(after) != _artifact_revision(before)
            or _artifact_revision(current) != _artifact_revision(before)
            or size != before.st_size
        ):
            raise SourceAccessError("source_access_helper_changed")
    finally:
        os.close(descriptor)
    try:
        import plistlib

        info = plistlib.loads(info_payload)
    except (ValueError, TypeError) as exc:
        raise SourceAccessError("source_access_helper_invalid") from exc
    if (
        not isinstance(info, dict)
        or info.get("CFBundleIdentifier") != HELPER_BUNDLE_ID
        or info.get("CFBundleExecutable") != HELPER_EXECUTABLE
        or info.get("CFBundlePackageType") != "APPL"
    ):
        raise SourceAccessError("source_access_helper_identity_invalid")
    return HelperArtifact(
        app_path=app,
        executable_path=executable,
        digest=digest,
        size=size,
        revision=_artifact_revision(before),
    )


def helper_unchanged(artifact: HelperArtifact) -> bool:
    try:
        return validate_helper(artifact.app_path) == artifact
    except (OSError, SourceAccessError):
        return False


def _kind_identity(
    path: str | os.PathLike[str], *, kind: str, require_account_layout: bool
) -> SourceAccessIdentity:
    target = Path(path).expanduser()
    if not target.is_absolute():
        raise SourceAccessError("source_access_root_invalid")
    try:
        metadata = target.lstat()
        resolved = target.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise SourceAccessError("source_access_root_invalid") from exc
    if target != resolved or target.is_symlink():
        raise SourceAccessError("source_access_root_invalid")
    if kind == "account_directory":
        if not stat.S_ISDIR(metadata.st_mode):
            raise SourceAccessError("source_access_root_invalid")
        if require_account_layout:
            database_root = target / "db_storage"
            try:
                database_metadata = database_root.lstat()
                contact = (database_root / "contact/contact.db").lstat()
                session = (database_root / "session/session.db").lstat()
            except OSError as exc:
                raise SourceAccessError("source_access_account_layout_invalid") from exc
            if (
                database_root.is_symlink()
                or not stat.S_ISDIR(database_metadata.st_mode)
                or not stat.S_ISREG(contact.st_mode)
                or not stat.S_ISREG(session.st_mode)
            ):
                raise SourceAccessError("source_access_account_layout_invalid")
    elif kind in {"shared_config_file", "shared_config_meta_file"}:
        if not stat.S_ISREG(metadata.st_mode):
            raise SourceAccessError("source_access_root_invalid")
    else:
        raise SourceAccessError("source_access_root_identity_invalid")
    return SourceAccessIdentity(str(resolved), int(metadata.st_ino), int(metadata.st_dev), kind)


def account_identity_from_source_root(source_root: str | os.PathLike[str]) -> SourceAccessIdentity:
    """Bind only one supported ``<account>/db_storage`` layout."""
    database_root = Path(source_root).expanduser()
    if (
        not database_root.is_absolute()
        or database_root.name != "db_storage"
        or database_root.is_symlink()
    ):
        raise SourceAccessError("source_access_account_layout_invalid")
    account = database_root.parent
    if (
        account.name in {"all_users", "config", "Documents"}
        or account.parent.name != "xwechat_files"
    ):
        raise SourceAccessError("source_access_account_layout_invalid")
    resolved = _kind_identity(account, kind="account_directory", require_account_layout=True)
    if resolved.path != str(account.resolve()):
        raise SourceAccessError("source_access_account_layout_invalid")
    return resolved


def shared_config_identities_from_source_root(
    source_root: str | os.PathLike[str],
) -> tuple[SourceAccessIdentity, SourceAccessIdentity]:
    """Return the exact optional MMKV data and metadata files, never their parent."""
    database_root = Path(source_root).expanduser()
    try:
        all_accounts = database_root.parents[1]
    except IndexError as exc:
        raise SourceAccessError("source_access_account_layout_invalid") from exc
    if all_accounts.name != "xwechat_files":
        raise SourceAccessError("source_access_account_layout_invalid")
    target = all_accounts / "all_users" / "config" / "global_config"
    data = _kind_identity(target, kind="shared_config_file", require_account_layout=False)
    meta = _kind_identity(
        Path(f"{target}.crc"),
        kind="shared_config_meta_file",
        require_account_layout=False,
    )
    if data.path != str(target.resolve()) or meta.path != str(Path(f"{target}.crc").resolve()):
        raise SourceAccessError("source_access_root_invalid")
    return data, meta


def _request(
    mode: str,
    nonce: str,
    artifact: HelperArtifact,
    expected_account: SourceAccessIdentity | None,
    expected_shared: tuple[SourceAccessIdentity, SourceAccessIdentity] | None,
    receiver_proof: str | None,
) -> bytes:
    if mode not in {"check", "authorize", "lease"} or not _NONCE.fullmatch(nonce):
        raise SourceAccessError("source_access_request_invalid")
    value = {
        "schema": HELPER_SCHEMA,
        "mode": mode,
        "nonce": nonce,
        "expectedAccount": expected_account.as_request() if expected_account else None,
        "expectedSharedConfig": expected_shared[0].as_request() if expected_shared else None,
        "expectedSharedConfigMeta": (expected_shared[1].as_request() if expected_shared else None),
        "includeSharedConfig": expected_shared is not None,
        "expectedHelperDigest": artifact.digest,
        "expectedReceiverVerifier": hashlib.sha256(
            ("sightglass.source-access.receiver-verifier.v1\x00" + (receiver_proof or "")).encode()
        ).hexdigest(),
        "receiverProof": receiver_proof,
    }
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    if not 0 < len(payload) <= MAX_REQUEST_BYTES:
        raise SourceAccessError("source_access_request_size_invalid")
    return payload + b"\n"


def _strict_json(payload: bytes) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise SourceAccessError("source_access_response_invalid")
            result[key] = value
        return result

    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                SourceAccessError("source_access_response_invalid")
            ),
        )
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise SourceAccessError("source_access_response_invalid") from exc
    if not isinstance(value, dict):
        raise SourceAccessError("source_access_response_invalid")
    return value


def _identity(value: Any) -> SourceAccessIdentity:
    if (
        not isinstance(value, dict)
        or set(value) != {"path", "inode", "device", "kind"}
        or type(value["inode"]) is not int
        or type(value["device"]) is not int
    ):
        raise SourceAccessError("source_access_response_invalid")
    return SourceAccessIdentity(value["path"], value["inode"], value["device"], value["kind"])


def _bookmark(value: Any) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 64 * 1024
        or not re.fullmatch(r"[A-Za-z0-9+/]+={0,2}", value)
    ):
        raise SourceAccessError("source_access_response_invalid")
    return value


def _parse_report(
    value: dict[str, Any],
    mode: str,
    nonce: str,
    artifact: HelperArtifact,
    expected_account: SourceAccessIdentity | None,
    expected_shared: tuple[SourceAccessIdentity, SourceAccessIdentity] | None,
    expected_proof: str | None,
    expected_verifier: str,
) -> SourceAccessReport:
    required = {
        "schema",
        "ok",
        "mode",
        "nonce",
        "code",
        "helperVersion",
        "helperDigest",
        "capabilityPresent",
        "grantsAccess",
        "account",
        "accountDigest",
        "sharedConfig",
        "sharedConfigBookmark",
        "sharedConfigDigest",
        "sharedConfigMeta",
        "sharedConfigMetaBookmark",
        "sharedConfigMetaDigest",
        "accountBookmark",
        "receiverProofAccepted",
        "receiverVerifier",
        "entitlementReady",
        "containerReady",
    }
    # Swift Codable omits nil optionals. Missing nullable fields have the same
    # meaning as JSON null; unknown keys and missing protocol scalars still fail.
    scalars = {"schema", "ok", "mode", "grantsAccess"}
    if set(value) - required or not scalars <= set(value):
        raise SourceAccessError("source_access_response_invalid")
    value = {key: value.get(key) for key in required}
    if value["schema"] != HELPER_SCHEMA:
        raise SourceAccessError("source_access_response_invalid")
    if value["mode"] != mode or value["nonce"] != nonce:
        raise SourceAccessError("source_access_nonce_mismatch")
    if value["ok"] is not True:
        code = value.get("code")
        if not isinstance(code, str) or not re.fullmatch(r"source_access_[a-z0-9_]{1,96}", code):
            code = "source_access_helper_failed"
        raise SourceAccessError(code)
    if (
        value["helperVersion"] != HELPER_VERSION
        or value["helperDigest"] != artifact.digest
        or type(value["capabilityPresent"]) is not bool
        or type(value["grantsAccess"]) is not bool
        or (mode == "check" and value["grantsAccess"])
    ):
        raise SourceAccessError("source_access_helper_binding_changed")
    if mode == "check":
        if value["receiverProofAccepted"] is not False:
            raise SourceAccessError("source_access_response_invalid")
        if value["receiverVerifier"] is not None:
            raise SourceAccessError("source_access_response_invalid")
    elif expected_proof is None or value["receiverProofAccepted"] is not True:
        raise SourceAccessError("source_access_receiver_unauthorized")
    elif value["receiverVerifier"] != expected_verifier:
        raise SourceAccessError("source_access_receiver_unauthorized")
    account = _identity(value["account"]) if value["account"] is not None else None
    shared = _identity(value["sharedConfig"]) if value["sharedConfig"] is not None else None
    shared_meta = (
        _identity(value["sharedConfigMeta"]) if value["sharedConfigMeta"] is not None else None
    )
    account_bookmark = _bookmark(value["accountBookmark"])
    shared_bookmark = _bookmark(value["sharedConfigBookmark"])
    shared_meta_bookmark = _bookmark(value["sharedConfigMetaBookmark"])
    if mode == "check":
        if any(
            item is not None
            for item in (
                account,
                shared,
                shared_meta,
                account_bookmark,
                shared_bookmark,
                shared_meta_bookmark,
                value["accountDigest"],
                value["sharedConfigDigest"],
                value["sharedConfigMetaDigest"],
            )
        ):
            raise SourceAccessError("source_access_response_invalid")
    else:
        if expected_account is None or account != expected_account:
            raise SourceAccessError("source_access_account_binding_changed")
        if (
            value["accountDigest"] != expected_account.digest
            or account_bookmark is None
            or value["grantsAccess"] is not True
            or value["capabilityPresent"] is not True
        ):
            raise SourceAccessError("source_access_account_binding_changed")
        if expected_shared is None:
            if shared is not None and shared != expected_shared:
                raise SourceAccessError("source_access_shared_config_binding_changed")
        elif (
            shared != expected_shared[0]
            or shared_meta != expected_shared[1]
            or shared_bookmark is None
            or shared_meta_bookmark is None
        ):
            raise SourceAccessError("source_access_shared_config_binding_changed")
        if expected_shared is not None and (
            value["sharedConfigDigest"] != expected_shared[0].digest
            or value["sharedConfigMetaDigest"] != expected_shared[1].digest
        ):
            raise SourceAccessError("source_access_shared_config_binding_changed")
        if (shared is None) != (shared_meta is None):
            raise SourceAccessError("source_access_response_invalid")
    if value["entitlementReady"] not in {None, True, False}:
        raise SourceAccessError("source_access_response_invalid")
    if value["containerReady"] not in {None, True, False}:
        raise SourceAccessError("source_access_response_invalid")
    return SourceAccessReport(
        mode=mode,
        capability_present=bool(value["capabilityPresent"]),
        grants_access=bool(value["grantsAccess"]),
        account=account,
        account_digest=value["accountDigest"],
        shared_config=shared,
        shared_config_digest=value["sharedConfigDigest"],
        shared_config_meta=shared_meta,
        shared_config_meta_digest=value["sharedConfigMetaDigest"],
        account_bookmark=account_bookmark,
        shared_config_bookmark=shared_bookmark,
        shared_config_meta_bookmark=shared_meta_bookmark,
        helper_digest=artifact.digest,
        entitlement_ready=value["entitlementReady"],
        receiver_proof_accepted=value["receiverProofAccepted"],
    )


def _read_line(process: subprocess.Popen[bytes], timeout: float) -> dict[str, Any]:
    if process.stdout is None:
        raise SourceAccessError("source_access_helper_failed")
    descriptor = process.stdout.fileno()
    os.set_blocking(descriptor, False)
    payload = bytearray()
    deadline = time.monotonic() + timeout
    selector = selectors.DefaultSelector()
    try:
        selector.register(descriptor, selectors.EVENT_READ)
        while 10 not in payload:
            if len(payload) > MAX_RESPONSE_BYTES:
                raise SourceAccessError("source_access_response_size_invalid")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SourceAccessError("source_access_timeout")
            if not selector.select(remaining):
                raise SourceAccessError("source_access_timeout")
            try:
                chunk = os.read(descriptor, min(65_536, MAX_RESPONSE_BYTES + 1 - len(payload)))
            except BlockingIOError:
                continue
            except OSError as exc:
                raise SourceAccessError("source_access_helper_failed") from exc
            if not chunk:
                raise SourceAccessError("source_access_response_invalid")
            payload.extend(chunk)
    finally:
        selector.close()
    newline = payload.index(10)
    if newline > MAX_RESPONSE_BYTES:
        raise SourceAccessError("source_access_response_size_invalid")
    return _strict_json(bytes(payload[:newline]))


def _write_request(process: subprocess.Popen[bytes], payload: bytes, timeout: float) -> None:
    if process.stdin is None:
        raise SourceAccessError("source_access_helper_failed")
    descriptor = process.stdin.fileno()
    os.set_blocking(descriptor, False)
    deadline = time.monotonic() + timeout
    view = memoryview(payload)
    selector = selectors.DefaultSelector()
    try:
        selector.register(descriptor, selectors.EVENT_WRITE)
        while view:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SourceAccessError("source_access_timeout")
            if not selector.select(remaining):
                raise SourceAccessError("source_access_timeout")
            try:
                written = os.write(descriptor, view[:65_536])
            except BlockingIOError:
                continue
            except OSError as exc:
                raise SourceAccessError("source_access_helper_failed") from exc
            if written <= 0:
                raise SourceAccessError("source_access_helper_failed")
            view = view[written:]
    finally:
        selector.close()


def _terminate(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=2.0)


@dataclass
class _HelperOwner:
    process: subprocess.Popen[bytes]
    timeout: float
    closed: bool = False

    def close(self) -> None:
        if self.closed:
            return
        try:
            if self.process.stdin is not None:
                self.process.stdin.close()
        except (OSError, ValueError):
            pass
        try:
            self.process.wait(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            _terminate(self.process)
        finally:
            if self.process.stdout is not None:
                try:
                    self.process.stdout.close()
                except (OSError, ValueError):
                    pass
        self.closed = True


def _exchange(
    artifact: HelperArtifact,
    mode: str,
    expected_account: SourceAccessIdentity | None,
    expected_shared: tuple[SourceAccessIdentity, SourceAccessIdentity] | None,
    receiver_proof: str | None,
    timeout: float,
) -> tuple[SourceAccessReport, _HelperOwner | None]:
    nonce = secrets.token_urlsafe(24)
    request = _request(mode, nonce, artifact, expected_account, expected_shared, receiver_proof)
    try:
        process = subprocess.Popen(
            [str(artifact.executable_path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
            env={
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "HOME": os.environ.get("HOME", ""),
                "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
            },
        )
    except OSError as exc:
        raise SourceAccessError("source_access_helper_start_failed") from exc
    handed_off = False
    try:
        _write_request(process, request, timeout)
        value = _read_line(process, timeout)
        report = _parse_report(
            value,
            mode,
            nonce,
            artifact,
            expected_account,
            expected_shared,
            receiver_proof,
            hashlib.sha256(
                (
                    "sightglass.source-access.receiver-verifier.v1\x00" + (receiver_proof or "")
                ).encode()
            ).hexdigest(),
        )
        if mode == "lease":
            handed_off = True
            return report, _HelperOwner(process, timeout)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise SourceAccessError("source_access_timeout") from exc
        if process.returncode != 0:
            raise SourceAccessError("source_access_helper_failed")
        return report, None
    except BaseException:
        _terminate(process)
        raise
    finally:
        for stream in () if handed_off else (process.stdin, process.stdout):
            if stream is not None:
                try:
                    stream.close()
                except (OSError, ValueError):
                    pass


def check_helper(
    path: str | os.PathLike[str], *, timeout: float = DEFAULT_HELPER_TIMEOUT_SECONDS
) -> SourceAccessReport:
    artifact = validate_helper(path)
    report, owner = _exchange(artifact, "check", None, None, None, timeout)
    assert owner is None
    return report


def authorize_source_access(
    path: str | os.PathLike[str],
    expected_account: SourceAccessIdentity,
    *,
    expected_shared_config: tuple[SourceAccessIdentity, SourceAccessIdentity] | None = None,
    receiver_proof: str,
    timeout: float = 300.0,
) -> tuple[SourceAccessReport, HelperArtifact]:
    artifact = validate_helper(path)
    report, owner = _exchange(
        artifact,
        "authorize",
        expected_account,
        expected_shared_config,
        receiver_proof,
        timeout,
    )
    assert owner is None
    if not helper_unchanged(artifact):
        raise SourceAccessError("source_access_helper_changed")
    return report, artifact


@dataclass(frozen=True)
class ResolvedAccess:
    account_url: Any
    shared_config_url: Any | None
    shared_config_meta_url: Any | None


class SourceAccessLease(AbstractContextManager["SourceAccessLease"]):
    """Resolve implicit bookmarks and hold their security scopes in this process."""

    def __init__(
        self,
        helper_path: str | os.PathLike[str],
        expected_account: SourceAccessIdentity,
        *,
        expected_shared_config: tuple[SourceAccessIdentity, SourceAccessIdentity] | None = None,
        receiver_proof: str,
        timeout: float = DEFAULT_HELPER_TIMEOUT_SECONDS,
    ) -> None:
        self.helper_path = helper_path
        self.expected_account = expected_account
        self.expected_shared_config = expected_shared_config
        self.receiver_proof = receiver_proof
        self.timeout = timeout
        self.report: SourceAccessReport | None = None
        self._resolved: ResolvedAccess | None = None
        self._owner: _HelperOwner | None = None
        self._closed = False
        self._enter_and_resolve()

    def _enter_and_resolve(self) -> None:
        artifact = validate_helper(self.helper_path)
        report, owner = _exchange(
            artifact,
            "lease",
            self.expected_account,
            self.expected_shared_config,
            self.receiver_proof,
            self.timeout,
        )
        if owner is None:
            raise SourceAccessError("source_access_helper_handoff_invalid")
        self._owner = owner
        try:
            if not helper_unchanged(artifact):
                raise SourceAccessError("source_access_helper_changed")
            self.report = report
            self._resolved = _resolve_reports(report)
        except BaseException:
            owner.close()
            self._owner = None
            raise

    @property
    def resolved(self) -> ResolvedAccess:
        if self._resolved is None:
            raise SourceAccessError("source_access_lease_closed")
        return self._resolved

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._closed:
            return
        resolved, self._resolved = self._resolved, None
        try:
            if self._owner is not None:
                self._owner.close()
            self._owner = None
            self._closed = True
        finally:
            if resolved is not None:
                if resolved.shared_config_meta_url is not None:
                    _stop_url(resolved.shared_config_meta_url)
                if resolved.shared_config_url is not None:
                    _stop_url(resolved.shared_config_url)
                _stop_url(resolved.account_url)

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def __enter__(self) -> SourceAccessLease:
        return self

    def __repr__(self) -> str:
        return f"SourceAccessLease(closed={self._closed})"


def _foundation() -> tuple[Any, Any]:
    try:
        import Foundation  # type: ignore[import-not-found]
    except ImportError as exc:
        raise SourceAccessError("source_access_pyobjc_missing") from exc
    return getattr(Foundation, "NSURL"), Foundation


def _resolve_bookmark(bookmark: str, expected: SourceAccessIdentity) -> Any:
    nsurl, foundation = _foundation()
    try:
        resolution_options = int(foundation.NSURLBookmarkResolutionWithoutUI) | int(
            foundation.NSURLBookmarkResolutionWithoutImplicitStartAccessing
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise SourceAccessError("source_access_pyobjc_missing") from exc
    try:
        data = foundation.NSData.alloc().initWithBase64EncodedString_options_(bookmark, 0)
    except Exception as exc:
        raise SourceAccessError("source_access_capability_invalid") from exc
    if data is None:
        raise SourceAccessError("source_access_capability_invalid")
    try:
        alloc = nsurl.alloc()
        resolve = getattr(
            alloc,
            "initByResolvingBookmarkData_options_relativeToURL_bookmarkDataIsStale_error_",
        )
        result = resolve(data, resolution_options, None, None, None)
    except Exception as exc:
        raise SourceAccessError("source_access_capability_stale") from exc
    if not isinstance(result, tuple) or len(result) != 3:
        raise SourceAccessError("source_access_capability_stale")
    url, stale, _error = result
    if url is None or stale:
        raise SourceAccessError("source_access_capability_stale")
    try:
        started = bool(url.startAccessingSecurityScopedResource())
    except Exception as exc:
        raise SourceAccessError("source_access_capability_denied") from exc
    if not started:
        raise SourceAccessError("source_access_capability_denied")
    try:
        actual = _kind_identity(
            str(url.path()),
            kind=expected.kind,
            require_account_layout=expected.kind == "account_directory",
        )
        if actual != expected:
            raise SourceAccessError("source_access_capability_scope_changed")
    except BaseException:
        _stop_url(url)
        raise
    return url


def _resolve_reports(report: SourceAccessReport) -> ResolvedAccess:
    if report.account is None or report.account_bookmark is None:
        raise SourceAccessError("source_access_account_binding_changed")
    account_url = _resolve_bookmark(report.account_bookmark, report.account)
    shared_url = None
    shared_meta_url = None
    if report.shared_config is not None or report.shared_config_meta is not None:
        if (
            report.shared_config is None
            or report.shared_config_meta is None
            or report.shared_config_bookmark is None
            or report.shared_config_meta_bookmark is None
        ):
            _stop_url(account_url)
            raise SourceAccessError("source_access_shared_config_binding_changed")
        try:
            shared_url = _resolve_bookmark(report.shared_config_bookmark, report.shared_config)
            shared_meta_url = _resolve_bookmark(
                report.shared_config_meta_bookmark, report.shared_config_meta
            )
        except BaseException:
            if shared_url is not None:
                _stop_url(shared_url)
            _stop_url(account_url)
            raise
    return ResolvedAccess(
        account_url=account_url,
        shared_config_url=shared_url,
        shared_config_meta_url=shared_meta_url,
    )


def _stop_url(url: Any) -> None:
    try:
        url.stopAccessingSecurityScopedResource()
    except Exception:
        pass


def enable_in_settings(value: dict[str, Any], artifact: HelperArtifact) -> dict[str, Any]:
    required = validate_helper(artifact.app_path)
    if required != artifact:
        raise SourceAccessError("source_access_helper_changed")
    return {
        **value,
        "source_access_helper_path": str(required.app_path),
        "source_access_helper_digest": required.digest,
    }

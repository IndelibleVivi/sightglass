from __future__ import annotations

import hashlib
import os
import secrets
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Protocol

KEYCHAIN_SERVICE = "com.indeliblevivi.sightglass"
READER_SECRET_ACCOUNT = "mcp-reader-token"
OPERATOR_SECRET_ACCOUNT = "operator-token"
# One bounded read ceiling for every Linux secret; the stored values are opaque
# bearer tokens and derivation keys, never user content.
MAX_SECRET_BYTES = 4096
_SECRET_FILE_MODE = 0o600
_SECRET_DIRECTORY_MODE = 0o700


def semantic_secret_account(cf_account_id: str) -> str:
    return f"cloudflare-semantic-{cf_account_id}"


def new_token() -> str:
    return secrets.token_urlsafe(32)


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class SecretStore(Protocol):
    def get(self, account: str) -> str: ...

    def set(self, account: str, value: str) -> None: ...


class KeychainSecretStore:
    def __init__(self, service: str = KEYCHAIN_SERVICE) -> None:
        self.service = service

    def get(self, account: str) -> str:
        completed = subprocess.run(
            [
                "/usr/bin/security",
                "find-generic-password",
                "-a",
                account,
                "-s",
                self.service,
                "-w",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if completed.returncode != 0:
            raise RuntimeError(f"Sightglass Keychain secret is unavailable: {account}")
        return completed.stdout.rstrip("\n")

    def set(self, account: str, value: str) -> None:
        completed = subprocess.run(
            [
                "/usr/bin/security",
                "add-generic-password",
                "-U",
                "-a",
                account,
                "-s",
                self.service,
                "-w",
                value,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if completed.returncode != 0:
            raise RuntimeError(f"could not store Sightglass Keychain secret: {account}")


class MemorySecretStore:
    """Deterministic injectable boundary for synthetic tests only."""

    def __init__(self, values: dict[str, str] | None = None) -> None:
        self.values = dict(values or {})

    def get(self, account: str) -> str:
        try:
            return self.values[account]
        except KeyError as exc:
            raise RuntimeError(f"Sightglass test secret is unavailable: {account}") from exc

    def set(self, account: str, value: str) -> None:
        self.values[account] = value


class SyntheticTestEnvironmentSecretStore:
    """Subprocess-only test transport; production runtimes always use Keychain."""

    _variables = {
        READER_SECRET_ACCOUNT: "SIGHTGLASS_TEST_READER_TOKEN",
        OPERATOR_SECRET_ACCOUNT: "SIGHTGLASS_TEST_OPERATOR_TOKEN",
    }

    def get(self, account: str) -> str:
        if os.environ.get("SIGHTGLASS_SYNTHETIC_TEST_SECRETS") != "1":
            raise RuntimeError("synthetic test secret transport is disabled")
        value = os.environ.get(self._variables[account])
        if not value:
            raise RuntimeError("synthetic test secret is unavailable")
        return value

    def set(self, account: str, value: str) -> None:
        raise RuntimeError("synthetic test secrets are read-only process inputs")


def _safe_account_name(account: str) -> str:
    """Map one secret account name to a filesystem-safe, collision-free file name.

    Account names are fixed literals or generated hex labels, but the store still
    refuses to build a path from untrusted text: anything outside a conservative
    allowlist is rejected rather than escaped, so a crafted account can never traverse
    out of the private secrets directory or alias another account's file.
    """

    if (
        not account
        or len(account) > 128
        or account in {".", ".."}
        or any(
            character
            not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
            for character in account
        )
    ):
        raise RuntimeError("Sightglass secret account name is invalid")
    return account


def _open_private_directory(directory: Path, *, create: bool) -> int:
    """Open ``directory`` itself as a descriptor, proving it is owner-private.

    The directory is validated (owner, mode, not a symlink) and the *descriptor* is what
    later operations are anchored to via ``dir_fd``, so a secret is never read or written
    through a directory that was swapped for a symlink or loosened between the check and
    the access. ``O_NOFOLLOW`` refuses a final-component symlink. The caller closes the
    returned descriptor.
    """

    if create:
        if directory.is_symlink():
            raise RuntimeError("Sightglass secret directory cannot be a symlink")
        directory.mkdir(parents=True, mode=_SECRET_DIRECTORY_MODE, exist_ok=True)
    try:
        descriptor = os.open(
            directory, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
        )
    except OSError as exc:
        raise RuntimeError("Sightglass secret directory is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISDIR(metadata.st_mode):
            raise RuntimeError("Sightglass secret path is not a directory")
        if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise RuntimeError("Sightglass secret directory must be owner-private (0700)")
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


class FileSecretStore:
    """Owner-private on-disk secret store for a portable Linux production runtime.

    Every secret is a single-linked regular file inside one ``0700`` directory. The
    directory itself is opened as an ``O_DIRECTORY|O_NOFOLLOW`` descriptor and validated
    (owner, mode) before any read or write, so a secret is never read through a directory
    that was swapped for a symlink or loosened between the check and the access. Reads
    open the file with ``O_RDONLY|O_NOFOLLOW|O_NONBLOCK`` (the non-blocking flag keeps a
    FIFO or device from hanging before ``fstat`` proves a regular file), re-verify the
    inode, bound how many bytes may be read, and re-check the descriptor's identity and
    revision after the read so a swap or truncation mid-read is detected rather than
    trusted. Writes are atomic (a temp file in the same directory, ``fsync``,
    ``os.replace``, then a directory ``fsync``) and never reveal key material in an
    error. It exists so a Linux host does not need ``/usr/bin/security``; macOS keeps
    using the Keychain-backed store.
    """

    def __init__(self, directory: str | os.PathLike[str]) -> None:
        self.directory = Path(directory)

    def _file_name(self, account: str) -> str:
        return f"{_safe_account_name(account)}.secret"

    @staticmethod
    def _revision(metadata: os.stat_result) -> list[int]:
        return [
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mtime_ns,
            metadata.st_ctime_ns,
            stat.S_IMODE(metadata.st_mode),
        ]

    def get(self, account: str) -> str:
        name = self._file_name(account)
        try:
            directory_fd = _open_private_directory(self.directory, create=False)
        except RuntimeError:
            raise RuntimeError(f"Sightglass secret is unavailable: {account}") from None
        try:
            try:
                descriptor = os.open(
                    name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
                    dir_fd=directory_fd,
                )
            except OSError as exc:
                # A missing file, a symlink refused by O_NOFOLLOW, a FIFO that could
                # otherwise block, and an unreadable path all surface one reason.
                raise RuntimeError(f"Sightglass secret is unavailable: {account}") from exc
            try:
                metadata = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_nlink != 1
                    or metadata.st_uid != os.getuid()
                    or stat.S_IMODE(metadata.st_mode) & 0o077
                ):
                    raise RuntimeError(f"Sightglass secret is unavailable: {account}")
                if metadata.st_size > MAX_SECRET_BYTES:
                    raise RuntimeError(f"Sightglass secret is unavailable: {account}")
                before = self._revision(metadata)
                os.lseek(descriptor, 0, os.SEEK_SET)
                chunks: list[bytes] = []
                remaining = MAX_SECRET_BYTES + 1
                while remaining > 0:
                    chunk = os.read(descriptor, min(65_536, remaining))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                data = b"".join(chunks)
                after = self._revision(os.fstat(descriptor))
            finally:
                os.close(descriptor)
        finally:
            os.close(directory_fd)
        if before != after or len(data) != before[2]:
            # The file was replaced, truncated, or appended to while it was read.
            raise RuntimeError(f"Sightglass secret is unavailable: {account}")
        if not data or len(data) > MAX_SECRET_BYTES:
            raise RuntimeError(f"Sightglass secret is unavailable: {account}")
        try:
            return data.decode("utf-8").rstrip("\n")
        except UnicodeDecodeError as exc:
            raise RuntimeError(f"Sightglass secret is unavailable: {account}") from exc

    def set(self, account: str, value: str) -> None:
        encoded = value.encode("utf-8")
        if not encoded or len(encoded) > MAX_SECRET_BYTES:
            raise RuntimeError("Sightglass secret value is out of bounds")
        name = self._file_name(account)
        directory_fd = _open_private_directory(self.directory, create=True)
        temporary_name = ""
        try:
            # ``mkstemp`` in the validated directory; the temp file is created 0600 by
            # mkstemp and re-asserted, so it is never briefly world-readable.
            descriptor, temporary_path = tempfile.mkstemp(prefix=".secret-", dir=self.directory)
            temporary_name = temporary_path
            os.fchmod(descriptor, _SECRET_FILE_MODE)
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            # ``renameat`` against the already-validated directory descriptor replaces
            # any existing file by name only; a symlink or a swapped directory cannot
            # redirect this write outside the private namespace.  Both names are
            # relative to the descriptor, never absolute paths.
            os.rename(
                os.path.basename(temporary_path),
                name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
            temporary_name = ""
            os.fsync(directory_fd)
        except OSError as exc:
            raise RuntimeError(f"could not store Sightglass secret: {account}") from exc
        finally:
            if temporary_name:
                try:
                    os.unlink(temporary_name)
                except OSError:
                    pass
            os.close(directory_fd)

def default_secret_directory() -> Path:
    """The private directory the Linux file store uses when none is configured."""

    override = os.environ.get("SIGHTGLASS_SECRETS_DIR")
    if override:
        return Path(override).expanduser()
    xdg_state = os.environ.get("XDG_STATE_HOME") or str(Path.home() / ".local" / "state")
    return Path(xdg_state) / "sightglass" / "secrets"


def default_secret_store() -> SecretStore:
    if os.environ.get("SIGHTGLASS_SYNTHETIC_TEST_SECRETS") == "1":
        return SyntheticTestEnvironmentSecretStore()
    if sys.platform.startswith("linux"):
        return FileSecretStore(default_secret_directory())
    return KeychainSecretStore()

from __future__ import annotations

import hashlib
import os
import secrets
import subprocess
from typing import Protocol

KEYCHAIN_SERVICE = "com.indeliblevivi.sightglass"
READER_SECRET_ACCOUNT = "mcp-reader-token"
OPERATOR_SECRET_ACCOUNT = "operator-token"


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


def default_secret_store() -> SecretStore:
    if os.environ.get("SIGHTGLASS_SYNTHETIC_TEST_SECRETS") == "1":
        return SyntheticTestEnvironmentSecretStore()
    return KeychainSecretStore()

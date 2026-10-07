"""Verified voice capture for the local transcription pipeline.

Capture re-verifies every precondition of one leased job before any recognizer work
starts: reader pause/deny, conversation policy, the resource's active binding and owning
account, and the recorded input revision the job was admitted for.  The SILK bytes come
from the resource service's own two-phase snapshot read (never from a private shortcut),
are pinned by digest, and are written to a private staging file that the job lease owns
and the pipeline deletes as soon as it is done.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.resources.types import ResourceReadPayload

from .decoder import MAX_SILK_BYTES

STAGING_DIRECTORY_NAME = "voice-work"
STAGING_FILE_MODE = 0o600
STAGING_DIRECTORY_MODE = 0o700
DEFAULT_STALE_STAGING_SECONDS = 3600.0
VOICE_MIME = "audio/silk"


@dataclass(frozen=True)
class CapturedVoice:
    job_id: str
    resource_id: str
    message_id: str
    resource_revision: str
    account_id: str
    account_binding_id: str | None
    input_digest: str
    byte_size: int
    silk_path: Path

    def release(self) -> None:
        unlink_quietly(self.silk_path)


def unlink_quietly(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def _blocked(reason: str) -> SightglassError:
    return SightglassError(
        ErrorCode.RESOURCE_BLOCKED, details={"reason": reason, "stage": "capture"}
    )


def _transient(error: SightglassError) -> SightglassError:
    return SightglassError(
        error.code,
        retryable=True,
        details={**error.details, "stage": "capture"},
    )


_TRANSIENT_CAPTURE_CODES = {
    ErrorCode.SERVICE_PAUSED,
    ErrorCode.SOURCE_GENERATION_CHANGED,
    ErrorCode.SERVICE_TIMEOUT,
    ErrorCode.STORAGE_PRESSURE,
}


class VoiceCapture:
    """Read one job's exact voice binding inside the resource service's snapshot rules."""

    def __init__(
        self,
        resource_service: Any,
        repository: Any,
        staging_root: Path,
        *,
        max_bytes: int = MAX_SILK_BYTES,
    ) -> None:
        self.resource_service = resource_service
        self.repository = repository
        self.staging_root = Path(staging_root)
        self.max_bytes = int(max_bytes)

    # -- staging -----------------------------------------------------------

    def prepare_root(self) -> None:
        root = self.staging_root
        if root.is_symlink():
            raise RuntimeError("voice staging directory cannot be a symlink")
        root.mkdir(parents=True, mode=STAGING_DIRECTORY_MODE, exist_ok=True)
        metadata = root.stat()
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise RuntimeError(f"voice staging directory must be private: {root}")

    def sweep_stale(self, *, max_age_seconds: float = DEFAULT_STALE_STAGING_SECONDS) -> int:
        """Remove staging files left by a crashed pipeline; never recursive."""

        if not self.staging_root.is_dir():
            return 0
        deadline = time.time() - float(max_age_seconds)
        removed = 0
        for entry in self.staging_root.iterdir():
            try:
                metadata = entry.lstat()
            except OSError:
                continue
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_mtime > deadline:
                continue
            unlink_quietly(entry)
            removed += 1
        return removed

    def staging_path(self, job_id: str, suffix: str) -> Path:
        safe = "".join(
            character for character in job_id if character.isalnum() or character in "_-"
        )
        return self.staging_root / f"{safe or 'job'}.{suffix}"

    def _write_staging(self, job_id: str, data: bytes) -> Path:
        self.prepare_root()
        path = self.staging_path(job_id, "silk")
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW,
                             STAGING_FILE_MODE)
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
        except Exception:
            unlink_quietly(path)
            raise
        os.chmod(path, STAGING_FILE_MODE)
        return path

    # -- capture -----------------------------------------------------------

    def _read_voice_original(self, resource_id: str) -> ResourceReadPayload:
        try:
            return self.resource_service.read_resource(
                resource_id=resource_id,
                mode="original",
                page=None,
                start_line=None,
                end_line=None,
                max_bytes=self.max_bytes,
                member=None,
                sheet=None,
                cell_range=None,
            )
        except SightglassError as error:
            if error.code in _TRANSIENT_CAPTURE_CODES:
                raise _transient(error) from error
            raise _blocked(f"resource_{error.code.value.lower()}") from error

    def _recorded_state(
        self, resource_id: str, account_id: str
    ) -> tuple[str | None, str | None, str]:
        row = self.repository.resource_context(resource_id)
        if row is None:
            raise _blocked("resource_missing")
        resolver = _resolver(row)
        if resolver.get("active", True) is False:
            raise _blocked("resource_inactive")
        if str(row["account_id"]) != account_id:
            raise _blocked("resource_account_mismatch")
        fingerprint = resolver.get("binding_fingerprint")
        revision = str(fingerprint) if isinstance(fingerprint, str) and fingerprint else None
        binding: str | None = None
        for account in self.repository.active_accounts():
            if str(account["account_id"]) != account_id:
                continue
            raw = account["account_binding_id"]
            binding = None if raw is None else str(raw)
            return revision, binding, str(row["message_id"])
        raise _blocked("account_missing")

    def capture(self, job: Mapping[str, Any]) -> CapturedVoice:
        job_id = str(job.get("job_id") or "")
        resource_id = str(job.get("resource_id") or "")
        revision = str(job.get("resource_revision") or "")
        account_id = str(job.get("account_id") or "")
        raw_binding = job.get("account_binding_id")
        account_binding = None if raw_binding is None else str(raw_binding)
        # ``voice_jobs`` carries the resource binding, not the message: message identity is
        # read back from the recorded resource context below, never taken on trust.
        if not job_id or not resource_id or not revision or not account_id:
            raise _blocked("job_input_incomplete")

        payload = self._read_voice_original(resource_id)
        if payload.data is None or payload.content_kind != "audio":
            raise _blocked("not_audio_original")
        if payload.mime_type != VOICE_MIME:
            raise _blocked("unexpected_audio_mime")
        data = bytes(payload.data)
        if not data:
            raise _blocked("empty_original")
        if len(data) > self.max_bytes:
            raise _blocked("input_too_large")

        current_revision, current_binding, message_id = self._recorded_state(
            resource_id, account_id
        )
        if current_binding != account_binding:
            raise _blocked("account_binding_changed")
        if current_revision is None:
            raise _blocked("resource_revision_unrecorded")
        if current_revision != revision:
            raise _blocked("resource_revision_changed")

        silk_path = self._write_staging(job_id, data)
        return CapturedVoice(
            job_id=job_id,
            resource_id=resource_id,
            message_id=message_id,
            resource_revision=revision,
            account_id=account_id,
            account_binding_id=account_binding,
            input_digest=hashlib.sha256(data).hexdigest(),
            byte_size=len(data),
            silk_path=silk_path,
        )


def _resolver(row: Any) -> dict[str, Any]:
    try:
        value = json.loads(str(row["resolver_json"]))
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise _blocked("resource_resolver_invalid") from exc
    if not isinstance(value, dict):
        raise _blocked("resource_resolver_invalid")
    return value

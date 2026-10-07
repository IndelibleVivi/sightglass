"""Apple SpeechAnalyzer transcription over the precompiled Swift helper.

The helper is a private, precompiled executable (``scripts/compile-voice-helper.sh``)
that runs Apple's on-device ``SpeechAnalyzer`` + ``SpeechTranscriber`` over one audio
file and prints one JSON line.  This module starts one helper per job with a wall-clock
watchdog, bounded streams, and a process-group kill, so a stuck or chatty helper cannot
hold the daemon or grow without limit.

The helper process's RSS is deliberately *not* treated as proof of anything about the
machine: macOS speech assets are shared system resources, and the daemon only bounds its
own child's runtime and output.
"""

from __future__ import annotations

import json
import os
import stat
import time
from collections.abc import Mapping
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.voice import VoiceTranscription
from sightglass.storage import StorageBudget

from .capture import CapturedVoice, VoiceCapture
from .decoder import DecodedAudio, SilkDecoder, pcm_recipe
from .subprocess import BoundedProcessError, run_bounded

HELPER_NAME = "sightglass-transcribe"
HELPER_SCHEMA = "sightglass.voice-transcript.v1"
DEFAULT_HELPER_TIMEOUT_SECONDS = 120.0
MAX_HELPER_STDOUT_BYTES = 2 * 1024 * 1024
MAX_HELPER_STDERR_BYTES = 64 * 1024
MAX_TRANSCRIPT_CHARS = 200_000
EXIT_MODEL_UNAVAILABLE = 2
EXIT_TRANSCRIPTION_FAILED = 3
EXIT_USAGE = 4
_ENV_ALLOWLIST = ("HOME", "LANG", "LC_ALL", "PATH", "TMPDIR", "USER")
_ENV_DENY_MARKERS = ("TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "KEY")


def helper_environment(environment: Mapping[str, str] | None = None) -> dict[str, str]:
    """Pass only the variables the helper needs, never credential-shaped ones."""

    source = dict(os.environ if environment is None else environment)
    allowed: dict[str, str] = {}
    for name in _ENV_ALLOWLIST:
        if name in source:
            allowed[name] = source[name]
    return {
        name: value
        for name, value in allowed.items()
        if not any(marker in name.upper() for marker in _ENV_DENY_MARKERS)
    }


@dataclass(frozen=True)
class HelperReadiness:
    present: bool
    executable: bool
    blocked_reason: str | None


@dataclass(frozen=True)
class HelperTranscript:
    text: str
    locale: str
    backend: str
    helper_version: str | None
    asset_status: str
    model: dict[str, Any]
    os_version: str | None
    segments: int

    def provenance(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "helper": HELPER_NAME,
            "helper_version": self.helper_version,
            "locale": self.locale,
            "asset_status": self.asset_status,
            "model": self.model,
            "os_version": self.os_version,
            "segments": self.segments,
            "volatile_excluded": True,
        }


def probe_helper(path: Path | None) -> HelperReadiness:
    """Content-free readiness of the compiled helper (no subprocess, no path in output)."""

    if path is None:
        return HelperReadiness(False, False, "helper_not_configured")
    try:
        metadata = path.lstat()
    except OSError:
        return HelperReadiness(False, False, "helper_missing")
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        return HelperReadiness(False, False, "helper_not_regular")
    if metadata.st_size <= 0:
        return HelperReadiness(False, False, "helper_empty")
    if not os.access(path, os.X_OK):
        return HelperReadiness(False, False, "helper_not_executable")
    return HelperReadiness(True, True, None)


def _blocked(reason: str) -> SightglassError:
    return SightglassError(
        ErrorCode.RESOURCE_BLOCKED, details={"reason": reason, "stage": "recognize"}
    )


def _timeout(reason: str) -> SightglassError:
    return SightglassError(
        ErrorCode.SERVICE_TIMEOUT, retryable=True, details={"reason": reason, "stage": "recognize"}
    )


class AppleSpeechHelper:
    """Run the precompiled Swift helper once per audio file inside hard bounds."""

    def __init__(
        self,
        helper_path: Path,
        *,
        locale: str,
        timeout_seconds: float = DEFAULT_HELPER_TIMEOUT_SECONDS,
    ) -> None:
        self.helper_path = Path(helper_path)
        self.locale = locale
        self.timeout_seconds = float(timeout_seconds)

    def _budgeted_timeout(self, deadline: float | None) -> float:
        timeout = self.timeout_seconds
        if deadline is None:
            return timeout
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise _timeout("operation_deadline")
        return max(0.1, min(timeout, remaining))

    def transcribe_pcm(self, pcm_path: Path, *, deadline: float | None = None) -> HelperTranscript:
        readiness = probe_helper(self.helper_path)
        if readiness.blocked_reason is not None:
            raise _blocked(readiness.blocked_reason)
        timeout = self._budgeted_timeout(deadline)
        try:
            outcome = run_bounded(
                (
                    str(self.helper_path),
                    "--pcm",
                    str(pcm_path),
                    "--locale",
                    self.locale,
                ),
                timeout_seconds=timeout,
                max_stdout_bytes=MAX_HELPER_STDOUT_BYTES,
                max_stderr_bytes=MAX_HELPER_STDERR_BYTES,
                env=helper_environment(),
            )
        except BoundedProcessError as exc:
            raise _blocked("helper_spawn_failed") from exc
        if outcome.timed_out:
            raise _timeout("helper_timeout")
        if outcome.limit_exceeded is not None:
            raise _blocked(f"helper_{outcome.limit_exceeded}_limit")
        if outcome.exit_code == EXIT_MODEL_UNAVAILABLE:
            raise _blocked(_helper_error(outcome.stderr) or "model_not_installed")
        if outcome.exit_code == EXIT_USAGE:
            raise _blocked(_helper_error(outcome.stderr) or "helper_usage")
        if outcome.exit_code != 0:
            raise SightglassError(
                ErrorCode.SERVICE_UNAVAILABLE,
                retryable=True,
                details={
                    "reason": _helper_error(outcome.stderr) or "helper_failed",
                    "stage": "recognize",
                },
            )
        return _parse_transcript(outcome.stdout, fallback_locale=self.locale)


def _helper_error(stderr: bytes) -> str | None:
    try:
        parsed = json.loads(stderr.decode("utf-8", "replace").strip().splitlines()[-1])
    except (IndexError, ValueError):
        return None
    if isinstance(parsed, dict) and isinstance(parsed.get("error"), str):
        return str(parsed["error"])
    return None


def _parse_transcript(stdout: bytes, *, fallback_locale: str) -> HelperTranscript:
    try:
        parsed = json.loads(stdout.decode("utf-8", "replace").strip())
    except ValueError as exc:
        raise _blocked("helper_report_invalid") from exc
    if not isinstance(parsed, dict) or parsed.get("schema") != HELPER_SCHEMA:
        raise _blocked("helper_report_invalid")
    text = parsed.get("text")
    if not isinstance(text, str):
        raise _blocked("helper_report_invalid")
    if len(text) > MAX_TRANSCRIPT_CHARS:
        raise _blocked("helper_text_too_long")
    segments = parsed.get("segments")
    model = parsed.get("model")
    version = parsed.get("helper_version")
    os_version = parsed.get("os_version")
    return HelperTranscript(
        text=text,
        locale=str(parsed.get("locale") or fallback_locale),
        backend=str(parsed.get("backend") or "unknown"),
        helper_version=str(version) if isinstance(version, str) else None,
        asset_status=str(parsed.get("asset_status") or "unknown"),
        model=dict(model) if isinstance(model, dict) else {"identifier": "unknown"},
        os_version=str(os_version) if isinstance(os_version, str) else None,
        segments=len(segments) if isinstance(segments, list) else 0,
    )


class AppleSilkTranscriber:
    """Capture → bounded decode → Apple helper, with a content-free provenance record."""

    def __init__(
        self,
        *,
        capture: VoiceCapture,
        decoder: SilkDecoder,
        helper: AppleSpeechHelper,
        recipe_engine: str = "sightglass.voice.apple-silk.v1",
        storage: StorageBudget | None = None,
    ) -> None:
        self.capture = capture
        self.decoder = decoder
        self.helper = helper
        self.recipe_engine = recipe_engine
        self.storage = storage

    def transcribe(
        self, job: dict[str, Any], *, duration_ms: int, deadline: float | None
    ) -> VoiceTranscription:
        budget = self.storage
        reservation = (
            budget.reserve(self.capture.max_bytes + self.decoder.max_pcm_bytes, background=True)
            if budget is not None else nullcontext()
        )
        with reservation:
            return self._transcribe(job, duration_ms=duration_ms, deadline=deadline)

    def _transcribe(
        self, job: dict[str, Any], *, duration_ms: int, deadline: float | None
    ) -> VoiceTranscription:
        captured: CapturedVoice | None = None
        pcm_path: Path | None = None
        try:
            captured = self.capture.capture(job)
            pcm_path = self.capture.staging_path(captured.job_id, "pcm")
            decoded = self.decoder.decode(captured.silk_path, pcm_path, deadline=deadline)
            transcript = self.helper.transcribe_pcm(pcm_path, deadline=deadline)
            return VoiceTranscription(
                text=transcript.text,
                provenance=self._provenance(
                    captured=captured,
                    decoded=decoded,
                    transcript=transcript,
                    declared_duration_ms=duration_ms,
                ),
            )
        finally:
            if captured is not None:
                captured.release()
            if pcm_path is not None:
                try:
                    pcm_path.unlink()
                except OSError:
                    pass
            if self.storage is not None:
                paths = [path for path in (pcm_path, captured.silk_path if captured else None)
                         if path is not None]
                self.storage.track(*paths)

    def _provenance(
        self,
        *,
        captured: CapturedVoice,
        decoded: DecodedAudio,
        transcript: HelperTranscript,
        declared_duration_ms: int,
    ) -> dict[str, Any]:
        return {
            "schema": "sightglass.voice-provenance.v1",
            "recipe": {
                "engine": self.recipe_engine,
                "decoder": decoded.decoder,
                "decoder_envelope": decoded.envelope,
                "pcm": pcm_recipe(),
            },
            "input": {
                "resource_revision": captured.resource_revision,
                "input_digest": captured.input_digest,
                "silk_bytes": captured.byte_size,
                "pcm_bytes": decoded.byte_size,
                "pcm_frames": decoded.frames,
                "decoded_duration_ms": decoded.duration_ms,
                "declared_duration_ms": declared_duration_ms,
            },
            "recognizer": transcript.provenance(),
            "derived": {"kind": "derived_transcript", "translation": False},
        }

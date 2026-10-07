"""Assemble the local voice recognizer for one daemon configuration.

The daemon never guesses whether transcription is possible: this module probes the
decoder extra, the precompiled Swift helper, and the configured language, and returns
either a ready recognizer or a content-free blocked readiness record.  Test builds inject
their own transcriber instead, and production code never fabricates a fake one.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sightglass.voice.apple import (
    DEFAULT_HELPER_TIMEOUT_SECONDS,
    HELPER_NAME,
    AppleSilkTranscriber,
    AppleSpeechHelper,
    probe_helper,
)
from sightglass.voice.capture import STAGING_DIRECTORY_NAME, VoiceCapture
from sightglass.voice.decoder import SilkDecoder

from .voice_worker import Transcriber

READINESS_SCHEMA = "sightglass.voice-readiness.v1"
INJECTED_TRANSCRIBER = "injected"


@dataclass(frozen=True)
class VoiceSetup:
    transcriber: Transcriber | None
    readiness: dict[str, Any]

    @property
    def blocked_reason(self) -> str | None:
        reason = self.readiness.get("blocked_reason")
        return str(reason) if isinstance(reason, str) else None

    def sweep_staging(self, *, max_age_seconds: float | None = None) -> int:
        capture = getattr(self.transcriber, "capture", None)
        if not isinstance(capture, VoiceCapture):
            return 0
        if max_age_seconds is None:
            return capture.sweep_stale()
        return capture.sweep_stale(max_age_seconds=max_age_seconds)


def resolve_helper_path(config: Any) -> Path:
    configured = str(getattr(config, "voice_helper_path", "") or "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path(config.data_dir) / "voice" / HELPER_NAME


def readiness_payload(
    *,
    enabled: bool,
    language: str,
    transcriber: str | None,
    decoder_available: bool,
    decoder_version: str | None,
    helper_present: bool,
    helper_executable: bool,
    blocked_reason: str | None,
) -> dict[str, Any]:
    """Content-free readiness: booleans, names, and reasons, never paths or key material."""

    return {
        "schema": READINESS_SCHEMA,
        "enabled": bool(enabled),
        "language": language,
        "transcriber": transcriber,
        "decoder": {
            "name": "silk-python",
            "available": bool(decoder_available),
            "version": decoder_version,
        },
        "helper": {
            "name": HELPER_NAME,
            "present": bool(helper_present),
            "executable": bool(helper_executable),
        },
        "ready": transcriber is not None,
        "blocked_reason": blocked_reason,
    }


def injected_readiness(transcriber: Transcriber) -> dict[str, Any]:
    return {
        "schema": READINESS_SCHEMA,
        "enabled": True,
        "language": "injected",
        "transcriber": INJECTED_TRANSCRIBER,
        "decoder": {"name": "injected", "available": True, "version": None},
        "helper": {"name": "injected", "present": True, "executable": True},
        "ready": True,
        "blocked_reason": None,
        "injected_type": type(transcriber).__name__,
    }


def build_voice_setup(config: Any, service: Any) -> VoiceSetup:
    """Probe one daemon configuration and build the real recognizer when it is complete."""

    enabled = bool(getattr(config, "voice_enabled", False))
    language = str(getattr(config, "voice_language", "auto") or "auto")
    decoder = SilkDecoder()
    decoder_available = decoder.available()
    decoder_version = SilkDecoder.version()
    helper_path = resolve_helper_path(config)
    probe = probe_helper(helper_path)
    helper_present = probe.present or probe.blocked_reason == "helper_empty"
    helper_executable = probe.executable

    def record(reason: str | None) -> dict[str, Any]:
        return readiness_payload(
            enabled=enabled,
            language=language,
            transcriber=None,
            decoder_available=decoder_available,
            decoder_version=decoder_version,
            helper_present=helper_present,
            helper_executable=helper_executable,
            blocked_reason=reason,
        )

    if not enabled:
        return VoiceSetup(None, record("voice_disabled"))
    if not decoder_available:
        return VoiceSetup(None, record("decoder_unavailable"))
    if probe.blocked_reason is not None:
        return VoiceSetup(None, record(probe.blocked_reason))

    capture = VoiceCapture(
        service.resource_service, service.repository, _staging_root(config),
    )
    try:
        capture.prepare_root()
    except (OSError, RuntimeError):
        return VoiceSetup(None, record("staging_unavailable"))
    staging_files = list(capture.staging_root.iterdir())
    capture.sweep_stale()
    if service.repository.database.storage is not None:
        service.repository.database.storage.track(*staging_files)
    transcriber = AppleSilkTranscriber(
        capture=capture,
        decoder=decoder,
        storage=service.repository.database.storage,
        helper=AppleSpeechHelper(
            helper_path,
            locale=language,
            timeout_seconds=float(
                getattr(config, "voice_helper_timeout_seconds", DEFAULT_HELPER_TIMEOUT_SECONDS)
                or DEFAULT_HELPER_TIMEOUT_SECONDS
            ),
        ),
    )
    return VoiceSetup(
        transcriber,
        readiness_payload(
            enabled=True,
            language=language,
            transcriber="apple-silk",
            decoder_available=True,
            decoder_version=decoder_version,
            helper_present=True,
            helper_executable=True,
            blocked_reason=None,
        ),
    )


def _staging_root(config: Any) -> Path:
    return Path(config.data_dir) / STAGING_DIRECTORY_NAME

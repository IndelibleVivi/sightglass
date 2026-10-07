"""Assemble the local voice recognizer for one daemon configuration.

The daemon never guesses whether transcription is possible: this module probes the
decoder extra, the precompiled Swift helper, and the configured language, and returns
either a ready recognizer or a content-free blocked readiness record.  Test builds inject
their own transcriber instead, and production code never fabricates a fake one.
"""

from __future__ import annotations

import sys
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
from sightglass.voice.linux import (
    DEFAULT_MAX_HELPER_RSS_BYTES,
    LinuxSilkTranscriber,
    LinuxWhisperHelper,
    ModelBinding,
    SystemdRunner,
)
from sightglass.voice.linux import (
    probe_helper as probe_linux_helper,
)
from sightglass.voice.linux import (
    resolve_model_binding as linux_resolve_model_binding,
)

from .voice_worker import Transcriber

READINESS_SCHEMA = "sightglass.voice-readiness.v1"
INJECTED_TRANSCRIBER = "injected"
APPLE_TRANSCRIBER = "apple-silk"
LINUX_TRANSCRIBER = "linux-whisper"


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
    return Path(config.data_dir) / "voice" / _helper_name()


def resolve_voice_model(helper_path: Path) -> ModelBinding | None:
    """The whisper.cpp model a Linux helper is bound to, or ``None`` on macOS.

    The binding comes from the ``models/model.json`` manifest the build script writes
    next to the helper, so one explicit ``voice.helper_path`` still names the whole Linux
    recognizer without inventing a second configuration owner.  There is no separate
    model config key; an operator who keeps the model elsewhere points
    ``voice.helper_path`` at a helper whose adjacent manifest already matches.
    """

    if not _is_linux():
        return None
    return linux_resolve_model_binding(helper_path)


def _is_linux() -> bool:
    return sys.platform.startswith("linux")

def production_runner_available() -> bool:
    """Whether a genuinely isolated production runner exists on this host."""

    return SystemdRunner(max_rss_bytes=DEFAULT_MAX_HELPER_RSS_BYTES).available()


def _helper_name() -> str:
    return "sightglass-whisper" if _is_linux() else HELPER_NAME


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
    helper_name: str = HELPER_NAME,
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
            "name": helper_name,
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
    """Probe one daemon configuration and build the real recognizer when it is complete.

    The recognizer is platform-selected: macOS links Apple's on-device
    ``SpeechAnalyzer`` helper, Linux runs an operator-prepared whisper.cpp helper.  Both
    consume the same bounded SILK→PCM capture pipeline and neither fabricates a fake
    recognizer.
    """

    enabled = bool(getattr(config, "voice_enabled", False))
    language = str(getattr(config, "voice_language", "auto") or "auto")
    decoder = SilkDecoder()
    decoder_available = decoder.available()
    decoder_version = SilkDecoder.version()
    helper_path = resolve_helper_path(config)
    helper_name = _helper_name()
    if _is_linux():
        model: ModelBinding | None = resolve_voice_model(helper_path)
        probe = probe_linux_helper(helper_path, model=model)
    else:
        model = None
        probe = probe_helper(helper_path)
    helper_present = probe.present or probe.blocked_reason == "helper_empty"
    helper_executable = probe.executable

    def record(reason: str | None, transcriber: str | None = None) -> dict[str, Any]:
        return readiness_payload(
            enabled=enabled,
            language=language,
            transcriber=transcriber,
            decoder_available=decoder_available,
            decoder_version=decoder_version,
            helper_present=helper_present,
            helper_executable=helper_executable,
            blocked_reason=reason,
            helper_name=helper_name,
        )

    if not enabled:
        return VoiceSetup(None, record("voice_disabled"))
    if not decoder_available:
        return VoiceSetup(None, record("decoder_unavailable"))
    if probe.blocked_reason is not None:
        return VoiceSetup(None, record(probe.blocked_reason))
    if _is_linux() and not production_runner_available():
        # Production requires a genuinely isolated runner; readiness must not claim
        # ``ready`` when the host cannot provide a cgroup scope for the helper tree.
        return VoiceSetup(None, record("helper_runner_unavailable"))

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
    storage = service.repository.database.storage
    helper_timeout = float(
        getattr(config, "voice_helper_timeout_seconds", DEFAULT_HELPER_TIMEOUT_SECONDS)
        or DEFAULT_HELPER_TIMEOUT_SECONDS
    )
    if _is_linux():
        assert isinstance(model, ModelBinding)
        linux_transcriber: Transcriber = LinuxSilkTranscriber(
            capture=capture,
            storage=storage,
            helper=LinuxWhisperHelper(
                helper_path,
                language=language,
                model=model,
                timeout_seconds=helper_timeout,
                max_silk_bytes=decoder.max_silk_bytes,
                max_pcm_bytes=decoder.max_pcm_bytes,
            ),
        )
        return VoiceSetup(linux_transcriber, record(None, LINUX_TRANSCRIBER))
    apple_transcriber: Transcriber = AppleSilkTranscriber(
        capture=capture,
        decoder=decoder,
        storage=storage,
        helper=AppleSpeechHelper(
            helper_path,
            locale=language,
            timeout_seconds=helper_timeout,
        ),
    )
    return VoiceSetup(apple_transcriber, record(None, APPLE_TRANSCRIBER))


def _staging_root(config: Any) -> Path:
    return Path(config.data_dir) / STAGING_DIRECTORY_NAME

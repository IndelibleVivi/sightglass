"""Linux whisper.cpp transcription over an explicitly prepared local helper.

The recognizer is an operator-prepared ``whisper-cli`` from upstream ``whisper.cpp``
wrapped by ``scripts/build-linux-voice-helper.sh`` around a versioned host directory
holding the multilingual quantized model.  Like the macOS Apple helper, one helper runs
per job with a wall-clock watchdog, bounded streams, and a process-group kill.  Unlike
it, the Linux recognizer also has to isolate its memory (a whisper model is far larger
than the PCM it is given) and it consumes *WAV*, not the raw PCM the SILK decoder emits,
so a small bounded conversion step sits between decode and recognition.

Isolation and bounds (no daemon-wide limit is introduced here):

* the job is submitted through a **runner seam** (``HelperRunner``).  Production always
  requires a genuinely isolated runner: :class:`SystemdRunner` launches a named transient
  unit for the whole helper tree with a hard cgroup memory ceiling, kill-on-OOM, and
  ``--collect`` reclamation.  There is **no automatic production fallback** to a runner
  that only applies ``RLIMIT``; when no systemd scope is available the read fails closed
  with ``helper_runner_unavailable``.  :class:`DirectRunner` exists only for explicitly
  injected synthetic/test wiring and reports ``isolates = False``.
* the wrapper additionally applies ``RLIMIT_AS``/``RLIMIT_DATA`` to the whisper process.
  ``RLIMIT_AS`` is a *supplementary* guard; it is never reported as cgroup isolation.
* the helper is invoked with an explicit thread count (never whisper's hardware-default
  thread count), bounded stdout/stderr, a bounded output JSON file, and a deadline.

The runner reports a distinctly versioned backend and real helper/model identity in
every provenance record, so a whisper.cpp transcript can never be confused with an Apple
``SpeechAnalyzer`` transcript, and it never downloads a model: a missing model is a
content-free blocked readiness reason, never an implicit fetch.
"""

from __future__ import annotations

import json
import math
import os
import secrets
import shutil
import stat
import subprocess
import sys
import time
import wave
from collections.abc import Mapping
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.voice import VoiceTranscription
from sightglass.storage import StorageBudget

from .capture import CapturedVoice, VoiceCapture
from .decoder import (
    MAX_PCM_BYTES,
    MAX_SILK_BYTES,
    PCM_CHANNELS,
    PCM_SAMPLE_RATE,
    pcm_recipe,
)
from .subprocess import BoundedProcessError, ProcessOutcome, run_bounded

HELPER_NAME = "sightglass-whisper"
HELPER_SCHEMA = "sightglass.voice-transcript.v1"
BACKEND = "whisper.cpp"
MODELS_DIRECTORY_NAME = "models"
MODEL_MANIFEST_NAME = "model.json"
MODEL_MANIFEST_SCHEMA = "sightglass.voice-model.v1"
# The manifest names the exact model file and its quantization; there is no implicit
# default.  A missing/invalid manifest is a blocked readiness reason, never a guess.
MAX_MANIFEST_BYTES = 16 * 1024
DEFAULT_HELPER_TIMEOUT_SECONDS = 600.0
MAX_HELPER_STDOUT_BYTES = 2 * 1024 * 1024
MAX_HELPER_STDERR_BYTES = 64 * 1024
MAX_TRANSCRIPT_CHARS = 200_000
MAX_WAV_BYTES = 32 * 1024 * 1024
# One bounded conversion workspace, matching a small WAV plus residue.
_CONVERSION_WORKSPACE_BYTES = MAX_WAV_BYTES + 1024 * 1024
# Fixed, explicit thread count.  whisper's hardware-default thread count is
# nondeterministic across hosts and can oversubscribe a shared VPS; two threads keeps
# one job's CPU footprint predictable and is what the isolated unit is sized for.
HELPER_THREADS = 2
# Default cgroup memory ceiling for one helper tree, in bytes (2 GiB).  This is a real
# cgroup ``MemoryMax`` on the transient systemd unit, not an alias for ``RLIMIT_AS``.
DEFAULT_MAX_HELPER_RSS_BYTES = 2 * 1024 * 1024 * 1024
# Bound on each ``systemctl`` cleanup call; a stop/kill/reset that does not return within
# this window is abandoned rather than waited on without limit.
SCOPE_CLEANUP_TIMEOUT_SECONDS = 10.0
# Language mapping: the config carries a BCP-47 locale; whisper.cpp wants a short code.
_LANGUAGE_MAP = {
    "zh-cn": "zh",
    "zh-hans": "zh",
    "zh-sg": "zh",
    "zh-tw": "zh",
    "zh-hant": "zh",
    "zh-hk": "zh",
    "en-us": "en",
    "en-gb": "en",
}
# Exit codes of the Linux job child (``sightglass.voice._linux_child``); the helper maps
# them back to blocked/retryable errors. ``EXIT_MODEL_UNAVAILABLE`` is the child's
# model-unavailable code, ``EXIT_RECOGNIZE_FAILED`` its generic recognition failure.
EXIT_USAGE = 4
EXIT_DECODE_FAILED = 5
EXIT_LIMIT = 6
EXIT_RECOGNIZE_FAILED = 8
EXIT_MODEL_UNAVAILABLE = 9
# The report the Linux job child (``sightglass.voice._linux_child``) prints.
_JOB_REPORT_SCHEMA = "sightglass.voice-linux-job.v1"
_ENV_ALLOWLIST = ("HOME", "LANG", "LC_ALL", "PATH", "TMPDIR", "USER")
_ENV_DENY_MARKERS = ("TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "KEY")
# An unsafe manifest ``file`` value (traversal, separators, control bytes) is refused
# rather than escaped; a model file is a plain basename inside the helper's own models
# directory.
_UNSAFE_FILE_CHARS = ("/", "\\", "\x00")


def whisper_language(locale: str) -> str:
    """Map a configured BCP-47 locale to a whisper.cpp language code.

    ``auto`` and bare codes pass through; region-qualified locales collapse to their base
    language (``zh-CN`` → ``zh``, ``en-US`` → ``en``). The original configured locale is
    preserved separately in provenance.
    """

    value = (locale or "auto").strip()
    lowered = value.casefold()
    if lowered in _LANGUAGE_MAP:
        return _LANGUAGE_MAP[lowered]
    if "-" in value:
        return value.split("-", 1)[0].casefold()
    return value


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
class ModelBinding:
    """The exact model file and its declared quantization, resolved once at readiness.

    ``path`` is ``None`` when no valid manifest names a model; ``identifier`` and
    ``quantization`` are then empty/``None`` rather than fabricated defaults.
    """

    path: Path | None
    identifier: str
    quantization: str | None


@dataclass(frozen=True)
class HelperReadiness:
    present: bool
    executable: bool
    blocked_reason: str | None
    model: ModelBinding | None = None


def _read_private_manifest(path: Path) -> dict[str, Any] | None:
    """Read one bounded, no-follow, single-linked private manifest, or ``None``.

    The manifest lives in the helper's own namespace; it is read with the same private
    guarantees the rest of the pipeline uses (regular, single-linked, owner-only, bounded
    bytes).  A missing, oversized, symlinked, hardlinked, foreign-owned, non-private,
    non-UTF-8 or non-object manifest is ``None``.
    """

    try:
        metadata = path.lstat()
    except OSError:
        return None
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
        or metadata.st_size <= 0
        or metadata.st_size > MAX_MANIFEST_BYTES
    ):
        return None
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError:
        return None
    try:
        if os.fstat(descriptor).st_ino != metadata.st_ino:
            return None
        raw = os.read(descriptor, MAX_MANIFEST_BYTES + 1)
    except OSError:
        return None
    finally:
        os.close(descriptor)
    if len(raw) > MAX_MANIFEST_BYTES:
        return None
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def resolve_model_binding(helper_path: Path) -> ModelBinding:
    """Resolve the exact model the helper is bound to from its adjacent manifest.

    The build script writes ``models/model.json`` (``sightglass.voice-model.v1``) naming
    the exact ``file`` and its ``quantization``.  A missing, malformed, or unsafe manifest
    resolves to an *unbound* model (``path = None``) that readiness reports as
    ``model_unresolved`` -- the recognizer never fabricates a default file name or
    quantization.
    """

    none_binding = ModelBinding(None, "", None)
    manifest = helper_path.parent / MODELS_DIRECTORY_NAME / MODEL_MANIFEST_NAME
    parsed = _read_private_manifest(manifest)
    if parsed is None or parsed.get("schema") != MODEL_MANIFEST_SCHEMA:
        return none_binding
    file_value = parsed.get("file")
    if (
        not isinstance(file_value, str)
        or not file_value
        or file_value in {".", ".."}
        or any(marker in file_value for marker in _UNSAFE_FILE_CHARS)
    ):
        return none_binding
    identifier_value = parsed.get("identifier")
    if not isinstance(identifier_value, str) or not identifier_value:
        return none_binding
    quantization_value = parsed.get("quantization")
    quantization = (
        quantization_value if isinstance(quantization_value, str) and quantization_value else None
    )
    model_dir = helper_path.parent / MODELS_DIRECTORY_NAME
    return ModelBinding(model_dir / file_value, identifier_value, quantization)


def probe_helper(path: Path | None, *, model: ModelBinding | None = None) -> HelperReadiness:
    """Content-free readiness of the compiled helper and its bound model (no subprocess).

    Only a bounded readiness reason is returned, never a path.
    """

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
    binding = model if model is not None else resolve_model_binding(path)
    if binding.path is None:
        return HelperReadiness(True, True, "model_unresolved", binding)
    try:
        model_metadata = binding.path.lstat()
    except OSError:
        return HelperReadiness(True, True, "model_missing", binding)
    if (
        not stat.S_ISREG(model_metadata.st_mode)
        or model_metadata.st_nlink != 1
        or model_metadata.st_size <= 0
    ):
        return HelperReadiness(True, True, "model_invalid", binding)
    return HelperReadiness(True, True, None, binding)


def _blocked(reason: str) -> SightglassError:
    return SightglassError(
        ErrorCode.RESOURCE_BLOCKED, details={"reason": reason, "stage": "recognize"}
    )


def _timeout(reason: str) -> SightglassError:
    return SightglassError(
        ErrorCode.SERVICE_TIMEOUT, retryable=True, details={"reason": reason, "stage": "recognize"}
    )


class HelperRunner:
    """Submit one helper invocation inside a bounded, isolated process tree.

    ``available()`` reports whether the runner can actually isolate; ``isolates`` is
    ``True`` only for a runner that places the whole helper tree under a cgroup.
    """

    name: str
    isolates: bool

    def available(self) -> bool:
        raise NotImplementedError

    def run(
        self,
        argv: list[str],
        *,
        timeout_seconds: float,
        env: Mapping[str, str],
        pass_fds: tuple[int, ...] = (),
    ) -> ProcessOutcome:
        raise NotImplementedError


class DirectRunner(HelperRunner):
    """Run the helper directly with the wrapper's own RLIMITs; no cgroup isolation.

    This is **not** a production runner: it exists so synthetic/test wiring and
    non-systemd development hosts can exercise the bounded child path.  It never claims
    cgroup isolation (``isolates = False``) and is never selected automatically in
    production.
    """

    name = "direct"
    isolates = False

    def available(self) -> bool:
        return True

    def run(
        self,
        argv: list[str],
        *,
        timeout_seconds: float,
        env: Mapping[str, str],
        pass_fds: tuple[int, ...] = (),
    ) -> ProcessOutcome:
        return run_bounded(
            argv,
            timeout_seconds=timeout_seconds,
            max_stdout_bytes=MAX_HELPER_STDOUT_BYTES,
            max_stderr_bytes=MAX_HELPER_STDERR_BYTES,
            env=dict(env),
            pass_fds=pass_fds,
        )


class SystemdRunner(HelperRunner):
    """Run one helper under a named, transient, memory-limited systemd unit.

    ``systemd-run --scope --collect`` places the helper tree in its own cgroup with a
    hard ``MemoryMax`` ceiling, ``MemorySwapMax=0`` and ``OOMPolicy=kill`` so a controlled
    OOM kills the whole group rather than leaving a surviving child, and reclaims the
    transient unit afterwards.  ``KillMode=control-group`` and a bounded ``RuntimeMaxSec``
    (set to the watchdog deadline) ensure the whole scope can be stopped on timeout.  The
    exact production line is built here so tests cover it through this seam.
    """

    name = "systemd"
    isolates = True

    def __init__(
        self,
        *,
        max_rss_bytes: int,
        unit_prefix: str = "sightglass-whisper",
        systemd_run: str = "systemd-run",
        systemctl: str = "systemctl",
        cleanup_timeout_seconds: float = SCOPE_CLEANUP_TIMEOUT_SECONDS,
    ) -> None:
        self.max_rss_bytes = int(max_rss_bytes)
        self.unit_prefix = unit_prefix
        self.systemd_run = systemd_run
        self.systemctl = systemctl
        self.cleanup_timeout_seconds = float(cleanup_timeout_seconds)

    def available(self) -> bool:
        return (
            shutil.which(self.systemd_run) is not None and shutil.which(self.systemctl) is not None
        )

    def command(self, argv: list[str], *, unit_name: str, runtime_max_seconds: float) -> list[str]:
        """The exact ``systemd-run`` line for one helper job."""

        return [
            self.systemd_run,
            "--scope",
            "--collect",
            "--quiet",
            f"--unit={unit_name}",
            "-p",
            "MemoryAccounting=yes",
            "-p",
            f"MemoryMax={self.max_rss_bytes}",
            "-p",
            "MemorySwapMax=0",
            "-p",
            "OOMPolicy=kill",
            "-p",
            "KillMode=control-group",
            "-p",
            f"RuntimeMaxSec={max(1, math.ceil(runtime_max_seconds))}",
            *argv,
        ]

    @staticmethod
    def unit_name(prefix: str = "sightglass-whisper") -> str:
        """A collision-proof, opaque transient unit name.

        A PID plus a millisecond counter can collide (multiple jobs in the same
        millisecond on one host, or a PID reused across boots and a stale unit still
        collected).  A 128-bit random nonce makes two concurrent jobs unable to select
        the same unit, so a timeout cleanup can only ever stop its own scope.
        """

        return f"{prefix}-{secrets.token_hex(16)}.scope"

    def stop_command(self, unit_name: str) -> list[str]:
        """The exact ``systemctl`` line that stops only this owned transient unit."""

        return [self.systemctl, "stop", unit_name]

    def kill_command(self, unit_name: str) -> list[str]:
        return [self.systemctl, "kill", "--signal=SIGKILL", unit_name]

    def reset_command(self, unit_name: str) -> list[str]:
        # A killed scope leaves a failed unit journal entry; reset it so the opaque name
        # is fully released (``--collect`` already reclaims a cleanly exited unit).
        return [self.systemctl, "reset-failed", unit_name]

    def _cleanup_unit(self, unit_name: str) -> None:
        """Stop exactly this job's transient unit with a bounded, escalating wait.

        The outer watchdog (or a stream cap) may kill ``systemd-run`` before the scope it
        started has exited, so the runner explicitly stops its own unit: SIGTERM via
        ``systemctl stop``, then SIGKILL via ``systemctl kill`` if it is still active.
        Only the generated 128-bit nonce unit is named; no other unit is ever touched, and
        every wait is bounded by ``cleanup_timeout_seconds``.
        """

        for command in (self.stop_command(unit_name), self.kill_command(unit_name)):
            try:
                subprocess.run(
                    command,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=self.cleanup_timeout_seconds,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired):
                # systemd-run's process group is not the scope's entire cgroup.
                # Escalate, then require evidence that this owned scope has stopped.
                continue
        try:
            state = subprocess.run(
                [
                    self.systemctl,
                    "show",
                    "--property=ActiveState",
                    "--property=LoadState",
                    unit_name,
                ],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=self.cleanup_timeout_seconds,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise _blocked("helper_scope_cleanup_failed") from exc
        properties = dict(
            line.split("=", 1)
            for line in state.stdout.decode("utf-8", "replace").splitlines()
            if "=" in line
        )
        if state.returncode != 0 or properties.get("ActiveState") not in {"inactive", "failed"}:
            raise _blocked("helper_scope_cleanup_failed")
        try:
            subprocess.run(
                self.reset_command(unit_name),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=self.cleanup_timeout_seconds,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            pass

    def run(
        self,
        argv: list[str],
        *,
        timeout_seconds: float,
        env: Mapping[str, str],
        pass_fds: tuple[int, ...] = (),
    ) -> ProcessOutcome:
        unit_name = self.unit_name(self.unit_prefix)
        try:
            outcome = run_bounded(
                self.command(argv, unit_name=unit_name, runtime_max_seconds=timeout_seconds),
                timeout_seconds=timeout_seconds,
                max_stdout_bytes=MAX_HELPER_STDOUT_BYTES,
                max_stderr_bytes=MAX_HELPER_STDERR_BYTES,
                env=dict(env),
                pass_fds=pass_fds,
            )
        except BoundedProcessError as exc:
            # The scope may not have started; still try to reap this unit by name.
            self._cleanup_unit(unit_name)
            raise _blocked("helper_runner_unavailable") from exc
        if outcome.timed_out or outcome.limit_exceeded is not None or outcome.exit_code != 0:
            # The outer watchdog killed systemd-run (or a stream cap fired); the scope it
            # started may still be running, so stop only this owned unit before returning.
            self._cleanup_unit(unit_name)
        return outcome


def production_runner(helper: LinuxWhisperHelper) -> HelperRunner:
    """The production runner: a genuinely isolated systemd scope, or fail closed.

    Production never silently degrades to a non-isolated runner.  When no systemd scope
    is available the returned runner's ``available()`` is ``False`` so the job is blocked
    with ``helper_runner_unavailable`` rather than run without a memory guarantee.
    """

    if helper.max_rss_bytes is None:
        raise SightglassError(
            ErrorCode.RESOURCE_BLOCKED,
            details={"reason": "helper_memory_unbounded", "stage": "recognize"},
        )
    return SystemdRunner(max_rss_bytes=helper.max_rss_bytes)


@dataclass(frozen=True)
class HelperTranscript:
    text: str
    language: str
    configured_locale: str
    backend: str
    helper_version: str | None
    whisper_source_tag: str | None
    model: dict[str, Any]
    quantization: str | None
    segments: int
    isolation: str
    decoder: str
    decoder_envelope: str
    pcm_bytes: int
    pcm_frames: int
    decoded_duration_ms: int

    def provenance(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "helper": HELPER_NAME,
            "helper_version": self.helper_version,
            "whisper_source_tag": self.whisper_source_tag,
            "locale": self.configured_locale,
            "language": self.language,
            "model": self.model,
            "quantization": self.quantization,
            "segments": self.segments,
            "isolation": self.isolation,
            "threads": HELPER_THREADS,
            "decoder": self.decoder,
            "decoder_envelope": self.decoder_envelope,
            "volatile_excluded": True,
        }


def write_wav(pcm_path: Path, wav_path: Path, *, max_bytes: int) -> int:
    """Wrap the decoded raw s16le mono PCM into a WAV container in bounded chunks.

    The PCM bytes (and therefore the input digest) are unchanged; only a 44-byte RIFF
    header is prepended.  Copying is streamed in fixed chunks with a running byte cap so
    a hostile decode cannot expand here and the wrapper never holds a whole stream.
    """

    total = 0
    with (
        pcm_path.open("rb") as reader,
        wave.open(str(wav_path), "wb") as writer,
    ):
        writer.setnchannels(PCM_CHANNELS)
        writer.setsampwidth(2)
        writer.setframerate(PCM_SAMPLE_RATE)
        while chunk := reader.read(1 << 20):
            total += len(chunk)
            if total > max_bytes:
                raise _blocked("wav_too_large")
            writer.writeframes(chunk)
    return total


class LinuxWhisperHelper:
    """Run one whole Linux ASR job inside a single isolated systemd scope.

    The job child (``python -m sightglass.voice._linux_child``) performs SILK decode,
    PCM→WAV wrap and whisper.cpp recognition inside one process tree, so the systemd
    scope's cgroup memory ceiling covers the decoder, the PCM/WAV buffers and the model.
    The helper only prepares the bounded command and interprets the child's report.
    """

    def __init__(
        self,
        helper_path: Path,
        *,
        language: str,
        model: ModelBinding | None = None,
        timeout_seconds: float = DEFAULT_HELPER_TIMEOUT_SECONDS,
        max_rss_bytes: int | None = DEFAULT_MAX_HELPER_RSS_BYTES,
        max_silk_bytes: int = MAX_SILK_BYTES,
        max_pcm_bytes: int = MAX_PCM_BYTES,
        python_executable: str | None = None,
        runner: HelperRunner | None = None,
    ) -> None:
        self.helper_path = Path(helper_path)
        self.language = language
        self.model = model if model is not None else resolve_model_binding(self.helper_path)
        self.timeout_seconds = float(timeout_seconds)
        self.max_rss_bytes = max_rss_bytes
        self.max_silk_bytes = int(max_silk_bytes)
        self.max_pcm_bytes = int(max_pcm_bytes)
        self.python_executable = python_executable or sys.executable
        # An injected runner (synthetic/test) is honoured; otherwise the production
        # isolated runner is required and no non-isolated fallback is ever chosen.
        self.runner = runner if runner is not None else production_runner(self)

    def _budgeted_timeout(self, deadline: float | None) -> float:
        timeout = self.timeout_seconds
        if deadline is None:
            return timeout
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise _timeout("operation_deadline")
        return max(0.1, min(timeout, remaining))

    def _command(self, silk_fd: int, *, timeout_seconds: float | None = None) -> list[str]:
        command = [
            self.python_executable,
            "-m",
            "sightglass.voice._linux_child",
            "--silk-fd",
            str(silk_fd),
            "--sample-rate",
            str(PCM_SAMPLE_RATE),
            "--max-input-bytes",
            str(self.max_silk_bytes),
            "--max-pcm-bytes",
            str(self.max_pcm_bytes),
            "--max-wav-bytes",
            str(MAX_WAV_BYTES),
            "--helper",
            str(self.helper_path),
            "--language",
            whisper_language(self.language),
            "--threads",
            str(HELPER_THREADS),
            "--helper-timeout-seconds",
            str(self._wrapper_timeout(timeout_seconds)),
        ]
        if self.model.path is not None:
            command.extend(["--model", str(self.model.path)])
        if self.max_rss_bytes is not None:
            command.extend(["--max-rss-bytes", str(int(self.max_rss_bytes))])
        return command

    def _wrapper_timeout(self, timeout_seconds: float | None = None) -> float:
        """The inner wrapper's own watchdog, strictly inside the outer job watchdog.

        The wrapper must give up (and reap its group) *before* the outer runner/child
        watchdog kills the whole job child, otherwise a wrapper that forked a descendant
        would be SIGKILLed before it could clean up.
        """

        timeout = self.timeout_seconds if timeout_seconds is None else timeout_seconds
        return max(0.05, timeout - min(1.0, timeout * 0.2))

    @staticmethod
    def _open_private_silk(silk_path: Path) -> int:
        """Open one capture-staged SILK file with the private-input guarantees.

        Capture already verifies the resource binding, but this helper may also be called
        directly, so it re-asserts the same no-follow/regular/private single-link boundary
        before handing a descriptor to the child: a symlink, a hardlinked file, a
        non-regular file, a foreign owner or a non-private mode is refused.
        """

        try:
            descriptor = os.open(
                silk_path,
                os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0),
            )
        except OSError as exc:
            raise _blocked("silk_open_failed") from exc
        try:
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
                or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) & 0o077
                or metadata.st_size <= 0
            ):
                raise _blocked("silk_unsafe")
        except BaseException:
            os.close(descriptor)
            raise
        return descriptor

    def transcribe_silk(
        self, silk_path: Path, *, deadline: float | None = None
    ) -> HelperTranscript:
        readiness = probe_helper(self.helper_path, model=self.model)
        if readiness.blocked_reason is not None:
            raise _blocked(readiness.blocked_reason)
        if not self.runner.available():
            raise _blocked("helper_runner_unavailable")
        timeout = self._budgeted_timeout(deadline)
        descriptor = self._open_private_silk(silk_path)
        try:
            try:
                outcome = self.runner.run(
                    self._command(descriptor, timeout_seconds=timeout),
                    timeout_seconds=timeout,
                    env=helper_environment(),
                    pass_fds=(descriptor,),
                )
            except BoundedProcessError as exc:
                raise _blocked("helper_spawn_failed") from exc
        finally:
            os.close(descriptor)
        if outcome.timed_out:
            raise _timeout("helper_timeout")
        if outcome.limit_exceeded is not None:
            raise _blocked(f"helper_{outcome.limit_exceeded}_limit")
        if outcome.exit_code == EXIT_MODEL_UNAVAILABLE:
            raise _blocked(_helper_error(outcome.stderr) or "model_unavailable")
        if outcome.exit_code == EXIT_USAGE:
            raise _blocked(_helper_error(outcome.stderr) or "helper_usage")
        if outcome.exit_code == EXIT_DECODE_FAILED:
            raise _blocked(_helper_error(outcome.stderr) or "decode_failed")
        if outcome.exit_code == EXIT_LIMIT:
            raise _blocked(_helper_error(outcome.stderr) or "decode_limit")
        if (
            outcome.exit_code == EXIT_RECOGNIZE_FAILED
            and _helper_error(outcome.stderr) == "helper_timeout"
        ):
            raise _timeout("helper_timeout")
        if outcome.exit_code != 0:
            raise SightglassError(
                ErrorCode.SERVICE_UNAVAILABLE,
                retryable=True,
                details={
                    "reason": _helper_error(outcome.stderr) or "helper_failed",
                    "stage": "recognize",
                },
            )
        return _parse_job_report(
            outcome.stdout,
            configured_locale=self.language,
            fallback_model={"identifier": self.model.identifier},
            fallback_isolation=self.runner.name if self.runner.isolates else "none",
            fallback_quantization=self.model.quantization,
        )


def _helper_error(stderr: bytes) -> str | None:
    try:
        parsed = json.loads(stderr.decode("utf-8", "replace").strip().splitlines()[-1])
    except (IndexError, ValueError):
        return None
    if isinstance(parsed, dict) and isinstance(parsed.get("error"), str):
        return str(parsed["error"])
    return None


def _parse_job_report(
    stdout: bytes,
    *,
    configured_locale: str,
    fallback_model: dict[str, Any],
    fallback_isolation: str,
    fallback_quantization: str | None,
) -> HelperTranscript:
    try:
        parsed = json.loads(stdout.decode("utf-8", "replace").strip())
    except ValueError as exc:
        raise _blocked("helper_report_invalid") from exc
    if not isinstance(parsed, dict) or parsed.get("schema") != _JOB_REPORT_SCHEMA:
        raise _blocked("helper_report_invalid")
    decode = parsed.get("decode")
    transcript = parsed.get("transcript")
    if not isinstance(decode, dict) or not isinstance(transcript, dict):
        raise _blocked("helper_report_invalid")
    text = transcript.get("text")
    if not isinstance(text, str):
        raise _blocked("helper_report_invalid")
    if len(text) > MAX_TRANSCRIPT_CHARS:
        raise _blocked("helper_text_too_long")
    segments = transcript.get("segments")
    model = transcript.get("model")
    version = transcript.get("helper_version")
    source_tag = transcript.get("whisper_source_tag")
    quantization = transcript.get("quantization")
    isolation = transcript.get("isolation")
    pcm_bytes = decode.get("pcm_bytes")
    frames = decode.get("frames")
    duration_ms = decode.get("duration_ms")
    if type(pcm_bytes) is not int or type(frames) is not int or type(duration_ms) is not int:
        raise _blocked("helper_report_invalid")
    assert isinstance(pcm_bytes, int) and isinstance(frames, int) and isinstance(duration_ms, int)
    # ``configured_locale`` is the daemon's configured locale (the original), never the
    # wrapper's mapped short code; ``language`` is the actual mapped code.
    language = str(transcript.get("language") or whisper_language(configured_locale))
    return HelperTranscript(
        text=text,
        language=language,
        configured_locale=configured_locale,
        backend=str(transcript.get("backend") or BACKEND),
        helper_version=str(version) if isinstance(version, str) and version else None,
        whisper_source_tag=source_tag if isinstance(source_tag, str) and source_tag else None,
        model=dict(model) if isinstance(model, dict) else fallback_model,
        quantization=quantization if isinstance(quantization, str) else fallback_quantization,
        segments=len(segments) if isinstance(segments, list) else 0,
        isolation=str(isolation) if isinstance(isolation, str) else fallback_isolation,
        decoder=str(decode.get("decoder") or "unknown"),
        decoder_envelope=str(decode.get("envelope") or "unknown"),
        pcm_bytes=pcm_bytes,
        pcm_frames=frames,
        decoded_duration_ms=duration_ms,
    )


class LinuxSilkTranscriber:
    """Capture → one isolated job child (decode + WAV wrap + whisper.cpp) → provenance.

    The CPU-heavy half runs as a single ``_linux_child`` process inside the helper's
    systemd scope, so the cgroup memory ceiling covers the decoder, the PCM/WAV buffers
    and the whisper model together.  Capture (the canonical, resource-service-verified
    SILK read) stays outside and before that child.
    """

    RECIPE_ENGINE = "sightglass.voice.linux-whisper.v1"

    def __init__(
        self,
        *,
        capture: VoiceCapture,
        helper: LinuxWhisperHelper,
        recipe_engine: str = RECIPE_ENGINE,
        storage: StorageBudget | None = None,
    ) -> None:
        self.capture = capture
        self.helper = helper
        self.recipe_engine = recipe_engine
        self.storage = storage

    def transcribe(
        self, job: dict[str, Any], *, duration_ms: int, deadline: float | None
    ) -> VoiceTranscription:
        budget = self.storage
        reservation = (
            budget.reserve(self.capture.max_bytes + self.helper.max_pcm_bytes, background=True)
            if budget is not None
            else nullcontext()
        )
        with reservation:
            return self._transcribe(job, duration_ms=duration_ms, deadline=deadline)

    def _transcribe(
        self, job: dict[str, Any], *, duration_ms: int, deadline: float | None
    ) -> VoiceTranscription:
        captured: CapturedVoice | None = None
        try:
            captured = self.capture.capture(job)
            transcript = self.helper.transcribe_silk(captured.silk_path, deadline=deadline)
            return VoiceTranscription(
                text=transcript.text,
                provenance=self._provenance(
                    captured=captured,
                    transcript=transcript,
                    declared_duration_ms=duration_ms,
                ),
            )
        finally:
            if captured is not None:
                captured.release()
            if self.storage is not None and captured is not None:
                self.storage.track(captured.silk_path)

    def _provenance(
        self,
        *,
        captured: CapturedVoice,
        transcript: HelperTranscript,
        declared_duration_ms: int,
    ) -> dict[str, Any]:
        return {
            "schema": "sightglass.voice-provenance.v1",
            "recipe": {
                "engine": self.recipe_engine,
                "decoder": transcript.decoder,
                "decoder_envelope": transcript.decoder_envelope,
                "pcm": pcm_recipe(),
                "input_container": "wav",
            },
            "input": {
                "resource_revision": captured.resource_revision,
                "input_digest": captured.input_digest,
                "silk_bytes": captured.byte_size,
                "pcm_bytes": transcript.pcm_bytes,
                "pcm_frames": transcript.pcm_frames,
                "decoded_duration_ms": transcript.decoded_duration_ms,
                "declared_duration_ms": declared_duration_ms,
            },
            "recognizer": transcript.provenance(),
            "derived": {"kind": "derived_transcript", "translation": False},
        }

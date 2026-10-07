"""Bounded SILK decoding for the local voice pipeline.

The decoder never touches the source itself: capture hands it one private staging file
and it returns one private PCM file.  Decoding always happens in a separate child
process (``python -m sightglass.voice._decode_child``) so a malformed envelope or an
over-long stream can only exhaust that child.
"""

from __future__ import annotations

import hashlib
import json
import sys
import time
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from importlib.util import find_spec
from pathlib import Path
from typing import Any, BinaryIO

from sightglass.contracts.errors import ErrorCode, SightglassError

from .subprocess import BoundedProcessError, run_bounded

PCM_SAMPLE_RATE = 16_000
PCM_CHANNELS = 1
PCM_SAMPLE_FORMAT = "s16le"
MAX_SILK_BYTES = 4 * 1024 * 1024
MAX_PCM_SECONDS = 300
MAX_PCM_BYTES = MAX_PCM_SECONDS * PCM_SAMPLE_RATE * 2
DEFAULT_DECODE_TIMEOUT_SECONDS = 60.0
MAX_CHILD_STDOUT_BYTES = 64 * 1024
MAX_CHILD_STDERR_BYTES = 64 * 1024
PCM_RECIPE_VERSION = 1
SIGXFSZ = 25


def pcm_recipe() -> dict[str, Any]:
    """The PCM contract every recognizer recipe and provenance record pins."""

    return {
        "sample_rate": PCM_SAMPLE_RATE,
        "channels": PCM_CHANNELS,
        "sample_format": PCM_SAMPLE_FORMAT,
        "version": PCM_RECIPE_VERSION,
    }


@dataclass(frozen=True)
class DecodedAudio:
    path: Path
    byte_size: int
    frames: int
    duration_ms: int
    decoder: str
    envelope: str
    recipe: dict[str, Any]


def _blocked(reason: str, *, stage: str = "decode") -> SightglassError:
    return SightglassError(
        ErrorCode.RESOURCE_BLOCKED, details={"reason": reason, "stage": stage}
    )


def _timeout(reason: str, *, stage: str = "decode") -> SightglassError:
    return SightglassError(
        ErrorCode.SERVICE_TIMEOUT, retryable=True, details={"reason": reason, "stage": stage}
    )


class SilkDecoder:
    """Decode one private SILK file into one private PCM file inside a bounded child."""

    def __init__(
        self,
        *,
        python_executable: str | None = None,
        timeout_seconds: float = DEFAULT_DECODE_TIMEOUT_SECONDS,
        max_silk_bytes: int = MAX_SILK_BYTES,
        max_pcm_seconds: int = MAX_PCM_SECONDS,
    ) -> None:
        self.python_executable = python_executable or sys.executable
        self.timeout_seconds = float(timeout_seconds)
        self.max_silk_bytes = int(max_silk_bytes)
        self.max_pcm_seconds = int(max_pcm_seconds)
        self.max_pcm_bytes = min(
            MAX_PCM_BYTES, self.max_pcm_seconds * PCM_SAMPLE_RATE * 2
        )

    @staticmethod
    def available() -> bool:
        try:
            return find_spec("pysilk") is not None
        except (ImportError, ValueError):
            return False

    @staticmethod
    def version() -> str | None:
        try:
            return version("silk-python")
        except PackageNotFoundError:
            return None

    @staticmethod
    def digest(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def _budgeted_timeout(self, deadline: float | None) -> float:
        timeout = self.timeout_seconds
        if deadline is None:
            return timeout
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise _timeout("operation_deadline")
        return max(0.1, min(timeout, remaining))

    def decode(
        self, silk_path: Path, pcm_path: Path, *, deadline: float | None = None
    ) -> DecodedAudio:
        if not self.available():
            raise _blocked("decoder_unavailable")
        timeout = self._budgeted_timeout(deadline)
        silk_handle: BinaryIO | None = None
        pcm_handle: BinaryIO | None = None
        try:
            silk_handle = silk_path.open("rb")
            pcm_handle = pcm_path.open("wb")
            silk_fd = silk_handle.fileno()
            pcm_fd = pcm_handle.fileno()
            outcome = run_bounded(
                (
                    self.python_executable,
                    "-m",
                    "sightglass.voice._decode_child",
                    "--silk-fd",
                    str(silk_fd),
                    "--pcm-fd",
                    str(pcm_fd),
                    "--sample-rate",
                    str(PCM_SAMPLE_RATE),
                    "--max-input-bytes",
                    str(self.max_silk_bytes),
                    "--max-pcm-bytes",
                    str(self.max_pcm_bytes),
                ),
                timeout_seconds=timeout,
                max_stdout_bytes=MAX_CHILD_STDOUT_BYTES,
                max_stderr_bytes=MAX_CHILD_STDERR_BYTES,
                pass_fds=(silk_fd, pcm_fd),
            )
        except BoundedProcessError as exc:
            raise _blocked("decoder_spawn_failed") from exc
        finally:
            for handle in (silk_handle, pcm_handle):
                if handle is None:
                    continue
                try:
                    handle.close()
                except OSError:
                    pass
        if outcome.timed_out:
            raise _timeout("decode_timeout")
        if outcome.limit_exceeded is not None:
            raise _blocked(f"decode_{outcome.limit_exceeded}_limit")
        if outcome.signal_number == SIGXFSZ:
            raise _blocked("pcm_too_large")
        if not outcome.ok:
            raise _blocked(_error_reason(outcome.stderr))
        payload = _parse_report(outcome.stdout)
        if int(payload["pcm_bytes"]) > self.max_pcm_bytes:
            raise _blocked("pcm_too_large")
        return DecodedAudio(
            path=pcm_path,
            byte_size=int(payload["pcm_bytes"]),
            frames=int(payload["frames"]),
            duration_ms=int(payload["duration_ms"]),
            decoder=str(payload["decoder"]),
            envelope=str(payload["envelope"]),
            recipe=pcm_recipe(),
        )


def _error_reason(stderr: bytes) -> str:
    try:
        parsed = json.loads(stderr.decode("utf-8", "replace").strip().splitlines()[-1])
    except (IndexError, ValueError):
        return "decode_failed"
    if isinstance(parsed, dict) and isinstance(parsed.get("error"), str):
        return str(parsed["error"])
    return "decode_failed"


def _parse_report(stdout: bytes) -> dict[str, Any]:
    try:
        parsed = json.loads(stdout.decode("utf-8", "replace").strip())
    except ValueError as exc:
        raise _blocked("decode_report_invalid") from exc
    if not isinstance(parsed, dict) or parsed.get("schema") != "sightglass.voice-decode.v1":
        raise _blocked("decode_report_invalid")
    for field in ("pcm_bytes", "frames", "duration_ms"):
        if type(parsed.get(field)) is not int:
            raise _blocked("decode_report_invalid")
    return parsed

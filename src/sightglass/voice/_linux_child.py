"""One bounded Linux job child: SILK decode + PCM→WAV wrap + whisper.cpp recognition.

The whole Linux ASR job runs in *one* process tree so it can be placed inside a single
systemd scope with a real cgroup memory ceiling (`LinuxWhisperHelper`/`SystemdRunner`).
This child inherits exactly one SILK descriptor and one job-local staging directory, and:

1. decodes SILK → raw 16 kHz mono ``s16le`` PCM with the same bounded helpers the
   standalone ``_decode_child`` uses (identical envelope handling, size caps and
   ``RLIMIT_FSIZE`` guard);
2. wraps that PCM into a WAV container without re-encoding it (only the 44-byte RIFF
   header is added), so the recorded input digest still binds the exact source bytes;
3. invokes the operator-prepared whisper helper (which applies its own supplementary
   ``RLIMIT_AS``/``RLIMIT_DATA``), reading the exact JSON whisper.cpp produced.

It emits one JSON report on stdout: the decode facts (so provenance records the real
decoder/envelope/sizes), the transcript, and the recognizer provenance fields.  Any
failure is a non-zero exit plus one JSON line on stderr, matching ``_decode_child``.

Exit codes: 4 usage, 5 decode failed, 6 size limit, 7 decoder extra not installed,
8 recognition failed, 9 model unavailable, 10 helper spawn failed.
"""

from __future__ import annotations

import argparse
import errno
import json
import os
import resource
import sys
import tempfile
import wave
from typing import Any

from . import _decode_child
from .subprocess import BoundedProcessError, run_bounded

EXIT_USAGE = 4
EXIT_DECODE_FAILED = 5
EXIT_LIMIT = 6
EXIT_DECODER_MISSING = 7
EXIT_RECOGNIZE_FAILED = 8
EXIT_MODEL_UNAVAILABLE = 9
EXIT_HELPER_SPAWN_FAILED = 10
PCM_SAMPLE_BYTES = 2
MAX_WRAPPER_STDOUT_BYTES = 2 * 1024 * 1024
MAX_WRAPPER_STDERR_BYTES = 64 * 1024
_REPORT_SCHEMA = "sightglass.voice-linux-job.v1"


def _emit(stream: Any, payload: dict[str, Any]) -> None:
    stream.write(json.dumps(payload, sort_keys=True) + "\n")
    stream.flush()


def _fail(reason: str, *, exit_code: int) -> None:
    _emit(sys.stderr, {"error": reason, "exit_code": exit_code})
    raise SystemExit(exit_code)


def _read_silk(descriptor: int, *, limit: int) -> bytes:
    return _decode_child._read_all(descriptor, limit=limit)


def _decode_to_pcm(
    data: bytes, *, sample_rate: int, max_pcm_bytes: int, directory: str,
) -> tuple[str, int, int, str]:
    """Decode inside this child; return (pcm_path, pcm_bytes, frames, envelope)."""

    payload, envelope = _decode_child.normalize_envelope(data)
    soft, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
    del soft
    capped = (
        max_pcm_bytes if hard == resource.RLIM_INFINITY else min(max_pcm_bytes, hard)
    )
    resource.setrlimit(resource.RLIMIT_FSIZE, (capped, hard))
    pcm_path = os.path.join(directory, "decoded.pcm")
    descriptor = os.open(
        pcm_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600
    )
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            pcm_bytes = _decode_child._decode(handle, payload, sample_rate)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        reason = "pcm_too_large" if exc.errno == errno.EFBIG else f"pcm_write_failed:{exc.errno}"
        _fail(reason, exit_code=EXIT_LIMIT)
    if pcm_bytes > max_pcm_bytes:
        _fail("pcm_too_large", exit_code=EXIT_LIMIT)
    if pcm_bytes == 0:
        _fail("empty_decode", exit_code=EXIT_DECODE_FAILED)
    frames = pcm_bytes // PCM_SAMPLE_BYTES
    return pcm_path, pcm_bytes, frames, envelope


def _wrap_wav(pcm_path: str, wav_path: str, *, sample_rate: int, max_bytes: int) -> None:
    total = 0
    with open(pcm_path, "rb") as reader, wave.open(wav_path, "wb") as writer:
        writer.setnchannels(_decode_child.PCM_CHANNELS)
        writer.setsampwidth(PCM_SAMPLE_BYTES)
        writer.setframerate(sample_rate)
        while chunk := reader.read(1 << 20):
            total += len(chunk)
            if total > max_bytes:
                _fail("wav_too_large", exit_code=EXIT_LIMIT)
            writer.writeframes(chunk)


def _run_whisper(arguments: argparse.Namespace, wav_path: str, directory: str) -> dict[str, Any]:
    """Invoke the whisper wrapper with bounded streams and a reaped process group.

    The wrapper runs with the same bounded-child helper the daemon uses elsewhere, so a
    chatty or stuck wrapper cannot grow this child's output without limit, the whole
    wrapper process group is killed on timeout or a stream cap, and the cgroup the job
    child already lives in still covers this grandchild.
    """

    command = [
        arguments.helper,
        "--pcm",
        wav_path,
        "--language",
        arguments.language,
        "--threads",
        str(arguments.threads),
    ]
    if arguments.model:
        command.extend(["--model", arguments.model])
    if arguments.max_rss_bytes is not None:
        command.extend(["--max-rss-bytes", str(arguments.max_rss_bytes)])
    try:
        outcome = run_bounded(
            command,
            timeout_seconds=arguments.helper_timeout_seconds,
            max_stdout_bytes=MAX_WRAPPER_STDOUT_BYTES,
            max_stderr_bytes=MAX_WRAPPER_STDERR_BYTES,
            # Even a cleanly-exiting wrapper must not leave a forked grandchild behind.
            reap_orphans=True,
        )
    except BoundedProcessError as exc:
        _fail(f"helper_spawn_failed:{type(exc).__name__}", exit_code=EXIT_HELPER_SPAWN_FAILED)
    if outcome.timed_out:
        _fail("helper_timeout", exit_code=EXIT_RECOGNIZE_FAILED)
    if outcome.limit_exceeded is not None:
        _fail(f"helper_{outcome.limit_exceeded}_limit", exit_code=EXIT_RECOGNIZE_FAILED)
    if outcome.exit_code == 2:
        _fail("model_unavailable", exit_code=EXIT_MODEL_UNAVAILABLE)
    if outcome.exit_code != 0:
        reason = _last_error(outcome.stderr) or "helper_failed"
        _fail(reason, exit_code=EXIT_RECOGNIZE_FAILED)
    try:
        parsed = json.loads(outcome.stdout.decode("utf-8", "replace").strip())
    except ValueError:
        _fail("helper_report_invalid", exit_code=EXIT_RECOGNIZE_FAILED)
    if not isinstance(parsed, dict):
        _fail("helper_report_invalid", exit_code=EXIT_RECOGNIZE_FAILED)
    return parsed


def _last_error(stderr: bytes) -> str | None:
    try:
        parsed = json.loads(stderr.decode("utf-8", "replace").strip().splitlines()[-1])
    except (IndexError, ValueError):
        return None
    if isinstance(parsed, dict) and isinstance(parsed.get("error"), str):
        return str(parsed["error"])
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sightglass-voice-linux-job")
    parser.add_argument("--silk-fd", type=int, required=True)
    parser.add_argument("--sample-rate", type=int, default=16_000)
    parser.add_argument("--max-input-bytes", type=int, required=True)
    parser.add_argument("--max-pcm-bytes", type=int, required=True)
    parser.add_argument("--max-wav-bytes", type=int, required=True)
    parser.add_argument("--helper", required=True)
    parser.add_argument("--language", required=True)
    parser.add_argument("--model", default="")
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--max-rss-bytes", type=int, default=None)
    parser.add_argument("--helper-timeout-seconds", type=float, default=600.0)
    arguments = parser.parse_args(argv)
    if (
        arguments.sample_rate <= 0
        or arguments.max_pcm_bytes <= 0
        or arguments.max_wav_bytes <= 0
        or arguments.threads < 1
    ):
        _fail("invalid_limits", exit_code=EXIT_USAGE)

    data = _read_silk(arguments.silk_fd, limit=arguments.max_input_bytes)
    with tempfile.TemporaryDirectory(prefix="sightglass-linux-job-") as directory:
        os.chmod(directory, 0o700)
        pcm_path, pcm_bytes, frames, envelope = _decode_to_pcm(
            data,
            sample_rate=arguments.sample_rate,
            max_pcm_bytes=arguments.max_pcm_bytes,
            directory=directory,
        )
        wav_path = os.path.join(directory, "input.wav")
        _wrap_wav(
            pcm_path, wav_path, sample_rate=arguments.sample_rate, max_bytes=arguments.max_wav_bytes
        )
        os.unlink(pcm_path)
        transcript = _run_whisper(arguments, wav_path, directory)

    _emit(
        sys.stdout,
        {
            "schema": _REPORT_SCHEMA,
            "decode": {
                "decoder": _decode_child._decoder_label(),
                "silk_bytes": len(data),
                "envelope": envelope,
                "pcm_bytes": pcm_bytes,
                "frames": frames,
                "duration_ms": frames * 1000 // arguments.sample_rate,
                "sample_rate": arguments.sample_rate,
                "channels": _decode_child.PCM_CHANNELS,
                "sample_format": _decode_child.PCM_SAMPLE_FORMAT,
            },
            "transcript": transcript,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

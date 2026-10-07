"""Bounded SILK → PCM decode child.

Runs as ``python -m sightglass.voice._decode_child`` in its own process so a malformed
or hostile envelope can only exhaust this child, never the daemon.  Input and output are
passed as inherited descriptors (never as argv data), the decoded size is capped with
``RLIMIT_FSIZE`` plus an explicit size check, and success is reported as one JSON line on
stdout.  Everything else is a non-zero exit plus one JSON line on stderr.

Exit codes: 4 usage, 5 unsupported/corrupt/empty decode, 6 size limit, 7 decoder extra
not installed.
"""

from __future__ import annotations

import argparse
import errno
import io
import json
import os
import resource
import sys
from importlib.metadata import PackageNotFoundError, version
from typing import Any, BinaryIO

SILK_V3_MAGIC = b"#!SILK_V3"
WECHAT_ENVELOPE_PREFIX = b"\x02"
EXIT_USAGE = 4
EXIT_DECODE_FAILED = 5
EXIT_LIMIT = 6
EXIT_DECODER_MISSING = 7
PCM_SAMPLE_FORMAT = "s16le"
PCM_CHANNELS = 1
PCM_SAMPLE_BYTES = 2


def _emit(stream: Any, payload: dict[str, Any]) -> None:
    stream.write(json.dumps(payload, sort_keys=True) + "\n")
    stream.flush()


def _fail(reason: str, *, exit_code: int) -> None:
    _emit(sys.stderr, {"error": reason, "exit_code": exit_code})
    raise SystemExit(exit_code)


def _read_all(descriptor: int, *, limit: int) -> bytes:
    chunks: list[bytes] = []
    remaining = limit + 1
    while remaining > 0:
        chunk = os.read(descriptor, min(65_536, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    data = b"".join(chunks)
    if len(data) > limit:
        _fail("input_too_large", exit_code=EXIT_LIMIT)
    return data


def normalize_envelope(data: bytes) -> tuple[bytes, str]:
    """Accept the two WeChat SILK envelopes and reject everything else."""

    envelope = "plain"
    if data.startswith(WECHAT_ENVELOPE_PREFIX):
        data = data[len(WECHAT_ENVELOPE_PREFIX) :]
        envelope = "wechat_prefix"
    if not data.startswith(SILK_V3_MAGIC):
        _fail("unsupported_silk_envelope", exit_code=EXIT_DECODE_FAILED)
    if len(data) <= len(SILK_V3_MAGIC):
        _fail("empty_silk_payload", exit_code=EXIT_DECODE_FAILED)
    return data, envelope


def _decoder_module() -> Any:
    try:
        import pysilk
    except ImportError:
        _fail("decoder_unavailable", exit_code=EXIT_DECODER_MISSING)
    return pysilk


def _decoder_label() -> str:
    try:
        return f"pysilk/{version('silk-python')}"
    except PackageNotFoundError:
        return "pysilk/unknown"


def _decode(pcm_handle: BinaryIO, data: bytes, sample_rate: int) -> int:
    pysilk = _decoder_module()
    try:
        decoded = pysilk.decode(io.BytesIO(data), pcm_handle, sample_rate)
    except OSError:
        # A write that hits RLIMIT_FSIZE surfaces as EFBIG; it is a size failure, not
        # a decode failure, so it must reach the size handler below unrelabelled.
        raise
    except Exception as exc:  # decoder backends raise their own exception types
        _fail(f"decode_failed:{type(exc).__name__}", exit_code=EXIT_DECODE_FAILED)
    if isinstance(decoded, (bytes, bytearray)):
        return len(decoded)
    return int(pcm_handle.tell())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sightglass-voice-decode")
    parser.add_argument("--silk-fd", type=int, required=True)
    parser.add_argument("--pcm-fd", type=int, required=True)
    parser.add_argument("--sample-rate", type=int, default=16_000)
    parser.add_argument("--max-input-bytes", type=int, required=True)
    parser.add_argument("--max-pcm-bytes", type=int, required=True)
    arguments = parser.parse_args(argv)
    if arguments.sample_rate <= 0 or arguments.max_pcm_bytes <= 0:
        _fail("invalid_limits", exit_code=EXIT_USAGE)

    data, envelope = normalize_envelope(
        _read_all(arguments.silk_fd, limit=arguments.max_input_bytes)
    )

    # The decoded stream is bounded in the child as well as by the parent: RLIMIT_FSIZE
    # caps what this child can write, and the size check below turns a silent truncation
    # into a hard failure.
    soft, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
    del soft
    capped = (
        arguments.max_pcm_bytes
        if hard == resource.RLIM_INFINITY
        else min(arguments.max_pcm_bytes, hard)
    )
    resource.setrlimit(resource.RLIMIT_FSIZE, (capped, hard))
    try:
        with os.fdopen(os.dup(arguments.pcm_fd), "wb") as pcm_handle:
            pcm_bytes = _decode(pcm_handle, data, arguments.sample_rate)
            pcm_handle.flush()
            os.fsync(pcm_handle.fileno())
    except OSError as exc:
        reason = "pcm_too_large" if exc.errno == errno.EFBIG else f"pcm_write_failed:{exc.errno}"
        _fail(reason, exit_code=EXIT_LIMIT)
    if pcm_bytes > arguments.max_pcm_bytes:
        _fail("pcm_too_large", exit_code=EXIT_LIMIT)
    if pcm_bytes == 0:
        _fail("empty_decode", exit_code=EXIT_DECODE_FAILED)
    frames = pcm_bytes // PCM_SAMPLE_BYTES
    _emit(
        sys.stdout,
        {
            "schema": "sightglass.voice-decode.v1",
            "decoder": _decoder_label(),
            "silk_bytes": len(data),
            "envelope": envelope,
            "pcm_bytes": pcm_bytes,
            "frames": frames,
            "duration_ms": frames * 1000 // arguments.sample_rate,
            "sample_rate": arguments.sample_rate,
            "channels": PCM_CHANNELS,
            "sample_format": PCM_SAMPLE_FORMAT,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

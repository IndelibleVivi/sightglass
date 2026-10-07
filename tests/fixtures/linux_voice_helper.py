"""Deterministic stand-in for the prepared Linux whisper.cpp helper.

The real helper is a generated POSIX-sh wrapper around an operator-built
``whisper-cli`` (see ``scripts/build-linux-voice-helper.sh``); these fixtures build a
small executable with the same command line, the same one-JSON-line report, and the
same exit codes so the bounded runner path can be exercised without whisper.cpp or a
real model.  Nothing here downloads a model or emits real audio/transcripts.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from sightglass.voice.linux import HELPER_SCHEMA

DEFAULT_HELPER_TEXT = "Synthetic linux transcript"
STUB_HELPER_MODE = 0o700


def helper_report(*, text: str = DEFAULT_HELPER_TEXT, **overrides: Any) -> str:
    """One synthetic helper report line with the production schema."""

    payload: dict[str, Any] = {
        "schema": HELPER_SCHEMA,
        "backend": "whisper.cpp",
        "helper_version": "whisper.cpp:1.7.4-synthetic",
        "language": "en",
        "locale": "en-US",
        "model": {"identifier": "ggml-small-q5_1"},
        "quantization": "q5_1",
        "threads": 2,
        "max_rss_bytes": 2 * 1024 * 1024 * 1024,
        "segments": [{"start_ms": 0, "end_ms": 400, "text": text}],
        "text": text,
    }
    payload.update(overrides)
    return json.dumps(payload, sort_keys=True)


def helper_exit_body(exit_code: int, *, error: str | None = None) -> str:
    """A helper that reports one error line and exits with ``exit_code``."""

    line = (
        "sys.stderr.write(json.dumps({'error': " + repr(error) + "}) + '\\n')\n"
        if error is not None
        else ""
    )
    return f"import json, sys\n{line}sys.exit({exit_code})\n"


def write_stub_helper(
    root: Path, body: str, *, name: str = "sightglass-whisper", mode: int = STUB_HELPER_MODE
) -> Path:
    """Write ``body`` as an executable helper stub and return its path."""

    root = Path(root)
    script = root / f"{name}.py"
    script.write_text(body)
    path = root / name
    path.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{script}" "$@"\n')
    path.chmod(mode)
    return path


def write_model_binding(
    helper_path: Path,
    *,
    file_name: str = "ggml-small-q5_1.bin",
    identifier: str = "ggml-small-q5_1",
    quantization: str | None = "q5_1",
) -> Path:
    """Create the helper-adjacent ``models/`` binding and return the model path."""

    models = helper_path.parent / "models"
    models.mkdir(mode=0o700, exist_ok=True)
    model = models / file_name
    model.write_bytes(b"synthetic-ggml-model-bytes")
    model.chmod(0o600)
    manifest = {
        "schema": "sightglass.voice-model.v1",
        "file": file_name,
        "identifier": identifier,
        "quantization": quantization,
    }
    manifest_path = models / "model.json"
    manifest_path.write_text(json.dumps(manifest))
    manifest_path.chmod(0o600)
    return model


def real_silk_payload(seconds: float = 0.3) -> bytes:
    """A real SILK V3 stream (WeChat ``\\x02`` prefix) built with the local codec.

    Only used when ``pysilk`` is importable; callers may prefer the synthetic undecodable
    envelope when the decoder extra is absent.
    """

    import io
    import math
    import struct

    import pysilk

    frames = int(seconds * 16_000)
    pcm = b"".join(
        struct.pack("<h", int(9_000 * math.sin(2 * math.pi * 440 * index / 16_000)))
        for index in range(frames)
    )
    output = io.BytesIO()
    pysilk.encode(io.BytesIO(pcm), output, 16_000, 24_000)
    return output.getvalue()

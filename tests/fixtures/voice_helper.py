"""Deterministic stand-in for the compiled Apple speech helper executable.

The real helper is a precompiled Swift binary (``scripts/compile-voice-helper.sh``); these
fixtures build a small executable with the same command line, the same one-JSON-line
report, and the same exit codes, so the bounded child-process path can be exercised
without Apple speech assets.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from sightglass.voice.apple import HELPER_SCHEMA

DEFAULT_HELPER_TEXT = "Synthetic local transcript"
STUB_HELPER_MODE = 0o700


def helper_report(*, text: str = DEFAULT_HELPER_TEXT, **overrides: Any) -> str:
    """One synthetic helper report line with the production schema."""

    payload: dict[str, Any] = {
        "schema": HELPER_SCHEMA,
        "helper": "sightglass-transcribe",
        "helper_version": "1",
        "backend": "SpeechAnalyzer+SpeechTranscriber",
        "locale": "en-US",
        "asset_status": "installed",
        "model": {"identifier": "unknown"},
        "os_version": "Version 26.5.2 (Build 25F84)",
        "volatile_excluded": True,
        "text": text,
        "segments": [{"start_ms": 0, "end_ms": 400, "text": text}],
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
    root: Path, body: str, *, name: str = "helper", mode: int = STUB_HELPER_MODE
) -> Path:
    """Write ``body`` as an executable helper stub and return its path.

    A shebang cannot carry this interpreter's path (a checkout path may contain a space),
    so the stub is a small shell wrapper: like the real helper it is an executable, not a
    script the kernel has to re-parse.
    """

    root = Path(root)
    script = root / f"{name}.py"
    script.write_text(body)
    path = root / name
    path.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{script}" "$@"\n')
    path.chmod(mode)
    return path


def print_report_body(*, text: str = DEFAULT_HELPER_TEXT) -> str:
    return f"print({helper_report(text=text)!r})\n"

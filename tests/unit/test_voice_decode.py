from __future__ import annotations

import io
import math
import os
import struct
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.voice.decoder import (
    PCM_SAMPLE_RATE,
    DecodedAudio,
    SilkDecoder,
    pcm_recipe,
)
from sightglass.voice.subprocess import BoundedProcessError, run_bounded

DECODER_AVAILABLE = SilkDecoder.available()
requires_decoder = unittest.skipUnless(
    DECODER_AVAILABLE, "silk-python voice extra is not installed"
)


def pcm_sine(seconds: float = 0.5, frequency: float = 440.0) -> bytes:
    frames = int(seconds * PCM_SAMPLE_RATE)
    return b"".join(
        struct.pack("<h", int(12_000 * math.sin(2 * math.pi * frequency * index / PCM_SAMPLE_RATE)))
        for index in range(frames)
    )


def encoded_silk(pcm: bytes, *, prefix: bool = True) -> bytes:
    import pysilk

    output = io.BytesIO()
    pysilk.encode(io.BytesIO(pcm), output, PCM_SAMPLE_RATE, 24_000)
    data = output.getvalue()
    return data if prefix else data.lstrip(b"\x02")


def rms(data: bytes) -> float:
    count = len(data) // 2
    if count == 0:
        return 0.0
    values = struct.unpack(f"<{count}h", data[: count * 2])
    return (sum(value * value for value in values) / count) ** 0.5


class VoiceDecodeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.decoder = SilkDecoder()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def decode(self, data: bytes, **kwargs: Any) -> tuple[bytes, DecodedAudio]:
        silk_path = self.root / "input.silk"
        pcm_path = self.root / "output.pcm"
        silk_path.write_bytes(data)
        decoder = SilkDecoder(**kwargs) if kwargs else self.decoder
        decoded = decoder.decode(silk_path, pcm_path)
        return pcm_path.read_bytes(), decoded

    @requires_decoder
    def test_roundtrip_restores_the_expected_pcm(self) -> None:
        pcm = pcm_sine(0.5)
        for prefix in (True, False):
            with self.subTest(prefix=prefix):
                decoded_pcm, decoded = self.decode(encoded_silk(pcm, prefix=prefix))
                self.assertEqual(len(decoded_pcm), len(pcm))
                self.assertAlmostEqual(rms(decoded_pcm), rms(pcm), delta=rms(pcm) * 0.15)
                self.assertEqual(decoded.frames, len(pcm) // 2)
                self.assertEqual(decoded.duration_ms, 500)
                self.assertEqual(decoded.recipe, pcm_recipe())
                self.assertEqual(decoded.recipe["sample_rate"], PCM_SAMPLE_RATE)
                self.assertEqual(decoded.recipe["sample_format"], "s16le")
                self.assertTrue(decoded.decoder.startswith("pysilk/"))
                self.assertEqual(decoded.envelope, "wechat_prefix" if prefix else "plain")

    @requires_decoder
    def test_decoder_reports_its_version(self) -> None:
        self.assertTrue(DECODER_AVAILABLE)
        self.assertIsNotNone(SilkDecoder.version())

    def test_malformed_envelopes_fail_closed(self) -> None:
        cases = {
            "unsupported_silk_envelope": [b"not-silk-at-all", b"#!SILK", b"\x02garbage", b""],
            "empty_silk_payload": [b"#!SILK_V3", b"\x02#!SILK_V3"],
        }
        for reason, payloads in cases.items():
            for payload in payloads:
                with self.subTest(reason=reason, size=len(payload)):
                    with self.assertRaises(SightglassError) as caught:
                        self.decode(payload)
                    self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)
                    self.assertEqual(caught.exception.details["reason"], reason)
                    self.assertEqual(caught.exception.details["stage"], "decode")

    @requires_decoder
    def test_truncated_stream_fails_closed(self) -> None:
        silk = encoded_silk(pcm_sine(1.0))
        with self.assertRaises(SightglassError) as caught:
            self.decode(silk[:10])
        self.assertEqual(caught.exception.details["reason"], "empty_silk_payload")
        for size in (20, len(silk) // 3, len(silk) // 2, len(silk) - 1):
            with self.subTest(size=size):
                with self.assertRaises(SightglassError) as caught:
                    self.decode(silk[:size])
                self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)
                self.assertEqual(caught.exception.details["stage"], "decode")
                self.assertTrue(
                    caught.exception.details["reason"].startswith("decode_failed"),
                    caught.exception.details,
                )

    def test_zero_byte_decode_fails_closed(self) -> None:
        # Some decoder inputs return no error and no frames; the child must still treat
        # that as a failure instead of committing an empty transcript.  The child sets
        # RLIMIT_FSIZE for its own file, so a in-process call must put the process-wide
        # limit back before anything else writes a file.
        import resource

        from sightglass.voice import _decode_child

        pcm_path = self.root / "guard.pcm"
        silk_path = self.root / "guard.silk"
        silk_path.write_bytes(b"#!SILK_V3" + b"\x00" * 8)
        previous = resource.getrlimit(resource.RLIMIT_FSIZE)
        try:
            with silk_path.open("rb") as silk_handle, pcm_path.open("wb") as pcm_handle:
                argv = [
                    "--silk-fd",
                    str(silk_handle.fileno()),
                    "--pcm-fd",
                    str(pcm_handle.fileno()),
                    "--max-input-bytes",
                    "4096",
                    "--max-pcm-bytes",
                    "4096",
                ]
                captured = io.StringIO()
                with mock.patch.object(_decode_child, "_decode", return_value=0):
                    with mock.patch.object(sys, "stderr", captured):
                        with self.assertRaises(SystemExit) as exit_info:
                            _decode_child.main(argv)
        finally:
            resource.setrlimit(resource.RLIMIT_FSIZE, previous)
        self.assertEqual(exit_info.exception.code, 5)
        self.assertIn("empty_decode", captured.getvalue())
        self.assertEqual(resource.getrlimit(resource.RLIMIT_FSIZE), previous)

    @requires_decoder
    def test_input_cap_fails_closed(self) -> None:
        silk = encoded_silk(pcm_sine(0.2))
        with self.assertRaises(SightglassError) as caught:
            self.decode(silk, max_silk_bytes=len(silk) - 1)
        self.assertEqual(caught.exception.details["reason"], "input_too_large")

    @requires_decoder
    def test_pcm_cap_fails_closed(self) -> None:
        silk = encoded_silk(pcm_sine(2.0))
        with self.assertRaises(SightglassError) as caught:
            self.decode(silk, max_pcm_seconds=1)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)
        self.assertEqual(caught.exception.details["reason"], "pcm_too_large")

    def test_missing_decoder_reports_blocked(self) -> None:
        with mock.patch.object(SilkDecoder, "available", return_value=False):
            with self.assertRaises(SightglassError) as caught:
                self.decode(b"\x02#!SILK_V3payload")
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)
        self.assertEqual(caught.exception.details["reason"], "decoder_unavailable")


class BoundedProcessTests(unittest.TestCase):
    def test_timeout_kills_the_process_group(self) -> None:
        with tempfile.TemporaryDirectory() as work:
            marker = Path(work) / "pids"
            script = (
                "import os, sys, time\n"
                "child = os.fork()\n"
                "if child == 0:\n"
                "    time.sleep(60)\n"
                "    os._exit(0)\n"
                f"open({str(marker)!r}, 'w').write(f'{{os.getpid()}} {{child}}')\n"
                "time.sleep(60)\n"
            )
            outcome = run_bounded(
                (sys.executable, "-c", script),
                timeout_seconds=1.0,
                max_stdout_bytes=4096,
                max_stderr_bytes=4096,
            )
            self.assertTrue(outcome.timed_out)
            self.assertIsNotNone(outcome.exit_code)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not marker.exists():
                time.sleep(0.05)
            parent_pid, child_pid = (int(value) for value in marker.read_text().split())
            for pid in (parent_pid, child_pid):
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)

    def test_stdout_limit_is_enforced(self) -> None:
        outcome = run_bounded(
            (sys.executable, "-c", "print('x' * 100000)"),
            timeout_seconds=10.0,
            max_stdout_bytes=1024,
            max_stderr_bytes=1024,
        )
        self.assertEqual(outcome.limit_exceeded, "stdout")
        self.assertLessEqual(len(outcome.stdout), 1024)
        self.assertFalse(outcome.ok)

    def test_stderr_limit_is_enforced(self) -> None:
        outcome = run_bounded(
            (sys.executable, "-c", "import sys; sys.stderr.write('e' * 100000)"),
            timeout_seconds=10.0,
            max_stdout_bytes=1024,
            max_stderr_bytes=512,
        )
        self.assertEqual(outcome.limit_exceeded, "stderr")
        self.assertLessEqual(len(outcome.stderr), 512)

    def test_success_captures_bounded_streams(self) -> None:
        outcome = run_bounded(
            (sys.executable, "-c", "print('ok')"),
            timeout_seconds=10.0,
            max_stdout_bytes=1024,
            max_stderr_bytes=1024,
        )
        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.stdout.strip(), b"ok")
        self.assertEqual(outcome.argv[0], sys.executable)

    def test_spawn_failure_raises(self) -> None:
        with self.assertRaises(BoundedProcessError):
            run_bounded(
                ("/nonexistent/sightglass-voice-probe",),
                timeout_seconds=1.0,
                max_stdout_bytes=16,
                max_stderr_bytes=16,
            )


if __name__ == "__main__":
    unittest.main()

"""Focused Linux platform coverage: secrets, image backend, voice runner.

These tests are host-agnostic Python; the platform branch is selected by patching
``sys.platform`` or by calling the Linux code paths directly.  Synthetic helper and
probe stubs establish the plumbing only.  Real Linux format support (libvips loaders)
and real whisper.cpp ASR remain coordinator work on the target host and are called out
where a test would otherwise over-claim.
"""

from __future__ import annotations

import json
import os
import stat
import struct
import sys
import tempfile
import time
import unittest
import wave
from pathlib import Path
from typing import Any
from unittest import mock

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.resources import processors
from sightglass.runtime.secrets import (
    FileSecretStore,
    KeychainSecretStore,
    MemorySecretStore,
    SyntheticTestEnvironmentSecretStore,
    default_secret_store,
)
from sightglass.voice import linux as linux_module
from sightglass.voice.capture import CapturedVoice
from sightglass.voice.decoder import PCM_SAMPLE_RATE, pcm_recipe
from sightglass.voice.linux import (
    DEFAULT_MAX_HELPER_RSS_BYTES,
    HELPER_THREADS,
    MODEL_MANIFEST_NAME,
    DirectRunner,
    LinuxSilkTranscriber,
    LinuxWhisperHelper,
    ModelBinding,
    SystemdRunner,
    helper_environment,
    probe_helper,
    production_runner,
    resolve_model_binding,
    whisper_language,
    write_wav,
)
from tests.fixtures.linux_voice_helper import (
    real_silk_payload,
    write_model_binding,
    write_stub_helper,
)

LOCALE = "en-US"


class _FakeRunner(DirectRunner):
    """Direct runner that records the argv/env it was handed."""

    def __init__(self) -> None:
        self.seen: list[tuple[list[str], dict[str, str]]] = []

    def run(self, argv, *, timeout_seconds, env):
        self.seen.append((list(argv), dict(env)))
        return super().run(argv, timeout_seconds=timeout_seconds, env=env)


class FileSecretStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.directory = self.root / "secrets"
        self.store = FileSecretStore(self.directory)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_roundtrip_and_atomic_replace(self) -> None:
        self.store.set("mcp-reader-token", "synthetic-token-value")
        self.assertEqual(self.store.get("mcp-reader-token"), "synthetic-token-value")
        self.store.set("mcp-reader-token", "synthetic-second")
        self.assertEqual(self.store.get("mcp-reader-token"), "synthetic-second")
        path = self.directory / "mcp-reader-token.secret"
        metadata = path.lstat()
        self.assertTrue(stat.S_ISREG(metadata.st_mode))
        self.assertEqual(stat.S_IMODE(metadata.st_mode), 0o600)
        self.assertEqual(metadata.st_nlink, 1)
        self.assertEqual(stat.S_IMODE(self.directory.stat().st_mode), 0o700)

    def test_missing_secret_is_a_safe_error(self) -> None:
        with self.assertRaises(RuntimeError) as caught:
            self.store.get("operator-token")
        self.assertIn("unavailable", str(caught.exception))

    def test_symlinked_secret_file_is_refused(self) -> None:
        self.store.set("mcp-reader-token", "value")
        link = self.directory / "operator-token.secret"
        link.symlink_to(self.directory / "mcp-reader-token.secret")
        with self.assertRaises(RuntimeError):
            self.store.get("operator-token")

    def test_hardlinked_secret_file_is_refused(self) -> None:
        self.store.set("mcp-reader-token", "value")
        os.link(self.directory / "mcp-reader-token.secret", self.directory / "extra.secret")
        with self.assertRaises(RuntimeError):
            self.store.get("extra")

    def test_fifo_does_not_hang_and_is_refused(self) -> None:
        self.store.set("mcp-reader-token", "value")
        os.mkfifo(self.directory / "operator-token.secret", 0o600)
        started = time.monotonic()
        with self.assertRaises(RuntimeError):
            self.store.get("operator-token")
        self.assertLess(time.monotonic() - started, 5.0)

    def test_public_directory_is_refused(self) -> None:
        public = self.root / "public"
        public.mkdir(mode=0o755)
        with self.assertRaises(RuntimeError):
            FileSecretStore(public).set("mcp-reader-token", "value")
        with self.assertRaises(RuntimeError):
            FileSecretStore(public).get("mcp-reader-token")

    def test_symlinked_directory_is_refused(self) -> None:
        real = self.root / "real-secrets"
        real.mkdir(mode=0o700)
        link = self.root / "linked-secrets"
        link.symlink_to(real)
        self.store.set("mcp-reader-token", "value")
        (link / "operator-token.secret").write_text("linked")
        with self.assertRaises(RuntimeError):
            FileSecretStore(link).get("operator-token")

    def test_oversize_secret_is_refused_on_read(self) -> None:
        self.directory.mkdir(mode=0o700)
        oversize = self.directory / "operator-token.secret"
        oversize.write_bytes(b"x" * 8192)
        oversize.chmod(0o600)
        with self.assertRaises(RuntimeError):
            self.store.get("operator-token")

    def test_empty_secret_is_refused(self) -> None:
        self.store.set("mcp-reader-token", "value")
        empty = self.directory / "operator-token.secret"
        empty.write_bytes(b"")
        empty.chmod(0o600)
        with self.assertRaises(RuntimeError):
            self.store.get("operator-token")

    def test_replacement_during_read_is_detected(self) -> None:
        self.store.set("mcp-reader-token", "value")
        real_read = os.read
        calls = {"n": 0}

        def swapping_read(fd, size):
            chunk = real_read(fd, size)
            calls["n"] += 1
            if calls["n"] == 1:
                os.replace(
                    self.directory / "spare.secret",
                    self.directory / "mcp-reader-token.secret",
                )
            return chunk

        (self.directory / "spare.secret").write_bytes(b"replacement-value")
        (self.directory / "spare.secret").chmod(0o600)
        with mock.patch("sightglass.runtime.secrets.os.read", side_effect=swapping_read):
            with self.assertRaises(RuntimeError):
                self.store.get("mcp-reader-token")

    def test_invalid_account_names_are_refused(self) -> None:
        for account in ("../escape", "a/b", "", "x" * 200):
            with self.subTest(account=account):
                with self.assertRaises(RuntimeError):
                    self.store.set(account, "value")

    def test_write_never_leaves_a_temp_file(self) -> None:
        self.store.set("mcp-reader-token", "value")
        entries = sorted(item.name for item in self.directory.iterdir())
        self.assertEqual(entries, ["mcp-reader-token.secret"])


class SecretStoreSelectionTests(unittest.TestCase):
    def test_linux_selects_file_store(self) -> None:
        with mock.patch.object(sys, "platform", "linux"):
            store = default_secret_store()
        self.assertIsInstance(store, FileSecretStore)

    def test_darwin_selects_keychain(self) -> None:
        with mock.patch.object(sys, "platform", "darwin"):
            store = default_secret_store()
        self.assertIsInstance(store, KeychainSecretStore)

    def test_synthetic_test_transport_wins(self) -> None:
        with mock.patch.dict(os.environ, {"SIGHTGLASS_SYNTHETIC_TEST_SECRETS": "1"}):
            store = default_secret_store()
        self.assertIsInstance(store, SyntheticTestEnvironmentSecretStore)

    def test_memory_store_is_injectable(self) -> None:
        store = MemorySecretStore({"mcp-reader-token": "value"})
        self.assertEqual(store.get("mcp-reader-token"), "value")


class VipsBackendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_linux_without_vips_fails_closed(self) -> None:
        with (
            mock.patch.object(sys, "platform", "linux"),
            mock.patch.object(processors.shutil, "which", return_value=None),
        ):
            with self.assertRaises(SightglassError) as caught:
                processors.inspect_image(b"\x89PNG\r\n\x1a\nsynthetic")
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)
        self.assertEqual(caught.exception.details["reason"], "image_backend_unavailable")

    def test_vips_probe_requires_both_tools(self) -> None:
        def which(name: str) -> str | None:
            return "/usr/bin/vipsheader" if name == "vipsheader" else None

        with mock.patch.object(processors.shutil, "which", side_effect=which):
            self.assertFalse(processors._vips_available())
        with mock.patch.object(processors.shutil, "which", return_value="/usr/bin/tool"):
            self.assertTrue(processors._vips_available())

    def test_macos_still_uses_sips_backend(self) -> None:
        with mock.patch.object(sys, "platform", "darwin"):
            backend = processors._image_backend()
        self.assertIsInstance(backend, processors._SipsImageBackend)

    def test_processor_status_reports_vips_without_claiming(self) -> None:
        status = processors.processor_status()
        self.assertIn("vipsheader", status)
        self.assertIn("vipsthumbnail", status)
        self.assertIn("sips", status)

    def test_preview_processor_version_tracks_backend(self) -> None:
        with mock.patch.object(sys, "platform", "darwin"):
            self.assertEqual(processors.image_preview_processor_version(), "sips-v1")
        with (
            mock.patch.object(sys, "platform", "linux"),
            mock.patch.object(processors, "_vips_available", return_value=True),
        ):
            self.assertEqual(processors.image_preview_processor_version(), "vips-v2")

    def test_vips_probe_reports_missing_loader_as_decode_failure(self) -> None:
        backend = processors._VipsImageBackend()

        def fake_run(command, output, *, max_output_bytes, stdout_to_output, timeout_seconds=None):
            del command, max_output_bytes, stdout_to_output, timeout_seconds
            output.write_bytes(b"")
            return b""

        with (
            mock.patch.object(processors, "_command", return_value="/usr/bin/vipsheader"),
            mock.patch.object(processors, "_run_bounded_file", side_effect=fake_run),
        ):
            with self.assertRaises(SightglassError) as caught:
                backend.inspect(b"synthetic")
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_DECODE_FAILED)

    def test_vips_inspect_parses_header_without_optional_page_count(self) -> None:
        backend = processors._VipsImageBackend()

        def fake_run(command, output, *, max_output_bytes, stdout_to_output, timeout_seconds=None):
            self.assertEqual(command[1], "--all")
            self.assertEqual(max_output_bytes, 64 * 1024)
            self.assertTrue(stdout_to_output)
            return b"input: 3200x2400 uchar, jpegload\nwidth: 3200\nheight: 2400\nformat: uchar\n"

        with (
            mock.patch.object(processors, "_command", return_value="/usr/bin/vipsheader"),
            mock.patch.object(processors, "_run_bounded_file", side_effect=fake_run),
        ):
            info = backend.inspect(b"\xff\xd8\xffsynthetic")
        self.assertEqual((info.width, info.height, info.format), (3200, 2400, "jpeg"))
        self.assertFalse(info.animated)

    def test_vips_inspect_rejects_oversize_dimensions(self) -> None:
        backend = processors._VipsImageBackend()

        def fake_run(command, output, *, max_output_bytes, stdout_to_output, timeout_seconds=None):
            del command, output, max_output_bytes, stdout_to_output, timeout_seconds
            return b"width: 60000\nheight: 60000\nformat: uchar\n"

        with (
            mock.patch.object(processors, "_command", return_value="/usr/bin/vipsheader"),
            mock.patch.object(processors, "_run_bounded_file", side_effect=fake_run),
        ):
            with self.assertRaises(SightglassError) as caught:
                backend.inspect(b"synthetic")
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_TOO_LARGE)

    def test_vips_preview_inspects_before_launch_and_uses_exact_png_output(self) -> None:
        from sightglass.resources.processors import ImageInfo

        inspected = []
        source = b"\xff\xd8\xffsynthetic"
        png = b"\x89PNG\r\n\x1a\nsynthetic"

        def fake_run(command, output, **arguments):
            self.assertEqual(inspected, [source])
            self.assertIn("--size=2048x2048>", command)
            self.assertIn(f"--output={output.as_posix()}", command)
            self.assertEqual(output.name, "preview.png")
            self.assertFalse(arguments["stdout_to_output"])
            return png

        def inspect(data):
            inspected.append(data)
            return ImageInfo(3, 2, "jpeg", False)

        with (
            mock.patch.object(processors, "_command", return_value="vipsthumbnail"),
            mock.patch.object(processors, "inspect_image", side_effect=inspect),
            mock.patch.object(processors, "_run_bounded_file", side_effect=fake_run),
            mock.patch.object(
                processors, "_vips_inspect_image", return_value=ImageInfo(3, 2, "png", False),
            ),
        ):
            preview, info = processors._vips_image_preview(source, max_bytes=1024)
        self.assertEqual(preview, png)
        self.assertEqual((info.width, info.height, info.format), (3, 2, "png"))


class _StubChildRunner(DirectRunner):
    """Return a fabricated ``_linux_child`` report without launching the real child."""

    def __init__(self) -> None:
        self.seen: list[tuple[list[str], dict[str, str]]] = []
        self.stdout = b""
        self.exit_code: int | None = 0
        self.stderr = b""

    def run(self, argv, *, timeout_seconds, env, pass_fds=()):
        del timeout_seconds, pass_fds
        self.seen.append((list(argv), dict(env)))
        from sightglass.voice.subprocess import ProcessOutcome

        return ProcessOutcome(
            argv=tuple(argv),
            exit_code=self.exit_code,
            signal_number=None,
            stdout=self.stdout,
            stderr=self.stderr,
            timed_out=False,
            limit_exceeded=None,
        )


def _child_report(*, text: str = "Synthetic linux transcript", **overrides: Any) -> bytes:
    """A synthetic ``_linux_child`` report with the production schema."""

    transcript: dict[str, Any] = {
        "schema": "sightglass.voice-transcript.v1",
        "backend": "whisper.cpp",
        "helper_version": "whisper.cpp:1.7.4-synthetic",
        "language": "en",
        "locale": "en-US",
        "model": {"identifier": "ggml-small-q5_1"},
        "quantization": "q5_1",
        "segments": [{"start_ms": 0, "end_ms": 400, "text": text}],
        "text": text,
    }
    transcript.update(overrides)
    decode = {
        "decoder": "pysilk/0.2.8",
        "silk_bytes": 128,
        "envelope": "wechat_prefix",
        "pcm_bytes": 3200,
        "frames": 1600,
        "duration_ms": 100,
        "sample_rate": 16_000,
        "channels": 1,
        "sample_format": "s16le",
    }
    report = {"schema": "sightglass.voice-linux-job.v1", "decode": decode, "transcript": transcript}
    return json.dumps(report).encode()


class LinuxVoiceHelperTests(unittest.TestCase):
    """Outer helper contract, driven through an injected stub child runner."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.silk = self.root / "audio.silk"
        self.silk.write_bytes(b"\x02#!SILK_V3" + bytes(range(64)))
        self.silk.chmod(0o600)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def helper(
        self, *, model: bool = True, mode: int = 0o700, runner: _StubChildRunner | None = None
    ) -> LinuxWhisperHelper:
        path = write_stub_helper(self.root, "pass\n", mode=mode)
        binding = None
        if model:
            model_path = write_model_binding(path)
            binding = ModelBinding(model_path, "ggml-small-q5_1", "q5_1")
        return LinuxWhisperHelper(
            path,
            language=LOCALE,
            model=binding,
            timeout_seconds=10.0,
            runner=runner if runner is not None else _StubChildRunner(),
        )

    def assertBlocked(self, helper: LinuxWhisperHelper, reason: str) -> SightglassError:
        with self.assertRaises(SightglassError) as caught:
            helper.transcribe_silk(self.silk)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)
        self.assertEqual(caught.exception.details["reason"], reason)
        self.assertEqual(caught.exception.details["stage"], "recognize")
        self.assertFalse(caught.exception.retryable)
        return caught.exception

    def test_success_parses_the_child_report_with_provenance(self) -> None:
        runner = _StubChildRunner()
        runner.stdout = _child_report()
        helper = self.helper(runner=runner)
        transcript = helper.transcribe_silk(self.silk)
        self.assertEqual(transcript.text, "Synthetic linux transcript")
        self.assertEqual(transcript.backend, "whisper.cpp")
        self.assertEqual(transcript.helper_version, "whisper.cpp:1.7.4-synthetic")
        self.assertEqual(transcript.quantization, "q5_1")
        self.assertEqual(transcript.segments, 1)
        self.assertEqual(transcript.decoder, "pysilk/0.2.8")
        self.assertEqual(transcript.decoder_envelope, "wechat_prefix")
        self.assertEqual(transcript.pcm_bytes, 3200)
        self.assertEqual(transcript.pcm_frames, 1600)
        self.assertEqual(transcript.decoded_duration_ms, 100)
        provenance = transcript.provenance()
        self.assertEqual(provenance["locale"], LOCALE)
        self.assertEqual(provenance["language"], "en")
        self.assertEqual(provenance["threads"], HELPER_THREADS)
        # The whole job runs as one bounded codec child inside the runner.
        argv, _env = runner.seen[0]
        self.assertIn("sightglass.voice._linux_child", argv)
        self.assertIn("--silk-fd", argv)

    def test_child_receives_language_threads_and_the_timeout(self) -> None:
        runner = _StubChildRunner()
        runner.stdout = _child_report()
        helper = self.helper(runner=runner)
        helper.transcribe_silk(self.silk)
        argv, _env = runner.seen[0]
        self.assertEqual(argv[argv.index("--language") + 1], "en")
        self.assertEqual(argv[argv.index("--threads") + 1], str(HELPER_THREADS))
        # The inner wrapper watchdog is strictly inside the 10 s outer job watchdog.
        inner = float(argv[argv.index("--helper-timeout-seconds") + 1])
        self.assertLess(inner, 10.0)
        self.assertGreater(inner, 0.0)

    def test_symlinked_silk_is_blocked(self) -> None:
        helper = self.helper()
        link = self.root / "linked.silk"
        link.symlink_to(self.silk)
        with self.assertRaises(SightglassError) as caught:
            helper.transcribe_silk(link)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)

    def test_hardlinked_silk_is_blocked(self) -> None:
        helper = self.helper()
        os.link(self.silk, self.root / "second.silk")
        with self.assertRaises(SightglassError) as caught:
            helper.transcribe_silk(self.root / "second.silk")
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)

    def test_public_mode_silk_is_blocked(self) -> None:
        helper = self.helper()
        self.silk.chmod(0o644)
        with self.assertRaises(SightglassError) as caught:
            helper.transcribe_silk(self.silk)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)

    def test_empty_silk_is_blocked(self) -> None:
        helper = self.helper()
        empty = self.root / "empty.silk"
        empty.write_bytes(b"")
        empty.chmod(0o600)
        with self.assertRaises(SightglassError) as caught:
            helper.transcribe_silk(empty)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)

    def test_model_unavailable_maps_to_blocked(self) -> None:
        runner = _StubChildRunner()
        runner.exit_code = 9
        runner.stderr = b'{"error":"model_unavailable"}\n'
        self.assertBlocked(self.helper(runner=runner), "model_unavailable")

    def test_decode_failure_maps_to_blocked(self) -> None:
        runner = _StubChildRunner()
        runner.exit_code = 5
        runner.stderr = b'{"error":"decode_failed:ValueError"}\n'
        self.assertBlocked(self.helper(runner=runner), "decode_failed:ValueError")

    def test_transcription_failure_stays_retryable(self) -> None:
        runner = _StubChildRunner()
        runner.exit_code = 8
        runner.stderr = b'{"error":"transcription_failed"}\n'
        with self.assertRaises(SightglassError) as caught:
            self.helper(runner=runner).transcribe_silk(self.silk)
        self.assertEqual(caught.exception.code, ErrorCode.SERVICE_UNAVAILABLE)
        self.assertTrue(caught.exception.retryable)

    def test_malformed_report_is_blocked(self) -> None:
        runner = _StubChildRunner()
        runner.stdout = b"not json"
        self.assertBlocked(self.helper(runner=runner), "helper_report_invalid")

    def test_wrong_child_schema_is_blocked(self) -> None:
        runner = _StubChildRunner()
        runner.stdout = json.dumps({"schema": "other.v1", "decode": {}, "transcript": {}}).encode()
        self.assertBlocked(self.helper(runner=runner), "helper_report_invalid")

    def test_oversized_transcript_is_blocked(self) -> None:
        runner = _StubChildRunner()
        runner.stdout = _child_report(text="x" * 200_001)
        self.assertBlocked(self.helper(runner=runner), "helper_text_too_long")

    def test_expired_deadline_never_starts_the_child(self) -> None:
        runner = _StubChildRunner()
        runner.stdout = _child_report()
        helper = self.helper(runner=runner)
        with self.assertRaises(SightglassError) as caught:
            helper.transcribe_silk(self.silk, deadline=time.monotonic() - 1)
        self.assertEqual(caught.exception.code, ErrorCode.SERVICE_TIMEOUT)
        self.assertEqual(caught.exception.details["reason"], "operation_deadline")
        self.assertEqual(runner.seen, [])

    def test_missing_model_is_blocked(self) -> None:
        path = write_stub_helper(self.root, "pass\n")
        binding = ModelBinding(self.root / "models" / "absent.bin", "absent", "q5_1")
        helper = LinuxWhisperHelper(
            path,
            language=LOCALE,
            model=binding,
            timeout_seconds=10.0,
            runner=_StubChildRunner(),
        )
        self.assertBlocked(helper, "model_missing")

    def test_symlinked_helper_is_blocked(self) -> None:
        path = write_stub_helper(self.root, "pass\n")
        link = self.root / "linked-helper"
        link.symlink_to(path)
        binding = write_model_binding(path)
        helper = LinuxWhisperHelper(
            link,
            language=LOCALE,
            model=ModelBinding(binding, "ggml-small-q5_1", "q5_1"),
            timeout_seconds=10.0,
            runner=_StubChildRunner(),
        )
        self.assertBlocked(helper, "helper_not_regular")

    def test_environment_has_no_credential_shaped_names(self) -> None:
        filtered = helper_environment(
            {
                "HOME": "/home/synthetic",
                "PATH": "/usr/bin",
                "LANG": "en_US.UTF-8",
                "OPENAI_API_KEY": "synthetic",
                "LD_PRELOAD": "/synthetic/lib.so",
                "PWD": "/synthetic",
            }
        )
        self.assertEqual(
            filtered,
            {"HOME": "/home/synthetic", "PATH": "/usr/bin", "LANG": "en_US.UTF-8"},
        )

    def test_spawn_failure_is_blocked(self) -> None:
        from sightglass.voice.subprocess import BoundedProcessError

        helper = self.helper()
        with mock.patch.object(
            _StubChildRunner, "run", side_effect=BoundedProcessError("synthetic spawn failure")
        ):
            self.assertBlocked(helper, "helper_spawn_failed")

    def test_runner_unavailable_is_blocked(self) -> None:
        helper = self.helper()
        helper.runner = _UnavailableRunner()
        self.assertBlocked(helper, "helper_runner_unavailable")


def _timed_out_outcome(argv, **kwargs):
    from sightglass.voice.subprocess import ProcessOutcome

    return ProcessOutcome(
        argv=tuple(str(value) for value in argv),
        exit_code=None,
        signal_number=9,
        stdout=b"",
        stderr=b"",
        timed_out=True,
        limit_exceeded=None,
    )


def _ok_outcome(argv, **kwargs):
    from sightglass.voice.subprocess import ProcessOutcome

    return ProcessOutcome(
        argv=tuple(str(value) for value in argv),
        exit_code=0,
        signal_number=None,
        stdout=b"",
        stderr=b"",
        timed_out=False,
        limit_exceeded=None,
    )


class _UnavailableRunner(DirectRunner):
    def available(self) -> bool:
        return False


class WhisperLanguageTests(unittest.TestCase):
    def test_supported_locales_map_to_short_codes(self) -> None:
        self.assertEqual(whisper_language("zh-CN"), "zh")
        self.assertEqual(whisper_language("zh-TW"), "zh")
        self.assertEqual(whisper_language("en-US"), "en")
        self.assertEqual(whisper_language("en-GB"), "en")
        self.assertEqual(whisper_language("auto"), "auto")
        self.assertEqual(whisper_language("de"), "de")

    def test_bare_language_passes_through(self) -> None:
        self.assertEqual(whisper_language("ja"), "ja")


class WriteWavTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_wav_preserves_pcm_bytes_and_header(self) -> None:
        pcm = self.root / "audio.pcm"
        pcm.write_bytes(struct.pack("<1600h", *range(1600)))
        wav = self.root / "audio.wav"
        written = write_wav(pcm, wav, max_bytes=1024 * 1024)
        self.assertEqual(written, 3200)
        with wave.open(str(wav), "rb") as reader:
            self.assertEqual(reader.getnchannels(), 1)
            self.assertEqual(reader.getsampwidth(), 2)
            self.assertEqual(reader.getframerate(), PCM_SAMPLE_RATE)
            self.assertEqual(reader.readframes(reader.getnframes()), pcm.read_bytes())

    def test_oversize_pcm_is_rejected(self) -> None:
        pcm = self.root / "audio.pcm"
        pcm.write_bytes(b"\x00" * 4096)
        with self.assertRaises(SightglassError) as caught:
            write_wav(pcm, self.root / "audio.wav", max_bytes=1024)
        self.assertEqual(caught.exception.details["reason"], "wav_too_large")


class RunnerSelectionTests(unittest.TestCase):
    def test_systemd_command_is_explicit_and_memory_limited(self) -> None:
        runner = SystemdRunner(max_rss_bytes=2 * 1024**3)
        command = runner.command(
            ["/helper", "--pcm", "x.wav"],
            unit_name="sightglass-whisper-1.scope",
            runtime_max_seconds=600.0,
        )
        self.assertIn("systemd-run", command[0])
        self.assertIn("--scope", command)
        self.assertIn("--collect", command)
        self.assertIn("--unit=sightglass-whisper-1.scope", command)
        self.assertIn("MemoryMax=2147483648", command)
        self.assertIn("MemorySwapMax=0", command)
        # Kill on OOM and on timeout: the whole helper tree must not survive.  The
        # property is the real systemd v255 ``OOMPolicy=kill`` (``MemoryOOMGroup`` is
        # not a valid scope property and was rejected by the actual target).
        self.assertIn("OOMPolicy=kill", command)
        self.assertNotIn("MemoryOOMGroup=yes", command)
        self.assertIn("KillMode=control-group", command)
        self.assertIn("RuntimeMaxSec=600", command)
        self.assertEqual(command[-3:], ["/helper", "--pcm", "x.wav"])
        self.assertTrue(runner.isolates)

    def test_unit_names_are_opaque_and_collision_resistant(self) -> None:
        names = {SystemdRunner.unit_name() for _ in range(1000)}
        self.assertEqual(len(names), 1000)
        for name in names:
            self.assertTrue(name.startswith("sightglass-whisper-"))
            self.assertTrue(name.endswith(".scope"))
            nonce = name.removeprefix("sightglass-whisper-").removesuffix(".scope")
            # 128-bit hex nonce, not a PID/millisecond tuple.
            self.assertEqual(len(nonce), 32)
            int(nonce, 16)

    def test_timeout_cleanup_targets_the_owned_transient_unit(self) -> None:
        # The watchdog must stop exactly this job's scope: the unit name is minted per
        # run and passed to systemd-run, so a timeout can only act on its own unit.
        runner = SystemdRunner(max_rss_bytes=2 * 1024**3)
        seen_units: list[str] = []

        real_command = runner.command

        def capture_command(argv, *, unit_name, runtime_max_seconds):
            seen_units.append(unit_name)
            return real_command(argv, unit_name=unit_name, runtime_max_seconds=runtime_max_seconds)

        with (
            mock.patch.object(SystemdRunner, "available", return_value=True),
            mock.patch.object(SystemdRunner, "command", side_effect=capture_command),
            mock.patch.object(
                linux_module.subprocess,
                "run",
                return_value=linux_module.subprocess.CompletedProcess(
                    [], 0, b"ActiveState=inactive\n", b""
                ),
            ),
            mock.patch.object(
                linux_module,
                "run_bounded",
                side_effect=lambda *a, **k: _timed_out_outcome(*a, **k),
            ),
        ):
            outcome = runner.run(["/helper"], timeout_seconds=5.0, env={})
        self.assertEqual(len(seen_units), 1)
        self.assertTrue(seen_units[0].startswith("sightglass-whisper-"))
        self.assertIn("--unit=" + seen_units[0], outcome.argv)
        self.assertIn("RuntimeMaxSec=5", outcome.argv)
        self.assertIn("KillMode=control-group", outcome.argv)
        self.assertTrue(outcome.timed_out)

    def test_timeout_stops_only_the_owned_unit(self) -> None:
        runner = SystemdRunner(max_rss_bytes=2 * 1024**3)
        systemctl_calls: list[list[str]] = []

        def fake_run(command, **kwargs):
            del kwargs
            systemctl_calls.append(list(command))
            from subprocess import CompletedProcess

            return CompletedProcess(command, 0, b"ActiveState=inactive\nLoadState=not-found\n", b"")

        with (
            mock.patch.object(SystemdRunner, "available", return_value=True),
            mock.patch.object(linux_module, "run_bounded", side_effect=_timed_out_outcome),
            mock.patch.object(linux_module.subprocess, "run", side_effect=fake_run),
        ):
            outcome = runner.run(["/helper"], timeout_seconds=5.0, env={})
        self.assertTrue(outcome.timed_out)
        # Exactly this opaque unit is stopped and killed; no other unit is named.
        self.assertTrue(all(call[-1].startswith("sightglass-whisper-") for call in systemctl_calls))
        self.assertTrue(any("stop" in call for call in systemctl_calls))
        self.assertTrue(any("kill" in call for call in systemctl_calls))
        units = {call[-1] for call in systemctl_calls}
        self.assertEqual(len(units), 1)
        self.assertTrue(all("--user" not in call for call in systemctl_calls))
        self.assertTrue(all(call[-1].endswith(".scope") for call in systemctl_calls))

    def test_stream_limit_stops_only_the_owned_unit(self) -> None:
        runner = SystemdRunner(max_rss_bytes=2 * 1024**3)
        systemctl_calls: list[list[str]] = []

        def limited_outcome(argv, **kwargs):
            from sightglass.voice.subprocess import ProcessOutcome

            return ProcessOutcome(
                argv=tuple(str(v) for v in argv),
                exit_code=0,
                signal_number=None,
                stdout=b"",
                stderr=b"",
                timed_out=False,
                limit_exceeded="stdout",
            )

        def fake_run(command, **kwargs):
            del kwargs
            systemctl_calls.append(list(command))
            from subprocess import CompletedProcess

            return CompletedProcess(command, 0, b"ActiveState=inactive\nLoadState=not-found\n", b"")

        with (
            mock.patch.object(SystemdRunner, "available", return_value=True),
            mock.patch.object(linux_module, "run_bounded", side_effect=limited_outcome),
            mock.patch.object(linux_module.subprocess, "run", side_effect=fake_run),
        ):
            outcome = runner.run(["/helper"], timeout_seconds=5.0, env={})
        self.assertEqual(outcome.limit_exceeded, "stdout")
        self.assertTrue(systemctl_calls)

    def test_spawn_failure_still_reaps_the_owned_unit(self) -> None:
        from sightglass.voice.subprocess import BoundedProcessError

        runner = SystemdRunner(max_rss_bytes=2 * 1024**3)
        systemctl_calls: list[list[str]] = []

        def fake_run(command, **kwargs):
            del kwargs
            systemctl_calls.append(list(command))
            from subprocess import CompletedProcess

            return CompletedProcess(command, 0, b"ActiveState=inactive\nLoadState=not-found\n", b"")

        with (
            mock.patch.object(SystemdRunner, "available", return_value=True),
            mock.patch.object(
                linux_module, "run_bounded", side_effect=BoundedProcessError("synthetic")
            ),
            mock.patch.object(linux_module.subprocess, "run", side_effect=fake_run),
        ):
            with self.assertRaises(SightglassError) as caught:
                runner.run(["/helper"], timeout_seconds=5.0, env={})
        self.assertEqual(caught.exception.details["reason"], "helper_runner_unavailable")
        self.assertTrue(systemctl_calls)

    def test_successful_run_does_not_touch_any_unit(self) -> None:
        runner = SystemdRunner(max_rss_bytes=2 * 1024**3)

        with (
            mock.patch.object(SystemdRunner, "available", return_value=True),
            mock.patch.object(
                linux_module, "run_bounded", side_effect=lambda *a, **k: _ok_outcome(*a, **k)
            ),
            mock.patch.object(linux_module.subprocess, "run") as systemctl,
        ):
            runner.run(["/helper"], timeout_seconds=5.0, env={})
        systemctl.assert_not_called()

    def test_cleanup_cannot_report_completion_while_the_scope_is_active(self) -> None:
        runner = SystemdRunner(max_rss_bytes=2 * 1024**3)
        with (
            mock.patch.object(linux_module, "run_bounded", side_effect=_timed_out_outcome),
            mock.patch.object(
                linux_module.subprocess,
                "run",
                return_value=linux_module.subprocess.CompletedProcess(
                    [], 0, b"ActiveState=active\nLoadState=loaded\n", b""
                ),
            ),
        ):
            with self.assertRaises(SightglassError) as caught:
                runner.run(["/helper"], timeout_seconds=0.2, env={})
        self.assertEqual(caught.exception.details["reason"], "helper_scope_cleanup_failed")

    def test_remaining_deadline_bounds_the_inner_watchdog(self) -> None:
        helper = LinuxWhisperHelper(
            Path("/synthetic/helper"), language="zh-CN", timeout_seconds=600, runner=DirectRunner()
        )
        command = helper._command(7, timeout_seconds=0.5)
        value = float(command[command.index("--helper-timeout-seconds") + 1])
        self.assertGreater(value, 0)
        self.assertLess(value, 0.5)

    def test_production_runner_is_always_the_isolated_systemd_runner(self) -> None:
        helper = mock.Mock()
        helper.max_rss_bytes = DEFAULT_MAX_HELPER_RSS_BYTES
        runner = production_runner(helper)
        self.assertIsInstance(runner, SystemdRunner)
        self.assertTrue(runner.isolates)

    def test_production_runner_never_falls_back_to_direct(self) -> None:
        helper = mock.Mock()
        helper.max_rss_bytes = DEFAULT_MAX_HELPER_RSS_BYTES
        with mock.patch.object(SystemdRunner, "available", return_value=False):
            runner = production_runner(helper)
            self.assertFalse(runner.available())
        # A missing systemd scope must not silently degrade to a non-isolated runner.
        self.assertIsInstance(runner, SystemdRunner)

    def test_production_runner_requires_a_memory_bound(self) -> None:
        helper = mock.Mock()
        helper.max_rss_bytes = None
        with self.assertRaises(SightglassError) as caught:
            production_runner(helper)
        self.assertEqual(caught.exception.details["reason"], "helper_memory_unbounded")

    def test_missing_systemd_blocks_the_job(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            silk = root / "audio.silk"
            silk.write_bytes(b"\x02#!SILK_V3" + bytes(range(64)))
            silk.chmod(0o600)
            path = write_stub_helper(root, "pass\n")
            model = write_model_binding(path)
            helper = LinuxWhisperHelper(
                path,
                language=LOCALE,
                model=ModelBinding(model, "ggml-small-q5_1", "q5_1"),
                timeout_seconds=10.0,
            )
            helper.runner = SystemdRunner(max_rss_bytes=DEFAULT_MAX_HELPER_RSS_BYTES)
            with mock.patch.object(SystemdRunner, "available", return_value=False):
                with self.assertRaises(SightglassError) as caught:
                    helper.transcribe_silk(silk)
            self.assertEqual(caught.exception.details["reason"], "helper_runner_unavailable")


class LinuxJobChildTests(unittest.TestCase):
    """Real end-to-end job: ``_linux_child`` decode+wrap+recognize, real SILK.

    The whisper wrapper is a stub with the production command line/report schema; the SILK
    decode and WAV wrap are the real ones.  These are synthetic plumbing tests, not real
    whisper.cpp verification.
    """

    def setUp(self) -> None:
        try:
            import pysilk  # noqa: F401
        except ImportError:
            self.skipTest("silk-python extra is not installed")
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.silk = self.root / "audio.silk"
        self.silk.write_bytes(real_silk_payload(0.3))
        self.silk.chmod(0o600)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _python_stub(self, source: str) -> Path:
        """A wrapper that runs ``source`` as Python with the caller's argv."""

        return write_stub_helper(self.root, source, name="sightglass-whisper")

    def _helper(self, whisper_source: str) -> LinuxWhisperHelper:
        helper_path = self._python_stub(whisper_source)
        model = write_model_binding(helper_path)
        return LinuxWhisperHelper(
            helper_path,
            language="zh-CN",
            model=ModelBinding(model, "ggml-small-q5_1", "q5_1"),
            timeout_seconds=30.0,
            runner=DirectRunner(),
        )

    def _report_source(self, text: str = " 合成语音 hello") -> str:
        report = json.dumps(
            {
                "schema": "sightglass.voice-transcript.v1",
                "backend": "whisper.cpp",
                "text": text,
                "language": "zh",
                "model": {"identifier": "ggml-small-q5_1"},
                "quantization": "q5_1",
                "segments": [{"text": text}],
            }
        )
        return f"print({report!r})\n"

    def test_whole_job_decodes_wraps_and_recognizes_in_one_child(self) -> None:
        helper = self._helper(self._report_source())
        transcript = helper.transcribe_silk(self.silk)
        self.assertEqual(transcript.text, " 合成语音 hello")
        self.assertEqual(transcript.decoder, "pysilk/0.2.8")
        self.assertEqual(transcript.decoder_envelope, "wechat_prefix")
        self.assertGreater(transcript.pcm_bytes, 0)
        self.assertGreater(transcript.pcm_frames, 0)
        # configured locale is preserved; the mapped whisper language is distinct.
        self.assertEqual(transcript.configured_locale, "zh-CN")
        self.assertEqual(transcript.language, "zh")
        provenance = transcript.provenance()
        self.assertEqual(provenance["locale"], "zh-CN")
        self.assertEqual(provenance["language"], "zh")
        self.assertEqual(provenance["decoder"], "pysilk/0.2.8")

    def test_wrapper_receives_a_wav_path(self) -> None:
        # The stub wrapper echoes its argv into the transcript text so the test can prove
        # whisper is handed a .wav path (the child wrapped the decoded PCM).
        source = (
            "import json, sys\n"
            "print(json.dumps({'schema': 'sightglass.voice-transcript.v1',"
            " 'text': ' '.join(sys.argv[1:]), 'language': 'zh'}))\n"
        )
        helper = self._helper(source)
        transcript = helper.transcribe_silk(self.silk)
        # The reported argv (space-joined) must name a .wav input and the fixed threads.
        self.assertIn("--pcm", transcript.text)
        self.assertIn(".wav", transcript.text)
        self.assertIn(f"--threads {HELPER_THREADS}", transcript.text)
        self.assertIn("--language zh", transcript.text)

    def test_missing_helper_blocks(self) -> None:
        helper_path = self._python_stub(self._report_source())
        model = write_model_binding(helper_path)
        helper_path.unlink()
        helper = LinuxWhisperHelper(
            helper_path,
            language="zh-CN",
            model=ModelBinding(model, "ggml-small-q5_1", "q5_1"),
            timeout_seconds=30.0,
            runner=DirectRunner(),
        )
        with self.assertRaises(SightglassError) as caught:
            helper.transcribe_silk(self.silk)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)

    def test_corrupt_silk_is_blocked_by_the_child(self) -> None:
        bad = self.root / "bad.silk"
        bad.write_bytes(b"not-silk-at-all")
        bad.chmod(0o600)
        helper = self._helper(self._report_source())
        with self.assertRaises(SightglassError) as caught:
            helper.transcribe_silk(bad)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)
        self.assertEqual(caught.exception.details["stage"], "recognize")

    def test_whisper_failure_is_retryable(self) -> None:
        helper = self._helper(
            "import json, sys\n"
            "sys.stderr.write(json.dumps({'error': 'transcription_failed'}) + '\\n')\n"
            "sys.exit(3)\n"
        )
        with self.assertRaises(SightglassError) as caught:
            helper.transcribe_silk(self.silk)
        self.assertEqual(caught.exception.code, ErrorCode.SERVICE_UNAVAILABLE)
        self.assertTrue(caught.exception.retryable)

    def test_whisper_timeout_is_retryable_timeout(self) -> None:
        helper = self._helper("import time\ntime.sleep(30)\n")
        helper.timeout_seconds = 1.0
        with self.assertRaises(SightglassError) as caught:
            helper.transcribe_silk(self.silk)
        self.assertEqual(caught.exception.code, ErrorCode.SERVICE_TIMEOUT)
        self.assertTrue(caught.exception.retryable)

    def test_orphaned_grandchild_is_reaped_after_a_successful_job(self) -> None:
        marker = self.root / "grandchild"
        report = json.dumps(
            {
                "schema": "sightglass.voice-transcript.v1",
                "text": "ok",
                "language": "en",
            }
        )
        # The wrapper forks a long-lived grandchild, records its pid, prints a valid
        # report and exits 0.  The bounded job must still reap the orphan it left behind.
        body = (
            "import os, time\n"
            "child = os.fork()\n"
            "if child == 0:\n"
            "    time.sleep(120)\n"
            "    os._exit(0)\n"
            f"open({str(marker)!r}, 'w').write(str(child))\n"
            f"print({report!r})\n"
        )
        helper = self._helper(body)
        helper.timeout_seconds = 2.0
        # The orphan inherits the wrapper's stdout pipe, so the bounded reader correctly
        # fails closed on the timeout; the same watchdog must reap the wrapper's whole
        # group (the orphan included).
        with self.assertRaises(SightglassError) as caught:
            helper.transcribe_silk(self.silk)
        self.assertIn(
            caught.exception.code, {ErrorCode.SERVICE_TIMEOUT, ErrorCode.SERVICE_UNAVAILABLE}
        )
        self.assertTrue(marker.exists())
        pid = int(marker.read_text())
        deadline = time.monotonic() + 5
        reaped = False
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                reaped = True
                break
            time.sleep(0.05)
        self.assertTrue(reaped, "orphaned grandchild survived a successful job")


class ModelBindingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_manifest_binding_is_read(self) -> None:
        helper = write_stub_helper(self.root, "pass\n")
        model = write_model_binding(
            helper, file_name="ggml-small-q8_0.bin", identifier="small-q8", quantization="q8_0"
        )
        binding = resolve_model_binding(helper)
        self.assertEqual(binding.path, model)
        self.assertEqual(binding.identifier, "small-q8")
        self.assertEqual(binding.quantization, "q8_0")

    def test_missing_manifest_is_unresolved_not_fabricated(self) -> None:
        helper = write_stub_helper(self.root, "pass\n")
        binding = resolve_model_binding(helper)
        self.assertIsNone(binding.path)
        self.assertEqual(binding.identifier, "")
        self.assertIsNone(binding.quantization)

    def test_probe_reports_unresolved_when_manifest_missing(self) -> None:
        helper = write_stub_helper(self.root, "pass\n")
        readiness = probe_helper(helper)
        self.assertEqual(readiness.blocked_reason, "model_unresolved")

    def test_probe_reports_unresolved_even_when_a_default_named_file_exists(self) -> None:
        # A file that happens to share the historical default name must not be adopted
        # when no manifest names it.
        helper = write_stub_helper(self.root, "pass\n")
        models = helper.parent / "models"
        models.mkdir(mode=0o700)
        (models / "ggml-small-q5_1.bin").write_bytes(b"stray")
        (models / "ggml-small-q5_1.bin").chmod(0o600)
        self.assertEqual(probe_helper(helper).blocked_reason, "model_unresolved")

    def test_corrupt_manifest_is_unresolved(self) -> None:
        helper = write_stub_helper(self.root, "pass\n")
        models = helper.parent / "models"
        models.mkdir(mode=0o700)
        (models / MODEL_MANIFEST_NAME).write_text("{not json")
        (models / MODEL_MANIFEST_NAME).chmod(0o600)
        self.assertIsNone(resolve_model_binding(helper).path)

    def test_wrong_schema_manifest_is_unresolved(self) -> None:
        helper = write_stub_helper(self.root, "pass\n")
        models = helper.parent / "models"
        models.mkdir(mode=0o700)
        (models / MODEL_MANIFEST_NAME).write_text(
            json.dumps({"schema": "other.v1", "file": "x.bin", "identifier": "x"})
        )
        (models / MODEL_MANIFEST_NAME).chmod(0o600)
        self.assertIsNone(resolve_model_binding(helper).path)

    def test_unsafe_manifest_file_values_are_unresolved(self) -> None:
        helper = write_stub_helper(self.root, "pass\n")
        models = helper.parent / "models"
        models.mkdir(mode=0o700)
        for unsafe in ("../escape.bin", "sub/dir.bin", "back\\slash.bin", ".", "..", "a\x00b"):
            with self.subTest(unsafe=unsafe):
                (models / MODEL_MANIFEST_NAME).write_text(
                    json.dumps(
                        {
                            "schema": "sightglass.voice-model.v1",
                            "file": unsafe,
                            "identifier": "x",
                            "quantization": "q5_1",
                        }
                    )
                )
                (models / MODEL_MANIFEST_NAME).chmod(0o600)
                self.assertIsNone(resolve_model_binding(helper).path)

    def test_symlinked_manifest_is_unresolved(self) -> None:
        helper = write_stub_helper(self.root, "pass\n")
        models = helper.parent / "models"
        models.mkdir(mode=0o700)
        real = self.root / "real-manifest.json"
        real.write_text(
            json.dumps({"schema": "sightglass.voice-model.v1", "file": "m.bin", "identifier": "m"})
        )
        real.chmod(0o600)
        (models / MODEL_MANIFEST_NAME).symlink_to(real)
        self.assertIsNone(resolve_model_binding(helper).path)

    def test_oversize_manifest_is_unresolved(self) -> None:
        helper = write_stub_helper(self.root, "pass\n")
        models = helper.parent / "models"
        models.mkdir(mode=0o700)
        (models / MODEL_MANIFEST_NAME).write_bytes(b"{" + b" " * (32 * 1024) + b"}")
        (models / MODEL_MANIFEST_NAME).chmod(0o600)
        self.assertIsNone(resolve_model_binding(helper).path)

    def test_probe_reports_ready_with_model(self) -> None:
        helper = write_stub_helper(self.root, "pass\n")
        model = write_model_binding(helper)
        readiness = probe_helper(helper, model=ModelBinding(model, "small", "q5_1"))
        self.assertTrue(readiness.present)
        self.assertIsNone(readiness.blocked_reason)


class _StubCapture:
    def __init__(self, staging: Path, silk: bytes) -> None:
        self.staging = staging
        self.silk = silk

    def capture(self, job: dict[str, Any]) -> CapturedVoice:
        path = self.staging / f"{job['job_id']}.silk"
        path.write_bytes(self.silk)
        path.chmod(0o600)
        return CapturedVoice(
            job_id=str(job["job_id"]),
            resource_id=str(job["resource_id"]),
            message_id=str(job["message_id"]),
            resource_revision=str(job["resource_revision"]),
            account_id=str(job["account_id"]),
            account_binding_id=None,
            input_digest="0" * 64,
            byte_size=len(self.silk),
            silk_path=path,
        )

    def staging_path(self, job_id: str, suffix: str) -> Path:
        return self.staging / f"{job_id}.{suffix}"


class LinuxTranscriberProvenanceTests(unittest.TestCase):
    """Transcriber provenance + staging cleanup through the stub child runner."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.silk = b"\x02#!SILK_V3" + bytes(range(32))

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _transcriber(self, runner: _StubChildRunner) -> LinuxSilkTranscriber:
        helper_path = write_stub_helper(self.root, "pass\n")
        model = write_model_binding(helper_path)
        return LinuxSilkTranscriber(
            capture=_StubCapture(self.root, self.silk),  # type: ignore[arg-type]
            helper=LinuxWhisperHelper(
                helper_path,
                language=LOCALE,
                model=ModelBinding(model, "ggml-small-q5_1", "q5_1"),
                timeout_seconds=10.0,
                runner=runner,
            ),
        )

    def test_provenance_and_staging_cleanup(self) -> None:
        runner = _StubChildRunner()
        runner.stdout = _child_report(text="transcribed")
        transcriber = self._transcriber(runner)
        result = transcriber.transcribe(
            {
                "job_id": "vjob_probe",
                "resource_id": "wxres_probe",
                "message_id": "msg_probe",
                "resource_revision": "fingerprint-1",
                "account_id": "synthetic-account",
            },
            duration_ms=620,
            deadline=None,
        )
        self.assertEqual(result.text, "transcribed")
        provenance = dict(result.provenance)
        self.assertEqual(provenance["schema"], "sightglass.voice-provenance.v1")
        self.assertEqual(provenance["recipe"]["engine"], "sightglass.voice.linux-whisper.v1")
        self.assertEqual(provenance["recipe"]["input_container"], "wav")
        self.assertEqual(provenance["recipe"]["pcm"], pcm_recipe())
        self.assertEqual(provenance["recipe"]["decoder"], "pysilk/0.2.8")
        self.assertEqual(provenance["input"]["input_digest"], "0" * 64)
        self.assertEqual(provenance["input"]["silk_bytes"], len(self.silk))
        self.assertEqual(provenance["input"]["pcm_bytes"], 3200)
        self.assertEqual(provenance["input"]["declared_duration_ms"], 620)
        self.assertEqual(provenance["recognizer"]["backend"], "whisper.cpp")
        self.assertEqual(provenance["recognizer"]["quantization"], "q5_1")
        self.assertEqual(provenance["recognizer"]["isolation"], "none")
        self.assertEqual(provenance["recognizer"]["locale"], LOCALE)
        self.assertEqual(provenance["recognizer"]["language"], "en")
        self.assertEqual(
            provenance["derived"], {"kind": "derived_transcript", "translation": False}
        )
        # The staged SILK file is removed on completion.
        self.assertEqual(list(self.root.glob("vjob_probe.*")), [])

    def test_failed_child_still_cleans_staging(self) -> None:
        runner = _StubChildRunner()
        runner.exit_code = 5
        runner.stderr = b'{"error":"decode_failed"}\n'
        transcriber = self._transcriber(runner)
        with self.assertRaises(SightglassError):
            transcriber.transcribe(
                {
                    "job_id": "vjob_probe",
                    "resource_id": "wxres_probe",
                    "message_id": "msg_probe",
                    "resource_revision": "fingerprint-1",
                    "account_id": "synthetic-account",
                },
                duration_ms=0,
                deadline=None,
            )
        self.assertEqual(list(self.root.glob("vjob_probe.*")), [])

    def test_apple_engine_never_confused_with_linux(self) -> None:
        self.assertNotEqual(LinuxSilkTranscriber.RECIPE_ENGINE, "sightglass.voice.apple-silk.v1")


class BuildScriptTests(unittest.TestCase):
    """End-to-end coverage of the real build script against a stub whisper-cli.

    These exercise the wrapper contract without whisper.cpp or a real model: a fake
    ``whisper-cli`` reproduces the documented behaviour that ``--output-file X`` writes
    ``X.json``, which the old wrapper read by the wrong name.
    """

    def setUp(self) -> None:
        import shutil

        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.script = (
            Path(__file__).resolve().parents[2] / "scripts" / "build-linux-voice-helper.sh"
        )
        if not self.script.exists() or not shutil.which("bash"):
            self.skipTest("bash or build script unavailable")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _fake_whisper(self) -> Path:
        bin_dir = self.root / "bin"
        bin_dir.mkdir(mode=0o700, exist_ok=True)
        cli = bin_dir / "whisper-cli"
        cli.write_text(
            "#!/bin/sh\n"
            'if [ "$1" = "--version" ]; then echo "whisper.cpp 1.7.4 (synthetic)"; exit 0; fi\n'
            'out=""\n'
            'while [ "$#" -gt 0 ]; do\n'
            '  case "$1" in\n'
            '    --output-file) out="$2"; shift 2 ;;\n'
            "    --model|--language|--threads) shift 2 ;;\n"
            "    *) shift ;;\n"
            "  esac\n"
            "done\n"
            'printf "%s" \'{"transcription":[{"text":" 合成 "},{"text":"hello"}],'
            '"model":{"type":"small"}}\' > "${out}.json"\n'
            "exit 0\n"
        )
        cli.chmod(0o700)
        return cli

    def test_build_produces_wrapper_manifest_and_runs(self) -> None:
        import subprocess

        cli = self._fake_whisper()
        model = self.root / "ggml-small-q5_1.bin"
        model.write_bytes(b"synthetic-model")
        helper = self.root / "voice" / "sightglass-whisper"
        completed = subprocess.run(
            [
                "bash",
                str(self.script),
                "--whisper-cli",
                str(cli),
                "--model",
                str(model),
                "--source-tag",
                "v1.9.5",
                "--output",
                str(helper),
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertTrue(helper.exists())

        models_dir = helper.parent / "models"
        manifest = json.loads((models_dir / "model.json").read_text())
        self.assertEqual(manifest["schema"], "sightglass.voice-model.v1")
        self.assertEqual(manifest["file"], "ggml-small-q5_1.bin")
        self.assertEqual(manifest["quantization"], "q5_1")
        self.assertTrue((models_dir / "ggml-small-q5_1.bin").exists())
        # A real SHA, the observed runtime version and the operator source tag are
        # recorded separately, never fabricated.
        self.assertEqual(len(manifest["model_sha256"]), 64)
        self.assertIn("1.7.4", manifest["whisper_version"])
        self.assertEqual(manifest["whisper_source_tag"], "v1.9.5")

        readme_receipt = json.loads((helper.parent / "helper-build.json").read_text())
        self.assertEqual(readme_receipt["schema"], "sightglass.voice-helper-build.v1")
        self.assertFalse(readme_receipt["network_fetch"])
        self.assertEqual(len(readme_receipt["cli_sha256"]), 64)
        self.assertEqual(readme_receipt["whisper_source_tag"], "v1.9.5")

        pcm = self.root / "input.pcm"
        pcm.write_bytes(struct.pack("<1600h", *([0] * 1600)))
        wav = self.root / "input.wav"
        write_wav(pcm, wav, max_bytes=1024 * 1024)

        # Point TMPDIR at a private dir so the wrapper's per-job directory is observable
        # and must be gone after a successful run.
        job_tmp = self.root / "job-tmp"
        job_tmp.mkdir(mode=0o700)
        environment = dict(os.environ, TMPDIR=str(job_tmp))
        # The build script/wrapper is Linux production code; ``ulimit -v`` is not
        # settable on the macOS dev host, so the memory guard is asserted separately and
        # this plumbing check runs without a limit (the daemon supplies one in production
        # through the cgroup runner regardless).
        run = subprocess.run(
            [
                str(helper),
                "--pcm",
                str(wav),
                "--language",
                "zh",
                "--threads",
                "2",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            env=environment,
        )
        self.assertEqual(run.returncode, 0, run.stderr)
        report = json.loads(run.stdout.strip())
        self.assertEqual(report["schema"], "sightglass.voice-transcript.v1")
        self.assertEqual(report["backend"], "whisper.cpp")
        self.assertEqual(report["text"], "合成 hello")
        # The observed --version line is preserved verbatim; the source tag is separate.
        self.assertIn("1.7.4", report["helper_version"])
        self.assertEqual(report["whisper_source_tag"], "v1.9.5")
        self.assertEqual(report["quantization"], "q5_1")
        # The exact manifest identifier is used, never whisper's payload model.type.
        self.assertEqual(report["model"], {"identifier": "ggml-small-q5_1"})
        # The wrapper removes only its own per-job directory on exit (never other
        # files a shell may drop in TMPDIR).
        self.assertEqual(list(job_tmp.glob("sightglass-whisper.*")), [])
        # The build ran the real wrapper (a private per-job JSON, not the mktemp bug),
        # so the reference recorded the produced file, not an empty placeholder.
        self.assertEqual(report["threads"], 2)

    def test_build_rejects_zero_threads_and_unsafe_model_name(self) -> None:
        import subprocess

        cli = self._fake_whisper()
        model = self.root / "ggml-small-q5_1.bin"
        model.write_bytes(b"m")
        helper = self.root / "voice" / "sightglass-whisper"
        zero = subprocess.run(
            [
                "bash",
                str(self.script),
                "--whisper-cli",
                str(cli),
                "--model",
                str(model),
                "--output",
                str(helper),
                "--threads",
                "0",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertNotEqual(zero.returncode, 0)
        self.assertFalse(helper.exists())

        unsafe = self.root / "bad name.bin"
        unsafe.write_bytes(b"m")
        rejected = subprocess.run(
            [
                "bash",
                str(self.script),
                "--whisper-cli",
                str(cli),
                "--model",
                str(unsafe),
                "--output",
                str(helper),
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertNotEqual(rejected.returncode, 0)
        self.assertFalse(helper.exists())

    def test_wrapper_projects_safe_json_for_a_metacharacter_version(self) -> None:
        import subprocess

        cli = self.root / "bin" / "whisper-cli"
        cli.parent.mkdir(mode=0o700, exist_ok=True)
        # A version string carrying shell/JSON metacharacters must not corrupt the report.
        cli.write_text(
            "#!/bin/sh\n"
            'if [ "$1" = "--version" ]; then '
            "printf '%s\\n' '1.9.5;$(touch /tmp/pwned)'; exit 0; fi\n"
            'out=""\n'
            'while [ "$#" -gt 0 ]; do case "$1" in --output-file) out="$2"; shift 2;; '
            "--model|--language|--threads) shift 2;; *) shift;; esac; done\n"
            'printf "%s" \'{"transcription":[{"text":"ok"}],'
            '"model":{"type":"small"}}\' > "${out}.json"\n'
            "exit 0\n"
        )
        cli.chmod(0o700)
        model = self.root / "ggml-small-q5_1.bin"
        model.write_bytes(b"m")
        helper = self.root / "voice" / "sightglass-whisper"
        subprocess.run(
            [
                "bash",
                str(self.script),
                "--whisper-cli",
                str(cli),
                "--model",
                str(model),
                "--output",
                str(helper),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
        pcm = self.root / "input.pcm"
        pcm.write_bytes(struct.pack("<1600h", *([0] * 1600)))
        wav = self.root / "input.wav"
        write_wav(pcm, wav, max_bytes=1024 * 1024)
        run = subprocess.run(
            [str(helper), "--pcm", str(wav), "--language", "en"],
            capture_output=True,
            text=True,
            timeout=60,
            env=dict(os.environ, TMPDIR=str(self.root)),
        )
        self.assertEqual(run.returncode, 0, run.stderr)
        report = json.loads(run.stdout.strip())
        self.assertEqual(report["text"], "ok")
        # The version is preserved as data and the report is valid JSON; the command
        # substitution never executed (no command-substitution artifact file).
        self.assertIn("1.9.5", report["helper_version"])
        self.assertFalse(Path("/tmp/pwned").exists())

    def test_wrapper_fails_closed_when_limit_cannot_be_set(self) -> None:
        import subprocess

        cli = self._fake_whisper()
        model = self.root / "ggml-small-q5_1.bin"
        model.write_bytes(b"synthetic-model")
        helper = self.root / "voice" / "sightglass-whisper"
        subprocess.run(
            [
                "bash",
                str(self.script),
                "--whisper-cli",
                str(cli),
                "--model",
                str(model),
                "--output",
                str(helper),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
        pcm = self.root / "input.pcm"
        pcm.write_bytes(struct.pack("<1600h", *([0] * 1600)))
        wav = self.root / "input.wav"
        write_wav(pcm, wav, max_bytes=1024 * 1024)
        environment = dict(os.environ, TMPDIR=str(self.root / "job-tmp"))
        (self.root / "job-tmp").mkdir(mode=0o700)
        # Force a limit that cannot be honored; the wrapper must not fall through and run
        # whisper unbounded.  On Linux this path is reachable when the runner asks for a
        # limit the shell cannot set.
        run = subprocess.run(
            [str(helper), "--pcm", str(wav), "--language", "en", "--max-rss-bytes", "1"],
            capture_output=True,
            text=True,
            timeout=60,
            env=environment,
        )
        self.assertNotEqual(run.returncode, 0)


class VoiceSetupPlatformTests(unittest.TestCase):
    """Readiness must reflect the real platform contract, not a hopeful default."""

    def setUp(self) -> None:
        from dataclasses import replace as _replace

        from sightglass.runtime.config import SightglassConfig

        self._replace = _replace
        self._config_cls = SightglassConfig
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    class _Repo:
        database = mock.Mock(storage=None)

    class _Service:
        def __init__(self) -> None:
            self.repository = VoiceSetupPlatformTests._Repo()
            self.resource_service = object()

    def _setup(self, helper: Path, *, systemd: bool, language: str = "zh-CN"):
        from sightglass.runtime import voice_setup

        config = self._replace(
            self._config_cls.create(self.root, self.root),
            voice_enabled=True,
            voice_language=language,
            voice_helper_path=str(helper),
        )
        with (
            mock.patch.object(voice_setup, "_is_linux", return_value=True),
            mock.patch.object(voice_setup, "production_runner_available", return_value=systemd),
            mock.patch.object(voice_setup.SilkDecoder, "available", staticmethod(lambda: True)),
            mock.patch.object(voice_setup.SilkDecoder, "version", staticmethod(lambda: "0.2.8")),
        ):
            return voice_setup.build_voice_setup(config, self._Service())

    def test_ready_selects_linux_transcriber_and_preserves_locale(self) -> None:
        helper = write_stub_helper(self.root, "print('{}')\n")
        write_model_binding(helper)
        setup = self._setup(helper, systemd=True)
        self.assertTrue(setup.readiness["ready"])
        self.assertEqual(setup.readiness["transcriber"], "linux-whisper")
        self.assertEqual(setup.readiness["helper"]["name"], "sightglass-whisper")
        assert setup.transcriber is not None
        self.assertEqual(setup.transcriber.helper.language, "zh-CN")  # type: ignore[attr-defined]

    def test_missing_systemd_is_blocked_not_ready(self) -> None:
        helper = write_stub_helper(self.root, "print('{}')\n")
        write_model_binding(helper)
        setup = self._setup(helper, systemd=False)
        self.assertFalse(setup.readiness["ready"])
        self.assertEqual(setup.blocked_reason, "helper_runner_unavailable")
        self.assertIsNone(setup.transcriber)

    def test_missing_manifest_is_blocked_not_ready(self) -> None:
        helper = write_stub_helper(self.root, "print('{}')\n")
        setup = self._setup(helper, systemd=True)
        self.assertFalse(setup.readiness["ready"])
        self.assertEqual(setup.blocked_reason, "model_unresolved")
        self.assertIsNone(setup.transcriber)

    def test_no_path_leaks_into_readiness(self) -> None:
        helper = write_stub_helper(self.root, "print('{}')\n")
        write_model_binding(helper)
        setup = self._setup(helper, systemd=True)
        self.assertNotIn(str(self.root), json.dumps(setup.readiness))


if __name__ == "__main__":
    unittest.main()

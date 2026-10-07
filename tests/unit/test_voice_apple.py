from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.voice.apple import (
    AppleSilkTranscriber,
    AppleSpeechHelper,
    helper_environment,
    probe_helper,
)
from sightglass.voice.capture import CapturedVoice
from sightglass.voice.decoder import PCM_SAMPLE_RATE, DecodedAudio, pcm_recipe
from sightglass.voice.subprocess import BoundedProcessError
from tests.fixtures.voice_helper import (
    helper_exit_body,
    helper_report,
    write_stub_helper,
)

LOCALE = "en-US"


def report(*, text: str = "hello there", **overrides: Any) -> str:
    return helper_report(text=text, **overrides)


class HelperScriptTests(unittest.TestCase):
    """Drive the real child-process path with a stand-in helper executable."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.pcm = self.root / "audio.pcm"
        self.pcm.write_bytes(b"\x00\x00" * 1600)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def helper(self, body: str, *, mode: int = 0o700, name: str = "helper") -> AppleSpeechHelper:
        return AppleSpeechHelper(
            write_stub_helper(self.root, body, name=name, mode=mode),
            locale=LOCALE,
            timeout_seconds=10.0,
        )

    def assertBlocked(self, helper: AppleSpeechHelper, reason: str) -> SightglassError:
        with self.assertRaises(SightglassError) as caught:
            helper.transcribe_pcm(self.pcm)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)
        self.assertEqual(caught.exception.details["reason"], reason)
        self.assertEqual(caught.exception.details["stage"], "recognize")
        self.assertFalse(caught.exception.retryable)
        return caught.exception

    def test_success_parses_one_json_line(self) -> None:
        helper = self.helper(f"print({report()!r})")
        transcript = helper.transcribe_pcm(self.pcm)
        self.assertEqual(transcript.text, "hello there")
        self.assertEqual(transcript.locale, LOCALE)
        self.assertEqual(transcript.backend, "SpeechAnalyzer+SpeechTranscriber")
        self.assertEqual(transcript.helper_version, "1")
        self.assertEqual(transcript.segments, 1)
        self.assertEqual(transcript.model, {"identifier": "unknown"})
        provenance = transcript.provenance()
        self.assertTrue(provenance["volatile_excluded"])
        self.assertEqual(provenance["helper"], "sightglass-transcribe")

    def test_helper_receives_the_pcm_path_and_an_explicit_locale(self) -> None:
        body = (
            "import json, sys\n"
            "print(json.dumps({'schema': 'sightglass.voice-transcript.v1',"
            " 'text': ' '.join(sys.argv[1:]), 'locale': 'en-US'}))\n"
        )
        helper = self.helper(body)
        transcript = helper.transcribe_pcm(self.pcm)
        self.assertEqual(transcript.text, f"--pcm {self.pcm} --locale {LOCALE}")

    def test_extra_output_after_the_report_is_ignored_when_short(self) -> None:
        helper = self.helper(f"print('noise')\nprint({report()!r})")
        self.assertBlocked(helper, "helper_report_invalid")

    def test_model_unavailable_maps_to_blocked(self) -> None:
        self.assertBlocked(
            self.helper(helper_exit_body(2, error="not_installed")), "not_installed"
        )

    def test_model_unavailable_without_a_reason_is_blocked(self) -> None:
        self.assertBlocked(
            self.helper(helper_exit_body(2)), "model_not_installed"
        )

    def test_usage_error_maps_to_blocked(self) -> None:
        self.assertBlocked(self.helper(helper_exit_body(4, error="usage")), "usage")

    def test_transcription_failure_stays_retryable(self) -> None:
        with self.assertRaises(SightglassError) as caught:
            self.helper(helper_exit_body(3, error="transcription_failed")).transcribe_pcm(
                self.pcm
            )
        self.assertEqual(caught.exception.code, ErrorCode.SERVICE_UNAVAILABLE)
        self.assertTrue(caught.exception.retryable)
        self.assertEqual(caught.exception.details["reason"], "transcription_failed")

    def test_malformed_report_is_blocked(self) -> None:
        self.assertBlocked(self.helper("print('not json')\n"), "helper_report_invalid")

    def test_wrong_schema_is_blocked(self) -> None:
        payload = json.loads(report())
        payload["schema"] = "other.schema.v9"
        self.assertBlocked(
            self.helper(f"print({json.dumps(payload)!r})"), "helper_report_invalid"
        )

    def test_oversized_transcript_is_blocked(self) -> None:
        self.assertBlocked(
            self.helper(f"print({report(text='x' * 200_001)!r})"), "helper_text_too_long"
        )

    def test_stdout_limit_is_enforced(self) -> None:
        self.assertBlocked(
            self.helper("import sys\nsys.stdout.write('x' * (3 * 1024 * 1024))\n"),
            "helper_stdout_limit",
        )

    def test_timeout_kills_the_helper_group(self) -> None:
        marker = self.root / "helper-pids"
        body = (
            "import os, time\n"
            "child = os.fork()\n"
            "if child == 0:\n"
            "    time.sleep(120)\n"
            "    os._exit(0)\n"
            f"open({str(marker)!r}, 'w').write(f'{{os.getpid()}} {{child}}')\n"
            "time.sleep(120)\n"
        )
        helper = self.helper(body)
        # This checks group cleanup after the fork. Allow interpreter startup on a
        # loaded host so the watchdog does not fire before the child even exists.
        helper.timeout_seconds = 5.0
        with self.assertRaises(SightglassError) as caught:
            helper.transcribe_pcm(self.pcm)
        self.assertEqual(caught.exception.code, ErrorCode.SERVICE_TIMEOUT)
        self.assertTrue(caught.exception.retryable)
        self.assertEqual(caught.exception.details["reason"], "helper_timeout")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not marker.exists():
            time.sleep(0.05)
        for pid in (int(value) for value in marker.read_text().split()):
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)

    def test_spawn_failure_is_blocked(self) -> None:
        helper = self.helper(f"print({report()!r})")
        with mock.patch(
            "sightglass.voice.apple.run_bounded",
            side_effect=BoundedProcessError("synthetic spawn failure"),
        ):
            self.assertBlocked(helper, "helper_spawn_failed")

    def test_expired_deadline_never_starts_a_helper(self) -> None:
        helper = self.helper(f"print({report()!r})")
        with self.assertRaises(SightglassError) as caught:
            helper.transcribe_pcm(self.pcm, deadline=time.monotonic() - 1)
        self.assertEqual(caught.exception.code, ErrorCode.SERVICE_TIMEOUT)
        self.assertEqual(caught.exception.details["reason"], "operation_deadline")

    def test_helper_environment_has_no_credential_shaped_names(self) -> None:
        body = (
            "import json, os\n"
            "print(json.dumps({'schema': 'sightglass.voice-transcript.v1',"
            " 'text': ','.join(sorted(os.environ)), 'locale': 'en-US'}))\n"
        )
        helper = self.helper(body)
        with mock.patch.dict(
            os.environ,
            {"SIGHTGLASS_TOKEN": "synthetic", "WECHAT_KEY": "synthetic", "LANG": "en_US.UTF-8"},
        ):
            transcript = helper.transcribe_pcm(self.pcm)
        names = set(transcript.text.split(","))
        self.assertIn("LANG", names)
        self.assertNotIn("SIGHTGLASS_TOKEN", names)
        self.assertNotIn("WECHAT_KEY", names)

    def test_helper_missing_after_probe_is_blocked(self) -> None:
        helper = self.helper(f"print({report()!r})")
        helper.helper_path.unlink()
        self.assertBlocked(helper, "helper_missing")


class HelperEnvironmentTests(unittest.TestCase):
    def test_allowlist_and_deny_markers(self) -> None:
        source = {
            "HOME": "/Users/synthetic",
            "PATH": "/usr/bin",
            "LANG": "en_US.UTF-8",
            "TMPDIR": "/tmp",
            "USER": "synthetic",
            "OPENAI_API_KEY": "synthetic",
            "AWS_SECRET_ACCESS_KEY": "synthetic",
            "SIGHTGLASS_PASSWORD": "synthetic",
            "LD_PRELOAD": "/synthetic/lib.dylib",
            "PWD": "/synthetic",
        }
        filtered = helper_environment(source)
        self.assertEqual(
            filtered,
            {
                "HOME": "/Users/synthetic",
                "PATH": "/usr/bin",
                "LANG": "en_US.UTF-8",
                "TMPDIR": "/tmp",
                "USER": "synthetic",
            },
        )


class ProbeHelperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_unconfigured_helper(self) -> None:
        readiness = probe_helper(None)
        self.assertFalse(readiness.present)
        self.assertEqual(readiness.blocked_reason, "helper_not_configured")

    def test_missing_helper(self) -> None:
        self.assertEqual(
            probe_helper(self.root / "absent").blocked_reason, "helper_missing"
        )

    def test_directory_is_not_a_helper(self) -> None:
        path = self.root / "dir"
        path.mkdir(mode=0o700)
        self.assertEqual(probe_helper(path).blocked_reason, "helper_not_regular")

    def test_symlink_is_not_a_helper(self) -> None:
        target = self.root / "real"
        target.write_text("#!/bin/sh\n")
        target.chmod(0o700)
        link = self.root / "link"
        link.symlink_to(target)
        self.assertEqual(probe_helper(link).blocked_reason, "helper_not_regular")

    def test_hardlink_is_not_a_helper(self) -> None:
        target = self.root / "real"
        target.write_text("#!/bin/sh\n")
        target.chmod(0o700)
        os.link(target, self.root / "second")
        self.assertEqual(probe_helper(target).blocked_reason, "helper_not_regular")

    def test_empty_helper(self) -> None:
        path = self.root / "empty"
        path.touch(mode=0o700)
        self.assertEqual(probe_helper(path).blocked_reason, "helper_empty")

    def test_non_executable_helper(self) -> None:
        path = self.root / "plain"
        path.write_text("#!/bin/sh\n")
        path.chmod(0o600)
        self.assertEqual(probe_helper(path).blocked_reason, "helper_not_executable")

    def test_ready_helper_reports_no_path(self) -> None:
        path = self.root / "ready"
        path.write_text("#!/bin/sh\n")
        path.chmod(0o700)
        readiness = probe_helper(path)
        self.assertTrue(readiness.present)
        self.assertTrue(readiness.executable)
        self.assertIsNone(readiness.blocked_reason)
        self.assertNotIn(str(path), json.dumps(readiness.__dict__))


class StubCapture:
    def __init__(self, staging: Path, silk: bytes) -> None:
        self.staging = staging
        self.silk = silk
        self.released: list[Path] = []

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


class StubDecoder:
    def decode(self, silk_path: Path, pcm_path: Path, *, deadline: float | None = None):
        del deadline
        pcm_path.write_bytes(b"\x00\x00" * 8000)
        return DecodedAudio(
            path=pcm_path,
            byte_size=16_000,
            frames=8000,
            duration_ms=500,
            decoder="pysilk/0.2.8",
            envelope="wechat_prefix",
            recipe=pcm_recipe(),
        )


class TranscriberProvenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.silk = b"\x02#!SILK_V3" + bytes(range(32))

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_provenance_and_staging_cleanup(self) -> None:
        helper_path = write_stub_helper(
            self.root, f"print({report(text='transcribed')!r})\n"
        )
        capture = StubCapture(self.root, self.silk)
        transcriber = AppleSilkTranscriber(
            capture=capture,  # type: ignore[arg-type]
            decoder=StubDecoder(),  # type: ignore[arg-type]
            helper=AppleSpeechHelper(helper_path, locale=LOCALE, timeout_seconds=10.0),
        )
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
        self.assertEqual(provenance["recipe"]["engine"], "sightglass.voice.apple-silk.v1")
        self.assertEqual(provenance["recipe"]["decoder"], "pysilk/0.2.8")
        self.assertEqual(provenance["recipe"]["decoder_envelope"], "wechat_prefix")
        self.assertEqual(provenance["recipe"]["pcm"], pcm_recipe())
        self.assertEqual(provenance["recipe"]["pcm"]["sample_rate"], PCM_SAMPLE_RATE)
        self.assertEqual(provenance["input"]["resource_revision"], "fingerprint-1")
        self.assertEqual(provenance["input"]["input_digest"], "0" * 64)
        self.assertEqual(provenance["input"]["silk_bytes"], len(self.silk))
        self.assertEqual(provenance["input"]["declared_duration_ms"], 620)
        self.assertEqual(provenance["input"]["decoded_duration_ms"], 500)
        self.assertEqual(provenance["recognizer"]["locale"], LOCALE)
        self.assertTrue(provenance["recognizer"]["volatile_excluded"])
        self.assertEqual(
            provenance["derived"], {"kind": "derived_transcript", "translation": False}
        )
        self.assertEqual(list(self.root.glob("vjob_probe.*")), [])

    def test_decode_failure_still_cleans_the_silk_file(self) -> None:
        class FailingDecoder(StubDecoder):
            def decode(self, silk_path: Path, pcm_path: Path, *, deadline: float | None = None):
                del silk_path, pcm_path, deadline
                raise SightglassError(
                    ErrorCode.RESOURCE_BLOCKED, details={"reason": "decode_failed"}
                )

        helper_path = write_stub_helper(self.root, f"print({report()!r})\n")
        transcriber = AppleSilkTranscriber(
            capture=StubCapture(self.root, self.silk),  # type: ignore[arg-type]
            decoder=FailingDecoder(),  # type: ignore[arg-type]
            helper=AppleSpeechHelper(helper_path, locale=LOCALE, timeout_seconds=10.0),
        )
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


if __name__ == "__main__":
    unittest.main()

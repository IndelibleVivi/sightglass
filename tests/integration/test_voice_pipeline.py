"""End-to-end voice pipeline: real capture, real SILK decode, stubbed Apple helper.

The recognizer half is stubbed by an executable with the production command line, report
schema, and exit codes; capture, admission, the bounded decode child, the worker, the
commit path, and the reader's transcript surface are all the real ones.
"""

from __future__ import annotations

import hashlib
import io
import json
import math
import os
import struct
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sightglass.mcp.tools import ReaderTools
from sightglass.model.db import WindowDB
from sightglass.model.repositories import WindowRepository
from sightglass.policy.readers import ReaderContext, ReaderPolicy
from sightglass.reader.service import ReaderService
from sightglass.runtime.config import SightglassConfig
from sightglass.runtime.voice_setup import build_voice_setup
from sightglass.runtime.voice_worker import VoiceWorker
from sightglass.source.direct_wechat import DirectWeChatSourceProvider
from sightglass.source.identity import SignedTokenCodec
from sightglass.source.synthetic import create_synthetic_source
from sightglass.voice.apple import AppleSilkTranscriber, AppleSpeechHelper
from sightglass.voice.capture import (
    DEFAULT_STALE_STAGING_SECONDS,
    STAGING_DIRECTORY_NAME,
    VoiceCapture,
)
from sightglass.voice.decoder import PCM_SAMPLE_RATE, SilkDecoder
from sightglass.voice.repository import VoiceRepository
from sightglass.voice.service import VoiceService
from tests.fixtures.factory import enable_keep_residency
from tests.fixtures.voice_helper import (
    helper_exit_body,
    print_report_body,
    write_stub_helper,
)
from tests.fixtures.voice_source import declare_voice_messages, silk_payload

POLL_TIMEOUT_SECONDS = 20.0
TRANSCRIPT_TEXT = "Synthetic local transcript"
DECODER_AVAILABLE = SilkDecoder.available()
requires_decoder = unittest.skipUnless(
    DECODER_AVAILABLE, "silk-python voice extra is not installed"
)


class ProcessDied(BaseException):
    """Stands in for the daemon process disappearing mid-job."""


def real_silk(index: int) -> bytes:
    """A real SILK V3 stream carrying the WeChat ``\\x02`` envelope prefix."""

    import pysilk

    seconds = 0.4 + 0.1 * index
    frequency = 330.0 + 20.0 * index
    frames = int(seconds * PCM_SAMPLE_RATE)
    pcm = b"".join(
        struct.pack(
            "<h", int(9_000 * math.sin(2 * math.pi * frequency * position / PCM_SAMPLE_RATE))
        )
        for position in range(frames)
    )
    output = io.BytesIO()
    pysilk.encode(io.BytesIO(pcm), output, PCM_SAMPLE_RATE, 24_000)
    return output.getvalue()


class CrashingCapture(VoiceCapture):
    """Write the staging file, then die before the caller can own it."""

    def __init__(self, *args: Any, crash_stage: str, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.crash_stage = crash_stage

    def capture(self, job: Any) -> Any:
        captured = super().capture(job)
        if self.crash_stage == "capture":
            raise ProcessDied("capture")
        return captured


class CrashingDecoder(SilkDecoder):
    def __init__(self, *args: Any, crash_stage: str, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.crash_stage = crash_stage

    def decode(self, silk_path: Path, pcm_path: Path, *, deadline: float | None = None):
        decoded = super().decode(silk_path, pcm_path, deadline=deadline)
        if self.crash_stage == "decode":
            raise ProcessDied("decode")
        return decoded


class CrashingHelper:
    """Run the real helper, then die before the result can be committed."""

    def __init__(self, helper: AppleSpeechHelper, stage: str) -> None:
        self.helper = helper
        self.stage = stage

    def transcribe_pcm(self, pcm_path: Path, *, deadline: float | None = None):
        transcript = self.helper.transcribe_pcm(pcm_path, deadline=deadline)
        if self.stage == "recognize":
            raise ProcessDied("recognize")
        return transcript


@requires_decoder
class VoicePipelineFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source_root = self.root / "source"
        create_synthetic_source(self.source_root)
        self.now = datetime(2026, 9, 18, 12, 0, tzinfo=UTC)
        self.helper_path = write_stub_helper(self.root, print_report_body(text=TRANSCRIPT_TEXT))
        self.worker: VoiceWorker | None = None

    def tearDown(self) -> None:
        if self.worker is not None:
            self.worker.stop()
        self.temporary.cleanup()

    # -- stack -------------------------------------------------------------

    def build(
        self,
        *,
        count: int = 2,
        payload: Any = real_silk,
        helper_body: str | None = None,
        crash_stage: str | None = None,
    ) -> None:
        if helper_body is not None:
            self.helper_path = write_stub_helper(self.root, helper_body, name="helper")
        declare_voice_messages(self.source_root, count=count, payload=payload)
        provider = DirectWeChatSourceProvider(self.source_root)
        database = WindowDB(self.root / "state" / "window.db")
        enable_keep_residency(database)
        repository = WindowRepository(database)
        reader = ReaderContext(
            "codex", "Codex", ReaderPolicy(mode="all_except_denylist", identity_debug=True)
        )
        codec = SignedTokenCodec(hashlib.sha256(b"synthetic-voice-pipeline-secret").digest())
        self.voice_service = VoiceService(
            VoiceRepository(database), codec, clock=lambda: self.now
        )
        self.config = replace(
            SightglassConfig.create(self.root / "state", self.source_root),
            voice_enabled=True,
            voice_policy="auto",
            voice_language="en-US",
            voice_helper_path=str(self.helper_path),
            voice_helper_timeout_seconds=60,
        )
        self.service = ReaderService(
            provider,
            repository,
            reader,
            codec,
            voice_service=self.voice_service,
            voice_settings=self.config.voice_settings(),
        )
        self.tools = ReaderTools(self.service, voice_service=self.voice_service)
        self.group_id = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        self.setup = build_voice_setup(self.config, self.service)
        if crash_stage is None:
            self.transcriber: Any = self.setup.transcriber
        else:
            self.transcriber = self._crashing_transcriber(crash_stage)
        assert self.transcriber is not None, self.setup.readiness

    @property
    def staging_root(self) -> Path:
        return Path(self.config.data_dir) / STAGING_DIRECTORY_NAME

    def _crashing_transcriber(self, stage: str) -> AppleSilkTranscriber:
        return AppleSilkTranscriber(
            capture=CrashingCapture(
                self.service.resource_service,
                self.service.repository,
                self.staging_root,
                crash_stage=stage,
            ),
            decoder=CrashingDecoder(crash_stage=stage),
            helper=CrashingHelper(  # type: ignore[arg-type]
                AppleSpeechHelper(self.helper_path, locale="en-US", timeout_seconds=60.0),
                stage,
            ),
        )

    # -- helpers -----------------------------------------------------------

    def prepare_batch(self) -> dict[str, Any]:
        page = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, voice="auto"
        )
        self.assertEqual(page["voice"]["state"], "prepared")
        return page

    def start_worker(self, transcriber: Any = None) -> VoiceWorker:
        self.worker = VoiceWorker(
            self.voice_service,
            self.transcriber if transcriber is None else transcriber,
            poll_interval_seconds=0.05,
        )
        self.worker.start()
        return self.worker

    def jobs(self) -> list[dict[str, Any]]:
        return self.voice_service.repository.rows("SELECT * FROM voice_jobs ORDER BY created_at")

    def job_states(self) -> set[str]:
        return {str(job["state"]) for job in self.jobs()}

    def wait_until(self, predicate: Any, *, timeout: float = POLL_TIMEOUT_SECONDS) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.02)
        self.fail(f"condition not reached; job states={sorted(self.job_states())}")

    def staging_files(self) -> list[str]:
        root = self.staging_root
        if not root.is_dir():
            return []
        return sorted(entry.name for entry in root.iterdir())

    def stored_result(self, digest: str) -> dict[str, Any]:
        row = self.voice_service.repository.object(digest)
        assert row is not None, digest
        return json.loads(self.voice_service.object_store.read_binding(row).data)

    def assert_ready_with_transcripts(self) -> None:
        for job in self.jobs():
            self.assertEqual(str(job["state"]), "ready")
            self.assertIsNone(job["error_code"])
            value = self.stored_result(str(job["result_digest"]))
            self.assertEqual(value["text"], TRANSCRIPT_TEXT)
        self.assertEqual(self.staging_files(), [])


class VoicePipelineTests(VoicePipelineFixture):
    def test_pipeline_commits_a_derived_transcript_with_provenance(self) -> None:
        self.build()
        self.assertEqual(self.setup.readiness["transcriber"], "apple-silk")
        self.assertTrue(self.setup.readiness["ready"])
        self.assertNotIn(str(self.root), json.dumps(self.setup.readiness))
        self.prepare_batch()
        self.assertEqual(len(self.jobs()), 2)

        worker = self.start_worker()
        self.wait_until(lambda: self.job_states() == {"ready"})
        self.assertEqual(worker.status().completed_count, 2)
        self.assertEqual(worker.status().failed_count, 0)
        self.assertEqual(worker.status().blocked_count, 0)

        for job in self.jobs():
            self.assertEqual(int(job["attempt"]), 1)
            value = self.stored_result(str(job["result_digest"]))
            provenance = value["provenance"]
            self.assertEqual(provenance["schema"], "sightglass.voice-provenance.v1")
            self.assertEqual(provenance["recipe"]["engine"], "sightglass.voice.apple-silk.v1")
            self.assertTrue(provenance["recipe"]["decoder"].startswith("pysilk/"))
            self.assertEqual(provenance["recipe"]["decoder_envelope"], "wechat_prefix")
            self.assertEqual(provenance["recipe"]["pcm"]["sample_rate"], PCM_SAMPLE_RATE)
            self.assertEqual(provenance["recipe"]["pcm"]["sample_format"], "s16le")
            self.assertEqual(provenance["input"]["resource_revision"], job["resource_revision"])
            self.assertEqual(len(provenance["input"]["input_digest"]), 64)
            self.assertGreater(provenance["input"]["silk_bytes"], 0)
            self.assertGreater(provenance["input"]["pcm_bytes"], 0)
            self.assertEqual(provenance["input"]["declared_duration_ms"], 120_000)
            self.assertEqual(provenance["recognizer"]["locale"], "en-US")
            self.assertTrue(provenance["recognizer"]["volatile_excluded"])
            self.assertEqual(provenance["derived"]["translation"], False)
            self.assertNotIn(str(self.root), json.dumps(provenance))
        self.assertEqual(self.staging_files(), [])

    def test_cached_transcripts_are_served_from_the_committed_result(self) -> None:
        self.build()
        self.prepare_batch()
        self.start_worker()
        self.wait_until(lambda: self.job_states() == {"ready"})

        cached = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, voice="cached"
        )
        self.assertEqual(cached["voice"]["state"], "cached")
        texts = {str(row[3]) for row in cached["voice"]["items"]}
        self.assertEqual(texts, {TRANSCRIPT_TEXT})

    def test_undecodable_payload_is_blocked_and_never_retried(self) -> None:
        self.build(payload=silk_payload)
        self.prepare_batch()
        worker = self.start_worker()
        self.wait_until(lambda: self.job_states() == {"blocked"})
        status = worker.status()
        self.assertEqual(status.blocked_count, 2)
        self.assertEqual(status.failed_count, 0)
        self.assertEqual(status.retry_count, 0)
        self.assertEqual(status.historical_error_code, "RESOURCE_BLOCKED")
        for job in self.jobs():
            self.assertEqual(int(job["attempt"]), 1)
            self.assertEqual(str(job["error_code"]), "RESOURCE_BLOCKED")
            self.assertIsNone(job["result_digest"])
        time.sleep(0.4)
        self.assertEqual(self.job_states(), {"blocked"})
        self.assertEqual(self.staging_files(), [])

    def test_missing_speech_asset_blocks_without_retrying(self) -> None:
        self.build(helper_body=helper_exit_body(2, error="not_installed"))
        self.prepare_batch()
        worker = self.start_worker()
        self.wait_until(lambda: self.job_states() == {"blocked"})
        self.assertEqual(worker.status().blocked_count, 2)
        self.assertEqual(worker.status().retry_count, 0)
        self.assertEqual(self.staging_files(), [])

    def test_transient_helper_failure_is_retried_until_attempts_run_out(self) -> None:
        self.build(helper_body=helper_exit_body(3, error="transcription_failed"))
        self.prepare_batch()
        worker = self.start_worker()
        self.wait_until(lambda: self.job_states() == {"failed"}, timeout=30.0)
        self.assertEqual(worker.status().failed_count, 2)
        for job in self.jobs():
            self.assertEqual(int(job["attempt"]), 3)
            self.assertEqual(str(job["error_code"]), "SERVICE_UNAVAILABLE")
        self.assertEqual(self.staging_files(), [])

    def test_stale_staging_from_a_killed_process_is_swept(self) -> None:
        self.build()
        self.staging_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        leaked = self.staging_root / "vjob_leaked.silk"
        leaked.write_bytes(b"\x02#!SILK_V3stale")
        fresh = self.staging_root / "vjob_fresh.pcm"
        fresh.write_bytes(b"\x00")
        old = time.time() - (DEFAULT_STALE_STAGING_SECONDS + 60)
        os.utime(leaked, (old, old))
        self.assertEqual(self.setup.sweep_staging(), 1)
        self.assertFalse(leaked.exists())
        self.assertTrue(fresh.exists())


class VoicePipelineCrashTests(VoicePipelineFixture):
    """A process that dies mid-job must leave a recoverable lease and no orphan bytes."""

    def setUp(self) -> None:
        super().setUp()
        # The injectors raise a BaseException inside the worker thread on purpose.
        self._excepthook = threading.excepthook
        threading.excepthook = lambda _args: None

    def tearDown(self) -> None:
        threading.excepthook = self._excepthook
        super().tearDown()

    def assert_crash_converges(self, stage: str, *, leaked_after_crash: bool) -> None:
        self.build(crash_stage=stage)
        self.prepare_batch()
        worker = self.start_worker()
        self.wait_until(lambda: "leased" in self.job_states(), timeout=10.0)
        self.wait_until(lambda: not worker.status().running, timeout=10.0)
        self.assertNotIn("ready", self.job_states())
        self.assertEqual([job["result_digest"] for job in self.jobs()], [None] * len(self.jobs()))
        self.assertEqual(self.staging_files() != [], leaked_after_crash)

        worker.stop()
        self.worker = None
        self.now += timedelta(seconds=600)
        self.assertGreaterEqual(self.voice_service.recover_expired_leases(), 1)
        self.assertIn("pending", self.job_states())

        assert self.setup.transcriber is not None
        self.start_worker(self.setup.transcriber)
        self.wait_until(lambda: self.job_states() == {"ready"})
        self.assert_ready_with_transcripts()

    def test_crash_after_capture_converges(self) -> None:
        self.assert_crash_converges("capture", leaked_after_crash=True)

    def test_crash_after_decode_converges(self) -> None:
        self.assert_crash_converges("decode", leaked_after_crash=False)

    def test_crash_after_recognize_converges(self) -> None:
        self.assert_crash_converges("recognize", leaked_after_crash=False)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any

from sightglass.mcp.bridge import DaemonReaderTools
from sightglass.runtime.config import ConfigStore, SightglassConfig
from sightglass.runtime.daemon import SightglassDaemon
from sightglass.runtime.ipc import IPCClient
from sightglass.runtime.secrets import (
    OPERATOR_SECRET_ACCOUNT,
    READER_SECRET_ACCOUNT,
    MemorySecretStore,
    token_hash,
)
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.voice_source import declare_voice_messages

POLL_TIMEOUT_SECONDS = 5.0
VOICE_TEXT = "Synthetic daemon transcript"


class SyntheticVoiceTranscriber:
    """Test-only recognizer with explicitly released completion."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.release = threading.Event()

    def transcribe(
        self, job: dict[str, Any], *, duration_ms: int, deadline: float | None,
    ) -> str:
        if not self.release.wait(timeout=POLL_TIMEOUT_SECONDS):
            raise TimeoutError("synthetic transcript completion was not released")
        self.calls.append(str(job["job_id"]))
        return f"{VOICE_TEXT} {len(self.calls) - 1}"


class VoiceReadDaemonTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source_root = self.root / "source"
        create_synthetic_source(self.source_root)
        declare_voice_messages(self.source_root, count=2)
        self.reader_token = "synthetic-reader-token"
        self.operator_token = "synthetic-operator-token"
        self.config = replace(
            SightglassConfig.create(self.root / "state", self.source_root),
            reader_token_hash=token_hash(self.reader_token),
            operator_token_hash=token_hash(self.operator_token),
            voice_enabled=True,
            voice_policy="auto",
            voice_language="zh",
        )
        self.config_store = ConfigStore(self.config.data_dir / "config.json")
        self.config_store.save(self.config)
        self.secrets = MemorySecretStore(
            {
                READER_SECRET_ACCOUNT: self.reader_token,
                OPERATOR_SECRET_ACCOUNT: self.operator_token,
            }
        )
        self.transcriber = SyntheticVoiceTranscriber()
        self.daemon: SightglassDaemon | None = None
        self.thread: threading.Thread | None = None
        self.start_daemon()

    def tearDown(self) -> None:
        self.transcriber.release.set()
        self.stop_daemon()
        self.temp.cleanup()

    def start_daemon(self) -> None:
        self.daemon = SightglassDaemon(
            config_store=self.config_store,
            secret_store=self.secrets,
            voice_transcriber=self.transcriber,
        )
        self.thread = threading.Thread(
            target=self.daemon.serve_forever,
            kwargs={"install_signal_handlers": False},
            daemon=True,
        )
        self.thread.start()
        deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
        while time.monotonic() < deadline and not self.config.socket_path.exists():
            time.sleep(0.01)
        self.assertTrue(self.config.socket_path.exists())

    def stop_daemon(self) -> None:
        if self.thread is None or not self.thread.is_alive():
            return
        self.operator.call("daemon.shutdown")
        self.thread.join(timeout=POLL_TIMEOUT_SECONDS)
        self.assertFalse(self.thread.is_alive())

    @property
    def reader(self) -> IPCClient:
        return IPCClient(config_store=self.config_store, secret_store=self.secrets, role="reader")

    @property
    def operator(self) -> IPCClient:
        return IPCClient(config_store=self.config_store, secret_store=self.secrets, role="operator")

    @property
    def bridge(self) -> DaemonReaderTools:
        return DaemonReaderTools(self.reader)

    def wait_until(self, predicate, *, timeout: float = POLL_TIMEOUT_SECONDS) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.02)
        self.fail("condition was not reached before the deadline")

    def group_id(self) -> str:
        return self.bridge.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]

    def job_count(self) -> int:
        assert self.daemon is not None
        with self.daemon.database.connection() as connection:
            return int(connection.execute("SELECT count(*) FROM voice_jobs").fetchone()[0])

    def voice_resource_id(self) -> str:
        page = self.bridge.wechat_read_messages(
            mode="recent", conversation_id=self.group_id(), projection="detail", limit=50,
            voice="off",
        )
        for message in page["messages"]:
            resources = self.bridge.wechat_list_resources(str(message["message_id"]))
            candidate = next(
                (item for item in resources["resources"] if item["kind"] == "voice"), None
            )
            if candidate is not None:
                return str(candidate["resource_id"])
        self.fail("no voice resource was delivered")

    def test_message_and_resource_reads_share_one_voice_batch_cache(self) -> None:
        page = self.bridge.wechat_read_messages(
            mode="recent", conversation_id=self.group_id(), projection="detail", limit=50,
            voice="auto",
        )
        sidecar = page["voice"]
        self.assertEqual(sidecar["state"], "prepared")
        self.assertEqual(sidecar["coverage"]["selected"], 2)
        jobs = self.job_count()
        self.assertEqual(jobs, 2)
        self.assertEqual(sidecar["coverage"]["pending"], 2)
        self.transcriber.release.set()
        self.wait_until(lambda: self.transcriber.calls and self._ready_count() == 2)

        cached = self.bridge.wechat_read_messages(
            mode="recent", conversation_id=self.group_id(), projection="detail", limit=50,
            voice="cached",
        )
        self.assertEqual(cached["voice"]["state"], "cached")
        message_transcripts = {
            str(row[0]): str(row[3]) for row in cached["voice"]["items"]
        }
        self.assertEqual(len(message_transcripts), 2)
        self.assertTrue(all(text.startswith(VOICE_TEXT) for text in message_transcripts.values()))

        resource_id = self.voice_resource_id()
        payload = self.bridge.wechat_read_resource(resource_id=resource_id, mode="text")
        descriptor = payload.structuredContent or {}
        self.assertEqual(descriptor["mode"], "text")
        self.assertEqual(descriptor["derivation"]["kind"], "derived_transcript")
        self.assertEqual(descriptor["transcript"]["state"], "ready")
        reference = descriptor["transcript"]["reading_token"]
        committed = self.bridge.wechat_read_transcripts(str(reference), wait_ms=0)
        self.assertEqual(committed["coverage"]["ready"], 1)
        blocks: list[str] = []
        for item in payload.content:
            text = getattr(item, "text", None)
            if text is None:
                text = getattr(getattr(item, "resource", None), "text", None)
            if isinstance(text, str):
                blocks.append(text)
        self.assertTrue(
            any(block in message_transcripts.values() for block in blocks), blocks
        )
        self.assertEqual(self.job_count(), jobs)

    def _ready_count(self) -> int:
        assert self.daemon is not None
        with self.daemon.database.connection() as connection:
            return int(
                connection.execute(
                    "SELECT count(*) FROM voice_jobs WHERE state = 'ready'"
                ).fetchone()[0]
            )

    def test_daemon_status_reports_the_voice_read_configuration(self) -> None:
        status = self.operator.call("daemon.status")
        voice_read = status["voice_read"]
        self.assertEqual(
            {key: value for key, value in voice_read.items() if key != "readiness"},
            {
                "enabled": True,
                "default_policy": "auto",
                "language": "zh",
                "open_item_limit": 3,
                "open_duration_ms": 300_000,
                "helper_timeout_seconds": 120,
            },
        )
        readiness = voice_read["readiness"]
        self.assertEqual(readiness["schema"], "sightglass.voice-readiness.v1")
        self.assertEqual(readiness["transcriber"], "injected")
        self.assertTrue(readiness["ready"])
        self.assertIsNone(readiness["blocked_reason"])
        self.assertTrue(status["voice_worker"]["enabled"])
        self.assertEqual(status["transcript_waiters"]["max_waiters"], 2)

    def test_invalid_voice_argument_is_rejected_over_ipc(self) -> None:
        arguments: dict[str, Any] = {
            "mode": "recent",
            "conversation_id": self.group_id(),
            "limit": 5,
            "voice": "sometimes",
        }
        envelope = self.bridge.wechat_read_messages(**arguments)
        self.assertEqual(envelope["schema"], "sightglass.error.v1")
        self.assertEqual(envelope["code"], "QUERY_INVALID")
        self.assertEqual(self.job_count(), 0)

    def test_denied_conversation_refuses_message_and_resource_voice_reads(self) -> None:
        resource_id = self.voice_resource_id()
        conversation_id = self.group_id()
        self.operator.call("operator.policy.deny", {"conversation_id": conversation_id})
        denied = self.bridge.wechat_read_messages(
            mode="recent", conversation_id=conversation_id, projection="detail", limit=50,
            voice="auto",
        )
        self.assertEqual(denied["code"], "POLICY_DENIED")
        resource = self.bridge.wechat_read_resource(resource_id=resource_id, mode="text")
        self.assertTrue(resource.isError)
        self.assertEqual((resource.structuredContent or {})["code"], "POLICY_DENIED")
        self.assertEqual(self.job_count(), 0)

        self.operator.call("operator.policy.clear_deny", {"conversation_id": conversation_id})
        restored = self.bridge.wechat_read_messages(
            mode="recent", conversation_id=conversation_id, projection="detail", limit=50,
            voice="auto",
        )
        self.assertEqual(restored["voice"]["state"], "prepared")
        self.assertEqual(restored["voice"]["coverage"]["pending"], 2)
        self.assertEqual(self.job_count(), 2)
        self.transcriber.release.set()
        self.wait_until(lambda: self._ready_count() == 2)
        cached = self.bridge.wechat_read_messages(
            mode="recent", conversation_id=conversation_id, projection="detail", limit=50,
            voice="cached",
        )
        self.assertEqual(cached["voice"]["state"], "cached")
        self.assertEqual(cached["voice"]["coverage"]["ready"], 2)
        self.assertEqual(len(self.transcriber.calls), 2)
        self.assertEqual(self.job_count(), 2)


if __name__ == "__main__":
    unittest.main()

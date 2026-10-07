from __future__ import annotations

import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from sightglass.runtime.config import ConfigStore, SightglassConfig
from sightglass.runtime.daemon import SightglassDaemon
from sightglass.runtime.secrets import (
    OPERATOR_SECRET_ACCOUNT,
    READER_SECRET_ACCOUNT,
    MemorySecretStore,
    token_hash,
)
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.voice_source import declare_voice_messages

POLL_TIMEOUT_SECONDS = 5.0
TRANSCRIPT_WAIT_MS = 8_000


class HeldTranscriber:
    """Test-only recognizer that never completes so transcript waiters keep park."""

    def __init__(self) -> None:
        self.release = threading.Event()

    def transcribe(
        self, job: dict[str, Any], *, duration_ms: int, deadline: float | None
    ) -> str:
        while not self.release.wait(timeout=0.05):
            pass
        return "Synthetic transcript"


class VoiceTranscriptWaiterPolicyTests(unittest.TestCase):
    """Exercise the daemon's real three-phase transcript path without IPC sockets."""

    def setUp(self) -> None:
        self.temp = TemporaryDirectory()
        root = Path(self.temp.name)
        self.source_root = root / "source"
        create_synthetic_source(self.source_root)
        declare_voice_messages(self.source_root, count=1)
        reader_token = "synthetic-reader-token"
        operator_token = "synthetic-operator-token"
        self.config = replace(
            SightglassConfig.create(root / "state", self.source_root),
            reader_token_hash=token_hash(reader_token),
            operator_token_hash=token_hash(operator_token),
            voice_enabled=True,
            voice_policy="auto",
            voice_language="zh",
        )
        self.config_store = ConfigStore(self.config.data_dir / "config.json")
        self.config_store.save(self.config)
        self.secrets = MemorySecretStore(
            {
                READER_SECRET_ACCOUNT: reader_token,
                OPERATOR_SECRET_ACCOUNT: operator_token,
            }
        )
        self.transcriber = HeldTranscriber()
        self.daemon = SightglassDaemon(
            config_store=self.config_store,
            secret_store=self.secrets,
            voice_transcriber=self.transcriber,
        )

    def tearDown(self) -> None:
        self.transcriber.release.set()
        self.daemon.voice_worker.stop()
        self.daemon.source_worker.stop()
        self.daemon.tools.close()
        self.temp.cleanup()

    def wait_until(self, predicate, *, timeout: float = POLL_TIMEOUT_SECONDS) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.02)
        self.fail("condition was not reached before the deadline")

    def _reading_token(self) -> str:
        conversation_id = str(
            self.daemon.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
                "conversation_id"
            ]
        )
        page = self.daemon.tools.wechat_read_messages(
            mode="recent",
            conversation_id=conversation_id,
            projection="detail",
            limit=50,
            voice="auto",
        )
        token = page["voice"]["reading_token"]
        assert token is not None
        return str(token)

    def _dispatch(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.daemon._dispatch_transcripts(
            {"name": "wechat_read_transcripts", "arguments": arguments}
        )

    def test_policy_revoked_while_a_transcript_waiter_is_parked_is_rechecked(self) -> None:
        token = self._reading_token()
        initial = self._dispatch(
            {"reading_token": token, "cursor": None, "wait_ms": 0}
        )
        self.assertEqual(initial["schema"], "sightglass.voice-page.v2")
        cursor = initial["next_cursor"]
        assert cursor is not None
        result: dict[str, Any] = {}

        def park() -> None:
            result["page"] = self._dispatch(
                {"reading_token": token, "cursor": cursor, "wait_ms": TRANSCRIPT_WAIT_MS}
            )

        thread = threading.Thread(target=park)
        thread.start()
        try:
            self.wait_until(
                lambda: self.daemon._transcript_waiters.status()["active_waiters"] == 1
            )
            reader = self.daemon.tools.service.reader
            reader.policy = replace(reader.policy, resource_preview=False)
            self.daemon._transcript_waiters.notify()
            thread.join(timeout=POLL_TIMEOUT_SECONDS)
            self.assertFalse(thread.is_alive())
        finally:
            self.transcriber.release.set()

        page = result["page"]
        self.assertNotEqual(page.get("schema"), "sightglass.voice-page.v2")
        self.assertEqual(page.get("schema"), "sightglass.error.v1")
        self.assertEqual(page.get("code"), "POLICY_DENIED")
        self.assertNotIn("items", page)


if __name__ == "__main__":
    unittest.main()

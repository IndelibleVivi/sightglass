"""A materialized reading journey under source and storage failure.

Only generated conversations/audio envelopes are used. Source admission and
optional voice work must not determine whether an already admitted page opens.
"""

from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.voice import VoiceReadSettings
from sightglass.policy.readers import ReaderPolicy
from sightglass.source.synthetic import create_synthetic_source
from sightglass.storage import MIB, StorageBudget, StorageSettings
from tests.fixtures.factory import build_test_stack
from tests.fixtures.voice_source import declare_voice_messages


class ReadingResilienceTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        source = root / "source"
        create_synthetic_source(source)
        declare_voice_messages(source, count=2)
        self.provider, self.repo, self.service, self.tools = build_test_stack(
            source, root / "state" / "window.db", default_projection=None,
            voice=VoiceReadSettings(enabled=True, default_policy="auto", language="zh"),
        )
        self.addCleanup(self.tools.close)
        self.group = self.service.find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        self.service.sync_source_once(initial_tail=100, conversation_limit=100)
        self.db = self.repo.database
        settings = StorageSettings(128 * MIB, 256 * MIB, MIB, 16 * MIB)
        self.storage = StorageBudget(self.db.path.parent, self.db.path, settings)
        self.db.storage = self.storage
        free_patch = patch("sightglass.storage._free", return_value=1024 * MIB)
        self.free = free_patch.start()
        self.addCleanup(free_patch.stop)
        used = self.storage.status()["accounted_bytes"]
        self.storage.settings = replace(settings, soft_limit_bytes=1, hard_limit_bytes=used + 1)
        with self.db.connection() as connection:
            self.focus = str(connection.execute(
                "SELECT message_id FROM messages WHERE conversation_id=? AND kind='voice' "
                "ORDER BY sort_primary DESC LIMIT 1", (self.group,),
            ).fetchone()[0])
        forbidden = AssertionError("admitted read opened source")
        for method in ("snapshot", "session"):
            source_patch = patch.object(self.provider, method, side_effect=forbidden)
            source_patch.start()
            self.addCleanup(source_patch.stop)

    def counts(self) -> tuple[int, ...]:
        with self.db.connection() as connection:
            return tuple(int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                         for table in ("message_observations", "voice_jobs", "voice_batches"))

    def test_recent_at_hard_pressure_preserves_progress_without_growth(self) -> None:
        before = self.counts()
        with self.db.connection() as connection:
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM reader_timeline_cursors"
            ).fetchone()[0], 0)
        page = self.service.read_messages(
            mode="recent", conversation_id=self.group, limit=2, projection="detail",
        )
        self.assertEqual(page["source_receipt"]["served_from"], "window_db")
        self.assertFalse(page["source_receipt"]["freshness"]["live_refresh_confirmed"])
        self.assertEqual(len(page["messages"]), 2)
        self.assertEqual(page["voice"]["state"], "storage_pressure")
        self.assertEqual(page["voice"]["coverage"]["not_scheduled"], 2)
        self.assertEqual(self.counts(), before)
        with self.db.connection() as connection:
            positions = connection.execute("SELECT * FROM reader_timeline_cursors").fetchall()
            self.assertEqual(len(positions), 1)
            self.assertIn(positions[0]["committed_message_id"],
                          [message["message_id"] for message in page["messages"]])
            update_position = connection.execute(
                "SELECT committed_observation_seq FROM reader_update_cursors"
            ).fetchone()
            self.assertIsNotNone(update_position)
        # Exhausting physical headroom still rejects a required progress write.
        self.free.return_value = MIB
        with self.assertRaises(SightglassError) as caught:
            self.service.read_messages(mode="recent", conversation_id=self.group, voice="off")
        self.assertEqual(caught.exception.code, ErrorCode.STORAGE_PRESSURE)

    def test_context_below_free_floor_keeps_text_and_reports_voice_not_scheduled(self) -> None:
        before = self.counts()
        self.free.return_value = MIB
        page = self.service.read_messages(
            mode="context", message_id=self.focus, before=0, after=0,
            projection="detail", voice="auto",
        )
        self.assertEqual([row["message_id"] for row in page["messages"]], [self.focus])
        self.assertEqual(page["voice"]["state"], "storage_pressure")
        self.assertEqual(page["voice"]["coverage"]["not_scheduled"], 1)
        self.assertEqual(self.counts(), before)
        with self.db.connection() as connection:
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM reader_timeline_cursors"
            ).fetchone()[0], 0)
        self.service.reader = replace(self.service.reader, policy=ReaderPolicy(mode="allowlist"))
        with self.assertRaises(SightglassError) as caught:
            self.service.read_messages(mode="context", message_id=self.focus, before=0, after=0)
        self.assertEqual(caught.exception.code, ErrorCode.POLICY_DENIED)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.policy.readers import ReaderPolicy
from sightglass.residency.decisions import ResidencySettings
from sightglass.runtime.config import SightglassConfig
from sightglass.runtime.service import build_daemon_tools
from sightglass.source.synthetic import create_synthetic_source
from sightglass.storage import MIB, StorageSettings


class StorageAdmissionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        create_synthetic_source(self.source)
        self.free = patch("sightglass.storage._free", return_value=1024 * MIB)
        self.free.start()
        self.addCleanup(self.free.stop)
        self.settings = StorageSettings(128 * MIB, 256 * MIB, 0, 16 * MIB)
        self.config = replace(
            SightglassConfig.create(self.root / "state", self.source),
            storage=self.settings,
        )
        self.tools = build_daemon_tools(self.config)
        self.addCleanup(self.tools.close)
        self.service = self.tools.service
        self.service.residency.set_settings(ResidencySettings(default_mode="keep"))
        self.repo = self.service.repository
        self.db = self.repo.database
        self.storage = self.db.storage
        assert self.storage is not None
        self.service.status()
        self.group = self.service.find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]

    def pressure(self, *, soft: bool = False) -> None:
        assert self.storage is not None
        used = self.storage.status()["accounted_bytes"]
        self.storage.settings = replace(
            self.settings,
            soft_limit_bytes=1,
            hard_limit_bytes=used + (64 * MIB if soft else 1),
        )

    def test_soft_pauses_backfill_without_cursor_advance_and_requires_explicit_resume(self) -> None:
        self.service.queue_backfill(conversation_id=self.group)
        job = self.repo.next_backfill_job()
        assert job is not None
        before = dict(job)
        self.pressure(soft=True)
        result = self.service.process_backfill_once()
        self.assertEqual(result["state"], "paused")
        self.assertEqual(result["reason"], "storage_pressure")
        self.assertEqual(self.service.backfill_status()["active_job_count"], 0)
        with self.db.connection() as connection:
            row = connection.execute("SELECT * FROM source_backfill_jobs").fetchone()
            self.assertEqual(row["cursor_token"], before["cursor_token"])
            self.assertEqual(row["processed_messages"], before["processed_messages"])
        with self.assertRaises(SightglassError):
            self.service.set_backfill_paused(False)
        # Ordinary reading still works inside the hard boundary.
        page = self.service.read_messages(mode="recent", conversation_id=self.group)
        self.assertTrue(page["messages"])
        assert self.storage is not None
        self.storage.settings = self.settings
        self.assertEqual(self.service.process_backfill_once()["state"], "idle")
        self.service.set_backfill_paused(False)
        self.assertNotEqual(self.service.process_backfill_once()["state"], "paused")

    def test_hard_blocks_hydrate_tail_resources_and_new_spool_with_truthful_status(self) -> None:
        page = self.service.read_messages(
            mode="recent", conversation_id=self.group, projection="detail"
        )
        resource_id = next(
            r["resource_id"] for m in page["messages"] for r in m.get("resources", [])
        )
        with self.db.connection() as connection:
            before = connection.execute("SELECT COUNT(*) FROM message_observations").fetchone()[0]
        self.pressure()
        for action in (
            lambda: self.service.read_messages(mode="recent", conversation_id=self.group),
            self.service.sync_source_once,
            lambda: self.service.read_resource(
                resource_id=resource_id,
                mode="original",
                page=None,
                start_line=None,
                end_line=None,
                max_bytes=MIB,
            ),
            lambda: self.service.delivery_store.write("synthetic-new", {"value": "synthetic"}),
        ):
            with self.assertRaises(SightglassError) as caught:
                action()
            self.assertEqual(caught.exception.code, ErrorCode.STORAGE_PRESSURE)
        with self.db.connection() as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM message_observations").fetchone()[0],
                before,
            )
        self.assertFalse(self.service.cached_status()["ready"])
        self.assertIn(
            self.service.cached_status()["storage"]["state"], {"hard_limit", "soft_limit"}
        )

    def test_pending_replays_and_ack_commits_at_pressure_without_new_delivery(self) -> None:
        first = self.service.read_messages(mode="updates", conversation_id=self.group)
        delivery_id = first["page"]["delivery_id"]
        self.assertIsNotNone(delivery_id)
        self.pressure()
        # No database writer is needed for exact replay when the policy is unchanged.
        with patch.object(self.db, "transaction", side_effect=AssertionError("unexpected write")):
            self.assertEqual(
                self.service.read_messages(mode="updates", conversation_id=self.group), first
            )
        for _ in range(2):
            with self.assertRaises(SightglassError) as caught:
                self.service.read_messages(
                    mode="updates",
                    conversation_id=self.group,
                    ack_delivery_id=delivery_id,
                )
            self.assertEqual(caught.exception.code, ErrorCode.STORAGE_PRESSURE)
            self.assertTrue(caught.exception.details["ack_committed"])
        delivery = self.repo.delivery(delivery_id)
        assert delivery is not None
        self.assertEqual(delivery["status"], "acknowledged")
        assert self.storage is not None
        self.storage.settings = self.settings
        resumed = self.service.read_messages(mode="updates", conversation_id=self.group)
        self.assertEqual(resumed["messages"], [])

    def test_policy_denial_never_replays_or_acknowledges_under_pressure(self) -> None:
        first = self.service.read_messages(mode="updates", conversation_id=self.group)
        self.pressure()
        self.service.reader = replace(self.service.reader, policy=ReaderPolicy(mode="allowlist"))
        for ack in (None, first["page"]["delivery_id"]):
            with self.assertRaises(SightglassError) as caught:
                self.service.read_messages(
                    mode="updates", conversation_id=self.group, ack_delivery_id=ack
                )
            self.assertEqual(caught.exception.code, ErrorCode.POLICY_DENIED)

    def test_restart_above_hard_limit_preserves_pending_replay_and_rejects_wrong_ack(self) -> None:
        first = self.service.read_messages(mode="updates", conversation_id=self.group)
        self.tools.close()
        self.pressure()
        assert self.storage is not None
        restarted = build_daemon_tools(replace(self.config, storage=self.storage.settings))
        self.addCleanup(restarted.close)
        self.assertFalse(restarted.service.cached_status()["ready"])
        self.assertEqual(
            restarted.service.read_messages(mode="updates", conversation_id=self.group), first
        )
        with self.assertRaises(SightglassError) as caught:
            restarted.service.read_messages(
                mode="updates",
                conversation_id=self.group,
                ack_delivery_id="synthetic-unknown-delivery",
            )
        self.assertEqual(caught.exception.code, ErrorCode.DELIVERY_ACK_INVALID)
        self.assertEqual(
            restarted.service.read_messages(mode="updates", conversation_id=self.group), first
        )


if __name__ == "__main__":
    unittest.main()

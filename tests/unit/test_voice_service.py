from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.voice import VoiceBatchReceipt, VoiceSelectionItem, VoiceTranscriptPage
from sightglass.model.db import WindowDB
from sightglass.source.identity import SignedTokenCodec
from sightglass.voice.repository import VoiceRepository
from sightglass.voice.service import VoiceLimits, VoiceService
from tests.unit.test_schema_v3_migration import seed_voice_parents


class VoiceFixture:
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "window.db"
        self.db = WindowDB(self.path)
        with self.db.transaction() as connection:
            seed_voice_parents(connection)
        self.repo = VoiceRepository(self.db)
        self.now = datetime(2026, 9, 18, tzinfo=UTC)
        self.codec = SignedTokenCodec(b"synthetic-voice-secret-32-bytes")
        self.service = VoiceService(self.repo, self.codec, clock=lambda: self.now)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def selection(self, count: int = 1, duration: int | None = 10000) -> list[VoiceSelectionItem]:
        with self.db.transaction() as connection:
            for ordinal in range(1, count):
                connection.execute("""
                    INSERT OR IGNORE INTO resources(resource_id, message_id, source_ordinal,
                        kind, availability, resolver_json, first_seen_at, last_seen_at)
                    VALUES (?, 'msg', ?, 'voice', 'available', '{}', 'now', 'now')
                """, (f"synthetic-{ordinal}", ordinal))
        return [VoiceSelectionItem("msg", "resource" if i == 0 else f"synthetic-{i}",
                                   "revision", duration) for i in range(count)]

    def create(self, selection: list[VoiceSelectionItem] | None = None, *,
               policy: str = "auto", recipe: str = "recipe") -> VoiceBatchReceipt:
        return self.service.create_batch(
            reader_id="reader", account_id="acct", account_binding_id=None,
            selection=self.selection() if selection is None else selection,
            recipe_digest=recipe, recipe_json='{"engine":"synthetic"}', voice_policy=policy,
        )

    def poll(self, token: str, cursor: str | None = None) -> VoiceTranscriptPage:
        return self.service.get_transcripts(reading_token=token, reader_id="reader",
                                            account_id="acct", account_binding_id=None,
                                            cursor=cursor)

    def token(self, receipt: VoiceBatchReceipt) -> str:
        assert receipt.reading_token is not None
        return receipt.reading_token

    def jobs(self) -> list[dict]:
        return self.repo.rows("SELECT * FROM voice_jobs ORDER BY rowid")


class VoiceServiceTests(VoiceFixture, unittest.TestCase):
    def test_new_queue_work_notifies_after_commit_and_expansion_notifies_again(self) -> None:
        observed: list[int] = []
        # A different repository handle cannot observe this service's active
        # transaction; the callback must see durable rows from its own read.
        observer = VoiceRepository(WindowDB(self.path))

        def committed():
            observed.append(len(observer.pending_jobs(32)))

        wake = Mock(side_effect=committed)
        self.service.set_worker_wake(wake)
        receipt = self.create(self.selection(12))
        self.assertEqual(observed, [3])
        page = self.poll(self.token(receipt))
        self.assertEqual(observed, [3])  # replay does not expand admission
        self.poll(self.token(receipt), page.next_cursor)
        self.assertEqual(observed, [3, 12])

    def test_nested_batch_admission_waits_for_outer_commit_and_rollback_discards_wake(self) -> None:
        wake = Mock()
        self.service.set_worker_wake(wake)
        with self.db.transaction():
            self.create()
            wake.assert_not_called()
        self.assertEqual(wake.call_count, 1)
        with self.assertRaises(ValueError):
            with self.db.transaction():
                self.create(recipe="synthetic-rolled-back")
                self.assertEqual(wake.call_count, 1)
                raise ValueError("synthetic rollback")
        self.assertEqual(wake.call_count, 1)
        self.assertEqual(len(self.jobs()), 1)

    def test_real_entry_lifecycle(self) -> None:
        receipt = self.create()
        self.assertTrue(receipt.created)
        self.assertEqual(receipt.coverage.pending, 1)
        token = self.token(receipt)
        first = self.poll(token)
        self.assertEqual(first.items[0].state, "pending")
        job = self.jobs()[0]["job_id"]
        fence = self.service.lease(job, owner_id="synthetic-worker")
        self.service.start(job, owner_id="synthetic-worker", fencing_token=fence)
        self.service.complete(job, owner_id="synthetic-worker", fencing_token=fence,
                              text="Synthetic transcript")
        final = self.poll(token)
        self.assertEqual(final.items, first.items)
        self.assertEqual(final.coverage.ready, 1)
        self.assertTrue(final.processing_complete)
        self.assertTrue(final.text_coverage_complete)

    def test_selection_identity_order_recipe_and_fixed_ttl(self) -> None:
        selected = self.selection(2)
        receipt = self.create(selected)
        manifest = self.repo.items(self.token(receipt))
        self.now += timedelta(hours=1)
        reused = self.create(selected)
        self.assertFalse(reused.created)
        self.assertEqual(reused.reading_token, receipt.reading_token)
        self.assertEqual(reused.expires_at, receipt.expires_at)
        self.assertEqual(self.repo.items(self.token(receipt)), manifest)
        reversed_receipt = self.create(list(reversed(selected)))
        self.assertNotEqual(reversed_receipt.reading_token, receipt.reading_token)
        other_receipt = self.create(selected, recipe="other")
        self.assertNotEqual(other_receipt.reading_token, receipt.reading_token)
        self.now += timedelta(hours=24)
        self.assertNotEqual(self.create(selected).reading_token, receipt.reading_token)
        with self.assertRaises(SightglassError) as caught:
            self.poll(self.token(receipt))
        self.assertEqual(caught.exception.code, ErrorCode.CURSOR_STALE)

    def test_off_and_no_voice_create_nothing(self) -> None:
        self.assertIsNone(self.create(policy="off").reading_token)
        self.assertIsNone(self.create([]).reading_token)
        with self.db.transaction() as connection:
            connection.execute("UPDATE resources SET kind='text'")
        self.assertIsNone(self.create().reading_token)
        self.assertEqual(self.jobs(), [])
        self.assertEqual(self.repo.rows("SELECT * FROM voice_batches"), [])

    def test_cached_only_ready_and_cross_batch_sharing(self) -> None:
        miss = self.create(policy="cached")
        self.assertEqual(miss.coverage.not_scheduled, 1)
        self.assertEqual(self.jobs(), [])
        first = self.create()
        selected = self.selection(2)
        shared = self.create(selected)
        self.assertEqual(len(self.jobs()), 2)
        self.assertEqual(self.repo.items(self.token(first))[0]["job_id"],
                         self.repo.items(self.token(shared))[0]["job_id"])
        job = self.jobs()[0]["job_id"]
        fence = self.service.lease(job, owner_id="worker")
        self.service.complete(job, owner_id="worker", fencing_token=fence, text="Synthetic")
        hit = self.create(list(reversed(selected)), policy="cached")
        self.assertEqual(hit.coverage.ready, 1)
        self.assertEqual(hit.coverage.not_scheduled, 1)
        self.assertEqual(len(self.jobs()), 2)

    def test_first_three_step_twelve_and_replay_no_new_admission(self) -> None:
        receipt = self.create(self.selection(30))
        self.assertEqual(receipt.coverage.pending, 3)
        token = self.token(receipt)
        first = self.poll(token)
        page = self.poll(token, first.next_cursor)
        self.assertEqual(page.coverage.pending, 15)
        self.assertEqual(self.poll(token, first.next_cursor).items, page.items)
        self.assertEqual(len(self.jobs()), 15)
        self.poll(token, page.next_cursor)
        self.assertEqual(len(self.jobs()), 27)
        self.poll(token, first.next_cursor)
        self.assertEqual(len(self.jobs()), 27)

    def test_global_count_duration_and_unknown_reservation(self) -> None:
        receipt = self.create(self.selection(40))
        token = self.token(receipt)
        cursor = None
        for _ in range(5):
            cursor = self.poll(token, cursor).next_cursor
        self.assertEqual(len(self.jobs()), 32)
        self.assertEqual(self.poll(token).coverage.not_scheduled, 8)
        self.assertEqual(self.repo.active_budget(120000), (32, 320000))

    def test_unknown_duration_uses_single_item_ceiling(self) -> None:
        receipt = self.create(self.selection(10, None))
        self.assertEqual(receipt.coverage.pending, 2)
        token = self.token(receipt)
        cursor = None
        for _ in range(5):
            cursor = self.poll(token, cursor).next_cursor
        self.assertEqual(self.repo.active_budget(120000), (5, 600000))
        self.assertEqual(self.poll(token).coverage.not_scheduled, 5)
        too_long = self.create(self.selection(1, 120001), recipe="too-long")
        self.assertEqual(too_long.coverage.pending, 0)

    def test_concurrent_same_selection_deduplicates(self) -> None:
        selected = self.selection(3)

        def create(_: int) -> VoiceBatchReceipt:
            service = VoiceService(VoiceRepository(WindowDB(self.path)), self.codec,
                                   clock=lambda: self.now)
            return service.create_batch(reader_id="reader", account_id="acct",
                                        account_binding_id=None, selection=selected,
                                        recipe_digest="recipe", recipe_json="{}")

        with ThreadPoolExecutor(max_workers=4) as pool:
            receipts = list(pool.map(create, range(8)))
        self.assertEqual(len({r.reading_token for r in receipts}), 1)
        self.assertEqual(sum(r.created for r in receipts), 1)
        self.assertEqual(len(self.jobs()), 3)

    def test_limits_reject_exceeding_hard_ceilings(self) -> None:
        for limits in ({"first_count": 4}, {"step_count": 13}, {"global_count": 33},
                       {"global_duration_ms": 600001}, {"step_duration_ms": 300001}):
            with self.assertRaises(ValueError):
                VoiceLimits(**limits)

    def test_requeue_spends_the_previous_fencing_token(self) -> None:
        self.create()
        job = self.jobs()[0]["job_id"]
        fence = self.service.lease(job, owner_id="worker")
        self.service.start(job, owner_id="worker", fencing_token=fence)
        self.service.requeue(job, owner_id="worker", fencing_token=fence)
        requeued = self.repo.job(job)
        assert requeued is not None
        self.assertEqual(requeued["state"], "pending")
        self.assertIsNone(requeued["owner_id"])
        self.assertIsNone(requeued["lease_expires_at"])
        self.assertEqual(requeued["fencing_token"], fence + 1)
        self.assertEqual(requeued["attempt"], 1)
        with self.assertRaises(SightglassError) as caught:
            self.service.complete(job, owner_id="worker", fencing_token=fence,
                                  text="Synthetic late transcript")
        self.assertEqual(caught.exception.code, ErrorCode.CURSOR_STALE)
        with self.assertRaises(SightglassError):
            self.service.requeue(job, owner_id="worker", fencing_token=fence)

    def test_recover_outstanding_leases_returns_live_leases_to_the_queue(self) -> None:
        receipt = self.create()
        job = self.jobs()[0]["job_id"]
        fence = self.service.lease(job, owner_id="departing-worker", lease_seconds=600)
        self.service.start(job, owner_id="departing-worker", fencing_token=fence)
        self.assertEqual(self.repo.expired_leases("2026-09-18T00:00:00.000000+00:00"), [])
        self.assertEqual(self.service.recover_outstanding_leases(), 1)
        replaced = self.repo.job(job)
        assert replaced is not None
        self.assertEqual(replaced["state"], "pending")
        self.assertGreater(int(replaced["fencing_token"]), fence)
        self.assertEqual(self.service.recover_outstanding_leases(), 0)
        replacement_fence = self.service.lease(job, owner_id="replacement-worker")
        self.service.complete(job, owner_id="replacement-worker",
                              fencing_token=replacement_fence, text="Synthetic transcript")
        page = self.poll(self.token(receipt))
        self.assertEqual(page.coverage.ready, 1)
        self.assertTrue(page.processing_complete)

    def test_retry_blocked_jobs_returns_them_to_the_queue(self) -> None:
        receipt = self.create()
        job = self.jobs()[0]["job_id"]
        fence = self.service.lease(job, owner_id="worker")
        self.service.start(job, owner_id="worker", fencing_token=fence)
        self.service.fail(job, owner_id="worker", fencing_token=fence,
                          error_code="RESOURCE_BLOCKED", state="blocked")
        page = self.poll(self.token(receipt))
        self.assertEqual(page.coverage.blocked, 1)
        self.assertEqual(self.service.retry_blocked_jobs(), 1)
        requeued = self.repo.job(job)
        assert requeued is not None
        self.assertEqual(requeued["state"], "pending")
        self.assertIsNone(requeued["error_code"])
        self.assertIsNone(requeued["owner_id"])
        self.assertIsNone(requeued["lease_expires_at"])
        self.assertGreater(int(requeued["fencing_token"]), fence)
        self.assertEqual(self.service.retry_blocked_jobs(), 0)
        with self.assertRaises(SightglassError) as caught:
            self.service.complete(job, owner_id="worker", fencing_token=fence,
                                  text="Synthetic stale transcript")
        self.assertEqual(caught.exception.code, ErrorCode.CURSOR_STALE)
        replacement_fence = self.service.lease(job, owner_id="worker")
        self.service.complete(job, owner_id="worker", fencing_token=replacement_fence,
                              text="Synthetic transcript")
        final = self.poll(self.token(receipt))
        self.assertEqual(final.coverage.ready, 1)
        self.assertTrue(final.processing_complete)

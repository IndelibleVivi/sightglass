from __future__ import annotations

import hashlib
import json
import unittest
from datetime import timedelta
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError
from tests.unit.test_voice_service import VoiceFixture


class VoiceTranscriptTests(VoiceFixture, unittest.TestCase):
    def test_out_of_order_results_replay_immutable_events(self) -> None:
        token = self.token(self.create(self.selection(2)))
        first = self.poll(token)
        initial_second = self.poll(token, first.next_cursor)
        jobs = self.jobs()
        for job in reversed(jobs):
            fence = self.service.lease(job["job_id"], owner_id="worker")
            self.service.complete(job["job_id"], owner_id="worker", fencing_token=fence,
                                  text=f"Synthetic {job['resource_id']}")
        self.assertEqual(self.poll(token).items, first.items)
        self.assertEqual(self.poll(token, first.next_cursor).items, initial_second.items)
        page = initial_second
        ready = []
        while True:
            page = self.poll(token, page.next_cursor)
            ready.extend(item.resource_id for item in page.items if item.state == "ready")
            if not page.has_more_results_now:
                break
        self.assertEqual(ready, ["synthetic-1", "resource"])
        self.assertTrue(page.text_coverage_complete)
        self.assertEqual(self.poll(token, page.next_cursor).items, ())
        for event in self.repo.events(token, limit=100):
            row = self.repo.object(event["result_digest"])
            assert row is not None
            obj = self.service.object_store.read_binding(row)
            self.assertEqual(hashlib.sha256(obj.data).hexdigest(), event["result_digest"])
            self.assertIn("state", json.loads(obj.data))
        for job in self.jobs():
            row = self.repo.object(job["result_digest"])
            assert row is not None
            self.assertEqual(json.loads(self.service.object_store.read_binding(row).data),
                             {"text": f"Synthetic {job['resource_id']}"})
        public = json.dumps(page.as_dict())
        self.assertNotIn(str(self.path.parent), public)
        self.assertNotIn("recipe_json", public)

    def test_no_more_events_does_not_mean_processing_complete(self) -> None:
        token = self.token(self.create())
        page = self.poll(token)
        self.assertFalse(page.has_more_results_now)
        self.assertFalse(page.processing_complete)
        self.assertFalse(page.text_coverage_complete)
        self.assertEqual(self.poll(token, page.next_cursor).items, ())

    def test_all_coverage_categories_and_terminal_completion(self) -> None:
        selected = self.selection(7)
        token = self.token(self.create(selected))
        first = self.poll(token)
        self.poll(token, first.next_cursor)
        jobs = self.jobs()
        for index, state in enumerate(("ready", "empty", "blocked", "failed", "cancelled")):
            job = jobs[index]["job_id"]
            fence = self.service.lease(job, owner_id="worker")
            self.service.start(job, owner_id="worker", fencing_token=fence)
            self.assertEqual(self.poll(token).coverage.pending, 7 - index)
            if state in {"ready", "empty"}:
                self.service.complete(job, owner_id="worker", fencing_token=fence,
                                      text="Synthetic" if state == "ready" else " \n")
            else:
                self.service.fail(job, owner_id="worker", fencing_token=fence,
                                  error_code="SYNTHETIC_FAILURE", state=state)
        page = self.poll(token)
        counts = page.coverage.as_dict()
        self.assertEqual(counts, dict(selected=7, ready=1, pending=2, not_scheduled=0,
                                      blocked=1, failed=1, empty=1, cancelled=1))
        self.assertEqual(sum(value for key, value in counts.items() if key != "selected"), 7)
        self.assertFalse(page.processing_complete)
        for job in jobs[5:]:
            fence = self.service.lease(job["job_id"], owner_id="worker")
            self.service.fail(job["job_id"], owner_id="worker", fencing_token=fence,
                              error_code="SYNTHETIC_FAILURE")
        page = self.poll(token)
        self.assertTrue(page.processing_complete)
        self.assertFalse(page.text_coverage_complete)

    def test_terminal_events_identify_their_content_free_error_code(self) -> None:
        token = self.token(self.create())
        initial = self.poll(token)
        job = self.jobs()[0]["job_id"]
        fence = self.service.lease(job, owner_id="worker")
        self.service.fail(
            job,
            owner_id="worker",
            fencing_token=fence,
            error_code="SOURCE_GENERATION_CHANGED",
        )

        cursor = initial.next_cursor
        terminal = None
        while terminal is None:
            page = self.poll(token, cursor)
            cursor = page.next_cursor
            terminal = next((item for item in page.items if item.state == "failed"), None)

        self.assertEqual(terminal.error_code, "SOURCE_GENERATION_CHANGED")
        self.assertEqual(page.as_dict()["items"][0][5], "SOURCE_GENERATION_CHANGED")

    def test_blocked_is_shared_without_retry_and_failure_idempotent(self) -> None:
        token = self.token(self.create())
        job = self.jobs()[0]["job_id"]
        fence = self.service.lease(job, owner_id="worker")
        self.service.fail(job, owner_id="worker", fencing_token=fence,
                          error_code="SYNTHETIC_BLOCKED", state="blocked")
        events = self.repo.events(token, limit=100)
        self.service.fail(job, owner_id="worker", fencing_token=fence,
                          error_code="SYNTHETIC_BLOCKED", state="blocked")
        self.assertEqual(self.repo.events(token, limit=100), events)
        shared = self.create(self.selection(2))
        self.assertEqual(shared.coverage.blocked, 1)
        self.assertEqual(len(self.jobs()), 2)
        self.now += timedelta(minutes=10)
        self.assertEqual(self.service.recover_expired_leases(), 0)
        blocked = self.repo.job(job)
        assert blocked is not None
        self.assertEqual(blocked["state"], "blocked")

    def test_expired_lease_fences_late_workers_and_recovers(self) -> None:
        token = self.token(self.create(self.selection(2)))
        jobs = self.jobs()
        fences = []
        for job in jobs:
            fences.append(self.service.lease(job["job_id"], owner_id="old", lease_seconds=5))
        self.service.start(jobs[0]["job_id"], owner_id="old", fencing_token=fences[0])
        self.now += timedelta(seconds=5)
        for job, fence in zip(jobs, fences, strict=True):
            with self.assertRaises(SightglassError) as caught:
                self.service.complete(job["job_id"], owner_id="old", fencing_token=fence,
                                      text="Late synthetic")
            self.assertEqual(caught.exception.code, ErrorCode.CURSOR_STALE)
        self.assertEqual(self.service.recover_expired_leases(), 2)
        self.assertEqual(self.service.recover_expired_leases(), 0)
        for job, fence in zip(jobs, fences, strict=True):
            current = self.repo.job(job["job_id"])
            assert current is not None
            self.assertEqual(current["state"], "pending")
            self.assertIsNone(current["owner_id"])
            self.assertGreater(current["fencing_token"], fence)
            new_fence = self.service.lease(job["job_id"], owner_id="new")
            with self.assertRaises(SightglassError):
                self.service.fail(job["job_id"], owner_id="old", fencing_token=fence,
                                  error_code="LATE")
            digest = self.service.complete(job["job_id"], owner_id="new", fencing_token=new_fence,
                                           text="New synthetic")
            events = self.repo.events(token, limit=100)
            self.assertEqual(self.service.complete(job["job_id"], owner_id="new",
                                                   fencing_token=new_fence, text="New synthetic"),
                             digest)
            self.assertEqual(self.repo.events(token, limit=100), events)
            with self.assertRaises(SightglassError):
                self.service.complete(job["job_id"], owner_id="new", fencing_token=new_fence,
                                      text="Changed synthetic")
        self.assertEqual(self.poll(token).coverage.ready, 2)

    def test_invalid_cursor_and_scope_fail_closed(self) -> None:
        token = self.token(self.create())
        page = self.poll(token)
        assert page.next_cursor is not None
        payload = self.codec.decode(page.next_cursor)
        invalid = ["tampered", self.codec.encode({**payload, "event_id": 999999}),
                   self.codec.encode({**payload, "event_id": True}),
                   self.codec.encode({**payload, "reader_id": "other"}),
                   self.codec.encode({**payload, "account_id": "other"}),
                   self.codec.encode({**payload, "account_binding_id": "other"})]
        for cursor in invalid:
            with self.assertRaises(SightglassError) as caught:
                self.poll(token, cursor)
            self.assertEqual(caught.exception.code, ErrorCode.CURSOR_INVALID)
        other = self.token(self.create(recipe="different"))
        with self.assertRaises(SightglassError):
            self.poll(other, page.next_cursor)
        for overrides in ({"reader_id": "other"}, {"account_id": "other"},
                          {"account_binding_id": "other"}, {"reading_token": "unknown"}):
            args: dict[str, Any] = dict(reading_token=token, reader_id="reader", account_id="acct",
                                       account_binding_id=None)
            args.update(overrides)
            with self.assertRaises(SightglassError) as caught:
                self.service.get_transcripts(**args)
            self.assertEqual(caught.exception.code, ErrorCode.CURSOR_INVALID)
        with self.db.transaction() as connection:
            connection.execute("UPDATE accounts SET account_binding_id='synthetic-rebinding'")
        with self.assertRaises(SightglassError) as caught:
            self.poll(token)
        self.assertEqual(caught.exception.code, ErrorCode.CURSOR_INVALID)

    def test_fixed_expiry_applies_even_with_valid_cursor(self) -> None:
        token = self.token(self.create())
        page = self.poll(token)
        self.now += timedelta(hours=24)
        with self.assertRaises(SightglassError) as caught:
            self.poll(token, page.next_cursor)
        self.assertEqual(caught.exception.code, ErrorCode.CURSOR_STALE)

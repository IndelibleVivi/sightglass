"""Synthetic-only regression for durable history failures and fair retry."""

from __future__ import annotations

import json
import tempfile
import time
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest import mock

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.operations import check_operation_budget, operation_budget
from sightglass.reader.service import ReaderService
from sightglass.runtime.source_worker import SourceWorker
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


class BackfillStaleJobTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        create_synthetic_source(self.root / "source")
        self.provider, self.repository, self.service, _ = build_test_stack(
            self.root / "source", self.root / "state/window.db"
        )
        self.service.sync_source_once(initial_tail=1, conversation_limit=100)
        account = self.repository.account_id_for("synthetic-account-demo")
        self.group = self.repository.conversation_id_for(account, "conv_group")
        self.other = self.repository.conversation_id_for(account, "conv_direct")

    def job(self, identity):
        row = self.repository.backfill_job(identity)
        assert row is not None
        return row

    def next_job(self):
        row = self.repository.next_backfill_job()
        assert row is not None
        return row

    def queue(self, conversation=None):
        return self.service.queue_backfill(
            conversation_id=conversation or self.group, max_messages=100
        )["job_ids"][0]

    def restart(self):
        previous = self.service
        self.service = ReaderService(
            self.provider, self.repository, previous.reader, previous.token_codec
        )

    def assert_progress_unchanged(self, identity, original):
        current = self.job(identity)
        for key in ("processed_messages", "cursor_token", "source_inventory_epoch"):
            self.assertEqual(current[key], original[key], key)
        return current

    def test_permanent_bindings_fail_durably_and_explicit_replacement_advances(self):
        for cause in ("epoch", "selected_generation", "cursor", "account"):
            with self.subTest(cause=cause):
                self.setUp()
                identity = self.queue()
                self.assertEqual(
                    self.service.process_backfill_once(batch_limit=2)["processed_messages"], 2
                )
                if cause == "selected_generation":
                    path = self.root / "source/source.json"
                    manifest = json.loads(path.read_text())
                    manifest["shards"][0]["generation_id"] = "synthetic-replaced-generation"
                    path.write_text(json.dumps(manifest))
                elif cause == "cursor":
                    with self.repository.database.transaction() as connection:
                        connection.execute(
                            "UPDATE source_backfill_jobs SET cursor_token=? WHERE job_id=?",
                            ("synthetic-invalid-token", identity),
                        )
                elif cause == "account":
                    # A valid but different account binding is never silently adopted.
                    with self.repository.database.transaction() as connection:
                        connection.execute(
                            "UPDATE accounts SET source_account_key=? WHERE account_id=?",
                            (
                                "synthetic-rebound-source",
                                self.job(identity)["account_id"],
                            ),
                        )
                original = self.job(identity)
                epoch = (
                    mock.patch("sightglass.reader.service.PARSER_VERSION", "synthetic-new-parser")
                    if cause == "epoch"
                    else nullcontext()
                )
                with epoch:
                    with self.assertRaises(SightglassError) as caught:
                        self.service.process_backfill_once(batch_limit=2)
                    self.assertFalse(caught.exception.retryable)
                    self.assertTrue(caught.exception.details["requeue_required"])
                    current = self.assert_progress_unchanged(identity, original)
                    self.assertEqual(current["state"], "failed")
                    self.assertEqual(current["last_error_code"], "SOURCE_GENERATION_CHANGED")
                    self.service.set_backfill_paused(True)
                    self.service.set_backfill_paused(False)
                    self.restart()
                    self.assertEqual(self.job(identity)["state"], "failed")
                    if cause == "account":
                        # Operator restores the source binding, then queues explicitly.
                        with self.repository.database.transaction() as connection:
                            connection.execute(
                                "UPDATE accounts SET source_account_key=? WHERE account_id=?",
                                ("synthetic-account-demo", original["account_id"]),
                            )
                    replacement = self.queue()
                    other = self.queue(self.other)
                    for _ in range(2):
                        self.service.process_backfill_once(batch_limit=2)
                    self.assertGreater(
                        self.job(replacement)["processed_messages"], 0
                    )
                    self.assertGreater(self.job(other)["processed_messages"], 0)
                    self.assert_progress_unchanged(identity, original)

    def test_transient_generation_drift_yields_without_losing_progress(self):
        identity = self.queue()
        self.service.process_backfill_once(batch_limit=2)
        other = self.queue(self.other)
        original = self.job(identity)
        with mock.patch.object(
            self.provider,
            "read_range",
            side_effect=SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True),
        ):
            with self.assertRaises(SightglassError) as caught:
                self.service.process_backfill_once(batch_limit=2)
        self.assertTrue(caught.exception.retryable)
        self.assertEqual(self.assert_progress_unchanged(identity, original)["state"], "running")
        self.restart()
        self.assertEqual(self.next_job()["job_id"], other)
        self.service.process_backfill_once(batch_limit=2)
        self.assertGreater(self.job(other)["processed_messages"], 0)
        self.service.process_backfill_once(batch_limit=2)
        self.assertGreater(self.job(identity)["processed_messages"], 2)

    def test_pause_during_attempt_cannot_be_overwritten_by_error_recording(self):
        identity = self.queue()
        original = self.job(identity)

        def pause(*args, **kwargs):
            self.service.set_backfill_paused(True)
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)

        with mock.patch.object(self.provider, "read_range", side_effect=pause):
            with self.assertRaises(SightglassError):
                self.service.process_backfill_once(batch_limit=2)
        self.assertEqual(self.assert_progress_unchanged(identity, original)["state"], "paused")
        self.restart()
        self.assertEqual(self.service.process_backfill_once()["state"], "idle")
        self.service.set_backfill_paused(False)
        self.assertEqual(self.service.process_backfill_once(batch_limit=2)["processed_messages"], 2)

    def test_expired_attempt_still_yields_durably_to_next_job(self):
        identity = self.queue()
        other = self.queue(self.other)
        original = self.job(identity)

        def deadline(*args, **kwargs):
            time.sleep(0.02)
            check_operation_budget()

        with operation_budget(0.01):
            with mock.patch.object(self.provider, "read_range", side_effect=deadline):
                with self.assertRaises(SightglassError):
                    self.service.process_backfill_once(batch_limit=2)
        self.assertEqual(self.assert_progress_unchanged(identity, original)["state"], "queued")
        self.assertEqual(self.next_job()["job_id"], other)

    def test_live_worker_does_not_retry_permanent_binding_in_same_turn(self):
        service = mock.Mock()
        service.provider.descriptor.source_mode = "live"
        error = SightglassError(
            ErrorCode.SOURCE_GENERATION_CHANGED,
            retryable=False,
            details={"reason": "backfill_epoch_changed"},
        )
        service.process_backfill_once.side_effect = error
        worker = SourceWorker(service)
        with self.assertRaises(SightglassError) as caught:
            worker._process_backfill_once()
        self.assertIs(caught.exception, error)
        service.process_backfill_once.assert_called_once()

"""Stopped synthetic origin upgrade; no source, config, credential or network I/O."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from sightglass.contracts.capture import CaptureAck, CaptureProtocolError, CaptureStreamPosition
from sightglass.runtime.edge import EdgeSpool
from sightglass.source.capture.codec import canonical_json


class EdgeOriginUpgradeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name) / "synthetic-spool"
        self.old = "synthetic-origin-v6"
        self.new = "synthetic-origin-v6-r1"
        self.position = CaptureStreamPosition("synthetic-stream", 18)
        self.spool = EdgeSpool.initialize(
            self.directory, source_instance_id="synthetic-source", account_id="synthetic-account",
            origin_epoch=self.old, stream_epoch=self.position.stream_epoch, next_sequence=18,
            previous_epoch="synthetic-previous-stream", transition_receipt_id="synthetic-receipt",
        )
        self.addCleanup(self.spool.close)
        self.ack = CaptureAck("synthetic-stream", 17, "synthetic-batch", "a" * 64,
                              "synthetic-request", "rejected", "synthetic-core-receipt")
        with self.spool._connection:
            self.spool._connection.execute("UPDATE stream SET last_ack_json=? WHERE singleton=1",
                                           (canonical_json(self.ack).decode(),))
        self.before = self.spool.recovery_state()
        self.raw_before = self.raw_state()

    def raw_state(self):
        return dict(self.spool._connection.execute("SELECT * FROM stream").fetchone())

    def rebind(self, **kwargs):
        return self.spool.rebind_origin(**{
            "expected_state": self.before, "core_position": self.position,
            "new_origin_epoch": self.new, **kwargs,
        })

    def assert_rejected(self, reason, **kwargs):
        before = self.raw_state()
        with self.assertRaises(CaptureProtocolError) as caught:
            self.rebind(**kwargs)
        self.assertEqual(caught.exception.reason, reason)
        self.assertEqual(self.raw_state(), before)
        self.assertFalse(self.spool._connection.in_transaction)

    def reopen(self, origin):
        return EdgeSpool(self.directory, source_instance_id="synthetic-source",
                         account_id="synthetic-account", origin_epoch=origin)

    def test_noninitial_sequence_ack_and_all_other_state_survive_reopen(self):
        with patch.object(self.spool, "pending", side_effect=AssertionError("no payload read")):
            receipt = self.rebind()
        self.assertEqual(receipt, {
            "before": self.before, "after": {**self.before, "origin_epoch": self.new},
        })
        self.assertEqual(self.raw_state(), {**self.raw_before, "origin_epoch": self.new})
        self.assertEqual(self.spool.recovery_state(), receipt["after"])
        self.assertTrue(self.spool.matches_last_ack(self.ack))
        self.spool.close()
        with self.assertRaisesRegex(CaptureProtocolError, "edge_spool_binding_changed"):
            self.reopen(self.old)
        with self.reopen(self.new) as reopened:
            self.assertEqual(reopened.recovery_state(), receipt["after"])
            self.assertTrue(reopened.matches_last_ack(self.ack))
            self.assertEqual(reopened.next_sequence, 18)

    def test_pending_rejects_without_reading_payload(self):
        with self.spool._connection:
            self.spool._connection.execute(
                "INSERT INTO pending VALUES(1,17,?,?,?,?)",
                ("synthetic-batch", "synthetic-request", "a" * 64, 123),
            )
        self.before = self.spool.recovery_state()
        with patch.object(self.spool, "pending", side_effect=AssertionError("no payload read")):
            self.assert_rejected("edge_origin_rebind_requires_empty_spool")

    def test_epoch_loss_rejects(self):
        with self.spool._connection:
            self.spool._connection.execute("UPDATE stream SET epoch_lost=1")
        self.before = self.spool.recovery_state()
        self.assert_rejected("edge_origin_rebind_requires_empty_spool")

    def test_stale_or_inexact_before_image_rejects(self):
        for modified in ({**self.before, "next_sequence": 17},
                         {**self.before, "account_id": "synthetic-other-account"},
                         {**self.before, "epoch_lost": 0},
                         {**self.before, "extra": "synthetic"}):
            with self.subTest(expected=modified):
                self.assert_rejected("edge_origin_before_image_mismatch", expected_state=modified)

    def test_core_stream_or_highwater_mismatch_rejects(self):
        for position in (CaptureStreamPosition("synthetic-other-stream", 18),
                         CaptureStreamPosition("synthetic-stream", 17),
                         CaptureStreamPosition("synthetic-stream", 19),
                         CaptureStreamPosition("synthetic-stream", True)):
            with self.subTest(position=position):
                self.assert_rejected("edge_origin_core_position_mismatch", core_position=position)

    def test_invalid_new_origin_or_repeated_change_rejects_explicitly(self):
        for epoch in ("", "bad\x00epoch", "x" * 65_537, "\ud800"):
            with self.subTest(epoch_length=len(epoch)):
                self.assert_rejected("invalid_origin_epoch", new_origin_epoch=epoch)
        self.rebind()
        self.assert_rejected("edge_origin_rebind_unchanged")
        # A fresh third origin cannot reuse the stale pre-upgrade snapshot.
        self.assert_rejected("edge_origin_before_image_mismatch",
                             new_origin_epoch="synthetic-third-origin")

    def test_process_lock_excludes_another_spool_owner(self):
        with self.assertRaisesRegex(CaptureProtocolError, "edge_spool_in_use_or_unsafe"):
            self.reopen(self.old)

    def test_update_failure_rolls_back_and_retains_old_binding(self):
        self.spool._connection.execute(
            "CREATE TRIGGER synthetic_fail BEFORE UPDATE OF origin_epoch ON stream"
            " BEGIN SELECT RAISE(ABORT, 'synthetic update failure'); END"
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "synthetic update failure"):
            self.rebind()
        self.assertEqual(self.raw_state(), self.raw_before)
        self.assertEqual(self.spool.recovery_state(), self.before)
        self.assertFalse(self.spool._connection.in_transaction)
        self.spool.close()
        with self.reopen(self.old) as reopened:
            self.assertEqual(reopened.recovery_state(), self.before)

    def test_commit_failure_rolls_back_and_does_not_change_memory(self):
        connection = self.spool._connection

        class FailCommit:
            def __getattr__(self, name: str) -> Any:
                return getattr(connection, name)

            def __enter__(self):
                connection.__enter__()
                return self

            def __exit__(self, kind, value, traceback):
                connection.rollback()
                raise sqlite3.OperationalError("synthetic commit failure")

        with patch.object(self.spool, "_connection", FailCommit()):
            with self.assertRaisesRegex(sqlite3.OperationalError, "synthetic commit failure"):
                self.rebind()
        self.assertEqual(self.raw_state(), self.raw_before)
        self.assertEqual(self.spool.recovery_state(), self.before)
        self.assertFalse(connection.in_transaction)
        self.spool.close()
        with self.reopen(self.old) as reopened:
            self.assertEqual(reopened.recovery_state(), self.before)
        with self.assertRaisesRegex(CaptureProtocolError, "edge_spool_binding_changed"):
            self.reopen(self.new)

    def test_closed_or_outer_transaction_or_nonfull_sync_rejects(self):
        self.spool._connection.execute("BEGIN IMMEDIATE")
        with self.assertRaisesRegex(CaptureProtocolError, "requires_own_transaction"):
            self.rebind()
        self.spool._connection.rollback()
        self.spool._connection.execute("PRAGMA synchronous=NORMAL")
        self.assert_rejected("edge_origin_rebind_requires_full_sync")
        self.spool.close()
        with self.assertRaisesRegex(CaptureProtocolError, "edge_spool_closed"):
            self.rebind()

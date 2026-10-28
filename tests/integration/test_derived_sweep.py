"""Generated dense/sparse stock, durable bounded traversal and publication races."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from sightglass.contracts.common import utc_now
from sightglass.contracts.errors import SightglassError
from sightglass.model.db import WindowDB
from sightglass.model.lexical import LEXICAL_RECIPE
from sightglass.model.links import (
    SCAN_KIND,
    LinkRepository,
    input_digest,
    prepare_links,
)
from sightglass.runtime.derived_worker import DerivedIndexWorker
from sightglass.source.links import LINK_EXTRACTION_VERSION
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


class DerivedSweepTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        _, self.repository, self.service, _ = build_test_stack(
            create_synthetic_source(root / "source"), root / "state/window.db"
        )
        self.group = self.service.find_conversations(query="Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        self.service.read_messages(mode="recent", conversation_id=self.group, limit=10, voice="off")
        self.database = self.repository.database
        self.links = LinkRepository(self.database)
        with self.database.connection() as c:
            self.template = dict(c.execute(
                "SELECT * FROM messages WHERE conversation_id=? LIMIT 1", (self.group,)
            ).fetchone())

    def seed(self, count, *, start=0, resident=True):
        """Full generated message/observation/receipt binding, not real-account data."""
        with self.database.transaction() as c:
            for index in range(start, start + count):
                row = {**self.template, "message_id": f"synthetic-sweep-{index:08}",
                       "source_message_id": f"synthetic-sweep-source-{index:08}",
                       "sort_primary": "2099-01-01T00:00:00+00:00", "sort_seq": index,
                       "kind": "text", "text": "Synthetic sweep needle", "search_text": None,
                       "structured_json": "{}", "body_available": int(resident),
                       "first_observation_seq": None, "current_observation_seq": None}
                c.execute("INSERT INTO messages(" + ",".join(row) + ") VALUES ("
                          + ",".join("?" for _ in row) + ")", tuple(row.values()))
                seq = c.execute(
                    "INSERT INTO message_observations(observation_id,message_id,observed_at,"
                    "source_generation_id,state,payload_digest,parsed_json,parser_version) "
                    "VALUES (?,?,'2099-01-01','synthetic-sweep','present',"
                    "'synthetic-only','{}','synthetic-sweep')",
                    (f"synthetic-sweep-observation-{index:08}", row["message_id"]),
                ).lastrowid
                c.execute("UPDATE messages SET first_observation_seq=?,current_observation_seq=? "
                          "WHERE message_id=?", (seq, seq, row["message_id"]))
                if resident:
                    c.execute("INSERT INTO message_link_projection VALUES (?,?,?,?,1,0,?)",
                              (row["message_id"], seq, input_digest(row),
                               LINK_EXTRACTION_VERSION, "2099-01-01"))
                    digest = hashlib.sha256(row["text"].casefold().encode()).hexdigest()
                    c.execute("INSERT INTO message_lexical_projection VALUES (?,?,?,?,?)",
                              (row["message_id"], seq, digest, LEXICAL_RECIPE, "2099-01-01"))
                    c.execute("INSERT INTO message_lexical(rowid,text) SELECT rowid,? "
                              "FROM messages WHERE message_id=?",
                              (row["text"].casefold(), row["message_id"]))

    def scan(self):
        with self.database.connection() as c:
            row = c.execute("SELECT * FROM derived_index_state WHERE index_kind=?",
                            (SCAN_KIND,)).fetchone()
        assert row is not None
        return dict(row)

    def hole(self, index):
        with self.database.transaction() as c:
            c.execute("DELETE FROM message_link_projection WHERE message_id=?",
                      (f"synthetic-sweep-{index:08}",))

    def finish(self, *, limit=40):
        total = 0
        for _ in range(100):
            result = self.links.backfill_batch(limit=limit)
            total += result["processed"]
            if result["state"] == "ready":
                return total
        self.fail("bounded generated sweep did not finish")

    def test_dense_warm_probe_and_selection_have_fixed_vm_work(self):
        self.seed(8000)
        self.hole(7999)
        ticks = 0
        with self.database.read_snapshot() as c:
            def tick():
                nonlocal ticks
                ticks += 1
                return int(ticks > 200)
            c.set_progress_handler(tick, 100)
            try:
                self.assertTrue(self.links.has_pending_backfill())
                plan = self.links._scan_plan(25)
            except sqlite3.DatabaseError as error:
                self.fail(f"bounded generated selection exceeded 20k VM steps: {type(error)}")
            finally:
                c.set_progress_handler(None, 0)
        self.assertEqual(plan.scanned, 25)
        self.assertLess(ticks, 200)

    def test_released_skeletons_do_not_consume_resident_scan_page(self):
        self.seed(5000, resident=False)
        self.seed(3, start=5000)
        plan = self.links._scan_plan(25)
        self.assertLess(plan.scanned, 25)
        self.assertTrue(plan.complete)

    def test_warm_pages_progress_and_restart_reaches_tail_hole(self):
        self.seed(400)
        self.hole(399)
        first = self.links.backfill_batch(limit=40)
        self.assertEqual(first["processed"], 0)
        self.assertEqual(first["state"], "building")
        before = json.loads(self.scan()["recipe"])["position"]
        self.links = LinkRepository(WindowDB(self.database.path))
        self.links.backfill_batch(limit=40)
        after = json.loads(self.scan()["recipe"])["position"]
        self.assertGreater(after, before)
        self.assertEqual(self.finish(), 1)

    def test_warm_verification_preserves_published_readiness_and_restarts_pass(self):
        self.links.backfill_batch()
        self.seed(120)
        first = self.links.backfill_batch(limit=40)
        self.assertEqual(first["state"], "building")
        self.assertEqual(self.links.state()["state"], "ready")
        self.finish()
        self.assertEqual(self.scan()["state"], "ready")
        self.hole(0)
        self.assertTrue(self.links.has_pending_backfill())
        self.assertGreater(self.links.backfill_batch(limit=40)["processed"], 0)

    def test_actual_worker_continues_warm_pages_until_tail_publication(self):
        self.links.backfill_batch()
        self.seed(400)
        self.hole(399)
        # Explicitly begin the next bounded audit; a current ready pass otherwise
        # waits for its periodic verification deadline to avoid rescanning warm stock.
        self.links.backfill_batch(limit=40)
        worker = DerivedIndexWorker(self.links, self.service.reader)
        self.assertTrue(worker.run_once())
        self.assertEqual(worker.status()["processed_messages"], 0)
        self.assertEqual(self.links.state()["state"], "ready")
        for _ in range(10):
            if not worker.run_once():
                break
        else:
            self.fail("generated worker did not finish its frozen resident pass")
        self.assertEqual(worker.status()["processed_messages"], 1)
        self.assertGreaterEqual(worker.status()["scanned_messages"], 350)
        self.assertIsNone(worker.status()["last_error_code"])

    def test_completed_large_warm_sweep_waits_then_rechecks_older_tail_hole(self):
        self.seed(400)
        self.finish()
        self.hole(399)
        self.assertFalse(self.links.has_pending_backfill())
        later = utc_now() + timedelta(hours=2)
        with patch("sightglass.model.links.utc_now", return_value=later):
            self.assertTrue(self.links.has_pending_backfill())
            self.assertEqual(self.finish(), 1)

    def test_generation_invalidation_cannot_inherit_warm_sweep_delay(self):
        self.seed(400)
        self.finish()
        self.hole(399)
        with self.database.transaction() as c:
            c.execute("UPDATE derived_index_state SET generation=generation+1 "
                      "WHERE index_kind=?", (SCAN_KIND,))
        self.assertTrue(self.links.has_pending_backfill())
        self.assertEqual(self.finish(), 1)

    def test_missing_receipt_behind_cursor_is_repaired_next_sweep(self):
        self.seed(120)
        self.links.backfill_batch(limit=40)
        self.hole(0)
        self.assertEqual(self.finish(), 0)
        self.assertGreater(self.links.backfill_batch(limit=40)["processed"], 0)

    def test_frozen_upper_finishes_despite_new_appends(self):
        self.seed(120)
        self.links.backfill_batch(limit=40)
        upper = json.loads(self.scan()["recipe"])["upper"]
        self.seed(120, start=120)
        self.finish()
        self.assertEqual(json.loads(self.scan()["recipe"])["upper"], upper)
        self.links.backfill_batch(limit=40)
        self.assertGreater(json.loads(self.scan()["recipe"])["upper"], upper)

    def test_publication_failure_cannot_advance_scan(self):
        self.seed(120)
        self.links.backfill_batch(limit=40)
        self.hole(50)
        before = self.scan()
        with patch("sightglass.model.links.publish_links", return_value=False):
            with self.assertRaises(SightglassError):
                self.links.backfill_batch(limit=40)
        self.assertEqual(self.scan(), before)
        self.assertEqual(self.links.backfill_batch(limit=40)["processed"], 1)

    def test_rebuild_during_preparation_fences_old_scan_and_publication(self):
        self.seed(120)
        self.links.backfill_batch(limit=40)
        self.hole(50)
        def rebuild(row):
            self.links.request_rebuild("lexical")
            return prepare_links(row)
        with patch("sightglass.model.links.prepare_links", side_effect=rebuild):
            with self.assertRaises(SightglassError):
                self.links.backfill_batch(limit=40)
        self.assertIsNone(json.loads(self.scan()["recipe"])["position"])
        self.assertGreater(self.links.backfill_batch(limit=40)["processed"], 0)

    def test_recipe_change_resets_optimization_without_losing_holes(self):
        self.seed(120)
        self.links.backfill_batch(limit=40)
        self.hole(0)
        with self.database.transaction() as c:
            c.execute("UPDATE derived_index_state SET recipe='synthetic-retired-scan' "
                      "WHERE index_kind=?", (SCAN_KIND,))
        self.assertGreater(self.links.backfill_batch(limit=40)["processed"], 0)

    def test_expired_positions_allow_progress_without_regrowing_receipts(self):
        self.seed(120)
        with self.database.transaction() as c:
            for index in range(40):
                c.execute("INSERT INTO message_body_residency VALUES "
                          "(?,?,'on_demand',10,'2000-01-01','2000-01-02')",
                          (f"synthetic-sweep-{index:08}", self.group))
                c.execute("DELETE FROM message_link_projection WHERE message_id=?",
                          (f"synthetic-sweep-{index:08}",))
        self.assertEqual(self.finish(), 0)
        self.assertEqual(self.links.backfill_batch(limit=40)["processed"], 0)


if __name__ == "__main__":
    unittest.main()

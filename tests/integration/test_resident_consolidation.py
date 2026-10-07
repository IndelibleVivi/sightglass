"""Generated end-to-end invariants for the resident/current-body convergence."""

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from sightglass.model.current_body import (
    LegacyBodyConflict,
    current_search_document,
    message_view,
    normalized_columns,
)
from sightglass.model.db import WindowDB
from sightglass.model.lexical import publish_lexical
from sightglass.model.links import LinkRepository, prepare_links, publish_links
from sightglass.reader.projections import CompactMessageProjector, DetailMessageProjector
from sightglass.residency.repository import ResidencyRepository
from sightglass.runtime.derived_worker import DerivedIndexWorker
from sightglass.semantic.service import _encoder_input
from sightglass.source.parser import parse_message
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


class CurrentBodyCompatibilityTests(unittest.TestCase):
    def test_null_and_empty_search_documents_preserve_legacy_literal_behavior(self):
        for search, body, expected in (
            (None, None, ""), (None, "Synthetic body", "Synthetic body"),
            ("", "Synthetic body", "Synthetic body"), ("", None, ""),
            ("Synthetic distinct card", "Synthetic body", "Synthetic distinct card"),
        ):
            with self.subTest(search=search, body=body):
                self.assertEqual(current_search_document({"search_text": search, "text": body}),
                                 expected)

    def test_exact_normalization_retains_distinct_empty_and_card_fields(self):
        for body in (None, "", "Synthetic body"):
            row = {"text": body, "search_text": body,
                   "structured_json": json.dumps({"kind": "text", "text": body})}
            result = normalized_columns(row)
            self.assertIsNone(result["search_text"])
            structured = result["structured_json"]
            assert isinstance(structured, str)
            self.assertNotIn("text", json.loads(structured))
        distinct = {"text": None, "search_text": "", "structured_json": "{}"}
        self.assertEqual(normalized_columns(distinct)["search_text"], "")
        conflict = {"text": "Synthetic A", "search_text": "Synthetic A",
                    "structured_json": json.dumps({"text": "Synthetic B"})}
        with self.assertRaises(LegacyBodyConflict):
            normalized_columns(conflict)

    def test_corrupt_normalization_fails_without_rendering_the_payload(self):
        for structured in ('["Synthetic private payload"]', '{"Synthetic private payload"'):
            with (self.subTest(structured=structured),
                  self.assertRaises(LegacyBodyConflict) as error):
                normalized_columns({"text": None, "search_text": None,
                                    "structured_json": structured})
            self.assertNotIn("Synthetic private payload", str(error.exception))
        self.assertIsNone(normalized_columns(
            {"text": None, "search_text": None, "structured_json": None}
        )["structured_json"])

    def test_semantic_v2_input_is_invariant_for_legacy_and_normalized_card(self):
        legacy = {
            "kind": "link", "text": "Synthetic body", "search_text": "Synthetic joined card",
            "structured_json": json.dumps({"kind": "link", "text": "Synthetic body",
                                           "title": "Synthetic title",
                                           "description": "Synthetic description"}),
        }
        normalized = {**legacy, **normalized_columns(legacy)}
        expected = ("Synthetic joined card\nSynthetic body\nSynthetic title\n"
                    "Synthetic description")
        self.assertEqual(_encoder_input(legacy), expected)
        self.assertEqual(_encoder_input(normalized), expected)
        self.assertEqual(message_view(legacy), message_view(normalized))


class ResidentConsolidationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        source = create_synthetic_source(root / "source")
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            source, root / "state" / "window.db", residency_default=None,
        )
        self.database = self.repository.database
        self.group = self.service.find_conversations(query="Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        self.service.read_messages(mode="recent", conversation_id=self.group, limit=10, voice="off")
        self.epoch = self.service._projection_inventory_epoch()
        self.seed = self.repository.materialized_message_rows(
            self.group, projection_epoch=self.epoch,
            observation_watermark=self.repository.observation_watermark(), limit=1,
            direction="forward",
        )[0]
        context = self.repository.conversation_context(self.group)
        assert context is not None
        with self.provider.snapshot() as snapshot:
            self.original = self.provider.get_message(
                context["source_account_key"], self.seed["source_message_id"], snapshot
            )
        self.assertIsNotNone(self.original)
        self.links = LinkRepository(self.database)
        self.residency = ResidencyRepository(self.database)

    def tearDown(self):
        self.temporary.cleanup()

    def admit(self, token: str, text: str) -> str:
        assert self.original is not None
        source = replace(
            self.original, source_message_id=f"synthetic-consolidation-{token}",
            wechat_type=1, raw_content=text, source_rowid=90_001, sort_seq=90_001,
            sent_at_utc=datetime(2026, 9, 25, tzinfo=UTC).isoformat(),
        )
        return self.repository.upsert_message(
            self.seed["account_id"], self.group, self.seed["sender_id"],
            self.seed["sender_membership_id"], source, parse_message(source),
            projection_epoch=self.epoch,
        )

    def row(self, message_id):
        with self.database.connection() as connection:
            return connection.execute("SELECT rowid,* FROM messages WHERE message_id=?",
                                      (message_id,)).fetchone()

    def counts(self):
        with self.database.connection() as connection:
            return {
                table: connection.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
                for table in ("messages", "message_observations", "message_links",
                              "message_link_projection", "message_lexical_projection",
                              "message_lexical")
            }

    def release(self):
        self.residency.release_stock(
            self.group, plan=self.residency.stock_preview(self.group)["plan"]
        )

    def test_release_worker_restart_both_rebuilds_never_regrow_derivatives(self):
        before = self.counts()
        self.release()
        self.links.backfill_batch()
        restarted = LinkRepository(WindowDB(self.database.path))
        for kind in ("links", "lexical"):
            restarted.request_rebuild(kind)
            self.assertEqual(restarted.backfill_batch()["processed"], 0)
        after = self.counts()
        self.assertEqual(after["messages"], before["messages"])
        self.assertEqual(after["message_observations"], before["message_observations"])
        for table in ("message_links", "message_link_projection", "message_lexical_projection",
                      "message_lexical"):
            self.assertEqual(after[table], 0, table)

    def test_same_source_rehydration_preserves_episode_and_rebuilds_on_admission(self):
        before = self.row(self.seed["message_id"])
        observation_count = self.counts()["message_observations"]
        self.release()
        self.service.read_messages(mode="recent", conversation_id=self.group, limit=10,
                                   refresh=True, voice="off")
        after = self.row(before["message_id"])
        self.assertEqual(after["rowid"], before["rowid"])
        self.assertEqual(after["current_observation_seq"], before["current_observation_seq"])
        self.assertEqual(self.counts()["message_observations"], observation_count)
        self.assertEqual(after["body_available"], 1)
        self.assertGreater(self.counts()["message_lexical"], 0)

    def test_both_renderers_preserve_legacy_and_normalized_output(self):
        normalized = dict(self.seed)
        legacy = {**normalized, "structured_json": json.dumps(message_view(normalized)),
                  "search_text": current_search_document(normalized)}
        detail = DetailMessageProjector(self.repository, self.service.token_codec)
        options = {"timezone_name": "UTC", "include_resources": True, "focus_ids": ()}
        self.assertEqual(
            detail.project([legacy], **options), detail.project([normalized], **options)
        )
        compact = CompactMessageProjector(self.repository)
        compact_options = {"timezone_name": "UTC", "include_resource_indicators": True,
                           "focus_message_ids": frozenset(), "context_only_ids": frozenset(),
                           "late_arrival_ids": frozenset()}
        self.assertEqual(compact.prepare([legacy], **compact_options),
                         compact.prepare([normalized], **compact_options))

    def test_zero_resident_keep_scope_can_restore_through_explicit_source_backfill(self):
        count = self.counts()["message_observations"]
        self.release()
        self.residency.set(self.group, mode="keep")
        self.assertNotIn(self.group, self.repository.resident_conversation_ids(
            self.seed["account_id"], self.epoch
        ))
        queued = self.service.queue_backfill(conversation_id=self.group)
        self.assertEqual(queued["queued_job_count"], 1)
        self.service.process_backfill_once(batch_limit=20)
        self.assertIn(self.group, self.repository.resident_conversation_ids(
            self.seed["account_id"], self.epoch
        ))
        self.assertEqual(self.counts()["message_observations"], count)

    def test_a_b_a_rejects_old_prepared_lexical_and_links(self):
        identifier = self.admit("episode", "Synthetic A")
        first = self.row(identifier)
        prepared = prepare_links(first)
        self.admit("episode", "Synthetic B")
        self.admit("episode", "Synthetic A")
        last = self.row(identifier)
        self.assertGreater(last["current_observation_seq"], first["current_observation_seq"])
        self.assertEqual(last["rowid"], first["rowid"])
        with self.database.transaction() as connection:
            self.assertFalse(publish_lexical(connection, first, historical=True))
            self.assertFalse(publish_links(connection, prepared, historical=True))

    def test_release_and_expiry_between_prepare_and_publish_fail_closed(self):
        identifier = self.admit("expired", "Synthetic expiryneedle")
        captured = self.row(identifier)
        prepared = prepare_links(captured)
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO message_body_residency "
                "VALUES (?,?,'on_demand',100,'2000-01-01T00:00:00+00:00',"
                "'2000-01-02T00:00:00+00:00')", (identifier, self.group)
            )
            self.assertFalse(publish_lexical(connection, captured, historical=True))
            self.assertFalse(publish_links(connection, prepared, historical=True))
        self.release()
        with self.database.transaction() as connection:
            self.assertFalse(publish_lexical(connection, captured, historical=True))

    def test_later_lexical_gate_failure_cannot_commit_links(self):
        identifier = self.admit("boundary", "Synthetic boundaryneedle")
        prepared = prepare_links(self.row(identifier))
        with self.database.transaction() as connection, patch(
            "sightglass.model.links.publish_lexical", return_value=False
        ):
            self.assertFalse(publish_links(connection, prepared, historical=True))

    def test_missing_receipt_behind_checkpoint_converges(self):
        while self.links.backfill_batch()["state"] != "ready":
            pass
        identifier = self.seed["message_id"]
        with self.database.transaction() as connection:
            connection.execute("DELETE FROM message_link_projection WHERE message_id=?",
                               (identifier,))
            connection.execute("UPDATE derived_index_state SET checkpoint_seq=1000000 "
                               "WHERE index_kind='links'")
        self.assertGreater(self.links.backfill_batch()["processed"], 0)
        with self.database.connection() as connection:
            self.assertIsNotNone(connection.execute(
                "SELECT 1 FROM message_link_projection WHERE message_id=?", (identifier,)
            ).fetchone())

    def test_ready_index_holes_preserve_candidate_recall(self):
        identifier = self.admit("recall", "Synthetic uniqueneedle")
        while self.links.backfill_batch()["state"] != "ready":
            pass
        for missing in ("receipt", "stale_episode", "fts"):
            with self.subTest(missing=missing), self.database.transaction() as connection:
                if missing == "receipt":
                    connection.execute("DELETE FROM message_lexical_projection WHERE message_id=?",
                                       (identifier,))
                elif missing == "stale_episode":
                    connection.execute("UPDATE message_lexical_projection SET "
                                       "source_observation_seq=0 WHERE message_id=?", (identifier,))
                else:
                    connection.execute("DELETE FROM message_lexical WHERE rowid=?",
                                       (self.row(identifier)["rowid"],))
                found = self.repository.search_candidate_window(
                    (self.group,), projection_epoch=self.epoch,
                    lexical_queries=("uniqueneedle",), limit=100,
                )
                self.assertIn(identifier, [row["message_id"] for row in found])
                # Re-publish through explicit rebuild before introducing the next hole.
            self.links.request_rebuild("lexical")
            while self.links.backfill_batch()["state"] != "ready":
                pass

    def test_backfill_repairs_missing_fts_row_with_current_receipt(self):
        identifier = self.admit("fts-hole", "Synthetic repairneedle")
        while self.links.backfill_batch()["state"] != "ready":
            pass
        with self.database.transaction() as connection:
            connection.execute("DELETE FROM message_lexical WHERE rowid=?",
                               (self.row(identifier)["rowid"],))
        self.assertEqual(self.links.state()["state"], "ready")
        worker = DerivedIndexWorker(self.links, self.service.reader)
        worker.run_once()
        self.assertGreater(worker.status()["processed_messages"], 0)
        before = self.database.writer_status()["wait_count"]
        self.assertFalse(worker.run_once())
        self.assertEqual(self.links.backfill_batch()["processed"], 0)
        self.assertEqual(self.database.writer_status()["wait_count"], before)
        with self.database.connection() as connection:
            self.assertIsNotNone(connection.execute(
                "SELECT 1 FROM message_lexical WHERE rowid=?",
                (self.row(identifier)["rowid"],),
            ).fetchone())

    def test_nonpresent_body_does_not_keep_backfill_active(self):
        identifier = self.admit("not-present", "Synthetic old body")
        with self.database.transaction() as connection:
            connection.execute("UPDATE messages SET current_state='recalled' WHERE message_id=?",
                               (identifier,))
            connection.execute("DELETE FROM message_link_projection WHERE message_id=?",
                               (identifier,))
        while self.links.backfill_batch()["state"] != "ready":
            pass
        self.assertEqual(self.links.backfill_batch()["processed"], 0)
        with self.database.connection() as connection:
            self.assertIsNone(connection.execute(
                "SELECT 1 FROM message_link_projection WHERE message_id=?", (identifier,)
            ).fetchone())

    def test_plain_body_single_copy_and_short_unicode_literal_matching(self):
        identifier = self.admit("unicode", "Synthetic C++ 中文 短词 Straße")
        row = self.row(identifier)
        self.assertIsNone(row["search_text"])
        self.assertNotIn("text", json.loads(row["structured_json"]))
        for query in (("中文",), ("短词",), ("c++",), ("strasse",), ("中文", "c++")):
            self.assertTrue(self.service._search_text_matches(row, query))
        self.assertFalse(self.service._search_text_matches(row, ("中文", "absent")))

    def test_resident_conversation_probe_preserves_exact_account_and_epoch_fence(self):
        self.release()
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO accounts(account_id,source_namespace,identity_confidence,"
                "reader_timezone,current_display_name,first_seen_at,last_seen_at) VALUES "
                "('synthetic-other-account','synthetic-consolidation','high','UTC',"
                "'Synthetic other account','2026-01-01','2026-01-01')"
            )
            # A deliberately mismatched FK-valid row must not satisfy another
            # account's resident probe merely by naming its conversation.
            connection.execute(
                "INSERT INTO messages(message_id,account_id,conversation_id,source_message_id,"
                "source_time_raw,sent_at_utc,sort_primary,sort_seq,sort_tie,"
                "sender_label_snapshot_json,kind,text,structured_json,first_seen_at,last_seen_at,"
                "current_state,current_generation_id,projection_epoch,body_available) "
                "VALUES ('synthetic-misbound-body','synthetic-other-account',?,"
                "'synthetic-misbound-source','2026-01-01','2026-01-01','2026-01-01',1,0,"
                "'{}','text','Synthetic other body','{}','2026-01-01','2026-01-01','present',"
                "'synthetic-generation',?,1)", (self.group, self.epoch),
            )
        self.assertNotIn(self.group, self.repository.resident_conversation_ids(
            self.seed["account_id"], self.epoch
        ))
        self.admit("valid-account", "Synthetic valid body")
        self.assertIn(self.group, self.repository.resident_conversation_ids(
            self.seed["account_id"], self.epoch
        ))
        self.assertNotIn(self.group, self.repository.resident_conversation_ids(
            self.seed["account_id"], "synthetic-other-epoch"
        ))


class ResidentScaleTests(unittest.TestCase):
    def test_history_scale_does_not_change_fixed_resident_query_work(self):
        path = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-resident-lifecycle.py"
        spec = importlib.util.spec_from_file_location("synthetic_lifecycle_benchmark", path)
        assert spec is not None and spec.loader is not None
        benchmark = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(benchmark)
        with tempfile.TemporaryDirectory() as temporary:
            for resident in (0, 10):
                for count in (10_000, 100_000):
                    result = benchmark.scale_probe(Path(temporary), count, resident)
                    self.assertEqual(
                        result["resident_conversations"]["returned"], int(resident > 0)
                    )
                    self.assertLess(result["resident_conversations"]["vm_steps_upper_bound"], 3000)
                    self.assertEqual(result["warm_candidate_page"]["returned"], min(5, resident))
                    self.assertLess(result["warm_candidate_page"]["vm_steps_upper_bound"], 4000)
                    for page in ("warm_forward_page", "warm_backward_page"):
                        self.assertEqual(result[page]["returned"], min(5, resident))
                        self.assertLess(result[page]["vm_steps_upper_bound"], 4000, page)
                    if resident:
                        self.assertEqual(result["warm_forward_page"]["sort_seqs"], list(range(5)))
                        self.assertEqual(
                            result["warm_backward_page"]["sort_seqs"], list(range(5, 10))
                        )
                    for probe in ("warm_bounds", "warm_read_plane", "warm_scope_present"):
                        self.assertEqual(result[probe]["returned"],
                                         (2 if probe == "warm_bounds" else 1) * int(resident > 0))
                        self.assertLess(result[probe]["vm_steps_upper_bound"], 4000, probe)

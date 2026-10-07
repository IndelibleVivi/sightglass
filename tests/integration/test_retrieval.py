from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from sightglass.model.links import LinkRepository, prepare_links, publish_links
from sightglass.policy.readers import ReaderPolicy
from sightglass.source.message_identity import native_message_token
from sightglass.source.parser import parse_message
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack
from tests.fixtures.retrieval import seed_aggregate_discussion


class RetrievalTests(unittest.TestCase):
    def test_quoted_phrase_uses_same_literal_terms_for_index_and_final_match(self):
        message_id = self.fixture["admit"](
            "phrase-target",
            "Synthetic exact phrase target",
            self.fixture["moment"] + timedelta(days=2),
        )
        while self.links.backfill_batch()["state"] != "ready":
            pass
        found = self.tools.wechat_retrieve(
            '"exact phrase"', conversation_ids=[self.fixture["group"]]
        )
        self.assertTrue(found["contexts"])
        self.assertIn(message_id, found["contexts"][0]["focus_message_ids"])
        self.assertEqual(found["lanes"]["lexical"], "trigram_candidates")

    def test_dense_chat_link_neighbor_survives_body_radius(self):
        instant = self.fixture["moment"] + timedelta(days=2)
        admit = self.fixture["admit"]
        first_id = admit("dense-first", "https://dense-ailover.example/", instant)
        for index in range(50):
            admit(
                f"dense-noise-{index}",
                "Synthetic interleaved chatter",
                instant + timedelta(seconds=index * 2 + 1),
            )
        second_id = admit(
            "dense-second", "https://dense-lutopia.example/", instant + timedelta(seconds=148)
        )
        found = self.tools.wechat_retrieve(
            "两个聚合项目",
            hints=["dense-ailover.example"],
            kinds=["link"],
            count_hint=2,
            conversation_ids=[self.fixture["group"]],
        )
        first = found["contexts"][0]
        self.assertEqual(
            {link["normalized_host"] for link in first["links"]},
            {"dense-ailover.example", "dense-lutopia.example"},
        )
        self.assertIn(first_id, first["focus_message_ids"])
        self.assertIn(second_id, [row[0] for row in first["messages"]])
        self.assertLessEqual(len(first["messages"]), 32)

    def test_native_reply_edge_recovers_distant_canonical_target_with_hard_time_scope(self):
        original = self.fixture["original"]
        group = self.fixture["group"]
        seed = self.fixture["seed"]
        instant = self.fixture["moment"] + timedelta(days=2)
        epoch = self.service._projection_inventory_epoch()
        target = replace(
            original,
            source_message_id=native_message_token(
                original.source_conversation_id, "server", (731,)
            ),
            raw_content="Synthetic original project context",
            wechat_type=1,
            sent_at_utc=instant.isoformat(),
            source_rowid=731,
            sort_seq=731,
        )
        target_id = self.repository.upsert_message(
            self.fixture["account"],
            group,
            seed["sender_id"],
            seed["sender_membership_id"],
            target,
            parse_message(target),
            projection_epoch=epoch,
        )
        reply = replace(
            target,
            source_message_id=native_message_token(
                original.source_conversation_id, "server", (732,)
            ),
            source_rowid=732,
            sort_seq=732,
            sent_at_utc=(instant + timedelta(minutes=20)).isoformat(),
            wechat_type=49,
            raw_content="<msg><appmsg><type>57</type>"
            "<title>Synthetic replyneedle</title><refermsg><svrid>731</svrid>"
            "<content>Synthetic quote</content></refermsg></appmsg></msg>",
        )
        reply_id = self.repository.upsert_message(
            self.fixture["account"],
            group,
            seed["sender_id"],
            seed["sender_membership_id"],
            reply,
            parse_message(reply),
            projection_epoch=epoch,
        )
        while self.links.backfill_batch()["state"] != "ready":
            pass
        found = self.tools.wechat_retrieve("replyneedle", conversation_ids=[group])
        first = found["contexts"][0]
        self.assertIn(target_id, [row[0] for row in first["messages"]])
        self.assertEqual(
            first["reply_edges"], [{"from_message_id": reply_id, "to_message_id": target_id}]
        )
        self.assertIn("reply_edge", first["matched_by"])
        bounded = self.tools.wechat_retrieve(
            "replyneedle",
            conversation_ids=[group],
            after=(instant + timedelta(minutes=10)).isoformat(),
        )
        self.assertNotIn(target_id, [row[0] for row in bounded["contexts"][0]["messages"]])
        self.assertEqual(bounded["contexts"][0]["reply_edges"], [])
        self.assertNotIn("reply_target_source_message_id", json.dumps(first))

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        source = create_synthetic_source(root / "source")
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            source,
            root / "state" / "window.db",
        )
        self.fixture = seed_aggregate_discussion(self.provider, self.repository, self.service)
        self.links = LinkRepository(self.repository.database)

    def tearDown(self):
        self.tools.close()
        self.temporary.cleanup()

    def _read_state(self):
        with self.repository.database.connection() as connection:
            return {
                table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table}")]
                for table in (
                    "reader_timeline_cursors",
                    "reader_update_cursors",
                    "reader_deliveries",
                    "voice_jobs",
                    "resource_jobs",
                )
            }

    def test_approximate_aggregate_first_page_finds_original_pair_and_preserves_state(self):
        before = self._read_state()
        with patch.object(self.provider, "snapshot", side_effect=AssertionError("source opened")):
            result = self.tools.wechat_retrieve(
                "两个聚合 AI companion 项目",
                hints=["ailovers.example"],
                conversation_ids=[self.fixture["group"]],
                kinds=["link"],
                count_hint=2,
            )
        self.assertEqual(result["schema"], "sightglass.retrieval-results.v1")
        self.assertTrue(result["contexts"])
        first = result["contexts"][0]
        hosts = {link["normalized_host"] for link in first["links"]}
        self.assertEqual(hosts, {"www.ailover-atlas.example", "lutopia.example"})
        self.assertIn("hostname_hint_fuzzy", first["matched_by"])
        self.assertIn("link_cooccurrence", first["matched_by"])
        self.assertIn(self.fixture["ids"]["interleaved"], [row[0] for row in first["messages"]])
        self.assertIn("context", first["markers"])
        self.assertEqual(before, self._read_state())
        self.assertEqual(result["freshness"], "materialized_observed")
        self.assertFalse(result["source_receipt"]["freshness"]["live_refresh_confirmed"])

    def test_candidate_batch_continuation_never_repeats_focus_messages(self):
        seen = set()
        cursor = None
        with (
            patch("sightglass.reader.retrieval.LINK_CANDIDATE_BUDGET", 2),
            patch("sightglass.reader.retrieval.LEXICAL_CANDIDATE_BUDGET", 200),
        ):
            for _ in range(25):
                result = self.tools.wechat_retrieve(
                    "project", kinds=["link"], cursor=cursor, limit=1
                )
                self.assertEqual(result.get("schema"), "sightglass.retrieval-results.v1")
                for context in result["contexts"]:
                    focus = set(context["focus_message_ids"])
                    self.assertFalse(seen & focus)
                    seen.update(focus)
                cursor = result["page"]["next_cursor"]
                if cursor is None:
                    break
            else:
                self.fail("bounded corpus continuation did not finish")
        self.assertTrue(seen)

    def test_large_context_bodies_fit_policy_and_expose_truncation(self):
        self.fixture["admit"](
            "large-body", "project " + "synthetic prose " * 4000, self.fixture["moment"]
        )
        self.service.reader.policy = ReaderPolicy(
            mode="all_except_denylist",
            max_compact_payload_chars=16000,
            max_compact_body_chars_per_message=500,
        )
        result = self.tools.wechat_retrieve(
            "project", limit=2, after=(self.fixture["moment"] - timedelta(seconds=1)).isoformat()
        )
        self.assertEqual(result.get("schema"), "sightglass.retrieval-results.v1")
        self.assertLessEqual(len(json.dumps(result, ensure_ascii=False)), 16000)
        self.assertTrue(
            any(
                context["projection_receipt"]["body_truncated_rows"]
                for context in result["contexts"]
            )
        )

    def test_lexical_rebuild_preserves_strict_recall_and_stales_retrieval(self):
        while self.links.backfill_batch()["state"] != "ready":
            pass
        indexed = self.repository.search_candidate_window(
            (self.fixture["group"],), lexical_queries=("ailover",), limit=100
        )
        with self.repository.database.connection() as connection:
            reference = {
                row["message_id"]
                for row in connection.execute(
                    "SELECT * FROM messages WHERE conversation_id=?", (self.fixture["group"],)
                )
                if "ailover" in str(row["search_text"] or row["text"] or "").casefold()
            }
        self.assertEqual({row["message_id"] for row in indexed}, reference)
        first = self.tools.wechat_retrieve("project", kinds=["link"], limit=1)
        cursor = first["page"]["next_cursor"]
        self.assertIsNotNone(cursor)
        self.links.request_rebuild("lexical")
        self.assertEqual(
            self.tools.wechat_retrieve("project", kinds=["link"], limit=1, cursor=cursor)["code"],
            "CURSOR_STALE",
        )
        fallback = self.repository.search_candidate_window(
            (self.fixture["group"],), lexical_queries=("ailover",), limit=2000
        )
        self.assertGreater(len(fallback), len(indexed))

    def test_reader_startup_uses_generation_readiness_without_full_statistics(self):
        from sightglass.reader.retrieval import RetrievalService

        with patch.object(
            RetrievalService, "status", side_effect=AssertionError("full-history status scan")
        ):
            _, _, service, tools = build_test_stack(
                Path(self.temporary.name) / "source",
                Path(self.temporary.name) / "cold" / "window.db"
            )
        self.assertEqual(service._cold_status["readiness"]["link_index"], "building")
        self.assertEqual(service._cold_status["readiness"]["retrieval"], "degraded")
        tools.close()

    def test_generation_readiness_refuses_stale_recipes(self):
        while self.links.backfill_batch()["state"] != "ready":
            pass
        self.assertEqual(self.service.retrieval.readiness()["retrieval"], "ready")
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE derived_index_state SET recipe='synthetic-retired-recipe'"
            )
        readiness = self.service.retrieval.readiness()
        self.assertEqual(readiness["link_index"], "building")
        self.assertEqual(readiness["lexical_index"], "building")
        self.assertEqual(readiness["retrieval"], "degraded")

    def test_operator_pause_stops_derived_backfill_and_resume_completes(self):
        from sightglass.runtime.derived_worker import DerivedIndexWorker

        self.links.request_rebuild()
        paused_reader = replace(self.tools.service.reader, paused=True)
        worker = DerivedIndexWorker(self.links, paused_reader)
        before = self.links.state()
        for _ in range(2):
            self.assertFalse(worker.run_once())
            self.assertEqual(self.links.state(), before)
        self.assertEqual(worker.status()["processed_messages"], 0)
        self.assertTrue(worker.status()["paused"])
        worker.reader = replace(paused_reader, paused=False)
        while worker.run_once():
            pass
        self.assertEqual(self.links.state()["state"], "ready")
        self.assertFalse(worker.status()["paused"])
        self.assertGreater(worker.status()["processed_messages"], 1205)

    def test_derived_worker_pauses_without_checkpoint_and_resumes(self):
        from unittest.mock import Mock

        from sightglass.contracts.errors import ErrorCode, SightglassError
        from sightglass.runtime.derived_worker import DerivedIndexWorker

        worker = DerivedIndexWorker(self.links, self.tools.service.reader)
        self.links.request_rebuild()
        before = self.links.state()["checkpoint_seq"]
        previous = self.repository.database.storage
        budget = Mock()
        budget.require.side_effect = SightglassError(ErrorCode.STORAGE_PRESSURE)
        self.repository.database.storage = budget
        try:
            self.assertFalse(worker.run_once())
            self.assertEqual(self.links.state()["checkpoint_seq"], before)
            self.assertTrue(worker.status()["paused_for_storage"])
        finally:
            self.repository.database.storage = previous
        while worker.run_once():
            pass
        self.assertEqual(self.links.state()["state"], "ready")
        self.assertFalse(worker.status()["paused_for_storage"])
        self.assertGreater(worker.status()["processed_messages"], 1205)

    def test_exact_domains_and_full_url_projection_do_not_network_fetch(self):
        result = self.tools.wechat_find_links(domains=["lutopia.example"])
        self.assertEqual(len(result["items"]), 1)
        self.assertEqual(
            result["items"][0]["raw_url"],
            "https://lutopia.example/companion/?token=synthetic#fragment",
        )
        self.assertTrue(result["items"][0]["context_anchor"])
        with self.repository.database.connection() as connection:
            receipts = [
                str(tuple(row)) for row in connection.execute("SELECT * FROM access_receipts")
            ]
        self.assertFalse(any("lutopia" in row or "token=synthetic" in row for row in receipts))

    def test_policy_filter_and_cross_conversation_context_boundary(self):
        self.service.reader.policy = ReaderPolicy(
            mode="all_except_denylist",
            denied_conversation_ids=frozenset({self.fixture["direct"]}),
        )
        result = self.tools.wechat_retrieve("Synthetic projects", kinds=["link"], count_hint=2)
        self.assertTrue(result["contexts"])
        self.assertTrue(
            all(
                context["conversation_id"] == self.fixture["group"]
                for context in result["contexts"]
            )
        )
        denied = self.tools.wechat_find_links(conversation_ids=[self.fixture["direct"]])
        self.assertEqual(denied["code"], "POLICY_DENIED")

    def test_link_cursor_excludes_appends_and_stales_after_old_link_repair(self):
        first = self.tools.wechat_find_links(limit=1)
        cursor = first["page"]["next_cursor"]
        self.assertIsNotNone(cursor)
        self.fixture["admit"](
            "later", "https://later.example/", self.fixture["moment"] + timedelta(days=3)
        )
        second = self.tools.wechat_find_links(limit=1, cursor=cursor)
        self.assertEqual(second["schema"], "sightglass.link-search.v1")
        self.assertNotEqual(second["items"][0]["normalized_host"], "later.example")
        message = self.repository.frozen_message_rows((self.fixture["ids"]["first"],))[0]
        source = replace(
            self.fixture["original"],
            source_message_id=message["source_message_id"],
            conversation_kind="direct",
            raw_content="https://replaced.example/",
            wechat_type=1,
            sent_at_utc=message["sent_at_utc"],
        )
        self.repository.upsert_message(
            message["account_id"],
            message["conversation_id"],
            message["sender_id"],
            message["sender_membership_id"],
            source,
            parse_message(source),
            projection_epoch=self.service._projection_inventory_epoch(),
        )
        self.assertEqual(
            self.tools.wechat_find_links(cursor=cursor, limit=1)["code"], "CURSOR_STALE"
        )
        links = self.links.links_for_messages((message["message_id"],))
        self.assertEqual([row["normalized_host"] for row in links], ["replaced.example"])

    def test_backfill_fences_version_and_keeps_checkpoint_atomic(self):
        row = self.repository.frozen_message_rows((self.fixture["ids"]["first"],))[0]
        old = prepare_links(row)
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET text='changed' WHERE message_id=?", (row["message_id"],)
            )
            self.assertFalse(publish_links(connection, old, historical=True))
        self.links.request_rebuild()
        checkpoint = self.links.state()["checkpoint_seq"]
        with patch("sightglass.model.links.publish_links", return_value=False):
            with self.assertRaises(Exception):
                self.links.backfill_batch(limit=5)
        self.assertEqual(self.links.state()["checkpoint_seq"], checkpoint)
        rebuilt = self.links.backfill_batch(limit=5)
        self.assertEqual(rebuilt["processed"], 5)
        self.assertGreater(rebuilt["checkpoint_seq"], checkpoint)

    def test_partial_zero_hits_have_honest_continuation_and_scope_bound_cursor(self):
        result = self.tools.wechat_retrieve("concept absent from corpus")
        self.assertEqual(result["contexts"], [])
        self.assertEqual(result["execution"]["state"], "partial")
        cursor = result["page"]["next_cursor"]
        self.assertIsNotNone(cursor)
        result2 = self.tools.wechat_retrieve("concept absent from corpus", cursor=cursor)
        self.assertEqual(result2["schema"], "sightglass.retrieval-results.v1")
        self.assertEqual(
            self.tools.wechat_retrieve("different query", cursor=cursor)["code"], "CURSOR_INVALID"
        )

    def test_context_pagination_and_count_hint_never_pad_targets(self):
        result = self.tools.wechat_retrieve("projects", kinds=["link"], count_hint=20, limit=1)
        self.assertEqual(len(result["contexts"]), 1)
        cursor = result["page"]["next_cursor"]
        self.assertIsNotNone(cursor)
        second = self.tools.wechat_retrieve(
            "projects", kinds=["link"], count_hint=20, limit=1, cursor=cursor
        )
        self.assertEqual(second["schema"], "sightglass.retrieval-results.v1")
        if second["contexts"]:
            self.assertNotEqual(
                result["contexts"][0]["context_id"], second["contexts"][0]["context_id"]
            )
        self.assertLess(
            len(
                {
                    link["normalized_url"]
                    for context in result["contexts"]
                    for link in context["links"]
                }
            ),
            20,
        )

    def test_time_and_sender_are_hard_constraints_and_invalid_values_are_structured(self):
        result = self.tools.wechat_retrieve(
            "projects", kinds=["link"], after="2026-09-22T00:00:00Z", before="2026-09-23T00:00:00Z"
        )
        self.assertTrue(result["contexts"])
        self.assertTrue(
            all(
                "awesome-atlas" in link["normalized_host"]
                for context in result["contexts"]
                for link in context["links"]
            )
        )
        invalid = self.tools.wechat_retrieve("projects", after="invalid date")
        self.assertEqual(invalid["code"], "QUERY_INVALID")
        self.assertEqual(
            self.tools.wechat_find_links(domains=["https://broken:bad/"])["code"], "QUERY_INVALID"
        )
        rendered = json.dumps(self.tools.wechat_retrieve("projects", participant_ids=["unknown"]))
        self.assertIn("PARTICIPANT_OUT_OF_SCOPE", rendered)

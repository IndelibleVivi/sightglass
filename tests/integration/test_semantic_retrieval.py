"""The real reader/MCP path accepts optional candidates under its frozen local view."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from sightglass.policy.readers import ReaderPolicy
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack
from tests.fixtures.retrieval import seed_aggregate_discussion


class CandidateLane:
    def __init__(self, ids: tuple[str, ...], query_hook=None) -> None:
        self.ids = ids
        self.calls = 0
        self.token = "synthetic-publication-1"
        self.query_hook = query_hook
        self.receipt: dict[str, Any] = {"state": "ready", "coverage": "complete"}

    def status(self):
        return self.receipt

    def state_token(self):
        return self.token

    def query(self, concept, **scope):
        self.calls += 1
        if self.query_hook:
            self.query_hook(scope)
        return SimpleNamespace(message_ids=self.ids, receipt=dict(self.receipt))

    def close(self):
        pass


class SemanticRetrievalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            create_synthetic_source(root / "source"), root / "state" / "window.db"
        )
        self.fixture = seed_aggregate_discussion(self.provider, self.repository, self.service)
        self.lane = CandidateLane((self.fixture["ids"]["description"],))
        self.service.semantic = self.lane  # type: ignore[assignment]

    def tearDown(self):
        self.tools.close()
        self.temp.cleanup()

    def retrieve(self, **kwargs):
        return self.tools.wechat_retrieve(
            "虚拟伙伴的跨平台入口", conversation_ids=[self.fixture["group"]], **kwargs
        )

    def test_concept_only_reaches_canonical_context_without_source_or_reader_progress(self):
        with self.repository.database.connection() as connection:
            before = {
                table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table}")]
                for table in (
                    "reader_timeline_cursors",
                    "reader_update_cursors",
                    "reader_deliveries",
                    "voice_jobs",
                    "resource_jobs",
                )
            }
        self.lane.query_hook = lambda _scope: self.assertIsNone(
            self.repository.database._active_connection.get()
        )
        with patch.object(self.provider, "snapshot", side_effect=AssertionError("source opened")):
            result = self.retrieve(kinds=["message"])
        self.assertEqual(result["lanes"]["semantic"], "ready")
        first = result["contexts"][0]
        self.assertIn("semantic", first["matched_by"])
        self.assertIn(self.fixture["ids"]["description"], first["focus_message_ids"])
        self.assertEqual(
            {link["normalized_host"] for link in first["links"]},
            {"www.ailover-atlas.example", "lutopia.example"},
        )
        with self.repository.database.connection() as connection:
            for table, rows in before.items():
                self.assertEqual(
                    rows, [tuple(row) for row in connection.execute(f"SELECT * FROM {table}")]
                )

    def test_link_kind_selects_semantic_link_focus(self):
        self.lane.ids = (self.fixture["ids"]["first"],)
        result = self.retrieve(kinds=["link"])
        self.assertEqual(result["contexts"][0]["focus_message_ids"], list(self.lane.ids))

    def test_many_weak_ANN_hits_and_neighbor_links_cannot_outrank_best_hit(self):
        strongest = self.fixture["admit"](
            "synthetic-best-semantic-hit",
            "Synthetic strongest concept evidence",
            self.fixture["moment"] + timedelta(days=5),
        )
        weak = tuple(
            self.fixture["admit"](
                f"synthetic-weak-semantic-{index}",
                "Synthetic unrelated long discussion",
                self.fixture["moment"] + timedelta(seconds=index),
            )
            for index in range(12)
        )
        # This weak context also contains the fixture's two links. Neither the
        # candidate count nor incidental cooccurrence may override the best ANN hit.
        self.lane.ids = (strongest, *weak)
        first = self.retrieve(limit=1, kinds=["message"])["contexts"][0]
        self.assertEqual(first["focus_message_ids"], [strongest])

    def test_final_scope_rejects_unknown_and_cross_conversation_candidates(self):
        self.lane.ids = ("synthetic-unknown", self.fixture["ids"]["other-conversation"])
        self.assertEqual(self.retrieve()["contexts"], [])

    def test_current_state_and_time_filters_remain_hard(self):
        self.lane.ids = (self.fixture["ids"]["description"],)
        self.assertEqual(
            self.retrieve(after=(self.fixture["moment"] + timedelta(days=2)).isoformat())[
                "contexts"
            ],
            [],
        )
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET current_state='revoked' WHERE message_id=?", self.lane.ids
            )
        self.assertEqual(self.retrieve()["contexts"], [])

    def test_remote_failure_keeps_deterministic_lane(self):
        self.lane.ids = ()
        self.lane.receipt = {"state": "degraded", "reason": "remote_unavailable"}
        result = self.tools.wechat_retrieve("companion", conversation_ids=[self.fixture["group"]])
        self.assertTrue(result["contexts"])
        self.assertEqual(result["lanes"]["semantic"], "degraded")
        self.assertIn("semantic_unavailable", result["source_receipt"]["warnings"])

    def test_pagination_freezes_ANN_and_rebuild_stales_it(self):
        second = self.fixture["admit"](
            "semantic-second-context",
            "Synthetic independent topic",
            self.fixture["moment"] + timedelta(days=4),
        )
        self.lane.ids = (self.fixture["ids"]["description"], second)
        first = self.retrieve(limit=1)
        self.assertIsNotNone(first["page"]["next_cursor"])
        self.lane.ids = ()
        following = self.retrieve(limit=1, cursor=first["page"]["next_cursor"])
        self.assertEqual(self.lane.calls, 1)
        self.assertTrue(following["contexts"])
        self.assertNotEqual(
            first["contexts"][0]["context_id"], following["contexts"][0]["context_id"]
        )
        self.lane.token = "synthetic-publication-2"
        self.assertEqual(
            self.retrieve(limit=1, cursor=first["page"]["next_cursor"])["code"], "CURSOR_STALE"
        )

    def test_correction_during_remote_query_rejects_the_captured_view(self):
        def correct(_scope):
            self.fixture["admit"]("description", "Synthetic corrected text", self.fixture["moment"])

        self.lane.query_hook = correct
        result = self.retrieve()
        self.assertEqual(result["code"], "CURSOR_STALE")

    def test_policy_is_checked_before_semantic_egress(self):
        self.service.reader.policy = ReaderPolicy(mode="allowlist")
        result = self.retrieve()
        self.assertEqual(result["code"], "POLICY_DENIED")
        self.assertEqual(self.lane.calls, 0)
        self.assertNotIn("虚拟伙伴", json.dumps(self.lane.status()))

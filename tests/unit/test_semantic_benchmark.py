"""Offline checks for the synthetic cloud experiment's scope and recovery truth."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from examples.benchmarks.cloudflare_semantic import (
    Cloudflare,
    encoded_cache,
    hard_messages,
    reply_units,
)
from examples.benchmarks.semantic import build_messages, build_units, score_ranked


class SemanticBenchmarkTests(unittest.TestCase):
    def test_readback_only_never_encodes_missing_cache(self) -> None:
        cloud = object.__new__(Cloudflare)
        with tempfile.TemporaryDirectory() as temporary, patch.object(cloud, "encode") as encode:
            with self.assertRaisesRegex(RuntimeError, "does not call Workers AI"):
                encoded_cache(
                    cloud, "synthetic-model", ["synthetic text"], Path(temporary), cached_only=True
                )
            encode.assert_not_called()

    def test_baseline_keeps_32_messages_and_same_conversation_units(self) -> None:
        messages = build_messages()
        self.assertEqual(len(messages), 32)
        canonical = {row["id"]: row for row in messages}
        for kind in ("message", "local_window", "topology_context"):
            for unit in build_units(messages, kind):
                self.assertEqual(
                    len({canonical[mid]["conversation"] for mid in unit["members"]}), 1
                )

    def test_parallel_temporal_window_is_not_reported_as_pure_topic(self) -> None:
        messages = hard_messages()
        units = build_units(messages, "topology_context")
        index = next(i for i, unit in enumerate(units) if "hard-0-0" in unit["members"])
        score = score_ranked("parallel_rail", [index], units, messages)
        self.assertTrue(score["first_page_correct"])
        self.assertTrue(score["false_merge"])
        self.assertLess(score["purity"], 1)

    def test_reply_root_does_not_join_different_conversations(self) -> None:
        messages = hard_messages()
        duplicate = dict(messages[0], id="separate", conversation=99)
        messages.append(duplicate)
        canonical = {row["id"]: row for row in messages}
        for unit in reply_units(messages):
            self.assertEqual(len({canonical[mid]["conversation"] for mid in unit["members"]}), 1)

    def test_long_reply_unit_preserves_whole_debate(self) -> None:
        messages = hard_messages()
        units = reply_units(messages)
        index = next(i for i, unit in enumerate(units) if "hard-2-0" in unit["members"])
        score = score_ranked("long_debate", [index], units, messages)
        self.assertEqual(score["boundary_loss"], 0)
        self.assertEqual(len(units[index]["members"]), 12)

    def test_refuses_existing_product_index_before_reading_credentials(self) -> None:
        with patch.object(Cloudflare, "load_token") as token:
            with self.assertRaisesRegex(ValueError, "dedicated"):
                Cloudflare({"index_name": "existing-product-index"})
            token.assert_not_called()

    def test_present_but_different_vector_cannot_be_resumed_as_ready(self) -> None:
        cloud = object.__new__(Cloudflare)
        cloud.vector_path = "/synthetic"
        expected = [
            {
                "id": "synthetic-unit",
                "namespace": "synthetic-scope",
                "values": [1.0] * 1024,
                "metadata": {"input": "synthetic-version"},
            }
        ]
        wrong = dict(expected[0], metadata={"input": "superseded-version"})
        with patch.object(cloud, "request", return_value=[wrong]):
            with self.assertRaisesRegex(RuntimeError, "refusing overwrite"):
                cloud.readback(expected)

    def test_queued_incomplete_batch_resumes_read_only(self) -> None:
        cloud = object.__new__(Cloudflare)
        expected = [{"id": "synthetic-unit", "metadata": {"input": "synthetic-version"}}]
        with tempfile.TemporaryDirectory() as temporary:
            journal = Path(temporary) / "queued.json"
            journal.write_text(json.dumps({"synthetic-unit": "synthetic-version"}))
            with (
                patch.object(cloud, "readback", return_value=[]),
                patch.object(cloud, "request") as request,
                patch(
                    "examples.benchmarks.cloudflare_semantic.time.monotonic", side_effect=[0, 301]
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "incomplete"):
                    cloud.publish(expected, journal)
                request.assert_not_called()

    def test_ambiguous_submission_keeps_intent_for_read_only_resume(self) -> None:
        cloud = object.__new__(Cloudflare)
        cloud.vector_path = "/synthetic"
        expected = [{"id": "synthetic-unit", "metadata": {"input": "synthetic-version"}}]
        with tempfile.TemporaryDirectory() as temporary:
            journal = Path(temporary) / "queued.json"
            with (
                patch.object(cloud, "readback", return_value=[]),
                patch.object(cloud, "request", side_effect=RuntimeError("transport unavailable")),
            ):
                with self.assertRaisesRegex(RuntimeError, "transport unavailable"):
                    cloud.publish(expected, journal)
            self.assertEqual(
                json.loads(journal.read_text()), {"synthetic-unit": "synthetic-version"}
            )


if __name__ == "__main__":
    unittest.main()

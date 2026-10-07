from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


class ReaderTimezoneTests(unittest.TestCase):
    def test_timezone_change_preserves_pending_delivery_exact_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = create_synthetic_source(root / "source")
            _provider, _repository, service, tools = build_test_stack(
                source, root / "state" / "window.db", default_projection=None
            )
            self.addCleanup(tools.close)
            conversation = tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
                "conversation_id"
            ]
            first = service.read_messages(mode="updates", conversation_id=conversation, limit=20)
            self.assertIsNotNone(first["page"]["delivery_id"])
            service.reader.timezone = "America/New_York"
            replay = service.read_messages(mode="updates", conversation_id=conversation, limit=20)
            self.assertEqual(replay, first)

    def test_explicit_reader_timezone_projects_pages_and_search_without_moving_instants(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = create_synthetic_source(root / "source")
            _provider, _repository, service, tools = build_test_stack(
                source, root / "state" / "window.db", default_projection=None
            )
            self.addCleanup(tools.close)
            conversation = tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
                "conversation_id"
            ]
            original = service.read_messages(mode="recent", conversation_id=conversation, limit=20)
            service.reader.timezone = "America/New_York"
            compact = service.read_messages(mode="recent", conversation_id=conversation, limit=20)
            detail = service.read_messages(
                mode="recent", conversation_id=conversation, limit=20, projection="detail"
            )
            self.assertEqual(compact["timezone"], "America/New_York")
            self.assertTrue(all(row["sent_at"].endswith("-04:00") for row in detail["messages"]))
            self.assertEqual(
                [r[0] for r in original["messages"]], [r[0] for r in compact["messages"]]
            )
            for before, after in zip(original["messages"], compact["messages"], strict=True):
                self.assertEqual(
                    datetime.fromisoformat(before[1]), datetime.fromisoformat(after[1])
                )
                self.assertTrue(after[1].endswith("-04:00"))
            search = service.search_messages(
                query="保留", conversation_ids=(conversation,), limit=10
            )
            self.assertEqual(search["timezone"], "America/New_York")
            self.assertTrue(search["hits"])
            self.assertTrue(all(row[1].endswith("-04:00") for row in search["hits"]))

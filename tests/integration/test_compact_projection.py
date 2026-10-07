from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from sightglass.mcp.server import create_server
from sightglass.policy.readers import ReaderPolicy
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


def _add_compact_messages(root: Path, *, long_final_body: bool = False) -> list[str]:
    people = [f"wxid_compact_{index:02d}" for index in range(20)]
    labels = ["同名成员" if index < 2 else f"成员 {index:02d}" for index in range(20)]
    with closing(sqlite3.connect(root / "catalog.db")) as connection:
        connection.executemany(
            "INSERT INTO principals VALUES (?, 0, 'person', ?, NULL, NULL)",
            zip(people, labels, strict=True),
        )
        connection.executemany(
            "INSERT INTO memberships VALUES ('conv_group', ?, ?, ?, ?)",
            (
                (person, f"compact-member-{index:02d}", labels[index], "2026-09-15T00:00:00Z")
                for index, person in enumerate(people)
            ),
        )
        connection.execute(
            """
            UPDATE conversations
            SET last_message_at_utc = '2026-09-15T00:08:19+00:00'
            WHERE source_conversation_id = 'conv_group'
            """
        )
        connection.commit()

    base = datetime(2026, 9, 15, tzinfo=UTC)
    expected_bodies: list[str] = []
    rows: list[tuple[object, ...]] = []
    for index in range(500):
        person_index = index % len(people)
        person = people[person_index]
        body = "猫" * 80
        if long_final_body and index == 499:
            body = "长" * 50_000
        expected_bodies.append(body)
        sent_at = (base + timedelta(seconds=index)).isoformat()
        resources = []
        if index < 10:
            resources.append(
                {
                    "source_ordinal": 0,
                    "kind": "image",
                    "source_resource_key": f"compact-resource-{index:03d}",
                    "mime_type": "image/jpeg",
                    "availability": "metadata_only",
                }
            )
        rows.append(
            (
                f"compact-msg-{index:03d}",
                "conv_group",
                sent_at,
                sent_at,
                sent_at,
                index,
                10_000 + index,
                1,
                f"{person}:\n{body}",
                0,
                person,
                None,
                labels[person_index],
                json.dumps(resources, ensure_ascii=False),
            )
        )
    with closing(sqlite3.connect(root / "messages-1.db")) as connection:
        connection.executemany(
            """
            INSERT INTO messages(
                source_message_id, source_conversation_id, source_time_raw,
                sent_at_utc, observed_at_utc, sort_seq, source_rowid,
                wechat_type, raw_content, is_outgoing, sender_internal_id,
                sender_local_token, sender_surface_label, resources_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        connection.commit()
    return expected_bodies


class CompactProjectionContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "source"
        create_synthetic_source(self.root)
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            self.root,
            Path(self.temp.name) / "state" / "window.db",
            default_projection=None,
        )
        self.group_id = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_read_tool_declares_projection_and_compact_default(self) -> None:
        registered = {
            tool.name: tool for tool in create_server(self.tools)._tool_manager.list_tools()
        }
        properties = registered["wechat_read_messages"].parameters["properties"]
        self.assertIn("projection", properties)

        compact = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=100
        )
        self.assertEqual(compact["schema"], "sightglass.message-batch.v1")
        self.assertEqual(compact["projection"], "compact")
        self.assertEqual(compact["fields"], ["id", "when", "who", "kind", "what", "resources"])
        self.assertTrue(compact["messages"])
        self.assertTrue(all(len(row) == 6 for row in compact["messages"]))
        self.assertNotIn("anchor", str(compact["messages"]))

    def test_detail_is_explicitly_versioned_and_message_defaults_to_detail(self) -> None:
        detailed_page = self.tools.wechat_read_messages(
            mode="recent",
            conversation_id=self.group_id,
            projection="detail",
            limit=5,
        )
        self.assertEqual(detailed_page["schema"], "sightglass.message-page.v1")
        self.assertEqual(detailed_page["projection"], "detail")
        self.assertTrue(
            all(
                message["schema"] == "sightglass.message-detail.v1"
                for message in detailed_page["messages"]
            )
        )
        target_id = detailed_page["messages"][0]["message_id"]
        message_page = self.tools.wechat_read_messages(mode="message", message_id=target_id)
        self.assertEqual(message_page["projection"], "detail")
        self.assertEqual(message_page["messages"][0]["message_id"], target_id)

    def test_explicit_detail_uses_the_detail_default_limit(self) -> None:
        detailed_page = self.tools.wechat_read_messages(
            mode="recent",
            conversation_id=self.group_id,
            projection="detail",
        )
        self.assertEqual(detailed_page["schema"], "sightglass.message-page.v1")
        self.assertEqual(detailed_page["projection"], "detail")

    def test_projection_specific_limits_are_enforced(self) -> None:
        too_many_compact = self.tools.wechat_read_messages(
            mode="recent",
            conversation_id=self.group_id,
            projection="compact",
            limit=501,
        )
        too_many_detail = self.tools.wechat_read_messages(
            mode="recent",
            conversation_id=self.group_id,
            projection="detail",
            limit=51,
        )
        invalid_message_limit = self.tools.wechat_read_messages(
            mode="message",
            message_id="wxmsg_unknown",
            projection="detail",
            limit=2,
        )
        self.assertEqual(too_many_compact["code"], "QUERY_INVALID")
        self.assertEqual(too_many_detail["code"], "QUERY_INVALID")
        self.assertEqual(invalid_message_limit["code"], "QUERY_INVALID")

    def test_search_reuses_compact_rows_and_sparse_matches(self) -> None:
        result = self.tools.wechat_search_messages(
            query="保留 第二行", conversation_ids=[self.group_id], limit=20
        )
        self.assertEqual(result["schema"], "sightglass.search-results.v2")
        self.assertEqual(result["projection"], "compact")
        self.assertEqual(result["fields"], ["id", "when", "who", "kind", "what", "resources"])
        self.assertEqual(len(result["hits"]), 1)
        self.assertEqual(result["hits"][0][3], "text")
        self.assertEqual(result["hits"][0][4], "  保留  内部空白！\n第二行")
        self.assertEqual(result["markers"]["matches"], {"0": ["text"]})
        self.assertNotIn("source", str(result["hits"]))

    def test_context_uses_sparse_focus_and_context_markers(self) -> None:
        recent = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=100
        )
        target_id = next(row[0] for row in recent["messages"] if row[4] == "我发出的消息")
        context = self.tools.wechat_read_messages(
            mode="context", message_id=target_id, before=1, after=1, limit=3
        )
        self.assertEqual(context["projection"], "compact")
        self.assertEqual(context["markers"]["focus"], [1])
        self.assertEqual(context["markers"]["context"], [0, 2])


class CompactProjectionScaleTests(unittest.TestCase):
    def _stack(
        self,
        *,
        long_final_body: bool = False,
        policy: ReaderPolicy | None = None,
    ):
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name) / "source"
        create_synthetic_source(root)
        expected = _add_compact_messages(root, long_final_body=long_final_body)
        provider, repository, service, tools = build_test_stack(
            root,
            Path(temporary.name) / "state" / "window.db",
            policy=policy,
            default_projection=None,
        )
        group_id = tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        return temporary, expected, provider, repository, service, tools, group_id

    def test_500_short_messages_are_complete_compact_and_bounded(self) -> None:
        temporary, expected, _provider, repository, _service, tools, group_id = self._stack()
        self.addCleanup(temporary.cleanup)
        with (
            patch.object(repository, "preferred_label", wraps=repository.preferred_label) as labels,
            patch.object(
                repository, "resources_for_message", wraps=repository.resources_for_message
            ) as resources,
        ):
            page = tools.wechat_read_messages(
                mode="recent", conversation_id=group_id, projection="compact", limit=500
            )
        self.assertEqual(labels.call_count, 0)
        self.assertEqual(resources.call_count, 0)
        self.assertEqual(len(page["messages"]), 500)
        self.assertEqual([row[4] for row in page["messages"]], expected)
        self.assertEqual(len(page["people"]), 20)
        duplicate_people = [person for person in page["people"] if person["label"] == "同名成员"]
        self.assertEqual(len(duplicate_people), 2)
        self.assertNotEqual(duplicate_people[0]["id"], duplicate_people[1]["id"])
        self.assertEqual(sum(row[5] for row in page["messages"]), 10)
        self.assertEqual(page["projection_receipt"]["returned_rows"], 500)
        self.assertEqual(page["projection_receipt"]["body_truncated_rows"], 0)
        self.assertTrue(page["page"]["message_rows_complete"])
        rendered = json.dumps(page, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        self.assertLessEqual(len(rendered), 150_000)
        self.assertEqual(page["projection_receipt"]["serialized_chars"], len(rendered))

    def test_one_extreme_body_does_not_evict_499_short_rows(self) -> None:
        temporary, expected, _provider, _repository, _service, tools, group_id = self._stack(
            long_final_body=True
        )
        self.addCleanup(temporary.cleanup)
        page = tools.wechat_read_messages(
            mode="recent", conversation_id=group_id, projection="compact", limit=500
        )
        self.assertEqual(len(page["messages"]), 500)
        truncated = page["markers"]["body_truncated"]
        self.assertEqual(set(truncated), {"499"})
        self.assertEqual(truncated["499"]["full_chars"], 50_000)
        self.assertEqual([row[4] for row in page["messages"][:499]], expected[:499])
        self.assertTrue(page["messages"][499][4].endswith("…"))
        self.assertTrue(page["page"]["message_rows_complete"])
        detail = tools.wechat_read_messages(mode="message", message_id=page["messages"][499][0])
        self.assertEqual(detail["messages"][0]["text"], expected[499])

    def test_compact_projection_is_deterministic(self) -> None:
        temporary, _expected, _provider, _repository, _service, tools, group_id = self._stack()
        self.addCleanup(temporary.cleanup)
        first = tools.wechat_read_messages(
            mode="recent", conversation_id=group_id, projection="compact", limit=500
        )
        second = tools.wechat_read_messages(
            mode="recent", conversation_id=group_id, projection="compact", limit=500
        )
        self.assertEqual(first["people"], second["people"])
        self.assertEqual(first["fields"], second["fields"])
        self.assertEqual(first["messages"], second["messages"])
        self.assertEqual(first["markers"], second["markers"])

    def test_compact_projection_does_not_hold_the_window_writer(self) -> None:
        temporary, _expected, _provider, repository, service, tools, group_id = self._stack()
        self.addCleanup(temporary.cleanup)
        original_prepare = service.compact_projector.prepare
        writer_entered = threading.Event()
        writer_finished = threading.Event()

        def competing_writer() -> None:
            with repository.database.transaction():
                writer_entered.set()
            writer_finished.set()

        def prepare(*args, **kwargs):
            worker = threading.Thread(target=competing_writer)
            worker.start()
            try:
                if not writer_entered.wait(timeout=0.25):
                    raise AssertionError("compact projection still owns the window writer")
                return original_prepare(*args, **kwargs)
            finally:
                worker.join(timeout=1)

        with patch.object(service.compact_projector, "prepare", side_effect=prepare):
            page = tools.wechat_read_messages(
                mode="recent",
                conversation_id=group_id,
                projection="compact",
                limit=500,
            )

        self.assertEqual(page["schema"], "sightglass.message-batch.v1")
        self.assertTrue(writer_finished.is_set())

    def test_fixed_envelope_overflow_crops_rows_and_returns_a_cursor(self) -> None:
        policy = ReaderPolicy(
            mode="all_except_denylist",
            identity_debug=True,
            max_compact_payload_chars=12_000,
        )
        temporary, _expected, _provider, _repository, _service, tools, group_id = self._stack(
            policy=policy
        )
        self.addCleanup(temporary.cleanup)
        first = tools.wechat_read_messages(
            mode="recent", conversation_id=group_id, projection="compact", limit=500
        )
        self.assertGreater(len(first["messages"]), 0)
        self.assertLess(len(first["messages"]), 500)
        self.assertFalse(first["page"]["message_rows_complete"])
        self.assertIsInstance(first["page"]["next_cursor"], str)
        self.assertLessEqual(
            len(json.dumps(first, ensure_ascii=False, sort_keys=True, separators=(",", ":"))),
            policy.max_compact_payload_chars,
        )
        continuation = tools.wechat_read_messages(
            mode="recent",
            conversation_id=group_id,
            projection="compact",
            limit=500,
            cursor=first["page"]["next_cursor"],
        )
        self.assertTrue(continuation["messages"])
        self.assertTrue(
            {row[0] for row in first["messages"]}.isdisjoint(
                row[0] for row in continuation["messages"]
            )
        )

    def test_search_fixed_envelope_overflow_uses_the_search_cursor(self) -> None:
        policy = ReaderPolicy(
            mode="all_except_denylist",
            identity_debug=True,
            max_compact_payload_chars=12_000,
        )
        temporary, _expected, _provider, _repository, _service, tools, group_id = self._stack(
            policy=policy
        )
        self.addCleanup(temporary.cleanup)
        first = tools.wechat_search_messages(query="猫", conversation_ids=[group_id], limit=200)
        self.assertNotIn("code", first, first)
        self.assertGreater(len(first["hits"]), 0)
        self.assertLess(len(first["hits"]), 200)
        self.assertFalse(first["page"]["message_rows_complete"])
        self.assertIsInstance(first["page"]["next_cursor"], str)
        continuation = tools.wechat_search_messages(
            query="猫",
            conversation_ids=[group_id],
            limit=200,
            cursor=first["page"]["next_cursor"],
        )
        self.assertNotIn("code", continuation, continuation)
        self.assertTrue(continuation["hits"])
        self.assertTrue(
            {row[0] for row in first["hits"]}.isdisjoint(row[0] for row in continuation["hits"])
        )

    def test_detail_update_ack_does_not_advance_past_payload_trim(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name) / "source"
        create_synthetic_source(root)
        policy = ReaderPolicy(
            mode="all_except_denylist",
            identity_debug=True,
            max_detail_payload_chars=3_500,
        )
        _provider, _repository, _service, tools = build_test_stack(
            root,
            Path(temporary.name) / "state" / "window.db",
            policy=policy,
            default_projection=None,
        )
        group_id = tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        tools.wechat_read_messages(
            mode="recent", conversation_id=group_id, projection="compact", limit=100
        )
        with closing(sqlite3.connect(root / "messages-2.db")) as connection:
            connection.executemany(
                """
                INSERT INTO messages(
                    source_message_id, source_conversation_id, source_time_raw,
                    sent_at_utc, observed_at_utc, sort_seq, source_rowid,
                    wechat_type, raw_content, is_outgoing, sender_internal_id,
                    sender_local_token, sender_surface_label, resources_json
                ) VALUES (?, 'conv_group', ?, ?, ?, ?, ?, 1, ?, 0,
                          'wxid_demo_member', NULL, '原账号昵称', '[]')
                """,
                (
                    (
                        f"source-msg-update-budget-{index}",
                        f"2026-09-15T00:00:0{index}+00:00",
                        f"2026-09-15T00:00:0{index}+00:00",
                        f"2026-09-15T00:01:0{index}+00:00",
                        20 + index,
                        200 + index,
                        f"wxid_demo_member:\n{str(index) * 900}",
                    )
                    for index in (1, 2)
                ),
            )
            connection.commit()
        manifest_path = root / "source.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["shards"][1]["generation_id"] = "generation-2-update-budget"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        first = tools.wechat_read_messages(
            mode="updates",
            conversation_id=group_id,
            projection="detail",
            limit=2,
        )
        self.assertNotIn("code", first, first)
        self.assertEqual(len(first["messages"]), 1)
        first_message_id = first["messages"][0]["message_id"]
        second = tools.wechat_read_messages(
            mode="updates",
            conversation_id=group_id,
            projection="detail",
            limit=2,
            ack_delivery_id=first["page"]["delivery_id"],
        )
        self.assertNotIn("code", second, second)
        self.assertEqual(len(second["messages"]), 1)
        self.assertNotEqual(second["messages"][0]["message_id"], first_message_id)

    def test_compact_update_fixed_crop_replays_then_continues_after_ack(self) -> None:
        policy = ReaderPolicy(
            mode="all_except_denylist",
            identity_debug=True,
            max_compact_payload_chars=6_000,
        )
        temporary, _expected, _provider, _repository, _service, tools, group_id = self._stack(
            policy=policy
        )
        self.addCleanup(temporary.cleanup)
        tools.wechat_read_messages(
            mode="recent", conversation_id=group_id, projection="compact", limit=500
        )
        root = Path(temporary.name) / "source"
        base = datetime(2026, 9, 16, tzinfo=UTC)
        with closing(sqlite3.connect(root / "messages-1.db")) as connection:
            connection.executemany(
                """
                INSERT INTO messages(
                    source_message_id, source_conversation_id, source_time_raw,
                    sent_at_utc, observed_at_utc, sort_seq, source_rowid,
                    wechat_type, raw_content, is_outgoing, sender_internal_id,
                    sender_local_token, sender_surface_label, resources_json
                ) VALUES (?, 'conv_group', ?, ?, ?, ?, ?, 1, ?, 0,
                          'wxid_demo_member', NULL, '原账号昵称', '[]')
                """,
                (
                    (
                        f"source-msg-compact-update-{index:03d}",
                        (base + timedelta(seconds=index)).isoformat(),
                        (base + timedelta(seconds=index)).isoformat(),
                        (base + timedelta(seconds=index)).isoformat(),
                        20_000 + index,
                        30_000 + index,
                        f"wxid_demo_member:\n更新 {index:03d}",
                    )
                    for index in range(100)
                ),
            )
            connection.commit()
        manifest_path = root / "source.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["shards"][0]["generation_id"] = "generation-1-compact-updates"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        first = tools.wechat_read_messages(
            mode="updates",
            conversation_id=group_id,
            projection="compact",
            limit=100,
        )
        self.assertNotIn("code", first, first)
        self.assertGreater(len(first["messages"]), 0)
        self.assertLess(len(first["messages"]), 100)
        self.assertFalse(first["page"]["message_rows_complete"])
        replay = tools.wechat_read_messages(
            mode="updates",
            conversation_id=group_id,
            projection="compact",
            limit=100,
        )
        self.assertEqual(replay, first)
        second = tools.wechat_read_messages(
            mode="updates",
            conversation_id=group_id,
            projection="compact",
            limit=100,
            ack_delivery_id=first["page"]["delivery_id"],
        )
        self.assertNotIn("code", second, second)
        self.assertTrue(second["messages"])
        self.assertTrue(
            {row[0] for row in first["messages"]}.isdisjoint(row[0] for row in second["messages"])
        )


if __name__ == "__main__":
    unittest.main()

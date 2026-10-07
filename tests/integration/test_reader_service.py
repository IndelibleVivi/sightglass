from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.identity import SourceIdentityKey, SourceParticipant
from sightglass.model.observation_codec import decode_observation_text
from sightglass.policy.readers import ReaderPolicy
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


class ReaderServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "source"
        self.window = Path(self.temp.name) / "state" / "window.db"
        create_synthetic_source(self.root)
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            self.root, self.window
        )
        status = self.tools.wechat_status()
        self.assertTrue(status["ready"])
        group_candidates = self.tools.wechat_find_conversations("Synthetic Group")
        self.group_id = group_candidates["candidates"][0]["conversation_id"]
        direct_candidates = self.tools.wechat_find_conversations("Demo Direct")
        self.direct_id = direct_candidates["candidates"][0]["conversation_id"]

    def tearDown(self):
        self.temp.cleanup()

    def _read_group(self):
        result = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=100
        )
        self.assertEqual(result["schema"], "sightglass.message-page.v1")
        return result

    def _participant(self, query: str):
        result = self.tools.wechat_find_participants(self.group_id, query)
        self.assertTrue(result["candidates"], result)
        return result["candidates"][0]

    def test_recent_preserves_text_orders_shards_and_keeps_unknown(self):
        result = self._read_group()
        messages = result["messages"]
        texts = [item["text"] for item in messages]
        self.assertIn("  保留  内部空白！\n第二行", texts)
        self.assertLess(
            texts.index("  保留  内部空白！\n第二行"),
            texts.index("同名另一个人"),
        )
        unknown = next(item for item in messages if item["kind"] == "unknown")
        self.assertEqual(unknown["text"], "[暂不支持的消息类型]")
        self.assertTrue(result["source_receipt"]["complete"])
        self.assertEqual(result["source_receipt"]["returned_count"], len(messages))

    def test_group_and_direct_outgoing_are_explicit_self(self):
        group = self._read_group()
        outgoing = next(item for item in group["messages"] if item["text"] == "我发出的消息")
        self.assertTrue(outgoing["sender"]["is_self"])
        direct = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.direct_id, limit=100
        )
        incoming = next(item for item in direct["messages"] if item["text"] == "私聊 incoming")
        outgoing = next(item for item in direct["messages"] if item["text"] == "私聊 outgoing")
        self.assertFalse(incoming["sender"]["is_self"])
        self.assertTrue(outgoing["sender"]["is_self"])

    def test_same_name_is_ambiguous_and_labels_are_layered(self):
        self._read_group()
        result = self.tools.wechat_find_participants(self.group_id, "示例甲")
        self.assertTrue(result["ambiguous"])
        self.assertEqual(len(result["candidates"]), 2)
        self.assertEqual(len({item["participant_id"] for item in result["candidates"]}), 2)
        demo_member = next(
            item for item in result["candidates"] if item["labels"]["contact_remark"] == "示例甲"
        )
        self.assertEqual(demo_member["label"], "示例甲")
        self.assertEqual(demo_member["label_source"], "contact_remark")
        self.assertEqual(demo_member["labels"]["account_nickname"], "原账号昵称")
        self.assertEqual(demo_member["labels"]["current_group_alias"], "群里的示例甲")
        self.assertEqual(demo_member["labels"]["public_handle"], "demo_member_old")

    def test_participant_ambiguity_survives_limit_one(self):
        first = self.tools.wechat_find_participants(self.group_id, "示例甲", limit=1)
        self.assertTrue(first["ambiguous"])
        self.assertEqual(first["total_matches"], 2)
        self.assertTrue(first["truncated"])
        self.assertEqual(len(first["candidates"]), 1)
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)

        second = self.tools.wechat_find_participants(
            self.group_id, "示例甲", limit=1, cursor=cursor
        )
        self.assertTrue(second["ambiguous"])
        self.assertEqual(second["total_matches"], 2)
        self.assertFalse(second["truncated"])
        self.assertEqual(len(second["candidates"]), 1)
        self.assertIsNone(second["page"]["next_cursor"])
        self.assertNotEqual(
            first["candidates"][0]["participant_id"],
            second["candidates"][0]["participant_id"],
        )
        wrong_scope = self.tools.wechat_find_participants(
            self.group_id, "demo_member_old", limit=1, cursor=cursor
        )
        self.assertEqual(wrong_scope["code"], "CURSOR_INVALID")

    def test_participant_cursor_stales_when_visible_candidate_evidence_changes(self):
        first = self.tools.wechat_find_participants(self.group_id, "示例甲", limit=1)
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)
        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.execute(
                """
                UPDATE principals SET contact_remark = '示例甲的新备注'
                WHERE internal_id = 'wxid_demo_member'
                """
            )
            connection.commit()

        continued = self.tools.wechat_find_participants(
            self.group_id, "示例甲", limit=1, cursor=cursor
        )
        self.assertEqual(continued["code"], "CURSOR_STALE")

    def test_conversation_ambiguity_survives_limit_one_after_policy(self):
        result = self.tools.wechat_find_conversations("示例甲", limit=1)
        self.assertTrue(result["ambiguous"])
        self.assertGreaterEqual(result["total_matches"], 2)
        self.assertTrue(result["truncated"])
        self.assertTrue(result["candidates"][0]["ambiguity"]["requires_selection"])

    def test_message_only_sender_label_can_find_conversation(self):
        result = self.tools.wechat_find_conversations("非好友成员")
        self.assertEqual(len(result["candidates"]), 1)
        self.assertEqual(result["candidates"][0]["conversation_id"], self.group_id)
        self.assertEqual(result["candidates"][0]["matched"]["kind"], "message_surface")

    def test_message_shown_as_does_not_retroactively_use_current_group_card(self):
        result = self._read_group()
        message = next(
            item for item in result["messages"] if item["text"] == "  保留  内部空白！\n第二行"
        )
        self.assertEqual(message["sender"]["label"], "示例甲")
        self.assertEqual(message["sender"]["shown_as"], "原账号昵称")
        self.assertEqual(message["sender"]["shown_as_temporal_confidence"], "exact")
        self.assertNotEqual(message["sender"]["shown_as"], "群里的示例甲")

    def test_public_handle_change_and_reuse_never_merge_participants(self):
        before = self.tools.wechat_find_participants(self.group_id, "demo_member_old")
        original_id = before["candidates"][0]["participant_id"]
        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.execute(
                """
                UPDATE principals SET public_handle = 'shared_handle'
                WHERE internal_id = 'wxid_demo_member'
                """
            )
            connection.execute(
                """
                UPDATE principals SET public_handle = 'shared_handle'
                WHERE internal_id = 'wxid_demo_member2'
                """
            )
            connection.commit()
        result = self.tools.wechat_find_participants(self.group_id, "shared_handle")
        self.assertTrue(result["ambiguous"])
        self.assertEqual(len(result["candidates"]), 2)
        self.assertIn(original_id, {item["participant_id"] for item in result["candidates"]})
        with self.repository.database.connection() as connection:
            key_kinds = {
                str(row[0])
                for row in connection.execute(
                    "SELECT DISTINCT key_kind FROM participant_source_keys"
                )
            }
        self.assertNotIn("public_handle", key_kinds)

    def test_alias_history_remains_searchable_without_changing_identity(self):
        before = self.tools.wechat_find_participants(self.group_id, "示例甲")
        original = next(
            item for item in before["candidates"] if item["labels"]["contact_remark"] == "示例甲"
        )
        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.execute(
                """
                UPDATE principals SET contact_remark = '示例甲的新备注'
                WHERE internal_id = 'wxid_demo_member'
                """
            )
            connection.commit()
        after = self.tools.wechat_find_participants(self.group_id, "示例甲的新备注")
        self.assertEqual(after["candidates"][0]["participant_id"], original["participant_id"])
        history = self.tools.wechat_find_participants(self.group_id, "示例甲")
        self.assertIn(
            original["participant_id"],
            {item["participant_id"] for item in history["candidates"]},
        )

    def test_group_card_and_account_nickname_changes_keep_participant_identity(self):
        before = self._participant("原账号昵称")
        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.execute(
                """
                UPDATE principals SET account_nickname = '新账号昵称'
                WHERE internal_id = 'wxid_demo_member'
                """
            )
            connection.execute(
                """
                UPDATE memberships SET group_card = '新群名片'
                WHERE source_conversation_id = 'conv_group'
                  AND internal_id = 'wxid_demo_member'
                """
            )
            connection.commit()
        after = self._participant("新账号昵称")
        self.assertEqual(after["participant_id"], before["participant_id"])
        self.assertEqual(after["labels"]["current_group_alias"], "新群名片")
        historical = self.tools.wechat_find_participants(self.group_id, "群里的示例甲")
        self.assertIn(
            before["participant_id"],
            {item["participant_id"] for item in historical["candidates"]},
        )

    def test_non_friend_group_sender_is_indexed(self):
        outsider = self._participant("非好友成员")
        self.assertEqual(outsider["resolution_state"], "stable")
        self.assertIsNone(outsider["labels"]["contact_remark"])

    def test_surface_label_without_sender_id_stays_alias_only(self):
        copied = self._participant("复制显示名")
        self.assertEqual(copied["resolution_state"], "alias_only")
        self.assertEqual(copied["identity_confidence"], "unknown")
        canonical = self._participant("原账号昵称")
        self.assertNotEqual(copied["participant_id"], canonical["participant_id"])

    def test_alias_only_message_projects_alias_only_identity_state(self):
        page = self._read_group()
        copied = next(item for item in page["messages"] if item["text"] == "复制来的消息")
        self.assertEqual(copied["sender"]["identity_state"], "alias_only")
        self.assertEqual(copied["sender"]["identity_confidence"], "unknown")

    def test_two_anonymous_memberships_remain_distinct(self):
        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.executemany(
                "INSERT INTO memberships VALUES (?, NULL, ?, ?, ?)",
                (
                    ("conv_group", "anonymous-a", "Anonymous A", "2026-09-13T10:00:00Z"),
                    ("conv_group", "anonymous-b", "Anonymous B", "2026-09-13T10:00:00Z"),
                ),
            )
            connection.commit()
        result = self.tools.wechat_find_participants(self.group_id, "Anonymous")
        self.assertTrue(result["ambiguous"])
        self.assertEqual(len(result["candidates"]), 2)
        self.assertEqual(len({item["participant_id"] for item in result["candidates"]}), 2)
        self.assertTrue(
            all(item["resolution_state"] == "conversation_local" for item in result["candidates"])
        )

    def test_repository_rejects_mutable_principal_keys(self):
        context = self.repository.conversation_context(self.group_id)
        assert context is not None
        malformed = SourceParticipant(
            source_conversation_id="conv_group",
            identity_keys=(
                SourceIdentityKey(
                    "public_handle",
                    "mutable-handle",
                    "stable",
                    True,
                    "malformed-provider",
                ),
            ),
            labels=(),
        )
        with self.assertRaises(SightglassError) as caught:
            self.repository.index_participant(
                str(context["account_id"]),
                self.group_id,
                malformed,
                "2026-09-13T10:00:00+00:00",
            )
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_MESSAGE_DECODE_FAILED)

    def test_speaker_filter_preserves_key_kind_and_conversation_scope(self):
        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            connection.execute(
                """
                UPDATE messages SET sender_local_token = 'local-copy-token'
                WHERE source_message_id = 'source-msg-009'
                """
            )
            connection.commit()
        copied = self._participant("复制显示名")
        filters = self.repository.participant_source_filters(
            self.group_id, (copied["participant_id"],)
        )
        self.assertEqual(len(filters), 1)
        self.assertEqual(filters[0].key_kind, "conversation_sender_id")
        self.assertEqual(filters[0].key_value, "local-copy-token")
        self.assertFalse(filters[0].principal_eligible)
        self.assertEqual(filters[0].scope_conversation_source_id, "conv_group")

    def test_account_self_exists_without_roster_or_outgoing_messages(self):
        root = Path(self.temp.name) / "self-only-source"
        window = Path(self.temp.name) / "self-only-state" / "window.db"
        create_synthetic_source(root)
        with closing(sqlite3.connect(root / "catalog.db")) as connection:
            connection.execute("DELETE FROM memberships WHERE internal_id = 'wxid_demo_owner'")
            connection.commit()
        for shard in (root / "messages-1.db", root / "messages-2.db"):
            with closing(sqlite3.connect(shard)) as connection:
                connection.execute("DELETE FROM messages WHERE is_outgoing = 1")
                connection.commit()
        _provider, repository, _service, tools = build_test_stack(root, window)
        self.assertTrue(tools.wechat_status()["ready"])
        with repository.database.connection() as connection:
            rows = connection.execute(
                "SELECT participant_id FROM participants WHERE is_self = 1"
            ).fetchall()
        self.assertEqual(len(rows), 1)

    def test_account_self_identity_change_fails_closed(self):
        manifest_path = self.root / "source.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["account"]["self_principal_key"] = "wxid_impostor"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        result = self.tools.wechat_status()
        self.assertEqual(result["code"], "SOURCE_INCOMPLETE")
        self.assertIn("account_self_identity_conflict", result["details"]["warning_codes"])
        with self.repository.database.connection() as connection:
            self_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM participants WHERE is_self = 1"
                ).fetchone()[0]
            )
        self.assertEqual(self_count, 1)

    def test_speaker_only_includes_image_and_file_without_advancing_conversation_cursor(self):
        self._read_group()
        conversation_cursor_count = self.repository.timeline_cursor_count(
            self.group_id, "conversation"
        )
        demo_member = self._participant("demo_member_old")
        result = self.tools.wechat_read_messages(
            mode="speaker",
            conversation_id=self.group_id,
            participant_ids=[demo_member["participant_id"]],
            speaker_view="only",
            limit=100,
        )
        self.assertTrue(
            all(
                item["sender"]["participant_id"] == demo_member["participant_id"]
                for item in result["messages"]
            )
        )
        self.assertIn("image", {item["kind"] for item in result["messages"]})
        outsider = self._participant("非好友成员")
        file_result = self.tools.wechat_read_messages(
            mode="speaker",
            conversation_id=self.group_id,
            participant_ids=[outsider["participant_id"]],
            speaker_view="only",
            limit=100,
        )
        self.assertEqual([item["kind"] for item in file_result["messages"]], ["file"])
        self.assertEqual(
            self.repository.timeline_cursor_count(self.group_id, "conversation"),
            conversation_cursor_count,
        )

    def test_range_uses_absolute_boundaries_and_reader_timezone(self):
        result = self.tools.wechat_read_messages(
            mode="range",
            conversation_id=self.group_id,
            time_after="2026-09-13T09:03:00+00:00",
            time_before="2026-09-13T09:05:00+00:00",
            direction="forward",
            limit=100,
        )
        self.assertEqual({item["kind"] for item in result["messages"]}, {"image", "file"})
        self.assertTrue(all("+08:00" in item["sent_at"] for item in result["messages"]))

    def test_message_mode_rehydrates_by_stable_message_id(self):
        recent = self._read_group()
        target = recent["messages"][0]
        result = self.tools.wechat_read_messages(
            mode="message", message_id=target["message_id"], limit=1
        )
        self.assertEqual(result["messages"][0]["message_id"], target["message_id"])
        self.assertEqual(result["messages"][0]["anchor"], target["anchor"])

    def test_generation_replacement_preserves_stable_message_ids(self):
        before = self._read_group()
        ids_before = {
            (item["kind"], item["text"]): item["message_id"] for item in before["messages"]
        }
        manifest_path = self.root / "source.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["shards"][0]["generation_id"] = "generation-1-rebuilt"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        after = self._read_group()
        ids_after = {(item["kind"], item["text"]): item["message_id"] for item in after["messages"]}
        self.assertEqual(ids_before, ids_after)

    def test_generation_failure_rolls_back_the_entire_admission_batch(self):
        with self.repository.database.connection() as connection:
            before = tuple(
                int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in ("messages", "message_observations")
            )
        original = self.repository.upsert_message
        calls = 0

        def mutate_after_first(*args, **kwargs):
            nonlocal calls
            message_id = original(*args, **kwargs)
            calls += 1
            if calls == 1:
                manifest_path = self.root / "source.json"
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                manifest["shards"][0]["generation_id"] = "generation-mutated-mid-admission"
                manifest_path.write_text(
                    json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
            return message_id

        self.repository.upsert_message = mutate_after_first  # type: ignore[method-assign]
        try:
            result = self.tools.wechat_read_messages(
                mode="recent", conversation_id=self.group_id, limit=100
            )
        finally:
            self.repository.upsert_message = original  # type: ignore[method-assign]
        self.assertEqual(result["code"], "SOURCE_GENERATION_CHANGED")
        with self.repository.database.connection() as connection:
            after = tuple(
                int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in ("messages", "message_observations")
            )
        self.assertEqual(after, before)

    def test_new_generation_page_does_not_splice_removed_cached_message(self):
        before = self._read_group()
        removed = next(item for item in before["messages"] if item["text"] == "复制来的消息")
        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            connection.execute("DELETE FROM messages WHERE source_message_id = 'source-msg-009'")
            connection.commit()
        manifest_path = self.root / "source.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["shards"][1]["generation_id"] = "generation-2-with-removal"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        after = self._read_group()
        self.assertNotIn(removed["message_id"], {item["message_id"] for item in after["messages"]})
        with self.repository.database.connection() as connection:
            retained = connection.execute(
                "SELECT current_state FROM messages WHERE message_id = ?",
                (removed["message_id"],),
            ).fetchone()
        self.assertIsNotNone(retained)
        self.assertEqual(retained["current_state"], "present")

    def test_sender_label_change_is_preserved_as_immutable_observation(self):
        before = self._read_group()
        target = next(
            item for item in before["messages"] if item["text"] == "  保留  内部空白！\n第二行"
        )
        with closing(sqlite3.connect(self.root / "messages-1.db")) as connection:
            connection.execute(
                """
                UPDATE messages SET sender_surface_label = 'NEW CURRENT LABEL'
                WHERE source_message_id = 'source-msg-001'
                """
            )
            connection.commit()
        manifest_path = self.root / "source.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["shards"][0]["generation_id"] = "generation-1-new-label"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        after = self._read_group()
        current = next(
            item for item in after["messages"] if item["message_id"] == target["message_id"]
        )
        self.assertEqual(current["sender"]["shown_as"], "NEW CURRENT LABEL")
        with self.repository.database.connection() as connection:
            observations = connection.execute(
                """
                SELECT payload_digest, parsed_json FROM message_observations
                WHERE message_id = ? ORDER BY observation_seq
                """,
                (target["message_id"],),
            ).fetchall()
        self.assertEqual(len(observations), 2)
        self.assertEqual(len({str(row["payload_digest"]) for row in observations}), 2)
        labels = {
            json.loads(decode_observation_text(row["parsed_json"]))["sender"]["surface_label"]
            for row in observations
        }
        self.assertEqual(labels, {"原账号昵称", "NEW CURRENT LABEL"})

    def test_partial_catalog_coverage_is_consistent_across_m1_tools(self):
        root = Path(self.temp.name) / "partial-source"
        window = Path(self.temp.name) / "partial-state" / "window.db"
        create_synthetic_source(root, catalog_complete=False)
        _provider, _repository, _service, tools = build_test_stack(root, window)
        conversation_result = tools.wechat_find_conversations("Synthetic Group")
        conversation_id = conversation_result["candidates"][0]["conversation_id"]
        participant_result = tools.wechat_find_participants(conversation_id, "示例甲")
        page = tools.wechat_read_messages(mode="recent", conversation_id=conversation_id, limit=100)
        self.assertEqual(conversation_result["coverage"]["catalog"], "partial")
        self.assertEqual(participant_result["coverage"]["catalog"], "partial")
        self.assertEqual(page["source_receipt"]["coverage"]["catalog"], "partial")
        self.assertFalse(page["source_receipt"]["complete"])

    def test_removed_current_labels_close_but_history_remains_searchable(self):
        before = self._participant("demo_member_old")
        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.execute(
                """
                UPDATE principals SET public_handle = NULL
                WHERE internal_id = 'wxid_demo_member'
                """
            )
            connection.execute(
                """
                UPDATE memberships SET group_card = NULL
                WHERE source_conversation_id = 'conv_group'
                  AND internal_id = 'wxid_demo_member'
                """
            )
            connection.commit()
        history = self.tools.wechat_find_participants(self.group_id, "demo_member_old")
        candidate = next(
            item
            for item in history["candidates"]
            if item["participant_id"] == before["participant_id"]
        )
        self.assertIsNone(candidate["labels"]["public_handle"])
        self.assertIsNone(candidate["labels"]["current_group_alias"])

    def test_debug_identity_detail_requires_explicit_reader_capability(self):
        policy = ReaderPolicy(mode="all_except_denylist", identity_debug=False)
        _provider, _repository, _service, tools = build_test_stack(
            self.root, self.window, policy=policy
        )
        result = tools.wechat_find_participants(self.group_id, "示例甲", detail_level="debug")
        self.assertEqual(result["code"], "POLICY_DENIED")

    def test_egress_budget_counts_structured_visible_payload(self):
        policy = ReaderPolicy(
            mode="all_except_denylist",
            identity_debug=True,
            max_text_chars_per_call=200,
        )
        _provider, _repository, _service, tools = build_test_stack(
            self.root, self.window, policy=policy
        )
        result = tools.wechat_read_messages(mode="recent", conversation_id=self.group_id, limit=100)
        self.assertEqual(result["code"], "OUTPUT_BUDGET_EXCEEDED")

    def test_nontext_transport_envelope_and_resource_path_stay_private(self):
        with closing(sqlite3.connect(self.root / "messages-1.db")) as connection:
            connection.execute(
                """
                UPDATE messages
                SET raw_content = ?, resources_json = ?
                WHERE source_message_id = 'source-msg-004'
                """,
                (
                    '<img fromusername="wxid_secret" path="/synthetic-private/x"/>',
                    json.dumps(
                        [
                            {
                                "source_ordinal": 0,
                                "kind": "image",
                                "source_resource_key": "image-004",
                                "original_name": "/synthetic-private/photo.jpg",
                                "availability": "metadata_only",
                            }
                        ]
                    ),
                ),
            )
            connection.commit()
        page = self._read_group()
        image = next(item for item in page["messages"] if item["kind"] == "image")
        self.assertIsNone(image["text"])
        self.assertEqual(image["resources"][0]["original_name"], "photo.jpg")
        self.assertNotIn("wxid_secret", str(image))
        self.assertNotIn("/synthetic-private", str(image))

    def test_decoded_anchor_contains_only_opaque_ids(self):
        page = self._read_group()
        message = page["messages"][0]
        decoded = self.service.token_codec.decode(message["anchor"])
        rendered = str(decoded)
        self.assertNotIn("source-msg-", rendered)
        self.assertNotIn("wxid_", rendered)
        self.assertEqual(decoded["message_id"], message["message_id"])

    def test_bounded_recent_absence_never_creates_recall_or_deletion(self):
        self._read_group()
        self.tools.wechat_read_messages(mode="recent", conversation_id=self.group_id, limit=1)
        with self.repository.database.connection() as connection:
            states = {
                str(row[0])
                for row in connection.execute("SELECT DISTINCT state FROM message_observations")
            }
        self.assertEqual(states, {"present"})

    def test_not_found_responses_are_coverage_aware(self):
        result = self.tools.wechat_find_participants(self.group_id, "不存在的人")
        self.assertEqual(result["candidates"], [])
        self.assertEqual(result["coverage"]["roster"], "partial")
        self.assertIn("not_found_is_coverage_bounded", result["coverage"]["notes"])
        self.assertIsNotNone(result["coverage"]["observed_time_after"])
        self.assertIsNotNone(result["coverage"]["observed_time_before"])
        missing = self.tools.wechat_read_messages(
            mode="message", message_id="wxmsg_missing", limit=1
        )
        self.assertEqual(missing["code"], "MESSAGE_NOT_FOUND")
        self.assertIn("coverage", missing["details"])

    def test_debug_participant_detail_exposes_key_kinds_not_raw_keys(self):
        result = self.tools.wechat_find_participants(
            self.group_id, "demo_member_old", detail_level="debug"
        )
        candidate = result["candidates"][0]
        self.assertEqual(candidate["source_key_kinds"], ["internal_username"])
        self.assertNotIn("wxid_demo_member", str(result))

    def test_policy_filters_discovery_and_blocks_direct_message_id(self):
        recent = self._read_group()
        target_id = recent["messages"][0]["message_id"]
        denied_policy = ReaderPolicy(
            mode="all_except_denylist",
            denied_conversation_ids=frozenset({self.group_id}),
        )
        _provider, _repository, _service, denied_tools = build_test_stack(
            self.root, self.window, policy=denied_policy
        )
        discovery = denied_tools.wechat_find_conversations("Synthetic Group")
        self.assertEqual(discovery["candidates"], [])
        direct = denied_tools.wechat_read_messages(mode="message", message_id=target_id, limit=1)
        self.assertEqual(direct["code"], "POLICY_DENIED")

    def test_paused_service_only_allows_status(self):
        _provider, _repository, _service, paused_tools = build_test_stack(
            self.root, self.window, paused=True
        )
        status = paused_tools.wechat_status()
        self.assertTrue(status["paused"])
        denied = paused_tools.wechat_find_conversations("Synthetic Group")
        self.assertEqual(denied["code"], "SERVICE_PAUSED")


if __name__ == "__main__":
    unittest.main()

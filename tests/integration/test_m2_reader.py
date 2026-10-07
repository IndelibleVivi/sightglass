from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from typing import Any
from unittest import mock

from sightglass.policy.readers import ReaderPolicy
from sightglass.reader.service import SEARCH_SCAN_BATCH_LIMIT
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


class _RosterBindingShiftProvider:
    """Delegate provider calls, bumping a shard generation after the roster read.

    The sender-query roster preflight reads participants under one snapshot; this
    shifts the manifest before the next snapshot opens so a search must refuse to
    scan under a different source version.
    """

    def __init__(self, provider: Any, shift: Any) -> None:
        self._provider = provider
        self._shift = shift
        self._roster_read = False
        self.shifted = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def list_participants(self, *args: Any, **kwargs: Any) -> Any:
        participants = self._provider.list_participants(*args, **kwargs)
        self._roster_read = True
        return participants

    def snapshot(self) -> Any:
        if self._roster_read and not self.shifted:
            self._shift()
            self.shifted = True
        return self._provider.snapshot()


class M2ReaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "source"
        self.window = Path(self.temp.name) / "state" / "window.db"
        create_synthetic_source(self.root)
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            self.root, self.window
        )
        status = self.tools.wechat_status()
        self.assertTrue(status["ready"])
        self.group_id = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _participant(self, query: str) -> dict[str, Any]:
        result = self.tools.wechat_find_participants(self.group_id, query)
        self.assertTrue(result["candidates"], result)
        return result["candidates"][0]

    def _advance_generation(self, suffix: str) -> None:
        manifest_path = self.root / "source.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["shards"][1]["generation_id"] = f"generation-2-{suffix}"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def _insert_late_demo_member_message(self) -> None:
        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            connection.execute(
                """
                INSERT INTO messages(
                    source_message_id, source_conversation_id, source_time_raw,
                    sent_at_utc, observed_at_utc, sort_seq, source_rowid,
                    wechat_type, raw_content, is_outgoing, sender_internal_id,
                    sender_local_token, sender_surface_label, resources_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "source-msg-late",
                    "conv_group",
                    "2026-09-13T08:59:00+00:00",
                    "2026-09-13T08:59:00+00:00",
                    "2026-09-13T11:00:00+00:00",
                    1,
                    99,
                    1,
                    "wxid_demo_member:\n晚到但不能漏掉",
                    0,
                    "wxid_demo_member",
                    None,
                    "原账号昵称",
                    "[]",
                ),
            )
            connection.commit()
        self._advance_generation("late")

    def _append_source_message(
        self,
        message_id: str,
        text: str,
        *,
        sender: str,
        shown_as: str,
        sent_at: str,
        sort_seq: int,
        rowid: int,
    ) -> None:
        """Append one synthetic group message the durable index will admit."""

        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            connection.execute(
                """
                INSERT INTO messages(
                    source_message_id, source_conversation_id, source_time_raw,
                    sent_at_utc, observed_at_utc, sort_seq, source_rowid,
                    wechat_type, raw_content, is_outgoing, sender_internal_id,
                    sender_local_token, sender_surface_label, resources_json
                ) VALUES (?, 'conv_group', ?, ?, '2026-09-13T11:00:00+00:00', ?, ?, 1, ?, 0,
                          ?, NULL, ?, '[]')
                """,
                (
                    message_id,
                    sent_at,
                    sent_at,
                    sort_seq,
                    rowid,
                    f"{sender}:\n{text}",
                    sender,
                    shown_as,
                ),
            )
            connection.commit()

    def test_context_anchor_returns_bounded_window_and_rejects_cross_scope(self) -> None:
        recent = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=100
        )
        target = next(item for item in recent["messages"] if item["text"] == "我发出的消息")
        context = self.tools.wechat_read_messages(
            mode="context", anchor=target["anchor"], before=1, after=1, limit=3
        )
        self.assertEqual(
            [item["message_id"] for item in context["messages"]][1], target["message_id"]
        )
        self.assertEqual(len(context["messages"]), 3)
        self.assertTrue(context["messages"][1]["retrieval"]["focus_match"])
        self.assertTrue(context["messages"][0]["retrieval"]["context_only"])
        self.assertTrue(context["messages"][2]["retrieval"]["context_only"])
        self.assertTrue(context["page"]["has_more_after"])

        wrong_scope = self.tools.wechat_read_messages(
            mode="context",
            conversation_id=self.tools.wechat_find_conversations("Demo Direct")["candidates"][0][
                "conversation_id"
            ],
            anchor=target["anchor"],
            before=1,
            after=1,
        )
        self.assertEqual(wrong_scope["code"], "CURSOR_INVALID")

        tampered = target["anchor"][:-1] + ("A" if target["anchor"][-1] != "A" else "B")
        invalid = self.tools.wechat_read_messages(
            mode="context", anchor=tampered, before=1, after=1
        )
        self.assertEqual(invalid["code"], "CURSOR_INVALID")
        mismatched = self.tools.wechat_read_messages(
            mode="context",
            anchor=target["anchor"],
            message_id="wxmsg_different",
            before=1,
            after=1,
        )
        self.assertEqual(mismatched["code"], "CURSOR_INVALID")
        ignored_field = self.tools.wechat_read_messages(
            mode="recent",
            conversation_id=self.group_id,
            anchor=target["anchor"],
            limit=100,
        )
        self.assertEqual(ignored_field["code"], "QUERY_INVALID")

    def test_context_at_conversation_boundary_is_explicit(self) -> None:
        recent = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=100
        )
        first = recent["messages"][0]
        context = self.tools.wechat_read_messages(
            mode="context", anchor=first["anchor"], before=1, after=1, limit=3
        )
        self.assertEqual(context["messages"][0]["message_id"], first["message_id"])
        self.assertFalse(context["page"]["has_more_before"])
        self.assertTrue(context["messages"][0]["retrieval"]["focus_match"])

    def test_signed_timeline_cursor_paginates_and_fails_stale(self) -> None:
        newest = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=2
        )
        cursor = newest["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)
        self.assertTrue(newest["page"]["truncated"])
        older = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=2, cursor=cursor
        )
        self.assertTrue(older["messages"])
        self.assertTrue(
            {item["message_id"] for item in newest["messages"]}.isdisjoint(
                item["message_id"] for item in older["messages"]
            )
        )

        invalid = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=2, cursor=cursor + "x"
        )
        self.assertEqual(invalid["code"], "CURSOR_INVALID")

        expired_payload = self.service.token_codec.decode(cursor)
        expired_payload["issued_at"] = "2020-01-01T00:00:00+00:00"
        expired = self.tools.wechat_read_messages(
            mode="recent",
            conversation_id=self.group_id,
            limit=2,
            cursor=self.service.token_codec.encode(expired_payload),
        )
        self.assertEqual(expired["code"], "CURSOR_STALE")

        position = self.service.token_codec.decode(cursor)["position"]
        source_row = self.repository.message_source_row(position["message_id"])
        assert source_row is not None
        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            connection.execute(
                "DELETE FROM messages WHERE source_message_id = ?",
                (source_row["source_message_id"],),
            )
            connection.commit()
        self._advance_generation("cursor-position-removed")
        stale = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=2, cursor=cursor
        )
        self.assertEqual(stale["code"], "CURSOR_STALE")

    def test_timeline_cursor_fails_closed_when_source_binding_changes(self) -> None:
        first = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=2
        )
        cursor = first["page"]["next_cursor"]
        self._advance_generation("cursor-generation-change")
        stale = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=2, cursor=cursor
        )
        self.assertEqual(stale["code"], "CURSOR_STALE")

    def test_signed_cursor_is_bound_to_reader_identity(self) -> None:
        newest = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=2
        )
        cursor = newest["page"]["next_cursor"]
        policy = ReaderPolicy(mode="all_except_denylist", search=True)
        _provider, _repository, _service, cove_tools = build_test_stack(
            self.root,
            self.window,
            policy=policy,
            reader_id="demo_reader",
            display_name="Demo Reader",
        )
        denied = cove_tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=2, cursor=cursor
        )
        self.assertEqual(denied["code"], "CURSOR_INVALID")

    def test_range_cursor_is_bound_to_time_filter(self) -> None:
        first = self.tools.wechat_read_messages(
            mode="range",
            conversation_id=self.group_id,
            time_after="2026-09-13T09:00:00+00:00",
            direction="backward",
            limit=1,
        )
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)
        changed_filter = self.tools.wechat_read_messages(
            mode="range",
            conversation_id=self.group_id,
            time_after="2026-09-13T09:03:00+00:00",
            direction="backward",
            limit=1,
            cursor=cursor,
        )
        self.assertEqual(changed_filter["code"], "CURSOR_INVALID")

    def test_speaker_with_context_merges_overlap_and_marks_context_only(self) -> None:
        self.tools.wechat_read_messages(mode="recent", conversation_id=self.group_id, limit=100)
        demo_member = self._participant("demo_member_old")
        result = self.tools.wechat_read_messages(
            mode="speaker",
            conversation_id=self.group_id,
            participant_ids=[demo_member["participant_id"]],
            speaker_view="with_context",
            before=1,
            after=1,
            limit=100,
        )
        message_ids = [item["message_id"] for item in result["messages"]]
        self.assertEqual(len(message_ids), len(set(message_ids)))
        self.assertTrue(any(item["retrieval"]["context_only"] for item in result["messages"]))
        self.assertTrue(
            all(
                item["retrieval"]["focus_match"] != item["retrieval"]["context_only"]
                for item in result["messages"]
            )
        )
        self.assertEqual(
            result["focus"]["matched_message_count"],
            sum(item["retrieval"]["focus_match"] for item in result["messages"]),
        )

    def test_speaker_reply_keeps_quoted_context_outside_sender_filter(self) -> None:
        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            connection.execute(
                """
                INSERT INTO messages(
                    source_message_id, source_conversation_id, source_time_raw,
                    sent_at_utc, observed_at_utc, sort_seq, source_rowid,
                    wechat_type, raw_content, is_outgoing, sender_internal_id,
                    sender_local_token, sender_surface_label, resources_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "source-msg-reply",
                    "conv_group",
                    "2026-09-13T09:07:00+00:00",
                    "2026-09-13T09:07:00+00:00",
                    "2026-09-13T11:00:00+00:00",
                    10,
                    100,
                    49,
                    (
                        "<msg><appmsg><title>我不同意</title><type>57</type>"
                        "<refermsg><displayname>另一个成员</displayname>"
                        "<content>被引用的原话</content></refermsg></appmsg></msg>"
                    ),
                    0,
                    "wxid_demo_member",
                    None,
                    "原账号昵称",
                    "[]",
                ),
            )
            connection.commit()
        self._advance_generation("reply")
        demo_member = self._participant("demo_member_old")
        result = self.tools.wechat_read_messages(
            mode="speaker",
            conversation_id=self.group_id,
            participant_ids=[demo_member["participant_id"]],
            speaker_view="with_context",
            before=0,
            after=0,
            limit=100,
        )
        reply = next(item for item in result["messages"] if item["kind"] == "reply")
        self.assertEqual(reply["reply"]["quoted_text"], "被引用的原话")
        self.assertEqual(reply["reply"]["quoted_sender"], "另一个成员")

    def test_speaker_cursor_pages_by_focus_not_context_message(self) -> None:
        self.tools.wechat_read_messages(mode="recent", conversation_id=self.group_id, limit=100)
        demo_member = self._participant("demo_member_old")
        first = self.tools.wechat_read_messages(
            mode="speaker",
            conversation_id=self.group_id,
            participant_ids=[demo_member["participant_id"]],
            speaker_view="with_context",
            before=1,
            after=1,
            limit=1,
        )
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)
        first_focus = {
            item["message_id"] for item in first["messages"] if item["retrieval"]["focus_match"]
        }
        second = self.tools.wechat_read_messages(
            mode="speaker",
            conversation_id=self.group_id,
            participant_ids=[demo_member["participant_id"]],
            speaker_view="with_context",
            before=0,
            after=0,
            limit=1,
            cursor=cursor,
        )
        second_focus = {
            item["message_id"] for item in second["messages"] if item["retrieval"]["focus_match"]
        }
        self.assertTrue(first_focus.isdisjoint(second_focus))

    def test_text_budget_truncates_at_message_boundary_with_continuation(self) -> None:
        policy = ReaderPolicy(
            mode="all_except_denylist",
            identity_debug=True,
            search=True,
            max_text_chars_per_call=1_900,
        )
        _provider, _repository, _service, budget_tools = build_test_stack(
            self.root, self.window, policy=policy, reader_id="budget-reader"
        )
        page = budget_tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=100
        )
        self.assertEqual(page["schema"], "sightglass.message-page.v1")
        self.assertTrue(page["messages"])
        self.assertTrue(page["page"]["truncated"])
        self.assertIsInstance(page["page"]["next_cursor"], str)
        continuation = budget_tools.wechat_read_messages(
            mode="recent",
            conversation_id=self.group_id,
            limit=100,
            cursor=page["page"]["next_cursor"],
        )
        self.assertTrue(continuation["messages"])
        self.assertTrue(
            {item["message_id"] for item in page["messages"]}.isdisjoint(
                item["message_id"] for item in continuation["messages"]
            )
        )

    def test_participant_updates_exact_replay_late_arrival_and_idempotent_ack(self) -> None:
        self.tools.wechat_read_messages(mode="recent", conversation_id=self.group_id, limit=100)
        demo_member = self._participant("demo_member_old")
        self.tools.wechat_read_messages(
            mode="speaker",
            conversation_id=self.group_id,
            participant_ids=[demo_member["participant_id"]],
            speaker_view="only",
            limit=100,
        )
        with self.repository.database.connection() as connection:
            conversation_before = int(
                connection.execute(
                    """
                    SELECT committed_observation_seq FROM reader_update_cursors
                    WHERE reader_id = 'codex' AND conversation_id = ?
                      AND scope_kind = 'conversation'
                    """,
                    (self.group_id,),
                ).fetchone()[0]
            )
        self._insert_late_demo_member_message()

        first = self.tools.wechat_read_messages(
            mode="updates",
            conversation_id=self.group_id,
            participant_ids=[demo_member["participant_id"]],
            limit=100,
        )
        self.assertEqual([item["text"] for item in first["messages"]], ["晚到但不能漏掉"])
        self.assertTrue(first["messages"][0]["retrieval"]["late_arrival"])
        delivery_id = first["page"]["delivery_id"]
        self.assertIsInstance(delivery_id, str)
        with self.repository.database.connection() as connection:
            delivery = connection.execute(
                "SELECT payload_ref FROM reader_deliveries WHERE delivery_id = ?",
                (delivery_id,),
            ).fetchone()
        payload_path = Path(delivery["payload_ref"])
        self.assertEqual(payload_path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(
            json.loads(payload_path.read_text(encoding="utf-8")),
            first,
        )
        self.assertEqual(self.service.delivery_store.serialize(first), payload_path.read_bytes())

        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.execute(
                """
                UPDATE principals SET contact_remark = '变更后的备注'
                WHERE internal_id = 'wxid_demo_member'
                """
            )
            connection.commit()
        replay = self.tools.wechat_read_messages(
            mode="updates",
            conversation_id=self.group_id,
            participant_ids=[demo_member["participant_id"]],
            limit=100,
        )
        self.assertEqual(replay, first)

        acknowledged = self.tools.wechat_read_messages(
            mode="updates",
            conversation_id=self.group_id,
            participant_ids=[demo_member["participant_id"]],
            ack_delivery_id=delivery_id,
            limit=100,
        )
        self.assertEqual(acknowledged["messages"], [])
        repeated_ack = self.tools.wechat_read_messages(
            mode="updates",
            conversation_id=self.group_id,
            participant_ids=[demo_member["participant_id"]],
            ack_delivery_id=delivery_id,
            limit=100,
        )
        self.assertEqual(repeated_ack["messages"], [])

        with self.repository.database.connection() as connection:
            conversation_cursor = connection.execute(
                """
                SELECT committed_observation_seq FROM reader_update_cursors
                WHERE reader_id = 'codex' AND conversation_id = ?
                  AND scope_kind = 'conversation'
                """,
                (self.group_id,),
            ).fetchone()
            participant_cursor = connection.execute(
                """
                SELECT committed_observation_seq FROM reader_update_cursors
                WHERE reader_id = 'codex' AND conversation_id = ?
                  AND scope_kind = 'participant' AND scope_key = ?
                """,
                (self.group_id, demo_member["participant_id"]),
            ).fetchone()
        self.assertIsNotNone(conversation_cursor)
        self.assertEqual(int(conversation_cursor["committed_observation_seq"]), conversation_before)
        self.assertIsNotNone(participant_cursor)

    def test_query_filtered_updates_advance_only_filter_set_scope(self) -> None:
        self.tools.wechat_read_messages(mode="recent", conversation_id=self.group_id, limit=100)
        demo_member = self._participant("demo_member_old")
        self.tools.wechat_read_messages(
            mode="speaker",
            conversation_id=self.group_id,
            participant_ids=[demo_member["participant_id"]],
            speaker_view="only",
            limit=100,
        )
        with self.repository.database.connection() as connection:
            participant_before = int(
                connection.execute(
                    """
                    SELECT committed_observation_seq FROM reader_update_cursors
                    WHERE reader_id = 'codex' AND conversation_id = ?
                      AND scope_kind = 'participant' AND scope_key = ?
                    """,
                    (self.group_id, demo_member["participant_id"]),
                ).fetchone()[0]
            )
        self._insert_late_demo_member_message()
        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            connection.execute(
                """
                INSERT INTO messages(
                    source_message_id, source_conversation_id, source_time_raw,
                    sent_at_utc, observed_at_utc, sort_seq, source_rowid,
                    wechat_type, raw_content, is_outgoing, sender_internal_id,
                    sender_local_token, sender_surface_label, resources_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "source-msg-filtered-update",
                    "conv_group",
                    "2026-09-13T09:08:00+00:00",
                    "2026-09-13T09:08:00+00:00",
                    "2026-09-13T11:01:00+00:00",
                    10,
                    101,
                    1,
                    "wxid_demo_member:\nneedle update",
                    0,
                    "wxid_demo_member",
                    None,
                    "原账号昵称",
                    "[]",
                ),
            )
            connection.commit()
        self._advance_generation("filtered-update")
        delivery = self.tools.wechat_read_messages(
            mode="updates",
            conversation_id=self.group_id,
            participant_ids=[demo_member["participant_id"]],
            query="needle",
            limit=100,
        )
        self.assertEqual([item["text"] for item in delivery["messages"]], ["needle update"])
        self.tools.wechat_read_messages(
            mode="updates",
            conversation_id=self.group_id,
            participant_ids=[demo_member["participant_id"]],
            query="needle",
            ack_delivery_id=delivery["page"]["delivery_id"],
            limit=100,
        )
        with self.repository.database.connection() as connection:
            participant_after = int(
                connection.execute(
                    """
                    SELECT committed_observation_seq FROM reader_update_cursors
                    WHERE reader_id = 'codex' AND conversation_id = ?
                      AND scope_kind = 'participant' AND scope_key = ?
                    """,
                    (self.group_id, demo_member["participant_id"]),
                ).fetchone()[0]
            )
            filter_set_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM reader_update_cursors
                    WHERE reader_id = 'codex' AND conversation_id = ?
                      AND scope_kind = 'filter_set'
                    """,
                    (self.group_id,),
                ).fetchone()[0]
            )
        self.assertEqual(participant_after, participant_before)
        self.assertEqual(filter_set_count, 1)

    def test_failed_ack_rolls_back_and_original_delivery_remains_replayable(self) -> None:
        self.tools.wechat_read_messages(mode="recent", conversation_id=self.group_id, limit=100)
        self._insert_late_demo_member_message()
        first = self.tools.wechat_read_messages(
            mode="updates", conversation_id=self.group_id, limit=100
        )
        delivery_id = first["page"]["delivery_id"]
        shard = self.root / "messages-2.db"
        held = self.root / "messages-2.db.held"
        shard.rename(held)
        try:
            failed = self.tools.wechat_read_messages(
                mode="updates",
                conversation_id=self.group_id,
                ack_delivery_id=delivery_id,
                limit=100,
            )
            self.assertEqual(failed["code"], "SOURCE_INCOMPLETE")
        finally:
            held.rename(shard)
        replay = self.tools.wechat_read_messages(
            mode="updates", conversation_id=self.group_id, limit=100
        )
        self.assertEqual(replay, first)

    def test_concurrent_updates_converge_on_one_exact_pending_delivery(self) -> None:
        self.tools.wechat_read_messages(mode="recent", conversation_id=self.group_id, limit=100)
        self._insert_late_demo_member_message()
        _provider, _repository, _service, second_tools = build_test_stack(self.root, self.window)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    tools.wechat_read_messages,
                    mode="updates",
                    conversation_id=self.group_id,
                    limit=100,
                )
                for tools in (self.tools, second_tools)
            ]
            results = [future.result(timeout=10) for future in futures]
        self.assertEqual(results[0], results[1])
        with self.repository.database.connection() as connection:
            pending_count = connection.execute(
                """
                SELECT COUNT(*) FROM reader_deliveries
                WHERE reader_id = 'codex' AND conversation_id = ?
                  AND scope_kind = 'conversation' AND status = 'pending'
                """,
                (self.group_id,),
            ).fetchone()[0]
        self.assertEqual(pending_count, 1)

    def test_policy_change_blocks_pending_delivery(self) -> None:
        self.tools.wechat_read_messages(mode="recent", conversation_id=self.group_id, limit=100)
        self._insert_late_demo_member_message()
        pending = self.tools.wechat_read_messages(
            mode="updates", conversation_id=self.group_id, limit=100
        )
        self.assertIsNotNone(pending["page"]["delivery_id"])
        denied_policy = ReaderPolicy(
            mode="all_except_denylist",
            denied_conversation_ids=frozenset({self.group_id}),
            search=True,
        )
        _provider, _repository, _service, denied_tools = build_test_stack(
            self.root, self.window, policy=denied_policy
        )
        denied = denied_tools.wechat_read_messages(
            mode="updates", conversation_id=self.group_id, limit=100
        )
        self.assertEqual(denied["code"], "POLICY_DENIED")
        with self.repository.database.connection() as connection:
            status = connection.execute(
                "SELECT status FROM reader_deliveries WHERE delivery_id = ?",
                (pending["page"]["delivery_id"],),
            ).fetchone()[0]
        self.assertEqual(status, "expired")

    def test_allowlist_filters_discovery_search_and_direct_id_reads(self) -> None:
        direct_id = self.tools.wechat_find_conversations("Demo Direct")["candidates"][0][
            "conversation_id"
        ]
        policy = ReaderPolicy(
            mode="allowlist",
            allowed_conversation_ids=frozenset({self.group_id}),
            search=True,
        )
        _provider, _repository, _service, allowed_tools = build_test_stack(
            self.root, self.window, policy=policy, reader_id="allowlisted"
        )
        self.assertEqual(allowed_tools.wechat_find_conversations("Demo Direct")["candidates"], [])
        blocked = allowed_tools.wechat_read_messages(
            mode="recent", conversation_id=direct_id, limit=100
        )
        self.assertEqual(blocked["code"], "POLICY_DENIED")
        search = allowed_tools.wechat_search_messages(query="消息", limit=50)
        self.assertTrue(search["hits"])
        self.assertEqual(
            {item["id"] for item in search["conversations"]},
            {self.group_id},
        )

    def test_search_is_canonical_scoped_and_supports_nontext_sender_filter(self) -> None:
        result = self.tools.wechat_search_messages(
            query="保留 第二行", conversation_ids=[self.group_id], limit=50
        )
        self.assertEqual(len(result["hits"]), 1)
        self.assertEqual(result["hits"][0][4], "  保留  内部空白！\n第二行")
        self.assertTrue(result["source_receipt"]["search"]["canonical_validated"])

        phrase = self.tools.wechat_search_messages(
            query='"内部空白！\n第二行"', conversation_ids=[self.group_id], limit=50
        )
        self.assertEqual(len(phrase["hits"]), 1)
        malformed = self.tools.wechat_search_messages(
            query='"没有结束', conversation_ids=[self.group_id], limit=50
        )
        self.assertEqual(malformed["code"], "QUERY_INVALID")
        whitespace = self.tools.wechat_search_messages(
            query="   ", conversation_ids=[self.group_id], limit=50
        )
        self.assertEqual(whitespace["code"], "QUERY_INVALID")

        demo_member = self._participant("demo_member_old")
        nontext = self.tools.wechat_search_messages(
            query="",
            conversation_ids=[self.group_id],
            participant_ids=[demo_member["participant_id"]],
            after="2026-09-13T09:02:30+00:00",
            before="2026-09-13T09:03:30+00:00",
            limit=50,
        )
        self.assertEqual([item[3] for item in nontext["hits"]], ["image"])

        ambiguous = self.tools.wechat_search_messages(
            query="消息",
            conversation_ids=[self.group_id],
            sender_query="示例甲",
            limit=50,
        )
        self.assertTrue(ambiguous["ambiguous_sender"])
        self.assertGreaterEqual(len(ambiguous["participant_candidates"]), 2)
        self.assertEqual(ambiguous["hits"], [])

        denied_policy = ReaderPolicy(
            mode="all_except_denylist",
            denied_conversation_ids=frozenset({self.group_id}),
            search=True,
        )
        _provider, _repository, _service, denied_tools = build_test_stack(
            self.root, self.window, policy=denied_policy, reader_id="search-denied"
        )
        denied = denied_tools.wechat_search_messages(
            query="保留", conversation_ids=[self.group_id], limit=50
        )
        self.assertEqual(denied["code"], "POLICY_DENIED")

        no_search = ReaderPolicy(mode="all_except_denylist", search=False)
        _provider, _repository, _service, no_search_tools = build_test_stack(
            self.root, self.window, policy=no_search, reader_id="no-search"
        )
        capability_denied = no_search_tools.wechat_search_messages(query="保留", limit=50)
        self.assertEqual(capability_denied["code"], "POLICY_DENIED")

    def test_search_cursor_continues_without_duplicates_and_rejects_tampering(self) -> None:
        first = self.tools.wechat_search_messages(
            query="消息", conversation_ids=[self.group_id], limit=1
        )
        self.assertTrue(first["page"]["truncated"])
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)
        decoded = self.service.token_codec.decode(cursor)
        self.assertNotIn("source-msg-", str(decoded))
        self.assertNotIn("wxid_", str(decoded))
        second = self.tools.wechat_search_messages(
            query="消息",
            conversation_ids=[self.group_id],
            limit=1,
            cursor=cursor,
        )
        self.assertTrue(second["hits"])
        self.assertNotEqual(first["hits"][0][0], second["hits"][0][0])
        tampered = self.tools.wechat_search_messages(
            query="消息",
            conversation_ids=[self.group_id],
            limit=1,
            cursor=cursor + "x",
        )
        self.assertEqual(tampered["code"], "CURSOR_INVALID")

    def test_search_canonical_validation_excludes_removed_cached_message(self) -> None:
        first = self.tools.wechat_search_messages(
            query="复制来的消息", conversation_ids=[self.group_id], limit=50
        )
        self.assertEqual(len(first["hits"]), 1)
        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            connection.execute("DELETE FROM messages WHERE source_message_id = 'source-msg-009'")
            connection.commit()
        self._advance_generation("search-removal")
        after = self.tools.wechat_search_messages(
            query="复制来的消息", conversation_ids=[self.group_id], limit=50
        )
        self.assertEqual(after["hits"], [])

    def test_search_single_hit_does_not_advertise_a_cursor(self) -> None:
        result = self.tools.wechat_search_messages(
            query="复制来的消息", conversation_ids=[self.group_id], limit=1
        )
        self.assertEqual(len(result["hits"]), 1)
        self.assertFalse(result["page"]["truncated"])
        self.assertIsNone(result["page"]["next_cursor"])

    def test_search_reports_partial_catalog_coverage(self) -> None:
        root = Path(self.temp.name) / "partial-search-source"
        window = Path(self.temp.name) / "partial-search-state" / "window.db"
        create_synthetic_source(root, catalog_complete=False)
        _provider, _repository, _service, tools = build_test_stack(root, window)
        result = tools.wechat_search_messages(query="消息", limit=50)
        self.assertFalse(result["source_receipt"]["complete"])
        self.assertEqual(result["source_receipt"]["coverage"]["catalog"], "partial")
        self.assertFalse(result["source_receipt"]["search"]["all_authorized_conversations"])
        self.assertIn("catalog_partial", result["source_receipt"]["warnings"])

    def test_search_unique_sender_query_matches_explicit_participant_scope(self) -> None:
        self._append_source_message(
            "source-msg-sender-a",
            "共享词 alpha",
            sender="wxid_demo_member",
            shown_as="原账号昵称",
            sent_at="2026-09-13T09:30:00+00:00",
            sort_seq=300,
            rowid=300,
        )
        self._append_source_message(
            "source-msg-sender-b",
            "共享词 beta",
            sender="wxid_demo_member2",
            shown_as="示例甲",
            sent_at="2026-09-13T09:31:00+00:00",
            sort_seq=301,
            rowid=301,
        )
        self._advance_generation("sender-scope")
        demo_member = self._participant("demo_member_old")["participant_id"]

        every_sender = self.tools.wechat_search_messages(
            query="共享词", conversation_ids=[self.group_id], limit=50
        )
        self.assertEqual(
            {item[4] for item in every_sender["hits"]},
            {"共享词 alpha", "共享词 beta"},
        )

        explicit = self.tools.wechat_search_messages(
            query="共享词",
            conversation_ids=[self.group_id],
            participant_ids=[demo_member],
            limit=50,
        )
        resolved = self.tools.wechat_search_messages(
            query="共享词",
            conversation_ids=[self.group_id],
            sender_query="demo_member_old",
            limit=50,
        )
        self.assertFalse(resolved["ambiguous_sender"])
        self.assertEqual(
            [item[0] for item in resolved["hits"]],
            [item[0] for item in explicit["hits"]],
        )
        self.assertEqual([item[4] for item in resolved["hits"]], ["共享词 alpha"])

    def test_search_cursor_is_bound_to_the_resolved_sender_scope(self) -> None:
        for index, (sender, shown_as) in enumerate(
            (
                ("wxid_demo_member", "原账号昵称"),
                ("wxid_demo_member", "原账号昵称"),
                ("wxid_demo_member2", "示例甲"),
            )
        ):
            self._append_source_message(
                f"source-msg-scope-{index}",
                f"共享词 scope {index}",
                sender=sender,
                shown_as=shown_as,
                sent_at=f"2026-09-13T09:3{index}:00+00:00",
                sort_seq=310 + index,
                rowid=310 + index,
            )
        self._advance_generation("sender-cursor")

        first = self.tools.wechat_search_messages(
            query="共享词",
            conversation_ids=[self.group_id],
            sender_query="demo_member_old",
            limit=1,
        )
        self.assertTrue(first["page"]["truncated"])
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)

        mismatch = self.tools.wechat_search_messages(
            query="共享词",
            conversation_ids=[self.group_id],
            sender_query="demo_member_other",
            limit=1,
            cursor=cursor,
        )
        self.assertEqual(mismatch["code"], "CURSOR_INVALID")

    def test_search_returns_the_current_source_version_on_first_read(self) -> None:
        cached = self.tools.wechat_search_messages(
            query="同名", conversation_ids=[self.group_id], limit=50
        )
        self.assertEqual([item[4] for item in cached["hits"]], ["同名另一个人"])

        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            connection.execute(
                "UPDATE messages SET raw_content = ? WHERE source_message_id = 'source-msg-005'",
                ("wxid_demo_member2:\n同名 新正文",),
            )
            connection.commit()
        self._advance_generation("body-correction")

        current = self.tools.wechat_search_messages(
            query="同名", conversation_ids=[self.group_id], limit=50
        )
        self.assertEqual([item[4] for item in current["hits"]], ["同名 新正文"])

    def test_search_excludes_a_message_whose_current_sender_changed(self) -> None:
        self._append_source_message(
            "source-msg-mover",
            "换人词 moved",
            sender="wxid_demo_member",
            shown_as="原账号昵称",
            sent_at="2026-09-13T09:30:00+00:00",
            sort_seq=340,
            rowid=340,
        )
        self._advance_generation("sender-correction-cache")
        demo_member = self._participant("demo_member_old")["participant_id"]
        outsider = self._participant("非好友成员")["participant_id"]
        before = self.tools.wechat_search_messages(
            query="换人词",
            conversation_ids=[self.group_id],
            participant_ids=[demo_member],
            limit=50,
        )
        self.assertEqual([item[4] for item in before["hits"]], ["换人词 moved"])

        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            connection.execute(
                """
                UPDATE messages SET sender_internal_id = 'wxid_outsider',
                       sender_surface_label = '非好友成员', raw_content = ?
                WHERE source_message_id = 'source-msg-mover'
                """,
                ("wxid_outsider:\n换人词 moved",),
            )
            connection.commit()
        self._advance_generation("sender-correction-moved")

        excluded = self.tools.wechat_search_messages(
            query="换人词",
            conversation_ids=[self.group_id],
            participant_ids=[demo_member],
            limit=50,
        )
        self.assertEqual(excluded["hits"], [])
        included = self.tools.wechat_search_messages(
            query="换人词",
            conversation_ids=[self.group_id],
            participant_ids=[outsider],
            limit=50,
        )
        self.assertEqual([item[4] for item in included["hits"]], ["换人词 moved"])

    def test_search_bounds_candidate_scan_for_a_large_common_term(self) -> None:
        for index in range(300):
            self._append_source_message(
                f"source-msg-bulk-{index:03d}",
                f"批量词 history {index:03d}",
                sender="wxid_demo_member",
                shown_as="原账号昵称",
                sent_at=f"2026-09-13T10:{index // 60:02d}:{index % 60:02d}+00:00",
                sort_seq=400 + index,
                rowid=400 + index,
            )
        self._advance_generation("bounded-scan")
        # Admit the whole synthetic batch so the candidate set really exceeds one
        # bounded scan window instead of relying on a bounded live tail.
        self.service.sync_source_once(initial_tail=500, batch_limit=500)

        real_candidates = self.repository.search_candidate_window
        real_get_message = self.provider.get_message
        rows_fetched = 0
        get_message_calls = 0
        batch_limits: list[int | None] = []

        def counting_candidates(*args: Any, **kwargs: Any) -> Any:
            nonlocal rows_fetched
            batch_limits.append(kwargs.get("limit"))
            rows = real_candidates(*args, **kwargs)
            rows_fetched += len(rows)
            return rows

        def counting_get_message(*args: Any, **kwargs: Any) -> Any:
            nonlocal get_message_calls
            get_message_calls += 1
            return real_get_message(*args, **kwargs)

        with (
            mock.patch.object(
                self.repository, "search_candidate_window", side_effect=counting_candidates
            ),
            mock.patch.object(self.provider, "get_message", side_effect=counting_get_message),
        ):
            page = self.tools.wechat_search_messages(
                query="批量词", conversation_ids=[self.group_id], limit=1
            )

        self.assertEqual(len(page["hits"]), 1)
        self.assertTrue(page["page"]["truncated"])
        self.assertTrue(batch_limits)
        self.assertTrue(all(limit is not None for limit in batch_limits))
        self.assertLessEqual(rows_fetched, SEARCH_SCAN_BATCH_LIMIT)
        self.assertLessEqual(get_message_calls, SEARCH_SCAN_BATCH_LIMIT)
        self.assertLess(rows_fetched, 300)

    def test_search_absent_term_examines_at_most_the_candidate_budget(self) -> None:
        # 300 durable rows exist, but the absent term matches none. Recall is an
        # ordered *keyset window* of the index, so the scan must examine at most the
        # candidate budget rather than running a LIKE scan over the whole body
        # history (which previously examined every indexed row before LIMIT stopped).
        for index in range(300):
            self._append_source_message(
                f"source-msg-absent-{index:03d}",
                f"存在词 body {index:03d}",
                sender="wxid_demo_member",
                shown_as="原账号昵称",
                sent_at=f"2026-09-13T11:{index // 60:02d}:{index % 60:02d}+00:00",
                sort_seq=900 + index,
                rowid=900 + index,
            )
        self._advance_generation("absent-scan")
        self.service.sync_source_once(initial_tail=500, batch_limit=500)

        real_candidates = self.repository.search_candidate_window
        rows_examined = 0
        windows: list[int] = []

        def counting_candidates(*args: Any, **kwargs: Any) -> Any:
            nonlocal rows_examined
            rows = real_candidates(*args, **kwargs)
            rows_examined += len(rows)
            windows.append(len(rows))
            return rows

        with (
            mock.patch.object(
                self.repository, "search_candidate_window", side_effect=counting_candidates
            ),
            mock.patch("sightglass.reader.service.SEARCH_SCAN_CANDIDATE_BUDGET", 250),
            mock.patch("sightglass.reader.service.SEARCH_SCAN_BATCH_LIMIT", 100),
        ):
            page = self.tools.wechat_search_messages(
                query="绝无此词",
                conversation_ids=[self.group_id],
                limit=5,
            )

        self.assertEqual(page["hits"], [])
        self.assertTrue(page["page"]["truncated"])
        self.assertIsNotNone(page["page"]["next_cursor"])
        self.assertTrue(page["source_receipt"]["search"]["scan"]["budget_exhausted"])
        self.assertIn("search_scan_budget_exhausted", page["source_receipt"]["warnings"])
        # Every examined row came from an explicitly bounded window, and the total
        # never exceeded the candidate budget.
        self.assertTrue(windows)
        self.assertTrue(all(size <= 100 for size in windows))
        # The scan itself is budget-bound; the only extra row is the single-row
        # ``exists`` peek that decides whether an exact end or a full window ended
        # the scan (a constant, not a history scan).
        self.assertLessEqual(rows_examined, 250 + 1)
        self.assertLess(rows_examined, 300)

        # The window API is keyset-bounded and *unfiltered*: with an absent term it
        # still returns exactly ``limit`` ordered rows instead of doing a SQL LIKE
        # scan that returns nothing and never engages the service budget.
        conversation_id = self.repository.conversation_id_for(
            self.repository.account_id_for("synthetic-account-demo"),
            "conv_group",
        )
        window = self.repository.search_candidate_window(
            (conversation_id,), after_key=None, limit=40
        )
        self.assertEqual(len(window), 40)
        keys = [self.service._search_keyset(row) for row in window]  # noqa: SLF001
        self.assertEqual(keys, sorted(keys))
        self.assertEqual(len(set(keys)), len(keys))

    def test_search_continuation_resumes_after_the_last_scanned_candidate(self) -> None:
        for index in range(3):
            self._append_source_message(
                f"source-msg-cont-{index}",
                f"继续词 history {index}",
                sender="wxid_demo_member",
                shown_as="原账号昵称",
                # These keyset-earliest rows sit before every fixture row, and the
                # time window below isolates them, so the bounded unfiltered scan
                # consumes exactly the rows this test reasons about.
                sent_at=f"2026-09-13T08:1{index}:00+00:00",
                sort_seq=500 + index,
                rowid=500 + index,
            )
        self._advance_generation("continuation")
        demo_member = self._participant("demo_member_old")["participant_id"]

        real_candidates = self.repository.search_candidate_window
        real_get_message = self.provider.get_message
        calls: list[tuple[Any, list[Any]]] = []

        def recording_candidates(*args: Any, **kwargs: Any) -> Any:
            rows = real_candidates(*args, **kwargs)
            calls.append((kwargs.get("after_key"), list(rows)))
            return rows

        def stale_first(*args: Any, **kwargs: Any) -> Any:
            if args[1] in {"source-msg-cont-0", "source-msg-cont-1"}:
                return None
            return real_get_message(*args, **kwargs)

        with (
            mock.patch.object(
                self.repository, "search_candidate_window", side_effect=recording_candidates
            ),
            mock.patch.object(self.provider, "get_message", side_effect=stale_first),
            mock.patch("sightglass.reader.service.SEARCH_SCAN_CANDIDATE_BUDGET", 2),
        ):
            first = self.tools.wechat_search_messages(
                query="继续词",
                conversation_ids=[self.group_id],
                participant_ids=[demo_member],
                after="2026-09-13T08:00:00+00:00",
                before="2026-09-13T09:00:00+00:00",
                limit=1,
            )
            first_calls = list(calls)
            calls.clear()
            second = self.tools.wechat_search_messages(
                query="继续词",
                conversation_ids=[self.group_id],
                participant_ids=[demo_member],
                after="2026-09-13T08:00:00+00:00",
                before="2026-09-13T09:00:00+00:00",
                limit=1,
                cursor=first["page"]["next_cursor"],
            )

        self.assertEqual(first["hits"], [])
        self.assertTrue(first["page"]["truncated"])
        self.assertIsNotNone(first["page"]["next_cursor"])
        self.assertTrue(first["source_receipt"]["search"]["scan"]["budget_exhausted"])
        self.assertIn(
            "search_scan_budget_exhausted", first["source_receipt"]["warnings"]
        )

        scanned = first_calls[0][1]
        self.assertEqual(len(scanned), 2)
        expected_resume = self.service._search_keyset(scanned[-1])  # noqa: SLF001
        self.assertEqual(calls[0][0], expected_resume)
        self.assertEqual([item[4] for item in second["hits"]], ["继续词 history 2"])

    def test_search_continuation_reaches_the_hit_after_the_scan_frontier(self) -> None:
        for index in range(3):
            self._append_source_message(
                f"source-msg-frontier-{index}",
                f"前锋词 history {index}",
                sender="wxid_demo_member",
                shown_as="原账号昵称",
                sent_at=f"2026-09-13T07:2{index}:00+00:00",
                sort_seq=520 + index,
                rowid=520 + index,
            )
        self._advance_generation("frontier")
        demo_member = self._participant("demo_member_old")["participant_id"]

        real_get_message = self.provider.get_message

        def stale_first(*args: Any, **kwargs: Any) -> Any:
            if args[1] in {"source-msg-frontier-0"}:
                return None
            return real_get_message(*args, **kwargs)

        with (
            mock.patch.object(self.provider, "get_message", side_effect=stale_first),
            mock.patch("sightglass.reader.service.SEARCH_SCAN_CANDIDATE_BUDGET", 1),
        ):
            first = self.tools.wechat_search_messages(
                query="前锋词",
                conversation_ids=[self.group_id],
                participant_ids=[demo_member],
                after="2026-09-13T07:00:00+00:00",
                before="2026-09-13T08:00:00+00:00",
                limit=1,
            )
            second = self.tools.wechat_search_messages(
                query="前锋词",
                conversation_ids=[self.group_id],
                participant_ids=[demo_member],
                after="2026-09-13T07:00:00+00:00",
                before="2026-09-13T08:00:00+00:00",
                limit=1,
                cursor=first["page"]["next_cursor"],
            )

        self.assertEqual(first["hits"], [])
        self.assertEqual(
            [item[4] for item in second["hits"]], ["前锋词 history 1"]
        )

    def test_search_sender_free_query_reads_no_conversation_roster(self) -> None:
        with mock.patch.object(
            self.provider,
            "list_participants",
            side_effect=AssertionError("a sender-free search must not scan rosters"),
        ) as roster:
            result = self.tools.wechat_search_messages(
                query="保留", conversation_ids=[self.group_id], limit=50
            )
        self.assertFalse(roster.called)
        self.assertTrue(result["hits"])

    def test_search_sender_query_fails_closed_when_the_roster_snapshot_changes(self) -> None:
        self.service.sync_source_once(initial_tail=20, batch_limit=20)
        manifest = self.root / "source.json"

        def shift() -> None:
            data = json.loads(manifest.read_text(encoding="utf-8"))
            data["shards"][1]["generation_id"] = "generation-2-roster-shift"
            manifest.write_text(
                json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

        shifted = _RosterBindingShiftProvider(self.provider, shift)
        self.service.provider = shifted  # type: ignore[reportAttributeAccessIssue]
        result = self.tools.wechat_search_messages(
            query="保留",
            conversation_ids=[self.group_id],
            sender_query="demo_member_old",
            limit=50,
        )

        self.assertTrue(shifted.shifted)
        self.assertEqual(result["schema"], "sightglass.error.v1")
        self.assertEqual(result["code"], "SOURCE_GENERATION_CHANGED")
        self.assertNotIn("hits", result)
        self.assertNotIn("next_cursor", result)
        rendered = json.dumps(result, ensure_ascii=False)
        self.assertNotIn("wxmsg_", rendered)
        self.assertNotIn("wxperson_", rendered)
        self.assertNotIn("source-msg-", rendered)

    def test_every_tool_call_records_redacted_success_or_failure_receipt(self) -> None:
        self.tools.wechat_search_messages(
            query="PRIVATE BODY MUST NOT PERSIST", conversation_ids=[self.group_id], limit=50
        )
        failure = self.tools.wechat_read_messages(
            mode="context", anchor="not-a-token", before=1, after=1
        )
        self.assertEqual(failure["code"], "CURSOR_INVALID")
        with self.repository.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT tool_name, outcome, scope_digest, warning_codes_json
                FROM access_receipts ORDER BY started_at, receipt_id
                """
            ).fetchall()
        self.assertTrue(any(row["outcome"] == "ok" for row in rows))
        self.assertTrue(any(row["outcome"] == "CURSOR_INVALID" for row in rows))
        rendered = json.dumps([dict(row) for row in rows], ensure_ascii=False)
        self.assertNotIn("PRIVATE BODY MUST NOT PERSIST", rendered)
        self.assertNotIn("Synthetic Group", rendered)
        self.assertNotIn(str(self.root), rendered)


if __name__ == "__main__":
    unittest.main()

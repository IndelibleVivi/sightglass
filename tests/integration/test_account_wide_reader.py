from __future__ import annotations

import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest import mock

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.db import WindowDB
from sightglass.policy.readers import ReaderPolicy
from sightglass.source.identity import opaque_id
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


class _GatedReadProvider:
    """Delegate provider calls, but park message reads until the test releases them."""

    def __init__(
        self, provider: Any, entered: threading.Event, release: threading.Event
    ) -> None:
        self._provider = provider
        self._entered = entered
        self._release = release

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def _gate(self) -> None:
        self._entered.set()
        self._release.wait(timeout=15)

    def get_message(self, *args: Any, **kwargs: Any) -> Any:
        self._gate()
        return self._provider.get_message(*args, **kwargs)

    def read_recent(self, *args: Any, **kwargs: Any) -> Any:
        self._gate()
        return self._provider.read_recent(*args, **kwargs)

    def read_range(self, *args: Any, **kwargs: Any) -> Any:
        self._gate()
        return self._provider.read_range(*args, **kwargs)


class _RecordingReadProvider:
    """Delegate provider calls and record which conversations were read."""

    def __init__(self, provider: Any) -> None:
        self._provider = provider
        self.reads: list[str] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def take(self) -> list[str]:
        reads, self.reads = self.reads, []
        return reads

    def read_recent(
        self, _account_id: str, conversation_source_id: str, *args: Any, **kwargs: Any
    ) -> Any:
        self.reads.append(conversation_source_id)
        return self._provider.read_recent(
            _account_id, conversation_source_id, *args, **kwargs
        )

    def read_range(
        self, _account_id: str, conversation_source_id: str, *args: Any, **kwargs: Any
    ) -> Any:
        self.reads.append(conversation_source_id)
        return self._provider.read_range(
            _account_id, conversation_source_id, *args, **kwargs
        )


class _CatalogCountingProvider:
    """Delegate provider calls while counting explicit catalog traversals."""

    def __init__(self, provider: Any) -> None:
        self._provider = provider
        self.list_conversations_calls = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def list_conversations(self, *args: Any, **kwargs: Any) -> Any:
        self.list_conversations_calls += 1
        return self._provider.list_conversations(*args, **kwargs)


class _UnreadProvider:
    """Delegate provider calls, reporting a standing unread backlog."""

    def __init__(self, provider: Any, unread_by_conversation: dict[str, int]) -> None:
        self._provider = provider
        self._unread = dict(unread_by_conversation)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def list_conversations(self, account_id: str, snapshot: Any) -> Any:
        return [
            replace(
                conversation,
                unread_count=self._unread.get(
                    conversation.source_conversation_id, conversation.unread_count
                ),
            )
            for conversation in self._provider.list_conversations(account_id, snapshot)
        ]


class _MessageEvidenceProvider:
    """Delegate messages that claim complete sender evidence, forbidding roster scans."""

    def __init__(self, provider: Any) -> None:
        self._provider = provider
        self.descriptor = replace(
            provider.descriptor, message_sender_evidence_complete=True
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def list_participants(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("message reads must ingest sender evidence directly")


class _FailingExitSnapshot:
    """Wrap a real snapshot context, failing the final validation on a clean exit."""

    def __init__(self, context: Any) -> None:
        self._context = context

    def __enter__(self) -> Any:
        return self._context.__enter__()

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        self._context.__exit__(exc_type, exc, traceback)
        if exc_type is None:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        return False


class _FailingExitProvider:
    """Delegate provider calls, but report a generation change at snapshot exit."""

    def __init__(self, provider: Any) -> None:
        self._provider = provider

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def snapshot(self) -> _FailingExitSnapshot:
        return _FailingExitSnapshot(self._provider.snapshot())


class AccountWideReaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "source"
        self.window = Path(self.temp.name) / "state" / "window.db"
        create_synthetic_source(self.root)
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            self.root,
            self.window,
            policy=ReaderPolicy(mode="all_except_denylist", identity_debug=True),
        )
        self.account_id = opaque_id("wxacct", "synthetic-account-demo")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _append_message(
        self,
        *,
        message_id: str,
        conversation_id: str,
        sent_at: str,
        rowid: int,
        text: str,
    ) -> None:
        with closing(sqlite3.connect(self.root / "messages-1.db")) as connection:
            connection.execute(
                """
                INSERT INTO messages(
                    source_message_id, source_conversation_id, source_time_raw,
                    sent_at_utc, observed_at_utc, sort_seq, source_rowid,
                    wechat_type, raw_content, is_outgoing, sender_internal_id,
                    sender_local_token, sender_surface_label, resources_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, 0, 'wxid_demo_member', NULL, '示例甲', '[]')
                """,
                (
                    message_id,
                    conversation_id,
                    sent_at,
                    sent_at,
                    sent_at,
                    rowid,
                    rowid,
                    text,
                ),
            )
            connection.commit()

    def test_catalog_paginates_more_than_two_hundred_without_duplicates(self) -> None:
        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.executemany(
                "INSERT INTO conversations VALUES (?, 'direct', ?, ?, 1)",
                (
                    (
                        f"catalog-{index:03d}",
                        f"Catalog {index:03d}",
                        f"2026-09-12T{index % 24:02d}:{index % 60:02d}:00+00:00",
                    )
                    for index in range(205)
                ),
            )
            connection.commit()

        seen: list[str] = []
        cursor = None
        while True:
            page = self.tools.wechat_find_conversations(
                "", limit=100, cursor=cursor
            )
            self.assertEqual(page["schema"], "sightglass.conversation-catalog.v2")
            seen.extend(item["conversation_id"] for item in page["candidates"])
            cursor = page["page"]["next_cursor"]
            if cursor is None:
                break

        self.assertEqual(len(seen), 207)
        self.assertEqual(len(seen), len(set(seen)))

    def test_inbox_cursor_keeps_the_original_snapshot_when_a_chat_appends(self) -> None:
        first_sync = self.service.sync_source_once(initial_tail=20, batch_limit=20)
        self.assertEqual(first_sync["conversation_count"], 2)
        first = self.tools.wechat_read_inbox(limit=1, include_latest="text")
        self.assertEqual(first["schema"], "sightglass.inbox-page.v1")
        self.assertEqual(len(first["items"]), 1)
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)
        first_id = first["items"][0]["conversation_id"]
        self.assertEqual(first["coverage"]["indexed_conversations"], 2)

        self._append_message(
            message_id="source-msg-inbox-append",
            conversation_id="conv_direct",
            sent_at="2026-09-13T09:10:00+00:00",
            rowid=100,
            text="new direct append",
        )
        second_sync = self.service.sync_source_once(initial_tail=20, batch_limit=20)
        self.assertGreaterEqual(second_sync["message_count"], 1)

        continuation = self.tools.wechat_read_inbox(
            limit=1,
            include_latest="text",
            cursor=cursor,
        )
        self.assertEqual(len(continuation["items"]), 1)
        self.assertNotEqual(continuation["items"][0]["conversation_id"], first_id)
        self.assertIsNone(continuation["page"]["next_cursor"])
        self.assertEqual(continuation["coverage"]["indexed_conversations"], 2)

        fresh = self.tools.wechat_read_inbox(limit=1, include_latest="text")
        self.assertEqual(fresh["items"][0]["latest"]["text"], "new direct append")

    def test_catalog_stale_when_an_unseen_conversation_moves_ahead(self) -> None:
        first = self.tools.wechat_find_conversations("", limit=1)
        self.assertEqual(len(first["candidates"]), 1)
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)

        # The unseen conversation's activity moves it ahead of the returned row.
        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.execute(
                "UPDATE conversations SET last_message_at_utc = ? WHERE source_conversation_id = ?",
                ("2026-09-13T09:07:00+00:00", "conv_direct"),
            )
            connection.commit()

        continuation = self.tools.wechat_find_conversations("", limit=1, cursor=cursor)
        self.assertEqual(continuation["code"], "CURSOR_STALE")

    def test_catalog_stale_when_a_candidate_title_changes(self) -> None:
        first = self.tools.wechat_find_conversations("", limit=1)
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)

        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.execute(
                "UPDATE conversations SET title = ? WHERE source_conversation_id = ?",
                ("Demo Direct Renamed", "conv_direct"),
            )
            connection.commit()

        continuation = self.tools.wechat_find_conversations("", limit=1, cursor=cursor)
        self.assertEqual(continuation["code"], "CURSOR_STALE")

    def test_catalog_stale_when_a_candidate_kind_changes(self) -> None:
        first = self.tools.wechat_find_conversations("", limit=1)
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)

        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.execute(
                "UPDATE conversations SET kind = 'group' WHERE source_conversation_id = ?",
                ("conv_direct",),
            )
            connection.commit()

        continuation = self.tools.wechat_find_conversations("", limit=1, cursor=cursor)
        self.assertEqual(continuation["code"], "CURSOR_STALE")

    def test_inbox_stale_when_an_unseen_unread_conversation_becomes_read(self) -> None:
        self.service.sync_source_once(initial_tail=20, batch_limit=20)
        base = self.provider
        self.service.provider = _UnreadProvider(  # type: ignore[reportAttributeAccessIssue]
            base, {"conv_group": 2, "conv_direct": 1}
        )
        first = self.tools.wechat_read_inbox(unread_only=True, limit=1)
        self.assertEqual(len(first["items"]), 1)
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)

        # The unseen conversation is read without a new observation or watermark move.
        self.service.provider = _UnreadProvider(  # type: ignore[reportAttributeAccessIssue]
            base, {"conv_group": 2, "conv_direct": 0}
        )
        continuation = self.tools.wechat_read_inbox(
            unread_only=True, limit=1, cursor=cursor
        )
        self.assertEqual(continuation["code"], "CURSOR_STALE")

    def test_inbox_stale_when_a_projected_latest_text_changes(self) -> None:
        self.service.sync_source_once(initial_tail=20, batch_limit=20)
        first = self.tools.wechat_read_inbox(limit=1, include_latest="text")
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)

        latest = opaque_id("wxmsg", self.account_id, "source-msg-008")
        with closing(sqlite3.connect(self.window)) as connection:
            connection.execute(
                "UPDATE messages SET text = ? WHERE message_id = ?",
                ("corrected latest text", latest),
            )
            connection.commit()

        continuation = self.tools.wechat_read_inbox(
            limit=1, include_latest="text", cursor=cursor
        )
        self.assertEqual(continuation["code"], "CURSOR_STALE")

    def test_inbox_stale_when_a_projected_sender_label_changes(self) -> None:
        self.service.sync_source_once(initial_tail=20, batch_limit=20)
        first = self.tools.wechat_read_inbox(limit=1)
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)

        latest = opaque_id("wxmsg", self.account_id, "source-msg-008")
        row = self.repository.message_position_row(latest)
        self.assertIsNotNone(row)
        assert row is not None
        sender_id = row["sender_id"]
        self.assertIsNotNone(sender_id)
        with closing(sqlite3.connect(self.window)) as connection:
            connection.execute(
                "UPDATE participants SET current_reader_label = ? WHERE participant_id = ?",
                ("corrected label", sender_id),
            )
            connection.commit()

        continuation = self.tools.wechat_read_inbox(limit=1, cursor=cursor)
        self.assertEqual(continuation["code"], "CURSOR_STALE")

    def test_inbox_stale_when_an_eligible_conversation_becomes_invisible(self) -> None:
        self.service.sync_source_once(initial_tail=20, batch_limit=20)
        first = self.tools.wechat_read_inbox(limit=1)
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)

        # The unseen conversation leaves the visible set; dropping it from the catalog
        # keeps the continuation's catalog persist from reactivating it.
        with closing(sqlite3.connect(self.window)) as connection:
            connection.execute(
                "UPDATE conversations SET visibility_state = 'inactive' "
                "WHERE source_conversation_id = ?",
                ("conv_direct",),
            )
            connection.commit()
        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.execute(
                "DELETE FROM conversations WHERE source_conversation_id = ?",
                ("conv_direct",),
            )
            connection.commit()

        continuation = self.tools.wechat_read_inbox(limit=1, cursor=cursor)
        self.assertEqual(continuation["code"], "CURSOR_STALE")

    def _assert_writer_lock_free_during_source_read(self, operation: Any) -> Any:
        """Run ``operation`` on a worker thread parked inside a source read.

        While that read is blocked, an independent window.db write transaction must
        still complete; a writer lock held across the source read fails this check.
        """

        entered = threading.Event()
        release = threading.Event()
        self.service.provider = _GatedReadProvider(  # type: ignore[reportAttributeAccessIssue]
            self.provider, entered, release
        )
        operator_db = WindowDB(self.window)
        outcome: dict[str, Any] = {}

        def run_operation() -> None:
            try:
                outcome["result"] = operation()
            except BaseException as exc:
                outcome["error"] = exc
            finally:
                release.set()

        worker = threading.Thread(target=run_operation, name="sightglass-blocked-read")
        worker.start()
        try:
            self.assertTrue(entered.wait(5), "operation never reached the source read")
            started = time.monotonic()
            with operator_db.transaction() as connection:
                connection.execute(
                    "UPDATE reader_profiles SET policy_revision = policy_revision"
                )
            elapsed = time.monotonic() - started
        finally:
            release.set()
            worker.join(timeout=20)

        self.assertFalse(worker.is_alive())
        self.assertNotIn("error", outcome)
        self.assertLess(elapsed, 1.0)
        return outcome["result"]

    def test_sync_source_reads_do_not_hold_the_window_writer_lock(self) -> None:
        result = self._assert_writer_lock_free_during_source_read(
            lambda: self.service.sync_source_once(initial_tail=5, batch_limit=5)
        )

        self.assertEqual(result["conversation_count"], 2)

    def test_backfill_source_reads_do_not_hold_the_window_writer_lock(self) -> None:
        self.service.sync_source_once(initial_tail=20, batch_limit=20)
        group_id = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        self.service.queue_backfill(conversation_id=group_id, max_messages=20)

        result = self._assert_writer_lock_free_during_source_read(
            lambda: self.service.process_backfill_once(batch_limit=5)
        )

        self.assertIn(result["state"], {"running", "completed"})

    def test_backfill_step_reuses_queued_target_without_refreshing_full_catalog(
        self,
    ) -> None:
        self.service.sync_source_once(initial_tail=20, batch_limit=20)
        group_id = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        self.service.queue_backfill(conversation_id=group_id, max_messages=20)
        recorder = _CatalogCountingProvider(self.provider)
        self.service.provider = recorder  # type: ignore[reportAttributeAccessIssue]

        result = self.service.process_backfill_once(batch_limit=5)

        self.assertIn(result["state"], {"running", "completed"})
        self.assertEqual(recorder.list_conversations_calls, 0)

    def test_cold_reader_source_reads_do_not_hold_the_window_writer_lock(self) -> None:
        self.service.sync_source_once(initial_tail=20, batch_limit=20)
        group_id = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE source_conversation_state SET source_inventory_epoch = 'stale' "
                "WHERE conversation_id = ?",
                (group_id,),
            )

        result = self._assert_writer_lock_free_during_source_read(
            lambda: self.service.read_messages(
                mode="recent", conversation_id=group_id, projection="compact", limit=5
            )
        )

        self.assertEqual(result["schema"], "sightglass.message-batch.v1")

    def test_message_reads_do_not_repeat_a_full_roster_scan(self) -> None:
        self.service.sync_source_once(initial_tail=20, batch_limit=20)
        group_id = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]

        self.service.provider = _MessageEvidenceProvider(self.provider)  # type: ignore[reportAttributeAccessIssue]
        result = self.service.read_messages(
            mode="recent",
            conversation_id=group_id,
            projection="compact",
            limit=5,
        )
        updates = self.service.read_messages(
            mode="updates",
            conversation_id=group_id,
            projection="compact",
            limit=5,
        )

        self.assertEqual(result["schema"], "sightglass.message-batch.v1")
        self.assertEqual(updates["schema"], "sightglass.message-batch.v1")

    def test_sync_catalog_rotation_is_bounded_and_persisted(self) -> None:
        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.executemany(
                "INSERT INTO conversations VALUES (?, 'direct', ?, ?, 1)",
                (
                    (
                        f"rotation-{index}",
                        f"Rotation {index}",
                        "2026-09-12T08:00:00+00:00",
                    )
                    for index in range(4)
                ),
            )
            connection.commit()
        recorder = _RecordingReadProvider(self.provider)
        self.service.provider = recorder  # type: ignore[reportAttributeAccessIssue]

        reads: list[str] = []
        cursors: list[str] = []
        for _round in range(4):
            result = self.service.sync_source_once(
                initial_tail=5, batch_limit=5, conversation_limit=2
            )
            self.assertLessEqual(result["conversation_count"], 2)
            reads.extend(recorder.take())
            state = self.repository.source_catalog_state(self.account_id)
            if state is None or state["next_cursor_token"] is None:
                self.fail("sync did not persist a catalog rotation cursor")
            cursors.append(str(state["next_cursor_token"]))

        self.assertEqual(len(reads), 8)
        self.assertEqual(len(set(reads[:6])), 6)
        self.assertEqual(set(reads), set(reads[:6]))
        self.assertNotEqual(cursors[0], cursors[1])
        with self.repository.database.connection() as connection:
            covered = int(
                connection.execute(
                    "SELECT COUNT(*) FROM source_conversation_state"
                ).fetchone()[0]
            )
        self.assertEqual(covered, 6)

    def test_sync_prioritizes_catalog_activity_ahead_of_the_rotation_tail(self) -> None:
        third_source_id = "conv_third"
        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.execute(
                "INSERT INTO conversations VALUES (?, 'direct', ?, ?, 1)",
                (
                    third_source_id,
                    "Third Conversation",
                    "2026-09-13T09:02:00+00:00",
                ),
            )
            connection.executemany(
                "INSERT INTO memberships VALUES (?, ?, ?, NULL, ?)",
                (
                    (
                        third_source_id,
                        "wxid_demo_owner",
                        "third-self",
                        "2026-09-13T09:02:00+00:00",
                    ),
                    (
                        third_source_id,
                        "wxid_demo_member",
                        "third-demo_member",
                        "2026-09-13T09:02:00+00:00",
                    ),
                ),
            )
            connection.commit()
        self._append_message(
            message_id="source-msg-third-initial",
            conversation_id=third_source_id,
            sent_at="2026-09-13T09:02:00+00:00",
            rowid=800,
            text="third initial",
        )

        recorder = _RecordingReadProvider(self.provider)
        self.service.provider = recorder  # type: ignore[reportAttributeAccessIssue]
        self.service.sync_source_once(
            initial_tail=20, batch_limit=20, conversation_limit=20
        )
        recorder.take()

        ordered = self.repository.account_conversations(self.account_id)
        self.assertEqual(len(ordered), 3)
        target_source_id = str(ordered[-1]["source_conversation_id"])
        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.execute(
                "UPDATE conversations SET last_message_at_utc = ? "
                "WHERE source_conversation_id = ?",
                ("2026-09-13T10:00:00+00:00", target_source_id),
            )
            connection.commit()
        self._append_message(
            message_id="source-msg-priority-append",
            conversation_id=target_source_id,
            sent_at="2026-09-13T10:00:00+00:00",
            rowid=801,
            text="priority append",
        )

        result = self.service.sync_source_once(
            initial_tail=20, batch_limit=20, conversation_limit=2
        )
        selected = recorder.take()

        self.assertEqual(result["conversation_count"], 2)
        self.assertIn(target_source_id, selected)

    def test_sync_rolls_back_when_the_final_snapshot_validation_fails(self) -> None:
        failing = _FailingExitProvider(self.provider)
        self.service.provider = failing  # type: ignore[reportAttributeAccessIssue]

        with self.assertRaises(SightglassError) as caught:
            self.service.sync_source_once(initial_tail=5, batch_limit=5)

        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)
        with self.repository.database.connection() as connection:
            counts = {
                table: int(
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                )
                for table in ("accounts", "conversations", "messages", "source_catalog_state")
            }
        self.assertEqual(
            counts,
            {
                "accounts": 0,
                "conversations": 0,
                "messages": 0,
                "source_catalog_state": 0,
            },
        )

    def test_catalog_rotation_cursor_survives_a_service_restart(self) -> None:
        recorder = _RecordingReadProvider(self.provider)
        self.service.provider = recorder  # type: ignore[reportAttributeAccessIssue]
        first_round_result = self.service.sync_source_once(
            initial_tail=5, batch_limit=5, conversation_limit=1
        )
        first_round = recorder.take()

        self.assertEqual(first_round_result["conversation_count"], 1)
        self.assertEqual(len(first_round), 1)

        # Restart: a fresh WindowDB and ReaderService over the same private state.
        restarted_provider, _repository, restarted, _tools = build_test_stack(
            self.root,
            self.window,
            policy=ReaderPolicy(mode="all_except_denylist", identity_debug=True),
        )
        restarted_recorder = _RecordingReadProvider(restarted_provider)
        restarted.provider = restarted_recorder  # type: ignore[reportAttributeAccessIssue]
        second_round_result = restarted.sync_source_once(
            initial_tail=5, batch_limit=5, conversation_limit=1
        )
        second_round = restarted_recorder.take()

        self.assertEqual(second_round_result["conversation_count"], 1)
        self.assertEqual(len(second_round), 1)
        self.assertEqual(set(second_round) & set(first_round), set())
        self.assertEqual(len(set(first_round) | set(second_round)), 2)

    def test_unread_backlog_does_not_starve_the_catalog_rotation(self) -> None:
        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.executemany(
                "INSERT INTO conversations VALUES (?, 'direct', ?, ?, 1)",
                (
                    (
                        f"rotation-{index}",
                        f"Rotation {index}",
                        "2026-09-12T08:00:00+00:00",
                    )
                    for index in range(2)
                ),
            )
            connection.commit()
        recorder = _RecordingReadProvider(
            _UnreadProvider(self.provider, {"conv_group": 3, "conv_direct": 2})
        )
        self.service.provider = recorder  # type: ignore[reportAttributeAccessIssue]

        reads: list[str] = []
        for _round in range(2):
            result = self.service.sync_source_once(
                initial_tail=5, batch_limit=5, conversation_limit=2
            )
            self.assertEqual(result["conversation_count"], 2)
            reads.extend(recorder.take())

        self.assertEqual(len(reads), 4)
        self.assertEqual(len(set(reads)), 4)

    def test_policy_change_invalidates_the_inbox_cursor(self) -> None:
        self.service.sync_source_once(initial_tail=20, batch_limit=20)
        page = self.tools.wechat_read_inbox(limit=1)
        cursor = page["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)

        denied_id = opaque_id("wxconv", self.account_id, "conv_direct")
        _provider, _repository, _restricted, restricted_tools = build_test_stack(
            self.root,
            self.window,
            policy=ReaderPolicy(
                mode="all_except_denylist",
                denied_conversation_ids=frozenset({denied_id}),
                identity_debug=True,
            ),
        )
        continuation = restricted_tools.wechat_read_inbox(limit=1, cursor=cursor)

        self.assertEqual(continuation["code"], "CURSOR_INVALID")

    def test_backfill_rolls_back_job_resume_and_messages_when_validation_fails(
        self,
    ) -> None:
        for index in range(40):
            self._append_message(
                message_id=f"source-msg-backfill-history-{index:03d}",
                conversation_id="conv_group",
                sent_at=f"2026-09-12T11:{index // 60:02d}:{index % 60:02d}+00:00",
                rowid=3_000 + index,
                text=f"backfill history {index:03d}",
            )
        self.service.sync_source_once(initial_tail=10, batch_limit=10)
        group_id = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        self.service.queue_backfill(conversation_id=group_id, max_messages=50)
        with self.repository.database.connection() as connection:
            messages_before = int(
                connection.execute(
                    "SELECT COUNT(*) FROM messages WHERE conversation_id = ?",
                    (group_id,),
                ).fetchone()[0]
            )

        failing = _FailingExitProvider(self.provider)
        self.service.provider = failing  # type: ignore[reportAttributeAccessIssue]
        with self.assertRaises(SightglassError) as caught:
            self.service.process_backfill_once(batch_limit=10)

        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)
        job = self.repository.next_backfill_job()
        self.assertIsNotNone(job)
        assert job is not None
        self.assertEqual(str(job["state"]), "queued")
        self.assertEqual(int(job["processed_messages"]), 0)
        with self.repository.database.connection() as connection:
            messages_after = int(
                connection.execute(
                    "SELECT COUNT(*) FROM messages WHERE conversation_id = ?",
                    (group_id,),
                ).fetchone()[0]
            )
        self.assertEqual(messages_after, messages_before)

    def test_denylist_blocks_discovery_inbox_search_and_direct_reads(self) -> None:
        denied_id = opaque_id("wxconv", self.account_id, "conv_direct")
        _provider, _repository, service, tools = build_test_stack(
            self.root,
            Path(self.temp.name) / "denied" / "window.db",
            policy=ReaderPolicy(
                mode="all_except_denylist",
                denied_conversation_ids=frozenset({denied_id}),
                identity_debug=True,
            ),
        )
        service.sync_source_once(initial_tail=20, batch_limit=20)

        discovered = tools.wechat_find_conversations("")
        inbox = tools.wechat_read_inbox(limit=20)
        searched = tools.wechat_search_messages(query="私聊", limit=20)
        direct = tools.wechat_read_messages(
            mode="recent", conversation_id=denied_id, limit=1
        )

        self.assertNotIn(
            denied_id,
            {item["conversation_id"] for item in discovered["candidates"]},
        )
        self.assertNotIn(denied_id, {item["conversation_id"] for item in inbox["items"]})
        self.assertEqual(searched["hits"], [])
        self.assertEqual(direct["code"], "POLICY_DENIED")

    def test_account_scope_automatically_admits_a_new_conversation(self) -> None:
        self.service.sync_source_once(initial_tail=20, batch_limit=20)
        with closing(sqlite3.connect(self.root / "catalog.db")) as connection:
            connection.execute(
                "INSERT INTO conversations VALUES (?, 'direct', ?, ?, 1)",
                (
                    "conv_new",
                    "New Conversation",
                    "2026-09-13T09:20:00+00:00",
                ),
            )
            connection.commit()
        self._append_message(
            message_id="source-msg-new-conversation",
            conversation_id="conv_new",
            sent_at="2026-09-13T09:20:00+00:00",
            rowid=101,
            text="new conversation message",
        )

        self.service.sync_source_once(initial_tail=20, batch_limit=20)
        result = self.tools.wechat_find_conversations("New Conversation", limit=10)

        self.assertEqual(len(result["candidates"]), 1)
        new_id = result["candidates"][0]["conversation_id"]
        inbox = self.tools.wechat_read_inbox(limit=20)
        self.assertIn(new_id, {item["conversation_id"] for item in inbox["items"]})

    def test_account_search_does_not_synchronously_full_hydrate_conversations(self) -> None:
        for index in range(220):
            self._append_message(
                message_id=f"source-msg-search-history-{index:03d}",
                conversation_id="conv_group",
                sent_at=f"2026-09-11T10:{index // 60:02d}:{index % 60:02d}+00:00",
                rowid=20_000 + index,
                text=f"search history {index:03d}",
            )
        with mock.patch.object(
            self.service,
            "_read_hydrate_page",
            side_effect=AssertionError("full hydrate must not run"),
        ):
            result = self.tools.wechat_search_messages(query="私聊", limit=20)

        self.assertGreaterEqual(len(result["hits"]), 1)
        self.assertEqual(result["source_receipt"]["coverage"]["conversation"], "indexed")
        self.assertIn("history_not_fully_indexed", result["source_receipt"]["warnings"])

    def test_link_description_host_and_path_are_searchable_without_changing_visible_text(
        self,
    ) -> None:
        raw_link = """
        <msg><appmsg>
          <type>5</type>
          <title>Visible title</title>
          <des>Hidden discovery phrase</des>
          <sourcedisplayname>Example Publisher</sourcedisplayname>
          <url>https://docs.example.test/private/guide?secret=query#fragment</url>
        </appmsg></msg>
        """.strip()
        with closing(sqlite3.connect(self.root / "messages-1.db")) as connection:
            connection.execute(
                """
                INSERT INTO messages(
                    source_message_id, source_conversation_id, source_time_raw,
                    sent_at_utc, observed_at_utc, sort_seq, source_rowid,
                    wechat_type, raw_content, is_outgoing, sender_internal_id,
                    sender_local_token, sender_surface_label, resources_json
                ) VALUES (?, 'conv_direct', ?, ?, ?, ?, ?, 49, ?, 0,
                          'wxid_demo_member', NULL, '示例甲', '[]')
                """,
                (
                    "source-msg-link-search",
                    "2026-09-13T09:40:00+00:00",
                    "2026-09-13T09:40:00+00:00",
                    "2026-09-13T09:40:00+00:00",
                    5_100,
                    5_100,
                    raw_link,
                ),
            )
            connection.commit()

        description = self.tools.wechat_search_messages(
            query="discovery phrase", limit=20
        )
        host = self.tools.wechat_search_messages(query="docs.example.test", limit=20)
        path = self.tools.wechat_search_messages(query="private/guide", limit=20)
        query_secret = self.tools.wechat_search_messages(query="secret", limit=20)

        self.assertEqual(len(description["hits"]), 1)
        self.assertEqual(len(host["hits"]), 1)
        self.assertEqual(len(path["hits"]), 1)
        self.assertEqual(query_secret["hits"], [])
        self.assertEqual(description["hits"][0][4], "[链接] Visible title")
        self.assertIn(
            "link.description", description["markers"]["matches"]["0"]
        )

    def test_sync_persists_account_binding_and_source_shard_generation_state(self) -> None:
        self.service.sync_source_once(initial_tail=20, batch_limit=20)
        with self.provider.snapshot() as snapshot:
            expected_shards = dict(snapshot.generation_by_shard)
        with self.repository.database.connection() as connection:
            account = connection.execute(
                "SELECT account_binding_id, source_inventory_epoch FROM accounts"
            ).fetchone()
            shards = connection.execute(
                """
                SELECT source_shard_key, source_generation_id, availability_state
                FROM source_shard_state
                """
            ).fetchall()

        self.assertIsNone(account["account_binding_id"])
        self.assertIsNotNone(account["source_inventory_epoch"])
        self.assertEqual(
            {str(row["source_shard_key"]): str(row["source_generation_id"]) for row in shards},
            expected_shards,
        )
        self.assertTrue(all(row["availability_state"] == "available" for row in shards))

    def test_historical_backfill_is_bounded_resumable_and_tail_first(self) -> None:
        for index in range(120):
            self._append_message(
                message_id=f"source-msg-history-{index:03d}",
                conversation_id="conv_group",
                sent_at=f"2026-09-12T10:{index // 60:02d}:{index % 60:02d}+00:00",
                rowid=1_000 + index,
                text=f"history {index:03d}",
            )
        self.service.sync_source_once(initial_tail=10, batch_limit=10)
        group_id = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        self._append_message(
            message_id="source-msg-live-during-backfill",
            conversation_id="conv_direct",
            sent_at="2026-09-13T09:30:00+00:00",
            rowid=5_000,
            text="live during backfill",
        )
        tail = self.service.sync_source_once(initial_tail=10, batch_limit=10)
        self.assertGreaterEqual(tail["message_count"], 1)
        # Historical backfill into resident storage is an explicit, scoped
        # selection (SPEC 8.7): prospective ``keep`` alone would not collect the
        # old history this test asserts on.
        from sightglass.residency.repository import ResidencyRepository

        ResidencyRepository(self.repository.database).set(
            group_id, mode="keep", keep_backfill=True
        )
        queued = self.service.queue_backfill(
            conversation_id=group_id,
            max_messages=500,
        )
        self.assertEqual(queued["queued_job_count"], 1)

        for _attempt in range(20):
            result = self.service.process_backfill_once(batch_limit=15)
            if result["state"] in {"idle", "completed"}:
                status = self.service.backfill_status()
                if status["state_counts"].get("completed") == 1:
                    break
        else:
            self.fail("backfill did not complete")

        with self.repository.database.connection() as connection:
            count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM messages WHERE conversation_id = ?",
                    (group_id,),
                ).fetchone()[0]
            )
        self.assertGreaterEqual(count, 120)
        self.assertEqual(status["state_counts"].get("completed"), 1)


if __name__ == "__main__":
    unittest.main()

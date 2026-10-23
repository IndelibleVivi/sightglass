"""Synthetic SQLite fixtures exercising real native bounded identity admission."""

from __future__ import annotations

import sqlite3
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from sightglass.contracts.capture import CaptureCeiling, CaptureRequest
from sightglass.contracts.common import utc_now
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.identity import SourceConversation
from sightglass.source.base import SourceScope, SourceSnapshot
from sightglass.source.capture.executor import CaptureExecutor
from sightglass.source.macos_wechat.provider import MacOSWeChatSourceProvider, _SnapshotState


class NativeIdentityWindowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.provider: Any = object.__new__(MacOSWeChatSourceProvider)
        self.account = "synthetic-window-account"
        self.conversation = "synthetic-window-conversation"
        self.provider.settings = SimpleNamespace(
            source_account_key=self.account, source_account_binding_id="synthetic-window-binding"
        )
        self.shards = ("message/message_0.db", "message/message_1.db")
        generations = {name: "synthetic-generation-" + str(i)
                       for i, name in enumerate(self.shards)}
        self.state = _SnapshotState(
            (), generations, contacts={},
            conversations={self.conversation: SourceConversation(
                self.conversation, "direct", "Synthetic window fixture"
            )}, scope=SourceScope.conversation(self.account, self.conversation),
        )
        self.snapshot = SourceSnapshot(
            "synthetic-inventory", "synthetic-generations", utc_now().isoformat(),
            tuple(generations.items()), "synthetic-token", scope=self.state.scope,
        )
        self.connections = {name: sqlite3.connect(":memory:") for name in self.shards}
        self.table = self.provider._table_name(self.conversation)
        for connection in self.connections.values():
            connection.row_factory = sqlite3.Row
            connection.execute(f'''CREATE TABLE [{self.table}](
                local_id INTEGER, server_id INTEGER, local_type INTEGER, sort_seq INTEGER,
                create_time INTEGER, status INTEGER, message_content BLOB,
                WCDB_CT_message_content INTEGER, packed_info_data BLOB)''')
            connection.execute(f"CREATE INDEX synthetic_server ON [{self.table}](server_id)")
        self.add(0, 10, 100, "synthetic newest")
        self.add(0, 20, 90, "synthetic sentinel")
        self.add(0, 30, 80, "synthetic oldest")

        @contextmanager
        def connect(relative):
            yield self.connections[relative]

        @contextmanager
        def session(_scope):
            yield self.snapshot

        def dependencies(_state, snapshot, selected):
            snapshot.dependency_generation_by_shard.update(
                {relative: generations[relative] for relative in selected}
            )

        self.provider._connect = connect
        self.provider._assert_snapshot = lambda _snapshot: self.state
        self.provider._message_shards = lambda _state, _table: self.shards
        self.provider._record_message_dependencies = dependencies
        self.provider._real_sender_expression = lambda *_args, **_kwargs: "NULL"
        self.provider._raw_sender_expression = lambda *_args, **_kwargs: "NULL"
        self.provider._resource_resolver = SimpleNamespace(resources_for_message=lambda **_kw: ())
        self.provider.session = session

    def tearDown(self) -> None:
        for connection in self.connections.values():
            connection.close()

    def add(self, shard, identity, position, text):
        self.connections[self.shards[shard]].execute(
            f"INSERT INTO [{self.table}] VALUES (?, ?, 1, ?, ?, 0, ?, 0, NULL)",
            (identity, identity, position, 1_725_000_000 + position, text),
        )

    def recent(self):
        return self.provider.read_recent(self.account, self.conversation, 1, self.snapshot)

    def range(self, direction="backward", **kwargs):
        return self.provider.read_range(
            self.account, self.conversation, after=None, before=None,
            direction=direction, limit=1, snapshot=self.snapshot, **kwargs,
        )

    def assert_conflict(self, call):
        with self.assertRaises(SightglassError) as caught:
            call()
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)
        self.assertEqual(caught.exception.details["warning_codes"],
                         ["duplicate_message_identity_conflict"])

    def test_recent_and_range_reject_far_retained_conflict(self):
        self.add(1, 10, 10, "synthetic conflicting old copy")
        self.assert_conflict(self.recent)
        self.assert_conflict(self.range)
        identity = self.provider._message_token(
            self.conversation, {"server_id": 10}, self.shards[0]
        )
        self.assert_conflict(lambda: self.provider.get_message(
            self.account, identity, self.snapshot
        ))

    def test_recent_and_range_reject_far_sentinel_conflict(self):
        self.add(1, 20, 10, "synthetic conflicting sentinel")
        self.assert_conflict(self.recent)
        self.assert_conflict(self.range)

    def test_forward_and_time_filtered_windows_reject_far_conflict(self):
        self.add(1, 30, 200, "synthetic conflicting future copy")
        self.assert_conflict(lambda: self.range("forward"))
        self.add(1, 10, 10, "synthetic conflicting past copy")
        self.assert_conflict(lambda: self.range(
            time_after_utc="2024-08-30T06:41:20+00:00"
        ))

    def test_preparation_rejects_far_sentinel_conflict(self):
        self.add(1, 20, 10, "synthetic conflicting sentinel")
        self.assert_conflict(lambda: list(self.provider.prepare_search_page(
            self.account, self.conversation, direction="backward", limit=1,
            snapshot=self.snapshot,
        )))

    def test_context_rejects_far_sentinel_conflict(self):
        self.add(1, 30, 10, "synthetic conflicting context sentinel")
        identity = self.provider._message_token(
            self.conversation, {"server_id": 10}, self.shards[0]
        )
        focus = self.provider.get_message(self.account, identity, self.snapshot)
        self.assert_conflict(lambda: self.provider.read_context(
            self.account, self.conversation, focus=focus, before=1, after=0,
            snapshot=self.snapshot,
        ))

    def test_capture_never_seals_conflicted_recent_as_complete(self):
        self.add(1, 10, 10, "synthetic conflicting old copy")
        executor = CaptureExecutor(
            self.provider,
            CaptureCeiling(self.account, frozenset({self.conversation}), "synthetic-egress"),
            source_instance_id="synthetic-origin",
        )
        document = executor.capture(CaptureRequest(
            "synthetic-request", "recent", self.account, "synthetic-policy",
            conversation_source_id=self.conversation, limit=1,
        ), stream_epoch="synthetic-stream", sequence=1).document()
        self.assertNotEqual(document.receipt.terminal, "complete")
        self.assertEqual(len(document.evidence.messages), 0)
        self.assertEqual(document.receipt.coverage.kind, "none")

    def test_identity_queries_are_batched_and_do_not_hydrate_history(self):
        for identity in range(1000, 2000):
            self.add(1, identity, identity - 5000, "synthetic historical row")
        with (
            patch.object(self.provider, "_server_identity_rows",
                         wraps=self.provider._server_identity_rows) as queries,
            patch.object(self.provider, "_message", wraps=self.provider._message) as messages,
        ):
            page = self.recent()
        self.assertEqual(len(page.messages), 1)
        identity_calls = [call for call in queries.call_args_list if call.args[3]]
        self.assertEqual(len(identity_calls), 1)
        self.assertTrue(all(call.args[3] == (10, 20) for call in identity_calls))
        self.assertLessEqual(messages.call_count, 6)
        self.assertEqual(set(self.snapshot.dependency_generation_by_shard), set(self.shards))

    def test_equivalent_cross_shard_copy_and_pagination_stay_stable(self):
        self.add(1, 10, 100, "synthetic newest")  # Same physical rowid/evidence.
        page = self.recent()
        message = page.messages[0]
        canonical = self.provider.get_message(
            self.account, message.source_message_id, self.snapshot
        )
        self.assertEqual(message.source_message_id, canonical.source_message_id)
        self.assertEqual(message.source_rowid, canonical.source_rowid)
        older = self.provider.read_range(
            self.account, self.conversation, after=None, before=message.sort_key,
            direction="backward", limit=1, snapshot=self.snapshot,
        )
        self.assertEqual([row.raw_content for row in older.messages], ["synthetic sentinel"])
        self.assertTrue(older.has_more_before)
        context = self.provider.read_context(
            self.account, self.conversation, focus=canonical, before=1, after=0,
            snapshot=self.snapshot,
        )
        self.assertEqual([row.raw_content for row in context.messages],
                         ["synthetic sentinel", "synthetic newest"])
        steps = list(self.provider.prepare_search_page(
            self.account, self.conversation, direction="backward", limit=1,
            snapshot=self.snapshot,
        ))
        self.assertEqual(steps[-1].page.messages[0].source_message_id, message.source_message_id)

    def test_cross_shard_rowid_evidence_is_not_relaxed(self):
        self.add(1, 99, 10, "synthetic physical prefix")
        self.add(1, 10, 100, "synthetic newest")
        # Same content/time but a different physical rowid is still a conflict.
        self.assert_conflict(self.recent)
        identity = self.provider._message_token(
            self.conversation, {"server_id": 10}, self.shards[0]
        )
        self.assert_conflict(lambda: self.provider.get_message(
            self.account, identity, self.snapshot
        ))

    def test_high_word_text_does_not_reuse_plain_text_proof(self):
        connection = self.connections[self.shards[0]]
        connection.execute(f"UPDATE [{self.table}] SET local_type=? WHERE server_id=10",
                           ((1 << 32) | 1,))
        self.add(0, 10, 10, "synthetic conflicting flagged text")
        connection.execute(f"UPDATE [{self.table}] SET local_type=? WHERE server_id=10",
                           ((1 << 32) | 1,))
        self.assert_conflict(self.recent)

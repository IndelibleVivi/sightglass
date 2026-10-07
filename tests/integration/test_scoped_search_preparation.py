from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest import mock

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.source.identity import opaque_id
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack
from tests.integration import test_native_source_provider as native


class ScopedSearchPreparationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "source"
        create_synthetic_source(self.root)
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            self.root, Path(self.temporary.name) / "state" / "window.db"
        )
        self.tools.wechat_status()
        self.group = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _append(self, conversation: str, count: int, *, prefix: str, start: datetime) -> None:
        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            connection.executemany(
                """INSERT INTO messages(
                    source_message_id, source_conversation_id, source_time_raw,
                    sent_at_utc, observed_at_utc, sort_seq, source_rowid, wechat_type,
                    raw_content, is_outgoing, sender_internal_id, sender_surface_label,
                    resources_json
                ) VALUES (?,?,?,?,?,?,?,1,?,0,'wxid_demo_member','Synthetic Sender','[]')""",
                [
                    (
                        f"synthetic-{conversation}-{prefix}-{index}",
                        conversation,
                        (start + timedelta(seconds=index)).isoformat(timespec="microseconds"),
                        (start + timedelta(seconds=index)).isoformat(timespec="microseconds"),
                        "2026-10-02T00:00:00+00:00",
                        10_000 + index,
                        10_000 + index,
                        f"wxid_demo_member:\nSynthetic {prefix} {index}",
                    )
                    for index in range(count)
                ],
            )
            connection.commit()
        manifest = self.root / "source.json"
        data = json.loads(manifest.read_text(encoding="utf-8"))
        data["shards"][1]["generation_id"] += f"-{prefix}"
        manifest.write_text(json.dumps(data), encoding="utf-8")

    def _reader_positions(self) -> tuple[list[tuple[Any, ...]], ...]:
        with self.repository.database.connection() as connection:
            return tuple(
                list(map(tuple, connection.execute(f"SELECT * FROM {table}")))
                for table in (
                    "reader_timeline_cursors",
                    "reader_update_cursors",
                    "reader_deliveries",
                )
            )

    def test_first_time_scoped_page_admits_old_gap_without_unrelated_reads(self) -> None:
        self._append("conv_group", 220, prefix="later", start=datetime(2026, 10, 1, tzinfo=UTC))
        self._append(
            "conv_group", 48, prefix="historicalgap", start=datetime(2026, 9, 12, tzinfo=UTC)
        )
        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            connection.executemany(
                "UPDATE messages SET raw_content=? WHERE source_message_id=?",
                [
                    (
                        f"wxid_demo_member:\nSynthetic requestedneedle {index}",
                        f"synthetic-conv_group-historicalgap-{index}",
                    )
                    for index in (42, 47)
                ],
            )
            connection.commit()
        self.service.read_messages(
            mode="recent",
            conversation_id=self.group,
            refresh=True,
            projection="compact",
            limit=200,
            voice="off",
        )
        positions = self._reader_positions()
        real_range = self.provider.read_range
        calls = []

        def scoped_range(account: str, conversation: str, **kwargs: Any) -> Any:
            self.assertEqual(conversation, "conv_group")
            self.assertEqual(kwargs["time_after_utc"], "2026-09-12T00:00:00.000000+00:00")
            self.assertEqual(kwargs["time_before_utc"], "2026-09-12T00:01:00.000000+00:00")
            calls.append(kwargs)
            return real_range(account, conversation, **kwargs)

        with (
            mock.patch.object(self.provider, "read_range", side_effect=scoped_range),
            mock.patch.object(
                self.provider, "read_recent", side_effect=AssertionError("range scope")
            ),
            mock.patch.object(
                self.provider, "list_participants", side_effect=AssertionError("no roster")
            ),
        ):
            page = self.tools.wechat_search_messages(
                query="requestedneedle",
                conversation_ids=[self.group],
                after="2026-09-12T00:00:00+00:00",
                before="2026-09-12T00:01:00+00:00",
                limit=10,
            )
        self.assertEqual(
            [row[4] for row in page["hits"]],
            ["Synthetic requestedneedle 42", "Synthetic requestedneedle 47"],
        )
        self.assertEqual(len(calls), 1)
        self.assertTrue(page["source_receipt"]["search"]["canonical_validated"])
        self.assertFalse(page["source_receipt"]["complete"])
        self.assertEqual(self._reader_positions(), positions)

    def test_unrelated_legacy_deadline_cannot_block_a_scoped_first_page(self) -> None:
        self.service.sync_source_once(initial_tail=200)
        with self.repository.database.transaction() as connection:
            connection.execute(
                """UPDATE source_conversation_state SET coverage_version=0
                WHERE conversation_id IN (
                    SELECT conversation_id FROM conversations
                    WHERE source_conversation_id='conv_direct'
                )"""
            )
        real_range, real_recent = self.provider.read_range, self.provider.read_recent

        def range_read(account: str, conversation: str, **kwargs: Any) -> Any:
            if conversation == "conv_direct":
                raise SightglassError(
                    ErrorCode.SERVICE_TIMEOUT, details={"reason": "operation_deadline"}
                )
            return real_range(account, conversation, **kwargs)

        def recent_read(account: str, conversation: str, *args: Any) -> Any:
            self.assertEqual(conversation, "conv_group")
            return real_recent(account, conversation, *args)

        with (
            mock.patch.object(self.provider, "read_range", side_effect=range_read),
            mock.patch.object(self.provider, "read_recent", side_effect=recent_read),
        ):
            page = self.tools.wechat_search_messages(query="保留", conversation_ids=[self.group])
        self.assertEqual(len(page["hits"]), 1)

    def test_continuation_and_invalid_cursor_do_not_prepare_source_content(self) -> None:
        first = self.tools.wechat_search_messages(
            query="消息", conversation_ids=[self.group], limit=1
        )
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)
        with (
            mock.patch.object(
                self.provider, "read_recent", side_effect=AssertionError("no resync")
            ),
            mock.patch.object(self.provider, "read_range", side_effect=AssertionError("no resync")),
            mock.patch.object(
                self.provider, "get_message", wraps=self.provider.get_message
            ) as point,
        ):
            second = self.tools.wechat_search_messages(
                query="消息", conversation_ids=[self.group], cursor=cursor, limit=1
            )
            invalid = self.tools.wechat_search_messages(
                query="消息", conversation_ids=[self.group], cursor=cursor + "x", limit=1
            )
        self.assertEqual(len(second["hits"]), 1)
        self.assertNotEqual(first["hits"][0][0], second["hits"][0][0])
        self.assertGreater(point.call_count, 0)
        self.assertEqual(invalid["code"], "CURSOR_INVALID")

    def test_malformed_sender_cursor_is_rejected_before_roster_or_page_reads(self) -> None:
        with (
            mock.patch.object(self.provider, "read_recent", side_effect=AssertionError("no page")),
            mock.patch.object(self.provider, "read_range", side_effect=AssertionError("no page")),
            mock.patch.object(
                self.provider, "list_participants", side_effect=AssertionError("no roster")
            ),
        ):
            invalid = self.tools.wechat_search_messages(
                query="消息",
                conversation_ids=[self.group],
                sender_query="demo_member_old",
                cursor="synthetic-invalid-signed-cursor",
                limit=1,
            )
        self.assertEqual(invalid["code"], "CURSOR_INVALID")

    def test_signed_sender_scope_mismatch_is_rejected_before_roster_or_page_reads(self) -> None:
        first = self.tools.wechat_search_messages(
            query="消息", conversation_ids=[self.group], sender_query="demo_member_old", limit=1
        )
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)
        with (
            mock.patch.object(self.provider, "read_recent", side_effect=AssertionError("no page")),
            mock.patch.object(self.provider, "read_range", side_effect=AssertionError("no page")),
            mock.patch.object(
                self.provider, "list_participants", side_effect=AssertionError("no roster")
            ),
        ):
            invalid = self.tools.wechat_search_messages(
                query="changed literal scope",
                conversation_ids=[self.group],
                sender_query="demo_member_old",
                cursor=cursor,
                limit=1,
            )
        self.assertEqual(invalid["code"], "CURSOR_INVALID")

    def test_source_preparation_failure_stays_explicit_and_preserves_reader_state(self) -> None:
        positions = self._reader_positions()
        with mock.patch.object(
            self.provider, "read_range", side_effect=SightglassError(ErrorCode.SOURCE_INCOMPLETE)
        ):
            error = self.tools.wechat_search_messages(
                query="消息",
                conversation_ids=[self.group],
                after="2026-09-13T09:00:00+00:00",
                before="2026-09-13T10:00:00+00:00",
            )
        self.assertEqual(error["code"], "SOURCE_INCOMPLETE")
        self.assertEqual(self._reader_positions(), positions)

    def test_changed_authority_before_preparation_commit_cannot_admit_capture(self) -> None:
        account = self.repository.account_id_for("synthetic-account-demo")
        policy = self.service.reader.policy
        real_range = self.provider.read_range
        positions = self._reader_positions()
        for change, code in (("deny", "POLICY_DENIED"), ("pause", "SERVICE_PAUSED")):
            with self.subTest(change=change):
                self.service.reader.policy = policy
                self.service.reader.paused = False
                prefix = f"authoritybarrier-{change}"
                self._append(
                    "conv_group", 1, prefix=prefix, start=datetime(2026, 10, 1, tzinfo=UTC)
                )
                message_id = opaque_id("wxmsg", account, f"synthetic-conv_group-{prefix}-0")

                def read_then_change(account: str, conversation: str, **kwargs: Any) -> Any:
                    page = real_range(account, conversation, **kwargs)
                    if change == "deny":
                        self.service.reader.policy = replace(
                            policy, denied_conversation_ids=frozenset({self.group})
                        )
                    else:
                        self.service.reader.paused = True
                    return page

                with mock.patch.object(self.provider, "read_range", side_effect=read_then_change):
                    error = self.tools.wechat_search_messages(
                        query="authoritybarrier",
                        conversation_ids=[self.group],
                        after="2026-10-01T00:00:00+00:00",
                        before="2026-10-01T00:01:00+00:00",
                    )
                self.assertEqual(error["code"], code)
                self.assertIsNone(self.repository.message_position_row(message_id))
                self.assertEqual(self._reader_positions(), positions)
        self.service.reader.policy = policy
        self.service.reader.paused = False

    def test_account_preparation_has_one_total_budget_and_no_roster_scan(self) -> None:
        for conversation in ("conv_group", "conv_direct"):
            self._append(
                conversation, 220, prefix="budget", start=datetime(2026, 10, 1, tzinfo=UTC)
            )
        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            connection.execute(
                "UPDATE messages SET raw_content=? WHERE source_message_id=?",
                ("wxid_demo_member:\nSynthetic coldrecentneedle", "synthetic-conv_group-budget-30"),
            )
            connection.commit()
        requested, returned = [], []
        real = self.provider.read_recent

        def recent(account: str, conversation: str, limit: int, snapshot: Any) -> Any:
            page = real(account, conversation, limit, snapshot)
            requested.append(limit)
            returned.append(len(page.messages))
            return page

        with (
            mock.patch.object(self.provider, "read_recent", side_effect=recent),
            mock.patch.object(
                self.provider, "list_participants", side_effect=AssertionError("no roster")
            ),
        ):
            page = self.tools.wechat_search_messages(query="Synthetic", limit=50)
        self.assertEqual(len(requested), 2)
        self.assertLessEqual(sum(requested), 200)
        self.assertLessEqual(sum(returned), 200)
        self.assertTrue(page["hits"])
        self.assertFalse(page["source_receipt"]["complete"])

        cold = self.tools.wechat_search_messages(
            query="coldrecentneedle", conversation_ids=[self.group], limit=10
        )
        self.assertEqual([row[4] for row in cold["hits"]], ["Synthetic coldrecentneedle"])
        self.assertEqual(cold["source_receipt"]["search"]["preparation"]["message_count"], 200)

    def test_unsupported_old_generation_cannot_certify_complete_coverage(self) -> None:
        self.service.sync_source_once(initial_tail=200)
        self._append("conv_group", 1, prefix="newneedle", start=datetime(2026, 10, 1, tzinfo=UTC))
        page = self.tools.wechat_search_messages(
            query="newneedle",
            conversation_ids=[self.group],
            after="2026-10-01T00:00:00+00:00",
            before="2026-10-01T00:01:00+00:00",
        )
        self.assertEqual(len(page["hits"]), 1)
        self.assertFalse(page["source_receipt"]["complete"])
        self.assertEqual(page["source_receipt"]["coverage"]["conversation"], "indexed")
        self.assertEqual(page["source_receipt"]["search"]["history_complete_conversation_count"], 0)


class NativeScopedSearchPreparationTests(unittest.TestCase):
    def test_historical_preparation_reuses_selected_conversation_session(self) -> None:
        fixture = native.NativeSourceProviderTests(
            "test_projection_epoch_stales_timeline_and_search_cursors"
        )
        fixture.setUp()
        try:
            instant = 1_725_000_042
            fixture._insert_message(
                local_id=42,
                server_id=142,
                sort_seq=42,
                create_time=instant,
                content="Synthetic historical search needle",
                status=2,
            )
            fixture._insert_message(
                local_id=43,
                server_id=143,
                sort_seq=43,
                create_time=instant + 100,
                content="Synthetic newer tail",
                status=2,
            )
            tools = fixture._reader_tools()
            conversation = fixture._direct_conversation_id()
            tools.wechat_read_messages(
                mode="recent",
                conversation_id=conversation,
                refresh=True,
                projection="compact",
                limit=1,
                voice="off",
            )
            real_range = fixture.provider.read_range
            scopes = []

            def read_range(account: str, target: str, **kwargs: Any) -> Any:
                scope = kwargs["snapshot"].scope
                self.assertIsNotNone(scope)
                assert scope is not None
                self.assertEqual(scope.kind, "conversation")
                self.assertEqual(scope.conversation_source_id, fixture.conversation)
                scopes.append(scope)
                return real_range(account, target, **kwargs)

            with mock.patch.object(fixture.provider, "read_range", side_effect=read_range):
                page = tools.wechat_search_messages(
                    query="search needle",
                    conversation_ids=[conversation],
                    after=datetime.fromtimestamp(instant - 1, UTC).isoformat(),
                    before=datetime.fromtimestamp(instant + 1, UTC).isoformat(),
                    limit=50,
                )
            self.assertEqual(
                [row[4] for row in page["hits"]], ["Synthetic historical search needle"]
            )
            self.assertEqual(len(scopes), 1)
        finally:
            fixture.tearDown()

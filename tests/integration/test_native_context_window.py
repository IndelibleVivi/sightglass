"""Exact native context selection without a native timeline index."""

from __future__ import annotations

import unittest
from contextlib import contextmanager
from dataclasses import replace
from typing import Any
from unittest.mock import PropertyMock, patch

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.operations import operation_expired
from sightglass.source.base import SourceScope
from tests.integration import test_native_source_provider as native_fixture
from tests.integration.test_reading_correctness_sequences import _ReadingFixture


class NativeContextWindowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = native_fixture.NativeSourceProviderTests(
            "test_native_empty_recent_does_not_manufacture_tail_readiness"
        )
        self.fixture.setUp()
        self.provider = self.fixture.provider

    def tearDown(self) -> None:
        self.fixture.tearDown()

    def _admitted_target(self) -> tuple[Any, str, Any, list[Any]]:
        tools = self.fixture._reader_tools()
        conversation = tools.wechat_find_conversations("Fixture")["candidates"][0][
            "conversation_id"
        ]
        with self.provider.session(
            SourceScope.conversation(self.fixture.account_key, self.fixture.conversation)
        ) as snapshot:
            reference = self.provider._messages(
                self.fixture.account_key,
                self.fixture.conversation,
                snapshot,
                direction="forward",
                limit=200,
            )
        focus = reference[len(reference) // 2]
        context = tools.service.repository.conversation_context(conversation)
        message_id = tools.service._ingest_messages(context, (focus,))[0]
        return tools, message_id, focus, reference

    def test_v6_revalidates_v5_context_cache_and_stales_its_materialized_cursor(self) -> None:
        current = self.provider.descriptor
        self.assertEqual(current.implementation, "sightglass.macos-wechat.sqlcipher.v6")
        with patch.object(type(self.provider), "descriptor", new_callable=PropertyMock,
                          return_value=replace(
                              current, implementation="sightglass.macos-wechat.sqlcipher.v5"
                          )):
            tools = self.fixture._reader_tools()
            conversation = tools.wechat_find_conversations("Fixture")["candidates"][0][
                "conversation_id"
            ]
            old = tools.service.read_messages(
                mode="recent", conversation_id=conversation, limit=2,
                projection="detail", refresh=True,
            )
            old_epoch = tools.service._projection_inventory_epoch()
            first = tools.service.read_messages(
                mode="recent", conversation_id=conversation, limit=1, projection="detail"
            )
            cursor = first["page"]["next_cursor"]
            self.assertIsInstance(cursor, str)
            self.assertTrue(tools.service.repository.has_materialized_read_plane(old_epoch))
        new_epoch = tools.service._projection_inventory_epoch()
        self.assertNotEqual(new_epoch, old_epoch)
        self.assertFalse(tools.service.repository.has_materialized_read_plane(new_epoch))
        with self.assertRaises(SightglassError) as stale:
            tools.service.read_messages(
                mode="recent", conversation_id=conversation, limit=1,
                projection="detail", cursor=cursor,
            )
        self.assertEqual(stale.exception.code, ErrorCode.CURSOR_STALE)
        # The ordinary context path must obtain fresh canonical source evidence;
        # the older resident rows cannot satisfy it merely because their IDs exist.
        with patch.object(self.provider, "get_message", wraps=self.provider.get_message) as read:
            context = tools.service.read_messages(
                mode="context", message_id=old["messages"][-1]["message_id"],
                before=1, after=0, limit=2, projection="detail",
            )
        self.assertGreater(read.call_count, 0)
        self.assertEqual(len(context["messages"]), 2)
        self.assertTrue(tools.service.repository.has_materialized_read_plane(new_epoch))

    def test_zero_radius_does_not_seek_or_resolve_discarded_neighbors(self) -> None:
        self.fixture._insert_message(
            local_id=3,
            server_id=103,
            sort_seq=3,
            create_time=1725000003,
            content="synthetic voice neighbor",
            local_type=34,
        )
        tools, message_id, _, _ = self._admitted_target()
        resolver = self.provider._resource_resolver
        real_connect = self.provider._connect

        class Connection:
            def __init__(self, connection: Any) -> None:
                self.connection = connection

            def __getattr__(self, name: str) -> Any:
                return getattr(self.connection, name)

            def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
                if "SELECT rowid AS source_rowid, create_time" in " ".join(sql.split()):
                    raise AssertionError("zero radius planned or read neighbors")
                return self.connection.execute(sql, *args, **kwargs)

        @contextmanager
        def guarded_connect(relative: str) -> Any:
            with real_connect(relative) as connection:
                yield Connection(connection)

        with (
            patch.object(self.provider, "_connect", side_effect=guarded_connect),
            patch.object(self.provider, "read_range", side_effect=AssertionError("zero radius")),
            patch.object(
                resolver, "resources_for_message", wraps=resolver.resources_for_message
            ) as resources,
        ):
            page = tools.service.read_messages(
                mode="context",
                message_id=message_id,
                before=0,
                after=0,
                limit=1,
                projection="detail",
                refresh=True,
                voice="off",
                include_resources="none",
            )
        self.assertEqual(len(page["messages"]), 1)
        self.assertEqual(resources.call_count, 1)

    def test_indexed_zero_side_has_one_seek_and_no_discarded_resource_work(self) -> None:
        table = self.fixture._table_name(self.fixture.conversation)
        writer = self.fixture._connect_new("message/message_0.db")
        try:
            writer.execute(f"CREATE INDEX synthetic_time ON [{table}](create_time, sort_seq)")
            writer.commit()
        finally:
            writer.close()
        statements: list[str] = []
        real_connect = self.provider._connect

        class Connection:
            def __init__(self, connection: Any) -> None:
                self.connection = connection

            def __getattr__(self, name: str) -> Any:
                return getattr(self.connection, name)

            def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
                normalized = " ".join(sql.split())
                if normalized.startswith("SELECT rowid AS source_rowid, create_time"):
                    statements.append(normalized)
                return self.connection.execute(sql, *args, **kwargs)

        @contextmanager
        def counting_connect(relative: str) -> Any:
            with real_connect(relative) as connection:
                yield Connection(connection)

        focus_id = self.provider._message_token(
            self.fixture.conversation, {"server_id": 102}, "message/message_0.db"
        )
        resolver = self.provider._resource_resolver
        with self.provider.session(
            SourceScope.conversation(self.fixture.account_key, self.fixture.conversation)
        ) as snapshot:
            focus = self.provider.get_message(self.fixture.account_key, focus_id, snapshot)
            assert focus is not None
            with (
                patch.object(self.provider, "_connect", side_effect=counting_connect),
                patch.object(
                    resolver, "resources_for_message", wraps=resolver.resources_for_message
                ) as resources,
            ):
                page = self.provider.read_context(
                    self.fixture.account_key,
                    self.fixture.conversation,
                    focus=focus,
                    before=1,
                    after=0,
                    snapshot=snapshot,
                )
        self.assertEqual(
            [message.raw_content for message in page.messages], ["first message", "latest message"]
        )
        self.assertFalse(page.has_more_before)
        self.assertFalse(page.has_more_after)
        self.assertEqual(len(statements), 1)
        self.assertIn("ORDER BY create_time DESC", statements[0])
        self.assertEqual(resources.call_count, 1)

    def test_message_scope_cannot_expand_into_context_even_at_zero_radius(self) -> None:
        focus_id = self.provider._message_token(
            self.fixture.conversation, {"server_id": 102}, "message/message_0.db"
        )
        with self.provider.session(
            SourceScope.message(self.fixture.account_key, focus_id)
        ) as snapshot:
            focus = self.provider.get_message(self.fixture.account_key, focus_id, snapshot)
            assert focus is not None
            with self.assertRaises(SightglassError) as caught:
                self.provider.read_context(
                    self.fixture.account_key,
                    self.fixture.conversation,
                    focus=focus,
                    before=0,
                    after=0,
                    snapshot=snapshot,
                )
        self.assertEqual(caught.exception.code, ErrorCode.MESSAGE_NOT_FOUND)

    def test_zero_radius_midpoint_admits_only_point_window_without_history_or_tail_proof(
        self,
    ) -> None:
        for number in range(3, 8):
            self.fixture._insert_message(
                local_id=number,
                server_id=100 + number,
                sort_seq=number,
                create_time=1725000000 + number,
                content=f"synthetic midpoint neighbor {number}",
            )
        tools, message_id, focus, _ = self._admitted_target()
        service = tools.service
        conversation = service.repository.message_position_row(message_id)["conversation_id"]
        self.assertIsNone(service.repository.source_conversation_state(conversation))
        page = service.read_messages(
            mode="context",
            message_id=message_id,
            before=0,
            after=0,
            limit=1,
            projection="detail",
            refresh=True,
            voice="off",
        )
        self.assertEqual(len(page["messages"]), 1)
        self.assertIsNone(service.repository.source_conversation_state(conversation))
        with service.repository.database.connection() as connection:
            windows = connection.execute(
                "SELECT lower_message_id, upper_message_id FROM source_read_windows "
                "WHERE conversation_id=?",
                (conversation,),
            ).fetchall()
            observed = connection.execute(
                "SELECT source_message_id FROM messages WHERE conversation_id=?",
                (conversation,),
            ).fetchall()
        self.assertEqual(
            [tuple(row) for row in windows], [(focus.source_message_id, focus.source_message_id)]
        )
        self.assertEqual([row[0] for row in observed], [focus.source_message_id])
        with patch.object(self.provider, "session", side_effect=AssertionError("local context")):
            expanded = service.read_messages(
                mode="context",
                message_id=message_id,
                before=2,
                after=2,
                limit=5,
                projection="detail",
                voice="off",
            )
        self.assertEqual(len(expanded["messages"]), 1)
        self.assertFalse(expanded["source_receipt"]["complete"])
        self.assertEqual(expanded["source_receipt"]["coverage"]["conversation"], "indexed")
        self.assertIn("history_not_fully_indexed", expanded["source_receipt"]["warnings"])

    def test_two_shards_one_pass_preserves_chronology_ties_and_bounded_resource_work(self) -> None:
        base = 1725150000
        for number in range(3, 45):
            self.fixture._insert_message(
                local_id=number,
                server_id=100 + number,
                create_time=base + number // 4,
                sort_seq=50 - number,
                content=f"synthetic reversed sequence {number}",
            )
        self.fixture._add_message_shard(
            suffix="1",
            messages=[(number, base + number // 4, number % 3) for number in range(1, 40)],
        )
        tools, message_id, focus, reference = self._admitted_target()
        focus_index = reference.index(focus)
        expected = reference[focus_index - 5 : focus_index + 8]
        statements: list[str] = []
        real_connect = self.provider._connect

        class Connection:
            def __init__(self, connection: Any) -> None:
                self.connection = connection

            def __getattr__(self, name: str) -> Any:
                return getattr(self.connection, name)

            def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
                normalized = " ".join(sql.split())
                if normalized.startswith("SELECT rowid AS source_rowid, create_time"):
                    statements.append(normalized)
                return self.connection.execute(sql, *args, **kwargs)

        @contextmanager
        def counting_connect(relative: str) -> Any:
            with real_connect(relative) as connection:
                yield Connection(connection)

        resolver = self.provider._resource_resolver
        with (
            patch.object(self.provider, "_connect", side_effect=counting_connect),
            patch.object(self.provider, "get_message", wraps=self.provider.get_message) as get,
            patch.object(self.provider, "list_participants", side_effect=AssertionError("roster")),
            patch.object(
                resolver, "resources_for_message", wraps=resolver.resources_for_message
            ) as resources,
        ):
            page = tools.service.read_messages(
                mode="context",
                message_id=message_id,
                before=5,
                after=7,
                limit=13,
                projection="detail",
                refresh=True,
                voice="off",
            )
        actual = [
            tools.service.repository.message_position_row(item["message_id"])["source_message_id"]
            for item in page["messages"]
        ]
        self.assertEqual(actual, [item.source_message_id for item in expected])
        self.assertTrue(page["page"]["has_more_before"])
        self.assertTrue(page["page"]["has_more_after"])
        self.assertEqual(get.call_count, 1)
        self.assertEqual(len(statements), 2)
        self.assertFalse(any("ORDER BY" in sql for sql in statements))
        self.assertEqual(resources.call_count, 13)

    def test_same_position_fallback_tokens_keep_exact_source_id_tie_order(self) -> None:
        base = 1725150000
        self.fixture._insert_message(
            local_id=0,
            server_id=0,
            sort_seq=7,
            create_time=base,
            content="synthetic fallback alpha",
        )
        relative = self.fixture._add_message_shard(
            suffix="1",
            messages=[(1, base - 2, 100), (2, base - 1, 99), (3, base, 7)],
        )
        writer = self.fixture._connect_new(relative)
        try:
            table = self.fixture._table_name(self.fixture.conversation)
            writer.execute(
                f"UPDATE [{table}] SET local_id=0,server_id=0,message_content=? WHERE rowid=3",
                ("synthetic fallback beta",),
            )
            writer.commit()
        finally:
            writer.close()
        with self.provider.session(
            SourceScope.conversation(self.fixture.account_key, self.fixture.conversation)
        ) as snapshot:
            reference = self.provider._messages(
                self.fixture.account_key,
                self.fixture.conversation,
                snapshot,
                direction="forward",
                limit=20,
            )
            focus = reference[-2]
            page = self.provider.read_context(
                self.fixture.account_key,
                self.fixture.conversation,
                focus=focus,
                before=1,
                after=1,
                snapshot=snapshot,
            )
        self.assertEqual(
            [item.source_message_id for item in page.messages],
            [item.source_message_id for item in reference[-3:]],
        )

    def test_indexed_unindexed_and_mixed_layouts_bound_vm_payload_and_resource_work(self) -> None:
        relative = "message/message_0.db"
        table = self.fixture._table_name(self.fixture.conversation)
        base = 1725150000
        count = 20_000
        writer = self.fixture._connect_new(relative)
        try:
            writer.executemany(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    (
                        100 + index,
                        100_000 + index,
                        1,
                        count - index,
                        0,
                        base + (index * 7919) % count,
                        0,
                        f"synthetic unindexed position {index}",
                        0,
                        None,
                    )
                    for index in range(count)
                ),
            )
            # This layout deliberately offers only the nonchronological seq index.
            writer.execute(f"CREATE INDEX synthetic_sequence ON [{table}](sort_seq)")
            writer.commit()
        finally:
            writer.close()
        focus_id = self.provider._message_token(
            self.fixture.conversation, {"server_id": 110_000}, relative
        )
        ordered_ids = [
            self.provider._message_token(
                self.fixture.conversation, {"server_id": 100_000 + index}, relative
            )
            for index in sorted(range(count), key=lambda value: (value * 7919) % count)
        ]
        pivot = ordered_ids.index(focus_id)
        expected = ordered_ids[pivot - 15 : pivot + 18]
        steps = [0]
        batch_sizes: list[int] = []
        payload_sizes: list[int] = []
        statements: list[str] = []
        real_connect = self.provider._connect

        class Cursor:
            def __init__(self, cursor: Any, connection: Any) -> None:
                self.cursor = cursor
                self.connection = connection

            def fetchmany(self, size: int) -> Any:
                batch = self.cursor.fetchmany(size)
                batch_sizes.append(len(batch))
                if not batch:
                    self.connection.set_progress_handler(lambda: int(operation_expired()), 1000)
                return batch

            def fetchall(self) -> Any:
                rows = self.cursor.fetchall()
                batch_sizes.append(len(rows))
                self.connection.set_progress_handler(lambda: int(operation_expired()), 1000)
                return rows

        class Connection:
            def __init__(self, connection: Any) -> None:
                self.connection = connection

            def __getattr__(self, name: str) -> Any:
                return getattr(self.connection, name)

            def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
                normalized = " ".join(sql.split())
                if normalized.startswith("SELECT rowid AS source_rowid, create_time"):
                    statements.append(normalized)

                    def progress() -> int:
                        steps[0] += 100
                        return int(operation_expired())

                    self.connection.set_progress_handler(progress, 100)
                    return Cursor(self.connection.execute(sql, *args, **kwargs), self.connection)
                if "WHERE message_row.rowid IN" in normalized:
                    payload_sizes.append(len(args[0]))
                return self.connection.execute(sql, *args, **kwargs)

        @contextmanager
        def counting_connect(selected: str) -> Any:
            with real_connect(selected) as connection:
                yield Connection(connection)

        def assert_work(
            reference: list[str],
            *,
            query_count: int,
            position_count: int,
            payload_counts: list[int],
            vm_limit: int,
        ) -> None:
            steps[0] = 0
            batch_sizes.clear()
            payload_sizes.clear()
            statements.clear()
            resolver = self.provider._resource_resolver
            with self.provider.session(
                SourceScope.conversation(self.fixture.account_key, self.fixture.conversation)
            ) as snapshot:
                focus = self.provider.get_message(self.fixture.account_key, focus_id, snapshot)
                assert focus is not None
                with (
                    patch.object(self.provider, "_connect", side_effect=counting_connect),
                    patch.object(
                        resolver, "resources_for_message", wraps=resolver.resources_for_message
                    ) as resources,
                ):
                    page = self.provider.read_context(
                        self.fixture.account_key,
                        self.fixture.conversation,
                        focus=focus,
                        before=15,
                        after=17,
                        snapshot=snapshot,
                    )
            self.assertEqual([item.source_message_id for item in page.messages], reference)
            self.assertTrue(page.has_more_before)
            self.assertTrue(page.has_more_after)
            self.assertEqual(len(statements), query_count)
            self.assertEqual(sum(batch_sizes), position_count)
            self.assertLessEqual(max(batch_sizes), 1024)
            self.assertEqual(payload_sizes, payload_counts)
            self.assertLess(steps[0], vm_limit)
            self.assertEqual(resources.call_count, 32)

        with self.subTest(layout="no time index"):
            assert_work(
                expected,
                query_count=1,
                position_count=count + 2,
                payload_counts=[34],
                vm_limit=(count + 2) * 12,
            )
            self.assertNotIn("ORDER BY", statements[0])

        writer = self.fixture._connect_new(relative)
        try:
            writer.execute(f"CREATE INDEX synthetic_time ON [{table}](create_time, sort_seq)")
            rows = writer.execute(f"SELECT * FROM [{table}] ORDER BY rowid").fetchall()
            writer.commit()
        finally:
            writer.close()
        with self.subTest(layout="time index"):
            assert_work(
                expected,
                query_count=2,
                position_count=36,
                payload_counts=[34],
                vm_limit=3000,
            )
            self.assertTrue(all("ORDER BY" in sql for sql in statements))

        second = self.fixture._add_message_shard(suffix="1", messages=[])
        writer = self.fixture._connect_new(second)
        try:
            writer.executemany(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ((row[0], row[1] + 200_000, *row[2:]) for row in rows),
            )
            writer.commit()
        finally:
            writer.close()
        mixed_order = sorted(
            (
                (
                    base + (index * 7919) % count,
                    count - index,
                    index + 3,
                    self.provider._message_token(
                        self.fixture.conversation,
                        {"server_id": 100_000 + index + delta},
                        shard,
                    ),
                )
                for shard, delta in ((relative, 0), (second, 200_000))
                for index in range(count)
            )
        )
        mixed_ids = [item[3] for item in mixed_order]
        pivot = mixed_ids.index(focus_id)
        with self.subTest(layout="mixed indexed and unindexed shards"):
            assert_work(
                mixed_ids[pivot - 15 : pivot + 18],
                query_count=3,
                position_count=count + 38,
                payload_counts=[34, 35],
                vm_limit=(count + 2) * 12 + 3000,
            )
            self.assertEqual(sum("ORDER BY" in sql for sql in statements), 2)

    def test_overlap_duplicate_identity_is_deduplicated_or_fails_closed(self) -> None:
        relative = self.fixture._add_message_shard(
            suffix="1",
            messages=[(1, 1725000001, 1)],
        )
        table = self.fixture._table_name(self.fixture.conversation)
        with self.provider.session(
            SourceScope.conversation(self.fixture.account_key, self.fixture.conversation)
        ) as snapshot:
            focus_id = (
                self.provider.read_recent(
                    self.fixture.account_key,
                    self.fixture.conversation,
                    1,
                    snapshot,
                )
                .messages[-1]
                .source_message_id
            )
        for content, conflict in (("first message", False), ("synthetic conflict", True)):
            writer = self.fixture._connect_new(relative)
            try:
                writer.execute(
                    f"UPDATE [{table}] SET server_id=101,message_content=? WHERE rowid=1",
                    (content,),
                )
                writer.commit()
            finally:
                writer.close()
            with self.provider.session(
                SourceScope.conversation(self.fixture.account_key, self.fixture.conversation)
            ) as snapshot:
                focus = self.provider.get_message(self.fixture.account_key, focus_id, snapshot)
                assert focus is not None
                if conflict:
                    with self.assertRaises(SightglassError) as caught:
                        self.provider.read_context(
                            self.fixture.account_key,
                            self.fixture.conversation,
                            focus=focus,
                            before=1,
                            after=0,
                            snapshot=snapshot,
                        )
                    self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)
                    self.assertIn(
                        "duplicate_message_identity_conflict",
                        caught.exception.details["warning_codes"],
                    )
                else:
                    page = self.provider.read_context(
                        self.fixture.account_key,
                        self.fixture.conversation,
                        focus=focus,
                        before=1,
                        after=0,
                        snapshot=snapshot,
                    )
                    self.assertEqual(len(page.messages), 2)
                    self.assertFalse(page.has_more_before)

    def test_overlap_heavy_shards_fill_unique_radius_and_keep_sentinel_flags(self) -> None:
        for number in range(3, 23):
            self.fixture._insert_message(
                local_id=number,
                server_id=100 + number,
                sort_seq=number,
                create_time=1725000000 + number,
                content=f"synthetic overlapping message {number}",
            )
        table = self.fixture._table_name(self.fixture.conversation)
        original = self.fixture._connect_new("message/message_0.db")
        try:
            rows = original.execute(f"SELECT * FROM [{table}] ORDER BY rowid").fetchall()
        finally:
            original.close()
        for suffix in ("1", "2"):
            relative = self.fixture._add_message_shard(suffix=suffix, messages=[])
            writer = self.fixture._connect_new(relative)
            try:
                writer.executemany(
                    f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
                )
                writer.commit()
            finally:
                writer.close()
        focus_id = self.provider._message_token(
            self.fixture.conversation, {"server_id": 112}, "message/message_0.db"
        )
        expected = [
            self.provider._message_token(
                self.fixture.conversation, {"server_id": 100 + number}, "message/message_0.db"
            )
            for number in range(8, 18)
        ]
        resolver = self.provider._resource_resolver
        with self.provider.session(
            SourceScope.conversation(self.fixture.account_key, self.fixture.conversation)
        ) as snapshot:
            focus = self.provider.get_message(self.fixture.account_key, focus_id, snapshot)
            assert focus is not None
            with patch.object(
                resolver, "resources_for_message", wraps=resolver.resources_for_message
            ) as resources:
                page = self.provider.read_context(
                    self.fixture.account_key,
                    self.fixture.conversation,
                    focus=focus,
                    before=4,
                    after=5,
                    snapshot=snapshot,
                )
        self.assertEqual([item.source_message_id for item in page.messages], expected)
        self.assertTrue(page.has_more_before)
        self.assertTrue(page.has_more_after)
        self.assertEqual(resources.call_count, 9)

    def test_within_shard_duplicate_server_rows_match_existing_conflict_contract(self) -> None:
        # The server_id index is not unique. Same payload/time/seq rows with this
        # ID still have distinct physical rowids, which the current canonical
        # evidence contract treats as conflicting positions, not equal overlap.
        for number in range(3, 15):
            backward = number < 9
            self.fixture._insert_message(
                local_id=number,
                server_id=900 if backward else 901,
                sort_seq=100,
                create_time=1725000001 if backward else 1725000003,
                content="synthetic identical repeated server payload",
            )
        self.fixture._insert_message(
            local_id=15,
            server_id=902,
            sort_seq=1,
            create_time=1725000004,
            content="synthetic farther unique message",
        )
        table = self.fixture._table_name(self.fixture.conversation)
        focus_id = self.provider._message_token(
            self.fixture.conversation, {"server_id": 102}, "message/message_0.db"
        )
        for indexed in (False, True):
            if indexed:
                writer = self.fixture._connect_new("message/message_0.db")
                try:
                    writer.execute(
                        f"CREATE INDEX synthetic_time ON [{table}](create_time, sort_seq)"
                    )
                    writer.commit()
                finally:
                    writer.close()
            for direction in ("backward", "forward"):
                with self.subTest(indexed=indexed, direction=direction):
                    with self.provider.session(
                        SourceScope.conversation(
                            self.fixture.account_key, self.fixture.conversation
                        )
                    ) as snapshot:
                        focus = self.provider.get_message(
                            self.fixture.account_key, focus_id, snapshot
                        )
                        assert focus is not None
                        with self.assertRaises(SightglassError) as legacy:
                            self.provider.read_range(
                                self.fixture.account_key,
                                self.fixture.conversation,
                                after=focus.sort_key if direction == "forward" else None,
                                before=focus.sort_key if direction == "backward" else None,
                                direction=direction,
                                limit=3,
                                snapshot=snapshot,
                            )
                        resolver = self.provider._resource_resolver
                        with (
                            patch.object(
                                resolver,
                                "resources_for_message",
                                wraps=resolver.resources_for_message,
                            ) as resources,
                            self.assertRaises(SightglassError) as fused,
                        ):
                            self.provider.read_context(
                                self.fixture.account_key,
                                self.fixture.conversation,
                                focus=focus,
                                before=3 if direction == "backward" else 0,
                                after=3 if direction == "forward" else 0,
                                snapshot=snapshot,
                            )
                        for caught in (legacy, fused):
                            self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)
                            self.assertEqual(
                                caught.exception.details["warning_codes"],
                                ["duplicate_message_identity_conflict"],
                            )
                        self.assertEqual(resources.call_count, 0)
                        duplicate_id = self.provider._message_token(
                            self.fixture.conversation,
                            {"server_id": 900 if direction == "backward" else 901},
                            "message/message_0.db",
                        )
                        with self.assertRaises(SightglassError) as point:
                            self.provider.get_message(
                                self.fixture.account_key, duplicate_id, snapshot
                            )
                        self.assertEqual(point.exception.code, ErrorCode.SOURCE_INCOMPLETE)


    def test_context_fails_closed_on_retained_neighbor_conflict_in_far_shard(self) -> None:
        # The primary fixture has server_id 101 one step before focus 102. A
        # second shard holds a conflicting copy of 101 far outside the neighbor
        # radius, so the scan window never sees both copies. read_context must
        # still reconcile the retained neighbor's canonical identity and fail
        # closed exactly as get_message does for the same id.
        self.fixture._add_conflicting_conversation_shard(
            suffix="7",
            conversation=self.fixture.conversation,
            server_id=101,
            create_time=1_725_009_999,
        )
        with self.provider.session(
            SourceScope.conversation(self.fixture.account_key, self.fixture.conversation)
        ) as snapshot:
            focus_id = self.provider._message_token(
                self.fixture.conversation, {"server_id": 102}, "message/message_0.db"
            )
            focus = self.provider.get_message(self.fixture.account_key, focus_id, snapshot)
            assert focus is not None
            resolver = self.provider._resource_resolver
            with (
                patch.object(
                    resolver,
                    "resources_for_message",
                    wraps=resolver.resources_for_message,
                ) as resources,
                self.assertRaises(SightglassError) as caught,
            ):
                self.provider.read_context(
                    self.fixture.account_key,
                    self.fixture.conversation,
                    focus=focus,
                    before=1,
                    after=0,
                    snapshot=snapshot,
                )
            self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)
            self.assertEqual(
                caught.exception.details["warning_codes"],
                ["duplicate_message_identity_conflict"],
            )
            # A conflict must be detected before any resource work.
            self.assertEqual(resources.call_count, 0)

    def test_context_finds_far_conflict_hidden_behind_a_nearer_other_shard_row(self) -> None:
        # A second shard holds both a nearer unrelated row and the conflicting copy
        # of a retained neighbor's id, so the conflict copy is outside the nearest
        # forward candidate window and only canonical reconciliation can see it.
        self.fixture._insert_message(
            local_id=3,
            server_id=103,
            sort_seq=3,
            create_time=1_725_000_003,
            content="synthetic forward neighbor",
        )
        conflict_shard = self.fixture._add_conflicting_conversation_shard(
            suffix="8",
            conversation=self.fixture.conversation,
            server_id=103,
            create_time=1_725_009_999,
        )
        nearer = self.fixture._connect_new(conflict_shard)
        try:
            nearer.execute(
                f"INSERT INTO [{self.fixture._table_name(self.fixture.conversation)}] "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (901, 1901, 1, 5, 0, 1_725_000_004, 0, "nearer unrelated shard row", 0, None),
            )
            nearer.commit()
        finally:
            nearer.close()
        with self.provider.session(
            SourceScope.conversation(self.fixture.account_key, self.fixture.conversation)
        ) as snapshot:
            focus_id = self.provider._message_token(
                self.fixture.conversation, {"server_id": 102}, "message/message_0.db"
            )
            focus = self.provider.get_message(self.fixture.account_key, focus_id, snapshot)
            assert focus is not None
            with self.assertRaises(SightglassError) as caught:
                self.provider.read_context(
                    self.fixture.account_key,
                    self.fixture.conversation,
                    focus=focus,
                    before=0,
                    after=1,
                    snapshot=snapshot,
                )
            self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)
            self.assertEqual(
                caught.exception.details["warning_codes"],
                ["duplicate_message_identity_conflict"],
            )

    def test_context_returns_ordered_neighbors_when_no_far_duplicate_exists(self) -> None:
        # A second, fully consistent shard with distinct server ids must not change
        # the normal retained-neighbor order or count.
        self.fixture._add_message_shard(
            suffix="9",
            messages=[(number, 1_725_000_010 + number, number) for number in range(1, 5)],
        )
        with self.provider.session(
            SourceScope.conversation(self.fixture.account_key, self.fixture.conversation)
        ) as snapshot:
            focus_id = self.provider._message_token(
                self.fixture.conversation, {"server_id": 102}, "message/message_0.db"
            )
            focus = self.provider.get_message(
                self.fixture.account_key, focus_id, snapshot
            )
            assert focus is not None
            page = self.provider.read_context(
                self.fixture.account_key,
                self.fixture.conversation,
                focus=focus,
                before=1,
                after=1,
                snapshot=snapshot,
            )
            keys = [message.sort_key.as_tuple() for message in page.messages]
            self.assertEqual(keys, sorted(keys))
            self.assertEqual(len({m.source_message_id for m in page.messages}), len(page.messages))


class SyntheticContextWindowTests(_ReadingFixture):
    def test_zero_radius_fallback_skips_both_range_calls(self) -> None:
        self._append(1, 3)
        self.service.sync_source_once(initial_tail=100)
        with patch.object(self.provider, "read_range", side_effect=AssertionError("zero radius")):
            page = self._page(
                mode="context",
                message_id=self._id(2),
                before=0,
                after=0,
                limit=1,
                refresh=True,
                voice="off",
            )
        self.assertEqual(self._numbers(page), [2])

    def test_zero_radius_midpoint_never_certifies_end_or_stops_existing_source_cursor(self) -> None:
        self._append(1, 7)
        context = self.repository.conversation_context(self.group)
        self.service._ingest_messages(context, (self._source(4),))
        before = self._positions()
        with patch.object(self.provider, "read_range", side_effect=AssertionError("zero radius")):
            point = self._page(
                mode="context",
                message_id=self._id(4),
                before=0,
                after=0,
                limit=1,
                refresh=True,
                voice="off",
            )
        self.assertEqual(self._numbers(point), [4])
        self.assertEqual(self._positions(), before)
        self.assertIsNone(self.repository.source_conversation_state(self.group))
        with self.repository.database.connection() as connection:
            windows = connection.execute(
                "SELECT lower_seq,upper_seq FROM source_read_windows WHERE conversation_id=?",
                (self.group,),
            ).fetchall()
        self.assertEqual([tuple(row) for row in windows], [(4, 4)])
        expanded = self._page(
            mode="context",
            message_id=self._id(4),
            before=2,
            after=2,
            limit=5,
            voice="off",
        )
        self.assertEqual(self._numbers(expanded), [4])
        self.assertFalse(expanded["source_receipt"]["complete"])
        self.assertEqual(expanded["source_receipt"]["coverage"]["conversation"], "indexed")

        range_args = dict(
            mode="range",
            direction="forward",
            limit=2,
            voice="off",
            time_after="2026-01-01T00:00:00+00:00",
        )
        page = self._page(**range_args)
        observed = self._numbers(page)
        cursor = page["page"]["next_cursor"]
        self.assertIsNotNone(cursor)
        before = self._positions()
        self._page(
            mode="context",
            message_id=self._id(4),
            before=0,
            after=0,
            limit=1,
            refresh=True,
            voice="off",
        )
        self.assertEqual(self._positions(), before)
        self.assertIsNone(self.repository.source_conversation_state(self.group))
        while cursor:
            page = self._page(**range_args, cursor=cursor)
            observed.extend(self._numbers(page))
            cursor = page["page"]["next_cursor"]
        self.assertEqual(observed, list(range(1, 8)))
        self.assertIsNone(self.repository.source_conversation_state(self.group))

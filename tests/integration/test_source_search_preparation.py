"""Bounded, restart-honest source preparation using generated source databases."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.messages import SourceMessagePage
from sightglass.operations import operation_budget, operation_expired
from sightglass.source.base import SourcePreparationStep, SourceScope
from sightglass.source.direct_wechat import SyntheticSourceProvider
from sightglass.source.synthetic import create_synthetic_source
from tests.integration import test_native_source_provider as native_fixture


def _finish(iterator: Any) -> list[SourcePreparationStep]:
    steps = []
    while True:
        try:
            with operation_budget(2.0):
                steps.append(next(iterator))
        except StopIteration:
            break
    return steps


def _page(step: SourcePreparationStep) -> SourceMessagePage:
    assert step.phase == "complete" and step.page is not None
    return step.page


class NativeSearchPreparationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = native_fixture.NativeSourceProviderTests(
            "test_native_empty_recent_does_not_manufacture_tail_readiness"
        )
        self.fixture.setUp()
        self.provider = self.fixture.provider
        self.account = self.fixture.account_key
        self.target = self.fixture.conversation
        self.scope = SourceScope.conversation(self.account, self.target)

    def tearDown(self) -> None:
        self.fixture.tearDown()

    def _prepare(self, snapshot: Any, **kwargs: Any) -> Any:
        return self.provider.prepare_search_page(
            self.account,
            self.target,
            snapshot=snapshot,
            **{"direction": "forward", "limit": 50, "batch_size": 1024, **kwargs},
        )

    def _large_table(self, size: int = 20_000, *, invalid_unused: bool = False) -> None:
        table = self.fixture._table_name(self.target)
        writer = self.fixture._connect_new("message/message_0.db")
        try:
            writer.execute(f"DELETE FROM [{table}]")
            # Timestamp/sequence are deliberately unrelated to physical row order.
            writer.executemany(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    (
                        number,
                        number + 1000,
                        1,
                        size - number,
                        0,
                        1_725_000_000 + (number * 97) % size,
                        0,
                        b"invalid unused compression" if invalid_unused else f"synthetic {number}",
                        4 if invalid_unused else 0,
                        None,
                    )
                    for number in range(1, size + 1)
                ),
            )
            writer.commit()
        finally:
            writer.close()

    @contextmanager
    def _measure_scans(self) -> Any:
        measured: list[tuple[str, int, int, tuple[str, ...]]] = []
        real_connect = self.provider._connect

        class Cursor:
            def __init__(self, rows: list[Any]) -> None:
                self.rows = rows

            def fetchall(self) -> list[Any]:
                return self.rows

        class Connection:
            def __init__(self, connection: Any) -> None:
                self.connection = connection

            def __getattr__(self, name: str) -> Any:
                return getattr(self.connection, name)

            def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
                normalized = " ".join(sql.split())
                if normalized.startswith("SELECT rowid AS source_rowid, create_time"):
                    ticks = 0

                    def progress() -> int:
                        nonlocal ticks
                        ticks += 1
                        return int(operation_expired())

                    self.connection.set_progress_handler(progress, 100)
                    try:
                        plan = tuple(
                            str(row[3])
                            for row in self.connection.execute(
                                "EXPLAIN QUERY PLAN " + sql,
                                *args,
                                **kwargs,
                            )
                        )
                        rows = self.connection.execute(sql, *args, **kwargs).fetchall()
                        measured.append((normalized, len(rows), ticks * 100, plan))
                        return Cursor(rows)
                    finally:
                        self.connection.set_progress_handler(lambda: int(operation_expired()), 1000)
                return self.connection.execute(sql, *args, **kwargs)

        @contextmanager
        def connect(relative: str) -> Any:
            with real_connect(relative) as connection:
                yield Connection(connection)

        with patch.object(self.provider, "_connect", side_effect=connect):
            yield measured

    def test_unindexed_large_table_each_step_has_bounded_vm_work_and_exact_page(self) -> None:
        self._large_table()
        writer = self.fixture._connect_new("message/message_0.db")
        try:
            writer.execute(
                f"UPDATE [{self.fixture._table_name(self.target)}] SET rowid=-7 WHERE rowid=1"
            )
            writer.commit()
        finally:
            writer.close()
        resolver = self.provider._resource_resolver
        with self.provider.session(self.scope) as snapshot:
            expected = self.provider.read_range(
                self.account,
                self.target,
                after=None,
                before=None,
                direction="forward",
                limit=50,
                snapshot=snapshot,
                time_after_utc="2024-08-30T06:50:00Z",
                time_before_utc="2024-08-30T12:23:20Z",
            )
            with (
                self._measure_scans() as measured,
                patch.object(self.provider, "read_range", side_effect=AssertionError("sync range")),
                patch.object(
                    resolver, "resources_for_message", wraps=resolver.resources_for_message
                ) as resources,
            ):
                steps = _finish(
                    self._prepare(
                        snapshot,
                        batch_size=100_000,
                        time_after_utc="2024-08-30T06:50:00Z",
                        time_before_utc="2024-08-30T12:23:20Z",
                    )
                )
        self.assertEqual(steps[-1].page, expected)
        self.assertEqual(steps[-1].scanned_rows, 20_000)
        self.assertEqual(steps[-1].completed_shards, 1)
        self.assertEqual(resources.call_count, 50)
        self.assertEqual(len(measured), 20)
        self.assertLessEqual(max(rows for _sql, rows, _vm, _plan in measured), 1024)
        self.assertLessEqual(max(vm for _sql, _rows, vm, _plan in measured), 13_000)
        self.assertFalse(any("TEMP" in item for *_rest, plan in measured for item in plan))
        self.assertTrue(all("ORDER BY rowid" in sql for sql, *_rest in measured))
        self.assertTrue(all("create_time >=" not in sql for sql, *_rest in measured))
        self.assertTrue(all(step.page is None for step in steps[:-1]))

    def test_absent_time_window_scans_chunks_without_decoding_unused_payload(self) -> None:
        self._large_table(4097, invalid_unused=True)
        with self._measure_scans() as measured:
            with self.provider.session(self.scope) as snapshot:
                steps = _finish(
                    self._prepare(
                        snapshot,
                        time_after_utc="2026-01-01T00:00:00Z",
                        time_before_utc="2026-01-02T00:00:00Z",
                    )
                )
        self.assertEqual([size for _sql, size, _vm, _plan in measured], [1024] * 4 + [1])
        self.assertEqual(_page(steps[-1]).messages, ())
        self.assertFalse(_page(steps[-1]).has_more_after)
        self.assertEqual(steps[-1].scanned_rows, 4097)

    def test_old_chronological_sql_exceeds_one_chunk_work_before_returning_a_row(self) -> None:
        self._large_table()
        stop = threading.Event()
        ticks = 0

        def progress() -> int:
            nonlocal ticks
            ticks += 1
            if ticks >= 130:
                stop.set()
            return int(operation_expired())

        with self.assertRaises(SightglassError) as caught:
            with operation_budget(2.0, cancelled=stop):
                with self.provider.session(self.scope):
                    with self.provider._connect("message/message_0.db") as connection:
                        connection.set_progress_handler(progress, 100)
                        connection.execute(
                            "SELECT rowid AS source_rowid, create_time, COALESCE(sort_seq,0) "
                            f"FROM [{self.fixture._table_name(self.target)}] "
                            "WHERE create_time >= ? AND create_time < ? "
                            "ORDER BY create_time, COALESCE(sort_seq,0), rowid LIMIT 256",
                            (1_725_000_000, 1_725_020_000),
                        ).fetchall()
        self.assertEqual(caught.exception.code, ErrorCode.SERVICE_TIMEOUT)
        self.assertEqual(caught.exception.details["reason"], "operation_cancelled")
        self.assertEqual(ticks, 130)

    def test_cancelled_attempt_has_no_page_and_new_session_requeues_from_zero(self) -> None:
        self._large_table(2050)
        stop = threading.Event()
        with self.provider.session(self.scope) as snapshot:
            iterator = self._prepare(snapshot)
            self.assertEqual(next(iterator).scanned_rows, 0)
            self.assertEqual(next(iterator).scanned_rows, 1024)
            stop.set()
            with self.assertRaises(SightglassError) as caught:
                with operation_budget(2.0, cancelled=stop):
                    next(iterator)
            iterator.close()
        self.assertEqual(caught.exception.code, ErrorCode.SERVICE_TIMEOUT)
        self.assertEqual(caught.exception.details["reason"], "operation_cancelled")
        with self.provider.session(self.scope) as snapshot:
            restarted = self._prepare(snapshot)
            self.assertEqual(next(restarted).scanned_rows, 0)
            steps = _finish(restarted)
            expected = self.provider.read_range(
                self.account,
                self.target,
                after=None,
                before=None,
                direction="forward",
                limit=50,
                snapshot=snapshot,
            )
        self.assertEqual(steps[-1].page, expected)

    def test_authority_is_rechecked_after_chunk_before_progress_or_page(self) -> None:
        calls = 0

        def authority() -> None:
            nonlocal calls
            calls += 1
            if calls == 3:
                raise SightglassError(ErrorCode.POLICY_DENIED)

        with self.provider.session(self.scope) as snapshot:
            iterator = self._prepare(snapshot, check_authority=authority)
            self.assertEqual(next(iterator).scanned_rows, 0)
            with self.assertRaises(SightglassError) as caught:
                next(iterator)
        self.assertEqual(caught.exception.code, ErrorCode.POLICY_DENIED)

    def test_full_four_tuple_anchors_recent_and_fractional_time_bounds(self) -> None:
        base = 1_725_000_002
        relative = self.fixture._add_message_shard(
            suffix="1",
            messages=[(1, base, 2), (2, base - 1, 100), (3, base + 1, 0)],
        )
        writer = self.fixture._connect_new(relative)
        try:
            writer.execute(
                f"UPDATE [{self.fixture._table_name(self.target)}] SET rowid=10 WHERE rowid=2"
            )
            writer.execute(
                f"UPDATE [{self.fixture._table_name(self.target)}] SET rowid=2 WHERE local_id=1"
            )
            writer.commit()
        finally:
            writer.close()
        with self.provider.session(self.scope) as snapshot:
            reference = self.provider._messages(
                self.account,
                self.target,
                snapshot,
                direction="forward",
                limit=100,
            )
            for direction in ("forward", "backward"):
                for after, before in (
                    (None, None),
                    (reference[1].sort_key, None),
                    (reference[-2].sort_key, None),
                    (reference[-3].sort_key, None),
                    (None, reference[-2].sort_key),
                ):
                    expected = self.provider.read_range(
                        self.account,
                        self.target,
                        after=after,
                        before=before,
                        direction=direction,
                        limit=2,
                        snapshot=snapshot,
                    )
                    actual = _finish(
                        self._prepare(
                            snapshot,
                            direction=direction,
                            limit=2,
                            batch_size=1,
                            after=after,
                            before=before,
                        )
                    )[-1].page
                    self.assertEqual(actual, expected)
            recent = _finish(self._prepare(snapshot, direction="backward", limit=2))[-1].page
            self.assertEqual(
                recent, self.provider.read_recent(self.account, self.target, 2, snapshot)
            )
            boundary = datetime.fromtimestamp(base + 0.5, UTC).isoformat()
            page = _page(_finish(self._prepare(snapshot, time_before_utc=boundary))[-1])
            self.assertEqual(
                list(page.messages), [m for m in reference if m.sent_at_utc < boundary]
            )

    def test_selected_wal_drift_fails_final_session_proof_then_fresh_attempt_succeeds(self) -> None:
        table = self.fixture._table_name(self.target)
        writer = self.fixture._connect_new("message/message_0.db")
        try:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            with self.assertRaises(SightglassError) as caught:
                with self.provider.session(self.scope) as snapshot:
                    iterator = self._prepare(snapshot, batch_size=1)
                    next(iterator)
                    next(iterator)
                    writer.execute(f"UPDATE [{table}] SET message_content='synthetic WAL change'")
                    writer.commit()
                    _finish(iterator)  # Private page cannot pass the enclosing proof.
            self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)
            with self.provider.session(self.scope) as snapshot:
                page = _page(_finish(self._prepare(snapshot))[-1])
            self.assertTrue(all(m.raw_content == "synthetic WAL change" for m in page.messages))
        finally:
            writer.close()

    def test_unopened_unrelated_wal_change_does_not_invalidate_preparation(self) -> None:
        relative = self.fixture._add_probe_message_shard("9")
        writer = self.fixture._connect_new(relative)
        try:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            writer.execute("INSERT INTO shard_probe VALUES (8)")
            writer.commit()
            # Warm the existing schema catalog so this unrelated shard stays unopened.
            with self.provider.snapshot() as snapshot:
                self.provider.search_generation_binding(
                    self.account, self.target, snapshot=snapshot
                )
            with self.provider.session(self.scope) as snapshot:
                iterator = self._prepare(snapshot, batch_size=1)
                next(iterator)
                next(iterator)
                writer.execute("INSERT INTO shard_probe VALUES (9)")
                writer.commit()
                page = _page(_finish(iterator)[-1])
            self.assertEqual(len(page.messages), 2)
        finally:
            writer.close()

    def test_binding_selects_target_without_payload_in_full_and_narrow_views(self) -> None:
        unrelated = self.fixture._add_probe_message_shard("9")

        def binding(scoped: bool) -> tuple[tuple[str, str], ...]:
            lease = self.provider.session(self.scope) if scoped else self.provider.snapshot()
            with lease as snapshot:
                with (
                    patch.object(
                        self.provider, "_message_rows_by_rowid", side_effect=AssertionError
                    ),
                    patch.object(self.provider, "_iter_shard_rows", side_effect=AssertionError),
                ):
                    result = self.provider.search_generation_binding(
                        self.account,
                        self.target,
                        snapshot=snapshot,
                    )
                self.assertEqual(dict(result), snapshot.dependency_generation_by_shard)
                return result

        original = binding(False)
        self.assertEqual(original, binding(True))
        self.assertEqual([key for key, _generation in original], ["message/message_0.db"])
        self.fixture._insert_message(
            local_id=3,
            server_id=103,
            sort_seq=3,
            create_time=1_725_000_003,
            content="synthetic append",
        )
        self.assertEqual(original, binding(False))
        path = self.fixture.source / unrelated
        replacement = self.fixture.root / "synthetic-unrelated-replacement.db"
        shutil.copy2(path, replacement)
        os.replace(replacement, path)
        self.assertEqual(original, binding(True))
        path = self.fixture.source / "message/message_0.db"
        replacement = self.fixture.root / "synthetic-selected-replacement.db"
        shutil.copy2(path, replacement)
        os.replace(replacement, path)
        self.assertNotEqual(original, binding(False))

    def test_within_shard_duplicate_identity_remains_fail_closed(self) -> None:
        self.fixture._insert_message(
            local_id=3,
            server_id=101,
            sort_seq=1,
            create_time=1_725_000_001,
            content="synthetic conflict",
        )
        with self.assertRaises(SightglassError) as caught:
            with self.provider.session(self.scope) as snapshot:
                _finish(self._prepare(snapshot))
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)
        self.assertIn(
            "duplicate_message_identity_conflict", caught.exception.details["warning_codes"]
        )

    def test_equal_cross_shard_overlap_deduplicates_before_limit_and_sentinel(self) -> None:
        self._large_table(40)
        relative = self.fixture._add_message_shard(suffix="1", messages=[])
        source = self.fixture._connect_new("message/message_0.db")
        target = self.fixture._connect_new(relative)
        try:
            table = self.fixture._table_name(self.target)
            rows = source.execute(f"SELECT * FROM [{table}] ORDER BY rowid").fetchall()
            target.executemany(
                f"INSERT INTO [{table}] VALUES ({','.join('?' for _ in range(10))})", rows
            )
            target.commit()
        finally:
            source.close()
            target.close()
        with self.provider.session(self.scope) as snapshot:
            for direction in ("forward", "backward"):
                expected = self.provider.read_range(
                    self.account,
                    self.target,
                    after=None,
                    before=None,
                    direction=direction,
                    limit=10,
                    snapshot=snapshot,
                )
                actual = _page(
                    _finish(
                        self._prepare(
                            snapshot,
                            direction=direction,
                            limit=10,
                            batch_size=8,
                        )
                    )[-1]
                )
                self.assertEqual(actual, expected)
                self.assertEqual(len({m.source_message_id for m in actual.messages}), 10)
                self.assertTrue(actual.has_more_before or actual.has_more_after)


class SyntheticSearchPreparationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "synthetic-source"
        create_synthetic_source(self.root)
        self.provider = SyntheticSourceProvider(self.root)
        self.account = "synthetic-account-demo"
        self.scope = SourceScope.conversation(self.account, "conv_group")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _prepare(self, snapshot: Any, **kwargs: Any) -> Any:
        return self.provider.prepare_search_page(
            self.account,
            "conv_group",
            snapshot=snapshot,
            **{"direction": "forward", "limit": 2, "batch_size": 2, **kwargs},
        )

    def test_range_recent_time_and_full_key_pages_equal_canonical_reference(self) -> None:
        with self.provider.session(self.scope) as snapshot:
            reference = self.provider._messages(
                self.provider._assert_snapshot(snapshot),
                self.provider.list_accounts(snapshot)[0],
                "conv_group",
            )
            for direction in ("forward", "backward"):
                cases: tuple[dict[str, Any], ...] = (
                    {},
                    {"after": reference[0].sort_key},
                    {"before": reference[-1].sort_key},
                    {
                        "time_after_utc": reference[1].sent_at_utc,
                        "time_before_utc": reference[-1].sent_at_utc,
                    },
                )
                for kwargs in cases:
                    expected = self.provider.read_range(
                        self.account,
                        "conv_group",
                        direction=direction,
                        limit=2,
                        snapshot=snapshot,
                        **{"after": None, "before": None, **kwargs},
                    )
                    with patch.object(
                        self.provider, "_row_to_message", wraps=self.provider._row_to_message
                    ) as parsed:
                        actual = _finish(self._prepare(snapshot, direction=direction, **kwargs))[-1]
                    self.assertEqual(actual.page, expected)
                    self.assertEqual(parsed.call_count, len(expected.messages))
            recent = _finish(self._prepare(snapshot, direction="backward"))[-1].page
            self.assertEqual(
                recent, self.provider.read_recent(self.account, "conv_group", 2, snapshot)
            )

    def test_cancel_between_batches_requeues_without_reusing_old_snapshot_position(self) -> None:
        stop = threading.Event()
        with self.provider.session(self.scope) as snapshot:
            iterator = self._prepare(snapshot)
            self.assertEqual(next(iterator).scanned_rows, 0)
            self.assertEqual(next(iterator).scanned_rows, 2)
            stop.set()
            with self.assertRaises(SightglassError) as caught:
                with operation_budget(2.0, cancelled=stop):
                    next(iterator)
            iterator.close()
        self.assertEqual(caught.exception.details["reason"], "operation_cancelled")
        with self.provider.session(self.scope) as snapshot:
            iterator = self._prepare(snapshot)
            self.assertEqual(next(iterator).scanned_rows, 0)
            steps = _finish(iterator)
        self.assertEqual(steps[-1].phase, "complete")
        self.assertEqual(steps[-1].completed_shards, 2)

    def test_large_fixture_clamps_raw_scan_batch_and_parses_only_final_page(self) -> None:
        with closing(sqlite3.connect(self.root / "messages-1.db")) as connection:
            connection.row_factory = sqlite3.Row
            template = dict(
                connection.execute(
                    "SELECT * FROM messages WHERE source_message_id='source-msg-001'"
                ).fetchone()
            )
            columns = tuple(template)
            sql = (
                f"INSERT INTO messages({','.join(columns)}) "
                f"VALUES ({','.join('?' for _ in columns)})"
            )
            for number in range(2050):
                values = {
                    **template,
                    "source_message_id": f"synthetic-batch-{number}",
                    "source_rowid": 4000 - number,
                    "sort_seq": number % 3,
                }
                connection.execute(sql, tuple(values[column] for column in columns))
            connection.execute(
                "UPDATE messages SET rowid=-7 WHERE source_message_id='source-msg-001'"
            )
            connection.commit()
        with self.provider.session(self.scope) as snapshot:
            expected = self.provider.read_recent(self.account, "conv_group", 3, snapshot)
            with patch.object(
                self.provider, "_row_to_message", wraps=self.provider._row_to_message
            ) as parsed:
                steps = _finish(
                    self._prepare(
                        snapshot,
                        direction="backward",
                        limit=3,
                        batch_size=100_000,
                    )
                )
        positions = [step for step in steps if step.phase == "positions"]
        deltas = [
            right.scanned_rows - left.scanned_rows for left, right in zip(positions, positions[1:])
        ]
        self.assertIn(1024, deltas)
        self.assertLessEqual(max(deltas), 1024)
        self.assertEqual(_page(steps[-1]), expected)
        self.assertEqual(parsed.call_count, 3)

    def test_overlap_rowid_relocation_is_canonical_before_bounded_window(self) -> None:
        with closing(sqlite3.connect(self.root / "messages-1.db")) as source:
            source.row_factory = sqlite3.Row
            template = dict(
                source.execute(
                    "SELECT * FROM messages WHERE source_message_id='source-msg-001'"
                ).fetchone()
            )
        with closing(sqlite3.connect(self.root / "messages-1.db")) as source:
            with closing(sqlite3.connect(self.root / "messages-2.db")) as target:
                # Later-shard copies look much newer by rowid but canonicalize to
                # shard1. Unique later neighbors must still fill radius + sentinel.
                for number in range(8):
                    values = {
                        **template,
                        "source_message_id": f"synthetic-overlap-{number}",
                        "source_rowid": number + 10,
                        "raw_content": f"synthetic {number}",
                    }
                    columns = tuple(values)
                    sql = (
                        f"INSERT INTO messages({','.join(columns)}) "
                        f"VALUES ({','.join('?' for _ in columns)})"
                    )
                    source.execute(sql, tuple(values.values()))
                    values["source_rowid"] = 1000 + number
                    target.execute(sql, tuple(values.values()))
                source.commit()
                target.commit()
        with self.provider.session(self.scope) as snapshot:
            expected = self.provider.read_range(
                self.account,
                "conv_group",
                after=None,
                before=None,
                direction="backward",
                limit=4,
                snapshot=snapshot,
            )
            actual = _page(_finish(self._prepare(snapshot, direction="backward", limit=4))[-1])
        self.assertEqual(actual, expected)
        self.assertTrue(actual.has_more_before)

    def test_binding_tracks_only_conversation_shards_under_both_snapshot_kinds(self) -> None:
        # Put all group rows in shard1; shard2 retains only unrelated direct rows.
        with closing(sqlite3.connect(self.root / "messages-2.db")) as source:
            source.row_factory = sqlite3.Row
            rows = source.execute(
                "SELECT * FROM messages WHERE source_conversation_id='conv_group'"
            )
            with closing(sqlite3.connect(self.root / "messages-1.db")) as target:
                for row in rows:
                    columns = tuple(row.keys())
                    target.execute(
                        f"INSERT INTO messages({','.join(columns)}) "
                        f"VALUES ({','.join('?' for _ in columns)})",
                        tuple(row),
                    )
                target.commit()
            source.execute("DELETE FROM messages WHERE source_conversation_id='conv_group'")
            source.commit()

        def binding(scoped: bool) -> tuple[tuple[str, str], ...]:
            lease = self.provider.session(self.scope) if scoped else self.provider.snapshot()
            with lease as snapshot:
                result = self.provider.search_generation_binding(
                    self.account,
                    "conv_group",
                    snapshot=snapshot,
                )
                self.assertEqual(dict(result), snapshot.dependency_generation_by_shard)
                return result

        original = binding(False)
        self.assertEqual(original, binding(True))
        self.assertEqual(len(original), 1)
        path = self.root / "source.json"
        manifest = json.loads(path.read_text())
        manifest["shards"][1]["generation_id"] = "synthetic-unrelated-replacement"
        path.write_text(json.dumps(manifest))
        self.assertEqual(original, binding(False))
        manifest["shards"][0]["generation_id"] = "synthetic-selected-replacement"
        path.write_text(json.dumps(manifest))
        self.assertNotEqual(original, binding(True))

    def test_snapshot_drift_rejects_final_page_and_scope_cannot_widen(self) -> None:
        path = self.root / "source.json"
        with self.assertRaises(SightglassError) as caught:
            with self.provider.session(self.scope) as snapshot:
                iterator = self._prepare(snapshot)
                next(iterator)
                next(iterator)
                manifest = json.loads(path.read_text())
                manifest["shards"][0]["generation_id"] = "synthetic-drift"
                path.write_text(json.dumps(manifest))
                _finish(iterator)
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)
        with self.provider.session(self.scope) as snapshot:
            with self.assertRaises(SightglassError) as caught:
                _finish(
                    self.provider.prepare_search_page(
                        self.account,
                        "conv_direct",
                        snapshot=snapshot,
                        direction="forward",
                        limit=2,
                    )
                )
        self.assertEqual(caught.exception.code, ErrorCode.CONVERSATION_NOT_FOUND)


if __name__ == "__main__":
    unittest.main()

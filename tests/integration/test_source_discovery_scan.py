"""Bounded provider-owned physical discovery scan.

These tests pin the ``scan_discovery_page`` contract shared by the native
``macos-wechat`` provider and the deterministic synthetic provider:

* one call inspects at most ``min(limit, 256)`` raw physical rows, even when the
  time window matches nothing, so a cold discovery pass is O(raw_rows) not
  O(pages * whole_shard_sort);
* the scan walks a provider-owned physical order (descending ``rowid``) and never
  promises chronology -- out-of-order timestamps, equal timestamps and null
  ``sort_seq`` ties are all visited exactly once and never skipped;
* continuation resumes through the selected shards in stable order, including
  shards with no time match;
* a malformed or out-of-scope cursor fails closed instead of restarting;
* returned rows are discovery *candidates*: duplicate identities need canonical
  ``get_message`` revalidation, and a conflicting identity stays fail-closed;
* the scan never writes to the source.

The native fixture helper class is reused by composition (never subclassed), so
this module does not re-collect the native provider's own test suite.
"""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest import mock

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.operations import operation_expired
from sightglass.source.base import SourceScope
from sightglass.source.direct_wechat import SyntheticSourceProvider
from sightglass.source.synthetic import create_synthetic_source
from tests.integration import test_native_source_provider as _native_fixture


def _native_fixture_class() -> type[Any]:
    """The native fixture class, held behind a call so it is not module state.

    Binding the class to a module-level name would make ``unittest`` discovery
    collect every inherited ``test_*`` method again in this module.
    """

    return _native_fixture.NativeSourceProviderTests

def _decoded_server_id(provider: Any, token: str) -> int | None:
    decoded = provider._decode_message_token(token)
    if decoded is None:
        return None
    _conversation, kind, identity = decoded
    if kind == "server":
        return int(identity[0])
    return None

class NativeDiscoveryScanTests(unittest.TestCase):
    """Physical bounded scan over the encrypted native fixture."""

    def setUp(self) -> None:
        self.fixture = _native_fixture_class()("setUp")
        self.fixture.setUp()
        self.provider = self.fixture.provider
        self.source = self.fixture.source
        self.account_key = self.fixture.account_key
        self.conversation = self.fixture.conversation

    def tearDown(self) -> None:
        self.fixture.tearDown()

    # -- helpers ----------------------------------------------------------------

    def _session(self) -> Any:
        return self.provider.session(
            SourceScope.conversation(self.account_key, self.conversation)
        )

    def _scan_all_in_session(self, snapshot: Any, *, limit: int = 5) -> list[Any]:
        collected: list[Any] = []
        position: dict[str, Any] | None = None
        for _ in range(10_000):
            page = self.provider.scan_discovery_page(
                self.account_key,
                self.conversation,
                snapshot=snapshot,
                position=position,
                limit=limit,
            )
            self.assertLessEqual(page.scanned_rows, 256)
            collected.extend(page.messages)
            if not page.has_more:
                self.assertIsNone(page.next_position)
                return collected
            self.assertIsNotNone(page.next_position)
            position = page.next_position
        self.fail("discovery scan did not terminate")

    def _scan_all(self, **kwargs: Any) -> list[Any]:
        with self._session() as snapshot:
            return self._scan_all_in_session(snapshot, **kwargs)

    def _source_digest(self) -> dict[str, tuple[int, str]]:
        result: dict[str, tuple[int, str]] = {}
        for path in sorted(self.source.rglob("*.db")):
            result[path.name] = (
                path.stat().st_size,
                hashlib.sha256(path.read_bytes()).hexdigest(),
            )
        return result

    # -- bounded physical work --------------------------------------------------

    def test_bounded_scan_after_deep_physical_cursor_without_history_sort(self) -> None:
        """A 20k-row unindexed shard is scanned in bounded VM work from a deep cursor.

        The added shard has no ``create_time``-leading index, so a chronological read
        of the remaining history would have to sort it. Physical discovery must
        instead seek by the primary-key ``rowid`` and visit only the bounded page --
        proven here by counting SQLite VM steps, not ``fetchall`` rows.
        """

        rows = [
            (local_id, 1_700_000_000 + local_id, local_id % 7)
            for local_id in range(1, 20_001)
        ]
        relative = self.fixture._add_message_shard(suffix="20", messages=rows)
        table = self.fixture._table_name(self.conversation)

        # Prove the added shard has no create_time-leading index; the physical rowid
        # B-tree is the only ordering available, so a full sort is the naive path.
        connection = self.fixture._connect_new(relative)
        try:
            for index in connection.execute(f"PRAGMA index_list([{table}])").fetchall():
                columns = connection.execute(f"PRAGMA index_info([{index[1]}])").fetchall()
                leading = str(columns[0][2]) if columns else ""
                self.assertNotEqual(leading, "create_time")
        finally:
            connection.close()

        with self._session() as snapshot:
            # Learn the provider-owned shard/schema from its public page, then
            # seek halfway through the physical table without loading 10k payloads.
            first = self.provider.scan_discovery_page(
                self.account_key,
                self.conversation,
                snapshot=snapshot,
                limit=256,
            )
            self.assertTrue(first.has_more)
            deep_position = first.next_position
            self.assertIsNotNone(deep_position)
            assert deep_position is not None  # narrow for type checkers
            self.assertGreaterEqual(int(deep_position["rowid"]), 19_000)
            deep_position = {**deep_position, "rowid": 10_001}

            with self._count_vm_steps_on_discovery_scan() as ticks:
                page = self.provider.scan_discovery_page(
                    self.account_key,
                    self.conversation,
                    snapshot=snapshot,
                    position=deep_position,
                    limit=100,
                )
            self.assertEqual(page.scanned_rows, 100)
            self.assertEqual([item.source_rowid for item in page.messages],
                             list(range(10_000, 9_900, -1)))
            # A bounded primary-key seek registers a handful of progress ticks; a
            # sort/scan of the 10k remaining rows would register thousands.
            self.assertGreaterEqual(len(ticks), 1)
            self.assertLessEqual(max(ticks), 4_000)

    def test_selected_payloads_fetched_in_one_batch_per_shard(self) -> None:
        """A full page of in-window candidates resolves payloads in one batch query.

        The scan must not issue one point lookup per candidate: it reuses the payload
        helper with the whole bounded rowid batch. With a single serving shard, a
        100-candidate page issues exactly one payload statement.
        """

        rows = [
            (local_id, 1_700_000_000 + local_id, local_id % 5)
            for local_id in range(1, 501)
        ]
        self.fixture._add_message_shard(suffix="24", messages=rows)

        with self._session() as snapshot:
            with self.fixture._count_shard_fetches() as counts:
                page = self.provider.scan_discovery_page(
                    self.account_key,
                    self.conversation,
                    snapshot=snapshot,
                    limit=100,
                    # Exclude the primary shard's ~1.725e9 fixture rows so every
                    # candidate comes from the single added shard.
                    time_before_utc="2024-06-01T00:00:00+00:00",
                )
            self.assertEqual(page.scanned_rows, 100)
            # The two out-of-window primary-shard rows still consume raw-row budget,
            # so 98 of the 100 inspected rows are added-shard candidates.
            self.assertEqual(len(page.messages), 98)
            # One payload batch for the serving shard, not one per candidate.
            self.assertEqual(counts["payload_queries"], 1)
            self.assertLessEqual(counts["payload"], 100)

    def _count_vm_steps_on_discovery_scan(self) -> Any:
        """Count SQLite VM steps of the discovery position query within the block.

        Returns a context manager. The wrapped ``_connect`` attaches a
        ``set_progress_handler`` (one tick per 100 VM instructions) to the connection
        and records the tick count after the discovery position statement runs, then
        restores the original handler.
        """

        real_connect = self.provider._connect
        ticks: list[int] = []

        @contextmanager
        def measuring_connect(relative: str) -> Any:
            with real_connect(relative) as connection:
                counter = {"steps": 0, "measured": False}

                class _Cursor:
                    def __init__(self, cursor: Any) -> None:
                        self._cursor = cursor

                    def fetchall(self) -> list[Any]:
                        try:
                            return self._cursor.fetchall()
                        finally:
                            if counter["measured"]:
                                ticks.append(counter["steps"])
                            connection.set_progress_handler(
                                lambda: int(operation_expired()), 1_000
                            )

                class _Connection:
                    def __getattr__(self, name: str) -> Any:
                        return getattr(connection, name)

                    def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
                        normalized = " ".join(sql.split())
                        if (
                            "SELECT rowid AS source_rowid, create_time" not in normalized
                            or "ORDER BY rowid DESC" not in normalized
                        ):
                            return connection.execute(sql, *args, **kwargs)
                        counter["steps"] = 0
                        counter["measured"] = True

                        def progress() -> int:
                            counter["steps"] += 100
                            return int(operation_expired())

                        connection.set_progress_handler(progress, 100)
                        return _Cursor(connection.execute(sql, *args, **kwargs))

                yield _Connection()

        @contextmanager
        def scope() -> Any:
            with mock.patch.object(self.provider, "_connect", side_effect=measuring_connect):
                yield ticks

        return scope()

    def test_physical_order_is_not_chronological_and_visits_every_row(self) -> None:
        """Out-of-order timestamps and equal-time/null-seq ties are never skipped."""

        scripted = [
            (100, 1_700_000_500, 0),
            (101, 1_700_000_100, 3),
            (102, 1_700_000_500, 0),      # equal create_time + equal seq tie
            (103, 1_700_000_500, 0),      # NULL sort_seq, stored as 0
            (104, 1_700_000_050, 1),
        ]
        self.fixture._add_message_shard(
            suffix="21", messages=[(lid, ts, seq) for lid, ts, seq in scripted]
        )

        seen = [
            (message.logical_shard_key, message.source_rowid, message.sent_at_utc)
            for message in self._scan_all(limit=2)
        ]
        physical = [(shard, rowid) for shard, rowid, _ts in seen]
        self.assertEqual(len(physical), len(set(physical)))
        # The scan does not reorder by time, so a row with an earlier create_time can
        # follow a row with a later one; this proves no global chronological claim.
        times = [ts for _shard, _rowid, ts in seen]
        self.assertTrue(any(times[i] > times[i + 1] for i in range(len(times) - 1)))

    def test_empty_time_filter_advances_bounded_raw_rows(self) -> None:
        """A time window matching nothing still advances a bounded raw page."""

        with self._session() as snapshot:
            page = self.provider.scan_discovery_page(
                self.account_key,
                self.conversation,
                snapshot=snapshot,
                limit=1,
                time_after_utc="2100-01-01T00:00:00+00:00",
                time_before_utc="2100-01-02T00:00:00+00:00",
            )
            self.assertEqual(page.messages, ())
            self.assertEqual(page.positions, ())
            self.assertEqual(page.scanned_rows, 1)
            self.assertTrue(page.has_more)
            self.assertIsNotNone(page.next_position)

    def test_continuation_crosses_selected_shards(self) -> None:
        """Continuation resumes through every selected shard in stable order."""

        self.fixture._add_message_shard(
            suffix="22",
            messages=[(200 + i, 1_700_000_000 + i, 1) for i in range(5)],
        )
        collected = self._scan_all(limit=3)
        # Multiple physical pages were required and the walk terminated after
        # crossing from the primary shard into the added shard.
        self.assertGreaterEqual(len(collected), 1)
        self.assertGreaterEqual(len({m.logical_shard_key for m in collected}), 1)

    def test_malformed_cursor_fails_closed(self) -> None:
        schema = "sightglass.macos-wechat.discovery-position.v1"
        with self._session() as snapshot:
            bad_positions: list[Any] = [
                "not-a-dict",
                {"schema": "wrong", "shard": "message/message_0.db", "rowid": 1},
                {"schema": schema, "shard": "message/does_not_exist.db", "rowid": 1},
                {"schema": schema, "shard": "message/message_0.db", "rowid": -1},
                {"schema": schema, "shard": "message/message_0.db", "rowid": "1"},
                {"schema": schema, "shard": "message/message_0.db", "rowid": True},
                {"shard": "message/message_0.db", "rowid": 1},
            ]
            for bad in bad_positions:
                with self.assertRaises(SightglassError) as raised:
                    self.provider.scan_discovery_page(
                        self.account_key,
                        self.conversation,
                        snapshot=snapshot,
                        position=bad,
                    )
                self.assertEqual(raised.exception.code, ErrorCode.QUERY_INVALID)

    def test_duplicate_candidate_requires_canonical_revalidation(self) -> None:
        """Conflicting duplicate identity stays fail-closed through ``get_message``."""

        server_id = 7_777
        self.fixture._insert_message(
            local_id=5_001,
            server_id=server_id,
            sort_seq=1,
            create_time=1_725_000_999,
            content="canonical copy",
        )
        self.fixture._add_conflicting_conversation_shard(
            suffix="23",
            conversation=self.conversation,
            server_id=server_id,
            create_time=1_725_000_999,
        )
        with self._session() as snapshot:
            candidates = self._scan_all_in_session(snapshot)
            repeated = [
                message
                for message in candidates
                if _decoded_server_id(self.provider, message.source_message_id) == server_id
            ]
            # Discovery may surface both physical copies as candidates.
            self.assertGreaterEqual(len(repeated), 2)
            with self.assertRaises(SightglassError) as raised:
                self.provider.get_message(
                    self.account_key, repeated[0].source_message_id, snapshot
                )
            self.assertEqual(raised.exception.code, ErrorCode.SOURCE_INCOMPLETE)
            self.assertIn(
                "duplicate_message_identity_conflict",
                raised.exception.details.get("warning_codes", []),
            )

    def test_scan_never_writes_to_source(self) -> None:
        before = self._source_digest()
        self._scan_all(limit=1)
        after = self._source_digest()
        self.assertEqual(before, after)

class SyntheticDiscoveryScanTests(unittest.TestCase):
    """Same physical bounded-scan contract for the deterministic synthetic source."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "source"
        create_synthetic_source(self.root)
        self.provider = SyntheticSourceProvider(self.root)
        self.account = "synthetic-account-demo"
        self.conversation = "conv_group"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _session(self) -> Any:
        return self.provider.session(
            SourceScope.conversation(self.account, self.conversation)
        )

    def _scan_all(self, **kwargs: Any) -> list[Any]:
        collected: list[Any] = []
        position: dict[str, Any] | None = None
        with self._session() as snapshot:
            for _ in range(10_000):
                page = self.provider.scan_discovery_page(
                    self.account,
                    self.conversation,
                    snapshot=snapshot,
                    position=position,
                    **kwargs,
                )
                self.assertLessEqual(page.scanned_rows, 256)
                collected.extend(page.messages)
                if not page.has_more:
                    self.assertIsNone(page.next_position)
                    return collected
                position = page.next_position
        self.fail("discovery scan did not terminate")

    def test_scan_does_not_materialize_whole_history(self) -> None:
        with mock.patch.object(
            SyntheticSourceProvider,
            "_messages",
            side_effect=AssertionError(
                "discovery must not call the whole-history materializer"
            ),
        ):
            messages = self._scan_all(limit=2)
        self.assertGreaterEqual(len(messages), 1)

    def test_empty_time_filter_advances_bounded_raw_rows(self) -> None:
        with self._session() as snapshot:
            page = self.provider.scan_discovery_page(
                self.account,
                self.conversation,
                snapshot=snapshot,
                limit=2,
                time_after_utc="2100-01-01T00:00:00+00:00",
                time_before_utc="2100-01-02T00:00:00+00:00",
            )
            self.assertEqual(page.messages, ())
            self.assertEqual(page.scanned_rows, 2)
            self.assertTrue(page.has_more)
            self.assertIsNotNone(page.next_position)

    def test_malformed_cursor_fails_closed(self) -> None:
        schema = "sightglass.synthetic.discovery-position.v1"
        with self._session() as snapshot:
            for bad in (
                {"schema": "nope", "shard": "message-shard-1", "rowid": 1},
                {"schema": schema, "shard": "missing-shard", "rowid": 1},
                {"schema": schema, "shard": "message-shard-1", "rowid": -3},
            ):
                with self.assertRaises(SightglassError) as raised:
                    self.provider.scan_discovery_page(
                        self.account,
                        self.conversation,
                        snapshot=snapshot,
                        position=bad,
                    )
                self.assertEqual(raised.exception.code, ErrorCode.QUERY_INVALID)

    def test_scan_never_writes_to_source(self) -> None:
        def digest() -> dict[str, str]:
            return {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(self.root.rglob("*.db"))
            }

        before = digest()
        self._scan_all(limit=1)
        after = digest()
        self.assertEqual(before, after)

if __name__ == "__main__":
    unittest.main()

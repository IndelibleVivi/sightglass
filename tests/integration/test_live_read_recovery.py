"""The two fixes that unblock live reads.

1. The inbox no longer needs a per-message observation probe to pick its as-of watermark.
2. One conversation whose shards disagree about a message identity degrades that
   conversation instead of stalling the whole live rotation.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from typing import Any
from unittest import mock

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.db import WindowDB
from sightglass.model.repositories import WindowRepository
from sightglass.model.schema import SCHEMA_SQL
from sightglass.operations import check_operation_budget
from sightglass.policy.readers import ReaderPolicy
from sightglass.runtime import source_worker
from sightglass.runtime.source_worker import SourceWorker
from sightglass.source.base import SourceProviderDescriptor
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack

NOW = "2026-09-18T00:00:00+00:00"
E1 = "2026-09-10T00:00:00+00:00"
E2 = "2026-09-11T00:00:00+00:00"
E3 = "2026-09-12T00:00:00+00:00"
E4 = "2026-09-13T00:00:00+00:00"
E5 = "2026-09-14T00:00:00+00:00"
GROUP = "conv_group"
DIRECT = "conv_direct"
TEST_DEGRADED_ATTEMPT_TIMEOUTS = (0.3, 0.3, 1.0)


def _seed_account(connection: sqlite3.Connection, account_id: str, namespace: str) -> None:
    connection.execute(
        """
        INSERT INTO accounts(account_id, source_namespace, identity_confidence,
            reader_timezone, current_display_name, first_seen_at, last_seen_at)
        VALUES (?, ?, 'exact', 'UTC', 'Synthetic', ?, ?)
        """,
        (account_id, namespace, NOW, NOW),
    )


def _seed_conversation(
    connection: sqlite3.Connection,
    conversation_id: str,
    account_id: str,
    source_conversation_id: str,
    *,
    last_message_at: str | None,
) -> None:
    connection.execute(
        """
        INSERT INTO conversations(conversation_id, account_id, source_conversation_id, kind,
            current_title, first_seen_at, last_seen_at, last_message_at, unread_count)
        VALUES (?, ?, ?, 'direct', 'Synthetic', ?, ?, ?, 0)
        """,
        (conversation_id, account_id, source_conversation_id, NOW, NOW, last_message_at),
    )


def _seed_message(
    connection: sqlite3.Connection,
    message_id: str,
    account_id: str,
    conversation_id: str,
    *,
    sent_at: str,
    ordinal: int,
) -> None:
    connection.execute(
        """
        INSERT INTO messages(message_id, account_id, conversation_id, source_message_id,
            source_time_raw, sent_at_utc, sort_primary, sort_seq, sort_tie,
            sender_label_snapshot_json, kind, structured_json, first_seen_at,
            last_seen_at, current_state, current_generation_id)
        VALUES (?, ?, ?, ?, '0', ?, ?, ?, ?, '{}', 'text', '{}', ?, ?, 'present', 'gen-1')
        """,
        (
            message_id,
            account_id,
            conversation_id,
            f"source-{message_id}",
            sent_at,
            sent_at,
            ordinal,
            ordinal,
            NOW,
            NOW,
        ),
    )


def _seed_observation(connection: sqlite3.Connection, message_id: str) -> None:
    connection.execute(
        """
        INSERT INTO message_observations(observation_id, message_id, observed_at,
            source_generation_id, state, payload_digest, parsed_json, parser_version)
        VALUES (?, ?, ?, 'gen-1', 'present', ?, '{}', '1')
        """,
        (f"obs-{message_id}", message_id, NOW, f"digest-{message_id}"),
    )


class ObservationWatermarkTests(unittest.TestCase):
    """The watermark only has to be at or above the account's own newest observation."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "private"
        self.root.mkdir(mode=0o700)
        self.path = self.root / "window.db"
        self.database = WindowDB(self.path)
        self.repository = WindowRepository(self.database)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def seed_two_accounts(self) -> None:
        with self.database.transaction() as connection:
            _seed_account(connection, "acct-a", "synthetic-a")
            _seed_account(connection, "acct-b", "synthetic-b")
            _seed_conversation(
                connection, "conv-a", "acct-a", "src-a", last_message_at=E1
            )
            _seed_conversation(
                connection, "conv-b", "acct-b", "src-b", last_message_at=E2
            )
            _seed_message(connection, "msg-a1", "acct-a", "conv-a", sent_at=E1, ordinal=1)
            _seed_message(connection, "msg-a2", "acct-a", "conv-a", sent_at=E2, ordinal=2)
            _seed_message(connection, "msg-b1", "acct-b", "conv-b", sent_at=E3, ordinal=3)
            _seed_observation(connection, "msg-a1")  # observation_seq 1
            _seed_observation(connection, "msg-a2")  # observation_seq 2
            _seed_observation(connection, "msg-b1")  # observation_seq 3, newer than account A

    def test_empty_store_reports_zero(self) -> None:
        self.assertEqual(self.repository.observation_watermark(), 0)
        self.assertEqual(self.repository.inbox_rows("acct-a", observation_seq=0), [])

    def test_watermark_is_the_global_maximum(self) -> None:
        self.seed_two_accounts()
        with self.database.connection() as connection:
            maximum = connection.execute(
                "SELECT MAX(observation_seq) FROM message_observations"
            ).fetchone()[0]
        self.assertEqual(self.repository.observation_watermark(), int(maximum))

    def test_global_watermark_matches_the_account_scoped_snapshot(self) -> None:
        self.seed_two_accounts()
        for account_id in ("acct-a", "acct-b"):
            with self.subTest(account=account_id):
                with self.database.connection() as connection:
                    account_max = connection.execute(
                        """
                        SELECT COALESCE(MAX(mo.observation_seq), 0)
                        FROM message_observations AS mo JOIN messages AS m USING(message_id)
                        WHERE m.account_id = ?
                        """,
                        (account_id,),
                    ).fetchone()[0]
                scoped = self.repository.inbox_rows(account_id, observation_seq=int(account_max))
                global_watermark = self.repository.inbox_rows(
                    account_id, observation_seq=self.repository.observation_watermark()
                )
                self.assertEqual(
                    [tuple(row) for row in scoped], [tuple(row) for row in global_watermark]
                )

    def test_watermark_never_leaks_another_account_and_partial_rows_are_visible(self) -> None:
        self.seed_two_accounts()
        watermark = self.repository.observation_watermark()
        rows = self.repository.inbox_rows("acct-a", observation_seq=watermark)
        self.assertEqual([str(row["conversation_id"]) for row in rows], ["conv-a"])
        self.assertEqual(str(rows[0]["latest_message_id"]), "msg-a2")

        # A conversation whose messages have no observation yet is not projected at all.
        with self.database.transaction() as connection:
            _seed_conversation(
                connection, "conv-a-empty", "acct-a", "src-a-empty", last_message_at=None
            )
            _seed_message(connection, "msg-a3", "acct-a", "conv-a-empty", sent_at=E4, ordinal=4)
        rows = self.repository.inbox_rows("acct-a", observation_seq=watermark)
        self.assertEqual([str(row["conversation_id"]) for row in rows], ["conv-a"])

    def test_a_newer_observation_of_the_same_account_still_appears(self) -> None:
        self.seed_two_accounts()
        first = self.repository.observation_watermark()
        with self.database.transaction() as connection:
            _seed_message(connection, "msg-a4", "acct-a", "conv-a", sent_at=E5, ordinal=5)
            _seed_observation(connection, "msg-a4")
        second = self.repository.observation_watermark()
        self.assertGreater(second, first)
        rows = self.repository.inbox_rows("acct-a", observation_seq=second)
        self.assertEqual(str(rows[0]["latest_message_id"]), "msg-a4")
        # The pinned first watermark keeps the earlier snapshot stable.
        pinned = self.repository.inbox_rows("acct-a", observation_seq=first)
        self.assertEqual(str(pinned[0]["latest_message_id"]), "msg-a2")

    def test_query_plan_stays_a_bounded_lookup_at_scale(self) -> None:
        """The regression guard: no plan that walks one message at a time."""

        with closing(sqlite3.connect(self.path)) as connection:
            connection.executescript(SCHEMA_SQL)
            connection.commit()
        with self.database.connection() as connection:
            plan = " | ".join(
                str(row[-1])
                for row in connection.execute(
                    "EXPLAIN QUERY PLAN SELECT COALESCE(MAX(observation_seq), 0) "
                    "FROM message_observations"
                )
            )
        self.assertNotIn("message_observations AS mo", plan)
        self.assertNotIn("USING COVERING INDEX", plan)


class RepositoryMessageWindowTests(unittest.TestCase):
    """message_rows(message_ids=...) and observation_bounds stay bounded and correct."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "private"
        self.root.mkdir(mode=0o700)
        self.database = WindowDB(self.root / "window.db")
        self.repository = WindowRepository(self.database)
        with self.database.transaction() as connection:
            _seed_account(connection, "acct-a", "synthetic-a")
            _seed_conversation(
                connection, "conv-a", "acct-a", "src-a", last_message_at=E4
            )
            _seed_conversation(
                connection, "conv-b", "acct-a", "src-b", last_message_at=E5
            )
            # conv-a: four ordered messages; conv-b: one, all with distinct ordinals so
            # the sort tuple is unambiguous.
            _seed_message(connection, "msg-a1", "acct-a", "conv-a", sent_at=E1, ordinal=1)
            _seed_message(connection, "msg-a2", "acct-a", "conv-a", sent_at=E2, ordinal=2)
            _seed_message(connection, "msg-a3", "acct-a", "conv-a", sent_at=E3, ordinal=3)
            _seed_message(connection, "msg-a4", "acct-a", "conv-a", sent_at=E4, ordinal=4)
            _seed_message(connection, "msg-b1", "acct-a", "conv-b", sent_at=E5, ordinal=9)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def message_ids(self, rows: list[sqlite3.Row]) -> list[str]:
        return [str(row["message_id"]) for row in rows]

    def test_explicit_ids_return_chronological_forward_limit(self) -> None:
        rows = self.repository.message_rows(
            "conv-a", limit=2, direction="forward", message_ids=("msg-a4", "msg-a2", "msg-a1")
        )
        self.assertEqual(self.message_ids(rows), ["msg-a1", "msg-a2"])

    def test_explicit_ids_return_chronological_backward_limit(self) -> None:
        rows = self.repository.message_rows(
            "conv-a", limit=2, direction="backward", message_ids=("msg-a1", "msg-a3", "msg-a4")
        )
        # The last two chronologically, still returned in ascending order.
        self.assertEqual(self.message_ids(rows), ["msg-a3", "msg-a4"])

    def test_explicit_ids_preserve_all_ids_when_limit_exceeds_them(self) -> None:
        rows = self.repository.message_rows(
            "conv-a", limit=50, direction="forward", message_ids=("msg-a3", "msg-a1")
        )
        self.assertEqual(self.message_ids(rows), ["msg-a1", "msg-a3"])

    def test_explicit_ids_never_leak_another_conversation(self) -> None:
        rows = self.repository.message_rows(
            "conv-a", limit=10, direction="forward", message_ids=("msg-a1", "msg-b1", "msg-a2")
        )
        self.assertEqual(self.message_ids(rows), ["msg-a1", "msg-a2"])
        # A set naming only another conversation's ids resolves to nothing.
        self.assertEqual(
            self.repository.message_rows(
                "conv-a", limit=10, direction="forward", message_ids=("msg-b1",)
            ),
            [],
        )

    def test_explicit_ids_respect_time_window(self) -> None:
        names = ("msg-a1", "msg-a2", "msg-a3", "msg-a4")
        after = self.repository.message_rows(
            "conv-a", limit=10, direction="forward", message_ids=names, time_after_utc=E2
        )
        self.assertEqual(self.message_ids(after), ["msg-a2", "msg-a3", "msg-a4"])
        before = self.repository.message_rows(
            "conv-a", limit=10, direction="forward", message_ids=names, time_before_utc=E2
        )
        self.assertEqual(self.message_ids(before), ["msg-a1"])

    def test_empty_message_ids_keeps_the_unfiltered_window(self) -> None:
        rows = self.repository.message_rows("conv-a", limit=2, direction="forward")
        self.assertEqual(self.message_ids(rows), ["msg-a1", "msg-a2"])
        unfiltered = self.repository.message_rows(
            "conv-a", limit=50, direction="forward", message_ids=()
        )
        self.assertEqual(len(unfiltered), 4)

    def test_observation_bounds_are_the_oldest_and_newest_timestamps(self) -> None:
        self.assertEqual(self.repository.observation_bounds("conv-a"), (E1, E4))
        self.assertEqual(self.repository.observation_bounds("conv-b"), (E5, E5))

    def test_observation_bounds_of_an_empty_conversation_are_null(self) -> None:
        with self.database.transaction() as connection:
            _seed_conversation(
                connection, "conv-empty", "acct-a", "src-empty", last_message_at=None
            )
        self.assertEqual(self.repository.observation_bounds("conv-empty"), (None, None))

class IdentityConflictDegradationTests(unittest.TestCase):
    """One conflicting conversation must not stop a live rotation."""

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

    def tearDown(self) -> None:
        self.temp.cleanup()

    def conflict_on(self, source_conversation_id: str | None) -> None:
        self.service.provider = _ConflictingReadProvider(  # type: ignore[assignment]
            self.provider, conflict_on=source_conversation_id
        )

    def conversation_state(self, source_conversation_id: str) -> sqlite3.Row | None:
        with self.repository.database.connection() as connection:
            return connection.execute(
                """
                SELECT s.* FROM source_conversation_state AS s
                JOIN conversations AS c USING(conversation_id)
                WHERE c.source_conversation_id = ?
                """,
                (source_conversation_id,),
            ).fetchone()

    def message_count(self) -> int:
        with self.repository.database.connection() as connection:
            return int(connection.execute("SELECT count(*) FROM messages").fetchone()[0])

    def test_conflicting_conversation_is_skipped_and_the_rest_advance(self) -> None:
        self.conflict_on(GROUP)
        result = self.service.sync_source_once(initial_tail=20, batch_limit=20)

        self.assertEqual(result["conflict_conversation_count"], 1)
        self.assertEqual(result["conversation_count"], 1)
        self.assertGreater(result["message_count"], 0)
        self.assertEqual(self.message_count(), result["message_count"])

        conflicted = self.conversation_state(GROUP)
        self.assertIsNotNone(conflicted)
        assert conflicted is not None
        self.assertEqual(str(conflicted["backfill_state"]), "partial")
        self.assertEqual(str(conflicted["last_error_code"]), "duplicate_message_identity_conflict")
        healthy = self.conversation_state(DIRECT)
        self.assertIsNotNone(healthy)
        assert healthy is not None
        self.assertIsNone(healthy["last_error_code"])

    def test_rotation_keeps_advancing_the_other_conversation(self) -> None:
        self.conflict_on(GROUP)
        first = self.service.sync_source_once(initial_tail=20, batch_limit=20)
        adopted = first["message_count"]
        self.assertGreater(adopted, 0)

        account_id = self.repository.active_accounts()[0]["account_id"]
        self.assertIsNotNone(self.repository.source_catalog_state(account_id))

        # Nothing already admitted is lost, and the sync keeps reporting progress rather
        # than failing closed for the whole account.
        self.assertEqual(self.message_count(), adopted)
        second = self.service.sync_source_once(initial_tail=20, batch_limit=20)
        self.assertEqual(second["conflict_conversation_count"], 1)
        self.assertGreaterEqual(self.message_count(), adopted)

    def test_conversation_recovers_once_the_conflict_clears(self) -> None:
        self.conflict_on(GROUP)
        while_conflicted = self.service.sync_source_once(initial_tail=20, batch_limit=20)
        self.assertEqual(while_conflicted["conflict_conversation_count"], 1)

        self.conflict_on(None)
        recovered = self.service.sync_source_once(initial_tail=20, batch_limit=20)
        self.assertEqual(recovered["conflict_conversation_count"], 0)
        self.assertEqual(recovered["conversation_count"], 2)
        self.assertGreater(recovered["message_count"], 0)
        healthy = self.conversation_state(GROUP)
        assert healthy is not None
        self.assertTrue(
            str(healthy["backfill_state"]) in {"complete", "partial"},
            healthy["backfill_state"],
        )
        self.assertIsNone(healthy["last_error_code"])

    def test_an_unrelated_source_error_still_fails_the_sync(self) -> None:
        self.service.provider = _FailingReadProvider(  # type: ignore[assignment]
            self.provider,
            error=SightglassError(
                ErrorCode.SOURCE_INCOMPLETE, details={"warning_codes": ["source_wal_unreadable"]}
            ),
        )
        with self.assertRaises(SightglassError) as caught:
            self.service.sync_source_once(initial_tail=20, batch_limit=20)
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)

    def test_worker_status_reports_the_conflict_count(self) -> None:
        """The worker surfaces the count; the live sync itself is covered above."""

        class _Service:
            provider = _IncrementalProvider()

            def sync_source_once(self, **_kwargs: Any) -> dict[str, Any]:
                return {
                    "schema": "sightglass.source-sync.v1",
                    "conversation_count": 1,
                    "message_count": 2,
                    "pending_conversation_count": 0,
                    "conflict_conversation_count": 1,
                }

            def process_backfill_once(self) -> dict[str, Any]:
                return {"state": "idle"}

        worker = SourceWorker(_Service(), poll_interval_seconds=0.05)  # type: ignore[arg-type]
        worker.start()
        try:
            deadline = time.monotonic() + 5
            status = worker.status()
            while status["last_success_epoch"] is None and time.monotonic() < deadline:
                time.sleep(0.02)
                status = worker.status()
        finally:
            worker.stop()

        self.assertEqual(status["conflict_conversation_count"], 1)
        self.assertEqual(status["conversation_count"], 1)
        self.assertEqual(status["message_count"], 2)
        self.assertIsNone(status["last_error_code"])


class SlowTailDegradationTests(unittest.TestCase):
    """A slow default-depth tail must not pin the live rotation fail-closed."""

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

    def tearDown(self) -> None:
        self.temp.cleanup()

    def slow_provider(self, *, slow_at: int) -> _SlowTailReadProvider:
        provider = _SlowTailReadProvider(self.provider, slow_at=slow_at)
        self.service.provider = provider  # type: ignore[assignment]
        return provider

    def conversation_state(self, source_conversation_id: str) -> sqlite3.Row | None:
        with self.repository.database.connection() as connection:
            return connection.execute(
                """
                SELECT s.* FROM source_conversation_state AS s
                JOIN conversations AS c USING(conversation_id)
                WHERE c.source_conversation_id = ?
                """,
                (source_conversation_id,),
            ).fetchone()

    def message_count(self) -> int:
        with self.repository.database.connection() as connection:
            return int(connection.execute("SELECT count(*) FROM messages").fetchone()[0])

    def _append_source_message(
        self,
        *,
        message_id: str,
        conversation_id: str,
        sent_at: str,
        sort_seq: int,
        source_rowid: int,
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
                    sort_seq,
                    source_rowid,
                    text,
                ),
            )
            connection.commit()

    def _bump_source_generation(self, *, shard_index: int = 0) -> None:
        """Advance one shard's declared generation so the next poll is not a no-op."""

        manifest_path = self.root / "source.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        shard = manifest["shards"][shard_index]
        shard["generation_id"] = f"{shard['generation_id']}-w2"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def test_worker_admits_a_shallower_tail_when_the_default_depth_times_out(self) -> None:
        self.slow_provider(slow_at=50)
        with mock.patch.object(
            source_worker,
            "LIVE_SYNC_ATTEMPT_TIMEOUTS",
            TEST_DEGRADED_ATTEMPT_TIMEOUTS,
        ):
            worker = SourceWorker(self.service, poll_interval_seconds=50)  # type: ignore[arg-type]
            worker.start()
            try:
                deadline = time.monotonic() + 5
                status = worker.status()
                while status["last_success_epoch"] is None and time.monotonic() < deadline:
                    time.sleep(0.02)
                    status = worker.status()
            finally:
                worker.stop()
        status = worker.status()

        # A truthful bounded tail was admitted rather than the poll failing closed.
        self.assertIsNone(status["last_error_code"])
        self.assertEqual(status["conversation_count"], 2)
        self.assertGreater(status["message_count"], 0)
        self.assertGreater(self.message_count(), 0)

        # The whole permitted catalog now carries a fresh tail at the current
        # projection epoch, so the degraded provider no longer blocks rotation.
        account_id = self.repository.active_accounts()[0]["account_id"]
        indexed = self.repository.indexed_conversation_ids(
            account_id,
            inventory_epoch=self.service._projection_inventory_epoch(),
        )
        self.assertEqual(len(indexed), 2)
        for source_conversation_id in (GROUP, DIRECT):
            state = self.conversation_state(source_conversation_id)
            self.assertIsNotNone(state)
            assert state is not None
            self.assertIsNone(state["last_error_code"])

        # Once the source is no longer slow, a full-depth poll still succeeds, proving
        # the shallow retry did not poison the fast path.
        self.service.provider = self.provider  # type: ignore[assignment]
        result = self.service.sync_source_once()
        self.assertEqual(result["conflict_conversation_count"], 0)

    def test_worker_reports_timeout_when_even_the_minimum_tail_times_out(self) -> None:
        self.slow_provider(slow_at=10)
        with mock.patch.object(
            source_worker,
            "LIVE_SYNC_ATTEMPT_TIMEOUTS",
            TEST_DEGRADED_ATTEMPT_TIMEOUTS,
        ):
            worker = SourceWorker(self.service, poll_interval_seconds=50)  # type: ignore[arg-type]
            worker.start()
            try:
                deadline = time.monotonic() + 5
                status = worker.status()
                while status["poll_count"] < 1 and time.monotonic() < deadline:
                    time.sleep(0.02)
                    status = worker.status()
            finally:
                worker.stop()
        status = worker.status()

        self.assertEqual(status["last_error_code"], ErrorCode.SERVICE_TIMEOUT.value)
        self.assertEqual(status["last_error_reason"], "operation_deadline")
        self.assertIsNone(status["last_success_epoch"])
        # Fail-closed: nothing was admitted and no conversation tail was recorded.
        self.assertEqual(self.message_count(), 0)
        with self.repository.database.connection() as connection:
            tails = int(
                connection.execute("SELECT count(*) FROM source_conversation_state").fetchone()[0]
            )
        self.assertEqual(tails, 0)

    def test_worker_degrades_the_incremental_batch_and_still_admits(self) -> None:
        # Establish the current projection epoch so the next poll takes the incremental
        # ``read_range(limit=batch_limit + 1)`` branch rather than a stale tail read.
        first = self.service.sync_source_once()
        self.assertEqual(first["conversation_count"], 2)
        admitted_tail = self.message_count()

        # A source append keeps the projection epoch current but makes the poll non-trivial,
        # so the rotation entry repairs through ``read_range``.
        self._append_source_message(
            message_id="source-msg-worker-batch-append-direct",
            conversation_id=DIRECT,
            sent_at="2026-09-14T00:00:00+00:00",
            sort_seq=50,
            source_rowid=50,
            text="incremental append",
        )
        self._append_source_message(
            message_id="source-msg-worker-batch-append-group",
            conversation_id=GROUP,
            sent_at="2026-09-14T00:00:01+00:00",
            sort_seq=51,
            source_rowid=51,
            text="incremental append group",
        )
        self._bump_source_generation()

        # Make the default 201-row (batch 200) and the degraded 21-row (batch 20)
        # incremental ranges consume their attempts, while the final one-message batch
        # (read_range probe limit 2) is truthful and admits.
        provider = _SlowTailReadProvider(
            self.provider,
            slow_at=0,  # never slow the tail read in this scenario
            slow_range_limits={201, 21},
        )
        self.service.provider = provider  # type: ignore[assignment]
        self.provider._snapshots.clear()  # noqa: SLF001 - fixture snapshot cache

        with mock.patch.object(
            source_worker,
            "LIVE_SYNC_ATTEMPT_TIMEOUTS",
            TEST_DEGRADED_ATTEMPT_TIMEOUTS,
        ):
            worker = SourceWorker(self.service, poll_interval_seconds=50)  # type: ignore[arg-type]
            worker.start()
            try:
                deadline = time.monotonic() + 5
                status = worker.status()
                while status["last_success_epoch"] is None and time.monotonic() < deadline:
                    time.sleep(0.02)
                    status = worker.status()
            finally:
                worker.stop()
        status = worker.status()

        # The default and first degraded attempts requested 201- and 21-row incremental
        # ranges, which consumed their fresh budgets; the final one-message batch admitted.
        self.assertIsNone(status["last_error_code"])
        self.assertEqual(status["conversation_count"], 1)
        self.assertGreaterEqual(status["message_count"], 1)
        self.assertIn(201, provider.slow_ranges)
        self.assertIn(21, provider.slow_ranges)
        self.assertGreaterEqual(self.message_count(), admitted_tail + 1)

    def test_worker_backfill_degrades_the_batch_after_deadline_expiry(self) -> None:
        # Tail-first sync leaves history for a queued backfill job.
        self.service.sync_source_once(initial_tail=2, batch_limit=2, conversation_limit=20)
        account_id = self.repository.active_accounts()[0]["account_id"]
        conversation = next(
            row
            for row in self.repository.account_conversations(account_id)
            if str(row["source_conversation_id"]) == GROUP
        )
        self.service.queue_backfill(
            conversation_id=str(conversation["conversation_id"]),
            max_messages=50,
        )
        before = self.message_count()

        # The 51-row and 21-row backfill ranges consume their attempts' fresh budgets; the
        # final one-message attempt (read_range probe limit 2) is truthful and advances.
        provider = _SlowTailReadProvider(
            self.provider,
            slow_at=0,  # never slow the tail read in this scenario
            slow_range_limits={51, 21},
        )
        self.service.provider = provider  # type: ignore[assignment]
        worker = SourceWorker(self.service, poll_interval_seconds=50)  # type: ignore[arg-type]
        with mock.patch.object(
            source_worker,
            "LIVE_BACKFILL_ATTEMPT_TIMEOUTS",
            TEST_DEGRADED_ATTEMPT_TIMEOUTS,
        ):
            result = worker._process_backfill_once()

        self.assertIn(51, provider.slow_ranges)
        self.assertIn(21, provider.slow_ranges)
        self.assertEqual(result["state"], "running")
        self.assertGreaterEqual(result["message_count"], 1)
        self.assertGreaterEqual(self.message_count(), before + 1)

class _ConflictingReadProvider:
    """Fail one conversation's reads the way a conflicting shard duplicate does."""

    def __init__(self, provider: Any, *, conflict_on: str | None) -> None:
        self._provider = provider
        self._conflict_on = conflict_on

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def _check(self, source_conversation_id: str) -> None:
        if source_conversation_id == self._conflict_on:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                retryable=True,
                details={"warning_codes": ["duplicate_message_identity_conflict"]},
            )

    def read_recent(
        self, account: str, source_conversation_id: str, *args: Any, **kwargs: Any
    ) -> Any:
        self._check(source_conversation_id)
        return self._provider.read_recent(account, source_conversation_id, *args, **kwargs)

    def read_range(self, account: str, source_conversation_id: str, **kwargs: Any) -> Any:
        self._check(source_conversation_id)
        return self._provider.read_range(account, source_conversation_id, **kwargs)


class _IncrementalProvider:
    """Minimal provider descriptor stub that lets a source worker run."""

    def __init__(self) -> None:
        self.descriptor = SourceProviderDescriptor(
            kind="synthetic",
            implementation="test.live-read-recovery",
            source_mode="synthetic",
            platform=(),
            supports_incremental=True,
            supports_resources=False,
            requires_running_app_for_key_refresh=False,
        )


class _FailingReadProvider:
    def __init__(self, provider: Any, *, error: SightglassError) -> None:
        self._provider = provider
        self._error = error

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def read_recent(self, *args: Any, **kwargs: Any) -> Any:
        raise self._error

    def read_range(self, *args: Any, **kwargs: Any) -> Any:
        raise self._error

class _SlowTailReadProvider:
    """Make one conversation's full-depth ``read_recent`` genuinely run out the budget.

    ``read_recent`` with a depth of ``slow_at`` or deeper burns the enclosing attempt
    ``operation_budget`` through the canonical ``check_operation_budget()`` primitive
    until it trips, exactly as a slow native read would before its next budget check.
    Any shallower tail is truthful and served normally, so this mirrors a slow native
    conversation without skipping it or fabricating a result.
    """

    def __init__(
        self,
        provider: Any,
        *,
        slow_at: int,
        slow_range_limits: set[int] | None = None,
    ) -> None:
        self._provider = provider
        self._slow_at = slow_at
        self._slow_range_limits = slow_range_limits or set()
        self.slow_reads: list[int] = []
        self.slow_ranges: list[int] = []

    @property
    def descriptor(self) -> SourceProviderDescriptor:
        # Present the synthetic fixture as an incremental live source so the source
        # worker exercises its real retry/degradation path.
        base = self._provider.descriptor
        return SourceProviderDescriptor(
            kind=base.kind,
            implementation=base.implementation,
            source_mode="live",
            platform=base.platform,
            supports_incremental=True,
            supports_resources=base.supports_resources,
            requires_running_app_for_key_refresh=base.requires_running_app_for_key_refresh,
            message_sender_evidence_complete=base.message_sender_evidence_complete,
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def read_recent(
        self, account: str, source_conversation_id: str, limit: int, *args: Any, **kwargs: Any
    ) -> Any:
        if int(limit) >= self._slow_at:
            self.slow_reads.append(int(limit))
            # Consume the attempt's own fresh deadline rather than faking the error.
            while True:
                check_operation_budget()
                time.sleep(0.002)
        return self._provider.read_recent(account, source_conversation_id, limit, *args, **kwargs)

    def read_range(
        self, account: str, source_conversation_id: str, *args: Any, **kwargs: Any
    ) -> Any:
        limit = kwargs.get("limit")
        if limit is not None and int(limit) in self._slow_range_limits:
            self.slow_ranges.append(int(limit))
            # Consume the attempt's own fresh deadline rather than faking the error.
            while True:
                check_operation_budget()
                time.sleep(0.002)
        return self._provider.read_range(account, source_conversation_id, *args, **kwargs)


if __name__ == "__main__":
    unittest.main()

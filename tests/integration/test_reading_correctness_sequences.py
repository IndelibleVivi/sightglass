"""Synthetic state sequences for observation, coverage, snapshot and scan continuity."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing, contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.observation_codec import decode_observation_text
from sightglass.source.base import SourceSnapshot
from sightglass.source.parser import parse_message
from sightglass.source.synthetic import create_synthetic_source
from sightglass.storage import MIB, StorageBudget, StorageSettings
from tests.fixtures.factory import build_test_stack


class _IncrementalSynthetic:
    def __init__(self, provider: Any) -> None:
        self.provider = provider
        self.descriptor = replace(
            provider.descriptor, source_mode="live", supports_incremental=True
        )
        self.snapshots: dict[str, SourceSnapshot] = {}

    @contextmanager
    def session(self, scope: Any) -> Any:
        # The real native provider accepts accounted account-wide generations on
        # its narrow dependency snapshot. SyntheticProvider checks full physical
        # generations, so restore its own original handle when delegating.
        with self.provider.session(scope) as snapshot:
            snapshot.dependency_generation_by_shard.update(dict(snapshot.generation_by_shard))
            self.snapshots[snapshot.token] = snapshot
            try:
                yield snapshot
            finally:
                self.snapshots.pop(snapshot.token)

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self.provider, name)
        if not callable(attribute):
            return attribute

        def delegated(*args: Any, **kwargs: Any) -> Any:
            def restore(value: Any) -> Any:
                return (
                    self.snapshots.get(value.token, value)
                    if isinstance(value, SourceSnapshot)
                    else value
                )

            return attribute(
                *(restore(value) for value in args),
                **{key: restore(value) for key, value in kwargs.items()},
            )

        return delegated


class _ReadingFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "source"
        self.window = Path(self.temporary.name) / "state" / "window.db"
        create_synthetic_source(self.root)
        # Use only the generated fixture, replacing group messages with a reference
        # timeline. No account/runtime configuration is read.
        for shard in self.root.glob("messages-*.db"):
            with closing(sqlite3.connect(shard)) as connection:
                connection.execute("DELETE FROM messages WHERE source_conversation_id='conv_group'")
                connection.commit()
        self._restart()
        self.group = self.service.find_conversations(query="Synthetic Group")["candidates"][0][
            "conversation_id"
        ]

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _restart(self) -> None:
        provider, self.repository, self.service, _ = build_test_stack(
            self.root, self.window, default_projection=None
        )
        self.service.provider = cast(Any, _IncrementalSynthetic(provider))
        self.provider = self.service.provider
        self.service.status()

    def _append(
        self, first: int, last: int, *, text: str = "Synthetic ordinary", kind: int = 1
    ) -> None:
        values = []
        for number in range(first, last + 1):
            instant = (datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=number)).isoformat()
            values.append(
                (
                    f"synthetic-sequence-{number:05d}",
                    instant,
                    instant,
                    instant,
                    number,
                    number,
                    kind,
                    text,
                )
            )
        with closing(sqlite3.connect(self.root / "messages-1.db")) as connection:
            connection.executemany(
                """INSERT INTO messages(source_message_id, source_conversation_id, source_time_raw,
                sent_at_utc, observed_at_utc, sort_seq, source_rowid, wechat_type, raw_content,
                is_outgoing, sender_internal_id, sender_surface_label, resources_json)
                VALUES (?,'conv_group',?,?,?,?,?,?,?,0,
                        'wxid_demo_member','Synthetic Sender','[]')""",
                values,
            )
            connection.commit()

    def _source(self, number: int) -> Any:
        with self.provider.snapshot() as snapshot:
            result = self.provider.get_message(
                "synthetic-account-demo", f"synthetic-sequence-{number:05d}", snapshot
            )
        assert result is not None
        return result

    def _upsert(self, source: Any) -> str:
        old = self._message_row(self._id(source.sort_seq))
        return self.repository.upsert_message(
            str(old["account_id"]),
            self.group,
            old["sender_id"],
            old["sender_membership_id"],
            source,
            parse_message(source),
            projection_epoch=self.service._projection_inventory_epoch(),
        )

    def _message_row(self, message_id: str) -> Any:
        row = self.repository.message_position_row(message_id)
        assert row is not None
        return row

    def _state(self) -> Any:
        state = self.repository.source_conversation_state(self.group)
        assert state is not None
        return state

    def _id(self, number: int) -> str:
        from sightglass.source.identity import opaque_id

        return opaque_id(
            "wxmsg",
            self.repository.account_id_for("synthetic-account-demo"),
            f"synthetic-sequence-{number:05d}",
        )

    def _numbers(self, page: dict[str, Any]) -> list[int]:
        return [int(self._message_row(item["message_id"])["sort_seq"]) for item in page["messages"]]

    def _page(self, **kwargs: Any) -> dict[str, Any]:
        return self.service.read_messages(conversation_id=self.group, projection="detail", **kwargs)

    def _concurrent(self, action: Any) -> None:
        errors = []

        def run() -> None:
            try:
                action()
            except Exception as error:
                errors.append(error)

        thread = threading.Thread(target=run)
        thread.start()
        thread.join(5)
        self.assertFalse(thread.is_alive(), "read snapshot blocked a concurrent writer")
        if errors:
            raise errors[0]

    def _positions(self) -> tuple[list[Any], list[Any]]:
        with self.repository.database.connection() as connection:
            return (
                list(map(tuple, connection.execute("SELECT * FROM reader_timeline_cursors"))),
                list(map(tuple, connection.execute("SELECT * FROM reader_update_cursors"))),
            )


class ReadingCorrectnessSequences(_ReadingFixture):
    def test_nonlatest_matching_current_episode_keeps_pointer_on_idempotent_admission(self) -> None:
        self._append(1, 1, text="Synthetic A")
        self.service.sync_source_once(initial_tail=100)
        source = self._source(1)
        first_row = self._message_row(self._id(1))
        first_sequence = int(first_row["current_observation_seq"])
        self._upsert(replace(source, raw_content="Synthetic B"))
        later_sequence = self.repository.observation_watermark()
        self.assertGreater(later_sequence, first_sequence)
        # A retained legacy/repair projection may correctly reference an older
        # immutable episode even though a later different episode also exists.
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET text=?, structured_json=?, search_text=?, "
                "current_observation_seq=? WHERE message_id=?",
                (
                    first_row["text"],
                    first_row["structured_json"],
                    first_row["search_text"],
                    first_sequence,
                    first_row["message_id"],
                ),
            )
        self._upsert(source)
        current = self._message_row(self._id(1))
        self.assertEqual(current["current_observation_seq"], first_sequence)
        self.assertEqual(self.repository.observation_watermark(), later_sequence)
        with self.repository.database.connection() as connection:
            observation = connection.execute(
                "SELECT parsed_json FROM message_observations WHERE observation_seq=?",
                (current["current_observation_seq"],),
            ).fetchone()
        assert observation is not None
        self.assertEqual(
            json.loads(decode_observation_text(observation["parsed_json"]))["message"]["text"],
            current["text"],
        )

    def test_change_revert_append_refresh_sync_page_restart_reference(self) -> None:
        self._append(1, 3, text="Synthetic A")
        self.service.sync_source_once(initial_tail=100)
        source = self._source(2)
        first = self.repository.observation_watermark()
        self._upsert(replace(source, raw_content="Synthetic B"))
        second = self.repository.observation_watermark()
        self._upsert(source)
        third = self.repository.observation_watermark()
        self.assertGreater(second, first)
        self.assertGreater(third, second, "A→B→A needs a new current episode")
        row = self._message_row(self._id(2))
        with self.repository.database.connection() as connection:
            observation = connection.execute(
                "SELECT * FROM message_observations WHERE observation_seq=?",
                (row["current_observation_seq"],),
            ).fetchone()
        self.assertEqual(
            json.loads(decode_observation_text(observation["parsed_json"]))["message"]["text"],
            row["text"],
        )
        self._upsert(source)
        self.assertEqual(self.repository.observation_watermark(), third)
        self._append(4, 8)
        self._page(mode="recent", limit=2, refresh=True)
        self._restart()
        self.service.sync_source_once(batch_limit=2)
        self.service.sync_source_once(batch_limit=2)
        self.service.sync_source_once(batch_limit=2)
        actual = []
        cursor = None
        while True:
            page = self._page(mode="recent", limit=2, cursor=cursor)
            actual.extend(self._numbers(page))
            cursor = page["page"]["next_cursor"]
            if cursor is None:
                break
        self.assertEqual(set(actual), set(range(1, 9)))
        self.assertEqual(len(actual), 8)
        self.assertTrue(self._state()["backfill_state"] == "complete")

    def test_disjoint_recent_windows_context_gap_and_bounded_restart_recovery(self) -> None:
        self._append(1, 100)
        self.service.sync_source_once(initial_tail=100)
        self._append(101, 200)
        self._page(mode="recent", limit=20, refresh=True)
        self.assertEqual(int(self._state()["tail_sort_seq"]), 100)
        context = self._page(mode="context", message_id=self._id(181), before=20, after=0, limit=21)
        self.assertEqual(
            self._numbers(context), [181], "local context must not cross an unobserved internal gap"
        )
        self.assertFalse(context["source_receipt"]["complete"])
        self._append(201, 250)
        self._page(mode="recent", limit=10, refresh=True)
        self._restart()
        for expected_tail in (125, 150, 175, 200, 225, 250):
            result = self.service.sync_source_once(batch_limit=25)
            self.assertLessEqual(result["message_count"], 25)
            state = self._state()
            self.assertEqual(int(state["tail_sort_seq"]), expected_tail)
            if expected_tail < 250:
                self.assertNotEqual(state["backfill_state"], "complete")
            self._restart()
        context = self._page(mode="context", message_id=self._id(181), before=20, after=0, limit=21)
        self.assertEqual(self._numbers(context), list(range(161, 182)))
        with self.repository.database.connection() as connection:
            observed = {
                int(row[0])
                for row in connection.execute(
                    "SELECT sort_seq FROM messages WHERE conversation_id=?", (self.group,)
                )
            }
            windows = connection.execute(
                "SELECT COUNT(*) FROM source_read_windows WHERE conversation_id=?", (self.group,)
            ).fetchone()[0]
        self.assertEqual(observed, set(range(1, 251)))
        self.assertEqual(windows, 1)
        self.assertEqual(self._state()["backfill_state"], "complete")

    def test_legacy_unproven_complete_revalidates_from_oldest_in_bounded_steps(self) -> None:
        self._append(1, 100)
        self.service.sync_source_once(initial_tail=100)
        self._append(101, 200)
        self._page(mode="recent", limit=20, refresh=True)
        with self.repository.database.transaction() as connection:
            connection.execute(
                (
                    "UPDATE source_conversation_state SET coverage_version=0, "
                    "history_complete=0, forward_complete=0, tail_sort_seq=200, "
                    "backfill_state='complete' WHERE conversation_id=?"
                ),
                (self.group,),
            )
            connection.execute(
                "DELETE FROM source_read_windows WHERE conversation_id=?", (self.group,)
            )
        self._restart()
        self.assertFalse(self._page(mode="recent", limit=1)["source_receipt"]["complete"])
        for expected in (40, 80, 120, 160, 200):
            self.service.sync_source_once(batch_limit=40)
            self.assertEqual(
                int(self._state()["tail_sort_seq"]),
                expected,
            )
        self.assertEqual(self._state()["backfill_state"], "complete")
        with self.repository.database.connection() as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM messages WHERE conversation_id=?", (self.group,)
                ).fetchone()[0],
                200,
            )

    def test_historical_backfill_uses_contiguous_floor_instead_of_old_island(self) -> None:
        self._append(1, 200)
        # A foreground historical island precedes the initial source-sync seed.
        self._page(
            mode="range",
            time_before="2026-01-01T00:01:41+00:00",
            direction="backward",
            limit=50,
            refresh=True,
        )
        self.service.sync_source_once(initial_tail=20)
        self.service.queue_backfill(conversation_id=self.group, max_messages=500)
        for _ in range(10):
            result = self.service.process_backfill_once(batch_limit=25)
            if result["state"] == "completed":
                break
        with self.repository.database.connection() as connection:
            actual = {
                row[0]
                for row in connection.execute(
                    "SELECT sort_seq FROM messages WHERE conversation_id=?", (self.group,)
                )
            }
        self.assertEqual(actual, set(range(1, 201)))
        self.assertEqual(self._state()["backfill_state"], "complete")

    def test_source_cursor_pages_join_validated_windows_without_advancing_frontier(self) -> None:
        self._append(1, 100)
        self.service.sync_source_once(initial_tail=100)
        self._append(101, 200)
        recent = self._page(mode="recent", limit=20, refresh=True)
        page = self._page(mode="recent", limit=20, cursor=recent["page"]["next_cursor"])
        self.assertEqual(self._numbers(page), list(range(161, 181)))
        context = self._page(mode="context", message_id=self._id(181), before=20, after=0, limit=21)
        self.assertEqual(self._numbers(context), list(range(161, 182)))
        self.assertEqual(int(self._state()["tail_sort_seq"]), 100)
        self.assertEqual(context["source_receipt"]["continuity"]["state"], "disjoint_windows")

    def test_gap_recovery_pressure_keeps_positions_then_resumes_after_restart(self) -> None:
        self._append(1, 100)
        self.service.sync_source_once(initial_tail=100)
        self._append(101, 200)
        self._page(mode="recent", limit=20, refresh=True)
        state = dict(self._state())
        watermark = self.repository.observation_watermark()
        budget = StorageBudget(
            self.window.parent,
            self.window,
            StorageSettings(64 * MIB, 128 * MIB, 1 << 60, 8 * MIB),
        )
        self.repository.database.storage = budget
        self.service.storage = budget
        with self.assertRaises(SightglassError) as caught:
            self.service.sync_source_once(batch_limit=25)
        self.assertEqual(caught.exception.code, ErrorCode.STORAGE_PRESSURE)
        self.assertEqual(self._state(), state)
        self.assertEqual(self.repository.observation_watermark(), watermark)
        self._restart()
        for _ in range(4):
            self.service.sync_source_once(batch_limit=25)
        with self.repository.database.connection() as connection:
            reference = {
                row[0]
                for row in connection.execute(
                    "SELECT sort_seq FROM messages WHERE conversation_id=?", (self.group,)
                )
            }
        self.assertEqual(reference, set(range(1, 201)))
        self.assertEqual(self._state()["backfill_state"], "complete")

    def test_cursor_check_and_row_selection_share_one_read_snapshot(self) -> None:
        self._append(1, 4, text="Synthetic before")
        self.service.sync_source_once(initial_tail=100)
        cursor = self._page(mode="recent", limit=1)["page"]["next_cursor"]
        source = replace(self._source(3), raw_content="Synthetic after")
        original = self.repository.materialized_snapshot_changed

        def barrier(*args: Any, **kwargs: Any) -> bool:
            result = original(*args, **kwargs)
            self._concurrent(lambda: self._upsert(source))
            return result

        with patch.object(self.repository, "materialized_snapshot_changed", side_effect=barrier):
            page = self._page(mode="recent", limit=1, cursor=cursor)
        self.assertEqual(self._numbers(page), [3])
        self.assertEqual(page["messages"][0]["text"], "Synthetic before")
        with self.repository.database.connection() as connection:
            committed = connection.execute(
                "SELECT MAX(committed_observation_seq) FROM reader_update_cursors "
                "WHERE conversation_id=?",
                (self.group,),
            ).fetchone()[0]
        self.assertLess(
            committed,
            self.repository.observation_watermark(),
            "progress must not ACK a correction written after the frozen read",
        )
        with self.assertRaises(SightglassError) as caught:
            self._page(mode="recent", limit=1, cursor=page["page"]["next_cursor"])
        self.assertEqual(caught.exception.code, ErrorCode.CURSOR_STALE)

    def test_body_label_and_resource_assembly_share_one_read_snapshot(self) -> None:
        self._append(1, 2, text="Synthetic before")
        self.service.sync_source_once(initial_tail=100)
        target = self._id(2)
        with self.repository.database.transaction() as connection:
            connection.execute(
                (
                    "INSERT INTO "
                    "resources(resource_id,message_id,source_ordinal,kind,mime_type,availab"
                    "ility,resolver_json,first_seen_at,last_seen_at) VALUES "
                    "('wxres_synthetic_snapshot',?,0,'file','text/plain','local_available',"
                    "'{}','synthetic','synthetic')"
                ),
                (target,),
            )

        def write() -> None:
            with self.repository.database.transaction() as connection:
                connection.execute(
                    "UPDATE messages SET text='Synthetic after' WHERE message_id=?", (target,)
                )
                connection.execute(
                    "UPDATE resources SET mime_type='application/pdf' WHERE message_id=?", (target,)
                )

        original = self.repository.preferred_labels_bulk

        def barrier(*args: Any, **kwargs: Any) -> Any:
            labels = original(*args, **kwargs)
            self._concurrent(write)
            return labels

        with patch.object(self.repository, "preferred_labels_bulk", side_effect=barrier):
            page = self._page(
                mode="message", message_id=target, limit=1, include_resources="metadata"
            )
        self.assertEqual(page["messages"][0]["text"], "Synthetic before")
        self.assertEqual(page["messages"][0]["resources"][0]["mime_type"], "text/plain")

    def test_empty_speaker_scan_page_continues_source_and_materialized_without_ack(self) -> None:
        self._append(1, 1, text="Synthetic needle")
        self._append(2, 10003)
        self.service.sync_source_once(initial_tail=1)
        participant = self._message_row(self._id(10003))["sender_id"]
        for local in (False, True):
            if local:
                while not self._state()["history_complete"]:
                    self.service.queue_backfill(conversation_id=self.group, max_messages=20000)
                    while (
                        self.service.process_backfill_once(batch_limit=500)["state"] != "completed"
                    ):
                        pass
            else:
                # Force source routing without changing authorization or fixtures.
                with self.repository.database.transaction() as connection:
                    connection.execute(
                        "UPDATE messages SET projection_epoch='legacy' WHERE conversation_id=?",
                        (self.group,),
                    )
            args = dict(
                mode="speaker",
                participant_ids=(participant,),
                query="needle",
                limit=2,
                direction="backward",
            )
            before = self._positions()
            first = self._page(**args)
            self.assertEqual(first["messages"], [])
            self.assertTrue(first["page"]["has_more_before"])
            self.assertIsNotNone(first["page"]["next_cursor"])
            self.assertEqual(
                self._positions(), before, "scanning must not become reader delivery or ACK"
            )
            second = self._page(**args, cursor=first["page"]["next_cursor"])
            self.assertEqual(self._numbers(second), [1])
            with self.assertRaises(SightglassError):
                self._page(**{**args, "query": "different"}, cursor=first["page"]["next_cursor"])

    def test_all_hidden_page_has_signed_scan_continuation_both_planes(self) -> None:
        self._append(1, 1, text="Synthetic visible")
        self._append(2, 4, text="Synthetic system", kind=10000)
        self.service.sync_source_once(initial_tail=100)
        for projection in ("detail", "compact"):
            first = self.service.read_messages(
                mode="recent",
                conversation_id=self.group,
                projection=projection,
                limit=2,
                system_policy="omit",
            )
            self.assertEqual(first["messages"], [])
            self.assertEqual(first["source_receipt"]["hidden_system_count"], 2)
            self.assertIsNotNone(first["page"]["next_cursor"])
            second = self.service.read_messages(
                mode="recent",
                conversation_id=self.group,
                projection=projection,
                limit=2,
                system_policy="omit",
                cursor=first["page"]["next_cursor"],
            )
            self.assertEqual(len(second["messages"]), 1)
            with self.assertRaises(SightglassError):
                self.service.read_messages(
                    mode="recent",
                    conversation_id=self.group,
                    projection=projection,
                    limit=2,
                    system_policy="include",
                    cursor=first["page"]["next_cursor"],
                )
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET projection_epoch='legacy' WHERE conversation_id=?",
                (self.group,),
            )
        first = self._page(mode="recent", limit=2, system_policy="omit")
        self.assertEqual(first["messages"], [])
        self.assertIsNotNone(first["page"]["next_cursor"])
        second = self._page(
            mode="recent", limit=2, system_policy="omit", cursor=first["page"]["next_cursor"]
        )
        self.assertEqual(self._numbers(second), [1])


if __name__ == "__main__":
    unittest.main()

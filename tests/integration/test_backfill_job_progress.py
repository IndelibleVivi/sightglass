"""Generated-only acceptance for explicit backfill progress and residency.

CoreCapture builds its real requests; only network submission and terminal-journal
hooks use local seams. This does not establish live relay IO.
"""

from __future__ import annotations

import shutil
import tempfile
import unittest
from dataclasses import fields, replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from sightglass.contracts.capture import CaptureCeiling, CaptureRequest
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.db import WindowDB
from sightglass.model.repositories import WindowRepository
from sightglass.policy.readers import ReaderContext, ReaderPolicy
from sightglass.reader.service import ReaderService
from sightglass.runtime.capture_core import CoreCapture
from sightglass.source.capture import CaptureExecutor
from sightglass.source.capture.codec import json_value, typed_value
from sightglass.source.capture.frozen import FrozenCaptureProvider
from sightglass.source.identity import SignedTokenCodec
from sightglass.source.remote import RemoteCaptureProvider, RemoteCaptureSettings
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack, enable_keep_residency
from tests.integration import test_native_source_provider as native_fixtures


class RemoteBackfillJobProgressTests(unittest.TestCase):
    def setUp(self):
        self.fixture = native_fixtures.NativeSourceProviderTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.root = self.fixture.root
        self.provider = self.fixture.provider
        self.table = self.fixture._table_name(self.fixture.conversation)
        with self.fixture._connect_new("message/message_0.db") as connection:
            connection.executemany(
                f"INSERT INTO [{self.table}] VALUES (?,?,?,?,?,?,?,?,?,?)",
                [
                    (
                        index,
                        100 + index,
                        1,
                        index,
                        0,
                        1_725_000_000 + index,
                        0,
                        f"Synthetic history {index}",
                        0,
                        None,
                    )
                    for index in range(3, 426)
                ],
            )
        database = WindowDB(self.root / "state" / "window.db")
        enable_keep_residency(database)
        self.repository = WindowRepository(database)
        reader = ReaderContext(
            "synthetic-operator", "Synthetic", ReaderPolicy(mode="all_except_denylist")
        )
        codec = SignedTokenCodec(b"synthetic-only-backfill-secret-value")
        baseline = ReaderService(self.provider, self.repository, reader, codec)
        baseline.sync_source_once(initial_tail=1, conversation_limit=100)
        self.baseline = baseline
        self.group = self.repository.conversation_id_for(
            self.repository.account_id_for(self.fixture.account_key), self.fixture.conversation
        )
        self.settings = RemoteCaptureSettings(
            "synthetic-native-origin",
            self.fixture.account_key,
            frozenset({self.fixture.conversation}),
            "synthetic-egress",
            "synthetic-stream",
            "0" * 64,
            self.root / "unused-capture.sock",
            self.provider.descriptor,
        )
        self.service = ReaderService(
            RemoteCaptureProvider(self.settings), self.repository, reader, codec
        )
        self.executor = CaptureExecutor(
            self.provider,
            CaptureCeiling(
                self.settings.account_id, self.settings.conversations, self.settings.egress_revision
            ),
            source_instance_id=self.settings.source_instance_id,
        )
        # Isolate the reader/core binding from the concurrently edited relay. Execute
        # real generated native captures and FrozenCaptureProvider replay; the only
        # seams replace network submission and terminal-journal hooks.
        self.core = object.__new__(CoreCapture)
        self.core.service = self.service
        self.core.settings = self.settings
        self.captures = []

        def submit(values):
            decoded = {
                field.name: json_value(values.get(field.name, field.default))
                for field in fields(CaptureRequest)
            }
            decoded.update(
                request_id="synthetic-request-" + str(len(self.captures)),
                policy_revision=self.service._policy_revision(),
            )
            request = typed_value(CaptureRequest, decoded)
            envelope = self.executor.capture(
                request, stream_epoch="synthetic-stream", sequence=len(self.captures) + 1
            )
            self.captures.append(envelope.document())
            return SimpleNamespace(provider=FrozenCaptureProvider(envelope), durable_ack=True)

        submit_patch = mock.patch.object(self.core, "_submit", side_effect=submit)
        hooks_patch = mock.patch.object(self.core, "_hooks", return_value=(None, None))
        submit_patch.start()
        hooks_patch.start()
        self.addCleanup(submit_patch.stop)
        self.addCleanup(hooks_patch.stop)

    def queue(self, **arguments):
        return self.core.catalog_call(
            lambda: self.service.queue_backfill(conversation_id=self.group, **arguments)
        )

    def restart_reader(self):
        previous = self.service
        self.service = ReaderService(
            RemoteCaptureProvider(self.settings),
            self.repository,
            previous.reader,
            previous.token_codec,
        )
        self.core.service = self.service

    def test_native_remote_scope_digest_differs_but_job_resumes_exactly_after_restart(self):
        self.repository.upsert_reader("synthetic-operator", "Synthetic", {}, "2026-10-23T00:00:00Z")
        with self.repository.database.transaction() as connection:
            connection.execute(
                "INSERT INTO reader_update_cursors VALUES (?,?,?,?,?,?)",
                (
                    "synthetic-operator",
                    self.group,
                    "conversation",
                    "synthetic-ack-scope",
                    1,
                    "2026-10-23T00:00:00Z",
                ),
            )
        queued = self.queue(max_messages=500)
        first = self.core.backfill_once()
        self.assertEqual(first["processed_messages"], 199)
        self.restart_reader()
        second = self.core.backfill_once()
        self.assertEqual(second["processed_messages"], 398)
        third = self.core.backfill_once()
        self.assertEqual((third["state"], third["processed_messages"]), ("completed", 425))
        job = self.repository.backfill_job(queued["job_ids"][0])
        assert job is not None
        self.assertEqual(job["processed_messages"], 425)
        self.assertEqual(
            self.repository.update_position(
                "synthetic-operator", self.group, "conversation", "synthetic-ack-scope"
            ),
            1,
        )
        catalog = next(item for item in self.captures if item.request.operation == "catalog")
        ranges = [item for item in self.captures if item.request.operation == "range"]
        self.assertEqual(len(ranges), 3)
        self.assertTrue(
            all(item.origin.origin_epoch == catalog.origin.origin_epoch for item in ranges)
        )
        self.assertTrue(
            all(item.origin.inventory_digest != catalog.origin.inventory_digest for item in ranges)
        )
        self.assertEqual(ranges[0].request.expected_generations, ())
        self.assertEqual(
            ranges[1].request.expected_generations, ranges[0].origin.selected_generations
        )
        self.assertEqual(
            ranges[2].request.expected_generations, ranges[0].origin.selected_generations
        )
        with self.repository.database.connection() as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM messages WHERE conversation_id=? AND body_available=1",
                    (self.group,),
                ).fetchone()[0],
                425,
            )

    def test_selected_replacement_fails_closed_without_progress(self):
        self.queue(max_messages=500)
        self.core.backfill_once()
        path = self.fixture.source / "message/message_0.db"
        replacement = path.with_suffix(".replacement")
        shutil.copy2(path, replacement)
        replacement.replace(path)
        with self.assertRaises(SightglassError):
            self.core.backfill_once()
        job = self.repository.next_backfill_job()
        assert job is not None
        self.assertEqual(job["processed_messages"], 199)
        self.assertEqual(self.captures[-1].receipt.terminal, "rejected")

    def test_ordinary_append_between_pages_keeps_exact_job_position(self):
        self.queue(max_messages=500)
        self.core.backfill_once()
        with self.fixture._connect_new("message/message_0.db") as connection:
            connection.execute(
                f"INSERT INTO [{self.table}] VALUES (?,?,?,?,?,?,?,?,?,?)",
                (426, 526, 1, 426, 0, 1_725_000_426, 0, "Synthetic append", 0, None),
            )
        second = self.core.backfill_once()
        third = self.core.backfill_once()
        self.assertEqual(second["processed_messages"], 398)
        self.assertEqual(third["processed_messages"], 425)
        before_tail = self.repository.source_conversation_state(self.group)
        assert before_tail is not None
        self.assertEqual(before_tail["tail_sort_seq"], 425)
        self.baseline.sync_source_once(initial_tail=1, conversation_limit=100)
        after_tail = self.repository.source_conversation_state(self.group)
        assert after_tail is not None
        self.assertEqual(after_tail["tail_sort_seq"], 426)

    def test_epoch_change_is_rejected_before_retry_send(self):
        self.queue(max_messages=500)
        descriptor = replace(
            self.provider.descriptor, implementation="synthetic-new-interpretation"
        )
        operation_provider = RemoteCaptureProvider(replace(self.settings, origin=descriptor))
        self.service.provider = operation_provider
        with self.assertRaises(SightglassError) as caught:
            self.core.backfill_once()
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)
        self.assertEqual(len(self.captures), 1)

    def test_paused_reader_cannot_start_another_range(self):
        self.queue(max_messages=500)
        self.service.reader = replace(self.service.reader, paused=True)
        with self.assertRaises(SightglassError):
            self.core.backfill_once()
        self.assertEqual(len(self.captures), 1)

    def test_selected_dependency_changes_during_range_reject_before_admission(self):
        self.queue(max_messages=500)
        writer = self.fixture._connect_new("message/message_0.db")
        self.addCleanup(writer.close)
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        original = self.provider.read_range

        def mutate(*args, **kwargs):
            page = original(*args, **kwargs)
            writer.execute(
                f"UPDATE [{self.table}] SET message_content='Synthetic correction' WHERE local_id=1"
            )
            writer.commit()
            return page

        with mock.patch.object(self.provider, "read_range", side_effect=mutate):
            with self.assertRaises(SightglassError):
                self.core.backfill_once()
        self.assertEqual(self.captures[-1].receipt.terminal, "rejected")
        self.assertEqual(self.captures[-1].receipt.reason, "SOURCE_GENERATION_CHANGED")
        job = self.repository.next_backfill_job()
        assert job is not None
        self.assertEqual(job["processed_messages"], 0)

    def test_remote_untouched_legacy_job_uses_fresh_catalog_then_real_range(self):
        identity = self.queue(max_messages=500)["job_ids"][0]
        inventory = self.captures[-1].origin.inventory_digest
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE source_backfill_jobs SET cursor_token=NULL,source_inventory_epoch=? "
                "WHERE job_id=?",
                (inventory, identity),
            )
        result = self.core.backfill_once()
        self.assertEqual(result["processed_messages"], 199)
        self.assertEqual(
            [item.request.operation for item in self.captures], ["catalog", "catalog", "range"]
        )
        row = self.repository.backfill_job(identity)
        assert row is not None
        self.assertTrue(row["cursor_token"])

    def test_source_account_binding_change_is_rejected_before_capture(self):
        self.queue(max_messages=500)
        job = self.repository.next_backfill_job()
        assert job is not None
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE accounts SET source_account_key=? WHERE account_id=?",
                ("synthetic-rebound-source-account", job["account_id"]),
            )
        with self.assertRaises(SightglassError) as caught:
            self.core.backfill_once()
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)
        self.assertEqual(len(self.captures), 1)

    def test_released_native_bodies_are_reacquired_only_in_requested_time_range(self):
        self.queue(max_messages=500)
        for _ in range(3):
            result = self.core.backfill_once()
        self.assertEqual(result["state"], "completed")
        preview = self.service.residency.stock_preview(self.group, limit=500)
        self.assertEqual(preview["examined_messages"], 425)
        self.service.residency.release_stock(self.group, plan=preview["plan"], limit=500)

        def timestamp(number):
            return datetime.fromtimestamp(1_725_000_000 + number, UTC).isoformat()

        self.queue(after=timestamp(80), before=timestamp(100), max_messages=500)
        result = self.core.backfill_once()
        self.assertEqual((result["state"], result["processed_messages"]), ("completed", 20))
        with self.repository.database.connection() as connection:
            resident = connection.execute(
                "SELECT sort_seq FROM messages WHERE conversation_id=? "
                "AND body_available=1 ORDER BY sort_seq",
                (self.group,),
            ).fetchall()
        self.assertEqual([row[0] for row in resident], list(range(80, 100)))

    def test_residency_revocation_during_preparation_rolls_back_page_and_progress(self):
        self.queue(max_messages=500)
        prepare = self.service._prepare_messages

        def revoke(messages):
            prepared = prepare(messages)
            self.service.residency.set(self.group, mode="on_demand")
            return prepared

        with mock.patch.object(self.service, "_prepare_messages", side_effect=revoke):
            with self.assertRaises(SightglassError):
                self.core.backfill_once()
        job = self.repository.next_backfill_job()
        assert job is not None
        self.assertEqual(job["processed_messages"], 0)

    def test_binding_change_during_preparation_rolls_back_progress(self):
        self.queue(max_messages=500)
        prepare = self.service._prepare_messages
        job = self.repository.next_backfill_job()
        assert job is not None

        def rebind(messages):
            prepared = prepare(messages)
            with self.repository.database.transaction() as connection:
                connection.execute(
                    "UPDATE accounts SET source_account_key=? WHERE account_id=?",
                    ("synthetic-rebound-during-read", job["account_id"]),
                )
            return prepared

        with mock.patch.object(self.service, "_prepare_messages", side_effect=rebind):
            with self.assertRaises(SightglassError):
                self.core.backfill_once()
        current = self.repository.next_backfill_job()
        assert current is not None
        self.assertEqual(current["processed_messages"], 0)

    def test_stale_job_page_cannot_double_commit_progress(self):
        self.queue(max_messages=500)
        previous = self.repository.next_backfill_job()
        assert previous is not None
        self.core.backfill_once()
        with mock.patch.object(self.repository, "next_backfill_job", return_value=previous):
            with self.assertRaises(SightglassError):
                self.core.backfill_once()
        current = self.repository.next_backfill_job()
        assert current is not None
        self.assertEqual(current["processed_messages"], 199)

    def test_bounded_history_is_disjoint_and_does_not_move_live_frontier(self):
        state = self.repository.source_conversation_state(self.group)
        assert state is not None
        floor, tail = state["contiguous_floor_position"], state["tail_sort_primary"]

        def timestamp(number):
            return datetime.fromtimestamp(1_725_000_000 + number, UTC).isoformat()

        self.queue(after=timestamp(80), before=timestamp(100), max_messages=500)
        result = self.core.backfill_once()
        self.assertEqual(result["state"], "completed")
        self.assertGreater(result["processed_messages"], 0)
        state = self.repository.source_conversation_state(self.group)
        assert state is not None
        self.assertEqual(state["contiguous_floor_position"], floor)
        self.assertEqual(state["tail_sort_primary"], tail)
        self.assertFalse(state["history_complete"])
        with self.repository.database.connection() as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM source_read_windows WHERE conversation_id=?",
                    (self.group,),
                ).fetchone()[0],
                2,
            )

    def test_message_limit_completes_job_without_claiming_history(self):
        self.queue(max_messages=5)
        result = self.core.backfill_once()
        self.assertEqual((result["state"], result["processed_messages"]), ("completed", 5))
        state = self.repository.source_conversation_state(self.group)
        assert state is not None
        self.assertFalse(state["history_complete"])


class ReleasedBackfillJobTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        create_synthetic_source(root / "source")
        _, self.repository, self.service, _ = build_test_stack(
            root / "source", root / "state" / "window.db"
        )
        self.service.sync_source_once(initial_tail=200, conversation_limit=100)
        self.group = self.repository.conversation_id_for(
            self.repository.account_id_for("synthetic-account-demo"), "conv_group"
        )
        self.service.queue_backfill(conversation_id=self.group)
        self.assertEqual(self.service.process_backfill_once()["state"], "completed")
        state = self.repository.source_conversation_state(self.group)
        assert state is not None
        self.floor = state["contiguous_floor_position"]
        preview = self.service.residency.stock_preview(self.group)
        self.original_bodies = preview["examined_messages"]
        self.assertGreater(self.original_bodies, 0)
        with self.repository.database.connection() as connection:
            self.episodes = dict(
                connection.execute(
                    "SELECT message_id,current_observation_seq FROM messages "
                    "WHERE conversation_id=?",
                    (self.group,),
                ).fetchall()
            )
        self.service.residency.release_stock(self.group, plan=preview["plan"])

    def resident_count(self):
        with self.repository.database.connection() as connection:
            return connection.execute(
                "SELECT COUNT(*) FROM messages WHERE conversation_id=? AND body_available=1",
                (self.group,),
            ).fetchone()[0]

    def test_explicit_job_reacquires_released_requested_history(self):
        self.assertEqual(self.resident_count(), 0)
        self.assertEqual(self.service.process_backfill_once()["state"], "idle")
        self.assertEqual(self.resident_count(), 0)
        self.service.queue_backfill(conversation_id=self.group)
        result = self.service.process_backfill_once(batch_limit=2)
        while result["state"] != "completed":
            result = self.service.process_backfill_once(batch_limit=2)
        self.assertEqual(result["processed_messages"], self.original_bodies)
        self.assertEqual(self.resident_count(), self.original_bodies)
        state = self.repository.source_conversation_state(self.group)
        assert state is not None
        self.assertEqual(self.floor, state["contiguous_floor_position"])
        with self.repository.database.connection() as connection:
            self.assertEqual(
                self.episodes,
                dict(
                    connection.execute(
                        "SELECT message_id,current_observation_seq FROM messages "
                        "WHERE conversation_id=?",
                        (self.group,),
                    ).fetchall()
                ),
            )

    def legacy(self, *, processed=0, epoch=None):
        queued = self.service.queue_backfill(conversation_id=self.group)
        identity = queued["job_ids"][0]
        if epoch is None:
            with self.service.provider.snapshot() as snapshot:
                epoch = snapshot.inventory_digest
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE source_backfill_jobs SET cursor_token=NULL,source_inventory_epoch=?,"
                "processed_messages=? WHERE job_id=?",
                (epoch, processed, identity),
            )
        return identity

    def test_untouched_legacy_job_rebinds_matching_catalog_and_reacquires(self):
        identity = self.legacy()
        result = self.service.process_backfill_once()
        self.assertEqual(result["state"], "completed")
        self.assertEqual(self.resident_count(), self.original_bodies)
        row = self.repository.backfill_job(identity)
        assert row is not None
        self.assertTrue(row["cursor_token"])
        self.assertEqual(row["source_inventory_epoch"], self.service._projection_inventory_epoch())

    def test_partial_legacy_job_fails_explicitly_preserving_count_and_allows_new_job(self):
        identity = self.legacy(processed=2)
        result = self.service.process_backfill_once()
        self.assertEqual(
            (result["state"], result["reason"]), ("failed", "legacy_backfill_requeue_required")
        )
        row = self.repository.backfill_job(identity)
        assert row is not None
        self.assertEqual(row["processed_messages"], 2)
        self.assertEqual(self.resident_count(), 0)
        self.service.queue_backfill(conversation_id=self.group)
        self.assertEqual(self.service.process_backfill_once()["state"], "completed")
        self.assertEqual(self.resident_count(), self.original_bodies)

    def test_legacy_catalog_binding_change_fails_instead_of_adopting_new_source(self):
        identity = self.legacy(epoch="synthetic-obsolete-binding")
        result = self.service.process_backfill_once()
        self.assertEqual(result["state"], "failed")
        row = self.repository.backfill_job(identity)
        assert row is not None
        self.assertEqual(row["processed_messages"], 0)
        self.assertEqual(self.resident_count(), 0)

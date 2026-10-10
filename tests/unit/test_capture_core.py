from __future__ import annotations

import json
import socket
import sqlite3
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from inspect import signature
from pathlib import Path
from unittest import mock

from sightglass.contracts.capture import CaptureCeiling, CaptureProtocolError, CaptureRequest
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.mcp.tools import ReaderTools
from sightglass.reader.service import ReaderService
from sightglass.runtime.activation import write_activation
from sightglass.runtime.capture_core import CoreCapture
from sightglass.runtime.config import SightglassConfig
from sightglass.runtime.edge import EdgeSpool
from sightglass.runtime.edge_relay import (
    EdgeSession,
    FramedStream,
    PreparedRemoteCapture,
    edge_token_hash,
)
from sightglass.source.capture import CaptureExecutor
from sightglass.source.capture.codec import SealedCapture
from sightglass.source.remote import RemoteCaptureProvider, RemoteCaptureSettings
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


class CoreCaptureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        source = create_synthetic_source(self.root / "source")
        self.source_root = source
        provider, repository, baseline, _ = build_test_stack(
            source,
            self.root / "state" / "window.db",
            default_projection=None,
        )
        baseline.sync_source_once(initial_tail=200)
        self.baseline = baseline
        self.provider, self.repository = provider, repository
        self.group = repository.conversation_id_for(
            repository.account_id_for("synthetic-account-demo"),
            "conv_group",
        )
        self.token = "synthetic-separate-edge-capability"
        self.settings = RemoteCaptureSettings(
            "synthetic-origin-instance",
            "synthetic-account-demo",
            frozenset({"conv_group", "conv_direct"}),
            "synthetic-egress-v1",
            "synthetic-stream-v1",
            edge_token_hash(self.token),
            self.root / "capture.sock",
            provider.descriptor,
        )
        remote = RemoteCaptureProvider(self.settings)
        self.service = ReaderService(
            remote, repository, baseline.reader, baseline.token_codec, default_view="replica"
        )
        self.tools = ReaderTools(self.service)
        self.config = replace(
            SightglassConfig.create(self.root / "state", source),
            source_kind="remote-capture",
            source_instance_id=self.settings.source_instance_id,
            reader_default_view="replica",
            activation_generation="synthetic-core-generation",
            activation_path=self.root / "owner",
        )
        assert self.config.activation_path is not None
        write_activation(
            self.config.activation_path,
            generation=self.config.activation_generation,
            state="active",
            role="core",
            namespace=self.config.window_db_path,
        )
        self.core = CoreCapture(self.service, self.config)
        self.core.start()
        self.addCleanup(self.core.close)
        self.executor = CaptureExecutor(
            provider,
            CaptureCeiling(
                self.settings.account_id, self.settings.conversations, self.settings.egress_revision
            ),
            source_instance_id=self.settings.source_instance_id,
        )
        self.spool = EdgeSpool.initialize(
            self.root / "edge",
            source_instance_id=self.settings.source_instance_id,
            account_id=self.settings.account_id,
            origin_epoch=self.executor.origin_epoch,
            stream_epoch=self.settings.stream_epoch,
        )
        self.addCleanup(self.spool.close)
        self.edge_socket: socket.socket | None = None
        self.edge_thread: threading.Thread | None = None
        self.edge_stop = threading.Event()
        self.addCleanup(self.stop_edge)

    def start_edge(self) -> None:
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.connect(str(self.settings.socket_path))
        self.edge_socket = connection

        def run() -> None:
            try:
                with connection.makefile("rb") as reader, connection.makefile("wb") as writer:
                    EdgeSession(
                        self.executor,
                        self.spool,
                        token=self.token,
                        core_generation=self.config.activation_generation,
                    ).run(
                        FramedStream(reader, writer),
                        stop=self.edge_stop,
                    )
            except (OSError, RuntimeError, ValueError):
                if not self.edge_stop.is_set():
                    raise

        self.edge_thread = threading.Thread(target=run, daemon=True)
        self.edge_thread.start()
        deadline = time.monotonic() + 2
        while not self.core.broker.connected and time.monotonic() < deadline:
            self.edge_stop.wait(0.005)
        self.assertTrue(self.core.broker.connected)

    def stop_edge(self) -> None:
        self.edge_stop.set()
        if self.edge_socket is not None:
            try:
                self.edge_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.edge_socket.close()
        if self.edge_thread is not None:
            self.edge_thread.join(timeout=2)

    def call(self, name: str, **arguments):
        return self.core.call(name, arguments, lambda: getattr(self.tools, name)(**arguments))

    def _unfinalized_catalog_ticket(self, *, expired: bool = False) -> PreparedRemoteCapture:
        request = CaptureRequest(
            "synthetic-unfinalized-catalog", "catalog", self.settings.account_id,
            self.core.expected.policy_revision,
        )
        self.core._record_request(request)
        envelope = self.executor.capture(
            request, stream_epoch=self.settings.stream_epoch, sequence=1,
            batch_id="synthetic-unfinalized-batch",
        )
        if expired:
            document = envelope.document()
            envelope = SealedCapture.seal(
                replace(
                    document,
                    origin=replace(document.origin, captured_at="2000-01-01T00:00:00+00:00"),
                    receipt=replace(
                        document.receipt, sealed_at="2000-01-01T00:00:00+00:00",
                        fresh_until="2000-01-01T00:01:00+00:00",
                    ),
                )
            )
        with ThreadPoolExecutor(max_workers=1) as pool:
            waiter = pool.submit(self.core.broker.submit, request, timeout=2)
            with self.core.broker._condition:
                self.assertTrue(
                    self.core.broker._condition.wait_for(lambda: bool(self.core.broker._queue), 1)
                )
            self.assertIsNotNone(self.core.broker._next_work())
            ticket = self.core.broker._receive_capture(envelope)
            self.assertIs(waiter.result(timeout=1), ticket)
        assert isinstance(ticket, PreparedRemoteCapture)
        return ticket

    def test_failed_catalog_reject_detaches_caller_for_exact_durable_recovery(self) -> None:
        from sightglass.runtime.capture_journal import CAPTURE_REQUEST_TOOL, _identity

        ticket = self._unfinalized_catalog_ticket()
        original = self.core.journal.record_terminal

        def fail_after_write(document, ack):
            original(document, ack)
            raise SightglassError(ErrorCode.SERVICE_BUSY, retryable=True)

        with mock.patch.object(self.core.journal, "record_terminal", side_effect=fail_after_write):
            with self.assertRaises(SightglassError):
                self.core._reject(ticket)
        self.assertIsNone(ticket.durable_ack)
        self.assertIsNone(self.core.receiver.lookup_terminal(ticket.envelope))
        with self.core.database.connection() as connection:
            row = connection.execute(
                "SELECT outcome FROM access_receipts WHERE receipt_id=? AND tool_name=?",
                (_identity("request", ticket.request.request_id), CAPTURE_REQUEST_TOOL),
            ).fetchone()
        self.assertEqual(row[0], "pending")
        replayed = self.core.broker._receive_capture(ticket.envelope)
        self.assertIs(replayed, ticket)
        ack = ticket.durable_ack
        assert ack is not None
        self.assertEqual(ack.terminal, "rejected")
        self.assertEqual(self.core.receiver.lookup_terminal(ticket.envelope), ack)
        with self.core.database.connection() as connection:
            row = connection.execute(
                "SELECT outcome FROM access_receipts WHERE receipt_id=? AND tool_name=?",
                (_identity("request", ticket.request.request_id), CAPTURE_REQUEST_TOOL),
            ).fetchone()
        self.assertEqual(row[0], "rejected")

    def test_expired_abandoned_ticket_can_reject_but_cannot_admit_new_body(self) -> None:
        ticket = self._unfinalized_catalog_ticket(expired=True)
        with self.assertRaisesRegex(CaptureProtocolError, "fresh_receipt_expired"):
            self.core.receiver.admit(
                self.core.receiver.prepare(ticket.envelope),
                lambda *_: self.fail("expired capture body was admitted"),
                receipt_id="synthetic-expired-must-not-accept",
            )
        self.assertIsNone(self.core.receiver.lookup_terminal(ticket.envelope))
        self.core.broker.abandon(ticket)
        self.assertIs(self.core.broker._receive_capture(ticket.envelope), ticket)
        ack = ticket.durable_ack
        assert ack is not None
        self.assertEqual(ack.terminal, "rejected")
        self.assertEqual(self.core.receiver.lookup_terminal(ticket.envelope), ack)

    def test_unknown_exact_replays_fail_closed_on_every_recovery_attempt(self) -> None:
        request = CaptureRequest(
            "synthetic-unknown-catalog", "catalog", self.settings.account_id,
            self.core.expected.policy_revision,
        )
        envelope = self.executor.capture(
            request, stream_epoch=self.settings.stream_epoch, sequence=1,
            batch_id="synthetic-unknown-batch",
        )
        for _ in range(2):
            with self.assertRaisesRegex(CaptureProtocolError, "requires_operator_recovery"):
                self.core.broker._receive_capture(envelope)
            self.assertIsNone(self.core.receiver.lookup_terminal(envelope))

    def test_replay_recovery_checks_current_owner_before_terminal_write(self) -> None:
        ticket = self._unfinalized_catalog_ticket()
        self.core.broker.abandon(ticket)
        assert self.config.activation_path is not None
        write_activation(
            self.config.activation_path, generation=self.config.activation_generation,
            state="revoked", role="core", namespace=self.config.window_db_path,
            expected_generation=self.config.activation_generation,
        )
        with self.assertRaisesRegex(RuntimeError, "active core ownership"):
            self.core.broker._receive_capture(ticket.envelope)
        self.assertIsNone(ticket.durable_ack)
        self.assertIsNone(self.core.receiver.lookup_terminal(ticket.envelope))

    def test_fresh_capture_accepts_the_complete_mcp_proxy_defaults(self) -> None:
        self.start_edge()
        bound = signature(ReaderTools.wechat_read_messages).bind(
            self.tools, mode="recent", conversation_id=self.group, view="fresh", limit=1
        )
        bound.apply_defaults()
        arguments = dict(bound.arguments)
        arguments.pop("self")
        self.assertIsNone(arguments["participant_ids"])
        result = self.call("wechat_read_messages", **arguments)
        self.assertNotEqual(result.get("ok"), False, result)
        self.assertEqual(len(result["messages"]), 1)
        position = self.core.journal.stream_position(
            self.settings.source_instance_id, self.settings.account_id
        )
        assert position is not None
        self.assertEqual(position.next_sequence, 2)

    def append(self, identity: str = "synthetic-capture-next") -> None:
        with closing(sqlite3.connect(self.source_root / "messages-2.db")) as connection:
            connection.execute(
                "INSERT INTO messages(source_message_id,source_conversation_id,source_time_raw,"
                "sent_at_utc,observed_at_utc,sort_seq,source_rowid,wechat_type,raw_content,is_outgoing,"
                "sender_internal_id,sender_local_token,sender_surface_label,resources_json) "
                "VALUES(?,'conv_group','2026-09-13T09:07:00+00:00',"
                "'2026-09-13T09:07:00+00:00','2026-09-13T11:00:00+00:00',10,100,1,"
                "'wxid_demo_member:\nSynthetic captured update',0,'wxid_demo_member',NULL,"
                "'Synthetic participant','[]')",
                (identity,),
            )
            connection.commit()
        path = self.source_root / "source.json"
        value = json.loads(path.read_text())
        value["shards"][1]["generation_id"] = "synthetic-capture-generation-next"
        path.write_text(json.dumps(value))

    def test_offline_replica_remains_usable_and_fresh_fails_without_source_open(self) -> None:
        with mock.patch.object(self.provider, "session", side_effect=AssertionError("no source")):
            page = self.call("wechat_read_messages", mode="recent", conversation_id=self.group)
            self.assertIn("schema", page)
            self.assertNotEqual(page.get("ok"), False, page)
            search = self.call("wechat_search_messages", query="Synthetic")
            self.assertNotEqual(search.get("ok"), False, search)
        with self.assertRaises(SightglassError) as error:
            self.call(
                "wechat_read_messages", mode="recent", conversation_id=self.group, view="fresh"
            )
        self.assertEqual(error.exception.code, ErrorCode.SERVICE_UNAVAILABLE)
        self.assertIsNone(
            self.core.journal.stream_position(
                self.settings.source_instance_id,
                self.settings.account_id,
            )
        )

    def test_replica_status_reports_edge_transport_without_source_or_capture_confirmation(
        self,
    ) -> None:
        # The broker is the single transport-evidence owner. Status must reflect a live
        # edge connection without opening the source, initiating capture, or claiming a
        # fresh capture confirmation.
        with (
            mock.patch.object(
                self.provider, "session", side_effect=AssertionError("status opened source")
            ),
            mock.patch.object(
                self.provider, "snapshot", side_effect=AssertionError("status opened source")
            ),
            mock.patch.object(
                self.core, "_submit", side_effect=AssertionError("status requested capture")
            ),
        ):
            disconnected = self.service.status()
            self.assertEqual(disconnected["read_plane"]["default_view"], "replica")
            self.assertEqual(
                disconnected["read_plane"]["capture_transport"], "edge_disconnected"
            )
            self.assertEqual(disconnected["readiness"]["live_refresh"], "degraded")
            self.assertFalse(disconnected["read_plane"]["live_refresh_available"])
            self.assertFalse(disconnected["read_plane"]["live_refresh_confirmed"])
            self.assertIn("replica_view_not_live", disconnected["source"]["warnings"])

            self.start_edge()
            connected = self.service.status()
            self.assertEqual(connected["read_plane"]["capture_transport"], "edge_connected")
            # Connected transport is evidence, not confirmation: a fresh request can be
            # initiated, but source facts remain unconfirmed and readiness is explicitly
            # awaiting confirmation.
            self.assertEqual(connected["readiness"]["live_refresh"], "awaiting_confirmation")
            self.assertTrue(connected["read_plane"]["live_refresh_available"])
            self.assertFalse(connected["read_plane"]["live_refresh_confirmed"])
            self.assertIn("replica_view_not_live", connected["source"]["warnings"])
            self.assertEqual(connected["source"]["source_state"], "unknown")
            self.assertFalse(connected["source"]["available"])
        # The broker remains the sole source of transport evidence.
        self.assertEqual(self.core.transport_evidence(), "edge_connected")
        self.assertEqual(self.core.status()["edge_connected"], True)
        self.stop_edge()
        deadline = time.monotonic() + 2
        while self.core.broker.connected and time.monotonic() < deadline:
            time.sleep(0.005)
        self.assertEqual(self.core.transport_evidence(), "edge_disconnected")
        self.assertEqual(
            self.service.status()["read_plane"]["capture_transport"], "edge_disconnected"
        )

    def test_fresh_ack_capture_and_lost_response_replay_share_one_commit(self) -> None:
        first = self.call("wechat_read_messages", mode="updates", conversation_id=self.group)
        self.assertIsNotNone(first["page"]["delivery_id"], first)
        delivery_id = first["page"]["delivery_id"]
        self.append()
        self.start_edge()
        arguments = dict(
            mode="updates",
            conversation_id=self.group,
            view="fresh",
            ack_delivery_id=delivery_id,
            request_id="synthetic-lost-response",
        )
        result = self.call("wechat_read_messages", **arguments)
        self.assertNotEqual(result.get("ok"), False, result)
        delivery = self.repository.delivery(delivery_id)
        assert delivery is not None
        self.assertEqual(delivery["status"], "acknowledged")
        position = self.core.journal.stream_position(
            self.settings.source_instance_id,
            self.settings.account_id,
        )
        assert position is not None
        self.assertEqual(position.next_sequence, 2)
        self.stop_edge()
        replay = self.call("wechat_read_messages", **arguments)
        self.assertEqual(replay, result)
        self.assertEqual(self.service._projection_inventory_epoch(), self.executor.origin_epoch)

    def test_reader_and_receive_state_rollback_together_before_terminal_rejection(self) -> None:
        first = self.call("wechat_read_messages", mode="updates", conversation_id=self.group)
        delivery_id = first["page"]["delivery_id"]
        self.append()
        self.start_edge()
        original = self.core.journal.record_terminal
        failures = [True]

        def fail_once(document, ack):
            if failures:
                failures.pop()
                raise RuntimeError("synthetic final receive failure")
            return original(document, ack)

        with mock.patch.object(self.core.journal, "record_terminal", side_effect=fail_once):
            result = self.call(
                "wechat_read_messages",
                mode="updates",
                conversation_id=self.group,
                view="fresh",
                ack_delivery_id=delivery_id,
                request_id="synthetic-rollback",
            )
        self.assertEqual(result.get("ok"), False, result)
        delivery = self.repository.delivery(delivery_id)
        assert delivery is not None
        self.assertEqual(delivery["status"], "pending")
        with self.repository.database.connection() as connection:
            admitted = connection.execute(
                "SELECT 1 FROM messages WHERE source_message_id='synthetic-capture-next'"
            ).fetchone()
            outcomes = connection.execute(
                "SELECT outcome FROM access_receipts "
                "WHERE tool_name='_sightglass_updates_request.v1'"
            ).fetchall()
        self.assertIsNone(admitted)
        self.assertEqual(outcomes, [])
        position = self.core.journal.stream_position(
            self.settings.source_instance_id,
            self.settings.account_id,
        )
        assert position is not None
        self.assertEqual(position.next_sequence, 2)

    def test_catalog_background_sync_and_resource_capture_use_same_core_journal(self) -> None:
        self.append()
        self.start_edge()
        self.core.sync_once()  # full allowed catalog, no message coverage manufactured
        synced = [self.core.sync_once(), self.core.sync_once()]
        self.assertEqual(sum(item["message_count"] for item in synced), 1)
        with self.repository.database.connection() as connection:
            resource = connection.execute(
                "SELECT resource_id FROM resources WHERE mime_type='text/markdown'"
            ).fetchone()
        assert resource is not None
        with mock.patch.object(
            self.provider, "get_message", side_effect=AssertionError("no owner")
        ):
            result = self.call("wechat_read_resource", resource_id=resource[0], mode="original")
        self.assertFalse(result.isError, result)
        self.stop_edge()
        cached = self.call("wechat_read_resource", resource_id=resource[0], mode="original")
        self.assertFalse(cached.isError, cached)
        self.assertEqual(cached.content[1:], result.content[1:])
        position = self.core.journal.stream_position(
            self.settings.source_instance_id,
            self.settings.account_id,
        )
        assert position is not None
        self.assertEqual(position.next_sequence, 5)

    def test_fresh_current_pending_with_new_request_never_requests_extra_capture(self) -> None:
        first = self.call("wechat_read_messages", mode="updates", conversation_id=self.group)
        self.start_edge()
        with mock.patch.object(self.core, "_submit", side_effect=AssertionError("extra capture")):
            replay = self.call(
                "wechat_read_messages",
                mode="updates",
                conversation_id=self.group,
                view="fresh",
                request_id="synthetic-current-pending-request",
            )
        self.assertEqual(replay, first)
        self.assertIsNone(
            self.core.journal.stream_position(
                self.settings.source_instance_id, self.settings.account_id
            )
        )

    def test_permanent_receive_identity_survives_body_gc_and_exact_backup_restore(self) -> None:
        from sightglass.model.backups import (
            backup_plan,
            create_compressed_snapshot,
            restore_compressed_snapshot,
        )
        from sightglass.runtime.capture_journal import (
            CAPTURE_BATCH_TOOL,
            CAPTURE_RECOVERY_TOOL,
            CAPTURE_REQUEST_TOOL,
            CAPTURE_STREAM_TOOL,
        )
        from sightglass.runtime.control import cleanup_deliveries

        self.append()
        self.start_edge()
        tickets = []
        original = self.core._hooks

        def keep_ticket(ticket):
            tickets.append(ticket)
            return original(ticket)

        with mock.patch.object(self.core, "_hooks", side_effect=keep_ticket):
            result = self.call(
                "wechat_read_messages",
                mode="recent",
                conversation_id=self.group,
                view="fresh",
                limit=10,
            )
        self.assertNotEqual(result.get("ok"), False, result)
        self.stop_edge()
        self.core.close()
        envelope = tickets[0].envelope
        expected = self.core.receiver.lookup_terminal(envelope)
        assert expected is not None
        position = self.core.journal.stream_position(
            self.settings.source_instance_id, self.settings.account_id
        )
        preview = self.service.residency.stock_preview(self.group)
        released = self.service.residency.release_stock(self.group, plan=preview["plan"])
        self.assertGreater(released["released_messages"], 0)
        cleanup_deliveries(self.repository.database, apply=True)
        reserved = (
            CAPTURE_BATCH_TOOL,
            CAPTURE_STREAM_TOOL,
            CAPTURE_REQUEST_TOOL,
            CAPTURE_RECOVERY_TOOL,
        )
        with self.repository.database.transaction() as connection:
            connection.execute(
                "DELETE FROM access_receipts WHERE tool_name NOT IN (?,?,?,?)", reserved
            )
        path = self.repository.database.path
        created = create_compressed_snapshot(path)
        snapshots = backup_plan(path)["compressed_snapshots"]
        snapshot = next(item for item in snapshots if item["artifact"] == created["artifact"])
        restore_compressed_snapshot(path, snapshot["artifact"], snapshot["restore_ack"])
        prepared = self.core.receiver.prepare(envelope)
        with mock.patch.object(
            self.core.journal, "record_terminal", side_effect=AssertionError("duplicate new commit")
        ):
            duplicate = self.core.receiver.admit(
                prepared,
                lambda *_: self.fail("duplicate re-admitted body"),
                receipt_id="synthetic-new-receipt-must-not-replace-old",
                now="2099-01-01T00:00:00+00:00",
            )
        self.assertEqual(duplicate, expected)
        self.assertEqual(
            self.core.journal.stream_position(
                self.settings.source_instance_id, self.settings.account_id
            ),
            position,
        )

    def _assert_terminal_consistency(self, ticket, terminal, delivery_id) -> None:
        from sightglass.runtime.capture_journal import CAPTURE_REQUEST_TOOL, _identity

        ack = self.core.receiver.lookup_terminal(ticket.envelope)
        self.assertIsNotNone(ack)
        assert ack is not None
        self.assertEqual(ack.terminal, terminal)
        self.assertEqual(ticket.durable_ack, ack)
        with self.repository.database.connection() as connection:
            outcome = connection.execute(
                "SELECT outcome FROM access_receipts WHERE tool_name=? AND receipt_id=?",
                (CAPTURE_REQUEST_TOOL, _identity("request", ticket.request.request_id)),
            ).fetchone()
            admitted = connection.execute(
                "SELECT 1 FROM messages WHERE source_message_id='synthetic-capture-next'"
            ).fetchone()
            requests = connection.execute(
                "SELECT COUNT(*) FROM access_receipts "
                "WHERE tool_name='_sightglass_updates_request.v1'"
            ).fetchone()
        self.assertEqual(outcome[0], terminal)
        self.assertEqual(admitted is not None, terminal == "accepted")
        self.assertEqual(requests[0], int(terminal == "accepted"))
        delivery = self.repository.delivery(delivery_id)
        assert delivery is not None
        self.assertEqual(
            delivery["status"],
            "acknowledged" if terminal == "accepted" else "pending",
        )

    def test_reject_winning_before_writer_rolls_back_body_ack_and_delivery(self) -> None:
        first = self.call("wechat_read_messages", mode="updates", conversation_id=self.group)
        delivery_id = first["page"]["delivery_id"]
        self.append()
        self.start_edge()
        captured, release = threading.Event(), threading.Event()
        tickets, results = [], []
        original = self.core._hooks

        def wait_before_writer(ticket):
            tickets.append(ticket)
            captured.set()
            if not release.wait(2):
                raise RuntimeError("synthetic barrier timed out")
            return original(ticket)

        def call():
            results.append(
                self.call(
                    "wechat_read_messages",
                    mode="updates",
                    conversation_id=self.group,
                    view="fresh",
                    ack_delivery_id=delivery_id,
                    request_id="synthetic-reject-wins",
                )
            )

        with mock.patch.object(self.core, "_hooks", side_effect=wait_before_writer):
            thread = threading.Thread(target=call)
            thread.start()
            try:
                self.assertTrue(captured.wait(2))
                self.core._reject(tickets[0])
            finally:
                release.set()
                thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(results[0].get("ok"), False, results)
        self._assert_terminal_consistency(tickets[0], "rejected", delivery_id)

    def test_accepted_writer_wins_and_late_reject_preserves_exact_terminal(self) -> None:
        first = self.call("wechat_read_messages", mode="updates", conversation_id=self.group)
        delivery_id = first["page"]["delivery_id"]
        self.append()
        self.start_edge()
        claimed, release, rejecting = threading.Event(), threading.Event(), threading.Event()
        tickets, results, failures = [], [], []
        original = self.core._hooks

        def pause_inside_writer(ticket):
            tickets.append(ticket)
            before, after = original(ticket)

            def hook(connection):
                before(connection)
                claimed.set()
                if not release.wait(2):
                    raise RuntimeError("synthetic barrier timed out")

            return hook, after

        def call():
            results.append(
                self.call(
                    "wechat_read_messages",
                    mode="updates",
                    conversation_id=self.group,
                    view="fresh",
                    ack_delivery_id=delivery_id,
                    request_id="synthetic-accept-wins",
                )
            )

        def reject():
            rejecting.set()
            try:
                self.core._reject(tickets[0])
            except BaseException as error:
                failures.append(error)

        with mock.patch.object(self.core, "_hooks", side_effect=pause_inside_writer):
            thread = threading.Thread(target=call)
            thread.start()
            reject_thread = None
            try:
                self.assertTrue(claimed.wait(2))
                reject_thread = threading.Thread(target=reject)
                reject_thread.start()
                self.assertTrue(rejecting.wait(1))
            finally:
                release.set()
                thread.join(3)
                if reject_thread is not None:
                    reject_thread.join(3)
        self.assertFalse(thread.is_alive())
        assert reject_thread is not None
        self.assertFalse(reject_thread.is_alive())
        self.assertEqual(failures, [])
        self.assertNotEqual(results[0].get("ok"), False, results)
        self._assert_terminal_consistency(tickets[0], "accepted", delivery_id)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import io
import json
import socket
import subprocess
import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from unittest import mock

from sightglass.contracts.capture import (
    CAPTURE_VERSION,
    CaptureAck,
    CaptureCeiling,
    CaptureExpectation,
    CaptureProtocolError,
)
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.operations import operation_budget
from sightglass.runtime.edge import EdgeSpool
from sightglass.runtime.edge_relay import (
    CaptureBroker,
    CaptureBrokerServer,
    CaptureWaitTimeout,
    EdgeSession,
    FramedStream,
    PreparedRemoteCapture,
    RelayDisconnected,
    edge_token_hash,
)
from sightglass.source.capture.codec import SealedCapture, json_value
from sightglass.source.capture.executor import CaptureExecutor
from sightglass.source.capture.receiver import CaptureReceiver
from tests.unit import test_capture_boundary as fixtures


class CaptureEdgeRelayTests(unittest.TestCase):
    root: Path
    source: Path
    executor: CaptureExecutor
    ceiling: CaptureCeiling
    expected: CaptureExpectation
    setUp = fixtures.CaptureBoundaryTests.setUp
    request = fixtures.CaptureBoundaryTests.request
    capture = fixtures.CaptureBoundaryTests.capture

    @contextmanager
    def _live_channel(self, broker):
        server = CaptureBrokerServer(broker, self.root / "live-broker.sock")
        server.start()
        try:
            with socket.socket(socket.AF_UNIX) as connection:
                connection.connect(str(server.socket_path))
                with connection.makefile("rb") as reader, connection.makefile("wb") as writer:
                    channel = FramedStream(reader, writer)
                    channel.send({
                        "schema": CAPTURE_VERSION, "kind": "hello", "role": "edge",
                        "token": "synthetic-token",
                        "source_instance_id": self.expected.source_instance_id,
                        "account_id": self.expected.account_id,
                        "origin_epoch": self.expected.origin_epoch,
                        "stream_epoch": "fixture-stream", "next_sequence": 1,
                        "core_generation": "",
                    })
                    self.assertEqual(channel.receive(timeout=2), {
                        "schema": CAPTURE_VERSION, "kind": "ready", "core_generation": "",
                    })
                    yield channel
        finally:
            server.close()

    def test_unissued_queued_cancel_and_timeout_never_reach_relay_and_release_capacity(self):
        for reason in ("cancelled", "broker_timeout", "operation_deadline"):
            with self.subTest(reason=reason):
                broker = CaptureBroker(
                    self.expected, token_hash=edge_token_hash("synthetic-token"),
                    lookup_terminal=lambda _: None, max_pending_requests=1,
                )
                cancelled = threading.Event()
                request = self.request()

                def wait():
                    with operation_budget(
                        .05 if reason == "operation_deadline" else 5, cancelled=cancelled
                    ):
                        return broker.submit(
                            request, timeout=.05 if reason == "broker_timeout" else 3
                        )

                with ThreadPoolExecutor(max_workers=1) as pool:
                    waiter = pool.submit(wait)
                    try:
                        with broker._condition:
                            self.assertTrue(broker._condition.wait_for(
                                lambda: bool(broker._queue), 1))
                            original = broker._queue[0]
                            self.assertIsNone(broker._inflight)
                        if reason == "cancelled":
                            cancelled.set()
                        error = (
                            CaptureWaitTimeout if reason == "broker_timeout" else SightglassError
                        )
                        with self.assertRaises(error):
                            waiter.result(timeout=1)
                        self.assertEqual(original.waiters, 0)
                        self.assertIsNone(original.ticket)
                        with self._live_channel(broker) as channel:
                            channel.send({"schema": CAPTURE_VERSION, "kind": "poll"})
                            self.assertEqual(channel.receive(timeout=2), {
                                "schema": CAPTURE_VERSION, "kind": "idle", "retry_seconds": 1,
                            }, "an abandoned queued request escaped to the edge")
                            self.assertEqual(broker._by_request, {})
                            self.assertEqual(len(broker._queue), 0)
                            self.assertIsNone(broker._inflight)
                            fresh = replace(request, request_id="synthetic-fresh-after-cancel")
                            next_waiter = pool.submit(broker.submit, fresh, timeout=2)
                            with broker._condition:
                                self.assertTrue(broker._condition.wait_for(
                                    lambda: bool(broker._queue), 1))
                            channel.send({"schema": CAPTURE_VERSION, "kind": "poll"})
                            work = channel.receive(timeout=2)
                            assert isinstance(work, dict)
                            self.assertEqual(work["kind"], "work")
                            self.assertEqual(work["request"]["request_id"], fresh.request_id)
                            channel.send({
                                "schema": CAPTURE_VERSION, "kind": "failed",
                                "request_id": fresh.request_id, "reason": "capture_unsealed",
                            })
                            with self.assertRaises(CaptureProtocolError):
                                next_waiter.result(timeout=1)
                    finally:
                        broker.close()

    def test_cancelled_queued_waiter_keeps_joined_waiter_and_one_relay_capture(self):
        journal = fixtures.FixtureReceiveJournal(self.root / "joined-receive.sqlite")
        receiver = CaptureReceiver(journal, self.expected, stream_epoch="fixture-stream")
        broker = CaptureBroker(
            self.expected, token_hash=edge_token_hash("synthetic-token"),
            lookup_terminal=receiver.lookup_terminal, max_pending_requests=1,
        )
        cancelled = threading.Event()
        request = self.request()

        def wait():
            with operation_budget(5, cancelled=cancelled):
                return broker.submit(request, timeout=3)

        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(wait)
            second = pool.submit(broker.submit, request, timeout=3)
            try:
                with broker._condition:
                    self.assertTrue(broker._condition.wait_for(
                        lambda: (request.request_id in broker._by_request
                                 and broker._by_request[request.request_id].waiters == 2), 1))
                    work = broker._by_request[request.request_id]
                cancelled.set()
                with self.assertRaises(SightglassError):
                    first.result(timeout=1)
                self.assertEqual(work.waiters, 1)
                self.assertFalse(work.detached)
                self.assertIs(broker._queue[0], work)
                with self._live_channel(broker) as channel:
                    channel.send({"schema": CAPTURE_VERSION, "kind": "poll"})
                    frame = channel.receive(timeout=2)
                    assert isinstance(frame, dict)
                    self.assertEqual(frame["kind"], "work")
                    self.assertEqual(frame["request"]["request_id"], request.request_id)
                    envelope = self.capture(request)
                    channel.send(envelope)
                    ticket = second.result(timeout=1)
                    self.assertIs(work.ticket, ticket)
                    self.assertIsNone(ticket.durable_ack)
                    self.assertIsNone(receiver.lookup_terminal(envelope))
                    ack = receiver.admit(
                        receiver.prepare(envelope), journal.append,
                        receipt_id="synthetic-joined-commit",
                    )
                    ticket.complete(ack)
                    self.assertEqual(channel.receive(timeout=2), {
                        "schema": CAPTURE_VERSION, "kind": "ack", "ack": json_value(ack),
                    })
                    channel.send({"schema": CAPTURE_VERSION, "kind": "poll"})
                    self.assertEqual(channel.receive(timeout=2), {
                        "schema": CAPTURE_VERSION, "kind": "idle", "retry_seconds": 1,
                    })
                    self.assertEqual(broker._by_request, {})
                    self.assertIsNone(broker._inflight)
            finally:
                broker.close()

    def test_content_free_diagnostics_distinguish_queue_capture_and_terminal_recovery(self):
        journal = fixtures.FixtureReceiveJournal(self.root / "diagnostics-receive.sqlite")
        receiver = CaptureReceiver(journal, self.expected, stream_epoch="fixture-stream")
        recovering = threading.Event()
        commit = threading.Event()
        committed = threading.Event()
        finish = threading.Event()

        def recover(ticket):
            recovering.set()
            if not commit.wait(3):
                raise RuntimeError("synthetic diagnostics commit barrier expired")
            ack = receiver.admit(
                receiver.prepare(ticket.envelope), None,
                receipt_id="synthetic-diagnostics-reject", reject=True,
            )
            ticket.complete(ack)
            committed.set()
            if not finish.wait(3):
                raise RuntimeError("synthetic diagnostics finish barrier expired")

        broker = CaptureBroker(
            self.expected, token_hash=edge_token_hash("synthetic-token"),
            lookup_terminal=receiver.lookup_terminal, recover=recover,
        )
        snapshots = [broker.diagnostics()]
        self.assertEqual(snapshots[0], {"queued": 0, "detached_queued": 0, "inflight": None})
        with ThreadPoolExecutor(max_workers=1) as pool:
            waiter = pool.submit(broker.submit, self.request(), timeout=3)
            try:
                with broker._condition:
                    self.assertTrue(broker._condition.wait_for(lambda: bool(broker._queue), 1))
                snapshots.append(broker.diagnostics())
                self.assertEqual(snapshots[-1], {
                    "queued": 1, "detached_queued": 0, "inflight": None,
                })
                with self._live_channel(broker) as channel:
                    channel.send({"schema": CAPTURE_VERSION, "kind": "poll"})
                    frame = channel.receive(timeout=2)
                    assert isinstance(frame, dict)
                    self.assertEqual(frame["kind"], "work")
                    phase = {
                        "operation": self.request().operation, "captured": False,
                        "detached": False, "recovering": False, "waiters": 1,
                        "durable_terminal": False,
                    }
                    snapshots.append(broker.diagnostics())
                    self.assertEqual(snapshots[-1], {
                        "queued": 0, "detached_queued": 0, "inflight": phase,
                    })
                    channel.send(self.capture())
                    ticket = waiter.result(timeout=1)
                    phase = {**phase, "captured": True, "waiters": 0}
                    snapshots.append(broker.diagnostics())
                    self.assertEqual(snapshots[-1]["inflight"], phase)
                    broker.abandon(ticket)
                    self.assertTrue(recovering.wait(1))
                    phase = {**phase, "detached": True, "recovering": True}
                    snapshots.append(broker.diagnostics())
                    self.assertEqual(snapshots[-1]["inflight"], phase)
                    commit.set()
                    self.assertTrue(committed.wait(1))
                    snapshots.append(broker.diagnostics())
                    self.assertEqual(snapshots[-1]["inflight"], {
                        **phase, "durable_terminal": True,
                    })
                    finish.set()
                    ack = channel.receive(timeout=2)
                    assert isinstance(ack, dict)
                    self.assertEqual(ack["kind"], "ack")
                    channel.send({"schema": CAPTURE_VERSION, "kind": "poll"})
                    self.assertEqual(channel.receive(timeout=2), {
                        "schema": CAPTURE_VERSION, "kind": "idle", "retry_seconds": 1,
                    })
                    snapshots.append(broker.diagnostics())
                    self.assertEqual(snapshots[-1], snapshots[0])
            finally:
                commit.set()
                finish.set()
                broker.close()
        for snapshot in snapshots:
            self.assertEqual(set(snapshot), {"queued", "detached_queued", "inflight"})
            if snapshot["inflight"] is not None:
                self.assertEqual(set(snapshot["inflight"]), {
                    "operation", "captured", "detached", "recovering", "waiters",
                    "durable_terminal",
                })
        encoded = json.dumps(snapshots)
        for sensitive in (
            self.request().request_id, self.ceiling.account_id,
            self.executor.source_instance_id, "conv_group", str(self.root), "synthetic-token",
            "synthetic-diagnostics-reject",
        ):
            self.assertNotIn(sensitive, encoded)

    def test_cancelled_waiter_yields_and_late_capture_is_rejected_exactly(self) -> None:
        journal = fixtures.FixtureReceiveJournal(self.root / "cancelled-receive.sqlite")
        receiver = CaptureReceiver(journal, self.expected, stream_epoch="fixture-stream")

        def recover(ticket):
            ack = receiver.admit(
                receiver.prepare(ticket.envelope), None,
                receipt_id="synthetic-cancel-reject", reject=True,
            )
            ticket.complete(ack)

        broker = CaptureBroker(
            self.expected, token_hash=edge_token_hash("synthetic-token"),
            lookup_terminal=receiver.lookup_terminal, recover=recover,
        )
        cancelled = threading.Event()
        request = self.request()

        def wait():
            with operation_budget(5, cancelled=cancelled):
                return broker.submit(request, timeout=3)

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(wait)
            try:
                with broker._condition:
                    self.assertTrue(broker._condition.wait_for(lambda: bool(broker._queue), 1))
                work = broker._next_work()
                assert work is not None
                cancelled.set()
                with self.assertRaises(SightglassError) as caught:
                    future.result(timeout=.5)
                self.assertEqual(caught.exception.code, ErrorCode.SERVICE_TIMEOUT)
                self.assertEqual(caught.exception.details["reason"], "operation_cancelled")
                self.assertEqual(work.waiters, 0)
                self.assertTrue(work.detached)
                self.assertIs(broker._inflight, work)
                envelope = self.capture(request)
                ticket = broker._receive_capture(envelope)
                assert isinstance(ticket, PreparedRemoteCapture)
                ack = ticket.durable_ack
                assert ack is not None
                self.assertEqual(ack.terminal, "rejected")
                self.assertEqual(receiver.lookup_terminal(envelope), ack)
                self.assertEqual(ticket.envelope.to_bytes(), envelope.to_bytes())
            finally:
                broker.close()

    def test_operation_deadline_detaches_waiter_before_broker_timeout(self) -> None:
        broker = CaptureBroker(
            self.expected, token_hash=edge_token_hash("synthetic-token"),
            lookup_terminal=lambda _capture: None,
        )

        def wait():
            with operation_budget(.05):
                return broker.submit(self.request(), timeout=3)

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(wait)
            try:
                with broker._condition:
                    self.assertTrue(broker._condition.wait_for(lambda: bool(broker._queue), 1))
                    work = broker._queue[0]
                with self.assertRaises(SightglassError) as caught:
                    future.result(timeout=.5)
                self.assertEqual(caught.exception.code, ErrorCode.SERVICE_TIMEOUT)
                self.assertEqual(work.waiters, 0)
                self.assertTrue(work.detached)
            finally:
                broker.close()

    def test_abandoned_returned_ticket_recovers_before_finalization_timeout(self) -> None:
        journal = fixtures.FixtureReceiveJournal(self.root / "abandoned-receive.sqlite")
        receiver = CaptureReceiver(journal, self.expected, stream_epoch="fixture-stream")

        def recover(ticket):
            ack = receiver.admit(
                receiver.prepare(ticket.envelope), None,
                receipt_id="synthetic-abandoned-reject", reject=True,
            )
            ticket.complete(ack)

        broker = CaptureBroker(
            self.expected, token_hash=edge_token_hash("synthetic-token"),
            lookup_terminal=receiver.lookup_terminal, recover=recover,
            finalization_timeout=2,
        )
        request = self.request()
        envelope = self.capture(request)
        channel, outgoing = self._pending_session(envelope, poll=True)

        def serve():
            with self.assertRaises(RelayDisconnected):
                broker.serve_connection(channel)

        with ThreadPoolExecutor(max_workers=2) as pool:
            waiter = pool.submit(broker.submit, request, timeout=2)
            try:
                with broker._condition:
                    self.assertTrue(broker._condition.wait_for(lambda: bool(broker._queue), 1))
                server = pool.submit(serve)
                ticket = waiter.result(timeout=1)
                broker.abandon(ticket)
                server.result(timeout=.5)
                ack = ticket.durable_ack
                assert ack is not None
                self.assertEqual(ack.terminal, "rejected")
                self.assertEqual(receiver.lookup_terminal(envelope), ack)
                responses = FramedStream(io.BytesIO(outgoing.getvalue()), io.BytesIO())
                ready = responses.receive()
                assert isinstance(ready, dict)
                self.assertEqual(ready["kind"], "ready")
                frame = responses.receive()
                assert isinstance(frame, dict)
                self.assertEqual(frame["kind"], "work")
                self.assertEqual(responses.receive(), {
                    "schema": CAPTURE_VERSION, "kind": "ack", "ack": json_value(ack),
                })
            finally:
                broker.close()

    def _pending_session(
        self, envelope: SealedCapture, *, poll: bool = False
    ) -> tuple[FramedStream, io.BytesIO]:
        wire, outgoing = io.BytesIO(), io.BytesIO()
        writer = FramedStream(io.BytesIO(), wire)
        writer.send(
            {
                "schema": CAPTURE_VERSION,
                "kind": "hello",
                "role": "edge",
                "token": "synthetic-token",
                "source_instance_id": self.executor.source_instance_id,
                "account_id": self.ceiling.account_id,
                "origin_epoch": self.executor.origin_epoch,
                "stream_epoch": "fixture-stream",
                "next_sequence": 2,
                "core_generation": "",
            }
        )
        if poll:
            writer.send({"schema": CAPTURE_VERSION, "kind": "poll"})
        writer.send(envelope)
        return FramedStream(io.BytesIO(wire.getvalue()), outgoing), outgoing

    def _unfinalized_ticket(
        self, broker: CaptureBroker, envelope: SealedCapture
    ) -> PreparedRemoteCapture:
        with ThreadPoolExecutor(max_workers=1) as pool:
            waiter = pool.submit(broker.submit, envelope.document().request, timeout=2)
            with broker._condition:
                self.assertTrue(broker._condition.wait_for(lambda: bool(broker._queue), 1))
            channel, outgoing = self._pending_session(envelope, poll=True)
            broker.serve_connection(channel)
            ticket = waiter.result(timeout=1)
        responses = FramedStream(io.BytesIO(outgoing.getvalue()), io.BytesIO())
        self.assertEqual(
            responses.receive(), {"schema": CAPTURE_VERSION, "kind": "ready", "core_generation": ""}
        )
        work = responses.receive()
        assert isinstance(work, dict)
        self.assertEqual(work["kind"], "work")
        self.assertEqual(responses.receive(), {"schema": CAPTURE_VERSION, "kind": "retry"})
        self.assertIsNone(ticket.durable_ack)
        self.assertFalse(broker.connected)
        return ticket

    def test_existing_unfinalized_catalog_recovers_on_exact_replay_after_timeout(self) -> None:
        journal = fixtures.FixtureReceiveJournal(self.root / "receive.sqlite")
        receiver = CaptureReceiver(journal, self.expected, stream_epoch="fixture-stream")
        recovered: list[PreparedRemoteCapture] = []

        def recover(ticket):
            recovered.append(ticket)
            ack = receiver.admit(
                receiver.prepare(ticket.envelope), None,
                receipt_id="synthetic-timeout-reject", reject=True,
            )
            ticket.complete(ack)

        broker = CaptureBroker(
            self.expected, token_hash=edge_token_hash("synthetic-token"),
            lookup_terminal=receiver.lookup_terminal, recover=recover,
            finalization_timeout=0.01,
        )
        self.addCleanup(broker.close)
        envelope = self.capture(self.request("catalog"))
        spool = EdgeSpool.initialize(
            self.root / "timeout-spool", source_instance_id=self.executor.source_instance_id,
            account_id=self.ceiling.account_id, origin_epoch=self.executor.origin_epoch,
            stream_epoch="fixture-stream",
        )
        self.addCleanup(spool.close)
        spool.store(envelope)
        ticket = self._unfinalized_ticket(broker, envelope)
        self.assertEqual(recovered, [])
        self.assertIsNone(receiver.lookup_terminal(envelope))
        pending = spool.pending()
        assert pending is not None
        self.assertEqual(pending.to_bytes(), envelope.to_bytes())
        channel, outgoing = self._pending_session(envelope)
        with self.assertRaises(RelayDisconnected):
            broker.serve_connection(channel)
        self.assertEqual(recovered, [ticket])
        ack = ticket.durable_ack
        assert ack is not None
        self.assertEqual(ack.terminal, "rejected")
        self.assertEqual(ticket.envelope.to_bytes(), envelope.to_bytes())
        self.assertEqual(receiver.lookup_terminal(envelope), ack)
        responses = FramedStream(io.BytesIO(outgoing.getvalue()), io.BytesIO())
        self.assertEqual(
            responses.receive(), {"schema": CAPTURE_VERSION, "kind": "ready", "core_generation": ""}
        )
        self.assertEqual(
            responses.receive(),
            {"schema": CAPTURE_VERSION, "kind": "ack", "ack": json_value(ack)},
        )
        with journal.connection() as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM episodes").fetchone()[0], 0)
            self.assertEqual(
                connection.execute("SELECT position FROM reader_state").fetchone()[0], 0
            )
        spool.acknowledge(ack)
        self.assertIsNone(spool.pending())
        self.assertEqual(spool.next_sequence, 2)
        self.assertEqual(broker._by_request, {})

    def test_existing_ticket_retries_recovery_after_terminal_transaction_rollback(self) -> None:
        journal = fixtures.FixtureReceiveJournal(self.root / "receive.sqlite")
        receiver = CaptureReceiver(journal, self.expected, stream_epoch="fixture-stream")
        attempts: list[PreparedRemoteCapture] = []

        def recover(ticket):
            attempts.append(ticket)
            ack = receiver.admit(
                receiver.prepare(ticket.envelope), None,
                receipt_id="synthetic-retry-reject", reject=True,
            )
            ticket.complete(ack)

        broker = CaptureBroker(
            self.expected, token_hash=edge_token_hash("synthetic-token"),
            lookup_terminal=receiver.lookup_terminal, recover=recover,
            finalization_timeout=0.01,
        )
        self.addCleanup(broker.close)
        envelope = self.capture(self.request("catalog"))
        ticket = self._unfinalized_ticket(broker, envelope)
        original = journal.record_terminal

        def fail_after_write(document, ack):
            original(document, ack)
            raise RuntimeError("synthetic temporary terminal failure")

        channel, outgoing = self._pending_session(envelope)
        with mock.patch.object(journal, "record_terminal", side_effect=fail_after_write):
            with self.assertRaisesRegex(RuntimeError, "temporary terminal failure"):
                broker.serve_connection(channel)
        self.assertIsNone(ticket.durable_ack)
        self.assertIsNone(receiver.lookup_terminal(envelope))
        self.assertIsNone(
            journal.stream_position(self.executor.source_instance_id, self.ceiling.account_id)
        )
        responses = FramedStream(io.BytesIO(outgoing.getvalue()), io.BytesIO())
        self.assertEqual(
            responses.receive(), {"schema": CAPTURE_VERSION, "kind": "ready", "core_generation": ""}
        )
        with self.assertRaises(RelayDisconnected):
            responses.receive()  # No ACK escaped the rolled-back terminal writer.
        channel, outgoing = self._pending_session(envelope)
        with self.assertRaises(RelayDisconnected):
            broker.serve_connection(channel)
        self.assertEqual(attempts, [ticket, ticket])
        self.assertEqual(ticket.envelope.to_bytes(), envelope.to_bytes())
        ack = ticket.durable_ack
        assert ack is not None
        self.assertEqual(receiver.lookup_terminal(envelope), ack)
        self.assertEqual(ack.terminal, "rejected")

    def test_active_ticket_exact_replay_does_not_start_recovery_before_deadline(self) -> None:
        recovered: list[PreparedRemoteCapture] = []
        broker = CaptureBroker(
            self.expected, token_hash=edge_token_hash("synthetic-token"),
            lookup_terminal=lambda _capture: None, recover=recovered.append,
        )
        self.addCleanup(broker.close)
        envelope = self.capture()
        with ThreadPoolExecutor(max_workers=1) as pool:
            waiter = pool.submit(broker.submit, envelope.document().request, timeout=2)
            with broker._condition:
                self.assertTrue(broker._condition.wait_for(lambda: bool(broker._queue), 1))
            self.assertIsNotNone(broker._next_work())
            ticket = broker._receive_capture(envelope)
            self.assertIs(waiter.result(timeout=1), ticket)
        self.assertIs(broker._receive_capture(envelope), ticket)
        self.assertEqual(recovered, [])
        assert isinstance(ticket, PreparedRemoteCapture)
        self.assertIsNone(ticket.durable_ack)

    def test_exact_replay_recovery_is_singleflight_and_unfinished_attempt_can_retry(self) -> None:
        entered, release = threading.Event(), threading.Event()
        attempts: list[PreparedRemoteCapture] = []

        def recover(ticket):
            attempts.append(ticket)
            entered.set()
            if not release.wait(2):
                raise RuntimeError("synthetic recovery barrier timed out")

        broker = CaptureBroker(
            self.expected, token_hash=edge_token_hash("synthetic-token"),
            lookup_terminal=lambda _capture: None, recover=recover,
            finalization_timeout=0.01,
        )
        self.addCleanup(broker.close)
        envelope = self.capture(self.request("catalog"))
        ticket = self._unfinalized_ticket(broker, envelope)
        with ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(broker._receive_capture, envelope)
            try:
                self.assertTrue(entered.wait(1))
                self.assertIs(broker._receive_capture(envelope), ticket)
                self.assertEqual(attempts, [ticket])
            finally:
                release.set()
            self.assertIs(first.result(timeout=1), ticket)
        self.assertIs(broker._receive_capture(envelope), ticket)
        self.assertEqual(attempts, [ticket, ticket])
        self.assertIsNone(ticket.durable_ack)

    def test_disconnect_detaches_waiters_and_does_not_reissue_inflight_read(self) -> None:
        broker = CaptureBroker(
            self.expected,
            token_hash=edge_token_hash("synthetic-token"),
            lookup_terminal=lambda _capture: None,
        )
        wire = io.BytesIO()
        frames = FramedStream(io.BytesIO(), wire)
        frames.send(
            {
                "schema": CAPTURE_VERSION,
                "kind": "hello",
                "role": "edge",
                "token": "synthetic-token",
                "source_instance_id": self.executor.source_instance_id,
                "account_id": self.ceiling.account_id,
                "origin_epoch": self.executor.origin_epoch,
                "stream_epoch": "fixture-stream",
                "next_sequence": 1,
                "core_generation": "",
            }
        )
        frames.send({"schema": CAPTURE_VERSION, "kind": "poll"})
        with ThreadPoolExecutor(max_workers=2) as pool:
            waiting = [
                pool.submit(broker.submit, self.request(), timeout=5),
                pool.submit(
                    broker.submit, replace(self.request(), request_id="fixture-second"), timeout=5
                ),
            ]
            with broker._condition:
                self.assertTrue(broker._condition.wait_for(lambda: len(broker._by_request) == 2, 2))
            with self.assertRaises(RelayDisconnected):
                broker.serve_connection(FramedStream(io.BytesIO(wire.getvalue()), io.BytesIO()))
            for future in waiting:
                with self.assertRaisesRegex(RelayDisconnected, "before_capture"):
                    future.result(timeout=1)
        self.assertEqual(broker._by_request, {})
        self.assertIsNone(broker._inflight)
        self.assertEqual(len(broker._queue), 0)
        self.assertFalse(broker.connected)

    def test_wrong_request_or_generation_control_disconnects_and_preserves_terminal(self) -> None:
        from sightglass.operations import check_operation_budget

        for wrong in ("request_id", "core_generation"):
            with self.subTest(wrong=wrong), EdgeSpool.initialize(
                self.root / ("wrong-control-" + wrong),
                source_instance_id=self.executor.source_instance_id,
                account_id=self.ceiling.account_id,
                origin_epoch=self.executor.origin_epoch,
                stream_epoch="fixture-stream",
            ) as spool:
                request = self.request()
                reply = {"schema": CAPTURE_VERSION, "kind": "cancel",
                         "request_id": request.request_id, "core_generation": ""}
                reply[wrong] = "synthetic-wrong-binding"
                wire = io.BytesIO()
                FramedStream(io.BytesIO(), wire).send(reply)

                def slow(*args):
                    while True:
                        check_operation_budget()

                with mock.patch.object(self.executor, "_execute", side_effect=slow):
                    with self.assertRaisesRegex(RelayDisconnected, "control_disconnected"):
                        edge = EdgeSession(self.executor, spool, token="synthetic-token")
                        edge._capture_controlled(
                            FramedStream(io.BytesIO(wire.getvalue()), io.BytesIO()),
                            request, 1.0, threading.Event(),
                        )
                pending = spool.pending()
                assert pending is not None
                self.assertEqual(pending.document().request, request)
                self.assertEqual(pending.document().receipt.terminal, "cancelled")
                self.assertEqual(spool.next_sequence, 2)

    def test_invalid_relative_budget_never_starts_source(self) -> None:
        with EdgeSpool.initialize(
            self.root / "invalid-budget-spool",
            source_instance_id=self.executor.source_instance_id,
            account_id=self.ceiling.account_id,
            origin_epoch=self.executor.origin_epoch,
            stream_epoch="fixture-stream",
        ) as spool, mock.patch.object(self.executor, "capture") as capture:
            for budget in (float("nan"), float("inf"), -1, 121, True, "1"):
                with self.subTest(budget=budget), self.assertRaises(CaptureProtocolError):
                    EdgeSession(self.executor, spool, token="synthetic-token")._capture_controlled(
                        FramedStream(io.BytesIO(), io.BytesIO()), self.request(), budget,
                        threading.Event(),
                    )
            capture.assert_not_called()
            self.assertIsNone(spool.pending())
            self.assertEqual(spool.next_sequence, 1)

    def test_new_edge_rejects_legacy_core_ready_without_source_execution(self) -> None:
        wire = io.BytesIO()
        FramedStream(io.BytesIO(), wire).send({
            "schema": CAPTURE_VERSION, "kind": "ready", "core_generation": "",
        })
        with EdgeSpool.initialize(
            self.root / "legacy-core-spool",
            source_instance_id=self.executor.source_instance_id,
            account_id=self.ceiling.account_id,
            origin_epoch=self.executor.origin_epoch,
            stream_epoch="fixture-stream",
        ) as spool, mock.patch.object(self.executor, "capture") as capture:
            with self.assertRaisesRegex(CaptureProtocolError, "handshake_rejected"):
                EdgeSession(self.executor, spool, token="synthetic-token").run(
                    FramedStream(io.BytesIO(wire.getvalue()), io.BytesIO())
                )
            capture.assert_not_called()
            self.assertEqual(spool.next_sequence, 1)

    def test_unsealed_failure_control_is_content_free_and_consumes_no_sequence(self) -> None:
        from unittest import mock

        stop = threading.Event()
        incoming = io.BytesIO()
        frames = FramedStream(io.BytesIO(), incoming)
        frames.send({"schema": CAPTURE_VERSION, "kind": "ready", "core_generation": "",
                     "transport_revision": 2})
        frames.send(
            {"schema": CAPTURE_VERSION, "kind": "work", "request": json_value(self.request()),
             "remaining_seconds": 5.0}
        )
        outgoing = io.BytesIO()

        class StopAfterFailure(FramedStream):
            def send(self, value, **kwargs):
                super().send(value, **kwargs)
                if isinstance(value, dict) and value.get("kind") == "failed":
                    stop.set()

        with EdgeSpool.initialize(
            self.root / "unsealed-spool",
            source_instance_id=self.executor.source_instance_id,
            account_id=self.ceiling.account_id,
            origin_epoch=self.executor.origin_epoch,
            stream_epoch="fixture-stream",
        ) as spool:
            with mock.patch.object(
                EdgeSession, "_capture_controlled",
                side_effect=CaptureProtocolError(str(self.root))
            ):
                EdgeSession(self.executor, spool, token="synthetic-token").run(
                    StopAfterFailure(io.BytesIO(incoming.getvalue()), outgoing), stop=stop
                )
            self.assertEqual(spool.next_sequence, 1)
            self.assertIsNone(spool.pending())
        self.assertNotIn(str(self.root).encode(), outgoing.getvalue())
        broker = CaptureBroker(
            self.expected,
            token_hash=edge_token_hash("synthetic-token"),
            lookup_terminal=lambda _capture: None,
        )
        with ThreadPoolExecutor(max_workers=1) as pool:
            waiter = pool.submit(broker.submit, self.request(), timeout=5)
            with self.assertRaises(RelayDisconnected):
                broker.serve_connection(FramedStream(io.BytesIO(outgoing.getvalue()), io.BytesIO()))
            with self.assertRaisesRegex(CaptureProtocolError, "capture_unsealed"):
                waiter.result(timeout=1)
        self.assertEqual(broker._by_request, {})
        self.assertIsNone(broker._inflight)

    def test_private_unix_broker_stdio_proxy_returns_ticket_before_durable_ack(self) -> None:
        journal = fixtures.FixtureReceiveJournal(self.root / "receive.sqlite")
        receiver = CaptureReceiver(journal, self.expected, stream_epoch="fixture-stream")
        token = "synthetic-edge-capability"
        broker = CaptureBroker(
            self.expected,
            token_hash=edge_token_hash(token),
            lookup_terminal=receiver.lookup_terminal,
        )
        server = CaptureBrokerServer(broker, self.root / "broker" / "capture.sock")
        server.start()
        self.addCleanup(server.close)
        proxy = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "from pathlib import Path; import sys; "
                "from sightglass.runtime.edge_relay import run_core_stdio; "
                "run_core_stdio(Path(sys.argv[1]))",
                str(server.socket_path),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        def close_proxy():
            if proxy.poll() is None:
                proxy.kill()
            proxy.wait(timeout=5)
            for stream in (proxy.stdin, proxy.stdout, proxy.stderr):
                if stream is not None:
                    stream.close()

        self.addCleanup(close_proxy)
        assert proxy.stdin is not None and proxy.stdout is not None
        channel = FramedStream(proxy.stdout, proxy.stdin)
        channel.send(
            {
                "schema": CAPTURE_VERSION,
                "kind": "hello",
                "role": "edge",
                "token": token,
                "source_instance_id": self.executor.source_instance_id,
                "account_id": self.ceiling.account_id,
                "origin_epoch": self.executor.origin_epoch,
                "stream_epoch": "fixture-stream",
                "next_sequence": 1,
                "core_generation": "",
            }
        )
        self.assertEqual(
            channel.receive(), {"schema": CAPTURE_VERSION, "kind": "ready", "core_generation": ""}
        )
        # The peer can receive ready before the serving thread publishes its
        # next local state change. Synchronize on that state rather than OS
        # scheduling between the wire write and the condition acquisition.
        with broker._condition:
            self.assertTrue(broker._condition.wait_for(lambda: broker.connected, 5))
        with ThreadPoolExecutor(max_workers=1) as pool:
            waiting = pool.submit(broker.submit, self.request(), timeout=5.0)
            channel.send({"schema": CAPTURE_VERSION, "kind": "poll"})
            work = channel.receive()
            assert isinstance(work, dict)
            self.assertEqual(work["kind"], "work")
            self.assertEqual(work["request"]["request_id"], self.request().request_id)
            envelope = self.capture()
            channel.send(envelope)
            ticket = waiting.result(timeout=5)
            self.assertEqual(ticket.envelope.to_bytes(), envelope.to_bytes())
            self.assertIsNone(journal.batch_receipt(envelope.document().origin.batch_id))
            ack = receiver.admit(
                receiver.prepare(envelope), journal.append, receipt_id="fixture-unix-commit"
            )
            ticket.complete(ack)
            wire_ack = channel.receive()
            assert isinstance(wire_ack, dict)
            self.assertEqual(wire_ack["kind"], "ack")
            self.assertEqual(wire_ack["ack"]["receipt_id"], ack.receipt_id)
        proxy.stdin.close()
        self.assertEqual(proxy.wait(timeout=5), 0)
        proxy.stdout.close()
        assert proxy.stderr is not None
        self.assertEqual(proxy.stderr.read(), b"")
        proxy.stderr.close()
        with broker._condition:
            self.assertTrue(broker._condition.wait_for(lambda: not broker.connected, 5))
        self.assertEqual(broker._by_request, {})

    def test_two_process_disconnect_and_lost_ack_replay_unchanged(self) -> None:
        journal = fixtures.FixtureReceiveJournal(self.root / "receive.sqlite")
        receiver = CaptureReceiver(journal, self.expected, stream_epoch="fixture-stream")
        token = "synthetic-edge-capability"
        broker = CaptureBroker(
            self.expected,
            token_hash=edge_token_hash(token),
            lookup_terminal=receiver.lookup_terminal,
        )
        self.addCleanup(broker.close)
        process_args = [
            sys.executable,
            "-m",
            "tests.fixtures.capture_process",
            str(self.source),
            str(self.root / "spool"),
            token,
        ]
        errors: list[BaseException] = []

        class LoseAck(FramedStream):
            def send(self, value, **kwargs):
                if isinstance(value, dict) and value.get("kind") == "ack":
                    raise RelayDisconnected("fixture deliberately drops terminal ACK")
                super().send(value, **kwargs)

        def run_broker(channel):
            try:
                broker.serve_connection(channel)
            except RelayDisconnected:
                pass
            except BaseException as exc:
                errors.append(exc)

        first = subprocess.Popen(
            [*process_args, "initialize"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.addCleanup(lambda: first.poll() is None and first.kill())
        assert first.stdin is not None and first.stdout is not None
        server = threading.Thread(
            target=run_broker, args=(LoseAck(first.stdout, first.stdin),), daemon=True
        )
        server.start()
        with ThreadPoolExecutor(max_workers=1) as pool:
            ticket = pool.submit(broker.submit, self.request(), timeout=5.0).result(timeout=6.0)
            # Receiving/parsing did not commit ingestion or release the Mac spool.
            self.assertIsNone(journal.batch_receipt(ticket.envelope.document().origin.batch_id))
            ack = receiver.admit(
                receiver.prepare(ticket.envelope),
                journal.append,
                receipt_id="fixture-durable-receipt",
            )
            ticket.complete(ack)
        server.join(timeout=3)
        self.assertFalse(server.is_alive())
        first.stdin.close()
        self.assertEqual(first.wait(timeout=5), 0)
        first.stdout.close()
        assert first.stderr is not None
        self.assertEqual(first.stderr.read(), b"")
        first.stderr.close()
        self.assertFalse(broker.connected)
        expected_bytes = ticket.envelope.to_bytes()
        with EdgeSpool(
            self.root / "spool",
            source_instance_id=self.executor.source_instance_id,
            account_id=self.ceiling.account_id,
            origin_epoch=self.executor.origin_epoch,
        ) as spool:
            pending = spool.pending()
            assert pending is not None
            self.assertEqual(pending.to_bytes(), expected_bytes)
            self.assertEqual(spool.next_sequence, 2)

        second = subprocess.Popen(
            [*process_args, "open"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.addCleanup(lambda: second.poll() is None and second.kill())
        assert second.stdin is not None and second.stdout is not None
        replayed: list[bytes] = []
        replay_done = threading.Event()

        class InspectReplay(FramedStream):
            def receive(self, **kwargs):
                value = super().receive(**kwargs)
                if isinstance(value, SealedCapture):
                    replayed.append(value.to_bytes())
                if isinstance(value, dict) and value.get("kind") == "poll":
                    replay_done.set()  # Child cleared only after the exact durable ACK.
                    raise RelayDisconnected("fixture ends after replay ACK")
                return value

        server = threading.Thread(
            target=run_broker, args=(InspectReplay(second.stdout, second.stdin),), daemon=True
        )
        server.start()
        self.assertTrue(replay_done.wait(5))
        server.join(timeout=3)
        second.stdin.close()
        self.assertEqual(second.wait(timeout=5), 0)
        second.stdout.close()
        assert second.stderr is not None
        self.assertEqual(second.stderr.read(), b"")
        second.stderr.close()
        self.assertEqual(replayed, [expected_bytes])
        self.assertEqual(errors, [])
        with EdgeSpool(
            self.root / "spool",
            source_instance_id=self.executor.source_instance_id,
            account_id=self.ceiling.account_id,
            origin_epoch=self.executor.origin_epoch,
        ) as spool:
            self.assertIsNone(spool.pending())
            self.assertEqual(spool.next_sequence, 2)
        with journal.connection() as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM episodes").fetchone()[0], 1)

    def test_timed_out_owner_late_arrival_enters_terminal_recovery(self) -> None:
        journal = fixtures.FixtureReceiveJournal(self.root / "receive.sqlite")
        receiver = CaptureReceiver(journal, self.expected, stream_epoch="fixture-stream")
        recovered: list[PreparedRemoteCapture] = []

        def recover(ticket):
            recovered.append(ticket)
            ack = receiver.admit(
                receiver.prepare(ticket.envelope),
                None,
                receipt_id="fixture-terminal-recovery",
                reject=True,
            )
            ticket.complete(ack)

        broker = CaptureBroker(
            self.expected,
            token_hash=edge_token_hash("synthetic-token"),
            lookup_terminal=receiver.lookup_terminal,
            recover=recover,
        )
        with ThreadPoolExecutor(max_workers=1) as pool, self._live_channel(broker) as channel:
            waiter = pool.submit(broker.submit, self.request(), timeout=2)
            with broker._condition:
                self.assertTrue(broker._condition.wait_for(lambda: bool(broker._queue), 1))
            channel.send({"schema": CAPTURE_VERSION, "kind": "poll"})
            frame = channel.receive(timeout=1)
            assert isinstance(frame, dict)
            self.assertEqual(frame["kind"], "work")
            with self.assertRaises(CaptureWaitTimeout):
                waiter.result(timeout=3)
            work = broker._inflight
            assert work is not None
            self.assertEqual(work.waiters, 0)
            self.assertTrue(work.detached)
            self.assertIs(broker._by_request[self.request().request_id], work)
            self.assertEqual(len(broker._queue), 0)
            channel.send(self.capture())
            ack = channel.receive(timeout=2)
            assert isinstance(ack, dict)
            self.assertEqual(ack["kind"], "ack")
            self.assertEqual(ack["ack"]["terminal"], "rejected")
            channel.send({"schema": CAPTURE_VERSION, "kind": "poll"})
            self.assertEqual(channel.receive(timeout=2), {
                "schema": CAPTURE_VERSION, "kind": "idle", "retry_seconds": 1,
            })
        self.assertEqual(len(recovered), 1)
        durable_ack = recovered[0].durable_ack
        assert durable_ack is not None
        self.assertEqual(durable_ack.terminal, "rejected")
        with journal.connection() as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM episodes").fetchone()[0], 0)
        self.assertFalse(broker.connected)
        self.assertEqual(broker._by_request, {})

    def test_old_core_generation_is_rejected_before_work_or_replay(self) -> None:
        broker = CaptureBroker(
            self.expected,
            token_hash=edge_token_hash("synthetic-token"),
            lookup_terminal=lambda _capture: self.fail("old owner was admitted"),
            core_generation="synthetic-owner-next",
        )
        wire = io.BytesIO()
        channel = FramedStream(io.BytesIO(), wire)
        channel.send(
            {
                "schema": CAPTURE_VERSION,
                "kind": "hello",
                "role": "edge",
                "token": "synthetic-token",
                "source_instance_id": self.executor.source_instance_id,
                "account_id": self.ceiling.account_id,
                "origin_epoch": self.executor.origin_epoch,
                "stream_epoch": "fixture-stream",
                "next_sequence": 1,
                "core_generation": "synthetic-owner-old",
            }
        )
        outgoing = io.BytesIO()
        with self.assertRaisesRegex(CaptureProtocolError, "capability_rejected"):
            broker.serve_connection(FramedStream(io.BytesIO(wire.getvalue()), outgoing))
        self.assertEqual(outgoing.getvalue(), b"")
        self.assertFalse(broker.connected)

    def test_revoked_edge_does_not_release_pending_on_delayed_accepted_ack(self) -> None:
        envelope = self.capture()
        document = envelope.document()
        ack = CaptureAck(
            document.origin.stream_epoch,
            document.origin.sequence,
            document.origin.batch_id,
            envelope.digest,
            document.request.request_id,
            "accepted",
            "synthetic-existing-ack",
        )
        owned = [True]

        def guard():
            if not owned[0]:
                raise RuntimeError("synthetic edge revoked")

        class RevokingAck(FramedStream):
            def receive(self, **kwargs):
                value = super().receive(**kwargs)
                if isinstance(value, dict) and value.get("kind") == "ack":
                    owned[0] = False
                return value

        wire = io.BytesIO()
        channel = FramedStream(io.BytesIO(), wire)
        channel.send(
            {"schema": CAPTURE_VERSION, "kind": "ready", "core_generation": "synthetic-core",
             "transport_revision": 2}
        )
        channel.send({"schema": CAPTURE_VERSION, "kind": "ack", "ack": json_value(ack)})
        with EdgeSpool.initialize(
            self.root / "revoked-spool",
            source_instance_id=self.executor.source_instance_id,
            account_id=self.ceiling.account_id,
            origin_epoch=self.executor.origin_epoch,
            stream_epoch="fixture-stream",
        ) as spool:
            spool.store(envelope)
            with self.assertRaisesRegex(RuntimeError, "edge revoked"):
                EdgeSession(
                    self.executor,
                    spool,
                    token="synthetic-token",
                    core_generation="synthetic-core",
                    ownership_guard=guard,
                ).run(RevokingAck(io.BytesIO(wire.getvalue()), io.BytesIO()))
            pending = spool.pending()
            assert pending is not None
            self.assertEqual(pending.to_bytes(), envelope.to_bytes())

    def test_reader_role_cannot_use_edge_session_and_request_queue_is_bounded(self) -> None:
        broker = CaptureBroker(
            self.expected,
            token_hash=edge_token_hash("synthetic-token"),
            lookup_terminal=lambda _capture: None,
            max_pending_requests=1,
        )
        with ThreadPoolExecutor(max_workers=1) as pool:
            waiting = pool.submit(broker.submit, self.request(), timeout=5)
            try:
                with broker._condition:
                    self.assertTrue(broker._condition.wait_for(lambda: bool(broker._queue), 1))
                with self.assertRaisesRegex(CaptureProtocolError, "queue_full"):
                    broker.submit(
                        replace(self.request(), request_id="second-request"), timeout=0.001
                    )
                wire = io.BytesIO()
                FramedStream(io.BytesIO(), wire).send(
                    {"schema": CAPTURE_VERSION, "kind": "hello", "role": "reader"}
                )
                with self.assertRaisesRegex(CaptureProtocolError, "capability_rejected"):
                    broker.serve_connection(FramedStream(io.BytesIO(wire.getvalue()), io.BytesIO()))
            finally:
                broker.close()
            with self.assertRaises(RelayDisconnected):
                waiting.result(timeout=1)

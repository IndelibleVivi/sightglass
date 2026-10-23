"""Generated socket/process crash, cancellation and ownership regressions."""

from __future__ import annotations

import io
import os
import shlex
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from sightglass.contracts.capture import (
    CAPTURE_VERSION,
    CaptureCeiling,
    CaptureExpectation,
    CaptureProtocolError,
)
from sightglass.runtime.edge import EdgeSpool
from sightglass.runtime.edge_relay import (
    CaptureBroker,
    CaptureBrokerServer,
    EdgeSession,
    FramedStream,
    RelayDisconnected,
    SSHRelayConnector,
    edge_token_hash,
)
from sightglass.source.capture.executor import CaptureExecutor
from tests.unit import test_capture_boundary as fixtures


class CaptureReliabilityTests(unittest.TestCase):
    root: Path
    executor: CaptureExecutor
    ceiling: CaptureCeiling
    expected: CaptureExpectation

    setUp = fixtures.CaptureBoundaryTests.setUp
    request = fixtures.CaptureBoundaryTests.request
    capture = fixtures.CaptureBoundaryTests.capture

    def server(self, path):
        server = CaptureBrokerServer(CaptureBroker(
            self.expected, token_hash=edge_token_hash("synthetic-token"),
            lookup_terminal=lambda _: None), path)
        self.addCleanup(server.close)
        return server

    def test_sigkill_stale_socket_restarts_but_active_owner_is_preserved(self):
        path = self.root / "broker.sock"
        code = (
            "import os,signal,socket,sys;os.umask(0o077);"
            "s=socket.socket(socket.AF_UNIX);s.bind(sys.argv[1]);"
            "os.chmod(sys.argv[1],0o600);s.listen(1);"
            "print('bound',flush=True);signal.pause()"
        )
        child = subprocess.Popen([sys.executable, "-u", "-c", code, str(path)],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            assert child.stdout is not None
            self.assertEqual(child.stdout.readline(), b"bound\n")
            child.kill()
            self.assertEqual(child.wait(3), -signal.SIGKILL)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(3)
            assert child.stdout is not None and child.stderr is not None
            child.stdout.close()
            child.stderr.close()
        self.assertTrue(path.is_socket())
        first = self.server(path)
        first.start()
        identity = path.stat().st_ino
        second = self.server(path)
        with self.assertRaisesRegex(CaptureProtocolError, "already_exists"):
            second.start()
        second.close()
        self.assertTrue(first.running)
        self.assertEqual(path.stat().st_ino, identity)
        first.close()
        self.assertFalse(path.exists())

    def test_unsafe_nodes_and_probe_replacement_are_never_removed(self):
        path = self.root / "broker.sock"
        path.write_text("synthetic protected node")
        with self.assertRaisesRegex(CaptureProtocolError, "path_unsafe"):
            self.server(path).start()
        self.assertEqual(path.read_text(), "synthetic protected node")
        path.unlink()
        target = self.root / "target"
        target.write_text("synthetic target")
        path.symlink_to(target)
        with self.assertRaisesRegex(CaptureProtocolError, "path_unsafe"):
            self.server(path).start()
        self.assertTrue(path.is_symlink())
        path.unlink()
        old, replacement = socket.socket(socket.AF_UNIX), socket.socket(socket.AF_UNIX)
        self.addCleanup(old.close)
        self.addCleanup(replacement.close)
        old.bind(str(path))
        path.chmod(0o600)
        server = self.server(path)

        def replace_during_probe():
            path.unlink()
            replacement.bind(str(path))
            path.chmod(0o600)
            raise ConnectionRefusedError

        with patch.object(server, "_probe_socket", side_effect=replace_during_probe):
            with self.assertRaisesRegex(CaptureProtocolError, "changed_during_probe"):
                server.start()
        identity = path.stat().st_ino
        server.close()
        self.assertEqual(path.stat().st_ino, identity)

    def test_failed_bind_and_close_preserve_another_socket(self):
        path = self.root / "broker.sock"
        active = socket.socket(socket.AF_UNIX)
        self.addCleanup(active.close)
        active.bind(str(path))
        path.chmod(0o600)
        active.listen(1)
        identity = path.stat().st_ino
        server = self.server(path)
        # Simulate another listener winning between preflight and bind.
        with patch.object(server, "_prepare_socket_path"):
            with self.assertRaises(OSError):
                server.start()
        server.close()
        self.assertEqual(path.stat().st_ino, identity)

    def test_broker_close_joins_partial_handshake_connections(self):
        server = self.server(self.root / "broker.sock")
        server.start()
        connection = socket.socket(socket.AF_UNIX)
        self.addCleanup(connection.close)
        connection.connect(str(server.socket_path))
        connection.sendall(b"\x00\x00")
        deadline = time.monotonic() + 1
        while not server._connection_threads and time.monotonic() < deadline:
            time.sleep(.005)
        self.assertTrue(server._connection_threads)
        threads = tuple(server._connection_threads)
        server.close()
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertFalse(server.socket_path.exists())

    def test_edge_stop_interrupts_handshake_work_and_pending_ack_reads(self):
        for phase in ("handshake", "work", "ack"):
            with self.subTest(phase=phase):
                left, right = socket.socketpair()
                right.settimeout(2)
                stop = threading.Event()
                errors = []
                with (
                    left, right,
                    left.makefile("rb") as reader,
                    left.makefile("wb") as writer,
                    right.makefile("rb") as control_reader,
                    right.makefile("wb") as control_writer,
                    EdgeSpool.initialize(
                        self.root / phase, source_instance_id=self.executor.source_instance_id,
                        account_id=self.ceiling.account_id, origin_epoch=self.executor.origin_epoch,
                        stream_epoch="fixture-stream") as spool,
                ):
                    envelope = self.capture(self.request()) if phase == "ack" else None
                    if envelope is not None:
                        spool.store(envelope)

                    def run():
                        try:
                            EdgeSession(self.executor, spool, token="synthetic-token").run(
                                FramedStream(reader, writer), stop=stop)
                        except BaseException as error:
                            errors.append(error)

                    thread = threading.Thread(target=run, daemon=True)
                    thread.start()
                    control = FramedStream(control_reader, control_writer)
                    hello = control.receive()
                    self.assertIsInstance(hello, dict)
                    if phase != "handshake":
                        control.send({"schema": CAPTURE_VERSION, "kind": "ready",
                                      "core_generation": ""})
                        control.receive()  # poll or the exact pending capture
                    started = time.monotonic()
                    stop.set()
                    thread.join(1)
                    self.assertFalse(thread.is_alive())
                    self.assertLess(time.monotonic() - started, .5)
                    self.assertEqual(len(errors), 1)
                    self.assertIsInstance(errors[0], RelayDisconnected)
                    if envelope is not None:
                        pending = spool.pending()
                        assert pending is not None
                        self.assertEqual(pending.to_bytes(), envelope.to_bytes())
                        self.assertEqual(spool.next_sequence, 2)
                    else:
                        self.assertIsNone(spool.pending())

    def test_partial_frames_observe_stop_and_absolute_deadline(self):
        for cancelled in (False, True):
            with self.subTest(cancelled=cancelled):
                left, right = socket.socketpair()
                stop = threading.Event()
                sender_stop = threading.Event()
                with left, right, left.makefile("rb") as reader, left.makefile("wb") as writer:
                    right.sendall(struct.pack("!I", 4096) + b"J{")

                    def trickle():
                        while not sender_stop.wait(.01):
                            right.sendall(b" ")

                    sender = threading.Thread(target=trickle, daemon=True)
                    sender.start()
                    timer = threading.Timer(.08, stop.set) if cancelled else None
                    if timer:
                        timer.start()
                    started = time.monotonic()
                    try:
                        with self.assertRaisesRegex(RelayDisconnected,
                                                    "cancelled" if cancelled else "timeout"):
                            FramedStream(reader, writer, stop=stop).receive(timeout=.15)
                        self.assertLess(time.monotonic() - started, .5)
                    finally:
                        sender_stop.set()
                        sender.join(1)
                        if timer:
                            timer.cancel()
                            timer.join(1)

    def test_backpressured_pipe_and_socket_send_observe_stop(self):
        for transport, cancel_channel in (
            ("pipe", False), ("pipe", True), ("socket", False), ("socket", True)
        ):
            with self.subTest(transport=transport, cancel_channel=cancel_channel):
                if transport == "pipe":
                    read_fd, write_fd = os.pipe()
                    reader = os.fdopen(read_fd, "rb")
                    writer = os.fdopen(write_fd, "wb")

                    def close_transport():
                        pass
                else:
                    left, right = socket.socketpair()
                    left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
                    reader = right.makefile("rb")
                    writer = left.makefile("wb")

                    def close_transport():
                        left.close()
                        right.close()

                stop = threading.Event()
                channel = FramedStream(reader, writer, stop=stop)
                errors = []

                def send():
                    try:
                        channel.send({"synthetic": "x" * 1_000_000})
                    except BaseException as error:
                        errors.append(error)

                thread = threading.Thread(target=send, daemon=True)
                try:
                    thread.start()
                    time.sleep(.05)
                    self.assertTrue(thread.is_alive(), "transport did not reach backpressure")
                    if cancel_channel:
                        channel.cancel()
                    else:
                        stop.set()
                    thread.join(.5)
                    self.assertFalse(thread.is_alive())
                    self.assertEqual(len(errors), 1)
                    self.assertRegex(str(errors[0]), "write_cancelled")
                finally:
                    stop.set()
                    thread.join(1)
                    reader.close()
                    writer.close()
                    close_transport()

    def test_send_absolute_deadline_survives_continuous_partial_progress(self):
        read_fd, write_fd = os.pipe()
        sender_stop = threading.Event()
        with os.fdopen(read_fd, "rb", buffering=0) as reader, os.fdopen(write_fd, "wb") as writer:
            channel = FramedStream(reader, writer)

            def drain():
                while not sender_stop.wait(.01):
                    os.read(read_fd, 4096)

            drainer = threading.Thread(target=drain, daemon=True)
            drainer.start()
            started = time.monotonic()
            try:
                with self.assertRaisesRegex(RelayDisconnected, "write_timeout"):
                    channel.send({"synthetic": "x" * 1_000_000}, timeout=.15)
                self.assertLess(time.monotonic() - started, .5)
            finally:
                sender_stop.set()
                drainer.join(1)
            self.assertFalse(drainer.is_alive())

    def test_send_lock_wait_and_synthetic_flush_observe_cancellation(self):
        channel = FramedStream(io.BytesIO(), io.BytesIO())
        channel._write_lock.acquire()
        try:
            started = time.monotonic()
            with self.assertRaisesRegex(RelayDisconnected, "write_timeout"):
                channel.send({"synthetic": True}, timeout=.05)
            self.assertLess(time.monotonic() - started, .3)
            timer = threading.Timer(.05, channel.cancel)
            timer.start()
            try:
                with self.assertRaisesRegex(RelayDisconnected, "write_cancelled"):
                    channel.send({"synthetic": True})
            finally:
                timer.cancel()
                timer.join(1)
        finally:
            channel._write_lock.release()
        stop = threading.Event()

        class StopOnFlush(io.BytesIO):
            def flush(self):
                stop.set()

        with self.assertRaisesRegex(RelayDisconnected, "write_cancelled"):
            FramedStream(io.BytesIO(), StopOnFlush(), stop=stop).send({"synthetic": True})

    def test_channel_cancel_interrupts_partial_nonblocking_read(self):
        left, right = socket.socketpair()
        with left, right, left.makefile("rb") as reader, left.makefile("wb") as writer:
            os.set_blocking(left.fileno(), False)
            right.sendall(b"\x00\x00")
            channel = FramedStream(reader, writer)
            timer = threading.Timer(.05, channel.cancel)
            timer.start()
            try:
                with self.assertRaisesRegex(RelayDisconnected, "read_cancelled"):
                    channel.receive()
            finally:
                timer.cancel()
                timer.join(1)

    def test_broker_close_joins_received_unfinalized_capture_without_ack(self):
        server = self.server(self.root / "unfinalized.sock")
        server.start()
        left = socket.socket(socket.AF_UNIX)
        left.connect(str(server.socket_path))
        errors = []
        tickets = []
        request = self.request()
        envelope = self.capture(request)
        entered_finalization = threading.Event()
        original = server.broker._wait_for_finalization

        def finalization(ticket):
            entered_finalization.set()
            return original(ticket)

        def submit():
            try:
                tickets.append(server.broker.submit(request, timeout=2))
            except BaseException as error:
                errors.append(error)

        with (
            left, left.makefile("rb") as reader, left.makefile("wb") as writer,
            patch.object(server.broker, "_wait_for_finalization", side_effect=finalization),
            EdgeSpool.initialize(
                self.root / "unfinalized-spool",
                source_instance_id=self.executor.source_instance_id,
                account_id=self.ceiling.account_id, origin_epoch=self.executor.origin_epoch,
                stream_epoch="fixture-stream",
            ) as spool,
        ):
            spool.store(envelope)
            channel = FramedStream(reader, writer)
            channel.send({
                "schema": CAPTURE_VERSION, "kind": "hello", "role": "edge",
                "token": "synthetic-token", "source_instance_id": self.expected.source_instance_id,
                "account_id": self.expected.account_id, "origin_epoch": self.expected.origin_epoch,
                "stream_epoch": "fixture-stream", "next_sequence": 2, "core_generation": "",
            })
            ready = channel.receive()
            assert isinstance(ready, dict)
            self.assertEqual(ready["kind"], "ready")
            waiter = threading.Thread(target=submit, daemon=True)
            waiter.start()
            channel.send({"schema": CAPTURE_VERSION, "kind": "poll"})
            work = channel.receive()
            assert isinstance(work, dict)
            self.assertEqual(work["kind"], "work")
            channel.send(envelope)
            self.assertTrue(entered_finalization.wait(1))
            waiter.join(1)
            self.assertFalse(waiter.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(len(tickets), 1)
            started = time.monotonic()
            server.close()
            self.assertLess(time.monotonic() - started, 1)
            self.assertIsNone(tickets[0].durable_ack)
            pending = spool.pending()
            assert pending is not None
            self.assertEqual(pending.to_bytes(), envelope.to_bytes())
            self.assertEqual(spool.next_sequence, 2)
            self.assertIs(server.broker._by_request[request.request_id].ticket, tickets[0])

    def test_ssh_connector_reaps_descendant_after_leader_exit(self):
        child_code = (
            "import os,signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);"
            "print(os.getpid(),flush=True);time.sleep(60)"
        )
        parent_code = (
            "import subprocess,sys;"
            f"p=subprocess.Popen([sys.executable,'-u','-c',{child_code!r}],"
            "stdin=subprocess.DEVNULL,stdout=subprocess.PIPE);"
            "print(p.stdout.readline().decode().strip(),flush=True)"
        )
        shim = self.root / "synthetic-ssh"
        shim.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable) + " -u -c "
                        + shlex.quote(parent_code) + "\n")
        shim.chmod(0o700)
        connector = SSHRelayConnector("synthetic-host", self.root / "synthetic-identity",
                                     ssh_binary=str(shim))
        with connector.connect() as channel:
            child_pid = int(channel.reader.readline())
            time.sleep(.05)  # Let the deliberately short-lived leader exit.
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            state = subprocess.run(["ps", "-p", str(child_pid), "-o", "stat="],
                                   capture_output=True, text=True, check=False).stdout.strip()
            if not state or state.startswith("Z"):
                break
            time.sleep(.02)
        else:
            os.kill(child_pid, signal.SIGKILL)
            self.fail("SSH descendant survived connector cleanup")

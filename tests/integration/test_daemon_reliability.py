"""Generated real-IPC lifecycle, admission-reserve and startup cleanup regressions."""

from __future__ import annotations

import socket
import struct
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock, patch

from sightglass.operations import check_operation_budget
from sightglass.runtime.config import ConfigStore, SightglassConfig
from sightglass.runtime.daemon import SightglassDaemon
from sightglass.runtime.daemon import main as daemon_main
from sightglass.runtime.ipc import IPCClient, IPCUnavailableError, receive_frame
from sightglass.runtime.process_lock import acquire_runtime_lock, release_runtime_lock
from sightglass.runtime.secrets import (
    OPERATOR_SECRET_ACCOUNT,
    READER_SECRET_ACCOUNT,
    MemorySecretStore,
    token_hash,
)
from sightglass.runtime.service import build_daemon_tools
from sightglass.source.synthetic import create_synthetic_source


class DaemonReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        source = create_synthetic_source(self.root / "source")
        self.config = replace(
            SightglassConfig.create(self.root / "state", source),
            reader_token_hash=token_hash("synthetic-reader"),
            operator_token_hash=token_hash("synthetic-operator"),
        )
        self.store = ConfigStore(self.config.data_dir / "config.json")
        self.store.save(self.config)
        self.secrets = MemorySecretStore(
            {
                READER_SECRET_ACCOUNT: "synthetic-reader",
                OPERATOR_SECRET_ACCOUNT: "synthetic-operator",
            }
        )
        self.daemon = SightglassDaemon(config_store=self.store, secret_store=self.secrets)
        self.thread = None
        self.failures = []
        self.addCleanup(self.stop)

    def client(self, role="reader", timeout=4):
        return IPCClient(
            config_store=self.store, secret_store=self.secrets, role=role, timeout=timeout
        )

    def start(self):
        def serve():
            try:
                self.daemon.serve_forever(install_signal_handlers=False)
            except BaseException as error:
                self.failures.append(error)

        self.thread = threading.Thread(target=serve, daemon=True)
        self.thread.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                if self.client().call("daemon.status")["ready"]:
                    return
            except IPCUnavailableError:
                pass
            time.sleep(0.01)
        self.fail(f"synthetic daemon startup failed: {self.failures}")

    def stop(self):
        self.daemon.shutdown()
        if self.thread is not None:
            self.thread.join(5)
            self.assertFalse(self.thread.is_alive())
        self.assertEqual(self.failures, [])

    def reload(self, config):
        with self.daemon._state_gate.write():
            self.daemon._reload(config)

    def threads(self):
        return [
            getattr(worker, "_thread", None)
            for worker in (
                self.daemon.source_worker,
                self.daemon.search_preparation,
                self.daemon.resource_worker,
                self.daemon.derived_worker,
                self.daemon.semantic_worker,
                self.daemon.voice_worker,
            )
        ]

    def test_partial_stop_failure_compensates_before_replacing_owner(self):
        self.start()
        self.client().call(
            "tools.call",
            {"name": "wechat_find_conversations", "arguments": {"query": "Synthetic Group"}},
        )
        original = self.daemon.semantic_worker.stop
        old_threads = self.threads()
        calls = 0

        def partial_stop():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("synthetic partial stop")
            return original()

        install = self.daemon._install_runtime

        def checked_install(*args):
            self.assertTrue(all(thread is None or not thread.is_alive() for thread in old_threads))
            return install(*args)

        with (
            patch.object(self.daemon.semantic_worker, "stop", side_effect=partial_stop),
            patch.object(self.daemon, "_install_runtime", side_effect=checked_install),
        ):
            with self.assertRaisesRegex(RuntimeError, "quiesced"):
                self.reload(replace(self.config, paused=True))
        self.assertEqual(calls, 2)
        status = self.client().call("daemon.status")
        self.assertTrue(status["ready"])
        self.assertEqual(status["lifecycle_state"], "ready")
        self.assertFalse(self.store.load().paused)
        self.assertTrue(self.client().call("tools.call", {"name": "wechat_status"})["ready"])

    def test_unquiesced_stop_does_not_start_replacement_and_can_recover(self):
        self.start()
        previous = self.daemon.tools
        with (
            patch.object(self.daemon.semantic_worker, "stop", return_value=False),
            patch.object(self.daemon, "_install_runtime") as install,
        ):
            with self.assertRaises(RuntimeError):
                self.reload(replace(self.config, paused=True))
            install.assert_not_called()
        self.assertIs(self.daemon.tools, previous)
        self.assertFalse(self.client().call("daemon.status")["ready"])
        self.assertEqual(self.daemon._lifecycle_state, "degraded")
        result = self.client().call(
            "tools.call", {"name": "wechat_find_conversations", "arguments": {"query": "Synthetic"}}
        )
        self.assertEqual(result["code"], "SERVICE_UNAVAILABLE")
        self.reload(self.config)
        self.assertTrue(self.client().call("daemon.status")["ready"])

    def test_restart_and_compensation_start_failure_stays_degraded(self):
        self.start()
        original = self.daemon._start_runtime_workers
        started = []

        def failed_start():
            # Both attempts really start workers, then fail. A later attempt must
            # find every preceding generation quiescent, rather than overlap it.
            self.assertTrue(all(not thread.is_alive() for thread in started))
            original()
            started.extend(thread for thread in self.threads() if thread is not None)
            raise RuntimeError("synthetic restart failure")

        with patch.object(self.daemon, "_start_runtime_workers", side_effect=failed_start):
            with self.assertRaisesRegex(RuntimeError, "restart failure"):
                self.reload(replace(self.config, paused=True))
        status = self.client().call("daemon.status")
        self.assertFalse(status["ready"])
        self.assertEqual(status["lifecycle_state"], "degraded")
        self.assertFalse(self.client().call("tools.call", {"name": "wechat_status"})["ready"])
        self.reload(self.config)
        self.assertTrue(self.client().call("daemon.status")["ready"])

    def test_status_remains_available_during_reload_and_paused_ready_is_distinct(self):
        self.start()
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        original = self.daemon.semantic_worker.stop

        def stopping():
            entered.set()
            if not release.wait(3):
                raise RuntimeError("synthetic stop not released")
            return original()

        with (
            patch.object(self.daemon.semantic_worker, "stop", side_effect=stopping),
            ThreadPoolExecutor(max_workers=1) as pool,
        ):
            pending = pool.submit(self.reload, replace(self.config, paused=True))
            self.assertTrue(entered.wait(2))
            before = time.monotonic()
            status = self.client(timeout=1).call("daemon.status")
            self.assertLess(time.monotonic() - before, 1)
            self.assertEqual(status["lifecycle_state"], "reloading")
            self.assertFalse(status["ready"])
            release.set()
            pending.result(5)
        status = self.client().call("daemon.status")
        self.assertTrue(status["ready"])
        self.assertTrue(status["paused"])
        self.assertFalse(self.client().call("tools.call", {"name": "wechat_status"})["ready"])

    def test_sixteen_real_slow_readers_and_followers_leave_status_and_control_capacity(self):
        self.start()
        group = self.client().call(
            "tools.call",
            {"name": "wechat_find_conversations", "arguments": {"query": "Synthetic Group"}},
        )["candidates"][0]["conversation_id"]
        release, entered = threading.Event(), threading.Event()
        barrier = threading.Barrier(17)
        original = self.daemon.tools.wechat_read_messages

        def slow(**arguments):
            entered.set()
            while not release.wait(0.02):
                check_operation_budget()
            return original(**arguments)

        def read():
            barrier.wait(3)
            return self.client().call(
                "tools.call",
                {
                    "name": "wechat_read_messages",
                    "arguments": {
                        "mode": "recent",
                        "conversation_id": group,
                        "limit": 1,
                        "refresh": True,
                    },
                },
            )

        with (
            patch.object(self.daemon.tools, "wechat_read_messages", side_effect=slow) as execute,
            ThreadPoolExecutor(max_workers=16) as pool,
        ):
            futures = [pool.submit(read) for _ in range(16)]
            barrier.wait(3)
            try:
                self.assertTrue(entered.wait(2))
                deadline = time.monotonic() + 2
                while self.daemon._joined_tool_calls < 11 and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertEqual(self.daemon._joined_tool_calls, 11)
                self.assertEqual(execute.call_count, 1)
                before = time.monotonic()
                self.assertTrue(self.client(timeout=1).call("daemon.status")["ready"])
                self.client("operator", timeout=1).call("operator.backfill.status")
                stopped = self.client("operator", timeout=1).call("daemon.shutdown")
                self.assertTrue(stopped["stopping"])
                self.assertLess(time.monotonic() - before, 1)
            finally:
                release.set()
            results = [future.result(4) for future in futures]
            busy = [result for result in results if result.get("code") == "SERVICE_BUSY"]
            self.assertEqual(len(busy), 4)
            self.assertTrue(
                all(result["details"]["reason"] == "ipc_reader_capacity" for result in busy)
            )

    def test_unauthenticated_partial_frames_cannot_pin_all_connections(self):
        self.start()
        sockets = []
        with patch("sightglass.runtime.daemon.IPC_FRAME_TIMEOUT_SECONDS", 0.2):
            try:
                for _ in range(16):
                    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    connection.settimeout(2)
                    connection.connect(str(self.config.socket_path))
                    connection.sendall(struct.pack("!I", 1024) + b"{")
                    sockets.append(connection)
                before = time.monotonic()
                self.assertTrue(self.client(timeout=1).call("daemon.status")["ready"])
                self.assertLess(time.monotonic() - before, 1)
            finally:
                for connection in sockets:
                    connection.close()

    def test_waiting_reader_rechecks_lifecycle_after_the_state_gate(self):
        self.start()
        entered = threading.Event()
        original = self.daemon._state_gate.read

        @contextmanager
        def gated():
            entered.set()
            with original():
                yield

        with (
            patch.object(self.daemon._state_gate, "read", side_effect=gated),
            ThreadPoolExecutor(max_workers=1) as pool,
        ):
            with self.daemon._state_gate.write():
                future = pool.submit(
                    self.client().call,
                    "tools.call",
                    {"name": "wechat_find_conversations", "arguments": {"query": "Synthetic"}},
                )
                self.assertTrue(entered.wait(1))
                self.daemon._lifecycle_state = "degraded"
            result = future.result(2)
        self.assertEqual(result["code"], "SERVICE_UNAVAILABLE")

    def test_frame_deadline_is_absolute_despite_continuous_partial_input(self):
        left, right = socket.socketpair()
        stop = threading.Event()

        def trickle():
            right.sendall(struct.pack("!I", 1024))
            while not stop.wait(0.02):
                right.sendall(b" ")

        sender = threading.Thread(target=trickle, daemon=True)
        sender.start()
        started = time.monotonic()
        try:
            with self.assertRaises(TimeoutError):
                receive_frame(left, deadline=started + 0.15)
            self.assertLess(time.monotonic() - started, 0.5)
        finally:
            stop.set()
            sender.join(1)
            left.close()
            right.close()

    def test_constructor_assembly_failure_closes_prepared_tools(self):
        prepared = []

        def build(*args, **kwargs):
            tools = build_daemon_tools(*args, **kwargs)
            tools.close = Mock(wraps=tools.close)
            prepared.append(tools)
            return tools

        with (
            patch("sightglass.runtime.daemon.build_daemon_tools", side_effect=build),
            patch.object(
                SightglassDaemon,
                "_configure_search_preparation",
                side_effect=RuntimeError("synthetic assembly failure"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "assembly failure"):
                SightglassDaemon(config_store=self.store, secret_store=self.secrets)
        self.assertEqual(len(prepared), 1)
        prepared[0].close.assert_called_once()

    def test_start_failure_closes_owned_socket_lock_and_started_workers(self):
        for boundary in ("capture", "later_worker"):
            with self.subTest(boundary=boundary):
                daemon = SightglassDaemon(config_store=self.store, secret_store=self.secrets)
                broker = SimpleNamespace(start=lambda: None, close=lambda: None)
                daemon.capture_core = cast(Any, broker)
                target = broker if boundary == "capture" else daemon.semantic_worker
                with (
                    patch.object(target, "start", side_effect=RuntimeError("synthetic startup")),
                    patch.object(broker, "close") as close,
                    patch.object(daemon.tools, "close", wraps=daemon.tools.close) as tools_close,
                ):
                    with self.assertRaisesRegex(RuntimeError, "synthetic startup"):
                        daemon.serve_forever(install_signal_handlers=False)
                    close.assert_called_once()
                    tools_close.assert_called_once()
                self.assertFalse(self.config.socket_path.exists())
                self.assertIsNone(daemon._lock_handle)
                self.assertEqual(daemon._lifecycle_state, "stopped")
                self.assertTrue(
                    all(
                        thread is None or not thread.is_alive()
                        for thread in (
                            getattr(worker, "_thread", None)
                            for worker in (
                                daemon.source_worker,
                                daemon.resource_worker,
                                daemon.derived_worker,
                            )
                        )
                    )
                )
                handle = acquire_runtime_lock(daemon.lock_path)
                release_runtime_lock(handle)

    def test_main_keeps_lock_when_failed_start_cannot_quiesce(self):
        retained = []

        def build(**kwargs):
            daemon = SightglassDaemon(**kwargs)
            original_stop = daemon.semantic_worker.stop
            daemon.semantic_worker.stop = Mock(return_value=False)
            retained.append((daemon, original_stop))
            return daemon

        try:
            with (
                patch("sightglass.runtime.daemon.SightglassDaemon", side_effect=build),
                patch("sightglass.runtime.daemon.default_secret_store", return_value=self.secrets),
                patch.object(
                    SightglassDaemon,
                    "_start_runtime_workers",
                    side_effect=RuntimeError("synthetic startup failure"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "startup failure"):
                    daemon_main(["--config", str(self.store.path)])
            daemon, _ = retained[0]
            self.assertFalse(daemon._lock_handle.closed)
            self.assertEqual(daemon._lifecycle_state, "degraded")
            with self.assertRaises(RuntimeError):
                acquire_runtime_lock(daemon.lock_path)
        finally:
            for daemon, original_stop in retained:
                daemon.semantic_worker.stop = original_stop
                daemon.shutdown()
        handle = acquire_runtime_lock(self.daemon.lock_path)
        release_runtime_lock(handle)

    def test_failed_second_owner_does_not_unlink_active_socket(self):
        self.start()
        second = SightglassDaemon(config_store=self.store, secret_store=self.secrets)
        with self.assertRaises(RuntimeError):
            second.serve_forever(install_signal_handlers=False)
        self.assertTrue(self.config.socket_path.exists())
        self.assertTrue(self.client().call("daemon.status")["ready"])

"""Real stdio/IPC cancellation, shared caller leases and bounded cleanup."""
from __future__ import annotations

import os
import socket
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import anyio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import CancelledNotification, CancelledNotificationParams, ClientNotification

from sightglass.contracts.errors import SightglassError
from sightglass.operations import (
    check_operation_budget,
    operation_budget,
    operation_budget_snapshot,
)
from sightglass.runtime.config import ConfigStore
from sightglass.runtime.daemon import SightglassDaemon
from sightglass.runtime.ipc import IPCClient, IPCTimeoutError
from tests.integration import test_daemon_reliability as reliability


class CallerCancellationTests(unittest.TestCase):
    daemon: SightglassDaemon
    store: ConfigStore
    root: Path

    setUp = reliability.DaemonReliabilityTests.setUp
    start = reliability.DaemonReliabilityTests.start
    stop = reliability.DaemonReliabilityTests.stop
    def client(self, role="reader", timeout: float = 4) -> IPCClient:
        return IPCClient(config_store=self.store, secret_store=self.daemon.secret_store,
                         role=role, timeout=timeout)

    def prepare(self):
        self.start()
        group = self.client().call('tools.call', {
            'name': 'wechat_find_conversations', 'arguments': {'query': 'Synthetic Group'}
        })['candidates'][0]['conversation_id']
        return {'name': 'wechat_read_messages', 'arguments': {
            'mode': 'recent', 'conversation_id': group, 'limit': 1, 'refresh': True,
            'response_profile': 'diagnostic'}}

    def settled(self):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            with self.daemon._connection_threads_lock:
                connections = len(self.daemon._connection_threads)
            if not self.daemon._operations and not connections:
                break
            time.sleep(.005)
        self.assertFalse(self.daemon._operations)
        self.assertEqual(connections, 0)
        self.assertEqual(self.daemon.active_bridge_calls, 0)
        self.assertEqual(self.daemon.source_worker._foreground_count, 0)

    def test_actual_ipc_timeout_cancels_cooperative_work(self):
        params = self.prepare()
        entered, stopped = threading.Event(), threading.Event()

        def slow(**kwargs):
            entered.set()
            try:
                while True:
                    check_operation_budget()
                    time.sleep(.005)
            finally:
                stopped.set()

        with patch.object(self.daemon.tools, 'wechat_read_messages', side_effect=slow):
            with self.assertRaises(IPCTimeoutError):
                self.client(timeout=.1).call('tools.call', params)
            self.assertTrue(entered.is_set())
            self.assertTrue(stopped.wait(1))
            self.settled()

    def test_owner_or_follower_departure_preserves_remaining_caller(self):
        params = self.prepare()
        original = self.daemon.tools.wechat_read_messages
        for departing in ('owner', 'follower'):
            with self.subTest(departing=departing):
                entered, release = threading.Event(), threading.Event()
                cancel_owner, cancel_follower = threading.Event(), threading.Event()
                invocations = []

                def slow(**kwargs):
                    invocations.append(1)
                    entered.set()
                    while not release.wait(.005):
                        check_operation_budget()
                    return original(**kwargs)

                def call(cancelled):
                    with operation_budget(3, cancelled=cancelled):
                        return self.client().call('tools.call', params)

                before = self.daemon._joined_tool_calls
                with patch.object(self.daemon.tools, 'wechat_read_messages', side_effect=slow):
                    with ThreadPoolExecutor(2) as pool:
                        owner = pool.submit(call, cancel_owner)
                        self.assertTrue(entered.wait(1))
                        follower = pool.submit(call, cancel_follower)
                        deadline = time.monotonic() + 1
                        while (self.daemon._joined_tool_calls == before
                               and time.monotonic() < deadline):
                            time.sleep(.005)
                        self.assertEqual(self.daemon._joined_tool_calls, before + 1)
                        (cancel_owner if departing == 'owner' else cancel_follower).set()
                        departed, live = ((owner, follower) if departing == 'owner'
                                          else (follower, owner))
                        with self.assertRaises(SightglassError):
                            departed.result(1)
                        # Give the daemon disconnect watcher a chance to reject an
                        # incorrectly owner-bound shared cancellation.
                        time.sleep(.08)
                        self.assertFalse(live.done())
                        release.set()
                        self.assertEqual(live.result(2)['schema'], 'sightglass.message-batch.v1')
                        self.assertEqual(len(invocations), 1)
                self.settled()

    def test_last_joined_caller_departure_cancels_shared_work(self):
        params = self.prepare()
        entered, stopped = threading.Event(), threading.Event()
        owner_cancel, follower_cancel = threading.Event(), threading.Event()

        def slow(**kwargs):
            entered.set()
            try:
                while True:
                    check_operation_budget()
                    time.sleep(.005)
            finally:
                stopped.set()

        def call(cancelled):
            with operation_budget(3, cancelled=cancelled):
                return self.client().call("tools.call", params)

        with patch.object(self.daemon.tools, "wechat_read_messages", side_effect=slow):
            before = self.daemon._joined_tool_calls
            with ThreadPoolExecutor(2) as pool:
                owner = pool.submit(call, owner_cancel)
                self.assertTrue(entered.wait(1))
                follower = pool.submit(call, follower_cancel)
                deadline = time.monotonic() + 1
                while (self.daemon._joined_tool_calls == before
                       and time.monotonic() < deadline):
                    time.sleep(.005)
                self.assertEqual(self.daemon._joined_tool_calls, before + 1)
                owner_cancel.set()
                with self.assertRaises(SightglassError):
                    owner.result(1)
                self.assertFalse(stopped.wait(.08))
                follower_cancel.set()
                with self.assertRaises(SightglassError):
                    follower.result(1)
                self.assertTrue(stopped.wait(1))
                self.settled()

    def test_cancelled_owner_keeps_singleflight_slot_until_quiescent(self):
        params = self.prepare()
        original = self.daemon.tools.wechat_read_messages
        entered, release = threading.Event(), threading.Event()
        cancelled = threading.Event()
        invocations = []

        def uncooperative(**kwargs):
            invocations.append(1)
            entered.set()
            release.wait(3)
            return original(**kwargs)

        def call():
            with operation_budget(3, cancelled=cancelled):
                return self.client().call("tools.call", params)

        with patch.object(self.daemon.tools, "wechat_read_messages", side_effect=uncooperative):
            with ThreadPoolExecutor(1) as pool:
                owner = pool.submit(call)
                try:
                    self.assertTrue(entered.wait(1))
                    with self.daemon._operations_lock:
                        old = next(iter(self.daemon._operations.values()))
                    cancelled.set()
                    with self.assertRaises(SightglassError):
                        owner.result(1)
                    deadline = time.monotonic() + 1
                    while not old.cancellation.is_set() and time.monotonic() < deadline:
                        time.sleep(.005)
                    self.assertTrue(old.cancellation.is_set())
                    rejected = self.client().call("tools.call", params)
                    self.assertEqual(rejected["code"], "SERVICE_BUSY")
                    self.assertEqual(rejected["details"]["reason"], "singleflight_cleanup")
                    with self.daemon._operations_lock:
                        self.assertEqual(list(self.daemon._operations.values()), [old])
                    self.assertEqual(self.daemon._operation_status()["active_count"], 1)
                    self.assertEqual(self.daemon.active_bridge_calls, 1)
                    self.assertEqual(len(invocations), 1)
                finally:
                    release.set()
                self.settled()
                fresh = self.client().call("tools.call", params)
                self.assertEqual(fresh["schema"], "sightglass.message-batch.v1")
                self.assertEqual(len(invocations), 2)
                self.settled()

    def test_200_last_callers_cancel_without_thread_or_fd_growth(self):
        params = self.prepare()
        # A thread already alive at baseline may finish independently of these
        # cancellations. Reproduce that transition without relying on suite order.
        release_baseline = threading.Event()
        departing = threading.Thread(target=release_baseline.wait, daemon=True)
        self.addCleanup(release_baseline.set)
        departing.start()
        baseline_threads = set(threading.enumerate())
        self.assertIn(departing, baseline_threads)
        release_baseline.set()
        departing.join(1)
        self.assertFalse(departing.is_alive())
        fd_dir = "/dev/fd" if sys.platform == "darwin" else "/proc/self/fd"
        baseline_fds = len(os.listdir(fd_dir))
        entered, entry_changed = threading.Event(), threading.Event()
        caller_deadline: list[float] = []
        invocations = []
        created_sockets: list[socket.socket] = []
        socket_lock = threading.Lock()
        original_socket = socket.socket

        def record_socket(*args, **kwargs):
            connection = original_socket(*args, **kwargs)
            if connection.family == socket.AF_UNIX:
                with socket_lock:
                    created_sockets.append(connection)
            return connection

        def slow(**kwargs):
            invocations.append(1)
            entered.set()
            entry_changed.set()
            while True:
                check_operation_budget()
                time.sleep(.001)

        def call(cancelled):
            with operation_budget(3, cancelled=cancelled):
                budget = operation_budget_snapshot()
                assert budget is not None and budget.deadline is not None
                caller_deadline.append(budget.deadline)
                entry_changed.set()
                return self.client().call("tools.call", params)

        # Wrap actual constructors, including socket.accept's real AF_UNIX socket
        # objects. Retaining the objects distinguishes close from fd-number reuse.
        with patch("sightglass.runtime.ipc.socket.socket", side_effect=record_socket):
            with (patch.object(self.daemon.tools, "wechat_read_messages", side_effect=slow),
                  ThreadPoolExecutor(1) as pool):
                for iteration in range(200):
                    entered.clear()
                    entry_changed.clear()
                    caller_deadline.clear()
                    cancelled = threading.Event()
                    submitted_at = time.monotonic()
                    future = pool.submit(call, cancelled)
                    future.add_done_callback(lambda _: entry_changed.set())
                    # Entry is not an independent latency SLO. Wait for entry or
                    # early completion within this same caller's existing budget.
                    while True:
                        entry_changed.clear()
                        if entered.is_set() or future.done():
                            break
                        deadline = (caller_deadline[0] if caller_deadline
                                    else submitted_at + 3)
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        entry_changed.wait(remaining)
                    if not entered.is_set() or future.done():
                        diagnostics = {
                            "iteration": iteration, "invocations": len(invocations),
                            "future_done": future.done(),
                            "operations": self.daemon._operation_status(include_stack=True),
                            "source_worker": self.daemon.source_worker.status(),
                            "lanes": self.daemon._lane_status(),
                        }
                        worker = self.daemon.source_worker
                        with worker._foreground_lock:
                            diagnostics["source_owner"] = {
                                "background_active": worker._background_active,
                                "background_idle": worker._background_idle.is_set(),
                                "foreground_count": worker._foreground_count,
                                "cancel_pending": worker._foreground_cancel_pending,
                                "cancel_work": worker._cancel_work.is_set(),
                            }
                        with self.daemon._connection_threads_lock:
                            diagnostics["connection_count"] = len(
                                self.daemon._connection_threads
                            )
                        if future.done():
                            try:
                                result = future.result()
                            except Exception as exc:
                                diagnostics["future_outcome"] = type(exc).__name__
                            else:
                                diagnostics["future_outcome"] = {
                                    key: result.get(key) for key in ("schema", "code", "details")
                                }
                        cancelled.set()
                        self.fail(f"entry barrier: {diagnostics}")
                    cancelled.set()
                    with self.assertRaises(SightglassError):
                        future.result(1)
                    self.settled()
            self.assertEqual(len(invocations), 200)
            fresh = self.client().call("tools.call", params)
            self.assertEqual(fresh["schema"], "sightglass.message-batch.v1")
            self.assertEqual(fresh["source_receipt"]["view"], "fresh")
            self.settled()
        # Keep the objects alive so reused OS thread identifiers cannot hide a
        # replacement. Allow completed connection threads to finish their final
        # bookkeeping, then reject any newly created thread that remains alive.
        for thread in set(threading.enumerate()) - baseline_threads:
            thread.join(timeout=1)
        survivors = set(threading.enumerate()) - baseline_threads
        self.assertFalse(survivors, [thread.name for thread in survivors])
        self.assertNotIn(departing, threading.enumerate())
        self.assertEqual(len(created_sockets), 402)  # 201 callers and 201 accepted peers
        self.assertTrue(all(connection.fileno() == -1 for connection in created_sockets))
        # Process-wide fd counts can include a receipt/background SQLite open.
        # Keep a separate bounded convergence check without mistaking that instant
        # for an owned IPC socket leak or allowing indefinite polling to pass.
        cleanup_deadline = time.monotonic() + 2
        current_fds = len(os.listdir(fd_dir))
        while current_fds > baseline_fds and time.monotonic() < cleanup_deadline:
            time.sleep(.005)
            current_fds = len(os.listdir(fd_dir))
        self.assertLessEqual(current_fds, baseline_fds, {
            "receipt_writer": self.daemon.tools.receipt_writer_status(),
            "source_worker": self.daemon.source_worker.status(),
        })

    def test_committed_delivery_survives_unknown_caller_outcome(self):
        params = self.prepare()
        params["arguments"] = {"mode": "updates",
                               "conversation_id": params["arguments"]["conversation_id"],
                               "limit": 1, "response_profile": "diagnostic"}
        original = self.daemon.tools.wechat_read_messages
        committed, release = threading.Event(), threading.Event()
        observed = []

        def after_commit(**kwargs):
            result = original(**kwargs)
            observed.append(result)
            committed.set()
            release.wait(2)
            return result

        with patch.object(self.daemon.tools, "wechat_read_messages", side_effect=after_commit):
            with ThreadPoolExecutor(1) as pool:
                unknown = pool.submit(lambda: self.client(timeout=.3).call("tools.call", params))
                self.assertTrue(committed.wait(1))
                with self.assertRaises(IPCTimeoutError):
                    unknown.result(1)
                release.set()
                self.settled()
        self.assertIsNotNone(observed[0]["page"]["delivery_id"])
        replay = self.client().call("tools.call", params)
        self.assertEqual(replay, observed[0])
        self.assertEqual(self.client().call("tools.call", params), replay)

    def test_real_stdio_status_and_cancellation_leave_no_work(self):
        params = self.prepare()
        entered, stopped = threading.Event(), threading.Event()

        def slow(**kwargs):
            entered.set()
            try:
                while True:
                    check_operation_budget()
                    time.sleep(.005)
            finally:
                stopped.set()

        env = {**os.environ, 'SIGHTGLASS_CONFIG': str(self.store.path),
               'SIGHTGLASS_SYNTHETIC_TEST_SECRETS': '1',
               'SIGHTGLASS_TEST_READER_TOKEN': 'synthetic-reader',
               'SIGHTGLASS_TEST_OPERATOR_TOKEN': 'synthetic-operator'}

        async def probe():
            process = StdioServerParameters(command=sys.executable,
                                           args=['-m', 'sightglass.mcp.server'], env=env)
            with (self.root / 'stdio.stderr').open('w') as log:
                async with stdio_client(process, errlog=log) as streams:
                    async with ClientSession(*streams) as session:
                        await session.initialize()
                        self.assertEqual(len((await session.list_tools()).tools), 13)
                        scope = anyio.CancelScope()
                        request_id = session._request_id

                        async def read():
                            with scope:
                                await session.call_tool(params['name'], params['arguments'])

                        async with anyio.create_task_group() as tasks:
                            tasks.start_soon(read)
                            self.assertTrue(await anyio.to_thread.run_sync(lambda: entered.wait(2)))
                            latencies = []
                            for _ in range(10):
                                started = time.monotonic()
                                with anyio.fail_after(5):
                                    status = await session.call_tool('wechat_status', {})
                                self.assertFalse(status.isError)
                                latencies.append(time.monotonic() - started)
                                # Status must finish while the slow request is still
                                # held, not after its deadline/cancellation releases it.
                                self.assertFalse(stopped.is_set())
                            print(f"stdio status samples={len(latencies)} "
                                  f"p99_seconds={max(latencies):.6f}")
                            await session.send_notification(ClientNotification(
                                CancelledNotification(method="notifications/cancelled",
                                    params=CancelledNotificationParams(requestId=request_id))
                            ))
                            scope.cancel()
                            self.assertTrue(await anyio.to_thread.run_sync(lambda: stopped.wait(1)))
                        self.assertFalse((await session.call_tool('wechat_status', {})).isError)

        with patch.object(self.daemon.tools, 'wechat_read_messages', side_effect=slow):
            anyio.run(probe)
            self.settled()

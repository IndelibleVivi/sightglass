"""Real stdio/IPC cancellation, shared caller leases and bounded cleanup."""
from __future__ import annotations

import os
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
from sightglass.operations import check_operation_budget, operation_budget
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
        baseline_threads = {t.ident for t in threading.enumerate()}
        fd_dir = "/dev/fd" if sys.platform == "darwin" else "/proc/self/fd"
        baseline_fds = len(os.listdir(fd_dir))
        entered = threading.Event()

        def slow(**kwargs):
            entered.set()
            while True:
                check_operation_budget()
                time.sleep(.001)

        def call(cancelled):
            with operation_budget(3, cancelled=cancelled):
                return self.client().call("tools.call", params)

        with patch.object(self.daemon.tools, "wechat_read_messages", side_effect=slow):
            with ThreadPoolExecutor(1) as pool:
                for _ in range(200):
                    entered.clear()
                    cancelled = threading.Event()
                    future = pool.submit(call, cancelled)
                    self.assertTrue(entered.wait(1))
                    cancelled.set()
                    with self.assertRaises(SightglassError):
                        future.result(1)
                    self.settled()
        self.assertEqual({t.ident for t in threading.enumerate()}, baseline_threads)
        self.assertLessEqual(len(os.listdir(fd_dir)), baseline_fds)

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

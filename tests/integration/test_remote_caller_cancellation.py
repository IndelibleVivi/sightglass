"""Generated end-to-end caller cancellation across stdio, IPC and Unix edge relay."""
from __future__ import annotations

import os
import socket
import sys
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import anyio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import CancelledNotification, CancelledNotificationParams, ClientNotification

from sightglass.operations import check_operation_budget
from sightglass.runtime.config import ConfigStore
from sightglass.runtime.daemon import SightglassDaemon
from sightglass.runtime.edge import EdgeSpool
from sightglass.runtime.ipc import IPC_VERSION, send_frame
from sightglass.runtime.secrets import (
    OPERATOR_SECRET_ACCOUNT,
    READER_SECRET_ACCOUNT,
    MemorySecretStore,
    token_hash,
)
from sightglass.source.capture.executor import CaptureExecutor
from sightglass.source.remote import RemoteCaptureSettings
from tests.integration import test_daemon_reliability as daemon_fixture
from tests.unit import test_capture_core as core_fixture


class RemoteCallerCancellationTests(unittest.TestCase):
    group: str
    root: Path
    settings: RemoteCaptureSettings
    executor: CaptureExecutor
    spool: EdgeSpool
    setUp = core_fixture.CoreCaptureTests.setUp
    start_edge = core_fixture.CoreCaptureTests.start_edge
    stop_edge = core_fixture.CoreCaptureTests.stop_edge
    start = daemon_fixture.DaemonReliabilityTests.start
    stop = daemon_fixture.DaemonReliabilityTests.stop
    client = daemon_fixture.DaemonReliabilityTests.client

    def exercise(self, disconnect: str) -> None:
        # Reuse the Core fixture's admitted generated catalog, source and enrolled
        # spool. Replace its standalone Core with the daemon's real remote assembly.
        self.core.close()
        settings_path = self.root / "enrollment" / "remote.json"
        self.settings.write(settings_path)
        self.config = replace(
            self.config, source_root=None, source_settings_path=settings_path,
            reader_token_hash=token_hash("synthetic-reader"),
            operator_token_hash=token_hash("synthetic-operator"),
        )
        self.store = ConfigStore(self.config.data_dir / "config.json")
        self.store.save(self.config)
        self.secrets = MemorySecretStore({READER_SECRET_ACCOUNT: "synthetic-reader",
                                         OPERATOR_SECRET_ACCOUNT: "synthetic-operator"})
        self.daemon = SightglassDaemon(config_store=self.store, secret_store=self.secrets)
        assert self.daemon.capture_core is not None
        self.core = self.daemon.capture_core
        self.thread = None
        self.failures = []
        self.addCleanup(self.stop)
        self.addCleanup(self.stop_edge)
        # Keep unrelated scheduled catalog reads out of the controlled stream.
        # Foreground daemon/Core/broker/executor work is unmodified.
        with patch.object(self.daemon.source_worker, "start"):
            self.start()
        self.start_edge()
        selected, source_stopped = threading.Event(), threading.Event()
        terminal_waiting, allow_terminal, acknowledged = (
            threading.Event(), threading.Event(), threading.Event()
        )
        self.addCleanup(allow_terminal.set)
        fresh_acknowledged = threading.Event()
        executed = []
        acknowledgements = []
        original_execute = self.executor._execute
        original_reject = self.core._reject
        original_ack = self.spool.acknowledge

        def slow(request, snapshot):
            executed.append(request.request_id)
            if len(executed) == 1:
                selected.set()
                try:
                    while True:
                        check_operation_budget()
                        source_stopped.wait(.005)
                finally:
                    source_stopped.set()
            return original_execute(request, snapshot)

        def reject(ticket):
            terminal_waiting.set()
            if not allow_terminal.wait(5):
                raise RuntimeError("synthetic terminal commit barrier expired")
            return original_reject(ticket)

        def acknowledge(ack):
            pending = self.spool.pending()
            self.assertIsNotNone(pending)
            assert pending is not None
            # An actual ACK must already have its durable Core terminal. Until
            # this callback, the exact sealed batch remains in the edge spool.
            self.assertEqual(self.core.receiver.lookup_terminal(pending), ack)
            acknowledgements.append(ack)
            original_ack(ack)
            if ack.sequence == 2:
                fresh_acknowledged.set()
            else:
                acknowledged.set()

        arguments = {"mode": "recent", "conversation_id": self.group, "refresh": True,
                     "limit": 1, "voice": "off", "include_resources": "none",
                     "response_profile": "diagnostic"}
        env = {**os.environ, "SIGHTGLASS_CONFIG": str(self.store.path),
               "SIGHTGLASS_SYNTHETIC_TEST_SECRETS": "1",
               "SIGHTGLASS_TEST_READER_TOKEN": "synthetic-reader",
               "SIGHTGLASS_TEST_OPERATOR_TOKEN": "synthetic-operator"}

        async def wait(event):
            self.assertTrue(await anyio.to_thread.run_sync(lambda: event.wait(3)))

        async def consumer():
            process = StdioServerParameters(command=sys.executable,
                                           args=["-m", "sightglass.mcp.server"], env=env)
            with (self.root / "stdio.stderr").open("w") as log:
                async with stdio_client(process, errlog=log) as streams:
                    async with ClientSession(*streams) as session:
                        await session.initialize()
                        scope = anyio.CancelScope()
                        request_id = session._request_id
                        raw = None

                        async def read():
                            with scope:
                                await session.call_tool("wechat_read_messages", arguments)

                        try:
                            async with anyio.create_task_group() as tasks:
                                if disconnect == "mcp_cancel":
                                    tasks.start_soon(read)
                                else:
                                    raw = socket.socket(socket.AF_UNIX)
                                    raw.connect(str(self.config.socket_path))
                                    send_frame(raw, {
                                        "version": IPC_VERSION, "id": "synthetic-close",
                                        "token": "synthetic-reader", "method": "tools.call",
                                        "params": {"name": "wechat_read_messages",
                                                   "arguments": arguments}})
                                await wait(selected)
                                with anyio.fail_after(5):
                                    status = await session.call_tool("wechat_status", {})
                                self.assertFalse(status.isError)
                                self.assertFalse(source_stopped.is_set())
                                if raw is not None:
                                    raw.close()
                                else:
                                    await session.send_notification(ClientNotification(
                                        CancelledNotification(method="notifications/cancelled",
                                            params=CancelledNotificationParams(
                                                requestId=request_id))))
                                    scope.cancel()
                                await wait(source_stopped)
                                await wait(terminal_waiting)
                                pending = self.spool.pending()
                                assert pending is not None
                                self.assertEqual(pending.document().receipt.terminal, "cancelled")
                                self.assertEqual(pending.document().origin.sequence, 1)
                                self.assertIsNone(self.core.receiver.lookup_terminal(pending))
                                self.assertFalse(acknowledgements)
                                retained = self.spool.pending()
                                assert retained is not None
                                self.assertEqual(retained.to_bytes(), pending.to_bytes())
                                allow_terminal.set()
                                await wait(acknowledged)
                                self.assertIsNone(self.spool.pending())
                                self.assertEqual(acknowledgements[0].terminal, "cancelled")
                                self.assertEqual(self.core.receiver.lookup_terminal(pending),
                                                 acknowledgements[0])
                            with anyio.fail_after(5):
                                fresh = await session.call_tool("wechat_read_messages", arguments)
                            await wait(fresh_acknowledged)
                            self.assertIsNone(self.spool.pending())
                            self.assertFalse(fresh.isError)
                            assert fresh.structuredContent is not None
                            self.assertEqual(fresh.structuredContent["source_receipt"]["view"],
                                             "fresh")
                            self.assertEqual(len(executed), 2)
                            self.assertEqual(len(set(executed)), 2)
                            self.assertEqual(acknowledgements[-1].sequence, 2)
                            self.assertEqual(acknowledgements[-1].terminal, "accepted")
                        finally:
                            allow_terminal.set()
                            scope.cancel()
                            if raw is not None:
                                raw.close()

        with (patch.object(self.executor, "_execute", side_effect=slow),
              patch.object(self.core, "_reject", side_effect=reject),
              patch.object(self.spool, "acknowledge", side_effect=acknowledge)):
            anyio.run(consumer)

    def test_real_mcp_cancel_settles_remote_terminal_before_following_fresh(self):
        self.exercise("mcp_cancel")

    def test_real_ipc_close_settles_remote_terminal_before_following_fresh(self):
        self.exercise("ipc_close")

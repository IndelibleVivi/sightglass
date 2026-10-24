"""Daily recent contacts and ordinary reads, independent of historical residency."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

import anyio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.runtime.config import ConfigStore
from sightglass.runtime.daemon import SightglassDaemon
from sightglass.runtime.secrets import (
    OPERATOR_SECRET_ACCOUNT,
    READER_SECRET_ACCOUNT,
    MemorySecretStore,
    token_hash,
)
from sightglass.source.synthetic import _row
from sightglass.storage import StorageSettings
from tests.fixtures.factory import build_test_stack
from tests.integration import test_daemon_reliability as daemon_fixture
from tests.unit import test_capture_core as fixtures


class DailyRecentsTests(unittest.TestCase):
    root: Path
    source_root: Path
    service: Any
    repository: Any
    core: Any
    config: Any
    settings: Any
    group: str
    spool: Any
    executor: Any
    edge_socket: Any
    edge_thread: Any
    edge_stop: Any
    token: str
    tools: Any
    daemon: Any
    thread: Any
    failures: Any
    store: Any
    secrets: Any
    setUp = fixtures.CoreCaptureTests.setUp
    start_edge = fixtures.CoreCaptureTests.start_edge
    stop_edge = fixtures.CoreCaptureTests.stop_edge
    call = fixtures.CoreCaptureTests.call
    start_daemon = daemon_fixture.DaemonReliabilityTests.start
    stop_daemon = daemon_fixture.DaemonReliabilityTests.stop
    client = daemon_fixture.DaemonReliabilityTests.client

    def direct(self) -> str:
        account = self.repository.account_id_for(self.settings.account_id)
        return self.repository.conversation_id_for(account, "conv_direct")

    def release_bodies(self) -> None:
        self.service.residency.set(self.direct(), mode="on_demand")
        with self.repository.database.transaction() as connection:
            connection.execute("UPDATE messages SET body_available=0")

    def append_direct(self) -> str:
        identity = "synthetic-new-direct-recent"
        row = _row(identity, "conv_direct", "2026-10-25T00:00:00+00:00", 1, 10_001, 1,
                   "Synthetic new direct message", sender="wxid_demo_member",
                   shown_as="Synthetic Direct Contact")
        with closing(sqlite3.connect(self.source_root / "messages-1.db")) as connection:
            connection.execute(
                f"INSERT INTO messages({','.join(row)}) VALUES({','.join('?' for _ in row)})",
                tuple(row.values()),
            )
            connection.commit()
        return identity

    def test_recents_lists_contacts_without_bodies_and_opens_them_normally(self):
        self.release_bodies()
        target = self.append_direct()
        inbox = self.call("wechat_read_inbox", kinds=["direct"], include_latest="text")
        self.assertEqual(inbox["schema"], "sightglass.inbox-page.v2")
        self.assertEqual(inbox["mode"], "recent")
        self.assertEqual([item["conversation_id"] for item in inbox["items"]], [self.direct()])
        item = inbox["items"][0]
        self.assertEqual(item["latest_state"], "not_resident")
        self.assertNotIn("latest", item)  # brief omits the diagnostic null preview
        self.assertEqual(inbox["coverage"]["message_scope"], "catalog_activity")
        self.assertEqual(self.call("wechat_read_inbox", mode="resident")["items"], [])
        self.start_edge()
        action = item["read_action"]
        page = self.call(action["tool"], **action["arguments"], limit=2,
                         voice="off", include_resources="none")
        self.assertTrue(page["messages"], page)
        self.assertEqual(page["source_receipt"]["view"], "fresh")
        ids = tuple(row[0] for row in page["messages"])
        self.assertIn(target, {row["source_message_id"]
                               for row in self.repository.frozen_message_rows(ids)})

    def test_connected_recent_is_current_and_explicit_replica_remains_local(self):
        target = self.append_direct()
        self.start_edge()
        arguments = {"mode": "recent", "conversation_id": self.direct(), "limit": 1,
                     "voice": "off", "include_resources": "none"}
        self.assertFalse(self.service.local_message_read_ready(arguments))
        page = self.call("wechat_read_messages", **arguments)
        self.assertEqual(page["source_receipt"]["view"], "fresh")
        row = self.repository.message_position_row(page["messages"][0][0])
        self.assertEqual(row["source_message_id"], target)
        explicit = dict(arguments, view="replica")
        self.assertTrue(self.service.local_message_read_ready(explicit))
        self.assertEqual(self.call("wechat_read_messages", **explicit)["source_receipt"]["view"],
                         "replica")
        self.stop_edge()
        with self.core.broker._condition:
            self.assertTrue(self.core.broker._condition.wait_for(
                lambda: not self.core.broker.connected, 2))
        self.assertTrue(self.service.local_message_read_ready(arguments))
        cached = self.call("wechat_read_messages", **arguments)
        self.assertEqual(cached["source_receipt"]["view"], "replica")
        self.assertFalse(cached["source_receipt"]["freshness"]["live_refresh_confirmed"])

    def test_contact_recents_apply_policy_and_pause_before_projection(self):
        self.release_bodies()
        self.service.reader.policy = replace(
            self.service.reader.policy, denied_conversation_ids=frozenset({self.direct()}))
        self.assertEqual(self.call("wechat_read_inbox", kinds=["direct"])["items"], [])
        self.service.reader.paused = True
        page = self.call("wechat_read_inbox")
        self.assertEqual(page["code"], ErrorCode.SERVICE_PAUSED.value)
        self.service.reader.paused = False
        self.start_edge()
        arguments = {"mode": "recent", "conversation_id": self.direct(), "limit": 1}
        self.assertTrue(self.service.local_message_read_ready(arguments))
        page = self.call("wechat_read_messages", **arguments)
        self.assertEqual(page["code"], ErrorCode.POLICY_DENIED.value)

    def test_recent_view_binding_survives_transport_changes(self):
        arguments = {"mode": "recent", "conversation_id": self.direct(), "limit": 1,
                     "voice": "off", "include_resources": "none"}
        offline = self.service.bind_recent_view(arguments)
        self.assertEqual(offline["view"], "replica")
        self.start_edge()
        # A reconnect cannot turn the already classified local call into source work.
        self.assertTrue(self.service.local_message_read_ready(offline))
        self.assertEqual(self.call("wechat_read_messages", **offline)["source_receipt"]["view"],
                         "replica")
        fresh = self.service.bind_recent_view(arguments)
        self.assertEqual(fresh["view"], "fresh")
        self.stop_edge()
        with self.core.broker._condition:
            self.assertTrue(self.core.broker._condition.wait_for(
                lambda: not self.core.broker.connected, 2))
        # A disconnect cannot silently replace a selected fresh read with cached text.
        self.assertFalse(self.service.local_message_read_ready(fresh))
        with self.assertRaises(SightglassError) as caught:
            self.call("wechat_read_messages", **fresh)
        self.assertEqual(caught.exception.code, ErrorCode.SERVICE_UNAVAILABLE)

    def test_recent_catalog_cursor_filters_and_conflicted_preview(self):
        first = self.call("wechat_read_inbox", limit=1, include_latest="text")
        cursor = first["page"]["next_cursor"]
        self.assertTrue(cursor)
        with self.repository.database.transaction() as connection:
            connection.execute("UPDATE conversations SET last_message_at=?,unread_count=2 "
                               "WHERE conversation_id=?",
                               ("2026-10-25T00:00:00+00:00", self.direct()))
        stale = self.call("wechat_read_inbox", limit=1, include_latest="text", cursor=cursor)
        self.assertEqual(stale["code"], ErrorCode.CURSOR_STALE.value)
        recent = self.call("wechat_read_inbox", kinds=["direct"], unread_only=True,
                           after="2026-10-24T00:00:00Z", include_latest="text")
        self.assertEqual(len(recent["items"]), 1)
        self.assertEqual(recent["items"][0]["latest_state"], "older_resident")
        self.assertLess(recent["items"][0]["latest"]["sent_at"],
                        recent["items"][0]["last_message_at"])
        self.repository.mark_source_conversation_attention(
            conversation_id=self.direct(), backfill_state="partial",
            error_code="duplicate_message_identity_conflict",
            observed_at="2026-10-25T00:00:00+00:00",
        )
        degraded = self.call("wechat_read_inbox", kinds=["direct"], include_latest="text")
        self.assertEqual(degraded["items"][0]["latest_state"], "source_conflict")
        self.assertNotIn("latest", degraded["items"][0])

    def test_local_confirmed_source_remains_available_for_catalog_recents(self):
        _provider, repository, service, tools = build_test_stack(
            self.source_root, self.root / "local" / "window.db", residency_default="on_demand")
        self.addCleanup(tools.close)
        service.sync_source_once()
        service.status()
        self.assertEqual(service.capture_transport_state(), "unobserved")
        inbox = service.read_inbox(kinds=("direct",))
        self.assertTrue(inbox["items"])
        self.assertTrue(inbox["source_receipt"]["freshness"]["live_refresh_available"])
        self.assertFalse(inbox["source_receipt"]["freshness"]["live_refresh_confirmed"])
        self.assertEqual(repository.observation_watermark(), 0)

    def test_real_stdio_opens_a_body_free_direct_contact_without_refresh_arguments(self):
        target = self.append_direct()
        self.release_bodies()
        self.core.close()
        settings_path = self.root / "enrollment" / "remote.json"
        self.settings.write(settings_path)
        self.config = replace(
            self.config, source_root=None, source_settings_path=settings_path,
            reader_token_hash=token_hash("synthetic-reader"),
            operator_token_hash=token_hash("synthetic-operator"),
            storage=StorageSettings(min_free_bytes=0),
        )
        self.store = ConfigStore(self.config.data_dir / "config.json")
        self.store.save(self.config)
        self.secrets = MemorySecretStore({READER_SECRET_ACCOUNT: "synthetic-reader",
                                         OPERATOR_SECRET_ACCOUNT: "synthetic-operator"})
        self.daemon = SightglassDaemon(config_store=self.store, secret_store=self.secrets)
        self.core = self.daemon.capture_core
        self.thread, self.failures = None, []
        self.addCleanup(self.stop_daemon)
        self.addCleanup(self.stop_edge)
        # Isolate the foreground journey from unrelated scheduled catalog/tail work.
        with patch.object(self.daemon.source_worker, "start"):
            self.start_daemon()
        self.start_edge()
        env = {**os.environ, "SIGHTGLASS_CONFIG": str(self.store.path),
               "SIGHTGLASS_SYNTHETIC_TEST_SECRETS": "1",
               "SIGHTGLASS_TEST_READER_TOKEN": "synthetic-reader",
               "SIGHTGLASS_TEST_OPERATOR_TOKEN": "synthetic-operator"}

        def unwrap(result):
            self.assertFalse(result.isError, result)
            return result.structuredContent or json.loads(result.content[0].text)

        async def consumer():
            process = StdioServerParameters(command=sys.executable,
                                           args=["-m", "sightglass.mcp.server"], env=env)
            with (self.root / "daily-stdio.stderr").open("w") as log:
                async with stdio_client(process, errlog=log) as streams:
                    async with ClientSession(*streams) as session:
                        await session.initialize()
                        catalog = await session.list_tools()
                        self.assertEqual(len(catalog.tools), 13)
                        inbox_tool = next(tool for tool in catalog.tools
                                          if tool.name == "wechat_read_inbox")
                        self.assertEqual(inbox_tool.inputSchema["properties"]["mode"]["default"],
                                         "recent")
                        inbox = unwrap(await session.call_tool("wechat_read_inbox", {
                            "kinds": ["direct"], "include_latest": "text",
                        }))
                        item = inbox["items"][0]
                        self.assertEqual(item["latest_state"], "not_resident")
                        action = item["read_action"]
                        self.assertNotIn("refresh", action["arguments"])
                        page = unwrap(await session.call_tool(action["tool"], {
                            **action["arguments"], "limit": 1, "voice": "off",
                            "include_resources": "none",
                        }))
                        self.assertEqual(page["source_receipt"]["view"], "fresh")
                        row = self.daemon.tools.service.repository.message_position_row(
                            page["messages"][0][0])
                        self.assertEqual(row["source_message_id"], target)
                        self.assertEqual(page["conversation"]["id"], item["conversation_id"])

        anyio.run(consumer)

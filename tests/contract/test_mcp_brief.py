from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import anyio
from mcp.types import EmbeddedResource, TextResourceContents

from sightglass.contracts.voice import VoiceReadSettings
from sightglass.mcp.bridge import DaemonReaderTools
from sightglass.mcp.projection import BRIEF_PROFILE, project_result
from sightglass.mcp.server import create_server
from sightglass.mcp.tools import ReaderTools
from sightglass.model.links import LinkRepository
from sightglass.runtime.lanes import RuntimeLanes
from sightglass.runtime.search_preparation import SearchPreparation
from sightglass.runtime.source_worker import SourceWorker
from sightglass.source.synthetic import create_synthetic_source
from sightglass.storage import StorageBudget, StorageSettings
from tests.fixtures.factory import build_test_stack
from tests.fixtures.voice_source import declare_voice_messages
from tests.integration.test_cold_retrieval_discovery import _append_source_row
from tests.integration.test_compact_projection import _add_compact_messages


def size(value) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


class McpBriefTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "source"
        create_synthetic_source(self.root)
        self.build()

    def build(self, **kwargs) -> None:
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            self.root, Path(self.temp.name) / "window.db", default_projection=None,
            response_profile="brief", **kwargs,
        )
        self.group = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]

    def tearDown(self) -> None:
        if hasattr(self, "manager"):
            self.assertTrue(self.manager.stop())
        self.tools.close()
        self.temp.cleanup()

    def assert_actions(self, result) -> None:
        for action in result.get("next_actions", []):
            self.assertTrue(action["tool"].startswith("wechat_"))
            if "token_path" in action:
                value = result
                for slot in action["token_path"].split("."):
                    value = value[slot]
                self.assertIsInstance(value, str, action)
                self.assertTrue(value, action)
                self.assertIn(action["parameter"], {"cursor", "reading_token"})

    def test_complete_catalog_matches_and_registered_calls_default_to_brief(self) -> None:
        # Raw ReaderTools has the production default, without the legacy fixture shim.
        local = create_server(ReaderTools(self.service, voice_service=self.tools.voice_service))
        daemon = create_server(DaemonReaderTools())
        self.assertEqual(
            [tool.model_dump() for tool in anyio.run(local.list_tools)],
            [tool.model_dump() for tool in anyio.run(daemon.list_tools)],
        )
        self.assertEqual(len(anyio.run(local.list_tools)), 13)

        async def call():
            return await local._tool_manager.call_tool("wechat_status", {})

        result = anyio.run(call)
        self.assertEqual(result["response_profile"], BRIEF_PROFILE)
        self.assertNotIn("inventory_digest", result["source"])
        full = self.tools.wechat_status(response_profile="diagnostic")
        self.assertIn("inventory_digest", full["source"])

        async def strict_compatibility():
            value = await local._tool_manager.call_tool("wechat_read_messages", {
                "conversation_id": self.group, "strict": True,
            })
            self.assertEqual(value["response_profile"], BRIEF_PROFILE)
            with self.assertRaisesRegex(Exception, "literal_error"):
                await local._tool_manager.call_tool("wechat_read_messages", {
                    "conversation_id": self.group, "strict": False,
                })

        anyio.run(strict_compatibility)

    def test_fresh_stdio_catalog_does_not_load_domain_services(self) -> None:
        code = """
import sys
import anyio
from sightglass.mcp.bridge import DaemonReaderTools
from sightglass.mcp.server import create_server
server = create_server(DaemonReaderTools())
assert len(anyio.run(server.list_tools)) == 13
for name in ('sightglass.reader.service', 'sightglass.voice.service',
             'sightglass.model.repositories', 'sightglass.resources.processors'):
    assert name not in sys.modules, name
from sightglass.reader import ReaderService
from sightglass.reader.service import ReaderService as CanonicalReaderService
assert ReaderService is CanonicalReaderService
"""
        completed = subprocess.run([sys.executable, "-c", code], capture_output=True,
                                   text=True, timeout=15)
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_fast_status_rejects_malformed_profiles_before_service_access(self) -> None:
        from sightglass.runtime.daemon import SightglassDaemon

        daemon = object.__new__(SightglassDaemon)
        for profile in ([], {}, None, 1, "unknown"):
            with self.subTest(profile=profile):
                result = daemon._fast_status({"response_profile": profile})
                self.assertEqual(result["code"], "QUERY_INVALID")

    def test_compact_attachments_and_resource_text_remain_reachable(self) -> None:
        page = self.tools.wechat_read_messages(conversation_id=self.group, limit=100)
        self.assertEqual(page["fields"], ["id", "when", "who", "kind", "what", "resources"])
        for row in page["messages"]:
            self.assertEqual(len(row), len(page["fields"]))
            self.assertLess(row[2], len(page["people"]))
        attachments = [row for row in page["messages"] if row[5]]
        self.assertTrue(attachments)
        text_resource = None
        for row in attachments:
            listed = self.tools.wechat_list_resources(row[0])
            self.assertEqual(listed["message_id"], row[0])
            descriptors = listed["resources"]
            self.assertTrue(descriptors)
            for descriptor in descriptors:
                if descriptor.get("available_views", {}).get("text"):
                    text_resource = descriptor["resource_id"]
        assert isinstance(text_resource, str)
        result = self.tools.wechat_read_resource(text_resource, mode="text")
        self.assertFalse(result.isError, result.structuredContent)
        self.assertGreater(len(result.content), 1)
        assert result.structuredContent is not None
        self.assertEqual(result.structuredContent["response_profile"], BRIEF_PROFILE)
        block = result.content[1]
        assert isinstance(block, EmbeddedResource)
        assert isinstance(block.resource, TextResourceContents)
        self.assertTrue(block.resource.text)
        found = self.tools.wechat_search_resource_text(text_resource, "Synthetic")
        self.assertNotIn("code", found)
        inbox = self.tools.wechat_read_inbox()
        self.assertTrue(inbox["items"])
        self.assertEqual(inbox["response_profile"], BRIEF_PROFILE)

    def test_multibyte_page_budget_keeps_all_ids_reachable_and_long_detail_is_bounded(self) -> None:
        self.tools.close()
        _add_compact_messages(self.root, long_final_body=True)
        self.build()
        ids = set()
        cursor = None
        first = True
        for _ in range(30):
            result = self.tools.wechat_read_messages(
                conversation_id=self.group, limit=500, cursor=cursor,
            )
            self.assertNotIn("code", result, result)
            self.assertLessEqual(size(result), 16 * 1024)
            self.assert_actions(result)
            returned = {row[0] for row in result["messages"]}
            self.assertFalse(returned & ids)
            ids.update(returned)
            if first:
                long_id = result["messages"][-1][0]
                detail = self.tools.wechat_read_messages(mode="message", message_id=long_id)
                self.assertLessEqual(size(detail), 16 * 1024)
                self.assertEqual(detail["messages"][0]["body_truncated"]["full_chars"], 50_000)
                self.assertLess(len(detail["messages"][0]["text"]), 50_000)
                full = self.tools.wechat_read_messages(
                    mode="message", message_id=long_id, response_profile="diagnostic"
                )
                self.assertEqual(len(full["messages"][0]["text"]), 50_000)
                first = False
            cursor = result.get("page", {}).get("next_cursor")
            if not cursor:
                break
        else:
            self.fail("bounded message pages did not drain")
        self.assertGreaterEqual(len(ids), 500)

    def test_search_budget_pages_and_scope_are_preserved(self) -> None:
        self.tools.close()
        _add_compact_messages(self.root)
        self.build()
        self.service.sync_source_once(initial_tail=600, conversation_limit=100)
        ids = set()
        cursor = None
        for _ in range(50):
            result = self.tools.wechat_search_messages(
                "猫", conversation_ids=[self.group], cursor=cursor,
            )
            self.assertNotIn("code", result, result)
            self.assertLessEqual(size(result), 8192)
            self.assertLessEqual(len(result["hits"]), 20)
            self.assert_actions(result)
            returned = {row[0] for row in result["hits"]}
            self.assertFalse(returned & ids)
            ids.update(returned)
            cursor = result.get("page", {}).get("next_cursor")
            if not cursor:
                break
            wrong = self.tools.wechat_search_messages(
                "wrong synthetic query", conversation_ids=[self.group], cursor=cursor,
            )
            self.assertEqual(wrong["code"], "CURSOR_INVALID")
        else:
            self.fail("bounded search pages did not drain")
        self.assertEqual(len(ids), 500)

    def test_updates_pending_payload_is_exact_across_profiles(self) -> None:
        original = self.tools.wechat_read_messages(
            mode="updates", conversation_id=self.group, limit=2, response_profile="diagnostic"
        )
        self.assertTrue(original["page"]["delivery_id"])
        for profile in ("brief", "diagnostic"):
            replay = self.tools.wechat_read_messages(
                mode="updates", conversation_id=self.group, limit=2, response_profile=profile,
            )
            self.assertEqual(json.dumps(replay, sort_keys=True),
                             json.dumps(original, sort_keys=True))
            self.assertNotIn("response_profile", replay)
            self.assertNotIn("next_actions", replay)
        acked = self.tools.wechat_read_messages(
            mode="updates", conversation_id=self.group, limit=2,
            ack_delivery_id=original["page"]["delivery_id"],
        )
        self.assertNotEqual(acked["page"]["delivery_id"], original["page"]["delivery_id"])

    def test_transcripts_drain_ready_text_after_old_processing_events(self) -> None:
        self.tools.close()
        declare_voice_messages(self.root, count=2, available=True)
        self.build(voice=VoiceReadSettings(enabled=True, default_policy="auto", language="zh"))
        page = self.tools.wechat_read_messages(conversation_id=self.group, limit=30)
        token = page["voice"]["reading_token"]
        voice = self.tools.voice_service
        assert voice is not None
        for job in voice.repository.rows("SELECT * FROM voice_jobs"):
            fence = voice.lease(job["job_id"], owner_id="synthetic-brief-worker")
            voice.complete(job["job_id"], owner_id="synthetic-brief-worker", fencing_token=fence,
                           text="Synthetic完整文本" * 500)
        cursor = None
        text = []
        old_pending = False
        for _ in range(20):
            result = self.tools.wechat_read_transcripts(token, cursor=cursor, wait_ms=0)
            self.assertTrue(result["processing_complete"])
            self.assert_actions(result)
            for item in result["items"]:
                self.assertEqual(len(item), len(result["fields"]))
                if item[3] == "ready":
                    text.append(item[4])
                else:
                    old_pending = True
            cursor = result.get("next_cursor")
            if not result["has_more_results_now"]:
                break
        self.assertTrue(old_pending)
        self.assertEqual(text, ["Synthetic完整文本" * 500] * 2)

    def test_preparation_and_dual_partial_actions_have_reachable_separate_tokens(self) -> None:
        pending = project_result("wechat_find_links", {
            "schema": "sightglass.retrieval-preparation.v1", "state": "preparing",
            "reading_token": "opaque-synthetic-poll", "retry_after_ms": 1000,
        }, {}, "brief")
        self.assertNotIn("items", pending)
        self.assertEqual(pending["next_actions"][0]["kind"], "poll")
        self.assertEqual(pending["next_actions"][0]["wait_ms"], 1000)
        self.assert_actions(pending)
        partial = project_result("wechat_find_links", {
            "schema": "sightglass.link-search.v1", "items": [],
            "page": {"next_cursor": "opaque-synthetic-result", "has_more": True},
            "source_receipt": {"complete": False, "source_continuation": {
                "reading_token": "opaque-synthetic-source", "available": True}},
        }, {}, "brief")
        self.assertEqual({item["kind"] for item in partial["next_actions"]},
                         {"result_page", "source_scan"})
        self.assert_actions(partial)
        transcript = project_result("wechat_read_transcripts", {
            "schema": "sightglass.voice-transcripts.v1", "items": [],
            "reading_token": "opaque-synthetic-batch", "next_cursor": None,
            "processing_complete": False, "has_more_results_now": False,
            "wait": {"retry_after_ms": 1200},
        }, {"reading_token": "opaque-synthetic-batch"}, "brief")
        self.assertEqual(transcript["next_actions"][0]["kind"], "poll")
        self.assertEqual(transcript["next_actions"][0]["arguments"], {"wait_ms": 8000})
        self.assertEqual(transcript["next_actions"][0]["wait_ms"], 1200)
        self.assert_actions(transcript)

    def test_projected_receipt_counts_actual_json_and_escape_heavy_detail_is_bounded(self) -> None:
        status = self.tools.wechat_status()
        with self.repository.database.connection() as connection:
            receipt = connection.execute(
                "SELECT bytes_returned FROM access_receipts WHERE tool_name='wechat_status' "
                "ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
        self.assertEqual(receipt["bytes_returned"], size(status))
        self.tools.close()
        body = "Synthetic escape-heavy " + ('"\\' * 15_000)
        _append_source_row(
            self.root, message_id="synthetic-escape-heavy", conversation_id="conv_group",
            rowid=94_000, sent_at="2026-09-28T00:00:00+00:00", content=body,
        )
        self.build()
        recent = self.tools.wechat_read_messages(conversation_id=self.group, limit=1)
        message_id = recent["messages"][0][0]
        detail = self.tools.wechat_read_messages(mode="message", message_id=message_id)
        self.assertLessEqual(size(detail), 16384)
        self.assertEqual(detail["messages"][0]["body_truncated"]["full_chars"], len(body))
        self.assert_actions(detail)

    def test_indivisible_url_and_context_explicitly_exceed_soft_target(self) -> None:
        self.tools.close()
        # Valid under the existing 8192-character extraction bound; its repeated
        # raw/normalized forms make the indivisible JSON envelope exceed 16 KiB.
        url = "https://exception-brief.example/" + "x" * 8000 + "?synthetic=1#full"
        _append_source_row(
            self.root, message_id="synthetic-long-url", conversation_id="conv_group",
            rowid=95_000, sent_at="2026-09-28T00:00:00+00:00",
            content="Synthetic exceptionneedle " + url,
        )
        self.build()
        self.service.sync_source_once(initial_tail=100, conversation_limit=100)
        links = LinkRepository(self.repository.database)
        while links.backfill_batch()["state"] != "ready":
            pass
        result = self.tools.wechat_find_links(domains=["exception-brief.example"])
        self.assertNotIn("code", result, result)
        self.assertEqual(result["response_budget"]["exception"], "single_item")
        self.assertGreater(size(result), 8192)
        self.assertEqual(result["items"][0]["raw_url"], url)
        retrieved = self.tools.wechat_retrieve("exceptionneedle")
        self.assertNotIn("code", retrieved, retrieved)
        self.assertEqual(retrieved["response_budget"]["exception"], "single_context")
        self.assertGreater(size(retrieved), 16384)
        self.assertEqual(retrieved["contexts"][0]["links"][0]["raw_url"], url)

    def test_link_budget_continuations_preserve_full_authorized_urls(self) -> None:
        self.tools.close()
        for index in range(24):
            _append_source_row(
                self.root, message_id=f"synthetic-brief-link-{index}",
                conversation_id="conv_group", rowid=90_000 + index,
                sent_at=f"2026-09-20T00:00:{index:02d}+00:00",
                content=f"https://synthetic:credential@brief.example/{'x' * 350}/{index}"
                        "?token=synthetic#fragment",
            )
        self.build()
        self.service.sync_source_once(initial_tail=100, conversation_limit=100)
        links = LinkRepository(self.repository.database)
        while links.backfill_batch()["state"] != "ready":
            pass
        ids = set()
        cursor = None
        for _ in range(25):
            result = self.tools.wechat_find_links(domains=["brief.example"], cursor=cursor)
            self.assertNotIn("code", result, result)
            self.assertLessEqual(size(result), 8192)
            self.assert_actions(result)
            for item in result["items"]:
                self.assertIn("?token=synthetic#fragment", item["raw_url"])
                self.assertIn("synthetic:credential@", item["raw_url"])
                self.assertNotIn(item["message_id"], ids)
                ids.add(item["message_id"])
            cursor = result.get("page", {}).get("next_cursor")
            if not cursor:
                break
        self.assertEqual(len(ids), 24)

    def test_old_preparation_limit_survives_new_default_and_query_stays_bound(self) -> None:
        self.service.storage = StorageBudget(self.repository.database.path.parent,
            self.repository.database.path, StorageSettings(min_free_bytes=0))
        self.repository.database.storage = self.service.storage
        worker = SourceWorker(self.service)
        manager = SearchPreparation(self.service, self.repository.database.path.with_name(
            "search-preparation.json"), binding="synthetic-brief", lanes=RuntimeLanes(),
            source_worker=worker)
        self.manager = manager
        original = self.tools.wechat_search_messages("消息", limit=50)
        token = original["reading_token"]
        self.assert_actions(original)
        # New client omits limit; the signed token resolves the existing job's 50.
        same = self.tools.wechat_search_messages("消息", reading_token=token)
        self.assertEqual(same["reading_token"], token)
        wrong = self.tools.wechat_search_messages("different", reading_token=token)
        self.assertEqual(wrong["code"], "CURSOR_INVALID")
        self.assertNotIn('"query":', manager.path.read_text())
        manager.start()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            result = self.tools.wechat_search_messages("消息", reading_token=token)
            if result["schema"] != "sightglass.search-preparation.v1":
                break
            time.sleep(0.01)
        self.assertEqual(result["schema"], "sightglass.search-results.v2", result)
        self.assertLessEqual(size(result), 8192)

    def test_retrieve_budget_pages_keep_each_focus_reachable(self) -> None:
        self.tools.close()
        for index in range(8):
            _append_source_row(
                self.root, message_id=f"synthetic-context-{index}",
                conversation_id="conv_group", rowid=91_000 + index,
                sent_at=f"2026-09-{20 + index:02d}T00:00:00+00:00",
                content="Synthetic contextneedle " + "多" * 2000,
            )
        self.build()
        self.service.sync_source_once(initial_tail=100, conversation_limit=100)
        focus = set()
        cursor = None
        for _ in range(20):
            result = self.tools.wechat_retrieve("contextneedle", limit=20, cursor=cursor)
            self.assertNotIn("code", result, result)
            self.assertLessEqual(size(result), 16384)
            self.assert_actions(result)
            for context in result["contexts"]:
                ids = set(context["focus_message_ids"])
                self.assertFalse(ids & focus)
                focus.update(ids)
            cursor = result.get("page", {}).get("next_cursor")
            if not cursor:
                break
        self.assertEqual(len(focus), 8)

    def test_cold_partial_result_pages_and_source_scans_are_independent(self) -> None:
        self.tools.close()
        for index in range(80):
            _append_source_row(
                self.root, message_id=f"synthetic-cold-brief-{index}",
                conversation_id="conv_group", rowid=92_000 + index,
                sent_at=f"2026-09-20T00:{index // 60:02d}:{index % 60:02d}+00:00",
                content=f"https://cold-brief.example/{index}",
            )
        self.build(residency_default="on_demand")
        self.service.storage = StorageBudget(self.repository.database.path.parent,
            self.repository.database.path, StorageSettings(min_free_bytes=0))
        self.repository.database.storage = self.service.storage
        worker = SourceWorker(self.service)
        manager = SearchPreparation(self.service, self.repository.database.path.with_name(
            "search-preparation.json"), binding="synthetic-cold-brief", lanes=RuntimeLanes(),
            source_worker=worker)
        self.manager = manager
        arguments = {"domains": ["cold-brief.example"], "conversation_ids": [self.group],
                     "limit": 1}
        manager.start()
        deadline = time.monotonic() + 10
        token = None
        found_dual = False
        with patch("sightglass.reader.service.DISCOVERY_CONVERSATION_SCAN_BUDGET", 60):
            while time.monotonic() < deadline:
                result = self.tools.wechat_find_links(**arguments, reading_token=token)
                self.assertNotIn("code", result, result)
                self.assert_actions(result)
                if result["schema"] == "sightglass.retrieval-preparation.v1":
                    self.assertNotIn("items", result)
                    token = result["reading_token"]
                    time.sleep(0.01)
                    continue
                kinds = {action["kind"] for action in result.get("next_actions", [])}
                if {"result_page", "source_scan"} <= kinds:
                    found_dual = True
                    source = result["source_receipt"]["source_continuation"]["reading_token"]
                    cursor = result["page"]["next_cursor"]
                    with patch.object(self.provider, "snapshot",
                                      side_effect=AssertionError("result page reopened source")):
                        second = self.tools.wechat_find_links(**arguments, cursor=cursor)
                    self.assertNotIn("code", second, second)
                    self.assertEqual(source, second["source_receipt"]["source_continuation"][
                        "reading_token"])
                    resumed = self.tools.wechat_find_links(**arguments, reading_token=source)
                    again = self.tools.wechat_find_links(**arguments, reading_token=source)
                    self.assertEqual(resumed.get("reading_token"), again.get("reading_token"))
                    break
                token = result["source_receipt"]["source_continuation"]["reading_token"]
        self.assertTrue(found_dual)

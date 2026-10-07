from __future__ import annotations

import inspect
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import anyio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import ImageContent

from sightglass.mcp.bridge import DaemonReaderTools
from sightglass.mcp.server import create_server
from sightglass.mcp.tools import ReaderTools
from sightglass.resources.processors import processor_status
from sightglass.runtime.config import ConfigStore, SightglassConfig
from sightglass.runtime.ipc import IPCClient
from sightglass.runtime.secrets import (
    OPERATOR_SECRET_ACCOUNT,
    READER_SECRET_ACCOUNT,
    MemorySecretStore,
    token_hash,
)
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


class McpContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name) / "source"
        create_synthetic_source(root)
        _provider, _repository, _service, self.tools = build_test_stack(
            root, Path(self.temp.name) / "window.db"
        )

    def tearDown(self):
        self.temp.cleanup()

    def test_registered_read_messages_rejects_string_refresh_before_execution(self) -> None:
        client = Mock(spec=IPCClient)
        client.call.side_effect = AssertionError("string refresh reached IPC")
        for tools in (self.tools, DaemonReaderTools(client)):
            with self.subTest(surface=type(tools).__name__):
                server = create_server(tools)

                async def call_invalid() -> None:
                    await server._tool_manager.call_tool(
                        "wechat_read_messages", {"refresh": "true"}
                    )

                with patch.object(self.tools.service, "read_messages") as read:
                    with self.assertRaisesRegex(Exception, "bool_type"):
                        anyio.run(call_invalid)
                    read.assert_not_called()
        client.call.assert_not_called()

    def test_exact_account_wide_tool_surface_and_typed_json_schema(self):
        server = create_server(self.tools)
        registered = {tool.name: tool for tool in server._tool_manager.list_tools()}
        self.assertEqual(
            set(registered),
            {
                "wechat_status",
                "wechat_find_conversations",
                "wechat_read_inbox",
                "wechat_find_participants",
                "wechat_read_messages",
                "wechat_read_transcripts",
                "wechat_search_messages",
                "wechat_find_links",
                "wechat_retrieve",
                "wechat_find_resources",
                "wechat_list_resources",
                "wechat_read_resource",
                "wechat_search_resource_text",
            },
        )
        read_properties = registered["wechat_read_messages"].parameters["properties"]
        limit_schema = read_properties["limit"]
        limit_variants = limit_schema.get("anyOf", [limit_schema])
        self.assertIn("integer", {variant.get("type") for variant in limit_variants})
        self.assertNotIn("strict", read_properties)
        self.assertEqual(read_properties["response_profile"]["default"], "brief")
        self.assertEqual(read_properties["refresh"]["type"], "boolean")
        self.assertEqual(read_properties["refresh"]["default"], False)
        self.assertIn("partial", read_properties["refresh"]["description"])
        self.assertEqual(
            set(registered["wechat_status"].parameters["properties"]["detail"]["enum"]),
            {"summary", "sources", "capabilities"},
        )
        self.assertEqual(
            set(read_properties["mode"]["enum"]),
            {"recent", "context", "updates", "range", "message", "speaker"},
        )
        self.assertEqual(set(read_properties["system_policy"]["enum"]), {"include", "omit"})
        resource_projection = read_properties["include_resources"]
        resource_variants = resource_projection.get("anyOf", [resource_projection])
        self.assertEqual(
            set(next(item["enum"] for item in resource_variants if "enum" in item)),
            {"none", "indicator", "metadata"},
        )
        self.assertIn("compact accepts", resource_projection["description"])
        self.assertIn("opaque message ID", read_properties["anchor"]["description"])
        voice_schema = read_properties["voice"]
        voice_variants = voice_schema.get("anyOf", [voice_schema])
        voice_enum = next(
            (variant.get("enum") for variant in voice_variants if "enum" in variant), None
        )
        self.assertEqual(set(voice_enum or ()), {"auto", "cached", "off"})
        self.assertIn("null", {variant.get("type") for variant in voice_variants})
        participant_schema = read_properties["participant_ids"]
        array_schema = participant_schema.get("anyOf", [participant_schema])[0]
        self.assertEqual(array_schema["type"], "array")
        self.assertEqual(array_schema["items"]["type"], "string")
        participant_properties = registered["wechat_find_participants"].parameters["properties"]
        participant_cursor = participant_properties["cursor"]
        participant_cursor_variants = participant_cursor.get("anyOf", [participant_cursor])
        self.assertIn("string", {variant.get("type") for variant in participant_cursor_variants})
        search_properties = registered["wechat_search_messages"].parameters["properties"]
        cursor_schema = search_properties["cursor"]
        cursor_variants = cursor_schema.get("anyOf", [cursor_schema])
        self.assertIn("string", {variant.get("type") for variant in cursor_variants})
        self.assertIn("two-sided anchor window", read_properties["cursor"]["description"])
        transcript_tool = registered["wechat_read_transcripts"]
        transcript_properties = transcript_tool.parameters["properties"]
        self.assertEqual(set(transcript_properties),
                         {"reading_token", "cursor", "wait_ms", "response_profile"})
        self.assertEqual(transcript_properties["reading_token"]["type"], "string")
        self.assertEqual(transcript_tool.parameters["required"], ["reading_token"])
        transcript_cursor = transcript_properties["cursor"]
        cursor_variants = transcript_cursor.get("anyOf", [transcript_cursor])
        self.assertIn("string", {variant.get("type") for variant in cursor_variants})
        transcript_wait = transcript_properties["wait_ms"]
        wait_variants = transcript_wait.get("anyOf", [transcript_wait])
        self.assertIn("integer", {variant.get("type") for variant in wait_variants})
        self.assertIn("15000", transcript_wait["description"])
        self.assertNotIn("strict", search_properties)
        for retrieval_tool in ("wechat_find_links", "wechat_retrieve"):
            retrieval_properties = registered[retrieval_tool].parameters["properties"]
            self.assertIn("reading_token", retrieval_properties)
            token_schema = retrieval_properties["reading_token"]
            variants = token_schema.get("anyOf", [token_schema])
            self.assertIn("string", {variant.get("type") for variant in variants})
        resource_properties = registered["wechat_read_resource"].parameters["properties"]
        self.assertEqual(
            set(resource_properties["mode"]["enum"]),
            {
                "metadata",
                "preview",
                "original",
                "text",
                "page",
                "members",
                "table",
                "slide",
            },
        )
        self.assertTrue({"member", "sheet", "cell_range"}.issubset(resource_properties))
        reader_state_tools = {"wechat_read_messages", "wechat_read_transcripts"}
        for name, tool in registered.items():
            annotations = tool.annotations
            if annotations is None:
                self.fail(f"{tool.name} is missing tool annotations")
            self.assertEqual(annotations.readOnlyHint, name not in reader_state_tools)
            self.assertFalse(annotations.destructiveHint)
            self.assertEqual(annotations.openWorldHint, name == "wechat_retrieve")

        daemon_registered = {
            tool.name: tool
            for tool in create_server(DaemonReaderTools())._tool_manager.list_tools()
        }
        for name in registered:
            self.assertEqual(daemon_registered[name].parameters, registered[name].parameters)
            self.assertEqual(daemon_registered[name].description, registered[name].description)
            self.assertEqual(daemon_registered[name].annotations, registered[name].annotations)


    def test_discovery_reading_token_reaches_daemon_unchanged(self):
        client = Mock(spec=IPCClient)
        client.call.return_value = {"schema": "sightglass.retrieval-preparation.v1"}
        bridge = DaemonReaderTools(client)
        token = "synthetic-discovery-token"
        for name, arguments in (
            ("wechat_find_links", {"domains": ["synthetic.example"]}),
            ("wechat_retrieve", {"concept": "synthetic concept"}),
        ):
            with self.subTest(tool=name):
                getattr(bridge, name)(**arguments, reading_token=token)
                self.assertEqual(client.call.call_args.args[0], "tools.call")
                forwarded = client.call.call_args.args[1]
                self.assertEqual(forwarded["name"], name)
                self.assertEqual(forwarded["arguments"]["reading_token"], token)

    def test_reader_identity_cannot_be_spoofed_by_tool_arguments(self):
        for name in (
            "wechat_status",
            "wechat_find_conversations",
            "wechat_read_inbox",
            "wechat_find_participants",
            "wechat_read_messages",
            "wechat_read_transcripts",
            "wechat_search_messages",
            "wechat_find_resources",
            "wechat_list_resources",
            "wechat_read_resource",
            "wechat_search_resource_text",
        ):
            parameters = inspect.signature(getattr(ReaderTools, name)).parameters
            self.assertNotIn("reader_id", parameters)
        transcript_parameters = inspect.signature(ReaderTools.wechat_read_transcripts).parameters
        self.assertEqual(
            list(transcript_parameters),
            ["self", "reading_token", "cursor", "wait_ms", "response_profile"],
        )

    def test_transcript_errors_use_the_structured_error_envelope(self):
        unknown = self.tools.wechat_read_transcripts("wxvoice_unknown", wait_ms=0)
        self.assertEqual(unknown["schema"], "sightglass.error.v1")
        self.assertEqual(unknown["code"], "CURSOR_INVALID")
        invalid_arguments: list[dict[str, Any]] = [
            {"reading_token": "", "wait_ms": 0},
            {"reading_token": "wxvoice_unknown", "wait_ms": -1},
            {"reading_token": "wxvoice_unknown", "wait_ms": "soon"},
            {"reading_token": "wxvoice_unknown", "cursor": 5},
        ]
        for arguments in invalid_arguments:
            invalid = self.tools.wechat_read_transcripts(**arguments)
            self.assertEqual(invalid["code"], "QUERY_INVALID")

    def test_m2_modes_use_structured_errors(self):
        result = self.tools.wechat_read_messages(mode="updates", conversation_id="wxconv_unknown")
        self.assertEqual(result["schema"], "sightglass.error.v1")
        self.assertEqual(result["code"], "CONVERSATION_NOT_FOUND")

    def test_resource_direct_ids_use_structured_errors_on_fresh_state(self):
        listed = self.tools.wechat_list_resources("wxmsg_unknown")
        read = self.tools.wechat_read_resource(resource_id="wxres_unknown")
        searched = self.tools.wechat_search_resource_text(
            resource_id="wxres_unknown", query="anything"
        )
        self.assertEqual(listed["code"], "MESSAGE_NOT_FOUND")
        self.assertEqual((read.structuredContent or {})["code"], "RESOURCE_NOT_FOUND")
        self.assertEqual(searched["code"], "RESOURCE_NOT_FOUND")

    def test_context_requires_anchor_or_message(self):
        result = self.tools.wechat_read_messages(
            mode="context", conversation_id="wxconv_unknown", before=1, after=1
        )
        self.assertEqual(result["code"], "QUERY_INVALID")

    def test_tool_calls_write_no_stdout(self):
        output = io.StringIO()
        with redirect_stdout(output):
            result = self.tools.wechat_status()
        self.assertEqual(output.getvalue(), "")
        self.assertEqual(result["schema"], "sightglass.status.v1")

    def test_real_stdio_subprocess_exposes_exact_protocol_surface(self):
        reader_token = "synthetic-reader-token"
        operator_token = "synthetic-operator-token"
        state_root = Path(self.temp.name) / "stdio-state"
        config = replace(
            SightglassConfig.create(state_root, Path(self.temp.name) / "source"),
            reader_token_hash=token_hash(reader_token),
            operator_token_hash=token_hash(operator_token),
        )
        config_store = ConfigStore(state_root / "config.json")
        config_store.save(config)
        process_env = {
            **os.environ,
            "SIGHTGLASS_CONFIG": str(config_store.path),
            "SIGHTGLASS_SYNTHETIC_TEST_SECRETS": "1",
            "SIGHTGLASS_TEST_READER_TOKEN": reader_token,
            "SIGHTGLASS_TEST_OPERATOR_TOKEN": operator_token,
        }
        daemon_stderr_path = Path(self.temp.name) / "daemon.stderr"
        daemon_stderr = daemon_stderr_path.open("w", encoding="utf-8")
        daemon = subprocess.Popen(
            [sys.executable, "-m", "sightglass.runtime.daemon", "--config", str(config_store.path)],
            stdin=subprocess.DEVNULL,
            stdout=daemon_stderr,
            stderr=daemon_stderr,
            env=process_env,
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not config.socket_path.exists():
            if daemon.poll() is not None:
                self.fail(daemon_stderr_path.read_text(encoding="utf-8"))
            time.sleep(0.02)
        self.assertTrue(config.socket_path.exists())

        async def exercise() -> tuple[
            list[str],
            bool,
            str | None,
            str | None,
            str | None,
            str | None,
            str | None,
            str | None,
            bool,
        ]:
            stderr_path = Path(self.temp.name) / "mcp.stderr"
            params = StdioServerParameters(
                command=sys.executable,
                args=["-m", "sightglass.mcp.server"],
                env=process_env,
            )
            with stderr_path.open("w", encoding="utf-8") as errlog:
                async with stdio_client(params, errlog=errlog) as (read, write):
                    async with ClientSession(read, write) as session:
                        initialized = await session.initialize()
                        self.assertEqual(initialized.protocolVersion, "2025-11-25")
                        listed = await session.list_tools()
                        transcript_description = next(
                            tool.description for tool in listed.tools
                            if tool.name == "wechat_read_transcripts"
                        ) or ""
                        for field in (
                            "fields", "next_cursor", "has_more_results_now",
                            "processing_complete",
                        ):
                            self.assertIn(field, transcript_description)
                        result = await session.call_tool("wechat_status", {})
                        self.assertEqual((result.structuredContent or {})["response_profile"],
                                         "sightglass.mcp.brief.v1")
                        self.assertNotIn("runtime", result.structuredContent or {})
                        diagnostic = await session.call_tool(
                            "wechat_status", {"response_profile": "diagnostic"}
                        )
                        self.assertIn("runtime", diagnostic.structuredContent or {})
                        transcripts = await session.call_tool(
                            "wechat_read_transcripts",
                            {"reading_token": "wxvoice_unknown", "wait_ms": 0},
                        )
                        search = await session.call_tool(
                            "wechat_search_messages", {"query": "消息", "limit": 1}
                        )
                        self.assertEqual((search.structuredContent or {})["state"], "preparing")
                        reading_token = (search.structuredContent or {})["reading_token"]
                        for _ in range(100):
                            search = await session.call_tool(
                                "wechat_search_messages",
                                {"query": "消息", "limit": 1, "cursor": reading_token},
                            )
                            if (search.structuredContent or {}).get("schema") != (
                                "sightglass.search-preparation.v1"
                            ):
                                break
                            self.assertNotEqual(
                                (search.structuredContent or {}).get("state"), "failed"
                            )
                            await anyio.sleep(0.02)
                        # The new catalog default sends limit=None through IPC. A ready
                        # poll must enter canonical validation rather than stay local forever.
                        default_search = await session.call_tool(
                            "wechat_search_messages", {"query": "消息"}
                        )
                        default_token = (default_search.structuredContent or {})["reading_token"]
                        for _ in range(100):
                            default_search = await session.call_tool(
                                "wechat_search_messages",
                                {"query": "消息", "reading_token": default_token},
                            )
                            if (default_search.structuredContent or {}).get("schema") != (
                                "sightglass.search-preparation.v1"
                            ):
                                break
                            await anyio.sleep(0.02)
                        self.assertEqual((default_search.structuredContent or {}).get("schema"),
                                         "sightglass.search-results.v2", default_search)
                        self.assertEqual(
                            (default_search.structuredContent or {})["response_profile"],
                            "sightglass.mcp.brief.v1",
                        )
                        page = await session.call_tool(
                            "wechat_read_messages",
                            {
                                "mode": "recent",
                                "conversation_id": (
                                    (
                                        await session.call_tool(
                                            "wechat_find_conversations",
                                            {"query": "Synthetic Group", "limit": 1},
                                        )
                                    ).structuredContent
                                    or {}
                                )["candidates"][0]["conversation_id"],
                                "projection": "detail",
                                "limit": 50,
                            },
                        )
                        self.assertEqual((page.structuredContent or {})["response_profile"],
                                         "sightglass.mcp.brief.v1")
                        self.assertNotIn("projection_receipt", page.structuredContent or {})
                        self.assertLessEqual(len(json.dumps(page.structuredContent,
                            ensure_ascii=False, separators=(",", ":")).encode("utf-8")), 16384)
                        conversation_id = (page.structuredContent or {})["conversation"][
                            "conversation_id"]
                        for name, arguments in (
                            ("wechat_read_inbox", {}),
                            ("wechat_find_participants", {"conversation_id": conversation_id,
                                                          "query": ""}),
                        ):
                            response = await session.call_tool(name, arguments)
                            self.assertFalse(response.isError, response.structuredContent)
                            self.assertEqual((response.structuredContent or {})["response_profile"],
                                             "sightglass.mcp.brief.v1")
                        found = await session.call_tool("wechat_find_resources", {"query": "notes"})
                        self.assertFalse(found.isError, found.structuredContent)
                        text_id = (found.structuredContent or {})["items"][0]["resource"][
                            "resource_id"]
                        for name, arguments in (
                            ("wechat_read_resource", {"resource_id": text_id, "mode": "text"}),
                            ("wechat_search_resource_text", {"resource_id": text_id,
                                                             "query": "Synthetic"}),
                        ):
                            response = await session.call_tool(name, arguments)
                            self.assertFalse(response.isError, response.structuredContent)
                        message_id = next(
                            item["message_id"]
                            for item in (page.structuredContent or {})["messages"]
                            if item["kind"] == "image"
                        )
                        resources = await session.call_tool(
                            "wechat_list_resources", {"message_id": message_id}
                        )
                        resource_id = (resources.structuredContent or {})["resources"][0][
                            "resource_id"
                        ]
                        resource = await session.call_tool(
                            "wechat_read_resource",
                            {"resource_id": resource_id, "mode": "preview"},
                        )
                        async def prepared_call(name, arguments):
                            response = await session.call_tool(name, arguments)
                            for _ in range(500):
                                content = response.structuredContent or {}
                                if content.get("schema") != "sightglass.retrieval-preparation.v1":
                                    return response
                                self.assertEqual(content["state"], "preparing", content)
                                await anyio.sleep(0.01)
                                response = await session.call_tool(
                                    name, {**arguments, "reading_token": content["reading_token"]}
                                )
                            self.fail("synthetic stdio preparation did not finish")

                        links = await prepared_call("wechat_find_links", {"limit": 2})
                        self.assertFalse(links.isError)
                        self.assertEqual(
                            (links.structuredContent or {}).get("schema"),
                            "sightglass.link-search.v1",
                        )
                        retrieval = await prepared_call(
                            "wechat_retrieve", {"concept": "Synthetic", "limit": 2}
                        )
                        self.assertFalse(retrieval.isError, retrieval.structuredContent)
                        self.assertEqual(
                            (retrieval.structuredContent or {}).get("schema"),
                            "sightglass.retrieval-results.v1",
                        )
                        invalid = await session.call_tool("wechat_retrieve", {"concept": ""})
                        self.assertTrue(invalid.isError)
                        self.assertEqual(
                            (invalid.structuredContent or {}).get("code"), "QUERY_INVALID"
                        )
                        if processor_status()["sips"]:
                            self.assertFalse(resource.isError)
                            descriptor_mime = (resource.structuredContent or {})["media"][
                                "mime_type"
                            ]
                            content_mime = next(
                                item.mimeType
                                for item in resource.content
                                if isinstance(item, ImageContent)
                            )
                        else:
                            self.assertTrue(resource.isError)
                            self.assertEqual(
                                (resource.structuredContent or {}).get("code"),
                                "RESOURCE_UNAVAILABLE",
                            )
                            self.assertEqual(
                                (resource.structuredContent or {}).get("details", {}).get("reason"),
                                "sips_unavailable",
                            )
                            self.assertFalse(
                                any(isinstance(item, ImageContent) for item in resource.content)
                            )
                            descriptor_mime = content_mime = None
                        return (
                            [tool.name for tool in listed.tools],
                            bool(result.isError),
                            (result.structuredContent or {}).get("schema"),
                            (search.structuredContent or {}).get("schema"),
                            (resources.structuredContent or {}).get("schema"),
                            descriptor_mime,
                            content_mime,
                            (transcripts.structuredContent or {}).get("code"),
                            bool(transcripts.isError),
                        )

        try:
            (
                tool_names,
                is_error,
                schema,
                search_schema,
                resource_schema,
                descriptor_mime,
                content_mime,
                transcript_error_code,
                transcript_is_error,
            ) = anyio.run(exercise)
        finally:
            secrets = MemorySecretStore(
                {
                    READER_SECRET_ACCOUNT: reader_token,
                    OPERATOR_SECRET_ACCOUNT: operator_token,
                }
            )
            try:
                IPCClient(config_store=config_store, secret_store=secrets, role="operator").call(
                    "daemon.shutdown"
                )
            except Exception:
                daemon.terminate()
            daemon.wait(timeout=5)
            daemon_stderr.close()
        self.assertEqual(
            tool_names,
            [
                "wechat_status",
                "wechat_find_conversations",
                "wechat_read_inbox",
                "wechat_find_participants",
                "wechat_read_messages",
                "wechat_read_transcripts",
                "wechat_search_messages",
                "wechat_find_links",
                "wechat_retrieve",
                "wechat_find_resources",
                "wechat_list_resources",
                "wechat_read_resource",
                "wechat_search_resource_text",
            ],
        )
        self.assertFalse(is_error)
        self.assertEqual(schema, "sightglass.status.v1")
        self.assertEqual(search_schema, "sightglass.search-results.v2")
        self.assertEqual(resource_schema, "sightglass.resource-list.v1")
        self.assertEqual(descriptor_mime, "image/png" if processor_status()["sips"] else None)
        self.assertEqual(content_mime, descriptor_mime)
        self.assertEqual(transcript_error_code, "CURSOR_INVALID")
        self.assertTrue(transcript_is_error)

    def test_m3_resource_tools_honor_pause_before_reading(self):
        conversations = self.tools.wechat_find_conversations("Synthetic Group")
        conversation_id = conversations["candidates"][0]["conversation_id"]
        page = self.tools.wechat_read_messages(
            mode="recent", conversation_id=conversation_id, limit=100
        )
        message = next(item for item in page["messages"] if item["kind"] == "image")
        resource = self.tools.wechat_list_resources(message["message_id"])["resources"][0]

        _provider, _repository, _service, paused = build_test_stack(
            Path(self.temp.name) / "source",
            Path(self.temp.name) / "window.db",
            paused=True,
            reader_id="paused-reader",
        )
        listed = paused.wechat_list_resources(message["message_id"])
        read = paused.wechat_read_resource(resource_id=resource["resource_id"])
        searched = paused.wechat_search_resource_text(
            resource_id=resource["resource_id"], query="anything"
        )
        self.assertEqual(listed["code"], "SERVICE_PAUSED")
        self.assertEqual((read.structuredContent or {})["code"], "SERVICE_PAUSED")
        self.assertEqual(searched["code"], "SERVICE_PAUSED")

    def test_mcp_responses_do_not_expose_paths_or_raw_source_ids(self):
        conversations = self.tools.wechat_find_conversations("Synthetic Group")
        conversation_id = conversations["candidates"][0]["conversation_id"]
        participants = self.tools.wechat_find_participants(conversation_id, "示例甲")
        page = self.tools.wechat_read_messages(
            mode="recent", conversation_id=conversation_id, limit=100
        )
        search = self.tools.wechat_search_messages(
            query="消息", conversation_ids=[conversation_id], limit=50
        )
        image_message = next(item for item in page["messages"] if item["kind"] == "image")
        resources = self.tools.wechat_list_resources(image_message["message_id"])
        resource = resources["resources"][0]
        content = self.tools.wechat_read_resource(
            resource_id=resource["resource_id"], mode="preview"
        )
        rendered = str(
            [conversations, participants, page, search, resources, content.structuredContent]
        )
        self.assertNotIn(str(Path(self.temp.name)), rendered)
        self.assertNotIn("wxid_", rendered)
        self.assertNotIn("source-msg-", rendered)
        self.assertNotIn("00112233445566778899aabbccddeeff", rendered)


if __name__ == "__main__":
    unittest.main()

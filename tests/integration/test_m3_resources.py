from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import shutil
import sqlite3
import stat
import tempfile
import threading
import time
import unittest
import zipfile
import zlib
from contextlib import closing, contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

from mcp.types import BlobResourceContents, EmbeddedResource, ImageContent, TextResourceContents

import sightglass.resources.service as resource_service_module
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.db import WindowDB
from sightglass.policy.readers import ReaderPolicy
from sightglass.resources.jobs import ResourceJobService
from sightglass.resources.rich import XLSX_MIME, ZIP_MIME
from sightglass.runtime.control import cleanup_cache
from sightglass.runtime.lanes import LaneLimits, RuntimeLanes, WorkClass
from sightglass.runtime.resource_worker import ResourceWorker
from sightglass.source.direct_wechat import DirectWeChatSourceProvider
from sightglass.source.synthetic import _pdf_bytes, _v2_image_bytes, create_synthetic_source
from tests.fixtures.factory import build_test_stack


class _GatedResourceProvider:
    """Delegate provider calls, but park message/resource reads until released."""

    def __init__(self, provider: Any, entered: threading.Event, release: threading.Event) -> None:
        self._provider = provider
        self._entered = entered
        self._release = release

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def _gate(self) -> None:
        self._entered.set()
        self._release.wait(timeout=15)

    def get_message(self, *args: Any, **kwargs: Any) -> Any:
        self._gate()
        return self._provider.get_message(*args, **kwargs)

    def read_resource(self, *args: Any, **kwargs: Any) -> Any:
        self._gate()
        return self._provider.read_resource(*args, **kwargs)


class _FailingExitSnapshot:
    """Wrap a real snapshot context, failing the final validation on a clean exit."""

    def __init__(self, context: Any) -> None:
        self._context = context

    def __enter__(self) -> Any:
        return self._context.__enter__()

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        self._context.__exit__(exc_type, exc, traceback)
        if exc_type is None:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        return False


class _FailingExitProvider:
    """Delegate provider calls, but report a generation change at context exit.

    ``fail_on`` selects the 1-based scoped session whose exit fails; ``None`` fails
    every scoped exit. Catalog snapshots remain independently counted.
    """

    def __init__(self, provider: Any, *, fail_on: int | None = None) -> None:
        self._provider = provider
        self.descriptor = provider.descriptor
        self._fail_on = fail_on
        self.opened = 0
        self.sessions = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def snapshot(self) -> Any:
        self.opened += 1
        return self._provider.snapshot()

    def session(self, scope: Any) -> Any:
        self.sessions += 1
        context = self._provider.session(scope)
        if self._fail_on is None or self.sessions == self._fail_on:
            return _FailingExitSnapshot(context)
        return context


class _SessionTrackingProvider:
    """Record scoped acquisition and prove processors run after lease release."""

    def __init__(self, provider: Any) -> None:
        self._provider = provider
        self.descriptor = provider.descriptor
        self.active_sessions = 0
        self.sessions = 0
        self.snapshots = 0
        self.resource_reads = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def snapshot(self) -> Any:
        self.snapshots += 1
        return self._provider.snapshot()

    @contextmanager
    def session(self, scope: Any) -> Any:
        self.sessions += 1
        with self._provider.session(scope) as snapshot:
            self.active_sessions += 1
            try:
                yield snapshot
            finally:
                self.active_sessions -= 1

    def read_resource(self, *args: Any, **kwargs: Any) -> Any:
        if self.active_sessions != 1:
            raise AssertionError("source bytes must be acquired inside one scoped session")
        self.resource_reads += 1
        return self._provider.read_resource(*args, **kwargs)


class _ThumbnailOnlyProvider:
    """Expose the named image resources as source-kept derived previews.

    The source declares no original for those images and the read path returns the
    derived entry, exactly like a native image that kept only ``_h/_M/_t/_t_M.dat``.
    """

    def __init__(self, provider: Any, *, derived_key_prefix: str) -> None:
        self._provider = provider
        self._derived_key_prefix = derived_key_prefix
        self.derived_only = True

    def __getattr__(self, name: str) -> Any:
        return getattr(self._provider, name)

    def _is_derived(self, resource_key: str) -> bool:
        return resource_key.startswith(self._derived_key_prefix)

    def get_message(self, *args: Any, **kwargs: Any) -> Any:
        message = self._provider.get_message(*args, **kwargs)
        if message is None:
            return None
        return replace(
            message,
            resources=tuple(
                replace(resource, availability="preview_only")
                if resource.kind == "image" and self.derived_only
                else resource
                for resource in message.resources
            ),
        )

    def read_resource(self, source_resource_key: str, *, max_bytes: int, snapshot: Any) -> Any:
        payload = self._provider.read_resource(
            source_resource_key, max_bytes=max_bytes, snapshot=snapshot
        )
        if self.derived_only and self._is_derived(source_resource_key):
            return replace(payload, variant="thumbnail")
        return payload


def _zip_resource_bytes(entries: list[tuple[str, bytes]]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in entries:
            archive.writestr(name, data)
    return output.getvalue()


def _xlsx_resource_bytes() -> bytes:
    return _zip_resource_bytes(
        [
            (
                "[Content_Types].xml",
                b'<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"/>',
            ),
            (
                "xl/workbook.xml",
                b'<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
                b'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
                b'<sheets><sheet name="Budget" sheetId="1" r:id="rId1"/></sheets></workbook>',
            ),
            (
                "xl/_rels/workbook.xml.rels",
                b'<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                b'<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/'
                b'officeDocument/2006/relationships/worksheet" '
                b'Target="worksheets/sheet1.xml"/></Relationships>',
            ),
            (
                "xl/worksheets/sheet1.xml",
                b'<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                b'<sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>Amount</t></is></c>'
                b'<c r="B1"><f>SUM(1,2)</f><v>3</v></c></row></sheetData></worksheet>',
            ),
        ]
    )


def _solid_rgba_png(width: int, height: int, pixel: tuple[int, int, int, int]) -> bytes:
    """A deterministic solid-colour RGBA PNG for flat/transparent preview checks."""

    import struct as _struct

    def chunk(kind: bytes, data: bytes) -> bytes:
        return (
            _struct.pack(">I", len(data))
            + kind
            + data
            + _struct.pack(">I", zlib.crc32(kind + data))
        )

    rows = b"".join(b"\x00" + bytes(pixel) * width for _ in range(height))
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", _struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(rows))
        + chunk(b"IEND", b"")
    )


class M3ResourceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = create_synthetic_source(Path(self.temp.name) / "source")
        self.window = Path(self.temp.name) / "state" / "window.db"
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            self.root, self.window
        )
        conversations = self.tools.wechat_find_conversations("Synthetic Group")
        self.group_id = conversations["candidates"][0]["conversation_id"]
        page = self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=100
        )
        self.image_message = next(item for item in page["messages"] if item["kind"] == "image")
        self.pdf_message = next(
            item
            for item in page["messages"]
            if any(
                resource.get("original_name") == "synthetic.pdf" for resource in item["resources"]
            )
        )
        self.text_message = next(
            item
            for item in page["messages"]
            if any(resource.get("original_name") == "notes.md" for resource in item["resources"])
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def _descriptor(result):
        return result.structuredContent or {}

    def _resource(self, message: dict[str, object]) -> dict[str, Any]:
        listed = self.tools.wechat_list_resources(str(message["message_id"]))
        self.assertEqual(listed["schema"], "sightglass.resource-list.v1")
        self.assertEqual(len(listed["resources"]), 1)
        return listed["resources"][0]

    def _replace_declared_payload(
        self,
        *,
        source_message_id: str,
        source_resource_key: str,
        path: Path,
        data: bytes,
        declared_data: bytes | None = None,
    ) -> None:
        path.write_bytes(data)
        declared = data if declared_data is None else declared_data
        shard = self.root / (
            "messages-1.db" if source_message_id == "source-msg-004" else "messages-2.db"
        )
        with closing(sqlite3.connect(shard)) as connection:
            row = connection.execute(
                "SELECT resources_json FROM messages WHERE source_message_id = ?",
                (source_message_id,),
            ).fetchone()
            self.assertIsNotNone(row)
            resources = json.loads(row[0])
            resource = next(
                item for item in resources if item.get("source_resource_key") == source_resource_key
            )
            resource["declared_size"] = len(declared)
            resource["declared_hash"] = hashlib.sha256(declared).hexdigest()
            connection.execute(
                "UPDATE messages SET resources_json = ? WHERE source_message_id = ?",
                (json.dumps(resources), source_message_id),
            )
            connection.commit()

    def _fresh_resource(
        self,
        *,
        window_name: str,
        kind: str | None = None,
        original_name: str | None = None,
    ):
        _provider, _repository, _service, tools = build_test_stack(
            self.root, Path(self.temp.name) / window_name / "window.db"
        )
        group_id = tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        page = tools.wechat_read_messages(mode="recent", conversation_id=group_id, limit=100)
        message = next(
            item
            for item in page["messages"]
            if (kind is None or item["kind"] == kind)
            and (
                original_name is None
                or any(
                    resource.get("original_name") == original_name for resource in item["resources"]
                )
            )
        )
        listed = tools.wechat_list_resources(message["message_id"])
        return tools, listed["resources"][0]

    def _replace_text_resource_contract(
        self,
        *,
        data: bytes,
        mime_type: str,
        original_name: str,
    ) -> None:
        self._replace_declared_payload(
            source_message_id="source-msg-009",
            source_resource_key="text-009",
            path=self.root / "resources" / "text-009.md",
            data=data,
        )
        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            row = connection.execute(
                "SELECT resources_json FROM messages WHERE source_message_id = ?",
                ("source-msg-009",),
            ).fetchone()
            self.assertIsNotNone(row)
            resources = json.loads(row[0])
            resource = next(
                item for item in resources if item.get("source_resource_key") == "text-009"
            )
            resource["mime_type"] = mime_type
            resource["original_name"] = original_name
            connection.execute(
                "UPDATE messages SET resources_json = ? WHERE source_message_id = ?",
                (json.dumps(resources), "source-msg-009"),
            )
            connection.commit()

    def _fresh_rich_resource(self, *, window_name: str, original_name: str):
        _provider, _repository, service, tools = build_test_stack(
            self.root,
            Path(self.temp.name) / window_name / "window.db",
        )
        group_id = tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        page = tools.wechat_read_messages(mode="recent", conversation_id=group_id, limit=100)
        message = next(
            item
            for item in page["messages"]
            if any(resource.get("original_name") == original_name for resource in item["resources"])
        )
        resource = tools.wechat_list_resources(message["message_id"])["resources"][0]
        return service.resource_service, resource, tools

    def test_list_resources_is_message_bound_and_policy_checked(self) -> None:
        status = self.tools.wechat_status("capabilities")
        self.assertEqual(
            status["resource_processors"],
            {
                "sips": True,
                "pdfinfo": True,
                "pdftotext": True,
                "pdftoppm": True,
                "ffmpeg": True,
            },
        )
        resource = self._resource(self.image_message)
        self.assertEqual(resource["source_message_id"], self.image_message["message_id"])
        self.assertEqual(resource["availability"], "local_available")
        self.assertTrue(resource["preview_available"])
        self.assertTrue(resource["original_available"])
        self.assertNotIn(str(self.root), str(resource))
        self.assertNotIn("image-004", str(resource))

        denied = ReaderPolicy(
            mode="all_except_denylist",
            denied_conversation_ids=frozenset({self.group_id}),
            identity_debug=True,
        )
        _provider, _repository, _service, denied_tools = build_test_stack(
            self.root,
            Path(self.temp.name) / "denied" / "window.db",
            policy=denied,
        )
        denied_tools.wechat_find_conversations("")
        denied_result = denied_tools.wechat_list_resources(str(self.image_message["message_id"]))
        self.assertEqual(denied_result["code"], "MESSAGE_NOT_FOUND")

        _provider, _repository, _service, persisted_denied = build_test_stack(
            self.root,
            self.window,
            policy=denied,
            reader_id="denylisted-reader",
        )
        persisted_denied.wechat_status()
        denied_result = persisted_denied.wechat_list_resources(
            str(self.image_message["message_id"])
        )
        self.assertEqual(denied_result["code"], "POLICY_DENIED")

        blocked = ReaderPolicy(
            mode="all_except_denylist",
            resource_metadata=False,
            identity_debug=True,
        )
        _provider, _repository, _service, blocked_tools = build_test_stack(
            self.root,
            self.window,
            policy=blocked,
            reader_id="no-resource-metadata",
        )
        blocked_result = blocked_tools.wechat_list_resources(str(self.image_message["message_id"]))
        self.assertEqual(blocked_result["code"], "POLICY_DENIED")

    def test_image_preview_and_original_align_descriptor_and_content(self) -> None:
        self.assertTrue(
            (self.root / "resources" / "image-004.bin")
            .read_bytes()
            .startswith(b"\x07\x08V2\x08\x07")
        )
        resource = self._resource(self.image_message)
        preview = self.tools.wechat_read_resource(
            resource_id=resource["resource_id"], mode="preview"
        )
        descriptor = self._descriptor(preview)
        self.assertFalse(preview.isError)
        self.assertEqual(descriptor["schema"], "sightglass.resource-read.v1")
        self.assertEqual(descriptor["mode"], "preview")
        self.assertEqual(descriptor["media"]["mime_type"], "image/png")
        self.assertEqual(descriptor["media"]["content_block_type"], "image")
        self.assertIn("declared_mime_mismatch", descriptor["warnings"])
        image = next(item for item in preview.content if isinstance(item, ImageContent))
        payload = base64.b64decode(image.data)
        self.assertTrue(payload.startswith(b"\x89PNG\r\n\x1a\n"))
        self.assertEqual(image.mimeType, descriptor["media"]["mime_type"])
        self.assertEqual(len(payload), descriptor["returned"]["bytes"])
        self.assertNotIn(str(self.root), str(descriptor))
        self.assertIsNotNone(
            self.repository.resource_binding(str(resource["resource_id"]), "preview:v2")
        )
        self.assertIsNone(self.repository.resource_binding(str(resource["resource_id"]), "preview"))

        original = self.tools.wechat_read_resource(
            resource_id=resource["resource_id"], mode="original"
        )
        original_descriptor = self._descriptor(original)
        original_image = next(item for item in original.content if isinstance(item, ImageContent))
        self.assertEqual(original_descriptor["derivation"]["kind"], "source_original")
        self.assertEqual(original_image.mimeType, "image/png")

        no_original = ReaderPolicy(
            mode="all_except_denylist",
            resource_original=False,
            identity_debug=True,
        )
        _provider, _repository, _service, restricted = build_test_stack(
            self.root,
            self.window,
            policy=no_original,
            reader_id="preview-only-reader",
        )
        restricted.wechat_status()
        denied = restricted.wechat_read_resource(
            resource_id=resource["resource_id"], mode="original"
        )
        self.assertEqual(self._descriptor(denied)["code"], "POLICY_DENIED")

    def test_oversized_image_header_is_rejected_before_processor(self) -> None:
        decoded = bytearray(b"\x89PNG\r\n\x1a\n" + b"\x00" * 80)
        decoded[16:20] = (20_000).to_bytes(4, "big")
        decoded[20:24] = (20_000).to_bytes(4, "big")
        self._replace_declared_payload(
            source_message_id="source-msg-004",
            source_resource_key="image-004",
            path=self.root / "resources" / "image-004.bin",
            data=_v2_image_bytes(bytes(decoded)),
            declared_data=bytes(decoded),
        )
        tools, resource = self._fresh_resource(window_name="oversized", kind="image")
        with patch(
            "sightglass.resources.processors._command",
            side_effect=AssertionError("processor must not run for oversized PNG header"),
        ):
            result = tools.wechat_read_resource(resource_id=resource["resource_id"], mode="preview")
        self.assertTrue(result.isError)
        self.assertEqual(self._descriptor(result)["code"], "RESOURCE_TOO_LARGE")

    def test_native_decoder_recipe_requires_source_provenance_before_cache_hit(self) -> None:
        from dataclasses import replace
        from unittest.mock import PropertyMock

        provider, tools = self.provider, self.tools
        descriptor = replace(
            provider.descriptor, kind="macos-wechat", implementation="synthetic.native-decoder.v1"
        )
        with patch.object(
            type(provider), "descriptor", new_callable=PropertyMock, return_value=descriptor
        ) as metadata:
            conversations = tools.wechat_find_conversations("Synthetic Group")
            conversation = conversations["candidates"][0]["conversation_id"]
            page = tools.wechat_read_messages(mode="recent", conversation_id=conversation, limit=50)
            image = next(item for item in page["messages"] if item["kind"] == "image")
            resource = tools.wechat_list_resources(image["message_id"])["resources"][0]
            service = tools.service.resource_service
            first = tools.wechat_read_resource(resource["resource_id"], mode="preview")
            self.assertFalse(first.isError)
            self.assertTrue(service.local_read_ready(resource["resource_id"], "preview"))
            metadata.return_value = replace(
                descriptor, implementation="synthetic.native-decoder.v2"
            )
            self.assertFalse(service.local_read_ready(resource["resource_id"], "preview"))
            repaired = tools.wechat_read_resource(resource["resource_id"], mode="preview")
            self.assertFalse(repaired.isError)
            self.assertTrue(service.local_read_ready(resource["resource_id"], "preview"))

    def test_v2_image_without_key_is_blocked(self) -> None:
        manifest_path = self.root / "source.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        image = next(
            item for item in manifest["resources"] if item["source_resource_key"] == "image-004"
        )
        image.pop("image_aes_key")
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        tools, resource = self._fresh_resource(window_name="missing-v2-key", kind="image")
        result = tools.wechat_read_resource(resource_id=resource["resource_id"], mode="preview")
        self.assertTrue(result.isError)
        self.assertEqual(self._descriptor(result)["code"], "RESOURCE_BLOCKED")
        self.assertEqual(self._descriptor(result)["details"]["reason"], "key_missing")

    def test_image_limits_corruption_and_symlink_escape_fail_closed(self) -> None:
        resource = self._resource(self.image_message)
        too_small = self.tools.wechat_read_resource(
            resource_id=resource["resource_id"], mode="original", max_bytes=16
        )
        self.assertTrue(too_small.isError)
        self.assertEqual(self._descriptor(too_small)["code"], "RESOURCE_TOO_LARGE")

        image_path = self.root / "resources" / "image-004.bin"
        image_path.write_bytes(b"\x89PNG\r\n\x1a\ncorrupt")
        corrupted = self.tools.wechat_read_resource(
            resource_id=resource["resource_id"], mode="preview"
        )
        self.assertTrue(corrupted.isError)
        self.assertEqual(self._descriptor(corrupted)["code"], "RESOURCE_DECODE_FAILED")

        # A fresh stack has no cached object that could mask the source symlink.
        self.temp.cleanup()
        self.temp = tempfile.TemporaryDirectory()
        self.root = create_synthetic_source(Path(self.temp.name) / "source")
        image_path = self.root / "resources" / "image-004.bin"
        outside = Path(self.temp.name) / "outside.png"
        outside.write_bytes(image_path.read_bytes())
        image_path.unlink()
        image_path.symlink_to(outside)
        _provider, _repository, _service, tools = build_test_stack(
            self.root, Path(self.temp.name) / "state" / "window.db"
        )
        conversation = tools.wechat_find_conversations("Synthetic Group")["candidates"][0]
        page = tools.wechat_read_messages(
            mode="recent", conversation_id=conversation["conversation_id"], limit=100
        )
        message = next(item for item in page["messages"] if item["kind"] == "image")
        listed = tools.wechat_list_resources(message["message_id"])
        blocked = tools.wechat_read_resource(
            resource_id=listed["resources"][0]["resource_id"], mode="preview"
        )
        self.assertTrue(blocked.isError)
        self.assertEqual(self._descriptor(blocked)["code"], "RESOURCE_BLOCKED")

    def test_parent_symlink_and_hardlink_escape_fail_closed(self) -> None:
        resources = self.root / "resources"
        moved = self.root / "moved-resources"
        resources.rename(moved)
        resources.symlink_to(moved, target_is_directory=True)
        tools, resource = self._fresh_resource(window_name="parent-symlink", kind="image")
        blocked = tools.wechat_read_resource(resource_id=resource["resource_id"], mode="preview")
        self.assertEqual(self._descriptor(blocked)["code"], "RESOURCE_BLOCKED")

        resources.unlink()
        moved.rename(resources)
        image_path = resources / "image-004.bin"
        outside = Path(self.temp.name) / "hardlinked-image.bin"
        outside.write_bytes(image_path.read_bytes())
        image_path.unlink()
        os.link(outside, image_path)
        tools, resource = self._fresh_resource(window_name="hardlink", kind="image")
        blocked = tools.wechat_read_resource(resource_id=resource["resource_id"], mode="preview")
        self.assertEqual(self._descriptor(blocked)["code"], "RESOURCE_BLOCKED")

    def test_pdf_metadata_page_text_search_and_original(self) -> None:
        resource = self._resource(self.pdf_message)
        metadata = self.tools.wechat_read_resource(
            resource_id=resource["resource_id"], mode="metadata"
        )
        meta = self._descriptor(metadata)
        self.assertEqual(meta["document"]["page_count"], 3)
        self.assertEqual(len(metadata.content), 1)

        text = self.tools.wechat_read_resource(
            resource_id=resource["resource_id"], mode="text", page=3
        )
        text_descriptor = self._descriptor(text)
        embedded_text = next(
            item.resource
            for item in text.content
            if isinstance(item, EmbeddedResource)
            and isinstance(item.resource, TextResourceContents)
        )
        self.assertIn("atomic publish", embedded_text.text.casefold())
        self.assertEqual(text_descriptor["page"], 3)
        self.assertEqual(text_descriptor["returned"]["chars"], len(embedded_text.text))

        rendered = self.tools.wechat_read_resource(
            resource_id=resource["resource_id"], mode="page", page=3
        )
        rendered_descriptor = self._descriptor(rendered)
        page_image = next(item for item in rendered.content if isinstance(item, ImageContent))
        self.assertEqual(page_image.mimeType, "image/png")
        self.assertEqual(rendered_descriptor["derivation"]["kind"], "rendered_pdf_page")

        original = self.tools.wechat_read_resource(
            resource_id=resource["resource_id"], mode="original"
        )
        pdf_blob = next(
            item.resource
            for item in original.content
            if isinstance(item, EmbeddedResource)
            and isinstance(item.resource, BlobResourceContents)
        )
        self.assertEqual(pdf_blob.mimeType, "application/pdf")
        self.assertTrue(base64.b64decode(pdf_blob.blob).startswith(b"%PDF-"))

        search = self.tools.wechat_search_resource_text(
            resource_id=resource["resource_id"], query="atomic publish", limit=20
        )
        self.assertEqual(search["schema"], "sightglass.resource-search-results.v1")
        self.assertEqual(search["hits"][0]["page"], 3)
        self.assertIn("atomic publish", search["hits"][0]["snippet"].casefold())
        self.assertNotIn("Page one", str(search))

    def test_encrypted_and_malformed_pdf_fail_closed(self) -> None:
        resource = self._resource(self.pdf_message)
        pdf_path = self.root / "resources" / "file-006.pdf"
        payload = pdf_path.read_bytes()
        marker = payload.rfind(b"trailer")
        self.assertGreater(marker, 0)
        payload = (
            payload[: marker + len(b"trailer")]
            + b"\n<< /Encrypt 99 0 R >>\n"
            + payload[marker + len(b"trailer") :]
        )
        self._replace_declared_payload(
            source_message_id="source-msg-006",
            source_resource_key="file-006",
            path=pdf_path,
            data=payload,
        )
        encrypted = self.tools.wechat_read_resource(
            resource_id=resource["resource_id"], mode="text", page=1
        )
        self.assertTrue(encrypted.isError)
        self.assertEqual(self._descriptor(encrypted)["code"], "RESOURCE_BLOCKED")

        malformed_payload = b"%PDF-1.7\nnot-a-document"
        self._replace_declared_payload(
            source_message_id="source-msg-006",
            source_resource_key="file-006",
            path=pdf_path,
            data=malformed_payload,
        )
        malformed_tools, malformed_resource = self._fresh_resource(
            window_name="malformed", original_name="synthetic.pdf"
        )
        malformed = malformed_tools.wechat_read_resource(
            resource_id=malformed_resource["resource_id"], mode="page", page=1
        )
        self.assertTrue(malformed.isError)
        self.assertEqual(self._descriptor(malformed)["code"], "RESOURCE_DECODE_FAILED")

    def test_pdf_page_limit_fails_closed(self) -> None:
        payload = _pdf_bytes(tuple(f"Page {number}" for number in range(1, 502)))
        self._replace_declared_payload(
            source_message_id="source-msg-006",
            source_resource_key="file-006",
            path=self.root / "resources" / "file-006.pdf",
            data=payload,
        )
        tools, resource = self._fresh_resource(
            window_name="oversized-pdf", original_name="synthetic.pdf"
        )
        result = tools.wechat_read_resource(resource_id=resource["resource_id"], mode="metadata")
        self.assertTrue(result.isError)
        self.assertEqual(self._descriptor(result)["code"], "RESOURCE_TOO_LARGE")

    def test_text_line_range_search_invalid_encoding_and_binary_masquerade(self) -> None:
        resource = self._resource(self.text_message)
        read = self.tools.wechat_read_resource(
            resource_id=resource["resource_id"], mode="text", start_line=2, end_line=3
        )
        descriptor = self._descriptor(read)
        embedded = next(
            item.resource
            for item in read.content
            if isinstance(item, EmbeddedResource)
            and isinstance(item.resource, TextResourceContents)
        )
        self.assertEqual(embedded.text, "second line\natomic publish keeps cache coherent")
        self.assertEqual(descriptor["line_range"], {"start": 2, "end": 3})
        too_small = self.tools.wechat_read_resource(
            resource_id=resource["resource_id"],
            mode="text",
            start_line=2,
            end_line=3,
            max_bytes=8,
        )
        self.assertEqual(self._descriptor(too_small)["code"], "RESOURCE_TOO_LARGE")
        invalid_parameters = self.tools.wechat_read_resource(
            resource_id=resource["resource_id"], mode="text", page=1, start_line=1
        )
        self.assertEqual(self._descriptor(invalid_parameters)["code"], "QUERY_INVALID")

        search = self.tools.wechat_search_resource_text(
            resource_id=resource["resource_id"], query="atomic publish", limit=20
        )
        self.assertEqual(search["hits"][0]["line"], 3)
        self.assertNotIn("first line", str(search))

        text_path = self.root / "resources" / "text-009.md"
        binary_payload = b"plain-prefix\x00binary"
        self._replace_declared_payload(
            source_message_id="source-msg-009",
            source_resource_key="text-009",
            path=text_path,
            data=binary_payload,
        )
        binary_tools, binary_resource = self._fresh_resource(
            window_name="binary", original_name="notes.md"
        )
        binary = binary_tools.wechat_read_resource(
            resource_id=binary_resource["resource_id"], mode="text"
        )
        self.assertTrue(binary.isError)
        self.assertEqual(self._descriptor(binary)["code"], "RESOURCE_DECODE_FAILED")

        invalid_payload = b"\xff\xfe\xfa"
        self._replace_declared_payload(
            source_message_id="source-msg-009",
            source_resource_key="text-009",
            path=text_path,
            data=invalid_payload,
        )
        invalid_tools, invalid_resource = self._fresh_resource(
            window_name="invalid", original_name="notes.md"
        )
        invalid = invalid_tools.wechat_read_resource(
            resource_id=invalid_resource["resource_id"], mode="text"
        )
        self.assertTrue(invalid.isError)
        self.assertEqual(self._descriptor(invalid)["code"], "RESOURCE_DECODE_FAILED")

    def test_extended_text_encodings_and_opaque_binary_metadata(self) -> None:
        for name, data, expected_encoding in (
            ("utf16", "Synthetic UTF-16 文本\n第二行".encode("utf-16"), "utf-16"),
            (
                "gb18030",
                "Synthetic GB18030 文本\n第二行".encode("gb18030"),
                "gb18030",
            ),
        ):
            with self.subTest(encoding=name):
                self._replace_text_resource_contract(
                    data=data,
                    mime_type="text/plain",
                    original_name=f"synthetic-{name}.txt",
                )
                tools, resource = self._fresh_resource(
                    window_name=f"encoded-{name}",
                    original_name=f"synthetic-{name}.txt",
                )
                result = tools.wechat_read_resource(
                    resource_id=resource["resource_id"], mode="text"
                )
                descriptor = self._descriptor(result)
                self.assertFalse(result.isError)
                self.assertEqual(descriptor["text_encoding"], expected_encoding)
                self.assertEqual(descriptor["resource"]["format_family"], "text")
                self.assertTrue(descriptor["resource"]["available_views"]["text"])

        opaque = b"\x00\x01\x02synthetic-binary\x00\xff"
        self._replace_text_resource_contract(
            data=opaque,
            mime_type="application/octet-stream",
            original_name="synthetic.bin",
        )
        tools, resource = self._fresh_resource(
            window_name="opaque-binary",
            original_name="synthetic.bin",
        )
        result = tools.wechat_read_resource(resource_id=resource["resource_id"], mode="metadata")
        descriptor = self._descriptor(result)
        self.assertFalse(result.isError)
        self.assertEqual(descriptor["resource"]["format_family"], "binary")
        self.assertEqual(descriptor["binary"]["byte_size"], len(opaque))
        self.assertEqual(descriptor["binary"]["mime_type"], "application/octet-stream")

    def test_resource_failure_ledger_is_bounded_and_content_free(self) -> None:
        resource = self._resource(self.image_message)
        result = self.tools.wechat_read_resource(resource_id=resource["resource_id"], mode="text")
        self.assertEqual(self._descriptor(result)["code"], "RESOURCE_UNSUPPORTED")

        status = self.service.resource_service.runtime_status()
        failure = status["recent_failures"][-1]
        self.assertEqual(failure["tool"], "wechat_read_resource")
        self.assertEqual(failure["code"], "RESOURCE_UNSUPPORTED")
        self.assertEqual(failure["format_family"], "image")
        self.assertFalse(failure["retryable"])
        self.assertNotIn(str(resource["resource_id"]), str(status))
        self.assertNotIn(str(self.root), str(status))

    def test_rich_resources_route_through_resource_service_with_selectors(self) -> None:
        self._replace_text_resource_contract(
            data=_xlsx_resource_bytes(),
            mime_type=XLSX_MIME,
            original_name="synthetic.xlsx",
        )
        core, resource, tools = self._fresh_rich_resource(
            window_name="rich-xlsx",
            original_name="synthetic.xlsx",
        )
        common = {
            "resource_id": resource["resource_id"],
            "start_line": None,
            "end_line": None,
            "max_bytes": 64 * 1024,
        }
        metadata = core.read_resource(
            **common,
            mode="metadata",
            page=None,
        ).descriptor
        self.assertEqual(metadata["structured"]["workbook"]["sheets"], ["Budget"])
        table = core.read_resource(
            **common,
            mode="table",
            page=None,
            sheet="Budget",
            cell_range="B1:B1",
        ).descriptor
        self.assertEqual(table["table"]["cell_range"], "B1:B1")
        formula = table["table"]["rows"][0]["cells"][0]
        self.assertEqual(formula["formula"], "SUM(1,2)")
        self.assertEqual(formula["cached_value"], "3")
        self.assertEqual(formula["value_source"], "cached_formula_result")
        through_tool = tools.wechat_read_resource(
            resource_id=resource["resource_id"],
            mode="table",
            sheet="Budget",
            cell_range="B1:B1",
        )
        self.assertFalse(through_tool.isError)
        self.assertEqual(
            (through_tool.structuredContent or {})["table"]["cell_range"],
            "B1:B1",
        )
        text = core.read_resource(
            **common,
            mode="text",
            page=None,
        )
        self.assertIn("Sheet: Budget", text.text or "")

        archive_bytes = _zip_resource_bytes(
            [("notes/selected.txt", b"safe selected member\n"), ("opaque.bin", b"\x00\x01")]
        )
        self._replace_text_resource_contract(
            data=archive_bytes,
            mime_type=ZIP_MIME,
            original_name="synthetic.zip",
        )
        archive_core, archive_resource, archive_tools = self._fresh_rich_resource(
            window_name="rich-zip",
            original_name="synthetic.zip",
        )
        archive_common = {
            "resource_id": archive_resource["resource_id"],
            "start_line": None,
            "end_line": None,
            "max_bytes": 64 * 1024,
        }
        members = archive_core.read_resource(
            **archive_common,
            mode="members",
            page=None,
        ).descriptor
        self.assertEqual(
            [item["name"] for item in members["archive"]["members"]],
            ["notes/selected.txt", "opaque.bin"],
        )
        selected = archive_core.read_resource(
            **archive_common,
            mode="text",
            page=None,
            member="notes/selected.txt",
        )
        self.assertEqual(selected.text, "safe selected member\n")
        self.assertNotIn(str(self.root), str(selected.descriptor))
        selected_through_tool = archive_tools.wechat_read_resource(
            resource_id=archive_resource["resource_id"],
            mode="text",
            member="notes/selected.txt",
        )
        self.assertFalse(selected_through_tool.isError)
        selected_content = next(
            item.resource
            for item in selected_through_tool.content
            if isinstance(item, EmbeddedResource)
            and isinstance(item.resource, TextResourceContents)
        )
        self.assertEqual(selected_content.text, "safe selected member\n")

        self._replace_text_resource_contract(
            data=b"name,count\nalpha,2\n",
            mime_type="application/octet-stream",
            original_name="synthetic.csv",
        )
        csv_core, csv_resource, _csv_tools = self._fresh_rich_resource(
            window_name="rich-csv",
            original_name="synthetic.csv",
        )
        csv_table = csv_core.read_resource(
            resource_id=csv_resource["resource_id"],
            mode="table",
            page=None,
            start_line=None,
            end_line=None,
            max_bytes=64 * 1024,
        ).descriptor
        self.assertEqual(csv_table["sniffed_mime_type"], "text/csv")
        self.assertEqual(csv_table["table"]["rows"][1]["cells"], ["alpha", "2"])

    def test_generic_local_file_original_matches_available_descriptor(self) -> None:
        # A provider-verified local file with an unknown-but-safe octet-stream body must
        # stay reachable as its source original whenever the descriptor advertises
        # original_available. MIME sniffing still governs reporting, and the caller
        # selects the transport kind rather than the media type alone.
        generic = b"\x01\x02\x03\xff\xfe\x00generic-blob"
        self._replace_text_resource_contract(
            data=generic,
            mime_type="application/octet-stream",
            original_name="synthetic.bin",
        )
        core, resource, tools = self._fresh_rich_resource(
            window_name="rich-generic",
            original_name="synthetic.bin",
        )
        self.assertTrue(resource["original_available"])
        self.assertEqual(resource["availability"], "local_available")

        read = core.read_resource(
            resource_id=resource["resource_id"],
            mode="original",
            page=None,
            start_line=None,
            end_line=None,
            max_bytes=64 * 1024,
        )
        descriptor = read.descriptor
        self.assertEqual(descriptor["derivation"]["kind"], "source_original")
        self.assertEqual(descriptor["sniffed_mime_type"], "application/octet-stream")
        self.assertEqual(descriptor["media"]["mime_type"], "application/octet-stream")
        self.assertEqual(descriptor["media"]["content_block_type"], "blob")
        self.assertEqual(read.content_kind, "blob")
        self.assertEqual(read.data, generic)
        self.assertEqual(descriptor["returned"]["bytes"], len(generic))

        # The bytes still cross the MCP boundary without exposing the source path.
        through_tool = tools.wechat_read_resource(
            resource_id=resource["resource_id"],
            mode="original",
        )
        self.assertFalse(through_tool.isError)
        blob = next(
            item.resource
            for item in through_tool.content
            if isinstance(item, EmbeddedResource)
            and isinstance(item.resource, BlobResourceContents)
        )
        self.assertEqual(blob.mimeType, "application/octet-stream")
        self.assertEqual(base64.b64decode(blob.blob), generic)
        self.assertNotIn(str(self.root), str(through_tool.structuredContent))

    def _thumbnail_only_stack(self, window_name: str):
        _provider, _repository, service, tools = build_test_stack(
            self.root,
            Path(self.temp.name) / window_name / "window.db",
        )
        derived = _ThumbnailOnlyProvider(service.provider, derived_key_prefix="image-")
        service.provider = derived  # type: ignore[reportAttributeAccessIssue]
        service.resource_service.provider = derived  # type: ignore[reportAttributeAccessIssue]
        return service, tools

    def _image_message(self, tools: Any) -> dict[str, Any]:
        group_id = tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        page = tools.wechat_read_messages(mode="recent", conversation_id=group_id, limit=100)
        return next(item for item in page["messages"] if item["kind"] == "image")

    def test_thumbnail_only_image_is_preview_only_and_never_a_source_original(self) -> None:
        # Declared size/digest describe the source original, not the derived preview
        # the provider serves, so keep them distinct from the preview bytes.
        image_path = self.root / "resources" / "image-004.bin"
        self._replace_declared_payload(
            source_message_id="source-msg-004",
            source_resource_key="image-004",
            path=image_path,
            data=image_path.read_bytes(),
            declared_data=b"synthetic-source-original-stand-in",
        )
        service, tools = self._thumbnail_only_stack("thumbnail-only")
        message = self._image_message(tools)
        resource = tools.wechat_list_resources(str(message["message_id"]))["resources"][0]

        self.assertEqual(resource["availability"], "preview_only")
        self.assertFalse(resource["original_available"])
        self.assertTrue(resource["preview_available"])

        preview = tools.wechat_read_resource(
            resource_id=str(resource["resource_id"]), mode="preview"
        )
        self.assertFalse(preview.isError)
        descriptor = self._descriptor(preview)
        self.assertEqual(descriptor["derivation"]["kind"], "generated_image_preview")
        self.assertEqual(descriptor["resolution"], {"path": "private_cache", "variant": "preview"})
        self.assertEqual(descriptor["resource"]["availability"], "preview_only")
        self.assertFalse(descriptor["resource"]["original_available"])
        self.assertTrue(descriptor["resource"]["preview_available"])
        image = next(item for item in preview.content if isinstance(item, ImageContent))
        self.assertTrue(base64.b64decode(image.data).startswith(b"\x89PNG\r\n\x1a\n"))

        original = tools.wechat_read_resource(
            resource_id=str(resource["resource_id"]), mode="original"
        )
        self.assertTrue(original.isError)
        self.assertEqual(self._descriptor(original)["code"], "RESOURCE_UNAVAILABLE")

        # The stored derived binding keeps the descriptor honest on later reads.
        again = tools.wechat_list_resources(str(message["message_id"]))["resources"][0]
        self.assertEqual(again["availability"], "preview_only")
        self.assertFalse(again["original_available"])
        self.assertTrue(again["preview_available"])
        repository = service.resource_service.repository
        self.assertIsNotNone(repository.resource_binding(str(resource["resource_id"]), "thumbnail"))
        self.assertIsNone(repository.resource_binding(str(resource["resource_id"]), "original"))
        self.assertNotIn(str(self.root), str(again))

    def test_downloaded_image_original_stays_a_source_original(self) -> None:
        resource = self._resource(self.image_message)

        self.assertEqual(resource["availability"], "local_available")
        self.assertTrue(resource["original_available"])
        original = self.tools.wechat_read_resource(
            resource_id=str(resource["resource_id"]), mode="original"
        )
        descriptor = self._descriptor(original)
        self.assertEqual(descriptor["derivation"]["kind"], "source_original")
        self.assertEqual(descriptor["resource"]["availability"], "local_available")
        self.assertTrue(descriptor["resource"]["original_available"])

    def test_later_source_original_replaces_a_cached_derived_preview(self) -> None:
        service, tools = self._thumbnail_only_stack("thumbnail-upgrade")
        message = self._image_message(tools)
        resource = tools.wechat_list_resources(str(message["message_id"]))["resources"][0]
        preview = tools.wechat_read_resource(
            resource_id=str(resource["resource_id"]), mode="preview"
        )
        self.assertFalse(preview.isError)

        # The source downloads the original after the derived preview was cached.
        service.resource_service.provider.derived_only = False  # type: ignore[reportAttributeAccessIssue]
        listed = tools.wechat_list_resources(str(message["message_id"]))["resources"][0]
        self.assertEqual(listed["availability"], "local_available")
        self.assertTrue(listed["original_available"])

        original = tools.wechat_read_resource(
            resource_id=str(resource["resource_id"]), mode="original"
        )
        self.assertFalse(original.isError)
        descriptor = self._descriptor(original)
        self.assertEqual(descriptor["derivation"]["kind"], "source_original")
        self.assertEqual(descriptor["resolution"]["variant"], "original")

    def _derivation_rows(
        self, resource_id: str, *, window: Path | None = None
    ) -> list[sqlite3.Row]:
        with closing(sqlite3.connect(window or self.window)) as connection:
            connection.row_factory = sqlite3.Row
            return connection.execute(
                """
                SELECT * FROM resource_derivations
                WHERE resource_id = ? ORDER BY created_at, derivation_id
                """,
                (resource_id,),
            ).fetchall()

    def test_preview_regenerates_when_cached_provenance_no_longer_matches(self) -> None:
        # A cached preview is only served when a recorded derivation proves it derives
        # from this exact input digest, source variant, recipe, and resolver revision.
        # If the provenance proof is absent or points at different bytes, the binding
        # must be regenerated rather than trusted.
        resource = self._resource(self.image_message)
        resource_id = str(resource["resource_id"])
        first = self.tools.wechat_read_resource(resource_id=resource_id, mode="preview")
        self.assertFalse(first.isError)
        binding = self.repository.resource_binding(resource_id, "preview:v2")
        self.assertIsNotNone(binding)
        assert binding is not None
        original_digest = str(binding["object_digest"])
        self.assertTrue(self._derivation_rows(resource_id))

        # Drop the provenance proof: the binding alone can no longer claim derivation.
        with self.repository.database.transaction() as connection:
            connection.execute(
                "DELETE FROM resource_derivations WHERE resource_id = ?", (resource_id,)
            )

        again = self.tools.wechat_read_resource(resource_id=resource_id, mode="preview")
        self.assertFalse(again.isError)
        self.assertTrue(self._derivation_rows(resource_id))
        self.assertEqual(
            str(
                (self.repository.resource_binding(resource_id, "preview:v2") or {})["object_digest"]
            ),
            original_digest,
        )

    def test_recipe_version_change_invalidates_cached_preview(self) -> None:
        resource = self._resource(self.image_message)
        resource_id = str(resource["resource_id"])
        self.tools.wechat_read_resource(resource_id=resource_id, mode="preview")
        original_binding = self.repository.resource_binding(resource_id, "original")
        self.assertIsNotNone(original_binding)
        assert original_binding is not None
        source_digest = str(original_binding["object_digest"])
        preview_binding = self.repository.resource_binding(resource_id, "preview:v2")
        assert preview_binding is not None
        original_digest = str(preview_binding["object_digest"])

        # A cached preview produced by an older recipe version is not proof for the
        # current recipe, so the read must regenerate and record the new provenance.
        # A real stale row carries a different derivation id (it was written by the
        # older recipe), so replace the current row with one to model that state.
        with self.repository.database.transaction() as connection:
            connection.execute(
                "DELETE FROM resource_derivations WHERE resource_id = ?", (resource_id,)
            )
            connection.execute(
                """
                INSERT INTO resource_derivations(
                    derivation_id, resource_id, source_digest, variant,
                    processor_name, processor_version, parameters_json,
                    derived_digest, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "wxresder_stale",
                    resource_id,
                    source_digest,
                    "preview:v2",
                    "image-preview",
                    "stale-recipe",
                    "{}",
                    original_digest,
                    "now",
                ),
            )

        regenerated = self.tools.wechat_read_resource(resource_id=resource_id, mode="preview")
        self.assertFalse(regenerated.isError)
        rows = self._derivation_rows(resource_id)
        self.assertTrue(
            any(
                row["source_digest"] == source_digest and row["processor_version"] != "stale-recipe"
                for row in rows
            )
        )

    def test_thumbnail_then_original_invalidates_stale_preview_provenance(self) -> None:
        window_name = "preview-provenance-upgrade"
        window = Path(self.temp.name) / window_name / "window.db"
        service, tools = self._thumbnail_only_stack(window_name)
        message = self._image_message(tools)
        resource = tools.wechat_list_resources(str(message["message_id"]))["resources"][0]
        resource_id = str(resource["resource_id"])

        preview = tools.wechat_read_resource(resource_id=resource_id, mode="preview")
        self.assertFalse(preview.isError)
        thumbnail_binding = service.resource_service.repository.resource_binding(
            resource_id, "preview:v2"
        )
        self.assertIsNotNone(thumbnail_binding)
        thumbnail_variants = {
            row["variant"]: row["source_digest"]
            for row in self._derivation_rows(resource_id, window=window)
        }
        self.assertIn("preview:v2", thumbnail_variants)

        # The source later produces the true original; a preview from those bytes is a
        # different provenance and must not be served from the thumbnail-era cache.
        service.resource_service.provider.derived_only = False  # type: ignore[reportAttributeAccessIssue]
        listed = tools.wechat_list_resources(str(message["message_id"]))["resources"][0]
        self.assertEqual(listed["availability"], "local_available")
        self.assertTrue(listed["original_available"])
        upgraded = tools.wechat_read_resource(resource_id=resource_id, mode="preview")
        self.assertFalse(upgraded.isError)
        rows = self._derivation_rows(resource_id, window=window)
        upgrade_source_digests = {
            row["source_digest"] for row in rows if row["variant"] == "preview:v2"
        }
        self.assertGreaterEqual(len(upgrade_source_digests), 1)
        # The recorded provenance must include the true original, not only the thumbnail.
        original_binding = service.resource_service.repository.resource_binding(
            resource_id, "original"
        )
        self.assertIsNotNone(original_binding)
        assert original_binding is not None
        self.assertIn(str(original_binding["object_digest"]), upgrade_source_digests)

    def test_flat_and_transparent_images_remain_valid_previews(self) -> None:
        # A flat black or fully transparent image is still a valid preview input: the
        # service must not add any "looks empty" quality heuristic that would reject it.
        # Exercised at the processor boundary where the synthetic source registers an
        # exact V2 digest, so the payload is encoded through the same V2 envelope.
        cases = {
            "black": _solid_rgba_png(4, 4, (0, 0, 0, 255)),
            "transparent": _solid_rgba_png(4, 4, (0, 0, 0, 0)),
        }
        for name, raw in cases.items():
            with self.subTest(image=name):
                info = resource_service_module.inspect_image(raw)
                self.assertEqual((info.width, info.height), (4, 4))
                if shutil.which("sips") is None:
                    self.skipTest("macOS sips is required for image preview rendering")
                preview, rendered = resource_service_module.image_preview(raw, max_bytes=64 * 1024)
                self.assertTrue(preview.startswith(b"\x89PNG\r\n\x1a\n"))
                self.assertEqual((rendered.width, rendered.height), (4, 4))

    def test_legacy_container_original_matches_available_descriptor(self) -> None:
        # A provider-verified attachment whose declared type is a legacy archive MIME
        # and whose bytes only resemble a container stays reachable as its source
        # original. The old behavior raised RESOURCE_DECODE_FAILED from the strict
        # archive parser while the descriptor still advertised original_available.
        payload = b"PK\x03\x04half-downloaded legacy archive body"
        self._replace_text_resource_contract(
            data=payload,
            mime_type="application/x-zip-compressed",
            original_name="legacy.bundle",
        )
        core, resource, tools = self._fresh_rich_resource(
            window_name="legacy-container",
            original_name="legacy.bundle",
        )
        self.assertTrue(resource["original_available"])
        self.assertEqual(resource["availability"], "local_available")

        read = core.read_resource(
            resource_id=resource["resource_id"],
            mode="original",
            page=None,
            start_line=None,
            end_line=None,
            max_bytes=64 * 1024,
        )
        descriptor = read.descriptor
        self.assertEqual(descriptor["derivation"]["kind"], "source_original")
        self.assertEqual(descriptor["media"]["content_block_type"], "blob")
        self.assertEqual(descriptor["media"]["mime_type"], descriptor["sniffed_mime_type"])
        self.assertEqual(read.content_kind, "blob")
        self.assertEqual(read.data, payload)

        # The parsed archive surface keeps its strict validator on the same bytes.
        with self.assertRaises(SightglassError) as caught:
            core.read_resource(
                resource_id=resource["resource_id"],
                mode="members",
                page=None,
                start_line=None,
                end_line=None,
                max_bytes=64 * 1024,
            )
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_DECODE_FAILED)

        through_tool = tools.wechat_read_resource(
            resource_id=resource["resource_id"],
            mode="original",
        )
        self.assertFalse(through_tool.isError)
        blob = next(
            item.resource
            for item in through_tool.content
            if isinstance(item, EmbeddedResource)
            and isinstance(item.resource, BlobResourceContents)
        )
        self.assertEqual(base64.b64decode(blob.blob), payload)
        self.assertNotIn(str(self.root), str(through_tool.structuredContent))

    def test_text_original_survives_an_unvalidated_structured_hint(self) -> None:
        # A file whose extension/declared type claims a structured text format the
        # bytes do not satisfy is still an inert text original. The old behavior
        # raised RESOURCE_DECODE_FAILED for mode="original" while the descriptor
        # advertised original_available; the parsed surfaces stay strict.
        payload = b"plain notes body, not json\n"
        self._replace_text_resource_contract(
            data=payload,
            mime_type="application/octet-stream",
            original_name="notes.json",
        )
        core, resource, tools = self._fresh_rich_resource(
            window_name="hinted-text",
            original_name="notes.json",
        )
        self.assertTrue(resource["original_available"])

        read = core.read_resource(
            resource_id=resource["resource_id"],
            mode="original",
            page=None,
            start_line=None,
            end_line=None,
            max_bytes=64 * 1024,
        )
        self.assertEqual(read.content_kind, "text")
        self.assertEqual(read.mime_type, "text/plain")
        self.assertEqual(read.text, payload.decode())
        self.assertEqual(read.descriptor["media"]["mime_type"], "text/plain")

        with self.assertRaises(SightglassError) as caught:
            core.read_resource(
                resource_id=resource["resource_id"],
                mode="metadata",
                page=None,
                start_line=None,
                end_line=None,
                max_bytes=64 * 1024,
            )
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_DECODE_FAILED)

        through_tool = tools.wechat_read_resource(
            resource_id=resource["resource_id"],
            mode="original",
        )
        self.assertFalse(through_tool.isError)
        embedded = next(
            item.resource
            for item in through_tool.content
            if isinstance(item, EmbeddedResource)
            and isinstance(item.resource, TextResourceContents)
        )
        self.assertEqual(embedded.text, payload.decode())
        self.assertNotIn(str(self.root), str(through_tool.structuredContent))

    def test_selector_combinations_are_rejected_as_invalid(self) -> None:
        _core, resource, tools = self._fresh_rich_resource(
            window_name="rich-selectors",
            original_name="synthetic.pdf",
        )
        resource_id = str(resource["resource_id"])
        invalid_combinations = (
            ("metadata_member", "metadata", {"member": "notes/selected.txt"}),
            ("members_cell_range", "members", {"cell_range": "B1:B1"}),
            ("members_page", "members", {"page": 1}),
            ("page_sheet", "page", {"page": 1, "sheet": "Budget"}),
            ("page_cell_range", "page", {"page": 1, "cell_range": "B1:B1"}),
            ("slide_cell_range", "slide", {"page": 1, "cell_range": "B1:B1"}),
            ("slide_sheet", "slide", {"page": 1, "sheet": "Budget"}),
            ("text_sheet", "text", {"sheet": "Budget"}),
            ("text_page_cell_range", "text", {"page": 1, "cell_range": "B1:B1"}),
            ("original_member", "original", {"member": "notes/selected.txt"}),
        )
        for label, mode, selectors in invalid_combinations:
            with self.subTest(label=label):
                result = cast(Any, tools.wechat_read_resource)(
                    resource_id=resource_id,
                    mode=mode,
                    page=selectors.get("page"),
                    start_line=selectors.get("start_line"),
                    end_line=selectors.get("end_line"),
                    member=selectors.get("member"),
                    sheet=selectors.get("sheet"),
                    cell_range=selectors.get("cell_range"),
                )
                self.assertTrue(result.isError)
                self.assertEqual(self._descriptor(result)["code"], "QUERY_INVALID")

    def test_mode_mime_guards_reject_mismatched_documents(self) -> None:
        self._replace_text_resource_contract(
            data=_xlsx_resource_bytes(),
            mime_type=XLSX_MIME,
            original_name="synthetic.xlsx",
        )
        xlsx_core, xlsx_resource, _tools = self._fresh_rich_resource(
            window_name="guard-xlsx",
            original_name="synthetic.xlsx",
        )
        # slide mode is PPTX-only; on an XLSX workbook it must not silently fall back.
        with self.assertRaises(SightglassError) as caught:
            xlsx_core.read_resource(
                resource_id=xlsx_resource["resource_id"],
                mode="slide",
                page=1,
                start_line=None,
                end_line=None,
                max_bytes=64 * 1024,
            )
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNSUPPORTED)

        _pdf_core, pdf_resource, pdf_tools = self._fresh_rich_resource(
            window_name="guard-pdf",
            original_name="synthetic.pdf",
        )
        # table mode is spreadsheet-only; on a PDF it must fail closed.
        pdf_table = pdf_tools.wechat_read_resource(
            resource_id=str(pdf_resource["resource_id"]),
            mode="table",
        )
        self.assertEqual(self._descriptor(pdf_table)["code"], "RESOURCE_UNSUPPORTED")

    def test_resource_reads_never_open_a_network_connection(self) -> None:
        resource = self._resource(self.pdf_message)
        with patch(
            "socket.create_connection",
            side_effect=AssertionError("resource reads must remain local"),
        ):
            result = self.tools.wechat_read_resource(
                resource_id=resource["resource_id"], mode="text", page=2
            )
        self.assertFalse(result.isError)

    def _run_while_a_window_writer_must_succeed(
        self,
        operation: Any,
        entered: threading.Event,
        release: threading.Event,
    ) -> Any:
        """Run ``operation`` on a worker parked inside a source read or processor.

        While the worker is parked, an independent window.db write transaction must
        still complete: a writer lock held across the source read, the attachment
        processing, or both fails this check.
        """

        operator_db = WindowDB(self.window)
        outcome: dict[str, Any] = {}

        def run_operation() -> None:
            try:
                outcome["result"] = operation()
            except BaseException as exc:
                outcome["error"] = exc
            finally:
                release.set()

        worker = threading.Thread(target=run_operation, name="sightglass-blocked-resource")
        worker.start()
        try:
            self.assertTrue(entered.wait(5), "operation never reached the blocking phase")
            started = time.monotonic()
            with operator_db.transaction() as connection:
                connection.execute("UPDATE reader_profiles SET policy_revision = policy_revision")
            elapsed = time.monotonic() - started
        finally:
            release.set()
            worker.join(timeout=20)

        self.assertFalse(worker.is_alive())
        self.assertNotIn("error", outcome)
        self.assertLess(elapsed, 1.0)
        return outcome["result"]

    def _assert_writer_lock_free(self, operation: Any) -> Any:
        entered = threading.Event()
        release = threading.Event()
        self.service.resource_service.provider = _GatedResourceProvider(  # type: ignore[reportAttributeAccessIssue]
            self.provider, entered, release
        )
        return self._run_while_a_window_writer_must_succeed(operation, entered, release)

    def test_resource_reads_do_not_hold_the_window_writer_lock(self) -> None:
        resource = self._resource(self.text_message)
        resource_id = str(resource["resource_id"])

        listed = self._assert_writer_lock_free(
            lambda: self.service.list_resources(str(self.text_message["message_id"]))
        )
        self.assertEqual(listed["schema"], "sightglass.resource-list.v1")

        read = self._assert_writer_lock_free(
            lambda: self.service.read_resource(
                resource_id=resource_id,
                mode="text",
                page=None,
                start_line=None,
                end_line=None,
                max_bytes=64 * 1024,
            )
        )
        self.assertEqual(read.content_kind, "text")

        # Force the search back through cold acquisition; the previous read populated
        # the private CAS and would otherwise prove only the local-cache fast path.
        with self.repository.database.transaction() as connection:
            connection.execute(
                "DELETE FROM resource_bindings WHERE resource_id = ?",
                (resource_id,),
            )

        found = self._assert_writer_lock_free(
            lambda: self.service.search_resource_text(
                resource_id=resource_id, query="atomic publish", limit=20
            )
        )
        self.assertEqual(found["schema"], "sightglass.resource-search-results.v1")

    def test_resource_processor_work_does_not_hold_the_window_writer_lock(self) -> None:
        resource = self._resource(self.image_message)
        entered = threading.Event()
        release = threading.Event()
        real_preview = resource_service_module.image_preview

        def gated_preview(*args: Any, **kwargs: Any) -> Any:
            entered.set()
            release.wait(timeout=15)
            return real_preview(*args, **kwargs)

        with patch("sightglass.resources.service.image_preview", new=gated_preview):
            result = self._run_while_a_window_writer_must_succeed(
                lambda: self.service.read_resource(
                    resource_id=str(resource["resource_id"]),
                    mode="preview",
                    page=None,
                    start_line=None,
                    end_line=None,
                    max_bytes=4 * 1024 * 1024,
                ),
                entered,
                release,
            )
        self.assertEqual(result.descriptor["derivation"]["kind"], "generated_image_preview")
        self.assertEqual(result.content_kind, "image")

    def test_cold_resource_processor_runs_after_scoped_lease_release(self) -> None:
        resource = self._resource(self.image_message)
        provider = _SessionTrackingProvider(self.provider)
        self.service.resource_service.provider = provider  # type: ignore[reportAttributeAccessIssue]
        real_preview = resource_service_module.image_preview

        def checked_preview(*args: Any, **kwargs: Any) -> Any:
            self.assertEqual(provider.active_sessions, 0)
            return real_preview(*args, **kwargs)

        with patch("sightglass.resources.service.image_preview", new=checked_preview):
            result = self.service.read_resource(
                resource_id=str(resource["resource_id"]),
                mode="preview",
                page=None,
                start_line=None,
                end_line=None,
                max_bytes=4 * 1024 * 1024,
            )

        self.assertEqual(result.content_kind, "image")
        self.assertEqual(provider.sessions, 1)
        self.assertEqual(provider.resource_reads, 1)
        self.assertEqual(provider.snapshots, 0)
        self.assertEqual(provider.active_sessions, 0)

    def _admission_counts(self) -> tuple[int, int]:
        with closing(sqlite3.connect(self.window)) as connection:
            objects = int(connection.execute("SELECT COUNT(*) FROM resource_objects").fetchone()[0])
            bindings = int(
                connection.execute("SELECT COUNT(*) FROM resource_bindings").fetchone()[0]
            )
        return objects, bindings

    def test_scoped_acquisition_validation_failure_leaves_admission_unchanged(self) -> None:
        resource_id = str(self._resource(self.text_message)["resource_id"])
        provider = _FailingExitProvider(self.provider, fail_on=1)
        self.service.resource_service.provider = provider  # type: ignore[reportAttributeAccessIssue]

        with self.assertRaises(SightglassError) as caught:
            self.service.read_resource(
                resource_id=resource_id,
                mode="text",
                page=None,
                start_line=None,
                end_line=None,
                max_bytes=64 * 1024,
            )

        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)
        self.assertEqual(self._admission_counts(), (0, 0))
        self.assertIsNone(self.repository.resource_binding(resource_id, "original"))
        self.assertEqual(provider.opened, 0)
        self.assertEqual(provider.sessions, 1)

    def test_live_resource_call_retries_the_scoped_acquisition(self) -> None:
        resource = self._resource(self.text_message)
        provider = _FailingExitProvider(self.provider, fail_on=1)
        provider.descriptor = replace(self.provider.descriptor, source_mode="live")
        self.service.provider = provider  # type: ignore[reportAttributeAccessIssue]
        self.service.resource_service.provider = provider  # type: ignore[reportAttributeAccessIssue]

        result = self.tools.wechat_read_resource(resource_id=resource["resource_id"], mode="text")

        self.assertFalse(result.isError)
        self.assertEqual(provider.opened, 0)
        self.assertEqual(provider.sessions, 2)

    def test_resolver_revision_change_rejects_cold_read_admission(self) -> None:
        resource_id = str(self._resource(self.text_message)["resource_id"])
        service = self.service.resource_service
        real_resolve = service._resolve_payload

        def change_revision(*args: Any, **kwargs: Any) -> Any:
            result = real_resolve(*args, **kwargs)
            with self.repository.database.transaction() as connection:
                connection.execute(
                    "UPDATE resources SET resolver_json = ? WHERE resource_id = ?",
                    ('{"active":true,"binding_fingerprint":"changed"}', resource_id),
                )
            return result

        with patch.object(service, "_resolve_payload", side_effect=change_revision):
            with self.assertRaises(SightglassError) as caught:
                self.service.read_resource(
                    resource_id=resource_id,
                    mode="text",
                    page=None,
                    start_line=None,
                    end_line=None,
                    max_bytes=64 * 1024,
                )

        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)
        self.assertEqual(self._admission_counts(), (0, 0))
        self.assertIsNone(self.repository.resource_binding(resource_id, "original"))
        cache_objects = self.window.parent / "resource-cache" / "objects"
        self.assertTrue(list(cache_objects.iterdir()))

    def test_resolver_revision_change_rejects_search_admission(self) -> None:
        resource_id = str(self._resource(self.text_message)["resource_id"])
        service = self.service.resource_service
        real_search = service._search_hits

        def change_revision(*args: Any, **kwargs: Any) -> Any:
            result = real_search(*args, **kwargs)
            with self.repository.database.transaction() as connection:
                connection.execute(
                    "UPDATE resources SET resolver_json = ? WHERE resource_id = ?",
                    ('{"active":true,"binding_fingerprint":"changed"}', resource_id),
                )
            return result

        with patch.object(service, "_search_hits", side_effect=change_revision):
            with self.assertRaises(SightglassError) as caught:
                self.service.search_resource_text(
                    resource_id=resource_id, query="atomic publish", limit=20
                )

        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)
        self.assertEqual(self._admission_counts(), (0, 0))

    def test_resolver_change_during_acquisition_is_never_bound(self) -> None:
        # The revision must be pinned to the value captured *before* the source read.
        # Here the resolver advances while the provider is reading the bytes, so the
        # acquired payload no longer belongs to the revision the coalescing key named.
        # Admission must fail closed and persist nothing.
        resource_id = str(self._resource(self.image_message)["resource_id"])
        service = self.service.resource_service
        repository = self.repository
        window = self.window
        real_read = service.provider.read_resource

        def advance_resolver(*args: Any, **kwargs: Any) -> Any:
            payload = real_read(*args, **kwargs)
            with closing(sqlite3.connect(window)) as connection:
                connection.execute(
                    "UPDATE resources SET resolver_json = ? WHERE resource_id = ?",
                    ('{"active":true,"binding_fingerprint":"advanced-mid-read"}', resource_id),
                )
                connection.commit()
            return payload

        with patch.object(service.provider, "read_resource", side_effect=advance_resolver):
            with self.assertRaises(SightglassError) as caught:
                self.service.read_resource(
                    resource_id=resource_id,
                    mode="preview",
                    page=None,
                    start_line=None,
                    end_line=None,
                    max_bytes=4 * 1024 * 1024,
                )

        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)
        self.assertEqual(self._admission_counts(), (0, 0))
        self.assertIsNone(repository.resource_binding(resource_id, "original"))
        self.assertIsNone(repository.resource_binding(resource_id, "preview:v2"))
        self.assertEqual(self._derivation_rows(resource_id), [])

    def test_cache_is_private_content_addressed_and_receipts_are_redacted(self) -> None:
        resource = self._resource(self.text_message)
        read = self.tools.wechat_read_resource(
            resource_id=resource["resource_id"], mode="text", start_line=1, end_line=2
        )
        self.assertFalse(read.isError)
        self.tools.wechat_search_resource_text(
            resource_id=resource["resource_id"], query="atomic publish", limit=20
        )

        cache_root = self.window.parent / "resource-cache"
        for directory in (cache_root, cache_root / "objects", cache_root / "tmp"):
            self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        objects = list((cache_root / "objects").iterdir())
        self.assertTrue(objects)
        for path in objects:
            data = path.read_bytes()
            self.assertEqual(path.name, hashlib.sha256(data).hexdigest())
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(path.stat().st_nlink, 1)

        with closing(sqlite3.connect(self.window)) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT tool_name, conversation_id, scope_kind, scope_digest,
                       resource_count, bytes_returned
                FROM access_receipts
                WHERE tool_name IN (
                    'wechat_list_resources',
                    'wechat_read_resource',
                    'wechat_search_resource_text'
                )
                ORDER BY started_at
                """
            ).fetchall()
        self.assertEqual(
            {row["tool_name"] for row in rows},
            {
                "wechat_list_resources",
                "wechat_read_resource",
                "wechat_search_resource_text",
            },
        )
        for row in rows:
            self.assertEqual(row["conversation_id"], self.group_id)
            self.assertGreaterEqual(row["resource_count"], 1)
            self.assertGreater(row["bytes_returned"], 0)
            rendered = str(dict(row))
            self.assertNotIn("notes.md", rendered)
            self.assertNotIn("atomic publish", rendered)
            self.assertNotIn(str(self.root), rendered)
            self.assertNotIn(resource["resource_id"], rendered)

        original_binding = self.repository.resource_binding(resource["resource_id"], "original")
        self.assertIsNotNone(original_binding)
        assert original_binding is not None
        cached_path = Path(str(original_binding["local_path_internal"]))
        cached = bytearray(cached_path.read_bytes())
        cached[0] ^= 0xFF
        cached_path.write_bytes(cached)
        os.chmod(cached_path, 0o600)
        blocked = self.tools.wechat_read_resource(resource_id=resource["resource_id"], mode="text")
        self.assertEqual(self._descriptor(blocked)["code"], "RESOURCE_BLOCKED")

    def test_resource_identity_survives_parser_discovery_reordering(self) -> None:
        def set_resources(values: list[dict[str, object]]) -> None:
            with closing(sqlite3.connect(self.root / "messages-1.db")) as connection:
                connection.execute(
                    "UPDATE messages SET resources_json = ? WHERE source_message_id = ?",
                    (json.dumps(values), "source-msg-004"),
                )
                connection.commit()

        first_values = [
            {
                "source_ordinal": 0,
                "kind": "image",
                "mime_type": "image/png",
                "declared_hash": "a" * 64,
                "availability": "metadata_only",
            },
            {
                "source_ordinal": 1,
                "kind": "image",
                "mime_type": "image/png",
                "declared_hash": "b" * 64,
                "availability": "metadata_only",
            },
        ]
        set_resources(first_values)
        page = self.tools.wechat_read_messages(
            mode="message", message_id=self.image_message["message_id"]
        )
        first = {
            item["declared_hash"]: item["resource_id"] for item in page["messages"][0]["resources"]
        }
        second_values = [
            {**first_values[1], "source_ordinal": 0},
            {**first_values[0], "source_ordinal": 1},
        ]
        set_resources(second_values)
        page = self.tools.wechat_read_messages(
            mode="message", message_id=self.image_message["message_id"]
        )
        second = {
            item["declared_hash"]: item["resource_id"] for item in page["messages"][0]["resources"]
        }
        self.assertEqual(first, second)

    def test_manifest_resource_path_traversal_is_rejected_without_opening_target(self) -> None:
        outside = Path(self.temp.name) / "outside.png"
        outside.write_bytes(zlib.compress(b"private"))
        manifest_path = self.root / "source.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["resources"][0]["file"] = "../outside.png"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        health = DirectWeChatSourceProvider(self.root).health()
        self.assertFalse(health.complete)
        self.assertIn("source_path_escape", health.warnings)

    def test_persist_staged_keeps_the_common_path_inside_a_short_transaction(self) -> None:
        service = self.service.resource_service
        resource_id = str(self._resource(self.image_message)["resource_id"])
        data = b"synthetic staged bytes for the identity fast path"
        staged: list[Any] = []
        service._stage(
            staged,
            resource_id,
            "identity-fast-path",
            data,
            mime_type="application/octet-stream",
            origin="private_cache",
        )
        with (
            patch.object(
                service.cache,
                "put",
                side_effect=AssertionError("intact staged object must not be re-read under lock"),
            ),
            self.repository.database.transaction(),
        ):
            service._persist_staged(staged)
        binding = self.repository.resource_binding(resource_id, "identity-fast-path")
        self.assertIsNotNone(binding)
        self.assertEqual(service.cache.read_binding(binding).data, data)

    def test_persist_staged_republishes_bytes_reaped_after_stage(self) -> None:
        """A cleanup that reaps a staged-but-unadmitted orphan cannot leave a dangling row."""

        service = self.service.resource_service
        resource_id = str(self._resource(self.image_message)["resource_id"])
        data = b"synthetic staged bytes for the cache-reap regression"
        staged: list[Any] = []
        cached = service._stage(
            staged,
            resource_id,
            "original",
            data,
            mime_type="application/octet-stream",
            origin="private_cache",
        )
        self.assertEqual(len(staged), 1)
        self.assertEqual(cached.digest, staged[0].object_digest)
        path = Path(staged[0].local_path_internal)
        digest = path.name
        self.assertEqual(digest, hashlib.sha256(data).hexdigest())
        self.assertTrue(path.exists())
        with self.repository.database.connection() as connection:
            self.assertIsNone(
                connection.execute(
                    "SELECT 1 FROM resource_objects WHERE object_digest = ?",
                    (digest,),
                ).fetchone()
            )

        # Age the unregistered CAS file beyond orphan grace so production cleanup removes it.
        old = time.time() - 25 * 60 * 60
        os.utime(path, (old, old))
        cleanup = cleanup_cache(self.repository.database, apply=True)
        self.assertGreaterEqual(cleanup["removed_count"], 1)
        self.assertFalse(path.exists())

        # Production admission republishes the staged bytes inside the writer transaction.
        with self.repository.database.transaction():
            service._persist_staged(staged)

        self.assertTrue(path.exists())
        binding = self.repository.resource_binding(resource_id, "original")
        assert binding is not None
        self.assertEqual(str(binding["object_digest"]), digest)
        self.assertEqual(str(binding["local_path_internal"]), str(path))
        self.assertEqual(service.cache.read_binding(binding).data, data)

    def test_mixed_resource_views_singleflight_one_source_acquisition(self) -> None:
        resource_id = str(self._resource(self.pdf_message)["resource_id"])
        original = self.provider.read_resource
        entered = threading.Event()
        release = threading.Event()
        calls = 0
        calls_lock = threading.Lock()
        results: list[Any] = []

        def gated(*args: Any, **kwargs: Any) -> Any:
            nonlocal calls
            with calls_lock:
                calls += 1
            entered.set()
            self.assertTrue(release.wait(timeout=2))
            return original(*args, **kwargs)

        def read(mode: str, page: int | None = None) -> None:
            results.append(
                self.service.read_resource(
                    resource_id=resource_id,
                    mode=mode,
                    page=page,
                    start_line=None,
                    end_line=None,
                    max_bytes=4 * 1024 * 1024,
                )
            )

        workers = [
            threading.Thread(target=read, args=("metadata",)),
            threading.Thread(target=read, args=("page", 1)),
            threading.Thread(target=read, args=("text", 1)),
        ]
        with patch.object(self.provider, "read_resource", side_effect=gated):
            for worker in workers:
                worker.start()
            self.assertTrue(entered.wait(timeout=1))
            time.sleep(0.05)
            release.set()
            for worker in workers:
                worker.join(timeout=5)
        self.assertTrue(all(not worker.is_alive() for worker in workers))
        self.assertEqual(calls, 1)
        self.assertEqual(len(results), 3)

    def test_deferred_pdf_page_survives_worker_handoff_and_polls_ready(self) -> None:
        resource_id = str(self._resource(self.pdf_message)["resource_id"])
        lanes = RuntimeLanes(LaneLimits(resource_derivation=1))
        jobs = ResourceJobService(self.repository)
        worker = ResourceWorker(
            self.service.resource_service,
            jobs,
            poll_interval_seconds=0.01,
        )
        self.service.resource_service.configure_runtime(
            lanes=lanes,
            jobs=jobs,
            wake_resource_worker=worker.wake,
            source_foreground_enter=lambda: None,
            source_foreground_exit=lambda: None,
        )
        try:
            with patch.object(resource_service_module, "_ASYNC_PDF_BYTES", 0):
                processing = self.service.read_resource(
                    resource_id=resource_id,
                    mode="page",
                    page=1,
                    start_line=None,
                    end_line=None,
                    max_bytes=4 * 1024 * 1024,
                )
            self.assertEqual(processing.descriptor["state"], "processing")
            token = str(processing.descriptor["reading_token"])
            self.assertIsNotNone(self.repository.resource_binding(resource_id, "original"))
            worker.start()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                job = jobs.by_id(token)
                if job is not None and job["state"] == "ready":
                    break
                time.sleep(0.02)
            else:
                self.fail("resource worker did not complete the deferred page")
            ready = self.service.read_resource(
                resource_id=resource_id,
                mode="page",
                page=1,
                start_line=None,
                end_line=None,
                max_bytes=4 * 1024 * 1024,
                reading_token=token,
            )
            self.assertEqual(ready.content_kind, "image")
            self.assertEqual(ready.descriptor["resolution"]["variant"], "page:1")
        finally:
            worker.stop()

    def test_lost_resource_job_lease_cannot_publish_staged_bindings(self) -> None:
        resource_id = str(self._resource(self.pdf_message)["resource_id"])

        def lost_lease() -> None:
            raise SightglassError(ErrorCode.CURSOR_STALE)

        with self.assertRaises(SightglassError) as raised:
            self.service.resource_service.read_resource(
                resource_id=resource_id,
                mode="page",
                page=1,
                start_line=None,
                end_line=None,
                max_bytes=4 * 1024 * 1024,
                allow_async=False,
                publish_guard=lost_lease,
            )
        self.assertEqual(raised.exception.code, ErrorCode.CURSOR_STALE)
        self.assertIsNone(self.repository.resource_binding(resource_id, "original"))
        self.assertIsNone(self.repository.resource_binding(resource_id, "page:1"))

    def test_cached_image_preview_uses_local_lane_while_derivation_is_saturated(self) -> None:
        resource_id = str(self._resource(self.image_message)["resource_id"])
        warmed = self.service.read_resource(
            resource_id=resource_id,
            mode="preview",
            page=None,
            start_line=None,
            end_line=None,
            max_bytes=4 * 1024 * 1024,
        )
        self.assertEqual(warmed.content_kind, "image")

        lanes = RuntimeLanes(LaneLimits(resource_derivation=1))
        jobs = ResourceJobService(self.repository)
        self.service.resource_service.configure_runtime(
            lanes=lanes,
            jobs=jobs,
            wake_resource_worker=lambda: None,
            source_foreground_enter=lambda: None,
            source_foreground_exit=lambda: None,
        )
        with lanes.held(WorkClass.RESOURCE_DERIVATION, wait=True):
            cached = self.service.read_resource(
                resource_id=resource_id,
                mode="preview",
                page=None,
                start_line=None,
                end_line=None,
                max_bytes=4 * 1024 * 1024,
            )
        self.assertEqual(cached.content_kind, "image")
        self.assertNotEqual(cached.descriptor.get("state"), "processing")

    def test_resource_finder_is_local_policy_bound_and_cursor_stable(self) -> None:
        with patch.object(
            self.provider,
            "snapshot",
            side_effect=AssertionError("resource finder must not open the source"),
        ):
            found = self.tools.wechat_find_resources(
                query="synthetic.pdf",
                conversation_ids=[self.group_id],
                format_families=["pdf"],
                limit=10,
            )
        self.assertEqual(found["schema"], "sightglass.resource-search.v1")
        self.assertEqual(len(found["items"]), 1)
        resource = found["items"][0]["resource"]
        self.assertEqual(resource["format_family"], "pdf")
        self.assertTrue(resource["available_views"]["page"])
        self.assertTrue(resource["available_views"]["text"])

        first = self.tools.wechat_find_resources(conversation_ids=[self.group_id], limit=1)
        self.assertTrue(first["page"]["has_more"])
        second = self.tools.wechat_find_resources(
            conversation_ids=[self.group_id],
            cursor=first["page"]["next_cursor"],
            limit=1,
        )
        first_id = first["items"][0]["resource"]["resource_id"]
        second_id = second["items"][0]["resource"]["resource_id"]
        self.assertNotEqual(first_id, second_id)

        changed_scope = self.tools.wechat_find_resources(
            query="different",
            conversation_ids=[self.group_id],
            cursor=first["page"]["next_cursor"],
            limit=1,
        )
        self.assertEqual(changed_scope["code"], "CURSOR_INVALID")

    def test_resource_finder_uses_cached_detected_mime(self) -> None:
        resource_id = str(self._resource(self.pdf_message)["resource_id"])
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE resources SET mime_type = 'application/octet-stream' WHERE resource_id = ?",
                (resource_id,),
            )
        read = self.service.read_resource(
            resource_id=resource_id,
            mode="metadata",
            page=None,
            start_line=None,
            end_line=None,
            max_bytes=4 * 1024 * 1024,
        )
        self.assertEqual(read.descriptor["sniffed_mime_type"], "application/pdf")

        found = self.tools.wechat_find_resources(
            query="synthetic.pdf",
            conversation_ids=[self.group_id],
            format_families=["pdf"],
            limit=10,
        )
        self.assertEqual(
            [item["resource"]["resource_id"] for item in found["items"]],
            [resource_id],
        )

    def test_resource_finder_cursor_excludes_rows_changed_after_first_page(self) -> None:
        baseline = self.tools.wechat_find_resources(conversation_ids=[self.group_id], limit=10)
        self.assertGreaterEqual(len(baseline["items"]), 3)
        first = self.tools.wechat_find_resources(conversation_ids=[self.group_id], limit=1)
        excluded = baseline["items"][1]
        excluded_resource_id = str(excluded["resource"]["resource_id"])
        watermark = self.repository.observation_watermark()
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET current_observation_seq = ? WHERE message_id = ?",
                (watermark + 1, str(excluded["message_id"])),
            )

        second = self.tools.wechat_find_resources(
            conversation_ids=[self.group_id],
            cursor=first["page"]["next_cursor"],
            limit=1,
        )
        self.assertNotEqual(second["items"][0]["resource"]["resource_id"], excluded_resource_id)


if __name__ == "__main__":
    unittest.main()

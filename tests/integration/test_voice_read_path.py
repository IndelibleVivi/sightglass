from __future__ import annotations

import dataclasses
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.voice import VoiceReadSettings
from sightglass.mcp.tools import ReaderTools
from sightglass.model.repositories import WindowRepository
from sightglass.policy.readers import ReaderPolicy
from sightglass.reader.service import ReaderService
from sightglass.reader.voice_read import (
    VOICE_READ_BUDGET_GUIDE,
    VOICE_READ_UNREADABLE_GUIDE,
    VOICE_SIDECAR_RESERVE_CHARS,
)
from sightglass.source.synthetic import create_synthetic_source
from sightglass.voice.service import VoiceService
from tests.fixtures.factory import build_test_stack
from tests.fixtures.voice_source import declare_voice_messages

COMPACT_BUDGET_POLICY = 40_000
CROPPED_VOICE_COUNT = 493


class VoiceReadFixture:
    provider: Any
    repository: WindowRepository
    service: ReaderService
    tools: ReaderTools
    voice_service: VoiceService

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.source_root = Path(self.temp.name) / "source"
        create_synthetic_source(self.source_root)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def build(
        self,
        *,
        voice_count: int = 2,
        voice_policy: str = "auto",
        enabled: bool = True,
        policy: ReaderPolicy | None = None,
        paused: bool = False,
        available: bool = True,
        first_time: datetime | None = None,
        name: str = "state",
    ) -> None:
        kwargs: dict[str, Any] = {"first_time": first_time} if first_time is not None else {}
        self.voice_message_ids = declare_voice_messages(
            self.source_root, count=voice_count, available=available, **kwargs
        )
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            self.source_root,
            Path(self.temp.name) / name / "window.db",
            default_projection=None,
            policy=policy,
            paused=paused,
            voice=VoiceReadSettings(enabled=enabled, default_policy=voice_policy, language="zh"),
        )
        resolved_voice = self.tools.voice_service
        assert resolved_voice is not None
        self.voice_service = resolved_voice
        self.group_id = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]

    # -- helpers -----------------------------------------------------------

    def counts(self) -> tuple[int, int, int]:
        with self.repository.database.connection() as connection:
            return (
                int(connection.execute("SELECT count(*) FROM voice_jobs").fetchone()[0]),
                int(connection.execute("SELECT count(*) FROM voice_batches").fetchone()[0]),
                int(connection.execute("SELECT count(*) FROM voice_batch_items").fetchone()[0]),
            )

    def jobs(self) -> list[dict[str, Any]]:
        return self.voice_service.repository.rows("SELECT * FROM voice_jobs ORDER BY created_at")

    def batch_items(self, token: str) -> list[dict[str, Any]]:
        return self.voice_service.repository.items(token)

    def complete_transcripts(self, text: str = "Synthetic transcript") -> int:
        completed = 0
        for job in self.jobs():
            fence = self.voice_service.lease(str(job["job_id"]), owner_id="synthetic-probe-worker")
            self.voice_service.complete(
                str(job["job_id"]),
                owner_id="synthetic-probe-worker",
                fencing_token=fence,
                text=f"{text} {completed}",
            )
            completed += 1
        return completed

    def read_page(self, **kwargs: Any) -> dict[str, Any]:
        return self.tools.wechat_read_messages(
            mode=kwargs.pop("mode", "recent"),
            conversation_id=kwargs.pop("conversation_id", self.group_id),
            **kwargs,
        )

    def read_updates(self, **kwargs: Any) -> dict[str, Any]:
        return self.tools.wechat_read_messages(
            mode="updates", conversation_id=self.group_id, **kwargs
        )

    def delivered_voice_rows(self, page: dict[str, Any]) -> int:
        if page.get("projection") == "detail":
            message_ids = [str(message["message_id"]) for message in page["messages"]]
        else:
            message_ids = [str(row[0]) for row in page["messages"]]
        if not message_ids:
            return 0
        with self.repository.database.connection() as connection:
            placeholders = ",".join("?" for _ in message_ids)
            return int(
                connection.execute(
                    "SELECT count(*) FROM resources WHERE kind = 'voice' "
                    f"AND message_id IN ({placeholders})",
                    tuple(message_ids),
                ).fetchone()[0]
            )

    def voice_resource_id(self) -> str:
        self.read_page(projection="detail", limit=50, voice="off")
        with self.repository.database.connection() as connection:
            row = connection.execute(
                "SELECT resource_id FROM resources WHERE kind = 'voice' ORDER BY resource_id"
            ).fetchone()
        assert row is not None
        return str(row["resource_id"])

    def read_voice_text(self, resource_id: str) -> Any:
        return self.service.read_resource(
            resource_id=resource_id,
            mode="text",
            page=None,
            start_line=None,
            end_line=None,
            max_bytes=4 * 1024 * 1024,
        )

    def voice_policy(self, requested: str | None) -> str:
        return self.service._voice_policy(requested)  # noqa: SLF001 - policy surface check


class VoicePolicyTests(VoiceReadFixture, unittest.TestCase):
    def test_off_and_disabled_leave_pages_and_tables_untouched(self) -> None:
        self.build(voice_policy="off")
        off_page = self.read_page(projection="detail", limit=50, voice="off")
        self.assertNotIn("voice", off_page)
        self.assertEqual(self.counts(), (0, 0, 0))

        default_page = self.read_page(projection="detail", limit=50)
        self.assertEqual(default_page["messages"], off_page["messages"])
        self.assertNotIn("voice", default_page)
        self.assertEqual(self.voice_policy(None), "off")
        self.assertEqual(self.voice_policy("off"), "off")

    def test_disabled_voice_configuration_ignores_the_request(self) -> None:
        self.build(enabled=False)
        self.assertEqual(self.voice_policy("auto"), "off")
        page = self.read_page(projection="detail", limit=50, voice="auto")
        self.assertNotIn("voice", page)
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_invalid_voice_policy_is_rejected_for_every_configuration(self) -> None:
        self.build(voice_policy="off")
        for value in ("sometimes", "AUTO", ""):
            with self.assertRaises(SightglassError) as caught:
                self.service.read_messages(
                    mode="recent",
                    conversation_id=self.group_id,
                    projection="detail",
                    limit=5,
                    voice=value,
                )
            self.assertEqual(caught.exception.code, ErrorCode.QUERY_INVALID)
        envelope = self.read_page(projection="detail", limit=5, voice="sometimes")
        self.assertEqual(envelope["schema"], "sightglass.error.v1")
        self.assertEqual(envelope["code"], "QUERY_INVALID")
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_auto_is_not_implicit_for_a_message_only_reader(self) -> None:
        self.build(policy=ReaderPolicy(mode="all_except_denylist", resource_preview=False))
        page = self.read_page(projection="detail", limit=50, voice="auto")
        self.assertTrue(page["messages"])
        self.assertNotIn("voice", page)
        self.assertEqual(self.voice_policy("auto"), "off")
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_pause_during_preparation_fails_closed(self) -> None:
        self.build()
        original = self.repository.voice_resources_for_messages

        def pause_then_collect(message_ids: tuple[str, ...]):
            self.service.reader.paused = True
            return original(message_ids)

        with patch.object(
            self.repository, "voice_resources_for_messages", side_effect=pause_then_collect
        ):
            with self.assertRaises(SightglassError) as caught:
                self.service.read_messages(
                    mode="recent",
                    conversation_id=self.group_id,
                    projection="detail",
                    limit=50,
                    voice="auto",
                )
        self.assertEqual(caught.exception.code, ErrorCode.SERVICE_PAUSED)
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_denied_conversation_refuses_every_voice_read(self) -> None:
        self.build(voice_count=1)
        resource_id = self.voice_resource_id()
        self.service.reader.policy = dataclasses.replace(
            self.service.reader.policy, denied_conversation_ids=frozenset({self.group_id})
        )
        with self.assertRaises(SightglassError) as caught:
            self.service.read_messages(
                mode="recent", conversation_id=self.group_id, projection="detail", limit=50,
                voice="auto",
            )
        self.assertEqual(caught.exception.code, ErrorCode.POLICY_DENIED)
        with self.assertRaises(SightglassError) as resource_error:
            self.read_voice_text(resource_id)
        self.assertEqual(resource_error.exception.code, ErrorCode.POLICY_DENIED)
        self.assertEqual(self.counts(), (0, 0, 0))


class VoiceTranscriptAccessTests(VoiceReadFixture, unittest.TestCase):
    def _reading_token(self) -> str:
        page = self.read_page(projection="detail", limit=50, voice="auto")
        token = page["voice"]["reading_token"]
        self.assertIsNotNone(token)
        return str(token)

    def test_valid_tokens_do_not_retain_a_revoked_preview_capability(self) -> None:
        self.build(voice_count=2)
        token = self._reading_token()
        self.complete_transcripts()

        granted = self.tools.wechat_read_transcripts(token, wait_ms=0)
        self.assertEqual(granted["schema"], "sightglass.voice-page.v2")
        self.assertTrue(granted["items"])
        cursor = granted.get("next_cursor")
        self.assertIsNotNone(cursor)

        self.service.reader.policy = dataclasses.replace(
            self.service.reader.policy, resource_preview=False
        )

        # The token was issued while preview was granted; it must not survive the
        # revocation, and neither may its signed continuation cursor.
        denied = self.tools.wechat_read_transcripts(token, wait_ms=0)
        self.assertEqual(denied["schema"], "sightglass.error.v1")
        self.assertEqual(denied["code"], "POLICY_DENIED")
        continued = self.tools.wechat_read_transcripts(token, cursor=cursor, wait_ms=0)
        self.assertEqual(continued["schema"], "sightglass.error.v1")
        self.assertEqual(continued["code"], "POLICY_DENIED")

        # Ordinary message reads stay allowed for a messages=true reader.
        messages = self.read_page(projection="detail", limit=10, voice="off")
        self.assertTrue(messages["messages"])


class VoicePagePreparationTests(VoiceReadFixture, unittest.TestCase):
    def test_auto_prepares_one_bounded_batch_from_the_delivered_rows(self) -> None:
        self.build(voice_count=5)
        page = self.read_page(projection="detail", limit=50, voice="auto")

        sidecar = page["voice"]
        self.assertEqual(sidecar["schema"], "sightglass.voice-sidecar.v1")
        self.assertEqual(sidecar["policy"], "auto")
        self.assertEqual(sidecar["state"], "prepared")
        self.assertEqual(sidecar["derivation"], {"kind": "derived_transcript"})
        self.assertEqual(sidecar["fields"], ["message_id", "resource_id", "state", "text"])
        self.assertEqual(sidecar["selected_count"], 5)
        self.assertEqual(sidecar["coverage"]["selected"], 5)
        self.assertEqual(sidecar["coverage"]["pending"], 2)
        self.assertEqual(sidecar["coverage"]["not_scheduled"], 3)
        self.assertEqual(sidecar["excluded"], {"unreadable": 0, "unknown_revision": 0})
        self.assertEqual(sidecar["items"], [])
        self.assertTrue(sidecar["items_complete"])
        self.assertIn("wechat_read_transcripts", sidecar["guide"])

        jobs, batches, items = self.counts()
        self.assertEqual((jobs, batches, items), (2, 1, 5))
        token = sidecar["reading_token"]
        self.assertTrue(token.startswith("voice_"))
        admitted = [item for item in self.batch_items(token) if item["job_id"]]
        self.assertEqual(len(admitted), 2)
        self.assertTrue(all(item["state"] == "admitted" for item in admitted))
        self.assertEqual(
            sorted(str(item["job_id"]) for item in admitted),
            sorted(str(job["job_id"]) for job in self.jobs()),
        )

    def test_auto_batches_cover_the_focus_rows_first(self) -> None:
        self.build(voice_count=3)
        page = self.service.read_messages(
            mode="context",
            message_id=self.newest_voice_message_id(),
            conversation_id=self.group_id,
            projection="detail",
            before=2,
            after=0,
            voice="auto",
        )
        focus_ids = {
            str(message["message_id"])
            for message in page["messages"]
            if message["retrieval"]["focus_match"]
        }
        context_ids = {
            str(message["message_id"])
            for message in page["messages"]
            if message["retrieval"]["context_only"]
        }
        self.assertEqual(len(focus_ids), 1)
        self.assertEqual(len(context_ids), 2)
        batch = self.batch_items(page["voice"]["reading_token"])
        focus_ordinals = [
            int(item["ordinal"]) for item in batch if str(item["message_id"]) in focus_ids
        ]
        context_ordinals = [
            int(item["ordinal"]) for item in batch if str(item["message_id"]) in context_ids
        ]
        self.assertEqual(focus_ordinals, [0])
        self.assertEqual(sorted(context_ordinals), [1, 2])
        admitted = {int(item["ordinal"]) for item in batch if item["job_id"]}
        self.assertEqual(admitted, {0, 1})

    def newest_voice_message_id(self) -> str:
        page = self.read_page(projection="detail", limit=50, voice="off")
        with self.repository.database.connection() as connection:
            voice_ids = {
                str(row[0])
                for row in connection.execute(
                    "SELECT message_id FROM resources WHERE kind = 'voice'"
                )
            }
        matching = [
            str(message["message_id"])
            for message in page["messages"]
            if str(message["message_id"]) in voice_ids
        ]
        if not matching:
            self.fail("no voice message was delivered")
        return matching[-1]

    def test_cached_only_matches_committed_transcripts_without_new_work(self) -> None:
        self.build(voice_policy="auto")
        cold = self.read_page(projection="detail", limit=50, voice="cached")
        self.assertEqual(cold["voice"]["state"], "cached_only")
        self.assertEqual(cold["voice"]["coverage"]["not_scheduled"], 2)
        self.assertEqual(cold["voice"]["coverage"]["pending"], 0)
        self.assertEqual(cold["voice"]["items"], [])
        self.assertEqual(self.counts(), (0, 1, 2))

        self.read_page(projection="detail", limit=50, voice="auto")
        self.assertEqual(self.complete_transcripts(), 2)
        jobs_before = [str(job["job_id"]) for job in self.jobs()]

        warm = self.read_page(projection="detail", limit=50, voice="cached")
        self.assertEqual(warm["voice"]["state"], "cached")
        self.assertEqual(warm["voice"]["coverage"]["ready"], 2)
        self.assertTrue(warm["voice"]["text_inline"])
        self.assertEqual(len(warm["voice"]["items"]), 2)
        for row in warm["voice"]["items"]:
            self.assertEqual(row[2], "ready")
            self.assertTrue(str(row[3]).startswith("Synthetic transcript"))
        self.assertEqual([str(job["job_id"]) for job in self.jobs()], jobs_before)

    def test_terminal_sidecar_does_not_mislabel_failed_work_as_blocked(self) -> None:
        self.build(voice_count=1)
        first = self.read_page(projection="detail", limit=50, voice="auto")
        job = self.jobs()[0]["job_id"]
        fence = self.voice_service.lease(job, owner_id="synthetic-worker")
        self.voice_service.fail(
            job,
            owner_id="synthetic-worker",
            fencing_token=fence,
            error_code="SOURCE_GENERATION_CHANGED",
        )

        replay = self.read_page(projection="detail", limit=50, voice="auto")

        self.assertEqual(replay["voice"]["reading_token"], first["voice"]["reading_token"])
        self.assertEqual(replay["voice"]["state"], "terminal")
        self.assertEqual(replay["voice"]["coverage"]["failed"], 1)
        self.assertEqual(replay["voice"]["coverage"]["blocked"], 0)

    def test_oversized_inline_transcripts_fall_back_to_the_reading_token(self) -> None:
        self.build(voice_count=2)
        self.read_page(projection="detail", limit=50, voice="auto")
        self.assertEqual(self.complete_transcripts(text="长" * 5_000), 2)
        warm = self.read_page(projection="detail", limit=50, voice="cached")

        sidecar = warm["voice"]
        self.assertEqual(sidecar["state"], "cached")
        self.assertEqual(sidecar["coverage"]["ready"], 2)
        self.assertFalse(sidecar["text_inline"])
        self.assertEqual(len(sidecar["items"]), 2)
        self.assertEqual([row[3] for row in sidecar["items"]], [None, None])
        self.assertEqual(sidecar["guide"], VOICE_READ_BUDGET_GUIDE)
        self.assertIsNotNone(sidecar["reading_token"])
        rendered = len(json.dumps(warm, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        self.assertLessEqual(rendered, self.service.reader.policy.max_detail_payload_chars)

    def test_unreadable_voice_rows_are_excluded_from_auto(self) -> None:
        self.build(voice_count=1, available=False)
        page = self.read_page(projection="detail", limit=50, voice="auto")
        sidecar = page["voice"]
        self.assertEqual(sidecar["state"], "unreadable")
        self.assertIsNone(sidecar["reading_token"])
        self.assertEqual(sidecar["selected_count"], 0)
        self.assertEqual(sidecar["excluded"], {"unreadable": 1, "unknown_revision": 0})
        self.assertEqual(sidecar["guide"], VOICE_READ_UNREADABLE_GUIDE)
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_transcript_completion_does_not_advance_message_state(self) -> None:
        self.build()
        page = self.read_page(projection="detail", limit=50, voice="auto")
        token = page["voice"]["reading_token"]

        def snapshot() -> dict[str, Any]:
            with self.repository.database.connection() as connection:
                return {
                    "update_cursors": [
                        tuple(row)
                        for row in connection.execute("SELECT * FROM reader_update_cursors")
                    ],
                    "timeline_cursors": [
                        tuple(row)
                        for row in connection.execute("SELECT * FROM reader_timeline_cursors")
                    ],
                    "deliveries": [
                        tuple(row) for row in connection.execute("SELECT * FROM reader_deliveries")
                    ],
                    "observations": int(
                        connection.execute(
                            "SELECT count(*) FROM message_observations"
                        ).fetchone()[0]
                    ),
                }

        before = snapshot()
        self.assertEqual(self.complete_transcripts(), 2)
        self.assertEqual(snapshot(), before)
        self.assertEqual(self.voice_service.read_batch(token, limit=8)["coverage"]["ready"], 2)

    def test_cropped_candidates_never_enter_the_batch(self) -> None:
        self.build(
            voice_count=CROPPED_VOICE_COUNT,
            policy=ReaderPolicy(
                mode="all_except_denylist",
                identity_debug=True,
                max_compact_payload_chars=COMPACT_BUDGET_POLICY,
            ),
        )
        cropped = self.read_page(projection="compact", limit=500, voice="auto")
        self.assertFalse(cropped["page"]["message_rows_complete"])
        delivered = self.delivered_voice_rows(cropped)
        self.assertLess(delivered, CROPPED_VOICE_COUNT)
        self.assertEqual(cropped["voice"]["selected_count"], delivered)
        self.assertEqual(cropped["voice"]["coverage"]["selected"], delivered)
        token = cropped["voice"]["reading_token"]
        self.assertEqual(len(self.batch_items(token)), delivered)
        self.assertEqual(self.counts()[0], 2)

    def test_compact_rows_stay_complete_with_a_sparse_sidecar(self) -> None:
        self.build(voice_count=CROPPED_VOICE_COUNT)
        page = self.read_page(projection="compact", limit=500, voice="auto")

        self.assertEqual(len(page["messages"]), 500)
        self.assertTrue(page["page"]["message_rows_complete"])
        rendered = len(json.dumps(page, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        self.assertLessEqual(rendered, self.service.reader.policy.max_compact_payload_chars)
        sidecar = page["voice"]
        self.assertEqual(sidecar["selected_count"], CROPPED_VOICE_COUNT)
        self.assertEqual(sidecar["coverage"]["pending"], 2)
        self.assertEqual(sidecar["coverage"]["not_scheduled"], CROPPED_VOICE_COUNT - 2)
        self.assertEqual(sidecar["items"], [])
        self.assertFalse(sidecar["items_complete"])
        self.assertLess(len(json.dumps(sidecar, ensure_ascii=False)), VOICE_SIDECAR_RESERVE_CHARS)
        self.assertEqual(self.counts()[0], 2)


class VoiceResourceTextTests(VoiceReadFixture, unittest.TestCase):
    def test_resource_text_reuses_the_message_batch_cache(self) -> None:
        self.build(voice_count=1)
        resource_id = self.voice_resource_id()
        pending = self.read_voice_text(resource_id)
        self.assertIsNone(pending.content_kind)
        self.assertEqual(pending.descriptor["derivation"]["kind"], "derived_transcript")
        self.assertEqual(pending.descriptor["transcript"]["state"], "pending")
        self.assertIsNotNone(pending.descriptor["transcript"]["reading_token"])
        self.assertIn("wechat_read_transcripts", pending.descriptor["transcript"]["guide"])
        jobs_before = self.counts()[0]
        self.assertEqual(jobs_before, 1)

        self.complete_transcripts(text="Derived transcript")
        ready = self.read_voice_text(resource_id)
        self.assertEqual(ready.content_kind, "text")
        self.assertTrue(str(ready.text).startswith("Derived transcript"))
        self.assertEqual(ready.mime_type, "text/plain")
        self.assertEqual(ready.descriptor["transcript"]["state"], "ready")
        self.assertEqual(
            ready.descriptor["media"],
            {"mime_type": "text/plain", "content_block_type": "text"},
        )
        self.assertEqual(ready.descriptor["resolution"]["variant"], "derived_transcript")
        self.assertEqual(ready.descriptor["warnings"], ["derived_transcript_source_not_reread"])
        self.assertEqual(ready.descriptor["returned"]["bytes"], len(str(ready.text).encode()))
        self.assertEqual(ready.descriptor["returned"]["chars"], len(str(ready.text)))
        self.assertEqual(self.counts()[0], jobs_before)

    def test_resource_text_reports_blocked_payloads(self) -> None:
        self.build(voice_count=1, available=False)
        payload = self.read_voice_text(self.voice_resource_id())
        self.assertIsNone(payload.content_kind)
        self.assertEqual(payload.descriptor["transcript"]["state"], "blocked")
        self.assertIn("voice_payload_unavailable", payload.descriptor["warnings"])
        self.assertIn("transcript_blocked", payload.descriptor["warnings"])
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_resource_text_shares_the_text_selector_rules_and_output_budgets(self) -> None:
        self.build(voice_count=1)
        resource_id = self.voice_resource_id()
        self.read_voice_text(resource_id)
        self.complete_transcripts(text="alpha\nbeta\ngamma")

        full = self.read_voice_text(resource_id)
        self.assertEqual(full.content_kind, "text")
        self.assertEqual(str(full.text), "alpha\nbeta\ngamma 0")
        self.assertEqual(
            full.descriptor["returned"],
            {
                "bytes": len(str(full.text).encode("utf-8")),
                "chars": len(str(full.text)),
                "truncated": False,
            },
        )

        windowed = self.service.read_resource(
            resource_id=resource_id,
            mode="text",
            page=None,
            start_line=2,
            end_line=2,
            max_bytes=4 * 1024 * 1024,
        )
        self.assertEqual(windowed.text, "beta")
        self.assertEqual(windowed.descriptor["line_range"], {"start": 2, "end": 2})
        self.assertEqual(
            windowed.descriptor["returned"],
            {"bytes": 4, "chars": 4, "truncated": False},
        )

        unsupported: tuple[dict[str, Any], ...] = (
            {"page": 1},
            {"member": "notes.txt"},
            {"sheet": "Budget"},
            {"cell_range": "A1:A1"},
            {"page": 1, "start_line": 1},
            {"start_line": 0},
            {"start_line": 2, "end_line": 1},
            {"start_line": 1, "end_line": 501},
        )
        for selectors in unsupported:
            with self.subTest(selectors=selectors):
                with self.assertRaises(SightglassError) as caught:
                    self.service.read_resource(
                        resource_id=resource_id,
                        mode="text",
                        page=selectors.get("page"),
                        start_line=selectors.get("start_line"),
                        end_line=selectors.get("end_line"),
                        member=selectors.get("member"),
                        sheet=selectors.get("sheet"),
                        cell_range=selectors.get("cell_range"),
                        max_bytes=4 * 1024 * 1024,
                    )
                self.assertEqual(caught.exception.code, ErrorCode.QUERY_INVALID)

        with self.assertRaises(SightglassError) as too_large:
            self.service.read_resource(
                resource_id=resource_id,
                mode="text",
                page=None,
                start_line=None,
                end_line=None,
                max_bytes=64 * 1024 * 1024,
            )
        self.assertEqual(too_large.exception.code, ErrorCode.QUERY_INVALID)

    def test_resource_text_truncates_on_a_valid_utf8_byte_boundary(self) -> None:
        self.build(voice_count=1)
        resource_id = self.voice_resource_id()
        self.read_voice_text(resource_id)
        self.complete_transcripts(text="语音转写")

        truncated = self.service.read_resource(
            resource_id=resource_id,
            mode="text",
            page=None,
            start_line=None,
            end_line=None,
            max_bytes=7,
        )
        self.assertEqual(truncated.content_kind, "text")
        self.assertEqual(truncated.text, "语音")
        self.assertEqual(
            truncated.descriptor["returned"],
            {"bytes": 6, "chars": 2, "truncated": True},
        )
        self.assertEqual(
            truncated.descriptor["returned"]["bytes"],
            len(str(truncated.text).encode("utf-8")),
        )

        envelope = self.tools.wechat_read_resource(
            resource_id=resource_id, mode="text", sheet="Budget"
        )
        self.assertTrue(envelope.isError)
        self.assertEqual((envelope.structuredContent or {})["code"], "QUERY_INVALID")

    def test_resource_text_is_unsupported_when_voice_is_disabled(self) -> None:
        self.build(voice_count=1, enabled=False)
        with self.assertRaises(SightglassError) as caught:
            self.read_voice_text(self.voice_resource_id())
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNSUPPORTED)
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_resource_text_does_not_hijack_other_kinds(self) -> None:
        self.build(voice_count=1)
        page = self.read_page(projection="detail", limit=50, voice="off")
        target = None
        for message in page["messages"]:
            listed = self.tools.wechat_list_resources(str(message["message_id"]))
            candidate = next(
                (item for item in listed["resources"] if item["kind"] == "file"), None
            )
            if candidate is not None and candidate["mime_type"] == "text/markdown":
                target = candidate
                break
        self.assertIsNotNone(target)
        assert target is not None
        payload = self.read_voice_text(str(target["resource_id"]))
        self.assertEqual(payload.content_kind, "text")
        self.assertNotIn("transcript", payload.descriptor)
        self.assertIn("first line", str(payload.text))
        self.assertEqual(self.counts(), (0, 0, 0))


class VoiceUpdatesTests(VoiceReadFixture, unittest.TestCase):
    def test_updates_freezes_the_sidecar_before_the_delivery(self) -> None:
        self.build()
        first = self.read_updates(projection="compact", limit=50, voice="auto")
        delivery_id = first["page"]["delivery_id"]
        self.assertIsNotNone(delivery_id)
        self.assertEqual(first["voice"]["state"], "prepared")
        self.assertEqual(first["voice"]["items"], [])
        token = first["voice"]["reading_token"]

        def cursors() -> list[tuple[Any, ...]]:
            with self.repository.database.connection() as connection:
                return [
                    tuple(row) for row in connection.execute("SELECT * FROM reader_update_cursors")
                ]

        cursors_before = cursors()
        self.assertEqual(self.complete_transcripts(text="Frozen transcript"), 2)
        self.assertEqual(cursors(), cursors_before)
        counts_before = self.counts()

        replay = self.read_updates(projection="compact", limit=50, voice="auto")
        self.assertEqual(replay, first)
        self.assertEqual(replay["voice"]["items"], [])
        self.assertFalse(replay["voice"]["text_inline"])
        self.assertEqual(self.counts(), counts_before)
        self.assertEqual(self.voice_service.read_batch(token, limit=8)["coverage"]["ready"], 2)

        acked = self.read_updates(
            projection="compact", limit=50, voice="auto", ack_delivery_id=str(delivery_id)
        )
        self.assertIsNone(
            self.repository.pending_delivery(
                self.service.reader.reader_id, self.group_id, "conversation", ""
            )
        )
        self.assertEqual(acked["messages"], [])
        self.assertEqual(self.counts(), counts_before)

    def test_failed_delivery_rolls_back_the_prepared_batch(self) -> None:
        self.build()
        with patch.object(
            self.repository,
            "create_pending_delivery",
            side_effect=SightglassError(ErrorCode.INTERNAL_ERROR),
        ):
            with self.assertRaises(SightglassError):
                self.service.read_messages(
                    mode="updates",
                    conversation_id=self.group_id,
                    projection="compact",
                    limit=50,
                    voice="auto",
                )
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_updates_detail_sidecar_matches_the_delivered_rows(self) -> None:
        self.build()
        page = self.read_updates(projection="detail", limit=50, voice="auto")
        self.assertEqual(page["schema"], "sightglass.message-page.v1")
        self.assertIsNotNone(page["page"]["delivery_id"])
        self.assertEqual(page["voice"]["coverage"]["selected"], 2)
        self.assertEqual(page["voice"]["state"], "prepared")
        self.assertEqual(self.delivered_voice_rows(page), 2)


class VoiceSidecarBudgetTests(unittest.TestCase):
    def test_minimal_sidecar_fits_the_reserved_budget(self) -> None:
        minimal = {
            "schema": "sightglass.voice-sidecar.v1",
            "policy": "auto",
            "state": "prepared",
            "derivation": {"kind": "derived_transcript"},
            "reading_token": "voice_" + "0" * 32,
            "expires_at": "2026-09-19T00:00:00.000000+00:00",
            "fields": ["message_id", "resource_id", "state", "text"],
            "items": [],
            "items_complete": True,
            "text_inline": False,
            "selected_count": 3,
            "coverage": {
                "selected": 3,
                "ready": 0,
                "pending": 3,
                "not_scheduled": 0,
                "blocked": 0,
                "failed": 0,
                "empty": 0,
                "cancelled": 0,
            },
            "excluded": {"unreadable": 0, "unknown_revision": 0},
            "guide": VOICE_READ_UNREADABLE_GUIDE,
        }
        rendered = len(
            json.dumps(minimal, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        )
        self.assertLess(rendered, VOICE_SIDECAR_RESERVE_CHARS)


if __name__ == "__main__":
    unittest.main()

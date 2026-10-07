from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing, contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

from sightglass.contracts.errors import SightglassError
from sightglass.reader.cursors import source_sort_key
from sightglass.source.base import SourceHealth
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


class MaterializedReadPlaneTests(unittest.TestCase):
    def test_narrow_neighbors_and_unchanged_snapshot_do_not_scan_history(self) -> None:
        epoch = self.service._projection_inventory_epoch()
        watermark = self.repository.observation_watermark()
        with self.repository.database.transaction() as connection:
            seed = dict(
                connection.execute(
                    "SELECT * FROM messages WHERE conversation_id=? AND kind='text' LIMIT 1",
                    (self.group,),
                ).fetchone()
            )
            columns = tuple(seed)
            values = []
            for index in range(10_000):
                row = dict(seed)
                instant = (
                    datetime(2025, 1, 1, tzinfo=UTC) + timedelta(seconds=index * 240)
                ).isoformat()
                row.update(
                    message_id=f"wxmsg_synthetic_seek_{index}",
                    source_message_id=f"synthetic-seek-{index}",
                    sent_at_utc=instant,
                    sort_primary=instant,
                    sort_seq=index,
                    sort_tie=index,
                )
                values.append(tuple(row[key] for key in columns))
                if index == 5000:
                    focus = row
            connection.executemany(
                f"INSERT INTO messages ({','.join(columns)}) VALUES "
                f"({','.join('?' for _ in columns)})",
                values,
            )
        instant = datetime.fromisoformat(focus["sent_at_utc"])
        steps = 0

        def bound_steps() -> int:
            nonlocal steps
            steps += 100
            return int(steps > 5000)

        with self.repository.database.read_snapshot() as connection:
            connection.set_progress_handler(bound_steps, 100)
            try:
                for direction in ("backward", "forward"):
                    rows = self.repository.materialized_message_rows(
                        self.group,
                        projection_epoch=epoch,
                        observation_watermark=watermark,
                        direction=direction,
                        limit=8,
                        time_after_utc=(instant - timedelta(seconds=180)).isoformat(),
                        time_before_utc=(instant + timedelta(seconds=180)).isoformat(),
                        before=source_sort_key(focus) if direction == "backward" else None,
                        after=source_sort_key(focus) if direction == "forward" else None,
                    )
                    self.assertEqual(rows, [])
                self.assertFalse(
                    self.repository.materialized_snapshot_changed(
                        self.group,
                        projection_epoch=epoch,
                        observation_watermark=watermark,
                    )
                )
            finally:
                connection.set_progress_handler(None, 0)
        self.assertLessEqual(steps, 5000)

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "source"
        self.window = Path(self.temporary.name) / "state" / "window.db"
        create_synthetic_source(self.root)
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            self.root, self.window, default_projection=None
        )
        self.tools.wechat_status()
        self.group = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        self.service.sync_source_once(initial_tail=100, conversation_limit=100)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _advance_generation(self, suffix: str) -> None:
        manifest_path = self.root / "source.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["shards"][1]["generation_id"] = f"generation-materialized-{suffix}"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def _append_message(self, source_message_id: str, text: str, rowid: int) -> None:
        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            connection.execute(
                """
                INSERT INTO messages(
                    source_message_id, source_conversation_id, source_time_raw,
                    sent_at_utc, observed_at_utc, sort_seq, source_rowid,
                    wechat_type, raw_content, is_outgoing, sender_internal_id,
                    sender_local_token, sender_surface_label, resources_json
                ) VALUES (?, 'conv_group', ?, ?, ?, 1, ?, 1, ?, 0,
                          'wxid_demo_member', NULL, '原账号昵称', '[]')
                """,
                (
                    source_message_id,
                    "2026-09-13T12:00:00+00:00",
                    "2026-09-13T12:00:00+00:00",
                    "2026-09-13T12:00:01+00:00",
                    rowid,
                    f"wxid_demo_member:\n{text}",
                ),
            )
            connection.commit()
        self._advance_generation(source_message_id)

    @staticmethod
    def _error_code(call) -> str:
        try:
            call()
        except SightglassError as exc:
            return exc.code.value
        raise AssertionError("expected SightglassError")

    def test_warm_message_read_uses_window_db_when_provider_snapshot_is_unavailable(self) -> None:
        arguments = {
            "mode": "recent",
            "conversation_id": self.group,
            "limit": 2,
            "projection": "detail",
        }
        self.assertTrue(self.service.local_message_read_ready(arguments))
        with patch.object(
            self.provider,
            "snapshot",
            side_effect=AssertionError("local read opened provider snapshot"),
        ):
            page = self.service.read_messages(**arguments)
        self.assertEqual(page["source_receipt"]["served_from"], "window_db")
        self.assertFalse(page["source_receipt"]["freshness"]["live_refresh_confirmed"])
        self.assertTrue(page["messages"])

        degraded = SourceHealth(
            configured=True,
            available=False,
            account_count=1,
            source_state="partial",
            fresh_as_of="2026-09-13T12:00:00+00:00",
            inventory_digest="unavailable",
            generation_set_digest="unavailable",
            shard_counts={},
            warnings=("synthetic_live_source_unavailable",),
        )
        with patch.object(self.provider, "health", return_value=degraded):
            status = self.service.status()
        self.assertTrue(status["ready"])
        self.assertEqual(status["readiness"]["indexed_reads"], "ready")
        self.assertEqual(status["readiness"]["live_refresh"], "degraded")

    def test_live_refresh_readiness_probe_seeks_the_resident_timeline(self) -> None:
        statements: list[str] = []
        database = self.repository.database
        open_connection = database.connection

        @contextmanager
        def traced_connection():
            with open_connection() as connection:
                connection.set_trace_callback(statements.append)
                yield connection

        projection_epoch = self.service._projection_inventory_epoch()
        with patch.object(database, "connection", traced_connection):
            self.assertTrue(
                self.repository.has_materialized_read_plane(projection_epoch)
            )
        statement = next(
            statement
            for statement in statements
            if "FROM conversations AS c" in statement
        )
        with open_connection() as connection:
            plan_rows = connection.execute(
                "EXPLAIN QUERY PLAN " + statement
            ).fetchall()
        plan = " | ".join(str(row[-1]) for row in plan_rows)
        self.assertIn("message_resident_timeline", plan)
        self.assertNotIn("SCAN m", plan)

    def test_materialized_cursor_ignores_append_but_stales_on_existing_row_change(self) -> None:
        first = self.service.read_messages(
            mode="recent",
            conversation_id=self.group,
            limit=1,
            projection="detail",
        )
        cursor = first["page"]["next_cursor"]
        self.assertIsNotNone(cursor)
        self._append_message("source-msg-materialized-append", "新追加", 9001)
        self.service.sync_source_once(initial_tail=100, conversation_limit=100)

        with patch.object(
            self.provider,
            "snapshot",
            side_effect=AssertionError("cursor continuation opened provider snapshot"),
        ):
            continued = self.service.read_messages(
                mode="recent",
                conversation_id=self.group,
                limit=1,
                projection="detail",
                cursor=cursor,
            )
        self.assertEqual(continued["source_receipt"]["served_from"], "window_db")
        self.assertNotIn(
            "新追加", [item["text"] for item in continued["messages"]]
        )

        first_again = self.service.read_messages(
            mode="recent",
            conversation_id=self.group,
            limit=1,
            projection="detail",
        )
        changed_cursor = first_again["page"]["next_cursor"]
        boundary = self.repository.message_position_row(
            first_again["messages"][0]["message_id"]
        )
        assert boundary is not None
        with self.repository.database.transaction() as connection:
            inserted = connection.execute(
                """
                INSERT INTO message_observations(
                    observation_id, message_id, observed_at, source_generation_id,
                    state, payload_digest, parsed_json, parser_version,
                    raw_payload_ref, reason_code
                ) VALUES (?, ?, ?, ?, 'present', ?, ?, ?, NULL, 'synthetic-correction')
                """,
                (
                    "wxobservation_synthetic_correction",
                    str(boundary["message_id"]),
                    "2026-09-13T12:01:00+00:00",
                    "generation-synthetic-correction",
                    "digest-synthetic-correction",
                    "{}",
                    "synthetic-parser-correction",
                ),
            )
            assert inserted.lastrowid is not None
            connection.execute(
                "UPDATE messages SET current_observation_seq = ? WHERE message_id = ?",
                (int(inserted.lastrowid), str(boundary["message_id"])),
            )
        self.assertEqual(
            self._error_code(
                lambda: self.service.read_messages(
                    mode="recent",
                    conversation_id=self.group,
                    limit=1,
                    projection="detail",
                    cursor=changed_cursor,
                )
            ),
            "CURSOR_STALE",
        )

    def test_all_non_update_message_modes_use_the_materialized_plane(self) -> None:
        recent = self.service.read_messages(
            mode="recent",
            conversation_id=self.group,
            limit=20,
            projection="detail",
        )
        target = next(
            item for item in recent["messages"] if item["sender"]["participant_id"]
        )
        participant_id = str(target["sender"]["participant_id"])
        calls = (
            {
                "mode": "message",
                "message_id": target["message_id"],
                "projection": "detail",
            },
            {
                "mode": "context",
                "anchor": target["anchor"],
                "before": 1,
                "after": 1,
                "limit": 3,
                "projection": "detail",
            },
            {
                "mode": "range",
                "conversation_id": self.group,
                "time_after": "2020-01-01T00:00:00+00:00",
                "limit": 2,
                "projection": "detail",
            },
            {
                "mode": "speaker",
                "conversation_id": self.group,
                "participant_ids": (participant_id,),
                "speaker_view": "with_context",
                "before": 1,
                "after": 1,
                "limit": 3,
                "projection": "detail",
            },
        )
        with patch.object(
            self.provider,
            "snapshot",
            side_effect=AssertionError("materialized mode opened provider snapshot"),
        ):
            results = [self.service.read_messages(**arguments) for arguments in calls]
        self.assertTrue(
            all(
                result["source_receipt"]["served_from"] == "window_db"
                for result in results
            )
        )

    def test_exact_admission_reads_without_tail_state_and_preserves_policy(self) -> None:
        recent = self.service.read_messages(
            mode="recent", conversation_id=self.group, limit=2, projection="detail"
        )
        target = recent["messages"][0]
        target_row = self.repository.message_position_row(target["message_id"])
        assert target_row is not None
        with self.repository.database.transaction() as connection:
            connection.execute(
                "DELETE FROM source_conversation_state",
            )
            connection.execute(
                "UPDATE messages SET projection_epoch = 'old' WHERE message_id != ?",
                (target["message_id"],),
            )
        calls = (
            {"mode": "message", "message_id": target["message_id"]},
            {"mode": "context", "message_id": target["message_id"]},
            {"mode": "context", "anchor": target["anchor"]},
        )
        with patch.object(
            self.provider, "snapshot", side_effect=AssertionError("admitted target opened source")
        ):
            for call in calls:
                arguments = {**call, "projection": "detail"}
                if call["mode"] == "context":
                    arguments.update(before=1, after=1, limit=3)
                self.assertTrue(self.service.local_message_read_ready(arguments))
                page = self.service.read_messages(**arguments)
                self.assertEqual(
                    [item["message_id"] for item in page["messages"]], [target["message_id"]]
                )
                receipt = page["source_receipt"]
                self.assertFalse(receipt["complete"])
                self.assertEqual(receipt["coverage"]["conversation"], "indexed")
                self.assertEqual(
                    receipt["coverage"]["observed_time_after"], target_row["sent_at_utc"]
                )
                self.assertEqual(
                    receipt["coverage"]["observed_time_before"], target_row["sent_at_utc"]
                )
                self.assertIn("has_more_describes_admitted_messages", receipt["coverage"]["notes"])
                self.assertFalse(receipt["freshness"]["live_refresh_confirmed"])
                self.assertFalse(page["page"]["has_more_before"])
                self.assertFalse(page["page"]["has_more_after"])
        self.assertIsNone(self.repository.source_conversation_state(self.group))
        with self.repository.database.connection() as connection:
            self.assertIsNone(
                connection.execute(
                    "SELECT 1 FROM source_conversation_state AS s "
                    "WHERE s.source_inventory_epoch = ? AND EXISTS ("
                    "SELECT 1 FROM messages AS m WHERE m.conversation_id = s.conversation_id "
                    "AND m.projection_epoch = ? AND m.current_state = 'present') LIMIT 1",
                    (self.service._projection_inventory_epoch(),) * 2,
                ).fetchone()
            )
        degraded = SourceHealth(
            configured=True,
            available=False,
            account_count=1,
            source_state="partial",
            fresh_as_of="2026-09-13T12:00:00+00:00",
            inventory_digest="unavailable",
            generation_set_digest="unavailable",
            shard_counts={},
        )
        with patch.object(self.provider, "health", return_value=degraded):
            status = self.service.status()
        self.assertTrue(status["ready"])
        self.assertTrue(status["read_plane"]["local_message_reads"])
        self.assertEqual(status["readiness"]["indexed_reads"], "ready")
        self.assertFalse(
            self.service.local_message_read_ready({"mode": "recent", "conversation_id": self.group})
        )

        self.service.reader.policy = replace(
            self.service.reader.policy, mode="allowlist", allowed_conversation_ids=frozenset()
        )
        arguments = {"mode": "message", "message_id": target["message_id"]}
        self.assertTrue(self.service.local_message_read_ready(arguments))
        with patch.object(
            self.provider, "snapshot", side_effect=AssertionError("policy denial opened source")
        ):
            self.assertEqual(
                self._error_code(lambda: self.service.read_messages(**arguments)), "POLICY_DENIED"
            )

    def test_invalid_or_missing_local_targets_are_local_errors(self) -> None:
        target = self.service.read_messages(
            mode="recent", conversation_id=self.group, projection="detail", limit=1
        )["messages"][0]
        for arguments, code in (
            (
                {
                    "mode": "context",
                    "anchor": target["anchor"],
                    "conversation_id": "wxconv_synthetic_wrong_scope",
                    "before": 0,
                    "after": 0,
                    "limit": 1,
                },
                "CURSOR_INVALID",
            ),
            (
                {
                    "mode": "context",
                    "anchor": "synthetic-invalid-anchor",
                    "before": 0,
                    "after": 0,
                    "limit": 1,
                },
                "CURSOR_INVALID",
            ),
            ({"mode": "message", "message_id": "wxmsg_synthetic_unobserved"}, "MESSAGE_NOT_FOUND"),
            (
                {
                    "mode": "recent",
                    "conversation_id": self.group,
                    "cursor": "synthetic-invalid-cursor",
                },
                "CURSOR_INVALID",
            ),
        ):
            with self.subTest(code=code, arguments=arguments):
                self.assertTrue(self.service.local_message_read_ready(arguments))
                with patch.object(
                    self.provider,
                    "snapshot",
                    side_effect=AssertionError("local error opened source"),
                ):
                    self.assertEqual(
                        self._error_code(
                            lambda: self.service.read_messages(**cast(dict[str, Any], arguments))
                        ),
                        code,
                    )

    def test_refresh_rejects_cursor_updates_and_non_boolean_arguments(self) -> None:
        for arguments in (
            {"mode": "recent", "conversation_id": self.group, "refresh": "true"},
            {"mode": "updates", "conversation_id": self.group, "refresh": True},
            {
                "mode": "recent",
                "conversation_id": self.group,
                "cursor": "synthetic-invalid-cursor",
                "refresh": True,
            },
        ):
            with self.subTest(arguments=arguments):
                with patch.object(
                    self.provider,
                    "snapshot",
                    side_effect=AssertionError("invalid refresh opened source"),
                ):
                    self.assertEqual(
                        self._error_code(lambda: self.service.read_messages(**arguments)),
                        "QUERY_INVALID",
                    )

    def test_materialized_cursor_is_bound_to_policy_and_projection_epoch(self) -> None:
        page = self.service.read_messages(
            mode="recent",
            conversation_id=self.group,
            limit=1,
            projection="detail",
        )
        cursor = page["page"]["next_cursor"]
        assert cursor is not None
        self.service.reader.policy = replace(
            self.service.reader.policy,
            max_detail_payload_chars=self.service.reader.policy.max_detail_payload_chars - 1,
        )
        self.assertEqual(
            self._error_code(
                lambda: self.service.read_messages(
                    mode="recent",
                    conversation_id=self.group,
                    limit=1,
                    projection="detail",
                    cursor=cursor,
                )
            ),
            "CURSOR_STALE",
        )
        self.service.reader.policy = replace(
            self.service.reader.policy,
            max_detail_payload_chars=self.service.reader.policy.max_detail_payload_chars + 1,
        )

        correction_page = self.service.read_messages(
            mode="recent",
            conversation_id=self.group,
            limit=1,
            projection="detail",
        )
        correction_cursor = correction_page["page"]["next_cursor"]
        assert correction_cursor is not None
        with self.repository.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO identity_corrections(
                    correction_id, action, subject_json, reason, created_at,
                    operator_identity, supersedes_correction_id
                ) VALUES (?, 'set_alias', '{}', 'synthetic cursor revision', ?, 'fixture', NULL)
                """,
                (
                    "wxcorrection_materialized_cursor",
                    "2026-09-13T12:02:00+00:00",
                ),
            )
        self.assertEqual(
            self._error_code(
                lambda: self.service.read_messages(
                    mode="recent",
                    conversation_id=self.group,
                    limit=1,
                    projection="detail",
                    cursor=correction_cursor,
                )
            ),
            "CURSOR_STALE",
        )

        projection_page = self.service.read_messages(
            mode="recent",
            conversation_id=self.group,
            limit=1,
            projection="detail",
        )
        projection_cursor = projection_page["page"]["next_cursor"]
        assert projection_cursor is not None
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE source_conversation_state SET source_inventory_epoch = 'stale' "
                "WHERE conversation_id = ?",
                (self.group,),
            )
        self.assertEqual(
            self._error_code(
                lambda: self.service.read_messages(
                    mode="recent",
                    conversation_id=self.group,
                    limit=1,
                    projection="detail",
                    cursor=projection_cursor,
                )
            ),
            "CURSOR_STALE",
        )

    def test_warm_resource_read_is_cache_only_and_revocation_still_denies(self) -> None:
        with self.repository.database.connection() as connection:
            row = connection.execute(
                "SELECT resource_id FROM resources "
                "WHERE availability IN ('local_available', 'archive_available') "
                "ORDER BY resource_id LIMIT 1"
            ).fetchone()
        assert row is not None
        resource_id = str(row["resource_id"])
        cold = self.service.read_resource(
            resource_id=resource_id,
            mode="metadata",
            page=None,
            start_line=None,
            end_line=None,
            max_bytes=4 * 1024 * 1024,
        )
        self.assertEqual(cold.descriptor["source_receipt"]["freshness"]["mode"], "live_source")
        with patch.object(
            self.provider,
            "snapshot",
            side_effect=AssertionError("warm resource read opened provider snapshot"),
        ):
            warm = self.service.read_resource(
                resource_id=resource_id,
                mode="metadata",
                page=None,
                start_line=None,
                end_line=None,
                max_bytes=4 * 1024 * 1024,
            )
        self.assertEqual(warm.descriptor["source_receipt"]["freshness"]["mode"], "local_cache")

        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE resources SET resolver_json = ? WHERE resource_id = ?",
                ('{"active":false,"binding_fingerprint":"revoked"}', resource_id),
            )
        self.assertEqual(
            self._error_code(
                lambda: self.service.read_resource(
                    resource_id=resource_id,
                    mode="metadata",
                    page=None,
                    start_line=None,
                    end_line=None,
                    max_bytes=4 * 1024 * 1024,
                )
            ),
            "RESOURCE_UNAVAILABLE",
        )

    def test_warm_resource_rechecks_cas_integrity_and_resolver_revision(self) -> None:
        with self.repository.database.connection() as connection:
            row = connection.execute(
                "SELECT resource_id FROM resources "
                "WHERE availability IN ('local_available', 'archive_available') "
                "ORDER BY resource_id LIMIT 1"
            ).fetchone()
        assert row is not None
        resource_id = str(row["resource_id"])
        arguments = {
            "resource_id": resource_id,
            "mode": "metadata",
            "page": None,
            "start_line": None,
            "end_line": None,
            "max_bytes": 4 * 1024 * 1024,
        }
        self.service.read_resource(**arguments)
        binding = self.repository.resource_binding(resource_id, "original")
        assert binding is not None
        object_path = Path(str(binding["local_path_internal"]))
        original = object_path.read_bytes()
        object_path.write_bytes(b"corrupt")
        try:
            self.assertEqual(
                self._error_code(lambda: self.service.read_resource(**arguments)),
                "RESOURCE_BLOCKED",
            )
        finally:
            object_path.write_bytes(original)

        resource_service = self.service.resource_service
        resolve = resource_service._resolve_payload

        def change_revision(*args, **kwargs):
            payload = resolve(*args, **kwargs)
            with self.repository.database.transaction() as connection:
                connection.execute(
                    "UPDATE resources SET resolver_json = ? WHERE resource_id = ?",
                    ('{"active":true,"binding_fingerprint":"changed"}', resource_id),
                )
            return payload

        with patch.object(resource_service, "_resolve_payload", side_effect=change_revision):
            self.assertEqual(
                self._error_code(lambda: self.service.read_resource(**arguments)),
                "SOURCE_GENERATION_CHANGED",
            )

        replacement, replacement_path = resource_service.cache.put(
            b"synthetic replacement binding",
            mime_type="application/octet-stream",
            origin="synthetic_test",
        )

        def change_binding(*args, **kwargs):
            payload = resolve(*args, **kwargs)
            with self.repository.database.transaction():
                self.repository.upsert_resource_object(
                    object_digest=replacement.digest,
                    local_path_internal=replacement_path,
                    mime_type=replacement.mime_type,
                    byte_size=len(replacement.data),
                    origin=replacement.origin,
                    observed_at="2026-09-13T12:03:00+00:00",
                )
                self.repository.bind_resource_object(
                    resource_id=resource_id,
                    object_digest=replacement.digest,
                    variant="original",
                    created_at="2026-09-13T12:03:00+00:00",
                )
            return payload

        with patch.object(resource_service, "_resolve_payload", side_effect=change_binding):
            self.assertEqual(
                self._error_code(lambda: self.service.read_resource(**arguments)),
                "SOURCE_GENERATION_CHANGED",
            )


if __name__ == "__main__":
    unittest.main()

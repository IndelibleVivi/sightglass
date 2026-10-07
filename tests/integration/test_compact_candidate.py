"""Frozen-input, real WindowDB candidate and durable batch interruption checks."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from typing import Any

from sightglass.model.compact_candidate import (
    CompactCandidateError,
    build_candidate,
    capability_probe,
    file_revision,
    freeze_input,
    preview_stock_release,
    read_legacy_preview,
    verify_candidate,
)
from sightglass.model.db import WindowDB
from sightglass.model.lexical import LEXICAL_RECIPE
from sightglass.model.observation_codec import decode_observation_bytes
from sightglass.residency.repository import ResidencyRepository
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


class CompactFixture:
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.window = root / "state" / "window.db"
        source = create_synthetic_source(root / "source")
        _, self.repository, self.service, self.tools = build_test_stack(source, self.window)
        assert self.tools.wechat_status()["ready"]
        self.service.sync_source_once(initial_tail=100, conversation_limit=100)
        with self.repository.database.transaction() as connection:
            for row in connection.execute(
                "SELECT observation_seq,parsed_json FROM message_observations"
            ).fetchall():
                connection.execute(
                    "UPDATE message_observations SET parsed_json=? WHERE observation_seq=?",
                    (decode_observation_bytes(row[1]).decode(), row[0]),
                )
            connection.execute(
                "UPDATE sqlite_sequence SET seq=seq+1000 WHERE name='message_observations'"
            )
        self.workspace = root / "compact"
        self.frozen = self.workspace / "frozen.db"
        self.candidate = self.workspace / "candidate.db"
        self.kwargs: dict[str, Any] = {"workspace_budget_bytes": 64 * 1024**2, "min_free_bytes": 0}

    def tearDown(self):
        self.temp.cleanup()

    def freeze(self):
        return freeze_input(self.window, self.workspace, **self.kwargs)


class CompactCandidateTests(CompactFixture, unittest.TestCase):
    def test_preview_is_metadata_only_read_only(self):
        before = file_revision(self.window)
        report = read_legacy_preview(self.window)
        self.assertIsNone(report["message_count"])
        self.assertIsNone(report["observation_count"])
        self.assertEqual(before, file_revision(self.window))
        with closing(sqlite3.connect(":memory:")) as probe:
            self.assertTrue(capability_probe(probe)["usable"])

    def test_freeze_copy_cut_resumes_the_same_recovery_input(self):
        def interrupted(phase):
            if phase == "freeze_copied":
                raise RuntimeError("synthetic freeze cut")

        with self.assertRaises(RuntimeError):
            freeze_input(self.window, self.workspace, fault=interrupted, **self.kwargs)
        original = self.frozen.read_bytes()
        receipt = self.freeze()
        self.assertEqual(original, self.frozen.read_bytes())
        self.assertEqual(receipt["frozen_bytes"], len(original))

    def test_final_candidate_retires_build_state_and_checks_durable_rows(self):
        self.freeze()
        build_candidate(self.frozen, self.candidate, **self.kwargs)
        with closing(sqlite3.connect(self.candidate)) as connection:
            self.assertFalse(
                connection.execute(
                    "SELECT name FROM sqlite_schema WHERE name LIKE 'compact_%'"
                ).fetchall()
            )
            connection.execute("UPDATE accounts SET current_display_name='Synthetic tamper'")
            connection.commit()
        with self.assertRaisesRegex(CompactCandidateError, "durable rows changed"):
            verify_candidate(self.frozen, self.candidate)

    def test_schema9_offline_scope_preview_and_protected_stock(self):
        from sightglass.model.compact_candidate import preview_release

        with closing(sqlite3.connect(self.window)) as connection:
            connection.execute("PRAGMA foreign_keys=OFF")
            for table in (
                "read_lease_message",
                "read_lease",
                "body_release_jobs",
                "message_body_residency",
                "residency_totals",
                "residency_state",
                "conversation_residency",
                "residency_settings",
            ):
                connection.execute(f"DROP TABLE {table}")
            connection.execute("DROP INDEX IF EXISTS message_resident_timeline")
            connection.execute("ALTER TABLE messages DROP COLUMN body_available")
            connection.execute("PRAGMA user_version=9")
            connection.commit()
            group = connection.execute("SELECT conversation_id FROM messages LIMIT 1").fetchone()[0]
        before = self.window.read_bytes()
        preview = preview_release(self.window, group, limit=1)
        self.assertEqual(before, self.window.read_bytes())
        self.assertEqual(preview["examined_messages"], 1)
        self.freeze()
        build_candidate(self.frozen, self.candidate, **self.kwargs)
        with closing(sqlite3.connect(self.candidate)) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0],
                connection.execute(
                    "SELECT SUM(message_count) FROM residency_totals WHERE owner='protected'"
                ).fetchone()[0],
            )
        stock = preview_stock_release(self.window)["plan"]
        released = self.workspace / "released-schema9.db"
        build_candidate(self.frozen, released, release_plans=[stock], **self.kwargs)
        self.assertTrue(verify_candidate(self.frozen, released, release_plans=[stock])["verified"])
        with closing(sqlite3.connect(released)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM messages WHERE body_available=1"
                ).fetchone()[0],
                0,
            )

    def test_exact_recovery_candidate_sequence_and_canonical_recipe(self):
        freeze = self.freeze()
        report = build_candidate(self.frozen, self.candidate, **self.kwargs)
        self.assertEqual(report["frozen_digest"], freeze["frozen_digest"])
        self.assertGreater(report["encoded"]["encoded_text_observations"], 0)
        self.assertTrue(verify_candidate(self.frozen, self.candidate)["verified"])
        with closing(sqlite3.connect(self.candidate)) as connection:
            ddl = connection.execute(
                "SELECT sql FROM sqlite_schema WHERE name='message_lexical'"
            ).fetchone()[0]
            self.assertIn("contentless_delete=1", ddl)
            self.assertNotIn("columnsize=0", ddl)
            self.assertEqual(
                connection.execute(
                    "SELECT recipe FROM derived_index_state WHERE index_kind='lexical'"
                ).fetchone()[0],
                LEXICAL_RECIPE,
            )
        WindowDB(self.candidate)

    def test_interrupted_committed_copy_batch_resumes_exactly(self):
        self.freeze()

        def interrupted(phase):
            if phase == "copy_batch":
                raise RuntimeError("synthetic crash after committed batch")

        with self.assertRaises(RuntimeError):
            build_candidate(self.frozen, self.candidate, fault=interrupted, **self.kwargs)
        with closing(sqlite3.connect(self.candidate)) as connection:
            self.assertTrue(connection.execute("SELECT * FROM compact_progress").fetchone())
        build_candidate(self.frozen, self.candidate, **self.kwargs)
        self.assertTrue(verify_candidate(self.frozen, self.candidate)["verified"])

    def test_interrupted_lexical_batch_resumes_without_duplicates(self):
        self.freeze()

        def interrupted(phase):
            if phase == "lexical_batch":
                raise RuntimeError("synthetic lexical crash")

        with self.assertRaises(RuntimeError):
            build_candidate(self.frozen, self.candidate, fault=interrupted, **self.kwargs)
        build_candidate(self.frozen, self.candidate, **self.kwargs)
        self.assertTrue(verify_candidate(self.frozen, self.candidate)["verified"])

    def _legacy_duplicate_copies(self, limit: int = 4) -> list[str]:
        """Give ordinary text rows the historical triple body representation."""

        with self.repository.database.transaction() as connection:
            rows = connection.execute(
                "SELECT message_id,text,structured_json FROM messages "
                "WHERE kind='text' AND text IS NOT NULL LIMIT ?",
                (limit,),
            ).fetchall()
            for row in rows:
                parsed = json.loads(row["structured_json"])
                parsed["text"] = row["text"]
                connection.execute(
                    "UPDATE messages SET structured_json=?,search_text=? WHERE message_id=?",
                    (json.dumps(parsed, sort_keys=True), row["text"], row["message_id"]),
                )
        return [row["message_id"] for row in rows]

    def test_candidate_normalizes_duplicate_body_and_preserves_identity(self):
        legacy_ids = self._legacy_duplicate_copies()
        with self.repository.database.connection() as connection:
            before = {
                message_id: dict(
                    connection.execute(
                        "SELECT rowid,text,current_observation_seq FROM messages "
                        "WHERE message_id=?",
                        (message_id,),
                    ).fetchone()
                )
                for message_id in legacy_ids
            }
        self.freeze()
        build_candidate(self.frozen, self.candidate, **self.kwargs)
        self.assertTrue(verify_candidate(self.frozen, self.candidate)["verified"])
        with closing(sqlite3.connect(self.candidate)) as connection:
            for message_id in legacy_ids:
                row = connection.execute(
                    "SELECT rowid,text,search_text,structured_json,current_observation_seq "
                    "FROM messages WHERE message_id=?",
                    (message_id,),
                ).fetchone()
                self.assertEqual(row[0], before[message_id]["rowid"])
                self.assertEqual(row[1], before[message_id]["text"])
                self.assertIsNone(row[2], "exact duplicate search_text is normalized away")
                self.assertNotIn("text", json.loads(row[3]))
                self.assertEqual(row[4], before[message_id]["current_observation_seq"])

    def test_candidate_normalization_is_resumable(self):
        self._legacy_duplicate_copies()
        self.freeze()

        def interrupted(phase):
            if phase == "lexical_batch":
                raise RuntimeError("synthetic lexical crash")

        with self.assertRaises(RuntimeError):
            build_candidate(self.frozen, self.candidate, fault=interrupted, **self.kwargs)
        build_candidate(self.frozen, self.candidate, **self.kwargs)
        self.assertTrue(verify_candidate(self.frozen, self.candidate)["verified"])

    def test_candidate_resume_rejects_another_body_transformation(self):
        self.freeze()

        def interrupted(phase):
            if phase == "lexical_batch":
                raise RuntimeError("synthetic lexical crash")

        with self.assertRaises(RuntimeError):
            build_candidate(self.frozen, self.candidate, fault=interrupted, **self.kwargs)
        with closing(sqlite3.connect(self.candidate)) as connection:
            connection.execute("UPDATE compact_identity SET body_version='synthetic-old-version'")
            connection.commit()
        before = self.candidate.read_bytes()
        with self.assertRaisesRegex(CompactCandidateError, "transformation changed"):
            build_candidate(self.frozen, self.candidate, **self.kwargs)
        self.assertEqual(self.candidate.read_bytes(), before)

    def test_completed_candidate_requires_its_recorded_body_transformation(self):
        self.freeze()
        build_candidate(self.frozen, self.candidate, **self.kwargs)
        manifest = self.candidate.with_suffix(self.candidate.suffix + ".json")
        receipt = json.loads(manifest.read_text())
        receipt.pop("current_body_version")
        manifest.write_text(json.dumps(receipt))
        before = self.candidate.read_bytes()
        with self.assertRaisesRegex(CompactCandidateError, "transformation changed"):
            build_candidate(self.frozen, self.candidate, **self.kwargs)
        self.assertEqual(self.candidate.read_bytes(), before)

    def test_candidate_conflicting_legacy_body_fails_closed(self):
        legacy_ids = self._legacy_duplicate_copies(limit=1)
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET structured_json=? WHERE message_id=?",
                (json.dumps({"kind": "text", "text": "synthetic different body"}), legacy_ids[0]),
            )
        self.freeze()
        with self.assertRaisesRegex(
            CompactCandidateError, "legacy current-body representation conflict"
        ):
            build_candidate(self.frozen, self.candidate, **self.kwargs)

    def test_selected_release_is_encoded_during_copy(self):
        group = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        plan = ResidencyRepository(self.repository.database).stock_preview(group)["plan"]
        self.freeze()
        build_candidate(self.frozen, self.candidate, release_plans=[plan], **self.kwargs)
        self.assertTrue(
            verify_candidate(self.frozen, self.candidate, release_plans=[plan])["verified"]
        )
        with closing(sqlite3.connect(self.candidate)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM messages WHERE conversation_id=? AND body_available=1",
                    (group,),
                ).fetchone()[0],
                0,
            )
        self.assertGreater(len(self.repository.current_message_ids((group,))), 0)

    def test_whole_stock_release_keeps_exact_active_dependencies_and_identities(self):
        with self.repository.database.connection() as connection:
            resource = connection.execute("SELECT * FROM resources LIMIT 1").fetchone()
        self.repository.insert_resource_job(
            job_id="synthetic-stock-pin",
            resource_id=resource["resource_id"],
            resource_revision="synthetic",
            recipe_digest="synthetic",
            recipe_json="{}",
            created_at="2026-01-01T00:00:00+00:00",
        )
        before = file_revision(self.window)
        preview = preview_stock_release(self.window)
        self.assertEqual(file_revision(self.window), before)
        plan = preview["plan"]
        self.assertGreater(plan["releasable_messages"], 0)
        self.assertEqual(plan["pinned_messages"], 1)
        self.assertNotIn("entries", plan)
        self.assertLess(len(json.dumps(plan)), 2000)
        self.freeze()
        build_candidate(self.frozen, self.candidate, release_plans=[plan], **self.kwargs)
        self.assertTrue(
            verify_candidate(self.frozen, self.candidate, release_plans=[plan])["verified"]
        )
        with closing(sqlite3.connect(self.candidate)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT message_id FROM messages WHERE body_available=1"
                ).fetchall(),
                [(resource["message_id"],)],
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0],
                plan["examined_messages"],
            )

    def test_whole_stock_preview_cannot_release_a_later_snapshot(self):
        plan = preview_stock_release(self.window)["plan"]
        with self.repository.database.transaction() as connection:
            connection.execute("UPDATE accounts SET current_display_name='Synthetic later'")
        self.freeze()
        with self.assertRaisesRegex(CompactCandidateError, "frozen identity changed"):
            build_candidate(self.frozen, self.candidate, release_plans=[plan], **self.kwargs)

    def test_whole_stock_plan_tampering_fails_closed(self):
        plan = preview_stock_release(self.window)["plan"]
        plan["pinned_messages"] += 1
        self.freeze()
        with self.assertRaisesRegex(CompactCandidateError, "release plan changed"):
            build_candidate(self.frozen, self.candidate, release_plans=[plan], **self.kwargs)

    def test_existing_unowned_candidate_and_input_change_fail_closed(self):
        self.freeze()
        self.candidate.write_bytes(b"unowned artifact")
        self.candidate.chmod(0o600)
        with self.assertRaises(sqlite3.DatabaseError):
            build_candidate(self.frozen, self.candidate, **self.kwargs)
        self.assertEqual(self.candidate.read_bytes(), b"unowned artifact")
        expected = file_revision(self.frozen)
        with closing(sqlite3.connect(self.frozen)) as connection:
            connection.execute("UPDATE accounts SET current_display_name='Synthetic changed'")
            connection.commit()
        with self.assertRaises(CompactCandidateError):
            build_candidate(
                self.frozen, self.workspace / "second.db", expected_identity=expected, **self.kwargs
            )

    def test_legacy_startup_refuses_before_any_snapshot_or_ddl(self):
        with closing(sqlite3.connect(self.window)) as connection:
            connection.execute("PRAGMA user_version=9")
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        before = self.window.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "stopped-only"):
            WindowDB(self.window)
        self.assertEqual(self.window.read_bytes(), before)
        self.assertFalse(list(self.window.parent.glob("window.db.v9.backup*")))


if __name__ == "__main__":
    unittest.main()

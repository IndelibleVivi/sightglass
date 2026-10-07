"""Offline regression tests for the durable Cloudflare semantic lane.

Every test uses a synthetic local source and a synthetic in-memory backend. No
test performs real network egress, reads an account, or touches a real index.
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.semantic.cloudflare import IndexConfig, MetadataIndex
from sightglass.semantic.service import (
    ACTIVE_DIMENSIONS,
    ENCODE_BATCH_LIMIT,
    SemanticService,
    _conversation_digest,
    _encoder_input,
    _namespace,
    _remote_id,
)
from sightglass.semantic.settings import SemanticSettings
from sightglass.source.parser import parse_message
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


def _vector(seed: float) -> tuple[float, ...]:
    return tuple(math.sin(seed + index * 0.01) for index in range(ACTIVE_DIMENSIONS))


class FakeBackend:
    """Synthetic Vectorize/Workers-AI double; records text/ids, never egresses."""

    def __init__(self) -> None:
        self.stored: dict[str, dict] = {}
        self.upsert_calls: list[list[dict]] = []
        self.encode_calls: list[tuple[str, ...]] = []
        self.query_calls: list[dict] = []
        self.verify_calls = 0
        self.fail_upsert_after_write = False
        self.fail_readback = False
        self.fail_index = False
        self.truncate_readback = False
        self.corrupt_values = False

    def verify_index(self, *, required_metadata):
        self.verify_calls += 1
        if self.fail_index:
            raise SightglassError(ErrorCode.SERVICE_UNAVAILABLE, details={"reason": "index"})
        return IndexConfig(
            ACTIVE_DIMENSIONS,
            "cosine",
            tuple(MetadataIndex(name, kind) for name, kind in required_metadata),
        )

    def encode(self, texts):
        self.encode_calls.append(tuple(texts))
        return tuple(_vector(float(index)) for index, _ in enumerate(texts))

    def upsert(self, rows):
        self.upsert_calls.append([dict(row) for row in rows])
        for row in rows:
            self.stored[row["id"]] = {
                "id": row["id"],
                "namespace": row["namespace"],
                "metadata": dict(row["metadata"]),
                "values": list(row["values"]),
            }
        if self.fail_upsert_after_write:
            raise SightglassError(ErrorCode.SERVICE_UNAVAILABLE, details={"reason": "ambiguous"})

    def get_by_ids(self, ids):
        if self.fail_readback:
            raise SightglassError(ErrorCode.SERVICE_UNAVAILABLE, details={"reason": "readback"})
        found = [self.stored[value] for value in ids if value in self.stored]
        if self.corrupt_values:
            found = [
                dict(row, values=[0.0] * ACTIVE_DIMENSIONS, id="corrupt-" + row["id"])
                for row in found
            ]
        return found[:-1] if self.truncate_readback and len(found) > 1 else found

    def query(self, vector, namespace, filter, top_k):
        self.query_calls.append({"namespace": namespace, "filter": filter, "top_k": top_k})
        candidates = [
            {
                "id": row["id"],
                "namespace": row["namespace"],
                "metadata": row["metadata"],
                "values": row["values"],
                "score": sum(a * b for a, b in zip(vector, row["values"])),
            }
            for row in self.stored.values()
            if row["namespace"] == namespace
        ]
        candidates.sort(key=lambda row: -row["score"])
        return candidates[:top_k]


class SemanticServiceTests(unittest.TestCase):
    def test_input_uses_distinct_canonical_fields_and_never_unknown_placeholder(self) -> None:
        row = {
            "kind": "text",
            "search_text": "Synthetic canonical body",
            "text": "Synthetic canonical body",
            "structured_json": json.dumps(
                {
                    "title": "Synthetic card title",
                    "description": "Synthetic canonical body",
                    "source_path": "/synthetic/private/resource",
                    "raw_content": "synthetic envelope",
                }
            ),
        }
        self.assertEqual(_encoder_input(row), "Synthetic canonical body\nSynthetic card title")
        self.assertEqual(_encoder_input(row | {"kind": "unknown"}), "")
        self.assertEqual(
            _encoder_input(
                {
                    "kind": "image",
                    "text": None,
                    "search_text": "  ",
                    "structured_json": "{}",
                }
            ),
            "",
        )
        self.assertEqual(
            _encoder_input(
                {
                    "kind": "image",
                    "text": None,
                    "search_text": None,
                    "structured_json": '{"title":"Synthetic authored caption"}',
                }
            ),
            "Synthetic authored caption",
        )

    def test_unrepresentable_rows_advance_capture_without_encoding_or_publication(self) -> None:
        backend = FakeBackend()
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET kind='unknown',text='[synthetic unsupported]',"
                "search_text='[synthetic unsupported]' WHERE conversation_id=?",
                (self.group_id,),
            )
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        result = self._index_until_ready(service, limit=1, passes=20)
        self.assertEqual(result["state"], "ready")
        self.assertEqual(result["indexed"], 0)
        self.assertEqual(service.coverage(), "complete")
        self.assertEqual(backend.encode_calls, [])
        self.assertEqual(backend.upsert_calls, [])
        self.assertEqual(backend.verify_calls, 0)

    def test_previous_input_recipe_is_preserved_and_refused_on_reopen(self) -> None:
        service = self._service()
        path = service._sidecar_path
        service.close()
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(
                "UPDATE semantic_state SET recipe='sightglass.semantic.bge-m3.message.v1' "
                "WHERE id=1"
            )
            connection.commit()
        with self.assertRaisesRegex(RuntimeError, "recipe/model identity differs"):
            self._service(sidecar=path)
        with closing(sqlite3.connect(path)) as connection:
            self.assertEqual(
                connection.execute("SELECT recipe FROM semantic_state WHERE id=1").fetchone()[0],
                "sightglass.semantic.bge-m3.message.v1",
            )

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        self.source_root = self.root / "source"
        create_synthetic_source(self.source_root)
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            self.source_root,
            self.root / "state" / "window.db",
            default_projection=None,
        )
        self.service.sync_source_once(initial_tail=100, conversation_limit=100)
        self.account = self.repository.active_account_ids()[0]
        conversations = self.repository.account_conversations(self.account)
        self.group = next(row for row in conversations if row["kind"] == "group")
        self.group_id = str(self.group["conversation_id"])
        self.direct_id = str(
            next(row for row in conversations if row["kind"] == "direct")["conversation_id"]
        )
        self.epoch = self.service._projection_inventory_epoch()

    # -- helpers -----------------------------------------------------------
    def _settings(self, **overrides) -> SemanticSettings:
        base = {
            "enabled": True,
            "external_data_authorized": True,
            "cf_account_id": "b" * 32,
            "index_name": "sightglass-semantic-test",
            "source_account_id": self.account,
            "conversation_ids": (self.group_id,),
        }
        base.update(overrides)
        return SemanticSettings(**base)

    def _service(self, settings=None, backend=None, sidecar=None) -> SemanticService:
        return SemanticService(
            self.repository,
            self.service.reader,
            settings=settings or self._settings(),
            backend=backend if backend is not None else FakeBackend(),
            epoch_factory=self.service._projection_inventory_epoch,
            sidecar_path=sidecar or (self.root / "state" / "semantic" / "sidecar.db"),
        )

    def _watermark(self) -> int:
        return self.repository.observation_watermark()

    def _row(self, message_id: str):
        return self.repository.frozen_message_rows((message_id,))[0]

    def _index_until_ready(self, service, *, limit: int = 32, passes: int = 12):
        """Drive bounded publication passes until the lane reports ready.

        The final (ready) pass legitimately publishes nothing, so the returned
        ``indexed`` is the cumulative count across passes.
        """
        result: dict = {"state": "building", "indexed": 0}
        total = 0
        for _ in range(passes):
            result = service.index_once(limit=limit)
            total += result.get("indexed", 0)
            if result["state"] == "ready":
                return {**result, "indexed": total}
        return {**result, "indexed": total}

    def _admit_many(self, count: int, conversation_id: str | None = None) -> list[str]:
        """Add `count` synthetic text messages to one conversation, past one page."""
        conversation_id = conversation_id or self.group_id
        conversation = self.repository.conversation_row(conversation_id)
        assert conversation is not None
        seed = self.repository.materialized_message_rows(
            conversation_id,
            projection_epoch=self.epoch,
            observation_watermark=self._watermark(),
            limit=1,
            direction="forward",
        )[0]
        context = self.repository.conversation_context(conversation_id)
        assert context is not None
        with self.provider.snapshot() as snapshot:
            original = self.provider.get_message(
                context["source_account_key"], seed["source_message_id"], snapshot
            )
        assert original is not None
        base = datetime(2026, 9, 22, tzinfo=UTC)
        created: list[str] = []
        with self.repository.database.transaction():
            for index in range(count):
                source = replace(
                    original,
                    source_message_id=f"synthetic-semantic-{conversation_id[-6:]}-{index}",
                    source_conversation_id=conversation["source_conversation_id"],
                    wechat_type=1,
                    raw_content=f"Synthetic semantic topic {index}",
                    sent_at_utc=(base + timedelta(seconds=index)).isoformat(
                        timespec="microseconds"
                    ),
                    source_time_raw=(base + timedelta(seconds=index)).isoformat(),
                    source_rowid=90_000 + index,
                )
                created.append(
                    self.repository.upsert_message(
                        self.account,
                        conversation_id,
                        seed["sender_id"],
                        seed["sender_membership_id"],
                        source,
                        parse_message(source),
                        projection_epoch=self.epoch,
                    )
                )
        return created

    # -- default off -------------------------------------------------------
    def test_disabled_lane_touches_nothing(self) -> None:
        backend = FakeBackend()
        with patch(
            "sightglass.semantic.service.sqlite3.connect",
            side_effect=AssertionError("sidecar must not open"),
        ):
            service = SemanticService(
                self.repository,
                self.service.reader,
                settings=SemanticSettings(),
                backend=backend,
                epoch_factory=self.service._projection_inventory_epoch,
                sidecar_path=self.root / "state" / "semantic" / "sidecar.db",
            )
        self.assertFalse(service.enabled)
        self.assertFalse((self.root / "state" / "semantic" / "sidecar.db").exists())
        result = service.query(
            "synthetic concept",
            account_id=self.account,
            conversation_ids=(self.group_id,),
            watermark=self._watermark(),
            epoch=self.epoch,
        )
        self.assertEqual(result.message_ids, ())
        self.assertEqual(result.receipt["state"], "disabled")
        self.assertEqual(backend.encode_calls, [])
        with self.assertRaises(SightglassError):
            service.index_once()

    # -- publication / resumable coverage ---------------------------------
    def test_first_pass_is_building_until_full_coverage(self) -> None:
        self._admit_many(150)
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        first = service.index_once(limit=32)
        # A partial pass over a large scope is "building", not a false "ready".
        self.assertEqual(first["state"], "building")
        self.assertEqual(service.status()["coverage"], "partial")
        final = self._index_until_ready(service)
        self.assertEqual(final["state"], "ready")
        self.assertEqual(service.status()["coverage"], "complete")

    def test_index_once_publishes_and_readback_is_manifest(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        result = self._index_until_ready(service)
        self.assertEqual(result["state"], "ready")
        self.assertGreaterEqual(result["indexed"], 1)
        for row in backend.upsert_calls[0]:
            self.assertEqual(len(row["id"]), 64)
            # Raw conversation/sender ids never leave; metadata carries digests.
            self.assertNotIn("wxconv_", json.dumps(row["metadata"]))
            self.assertNotIn("wxperson_", json.dumps(row["metadata"]))
            self.assertEqual(
                set(row["metadata"]),
                {"sent_at", "sender", "kind", "watermark", "has_link", "conversation"},
            )
            self.assertEqual(len(row["values"]), 1024)
        submissions = len(backend.upsert_calls)
        again = service.index_once(limit=32)
        self.assertEqual(again["indexed"], 0)
        self.assertEqual(len(backend.upsert_calls), submissions)

    def test_index_advances_past_first_page_for_large_scope(self) -> None:
        self._admit_many(150)
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        published: set[str] = set()
        for _ in range(10):
            self._index_until_ready(service)
            for call in backend.upsert_calls:
                published.update(row["id"] for row in call)
        self.assertGreater(len(published), 100)
        self.assertGreaterEqual(service.status()["indexed"], 100)

    def test_index_is_fair_and_bounded_across_conversations(self) -> None:
        self._admit_many(40, self.group_id)
        self._admit_many(40, self.direct_id)
        backend = FakeBackend()
        service = self._service(
            settings=self._settings(conversation_ids=(self.group_id, self.direct_id)),
            backend=backend,
        )
        self.addCleanup(service.close)
        self._index_until_ready(service)
        expected = {
            _conversation_digest(self.account, self.group_id),
            _conversation_digest(self.account, self.direct_id),
        }
        conversations = {row["metadata"]["conversation"] for row in backend.upsert_calls[0]}
        self.assertEqual(conversations, expected)

    def test_encode_batches_never_exceed_adapter_limit(self) -> None:
        self._admit_many(60)
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        self.assertTrue(backend.encode_calls)
        self.assertTrue(all(len(call) <= ENCODE_BATCH_LIMIT for call in backend.encode_calls))

    def test_preflight_failure_does_not_skip_unencoded_rows(self) -> None:
        ids = self._admit_many(45)
        backend = FakeBackend()
        backend.fail_index = True
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self.assertEqual(service.index_once()["state"], "degraded")
        self.assertEqual(backend.encode_calls, [])
        self.assertIsNone(service._checkpoint(self.group_id))
        backend.fail_index = False
        self._index_until_ready(service)
        with service._lock:
            published = {
                row[0]
                for row in service._db().execute(
                    "SELECT message_id FROM semantic_entries WHERE published=1"
                )
            }
        self.assertTrue(set(ids) <= published)

    def test_one_row_budget_rotates_conversations_without_starvation(self) -> None:
        backend = FakeBackend()
        service = self._service(
            settings=self._settings(conversation_ids=(self.group_id, self.direct_id)),
            backend=backend,
        )
        self.addCleanup(service.close)
        for _ in range(2):
            service.index_once(limit=1)
        self.assertEqual(
            {row["metadata"]["conversation"] for batch in backend.upsert_calls for row in batch},
            {
                _conversation_digest(self.account, self.group_id),
                _conversation_digest(self.account, self.direct_id),
            },
        )

    def test_publication_holds_canonical_writer_and_counts_only_admitted_rows(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        database = self.repository.database
        connection = service._db()
        inserts = []
        test = self

        class ObservedConnection:
            def execute(self, sql, parameters=()):
                if sql.startswith("INSERT INTO semantic_entries"):
                    test.assertIsNotNone(database._active_connection.get())
                    inserts.append(parameters[0])
                return connection.execute(sql, parameters)

            def __getattr__(self, name):
                return getattr(connection, name)

        with patch.object(service, "_db", return_value=ObservedConnection()):
            self._index_until_ready(service)
        self.assertTrue(inserts)
        count = service.status()["indexed"]
        self.assertEqual(count, len(inserts))
        self.assertEqual(service.status()["pending"], 0)

    def test_generation_change_at_readback_cannot_claim_publication(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        readback = backend.get_by_ids

        def rebuild_before_commit(ids):
            values = readback(ids)
            service.request_rebuild()
            return values

        with patch.object(backend, "get_by_ids", side_effect=rebuild_before_commit):
            result = service.index_once()
        self.assertEqual(result["indexed"], 0)
        self.assertEqual(service.status()["indexed"], 0)

    def test_epoch_change_fences_query_then_rebuilds_derivatives(self) -> None:
        backend = FakeBackend()
        epoch = [self.epoch]
        service = SemanticService(
            self.repository,
            self.service.reader,
            settings=self._settings(),
            backend=backend,
            epoch_factory=lambda: epoch[0],
            sidecar_path=self.root / "state" / "semantic" / "sidecar.db",
        )
        self.addCleanup(service.close)
        self._index_until_ready(service)
        before_generation = service.status()["generation"]
        before_calls = len(backend.encode_calls)
        epoch[0] = "synthetic-changed-epoch"
        result = service.query(
            "synthetic",
            account_id=self.account,
            conversation_ids=(self.group_id,),
            watermark=self._watermark(),
            epoch=epoch[0],
        )
        self.assertEqual(result.receipt["error"], "projection_refresh_pending")
        self.assertEqual(result.receipt["coverage"], "partial")
        self.assertEqual(len(backend.encode_calls), before_calls)
        service.index_once()
        self.assertEqual(service.status()["generation"], before_generation + 1)
        self.assertEqual(service.status()["indexed"], 0)

    def test_sidecar_cannot_be_reused_for_a_different_remote_store(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self._index_until_ready(service)
        service.close()
        with self.assertRaisesRegex(RuntimeError, "different store"):
            self._service(settings=self._settings(index_name="sightglass-other-synthetic"))

    def test_query_reports_partial_for_unconfigured_requested_conversation(self) -> None:
        service = self._service()
        self.addCleanup(service.close)
        self._index_until_ready(service)
        result = service.query(
            "synthetic",
            account_id=self.account,
            conversation_ids=(self.group_id, self.direct_id),
            watermark=self._watermark(),
            epoch=self.epoch,
        )
        self.assertEqual(result.receipt["state"], "ready")
        self.assertEqual(result.receipt["coverage"], "partial")

    def test_rebuild_changes_generation_and_namespace(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        first_namespace = backend.upsert_calls[0][0]["namespace"]
        first_token = service.state_token()
        service.request_rebuild()
        self.assertEqual(service.status()["indexed"], 0)
        self.assertNotEqual(service.state_token(), first_token)
        self._index_until_ready(service)
        second_namespace = backend.upsert_calls[-1][0]["namespace"]
        self.assertNotEqual(first_namespace, second_namespace)

    def test_correction_within_generation_requeues_without_rebuild(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        first = backend.upsert_calls[0][0]
        recorded = (
            service._db()
            .execute("SELECT message_id FROM semantic_entries WHERE remote_id=?", (first["id"],))
            .fetchone()
        )
        assert recorded is not None
        # Correct the canonical row: same message, new observation version.
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET text='Synthetic corrected body',"
                " current_observation_seq=current_observation_seq+1 WHERE message_id=?",
                (str(recorded["message_id"]),),
            )
        # Forget the checkpoint so the corrected row is re-examined in the same
        # generation; the stale manifest entry must requeue it for publication.
        service._db().execute("DELETE FROM semantic_capture_checkpoint")
        service._db().commit()
        result = service.index_once(limit=32)
        self.assertIn(result["state"], {"building", "ready", "degraded"})
        republished = {row["id"] for call in backend.upsert_calls for row in call}
        self.assertIn(first["id"], republished)

    def test_correction_requeues_via_natural_checkpoint_wraparound(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        first = backend.upsert_calls[0][0]
        recorded = (
            service._db()
            .execute("SELECT message_id FROM semantic_entries WHERE remote_id=?", (first["id"],))
            .fetchone()
        )
        assert recorded is not None
        submissions = len(backend.upsert_calls)
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET text='Synthetic corrected body',"
                " current_observation_seq=current_observation_seq+1 WHERE message_id=?",
                (str(recorded["message_id"]),),
            )
        republished = False
        for _ in range(4):
            outcome = service.index_once(limit=32)
            if outcome["indexed"]:
                republished = True
                break
        self.assertTrue(republished)
        self.assertGreater(len(backend.upsert_calls), submissions)
        # The correction publishes under a *new* version-bound remote id; the old
        # object is no longer the current manifest entry.
        latest_ids = {row["id"] for row in backend.upsert_calls[-1]}
        self.assertTrue(latest_ids)
        self.assertNotIn(first["id"], latest_ids)

    def test_capture_checkpoints_advance_for_large_scope(self) -> None:
        self._admit_many(120)
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        checkpoint = (
            service._db()
            .execute(
                "SELECT last_message_id FROM semantic_capture_checkpoint WHERE conversation_id=?",
                (self.group_id,),
            )
            .fetchone()
        )
        self.assertIsNotNone(checkpoint)
        last = str(checkpoint["last_message_id"])
        # A checkpoint is either the last examined row id, or "" after a wrap.
        if last:
            self._row(last)

    def test_sidecar_is_private_and_recipe_identity_enforced(self) -> None:
        backend = FakeBackend()
        sidecar = self.root / "state" / "semantic" / "sidecar.db"
        service = self._service(backend=backend, sidecar=sidecar)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        self.assertEqual(stat.S_IMODE(sidecar.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(sidecar.parent.stat().st_mode), 0o700)
        service.close()
        with closing(sqlite3.connect(sidecar)) as connection:
            connection.execute("UPDATE semantic_state SET model='@cf/other/model'")
            connection.commit()
        with self.assertRaises(RuntimeError):
            self._service(backend=backend, sidecar=sidecar)

    def test_sidecar_rejects_symlink_target(self) -> None:
        target = self.root / "real.db"
        target.write_bytes(b"")
        link = self.root / "state" / "semantic" / "sidecar.db"
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(target)
        with self.assertRaises(RuntimeError):
            self._service(backend=FakeBackend(), sidecar=link)

    # -- query admission ---------------------------------------------------
    def test_query_returns_admitted_current_ids_only(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        watermark = self._watermark()
        result = service.query(
            "内部空白",
            account_id=self.account,
            conversation_ids=(self.group_id,),
            watermark=watermark,
            epoch=self.epoch,
        )
        self.assertEqual(result.receipt["state"], "ready")
        self.assertTrue(result.message_ids)
        for message_id in result.message_ids:
            row = self._row(message_id)
            self.assertEqual(row["current_state"], "present")
            self.assertEqual(str(row["conversation_id"]), self.group_id)
            self.assertEqual(str(row["account_id"]), self.account)
            self.assertLessEqual(int(row["current_observation_seq"]), watermark)
        self.assertEqual(len(result.receipt["state_token"].split(":")), 3)

    def test_query_rejects_a_known_id_with_wrong_namespace_or_metadata(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        query = backend.query
        for corrupt in ("namespace", "metadata"):

            def corrupted(*args):
                matches = query(*args)
                for match in matches:
                    if corrupt == "namespace":
                        match["namespace"] = "synthetic-other-namespace"
                    else:
                        match["metadata"] = {**match["metadata"], "watermark": "wrong"}
                return matches

            with (
                self.subTest(corrupt=corrupt),
                patch.object(backend, "query", side_effect=corrupted),
            ):
                result = service.query(
                    "synthetic",
                    account_id=self.account,
                    conversation_ids=(self.group_id,),
                    watermark=self._watermark(),
                    epoch=self.epoch,
                )
                self.assertEqual(result.message_ids, ())

    def test_query_prefilters_time_sender_kind_and_watermark_remotely(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        service.query(
            "x",
            account_id=self.account,
            conversation_ids=(self.group_id,),
            participant_ids=("wxperson_synthetic_absent",),
            after="2026-09-13T00:00:00+00:00",
            before="2026-09-20T00:00:00+00:00",
            watermark=self._watermark(),
            epoch=self.epoch,
            kinds=("image",),
        )
        self.assertTrue(backend.query_calls)
        sent = backend.query_calls[-1]["filter"]
        encoded = json.dumps(sent)
        for field in ("conversation", "sender", "sent_at", "watermark", "kind"):
            self.assertIn(f'"{field}"', encoded)
        # Raw ids never leave; sender is an opaque digest and conversation too.
        self.assertNotIn("wxperson_synthetic_absent", encoded)
        self.assertNotIn(self.group_id, encoded)
        # No illegal logical operators / $-prefixed filter keys at the top level.
        self.assertNotIn("$and", encoded)
        self.assertNotIn("$or", encoded)
        for key in sent:
            self.assertFalse(key.startswith("$"))

    def test_remote_filter_is_legal_multifield_implicit_and(self) -> None:
        filters = SemanticService._remote_filters(
            account_id=self.account,
            conversation_ids=(self.group_id, self.direct_id),
            participant_ids=("wxperson_x",),
            after="2026-09-13T00:00:00+00:00",
            before="2026-09-20T00:00:00+00:00",
            watermark=5,
            kinds=(),
        )
        self.assertEqual(len(filters), 1)
        sent = filters[0]
        self.assertEqual(set(sent), {"conversation", "sender", "sent_at", "watermark"})
        self.assertEqual(
            sent["sent_at"], {"$gte": sent["sent_at"]["$gte"], "$lt": sent["sent_at"]["$lt"]}
        )
        encoded = json.dumps(sent)
        self.assertNotIn("$and", encoded)
        self.assertNotIn("$or", encoded)
        for key in sent:
            self.assertFalse(key.startswith("$"))

    def test_query_sends_only_legal_filters(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        service.query(
            "x",
            account_id=self.account,
            conversation_ids=(self.group_id,),
            watermark=self._watermark(),
            epoch=self.epoch,
            kinds=("link", "image"),
        )
        self.assertEqual(len(backend.query_calls), 2)
        for call in backend.query_calls:
            sent = call["filter"]
            for key in sent:
                self.assertFalse(key.startswith("$"))
            self.assertNotIn("$and", json.dumps(sent))
            self.assertNotIn("$or", json.dumps(sent))

    def test_query_reevaluates_policy_before_admission(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        original = service.reader.policy

        def flips(scope=None):
            service.reader.policy = type(original)(mode="allowlist")

        service.reader.policy = type(original)(
            mode="allowlist", allowed_conversation_ids=frozenset()
        )
        result = service.query(
            "x",
            account_id=self.account,
            conversation_ids=(self.group_id,),
            watermark=self._watermark(),
            epoch=self.epoch,
        )
        # Capable set is empty under a deny-all policy, so no egress and no ids.
        self.assertEqual(result.message_ids, ())
        service.reader.policy = original
        del flips

    def test_message_kind_is_any_kind_and_link_uses_has_link(self) -> None:
        filters = SemanticService._remote_filters(
            account_id=self.account,
            conversation_ids=(self.group_id,),
            participant_ids=(),
            after=None,
            before=None,
            watermark=0,
            kinds=("message",),
        )
        # message == any kind: no kind/has_link narrowing.
        self.assertEqual(set(filters[0]), {"conversation"})
        link_only = SemanticService._remote_filters(
            account_id=self.account,
            conversation_ids=(self.group_id,),
            participant_ids=(),
            after=None,
            before=None,
            watermark=0,
            kinds=("link",),
        )
        self.assertEqual(
            link_only,
            [{"conversation": link_only[0]["conversation"], "has_link": True}],
        )
        # Mixed link + image scope: two independent pre-filters, no logical operator.
        mixed = SemanticService._remote_filters(
            account_id=self.account,
            conversation_ids=(self.group_id,),
            participant_ids=(),
            after=None,
            before=None,
            watermark=0,
            kinds=("link", "image"),
        )
        self.assertEqual(len(mixed), 2)
        self.assertEqual(mixed[0]["kind"], {"$in": ["image"]})
        self.assertTrue(mixed[1]["has_link"])
        self.assertNotIn("$or", json.dumps(mixed))

    def test_query_link_kind_uses_has_link_metadata(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        service.query(
            "x",
            account_id=self.account,
            conversation_ids=(self.group_id,),
            watermark=self._watermark(),
            epoch=self.epoch,
            kinds=("link",),
        )
        self.assertIn('"has_link"', json.dumps(backend.query_calls[-1]["filter"]))

    def test_query_out_of_config_and_wrong_account_no_egress(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        before = len(backend.query_calls)
        out = service.query(
            "x",
            account_id=self.account,
            conversation_ids=(self.direct_id,),
            watermark=self._watermark(),
            epoch=self.epoch,
        )
        self.assertEqual(out.message_ids, ())
        self.assertEqual(out.receipt["state"], "out_of_config")
        wrong_account = service.query(
            "x",
            account_id="wxacct_other",
            conversation_ids=(self.group_id,),
            watermark=self._watermark(),
            epoch=self.epoch,
        )
        self.assertEqual(wrong_account.receipt["state"], "out_of_config")
        self.assertEqual(len(backend.query_calls), before)

    def test_query_rejects_paused_reader_before_egress(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        before = len(backend.query_calls)
        self.service.reader.paused = True
        try:
            out = service.query(
                "x",
                account_id=self.account,
                conversation_ids=(self.group_id,),
                watermark=self._watermark(),
                epoch=self.epoch,
            )
        finally:
            self.service.reader.paused = False
        self.assertEqual(out.message_ids, ())
        self.assertIn(out.receipt["state"], {"out_of_config", "not_ready"})
        self.assertEqual(len(backend.query_calls), before)

    def test_query_rejects_stale_watermark_and_epoch(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        stale = service.query(
            "x",
            account_id=self.account,
            conversation_ids=(self.group_id,),
            watermark=0,
            epoch=self.epoch,
        )
        self.assertEqual(stale.message_ids, ())
        wrong_epoch = service.query(
            "x",
            account_id=self.account,
            conversation_ids=(self.group_id,),
            watermark=self._watermark(),
            epoch="deadbeef",
        )
        self.assertEqual(wrong_epoch.message_ids, ())

    def test_query_rejects_fake_id_absent_from_manifest(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        real = next(iter(backend.stored.values()))
        backend.stored["f" * 64] = {
            "id": "f" * 64,
            "namespace": real["namespace"],
            "metadata": real["metadata"],
            "values": real["values"],
            "score": 1.0,
        }
        result = service.query(
            "x",
            account_id=self.account,
            conversation_ids=(self.group_id,),
            watermark=self._watermark(),
            epoch=self.epoch,
        )
        self.assertNotIn("f" * 64, result.message_ids)

    # -- failure / recovery ------------------------------------------------
    def test_correction_during_encode_refuses_publication(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        original_encode = backend.encode
        mutated: dict[str, int] = {}

        def encode_then_correct(texts):
            vectors = original_encode(texts)
            rows = self.repository.materialized_message_rows(
                self.group_id,
                projection_epoch=self.epoch,
                observation_watermark=self._watermark(),
                limit=1,
                direction="forward",
            )
            with self.repository.database.transaction() as connection:
                connection.execute(
                    "UPDATE messages SET current_observation_seq = current_observation_seq + 100"
                    " WHERE message_id = ?",
                    (str(rows[0]["message_id"]),),
                )
            mutated["done"] = 1
            return vectors

        backend.encode = encode_then_correct  # type: ignore[assignment]
        result = service.index_once(limit=8)
        self.assertEqual(mutated.get("done"), 1)
        self.assertEqual(result["state"], "degraded")
        self.assertEqual(result["indexed"], 0)
        self.assertEqual(result["failure"], "canonical_drift")
        self.assertEqual(backend.upsert_calls, [])

    def test_readback_mismatch_stays_pending_not_ready(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        backend.upsert = lambda rows: backend.upsert_calls.append([dict(r) for r in rows])
        result = service.index_once(limit=8)
        self.assertEqual(result["state"], "degraded")
        self.assertEqual(result["indexed"], 0)
        self.assertGreater(result["pending"], 0)
        self.assertEqual(service.status()["indexed"], 0)
        again = service.index_once(limit=8)
        self.assertNotEqual(again["state"], "ready")

    def test_ambiguous_upsert_resumes_from_stored_bytes_without_resubmit(self) -> None:
        sidecar = self.root / "state" / "semantic" / "sidecar.db"
        backend = FakeBackend()
        service = self._service(backend=backend, sidecar=sidecar)
        self.addCleanup(service.close)
        backend.fail_upsert_after_write = True
        first = service.index_once(limit=8)
        self.assertEqual(first["state"], "degraded")
        self.assertEqual(first["indexed"], 0)
        submissions = len(backend.upsert_calls)
        encodes = len(backend.encode_calls)
        self.assertEqual(submissions, 1)
        service.close()
        reopened = self._service(backend=backend, sidecar=sidecar)
        self.addCleanup(reopened.close)
        backend.fail_upsert_after_write = False
        resumed = reopened.index_once(limit=8)
        self.assertEqual(resumed["state"], "ready")
        self.assertGreaterEqual(resumed["indexed"], 1)
        self.assertEqual(len(backend.upsert_calls), submissions)
        self.assertEqual(len(backend.encode_calls), encodes)

    def test_restart_rejects_wrong_or_partial_readback_values(self) -> None:
        sidecar = self.root / "state" / "semantic" / "sidecar.db"
        backend = FakeBackend()
        service = self._service(backend=backend, sidecar=sidecar)
        self.addCleanup(service.close)
        backend.fail_upsert_after_write = True
        service.index_once(limit=8)
        service.close()
        backend.fail_upsert_after_write = False
        backend.corrupt_values = True
        reopened = self._service(backend=backend, sidecar=sidecar)
        self.addCleanup(reopened.close)
        resumed = reopened.index_once(limit=8)
        self.assertNotEqual(resumed["state"], "ready")
        self.assertEqual(reopened.status()["indexed"], 0)
        submissions = len(backend.upsert_calls)
        backend.corrupt_values = False
        backend.truncate_readback = True
        partial = reopened.index_once(limit=8)
        self.assertNotEqual(partial["state"], "ready")
        self.assertEqual(len(backend.upsert_calls), submissions)
        backend.truncate_readback = False
        final = reopened.index_once(limit=8)
        self.assertEqual(final["state"], "ready")
        self.assertGreaterEqual(final["indexed"], 1)

    def test_index_degrade_when_index_config_wrong(self) -> None:
        backend = FakeBackend()
        backend.fail_index = True
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        result = service.index_once(limit=8)
        self.assertEqual(result["state"], "degraded")
        query = service.query(
            "x",
            account_id=self.account,
            conversation_ids=(self.group_id,),
            watermark=self._watermark(),
            epoch=self.epoch,
        )
        self.assertEqual(query.message_ids, ())
        self.assertIn(query.receipt["state"], {"not_ready", "degraded"})

    def test_network_degrade_returns_empty(self) -> None:
        backend = FakeBackend()
        backend.fail_readback = True
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        indexed = service.index_once(limit=8)
        self.assertEqual(indexed["state"], "degraded")
        degraded = service.query(
            "x",
            account_id=self.account,
            conversation_ids=(self.group_id,),
            watermark=self._watermark(),
            epoch=self.epoch,
        )
        self.assertEqual(degraded.message_ids, ())
        self.assertIn(degraded.receipt["state"], {"not_ready", "degraded"})

    def test_query_never_persists_concept_in_sidecar(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        secret = "synthetic-observation-poison-PLACEHOLDER"
        service.query(
            secret,
            account_id=self.account,
            conversation_ids=(self.group_id,),
            watermark=self._watermark(),
            epoch=self.epoch,
        )
        sidecar = self.root / "state" / "semantic" / "sidecar.db"
        service.close()
        blob = b""
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(sidecar) + suffix)
            if candidate.exists():
                blob += candidate.read_bytes()
        self.assertNotIn(secret.encode(), blob)

    def test_storage_pressure_pauses_publication(self) -> None:
        class PressuredStorage:
            def reserve(self, *args, **kwargs):
                raise SightglassError(ErrorCode.STORAGE_PRESSURE)

            def track(self, *args, **kwargs):
                return None

        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        service._storage = PressuredStorage()  # type: ignore[assignment]
        with self.assertRaises(SightglassError) as caught:
            service.index_once(limit=8)
        self.assertEqual(caught.exception.code, ErrorCode.STORAGE_PRESSURE)

    def test_concurrent_query_and_index_are_safe(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        errors: list[BaseException] = []

        def worker() -> None:
            try:
                for _ in range(5):
                    service.query(
                        "concurrent",
                        account_id=self.account,
                        conversation_ids=(self.group_id,),
                        watermark=self._watermark(),
                        epoch=self.epoch,
                    )
                    service.index_once(limit=8)
            except BaseException as exc:  # noqa: BLE001 - surfaced by the assertion
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        with self.repository.database.transaction() as connection:
            connection.execute("SELECT 1")

    def test_remote_id_and_namespace_are_version_bound(self) -> None:
        self.assertNotEqual(_namespace("a", "e1", 1), _namespace("a", "e1", 2))
        self.assertNotEqual(_namespace("a", "e1", 1), _namespace("b", "e1", 1))
        self.assertNotEqual(_namespace("a", "e1", 1), _namespace("a", "e2", 1))
        ns = _namespace("a", "e1", 1)
        a = _remote_id(ns, "wxmsg_1", "h1", 1)
        self.assertNotEqual(a, "wxmsg_1")
        self.assertEqual(a, _remote_id(ns, "wxmsg_1", "h1", 1))
        # A corrected observation (new input hash/seq) is a distinct remote id.
        self.assertNotEqual(a, _remote_id(ns, "wxmsg_1", "h2", 2))

    def test_namespace_within_vectorize_byte_limit(self) -> None:
        ns = _namespace("a" * 64, "e" * 64, 99)
        self.assertLessEqual(len(ns.encode()), 64)
        self.assertGreater(len(ns), 0)

    def test_metadata_never_carries_raw_identifiers(self) -> None:
        backend = FakeBackend()
        service = self._service(backend=backend)
        self.addCleanup(service.close)
        self._index_until_ready(service)
        payload = json.dumps(backend.upsert_calls, ensure_ascii=False)
        self.assertNotIn(self.group_id, payload)
        self.assertNotIn(self.account, payload)
        self.assertNotIn("wxperson_", payload)
        # Conversation metadata equals the account-scoped digest.
        digests = {row["metadata"]["conversation"] for call in backend.upsert_calls for row in call}
        self.assertTrue(digests)
        for digest in digests:
            self.assertNotIn(digest, (self.group_id, self.direct_id))

    def test_message_kind_admits_any_kind_and_link_requires_evidence(self) -> None:
        from sightglass.semantic.service import _kinds_admit

        self.assertTrue(_kinds_admit("image", {"message"}, False))
        self.assertTrue(_kinds_admit("text", {"message"}, False))
        self.assertFalse(_kinds_admit("text", {"link"}, False))
        self.assertTrue(_kinds_admit("text", {"link"}, True))
        self.assertTrue(_kinds_admit("image", {"image"}, False))
        self.assertFalse(_kinds_admit("text", {"image"}, True))

    # -- process death -----------------------------------------------------
    def test_process_death_restores_cached_bytes_without_resubmission(self) -> None:
        repo_root = str(Path(__file__).resolve().parents[2])
        helper = """
import json
import os
from pathlib import Path
from sightglass.semantic.service import SemanticService
from sightglass.semantic.settings import SemanticSettings
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack
from tests.unit.test_semantic_service import FakeBackend

root = Path(os.environ["SEMANTIC_DEATH_ROOT"])
sidecar = Path(os.environ["SEMANTIC_DEATH_SIDECAR"])
create_synthetic_source(root / "source")
_provider, repository, service, _tools = build_test_stack(
    root / "source", root / "state" / "window.db", default_projection=None
)
service.sync_source_once(initial_tail=100, conversation_limit=100)
conversations = (
    str(
        repository.account_conversations(repository.active_account_ids()[0])[0][
            "conversation_id"
        ]
    ),
)
settings = SemanticSettings(
    enabled=True,
    external_data_authorized=True,
    cf_account_id="d" * 32,
    index_name="sightglass-death",
    source_account_id=repository.active_account_ids()[0],
    conversation_ids=conversations,
)
backend = FakeBackend()
backend.fail_upsert_after_write = True
svc = SemanticService(
    repository,
    service.reader,
    settings=settings,
    backend=backend,
    epoch_factory=service._projection_inventory_epoch,
    sidecar_path=sidecar,
)
svc.index_once(limit=8)
stored = {row["id"]: row for call in backend.upsert_calls for row in call}
Path(os.environ["SEMANTIC_DEATH_STORE"]).write_text(json.dumps(stored))
intents = svc._db().execute("SELECT COUNT(*) FROM semantic_intents").fetchone()[0]
print("INTENTS", intents, flush=True)
os._exit(7)
"""
        with tempfile.TemporaryDirectory() as workspace:
            root = Path(workspace)
            sidecar = root / "state" / "semantic" / "sidecar.db"
            store_path = root / "store.json"
            env = dict(os.environ)
            env["PYTHONPATH"] = repo_root
            env["SEMANTIC_DEATH_ROOT"] = str(root)
            env["SEMANTIC_DEATH_SIDECAR"] = str(sidecar)
            env["SEMANTIC_DEATH_STORE"] = str(store_path)
            result = subprocess.run(
                [sys.executable, "-c", helper],
                cwd=repo_root,
                capture_output=True,
                env=env,
                timeout=180,
            )
            self.assertEqual(result.returncode, 7, result.stderr.decode())
            self.assertTrue(sidecar.exists())
            stored = json.loads(store_path.read_text())
            self.assertTrue(stored)

            backend = FakeBackend()
            for row in stored.values():
                backend.stored[row["id"]] = dict(
                    row, metadata=dict(row["metadata"]), values=list(row["values"])
                )
            _provider, repository, service2, tools = build_test_stack(
                root / "source", root / "state" / "window.db", default_projection=None
            )
            settings = SemanticSettings(
                enabled=True,
                external_data_authorized=True,
                cf_account_id="d" * 32,
                index_name="sightglass-death",
                source_account_id=repository.active_account_ids()[0],
                conversation_ids=(
                    str(
                        repository.account_conversations(repository.active_account_ids()[0])[0][
                            "conversation_id"
                        ]
                    ),
                ),
            )
            reopened = SemanticService(
                repository,
                service2.reader,
                settings=settings,
                backend=backend,
                epoch_factory=service2._projection_inventory_epoch,
                sidecar_path=sidecar,
            )
            self.addCleanup(reopened.close)
            self.addCleanup(tools.close)
            resumed = reopened.index_once(limit=8)
            self.assertEqual(resumed["state"], "ready")
            self.assertGreaterEqual(resumed["indexed"], 1)
            self.assertEqual(backend.upsert_calls, [])
            self.assertEqual(backend.encode_calls, [])


if __name__ == "__main__":
    unittest.main()

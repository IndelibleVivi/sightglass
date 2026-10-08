"""Generated offline replica, capture-boundary and exact update replay contracts."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import fields, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

from sightglass.contracts.capture import CaptureCeiling, CaptureRequest
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.db import WindowDB
from sightglass.model.links import LinkRepository
from sightglass.model.repositories import WindowRepository
from sightglass.policy.readers import ReaderPolicy
from sightglass.reader.cursors import cursor_scope
from sightglass.reader.replica import UPDATE_REQUEST_TOOL, request_spool_ids
from sightglass.reader.service import ReaderService
from sightglass.runtime.capture_core import CoreCapture
from sightglass.runtime.config import SightglassConfig
from sightglass.runtime.control import cleanup_deliveries
from sightglass.source.capture.executor import CaptureExecutor
from sightglass.source.capture.frozen import FrozenCaptureProvider
from sightglass.source.identity import SignedTokenCodec
from sightglass.source.parser import parse_message
from sightglass.source.remote import RemoteCaptureProvider, RemoteCaptureSettings
from sightglass.source.synthetic import _create_shard, _row, create_synthetic_source
from tests.fixtures.factory import build_test_stack


class OfflineSource:
    def __init__(self, provider: Any) -> None:
        self.descriptor = provider.descriptor

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"replica accessed source: {name}")

    def close(self) -> None:
        pass


class ReplicaReadTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        source = create_synthetic_source(root / "source")
        self.source_root = source
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            source,
            root / "state" / "window.db",
            default_projection=None,
        )
        self.addCleanup(self.tools.close)
        self.service.status()
        self.group = self.service.find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        self.service.sync_source_once(initial_tail=100, conversation_limit=100)
        self.account = self.repository.active_account_ids()[0]
        self.context = self.repository.conversation_context(self.group)
        assert self.context is not None
        self.seed = self.repository.materialized_message_rows(
            self.group,
            projection_epoch=self.service._projection_inventory_epoch(),
            observation_watermark=self.repository.observation_watermark(),
            direction="forward",
            limit=1,
        )[0]
        with self.provider.snapshot() as snapshot:
            self.original = self.provider.get_message(
                self.context["source_account_key"],
                self.seed["source_message_id"],
                snapshot,
            )
        assert self.original is not None
        self.moment = datetime(2026, 9, 25, tzinfo=UTC)
        self.sources: dict[str, Any] = {}
        self.capture_sequence = 0

    def offline(self, *, partial_mode: str = "on_demand") -> None:
        self.service.default_view = "replica"
        self.service.residency.set(self.group, mode=partial_mode)
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE source_conversation_state SET history_complete=0,forward_complete=0,"
                "backfill_state='partial',tail_sort_primary=NULL WHERE conversation_id=?",
                (self.group,),
            )
        self.service.provider = OfflineSource(self.provider)  # type: ignore[assignment]

    def admit(self, name: str, text: str, *, offset: int = 0, epoch: str | None = None) -> str:
        assert self.original is not None
        instant = self.moment + timedelta(seconds=offset)
        source = replace(
            self.original,
            source_message_id=f"synthetic-replica-{name}",
            conversation_kind="direct",
            raw_content=text,
            wechat_type=1,
            sent_at_utc=instant.isoformat(),
            source_time_raw=instant.isoformat(),
            source_rowid=100_000 + offset,
            sort_seq=offset,
            observed_at_utc=(instant + timedelta(seconds=1)).isoformat(),
        )
        identity = self.repository.upsert_message(
            self.account,
            self.group,
            self.seed["sender_id"],
            self.seed["sender_membership_id"],
            source,
            parse_message(source),
            projection_epoch=epoch or self.service._projection_inventory_epoch(),
        )
        self.sources[source.source_message_id] = source
        return identity

    def assert_error(self, code: ErrorCode, method, **arguments: Any) -> SightglassError:
        with self.assertRaises(SightglassError) as caught:
            method(**arguments)
        self.assertEqual(caught.exception.code, code)
        return caught.exception

    def delivery(self, identity: str) -> Any:
        row = self.repository.delivery(identity)
        assert row is not None
        return row

    def profile(self) -> Any:
        row = self.repository.database.reader_profile("codex")
        assert row is not None
        return row

    def capture(self, arguments: dict[str, Any]) -> tuple[Any, FrozenCaptureProvider]:
        plan = self.service.replica.prepare_fresh_message_capture(arguments)
        selected = {
            field.name: getattr(plan, field.name)
            for field in fields(CaptureRequest)
            if hasattr(plan, field.name)
        }
        selected.update(
            account_id=plan.source_account_key,
            request_id=f"synthetic-capture-{self.capture_sequence}",
            policy_revision=self.service._policy_revision(),
        )
        request = CaptureRequest(**selected)
        executor = CaptureExecutor(
            self.provider,
            CaptureCeiling(
                plan.source_account_key,
                frozenset({plan.conversation_source_id}),
                "synthetic-egress-revision",
            ),
            source_instance_id="synthetic-reader-capture-source",
        )
        self.capture_sequence += 1
        sealed = executor.capture(
            request, stream_epoch="synthetic-stream-reader", sequence=self.capture_sequence
        )
        self.assertEqual(sealed.document().receipt.terminal, "complete")
        frozen = FrozenCaptureProvider(sealed)
        self.assertEqual(frozen.origin_epoch, self.service._projection_inventory_epoch())
        return plan, frozen

    def read_capture(self, arguments: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
        plan, frozen = self.capture(arguments)
        with self.service.captured_provider(
            frozen,
            updates_after=plan.after,
            updates_message_ids=plan.updates_message_ids,
            updates_reconcile_revision=plan.updates_reconcile_revision,
        ):
            return plan, self.service.read_messages(**arguments)

    def append_source(self, count: int, *, prefix: str = "synthetic-range") -> set[str]:
        rows = [
            _row(
                f"{prefix}-{index}",
                "conv_group",
                (self.moment + timedelta(seconds=index)).isoformat(),
                index,
                200_000 + index,
                1,
                f"wxid_demo_member:\nSynthetic captured range needle {index}",
                sender="wxid_demo_member",
                shown_as="Synthetic current speaker",
            )
            for index in range(count)
        ]
        columns = tuple(rows[0])
        with closing(sqlite3.connect(self.source_root / "messages-2.db")) as connection:
            connection.executemany(
                f"INSERT INTO messages ({','.join(columns)}) VALUES "
                f"({','.join('?' for _ in columns)})",
                [tuple(row[key] for key in columns) for row in rows],
            )
            connection.commit()
        return {row["source_message_id"] for row in rows}

    def append_tied_source(self) -> set[str]:
        rows = [
            _row(
                f"synthetic-tied-{index:04d}",
                "conv_group",
                self.moment.isoformat(),
                17,
                700_000,
                1,
                f"wxid_demo_member:\nSynthetic tied boundary body {index}",
                sender="wxid_demo_member",
            )
            for index in range(225)
        ]
        columns = tuple(rows[0])
        # Identical overlap spans both sides of the canonical page boundary.
        for filename, selected in (("messages-2.db", rows), ("messages-1.db", rows[150:210])):
            with closing(sqlite3.connect(self.source_root / filename)) as connection:
                connection.executemany(
                    f"INSERT INTO messages ({','.join(columns)}) VALUES "
                    f"({','.join('?' for _ in columns)})",
                    [tuple(row[key] for key in columns) for row in selected],
                )
                connection.commit()
        return {row["source_message_id"] for row in rows}

    def restart_reader(self) -> None:
        self.service = ReaderService(
            OfflineSource(self.provider),  # type: ignore[arg-type]
            WindowRepository(WindowDB(self.repository.database.path)),
            self.service.reader,
            self.service.token_codec,
            default_view="replica",
        )

    def test_partial_resident_status_catalog_inbox_and_message_modes_are_offline(self) -> None:
        for mode in ("on_demand", "recent"):
            with self.subTest(residency=mode):
                self.offline(partial_mode=mode)
                with (
                    patch.object(
                        self.service.retrieval,
                        "_resident_scope_complete",
                        side_effect=AssertionError("complete scope required"),
                    ),
                    patch.object(
                        self.service,
                        "_prepare_search_candidates",
                        side_effect=AssertionError("source preparer touched"),
                    ),
                ):
                    self.assertEqual(self.service.status()["read_plane"]["default_view"], "replica")
                    catalog = self.service.find_conversations("Synthetic Group")
                    self.assertEqual(catalog["candidates"][0]["conversation_id"], self.group)
                    people = self.service.find_participants(conversation_id=self.group, query="")
                    self.assertTrue(people["candidates"])
                    inbox = self.service.read_inbox(account_id=self.account)
                    self.assertTrue(inbox["items"])
                    cases: tuple[dict[str, Any], ...] = (
                        {"mode": "recent", "limit": 2},
                        {"mode": "range", "time_after": "2020-01-01T00:00:00+00:00"},
                        {"mode": "speaker", "participant_ids": (self.seed["sender_id"],)},
                        {"mode": "message", "message_id": self.seed["message_id"]},
                        {
                            "mode": "context",
                            "message_id": self.seed["message_id"],
                            "before": 1,
                            "after": 1,
                            "limit": 3,
                        },
                    )
                    for arguments in cases:
                        self.assertTrue(
                            self.service.local_only_tool_call("wechat_read_messages", arguments)
                        )
                        result = self.service.read_messages(conversation_id=self.group, **arguments)
                        self.assertTrue(result["messages"])
                        self.assertEqual(result["source_receipt"]["view"], "replica")
                        self.assertFalse(result["source_receipt"]["complete"])
                        self.assertFalse(
                            result["source_receipt"]["freshness"]["live_refresh_confirmed"]
                        )

    def test_replica_missing_scope_and_signed_cursor_mismatch_fail_locally(self) -> None:
        self.offline()
        self.assert_error(
            ErrorCode.SOURCE_INCOMPLETE,
            self.service.read_messages,
            mode="recent",
            conversation_id="wxconv_synthetic_absent",
        )
        self.assert_error(
            ErrorCode.MESSAGE_NOT_FOUND,
            self.service.read_messages,
            mode="message",
            message_id="wxmsg_synthetic_absent",
        )
        first = self.service.read_messages(mode="recent", conversation_id=self.group, limit=1)
        cursor = first["page"]["next_cursor"]
        self.assertTrue(cursor)
        self.assert_error(
            ErrorCode.CURSOR_INVALID,
            self.service.read_messages,
            mode="recent",
            conversation_id=self.group,
            limit=1,
            view="fresh",
            cursor=cursor,
        )
        self.assert_error(
            ErrorCode.QUERY_INVALID,
            self.service.read_messages,
            mode="recent",
            conversation_id=self.group,
            refresh=True,
            view="replica",
        )
        self.assertTrue(
            self.service.local_only_tool_call(
                "wechat_read_messages", {"refresh": "synthetic-invalid"}
            )
        )

    def test_replica_catalog_resolves_observed_alias(self) -> None:
        with self.repository.database.transaction() as connection:
            connection.execute(
                "INSERT INTO conversation_aliases VALUES "
                "('synthetic-replica-alias',?, 'Synthetic Former Title','synthetic former title',"
                "'source_alias','2026-09-01',NULL,'synthetic',1)",
                (self.group,),
            )
        self.offline()
        found = self.service.find_conversations("former title")
        self.assertEqual(found["candidates"][0]["matched"]["kind"], "alias")

    def test_literal_search_has_sender_time_index_fallback_and_correction_parity(self) -> None:
        target = self.admit("literal", "Synthetic Straße EXACT phrase Alpha", offset=1)
        self.admit("wrong-phrase", "Synthetic Straße exact interleaved phrase Alpha", offset=2)
        self.admit("late", "Synthetic Straße EXACT phrase Alpha", offset=100)
        with self.repository.database.transaction() as connection:
            connection.execute(
                "DELETE FROM message_lexical_projection WHERE message_id=?", (target,)
            )
        self.offline()
        arguments = {
            "query": 'STRASSE "exact phrase" alpha',
            "conversation_ids": (self.group,),
            "participant_ids": (self.seed["sender_id"],),
            "after": self.moment.isoformat(),
            "before": (self.moment + timedelta(seconds=10)).isoformat(),
        }
        found = self.service.search_messages(**arguments)
        self.assertEqual([row[0] for row in found["hits"]], [target])
        self.assertFalse(found["source_receipt"]["complete"])
        self.assertEqual(found["source_receipt"]["view"], "replica")
        self.assert_error(
            ErrorCode.QUERY_INVALID, self.service.search_messages, **arguments, strict=False
        )
        self.admit("literal", "Synthetic changed canonical body", offset=1)
        self.assertEqual(self.service.search_messages(**arguments)["hits"], [])

    def test_search_cursor_freezes_appends_and_stales_on_correction_or_view_change(self) -> None:
        first_id = self.admit("search-first", "Synthetic pagingneedle", offset=1)
        self.admit("search-second", "Synthetic pagingneedle", offset=2)
        self.offline()
        arguments = {"query": "pagingneedle", "conversation_ids": (self.group,), "limit": 1}
        first = self.service.search_messages(**arguments)
        cursor = first["page"]["next_cursor"]
        self.assertTrue(cursor)
        self.admit("search-third", "Synthetic pagingneedle", offset=3)
        next_page = self.service.search_messages(**arguments, cursor=cursor)
        self.assertEqual(len(next_page["hits"]), 1)
        self.assertIsNone(next_page["page"]["next_cursor"])
        self.assert_error(
            ErrorCode.CURSOR_INVALID,
            self.service.search_messages,
            **arguments,
            cursor=cursor,
            view="fresh",
        )
        self.admit("search-first", "Synthetic corrected body", offset=1)
        self.assertNotIn(
            first_id, [row[0] for row in self.service.search_messages(**arguments)["hits"]]
        )
        self.assert_error(
            ErrorCode.CURSOR_STALE, self.service.search_messages, **arguments, cursor=cursor
        )

    def test_bounded_literal_scan_continues_without_hidden_source_preparation(self) -> None:
        expected = {self.admit(f"bounded-{i}", "Synthetic scanneedle", offset=i) for i in range(6)}
        self.offline()
        seen: set[str] = set()
        cursor = None
        with patch("sightglass.reader.replica.REPLICA_SEARCH_BUDGET", 2):
            for _ in range(10):
                page = self.service.search_messages(
                    query="scanneedle", conversation_ids=(self.group,), cursor=cursor
                )
                seen.update(row[0] for row in page["hits"])
                cursor = page["page"]["next_cursor"]
                if not cursor:
                    break
            else:
                self.fail("bounded replica scan never finished")
        self.assertEqual(seen, expected)

    def test_expired_released_pending_release_and_old_epoch_bodies_never_project(self) -> None:
        excluded = [
            self.admit(f"ineligible-{i}", "Synthetic privateresidentneedle", offset=i)
            for i in range(4)
        ]
        retained = self.admit("eligible", "Synthetic privateresidentneedle", offset=10)
        links = LinkRepository(self.repository.database)
        self.admit("link-eligible", "https://synthetic-replica.example/needle", offset=11)
        while links.backfill_batch()["state"] != "ready":
            pass
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET body_available=0 WHERE message_id=?", (excluded[0],)
            )
            connection.execute(
                "INSERT INTO message_body_residency VALUES "
                "(?,?,'on_demand',0,'1999-01-01','2000-01-01T00:00:00Z')",
                (excluded[1], self.group),
            )
            connection.execute(
                "INSERT INTO body_release_jobs SELECT message_id,"
                "current_observation_seq,0 FROM messages WHERE message_id=?",
                (excluded[2],),
            )
            connection.execute(
                "UPDATE messages SET projection_epoch='synthetic-old-epoch' WHERE message_id=?",
                (excluded[3],),
            )
        self.offline()
        search = self.service.search_messages(
            query="privateresidentneedle", conversation_ids=(self.group,)
        )
        self.assertEqual([row[0] for row in search["hits"]], [retained])
        recent = self.service.read_messages(mode="recent", conversation_id=self.group, limit=50)
        self.assertFalse(set(excluded) & {row[0] for row in recent["messages"]})
        for identity in excluded:
            self.assert_error(
                ErrorCode.SOURCE_INCOMPLETE,
                self.service.read_messages,
                mode="message",
                message_id=identity,
            )
        with (
            patch.object(
                self.service.retrieval,
                "_discovery_request",
                side_effect=AssertionError("source discovery touched"),
            ),
            patch.object(
                self.service.retrieval,
                "_resident_scope_complete",
                side_effect=AssertionError("full source scope required"),
            ),
        ):
            found = self.service.retrieval.find_links(
                query="needle", conversation_ids=(self.group,)
            )
            self.assertTrue(found["items"])
            contexts = self.service.retrieval.retrieve(
                concept="privateresidentneedle", conversation_ids=(self.group,)
            )
            self.assertTrue(contexts["contexts"])
            self.assertFalse(
                set(excluded)
                & {row[0] for context in contexts["contexts"] for row in context["messages"]}
            )

    def test_inbox_ignores_latest_body_from_old_interpretation(self) -> None:
        old = self.admit(
            "inbox-old",
            "Synthetic old epoch latest",
            offset=100,
            epoch="synthetic-obsolete-interpretation",
        )
        self.offline()
        inbox = self.service.read_inbox(account_id=self.account, include_latest="text")
        item = next(item for item in inbox["items"] if item["conversation_id"] == self.group)
        self.assertNotEqual(item["latest"]["message_id"], old)

    def test_offline_updates_replay_ack_next_and_filter_scope_remains_independent(self) -> None:
        self.offline()
        arguments = {"mode": "updates", "conversation_id": self.group, "limit": 2}
        first = self.service.read_messages(**arguments, request_id="synthetic-replica-request1")
        delivery = self.repository.delivery(first["page"]["delivery_id"])
        assert delivery is not None
        original_bytes = Path(delivery["payload_ref"]).read_bytes()
        self.assertEqual(self.service.read_messages(**arguments), first)
        self.assertEqual(Path(delivery["payload_ref"]).read_bytes(), original_bytes)
        self.assertEqual(
            self.repository.update_position("codex", self.group, "conversation", "*"), 0
        )
        second = self.service.read_messages(
            **arguments,
            ack_delivery_id=delivery["delivery_id"],
            request_id="synthetic-replica-request2",
        )
        self.assertTrue(second["messages"])
        self.assertFalse(
            {row[0] for row in first["messages"]} & {row[0] for row in second["messages"]}
        )
        self.assertGreater(
            self.repository.update_position("codex", self.group, "conversation", "*"), 0
        )
        committed = self.repository.update_position("codex", self.group, "conversation", "*")
        filtered = self.service.read_messages(**arguments, query="synthetic-absent-filter")
        if filtered["page"]["delivery_id"]:
            self.service.read_messages(
                **arguments,
                query="synthetic-absent-filter",
                ack_delivery_id=filtered["page"]["delivery_id"],
            )
        self.assertEqual(
            self.repository.update_position("codex", self.group, "conversation", "*"), committed
        )

    def test_fresh_failed_source_rolls_ack_back_and_never_falls_back(self) -> None:
        first = self.service.read_messages(
            mode="updates", conversation_id=self.group, limit=2, view="fresh"
        )
        delivery_id = first["page"]["delivery_id"]
        with (
            patch.object(
                self.provider, "session", side_effect=SightglassError(ErrorCode.SOURCE_INCOMPLETE)
            ),
            patch.object(
                self.provider, "snapshot", side_effect=SightglassError(ErrorCode.SOURCE_INCOMPLETE)
            ),
        ):
            self.assert_error(
                ErrorCode.SOURCE_INCOMPLETE,
                self.service.read_messages,
                mode="updates",
                conversation_id=self.group,
                limit=2,
                ack_delivery_id=delivery_id,
                request_id="synthetic-failed-request",
                view="fresh",
            )
            self.assert_error(
                ErrorCode.SOURCE_INCOMPLETE,
                self.service.read_messages,
                mode="recent",
                conversation_id=self.group,
                view="fresh",
            )
        self.assertEqual(self.delivery(delivery_id)["status"], "pending")
        with self.repository.database.connection() as connection:
            self.assertFalse(
                connection.execute(
                    "SELECT 1 FROM access_receipts WHERE tool_name=?", (UPDATE_REQUEST_TOOL,)
                ).fetchone()
            )

    def test_committed_fresh_ack_response_loss_replays_identical_bytes_offline(self) -> None:
        arguments = {"mode": "updates", "conversation_id": self.group, "limit": 2, "view": "fresh"}
        first = self.service.read_messages(**arguments)
        ack = first["page"]["delivery_id"]
        request = {**arguments, "ack_delivery_id": ack, "request_id": "synthetic-lost-response"}
        committed = self.service.read_messages(**request)
        delivery = self.repository.delivery(committed["page"]["delivery_id"])
        assert delivery is not None
        payload = Path(delivery["payload_ref"]).read_bytes()
        self.assertEqual(self.delivery(ack)["status"], "acknowledged")
        self.offline()
        self.assertTrue(self.service.local_only_tool_call("wechat_read_messages", request))
        with patch.object(
            self.repository.database,
            "transaction",
            side_effect=AssertionError("durable request replay took writer"),
        ):
            self.assertEqual(self.service.read_messages(**request), committed)
        self.assertEqual(Path(delivery["payload_ref"]).read_bytes(), payload)
        with self.repository.database.connection() as connection:
            self.assertIn(delivery["delivery_id"], request_spool_ids(connection))
            metadata = connection.execute(
                "SELECT warning_codes_json FROM access_receipts WHERE tool_name=?",
                (UPDATE_REQUEST_TOOL,),
            ).fetchone()[0]
        self.assertNotIn(str(self.service.delivery_store.root), metadata)
        self.assert_error(
            ErrorCode.QUERY_INVALID, self.service.read_messages, **{**request, "limit": 3}
        )

    def test_empty_committed_fresh_request_has_offline_durable_outcome(self) -> None:
        first = self.service.read_messages(
            mode="updates",
            conversation_id=self.group,
            limit=100,
            view="fresh",
            projection="compact",
        )
        request = {
            "mode": "updates",
            "conversation_id": self.group,
            "view": "fresh",
            "projection": "compact",
            "limit": 100,
            "ack_delivery_id": first["page"]["delivery_id"],
            "request_id": "synthetic-empty-response",
        }
        empty = self.service.read_messages(**request)
        self.assertEqual(empty["messages"], [])
        self.assertIsNone(empty["page"]["delivery_id"])
        self.offline()
        self.assertEqual(self.service.read_messages(**request), empty)

    def test_policy_change_blocks_old_request_and_pending_payload(self) -> None:
        self.offline()
        request = {
            "mode": "updates",
            "conversation_id": self.group,
            "request_id": "synthetic-policy-request",
        }
        first = self.service.read_messages(**request)
        self.service.reader = replace(self.service.reader, policy=ReaderPolicy(mode="allowlist"))
        self.assert_error(ErrorCode.POLICY_DENIED, self.service.read_messages, **request)
        self.service.reader = replace(
            self.service.reader,
            policy=ReaderPolicy(
                mode="all_except_denylist", identity_debug=True, max_messages_per_call=10
            ),
        )
        self.assert_error(ErrorCode.QUERY_INVALID, self.service.read_messages, **request)
        self.assertEqual(self.delivery(first["page"]["delivery_id"])["status"], "expired")

    def test_pressure_maintenance_ack_and_request_outcome_are_atomic_and_local(self) -> None:
        self.offline()
        first = self.service.read_messages(mode="updates", conversation_id=self.group, limit=2)
        transaction = self.repository.database.transaction

        def maintenance_only(*args, **kwargs):
            if (
                not kwargs.get("maintenance")
                and self.repository.database._active_connection.get() is None
            ):
                raise SightglassError(ErrorCode.STORAGE_PRESSURE)
            return transaction(*args, **kwargs)

        request = {
            "mode": "updates",
            "conversation_id": self.group,
            "limit": 2,
            "ack_delivery_id": first["page"]["delivery_id"],
            "request_id": "synthetic-pressure-request",
        }
        with patch.object(self.repository.database, "transaction", maintenance_only):
            error = self.assert_error(
                ErrorCode.STORAGE_PRESSURE, self.service.read_messages, **request
            )
        self.assertTrue(error.details["ack_committed"])
        self.assertEqual(self.delivery(first["page"]["delivery_id"])["status"], "acknowledged")
        self.assertEqual(self.service.read_messages(**request), error.as_dict())

    def test_message_capture_plan_caps_pages_rejects_large_context_and_binds_view(self) -> None:
        self.offline()
        arguments = {
            "mode": "recent",
            "conversation_id": self.group,
            "limit": 500,
            "projection": "compact",
            "view": "fresh",
        }
        plan = self.service.replica.prepare_fresh_message_capture(arguments)
        self.assertEqual((plan.operation, plan.limit), ("recent", 200))
        assert self.context is not None
        self.assertEqual(plan.source_account_key, self.context["source_account_key"])
        self.assert_error(
            ErrorCode.QUERY_INVALID,
            self.service.replica.prepare_fresh_message_capture,
            arguments={
                "mode": "context",
                "message_id": self.seed["message_id"],
                "before": 120,
                "after": 100,
                "limit": 500,
            },
        )
        first = self.service.read_messages(mode="recent", conversation_id=self.group, limit=1)
        self.assert_error(
            ErrorCode.CURSOR_INVALID,
            self.service.replica.prepare_fresh_message_capture,
            arguments={**arguments, "cursor": first["page"]["next_cursor"]},
        )
        with self.service.captured_provider(self.provider):
            page = self.service.read_messages(**arguments)
        self.assertTrue(page["messages"])
        self.assertEqual(page["source_receipt"]["view"], "fresh")

    def test_capture_admission_hooks_share_writer_and_rollback_after_hook_failure(self) -> None:
        callbacks = []

        def before(connection):
            connection.execute(
                "UPDATE reader_profiles SET display_name='Synthetic admitted' "
                "WHERE reader_id='codex'"
            )
            callbacks.append("before")

        with self.service.captured_provider(
            self.provider,
            before_commit=before,
            after_commit=lambda: callbacks.append("after"),
        ):
            self.service.read_messages(mode="recent", conversation_id=self.group, view="fresh")
        self.assertEqual(callbacks, ["before", "after"])
        self.assertEqual(self.profile()["display_name"], "Synthetic admitted")
        callbacks.clear()

        def failing_before(connection):
            before(connection)
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED)

        with self.service.captured_provider(
            self.provider,
            before_commit=failing_before,
            after_commit=lambda: callbacks.append("unexpected-after"),
        ):
            self.assert_error(
                ErrorCode.SOURCE_GENERATION_CHANGED,
                self.service.read_messages,
                mode="recent",
                conversation_id=self.group,
                view="fresh",
            )
        self.assertEqual(callbacks, ["before"])

    def test_captured_search_rejects_omitted_prefix_and_touches_only_captured_ids(self) -> None:
        query = "保留"
        arguments = {"query": query, "conversation_ids": (self.group,), "view": "fresh", "limit": 1}
        candidates = self.service.replica.fresh_search_candidate_ids(arguments)
        self.assertTrue(candidates)
        called = []
        get_message = self.provider.get_message
        candidate_rows = self.repository.frozen_message_rows(candidates)
        allowed = {row["source_message_id"] for row in candidate_rows}

        def captured_get(account, source_id, snapshot):
            self.assertIn(source_id, allowed)
            called.append(source_id)
            return get_message(account, source_id, snapshot)

        with (
            patch.object(self.provider, "get_message", captured_get),
            patch.object(
                self.service,
                "_prepare_search_candidates",
                side_effect=AssertionError("captured search prepared source"),
            ),
            self.service.captured_provider(self.provider, search_candidate_ids=candidates),
        ):
            result = self.service.search_messages(**arguments)
        self.assertTrue(result["hits"])
        self.assertTrue(called)
        if len(candidates) > 1:
            with self.service.captured_provider(self.provider, search_candidate_ids=candidates[1:]):
                self.assert_error(ErrorCode.CURSOR_STALE, self.service.search_messages, **arguments)

    def test_actual_sealed_capture_serves_message_context_and_speaker_windows(self) -> None:
        self.offline()
        cases: tuple[dict[str, Any], ...] = (
            {"mode": "recent", "conversation_id": self.group, "limit": 500},
            {"mode": "message", "message_id": self.seed["message_id"]},
            {
                "mode": "context",
                "message_id": self.seed["message_id"],
                "before": 1,
                "after": 1,
                "limit": 3,
            },
            {
                "mode": "range",
                "conversation_id": self.group,
                "time_after": "2020-01-01T00:00:00+00:00",
                "limit": 2,
            },
            {
                "mode": "speaker",
                "conversation_id": self.group,
                "participant_ids": (self.seed["sender_id"],),
                "limit": 2,
            },
            {
                "mode": "speaker",
                "conversation_id": self.group,
                "participant_ids": (self.seed["sender_id"],),
                "speaker_view": "with_context",
                "before": 1,
                "after": 1,
                "limit": 3,
            },
        )
        for arguments in cases:
            with self.subTest(mode=arguments["mode"], speaker=arguments.get("speaker_view")):
                arguments = {**arguments, "view": "fresh"}
                plan, frozen = self.capture(arguments)
                with self.service.captured_provider(frozen):
                    page = self.service.read_messages(**arguments)
                self.assertTrue(page["messages"])
                self.assertEqual(page["source_receipt"]["view"], "fresh")
                self.assertLessEqual(len(page["messages"]), 200)
                self.assertLessEqual(plan.limit, 200)

    def test_actual_captured_updates_verify_known_then_reconcile_one_finite_pass(self) -> None:
        expected = self.append_source(450)
        self.offline()
        received: set[str] = set()
        previous_delivery = None
        operations = []
        scan_pages = []
        state = self.repository.source_conversation_state(self.group)
        assert state is not None
        background_position = tuple(
            state[key]
            for key in (
                "tail_sort_primary",
                "forward_complete",
                "history_complete",
                "backfill_state",
            )
        )
        for index in range(8):
            arguments = {
                "mode": "updates",
                "conversation_id": self.group,
                "limit": 500,
                "projection": "compact",
                "view": "fresh",
                "request_id": f"synthetic-batched-updates-{index}",
                "ack_delivery_id": previous_delivery,
            }
            plan, frozen = self.capture(arguments)
            operations.append(plan.operation)
            before = self.repository.update_position("codex", self.group, "conversation", "*")
            with self.service.captured_provider(
                frozen,
                updates_after=plan.after,
                updates_message_ids=plan.updates_message_ids,
                updates_reconcile_revision=plan.updates_reconcile_revision,
            ):
                page = self.service.read_messages(**arguments)
            current_delivery = page["page"]["delivery_id"]
            self.assertEqual(self.service.read_messages(**arguments), page)
            observed = {
                row["source_message_id"]
                for row in self.repository.frozen_message_rows(
                    tuple(row[0] for row in page["messages"])
                )
            }
            self.assertFalse(received & observed)
            received.update(observed)
            if previous_delivery is None:
                self.assertEqual(
                    self.repository.update_position("codex", self.group, "conversation", "*"),
                    before,
                )
            self.assertLessEqual(len(observed), 200)
            if plan.operation == "range":
                receipt = page["source_receipt"]["updates_reconciliation"]
                scan_pages.append(receipt)
                self.assertFalse(page["source_receipt"]["complete"])
                self.assertFalse(receipt["cross_page_snapshot"])
                state = self.repository.source_conversation_state(self.group)
                assert state is not None
                self.assertEqual(
                    tuple(
                        state[key]
                        for key in (
                            "tail_sort_primary",
                            "forward_complete",
                            "history_complete",
                            "backfill_state",
                        )
                    ),
                    background_position,
                )
            previous_delivery = current_delivery
            if expected <= received:
                break
        else:
            self.fail("captured forward batches did not reach every generated source row")
        self.assertEqual(operations[0], "verify")
        self.assertEqual(operations[1:], ["range", "range", "range"])
        self.assertEqual([page["page_messages"] for page in scan_pages[:2]], [200, 200])
        self.assertLess(scan_pages[2]["page_messages"], 200)
        self.assertEqual([page["pass_completed"] for page in scan_pages], [False, False, True])
        self.assertEqual(self.service.replica.fresh_updates_reconcile_position(self.group)[0], None)
        self.assertEqual(received & expected, expected)
        self.assertIsNotNone(previous_delivery)

    def test_actual_fresh_cursor_boundary_is_verified_without_entering_the_next_page(self) -> None:
        expected = self.append_source(225)
        self.offline()
        cursor = None
        received: set[str] = set()
        for _ in range(4):
            arguments = {
                "mode": "recent",
                "conversation_id": self.group,
                "limit": 500,
                "projection": "compact",
                "view": "fresh",
                "cursor": cursor,
            }
            plan, frozen = self.capture(arguments)
            if cursor is not None:
                self.assertEqual(plan.operation, "range")
                self.assertEqual(len(plan.message_ids), 1)
                self.assertEqual(plan.limit, 199)
            with self.service.captured_provider(frozen):
                page = self.service.read_messages(**arguments)
            rows = self.repository.frozen_message_rows(tuple(row[0] for row in page["messages"]))
            source_ids = {str(row["source_message_id"]) for row in rows}
            self.assertFalse(received & source_ids)
            if cursor is not None:
                self.assertFalse(set(plan.message_ids) & source_ids)
            received.update(source_ids)
            cursor = page["page"]["next_cursor"]
            if not cursor:
                break
        else:
            self.fail("bounded fresh timeline continuation failed to terminate")
        self.assertEqual(received & expected, expected)

    def test_actual_fresh_correction_cannot_ack_across_unverified_observations(self) -> None:
        expected = self.append_source(201, prefix="synthetic-correction")
        self.service.sync_source_once(batch_limit=500, conversation_limit=100)
        self.offline()
        arguments = {
            "mode": "updates",
            "conversation_id": self.group,
            "limit": 500,
            "projection": "compact",
            "view": "fresh",
        }
        original_plan = self.service.replica.prepare_fresh_message_capture(arguments)
        assert original_plan.updates_message_ids is not None
        self.assertEqual(len(original_plan.updates_message_ids), 200)
        selected = next(
            row
            for row in self.repository.frozen_message_rows(original_plan.updates_message_ids)
            if row["source_message_id"] == "synthetic-correction-0"
        )
        corrected_id = str(selected["message_id"])
        with closing(sqlite3.connect(self.source_root / "messages-2.db")) as connection:
            connection.execute(
                "UPDATE messages SET raw_content=? WHERE source_message_id=?",
                (
                    "wxid_demo_member:\nSynthetic corrected canonical body",
                    selected["source_message_id"],
                ),
            )
            connection.commit()
        plan, frozen = self.capture(arguments)
        with self.service.captured_provider(
            frozen,
            updates_after=plan.after,
            updates_message_ids=plan.updates_message_ids,
            updates_reconcile_revision=plan.updates_reconcile_revision,
        ):
            first = self.service.read_messages(**arguments)
        disclosed = {row[0] for row in first["messages"]}
        self.assertEqual(len(disclosed), 199)
        self.assertNotIn(corrected_id, disclosed)
        self.assertTrue(first["page"]["has_more_after"])
        remaining = self.repository.observation_rows_after(
            self.group,
            int(self.delivery(first["page"]["delivery_id"])["to_observation_seq"]),
            (),
            projection_epoch=self.service._projection_inventory_epoch(),
            limit=200,
        )
        self.assertIn(corrected_id, {row["message_id"] for row in remaining})
        next_arguments = {**arguments, "ack_delivery_id": first["page"]["delivery_id"]}
        next_plan, next_frozen = self.capture(next_arguments)
        self.assertEqual(next_plan.operation, "verify")
        with self.service.captured_provider(
            next_frozen,
            updates_after=next_plan.after,
            updates_message_ids=next_plan.updates_message_ids,
            updates_reconcile_revision=next_plan.updates_reconcile_revision,
        ):
            second = self.service.read_messages(**next_arguments)
        second_ids = {row[0] for row in second["messages"]}
        self.assertIn(corrected_id, second_ids)
        self.assertFalse(disclosed & second_ids)
        self.assertEqual(self.delivery(first["page"]["delivery_id"])["status"], "acknowledged")
        rows = self.repository.frozen_message_rows(tuple(disclosed | second_ids))
        self.assertEqual({row["source_message_id"] for row in rows} & expected, expected)
        self.assertEqual(
            next(row["text"] for row in rows if row["message_id"] == corrected_id),
            "Synthetic corrected canonical body",
        )

    def test_actual_fresh_ack_outcome_survives_reader_and_database_restart_offline(self) -> None:
        self.offline()
        arguments = {
            "mode": "updates",
            "conversation_id": self.group,
            "limit": 2,
            "view": "fresh",
        }
        plan, frozen = self.capture(arguments)
        with self.service.captured_provider(
            frozen,
            updates_after=plan.after,
            updates_message_ids=plan.updates_message_ids,
            updates_reconcile_revision=plan.updates_reconcile_revision,
        ):
            first = self.service.read_messages(**arguments)
        retry = {
            **arguments,
            "ack_delivery_id": first["page"]["delivery_id"],
            "request_id": "synthetic-restarted-fresh-outcome",
        }
        next_plan, next_frozen = self.capture(retry)
        with self.service.captured_provider(
            next_frozen,
            updates_after=next_plan.after,
            updates_message_ids=next_plan.updates_message_ids,
            updates_reconcile_revision=next_plan.updates_reconcile_revision,
        ):
            committed = self.service.read_messages(**retry)
        delivery = self.delivery(committed["page"]["delivery_id"])
        original_bytes = Path(delivery["payload_ref"]).read_bytes()
        restarted_db = WindowDB(self.repository.database.path)
        restarted = ReaderService(
            OfflineSource(self.provider),  # type: ignore[arg-type]
            WindowRepository(restarted_db),
            self.service.reader,
            self.service.token_codec,
            default_view="replica",
        )
        self.assertTrue(restarted.local_only_tool_call("wechat_read_messages", retry))
        with patch.object(
            restarted_db, "transaction", side_effect=AssertionError("restart replay took writer")
        ):
            self.assertEqual(restarted.read_messages(**retry), committed)
        self.assertEqual(Path(delivery["payload_ref"]).read_bytes(), original_bytes)
        self.assertEqual(self.delivery(first["page"]["delivery_id"])["status"], "acknowledged")

    def test_request_replay_horizon_protects_acknowledged_spool_until_expiry(self) -> None:
        self.offline()
        request = {
            "mode": "updates",
            "conversation_id": self.group,
            "limit": 2,
            "request_id": "synthetic-expiring-request-outcome",
        }
        page = self.service.read_messages(**request)
        delivery = self.delivery(page["page"]["delivery_id"])
        spool = Path(delivery["payload_ref"])
        next_page = self.service.read_messages(
            mode="updates",
            conversation_id=self.group,
            limit=2,
            ack_delivery_id=delivery["delivery_id"],
        )
        pending_spool = Path(self.delivery(next_page["page"]["delivery_id"])["payload_ref"])
        self.assertEqual(delivery["delivery_id"], spool.stem)
        self.assertEqual(
            cleanup_deliveries(self.repository.database, apply=True)["removed_count"], 0
        )
        self.assertTrue(spool.exists())
        self.assertEqual(self.service.read_messages(**request), page)
        future = datetime.now(UTC) + timedelta(days=31)
        with patch("sightglass.reader.replica.utc_now", return_value=future):
            with self.repository.database.connection() as connection:
                self.assertNotIn(spool.stem, request_spool_ids(connection))
            self.assert_error(ErrorCode.CURSOR_STALE, self.service.read_messages, **request)
            reclaimed = cleanup_deliveries(self.repository.database, apply=True)
        self.assertEqual(reclaimed["removed_count"], 1)
        self.assertFalse(spool.exists())
        self.assertTrue(pending_spool.exists())

    def test_actual_zero_match_captured_scope_ack_moves_only_that_scope(self) -> None:
        self.append_source(225)
        self.offline()
        ack = None
        source_positions = []
        scan_pages = []
        for index in range(4):
            arguments = {
                "mode": "updates",
                "conversation_id": self.group,
                "view": "fresh",
                "query": "synthetic-filter-without-hit",
                "ack_delivery_id": ack,
                "request_id": f"synthetic-empty-capture-{index}",
            }
            plan, frozen = self.capture(arguments)
            source_positions.append(plan.after)
            self.assertEqual(plan.participant_filters, ())
            with self.service.captured_provider(
                frozen,
                updates_after=plan.after,
                updates_message_ids=plan.updates_message_ids,
                updates_reconcile_revision=plan.updates_reconcile_revision,
            ):
                page = self.service.read_messages(**arguments)
            if plan.operation == "range":
                scan_pages.append(page["source_receipt"]["updates_reconciliation"])
            self.assertEqual(page["messages"], [])
            if index < 3:
                self.assertIsNotNone(page["page"]["delivery_id"])
            ack = page["page"]["delivery_id"]
        self.assertIsNotNone(source_positions[2])
        self.assertIsNone(source_positions[3])
        self.assertEqual([page["pass_completed"] for page in scan_pages], [False, True, False])
        self.assertEqual([page["pass"] for page in scan_pages], [0, 0, 1])
        self.assertEqual(
            self.repository.update_position("codex", self.group, "conversation", "*"), 0
        )

    def test_reconciliation_restart_finds_old_unseen_correction_and_new_shard_next_pass(
        self,
    ) -> None:
        expected = self.append_source(225)
        self.offline()
        arguments = {
            "mode": "updates",
            "conversation_id": self.group,
            "view": "fresh",
            "limit": 500,
            "projection": "compact",
        }
        ack = None
        received = set()
        for _ in range(3):
            plan, page = self.read_capture({**arguments, "ack_delivery_id": ack})
            ack = page["page"]["delivery_id"]
            received.update(
                str(row["source_message_id"])
                for row in self.repository.frozen_message_rows(
                    tuple(row[0] for row in page["messages"])
                )
            )
        self.assertEqual(received & expected, expected)
        self.assertEqual(plan.operation, "range")
        self.assertTrue(page["source_receipt"]["updates_reconciliation"]["pass_completed"])
        self.service.read_messages(**{**arguments, "view": "replica", "ack_delivery_id": ack})
        committed = self.repository.update_position("codex", self.group, "conversation", "*")
        timeline = self.service.replica.fresh_updates_after(self.group)
        assert timeline is not None

        # Start another pass, then mutate keys behind its already captured page.
        plan, head = self.read_capture(arguments)
        self.assertIsNone(plan.after)
        self.assertEqual(head["messages"], [])
        self.assertIsNotNone(head["page"]["delivery_id"])
        boundary, revision, scan_pass = self.service.replica.fresh_updates_reconcile_position(
            self.group
        )
        self.assertIsNotNone(boundary)
        self.assertEqual(scan_pass, 1)
        old_time = (self.moment - timedelta(days=1)).isoformat()
        unseen = _row(
            "synthetic-old-unseen",
            "conv_group",
            old_time,
            0,
            900_001,
            1,
            "wxid_demo_member:\nSynthetic old unseen body",
            sender="wxid_demo_member",
        )
        columns = tuple(unseen)
        with closing(sqlite3.connect(self.source_root / "messages-2.db")) as connection:
            connection.execute(
                f"INSERT INTO messages ({','.join(columns)}) VALUES "
                f"({','.join('?' for _ in columns)})",
                tuple(unseen[key] for key in columns),
            )
            connection.execute(
                "UPDATE messages SET raw_content=? WHERE source_message_id=?",
                ("wxid_demo_member:\nSynthetic canonical old-key correction", "synthetic-range-0"),
            )
            connection.commit()
        new_shard_row = _row(
            "synthetic-new-shard-old-key",
            "conv_group",
            old_time,
            1,
            900_002,
            1,
            "wxid_demo_member:\nSynthetic old-key shard row",
            sender="wxid_demo_member",
        )
        _create_shard(self.source_root / "messages-3.db", [new_shard_row])
        manifest_path = self.source_root / "source.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["shards"].append(
            {
                "logical_key": "message-shard-3",
                "file": "messages-3.db",
                "generation_id": "synthetic-generation-3a",
            }
        )
        manifest_path.write_text(json.dumps(manifest))

        # The private scan position, not a source-sort reader ACK, survives restart.
        self.restart_reader()
        self.assertEqual(
            self.service.replica.fresh_updates_reconcile_position(self.group),
            (boundary, revision, scan_pass),
        )
        plan, tail = self.read_capture(
            {**arguments, "ack_delivery_id": head["page"]["delivery_id"]}
        )
        self.assertEqual(plan.after, boundary)
        self.assertTrue(tail["source_receipt"]["updates_reconciliation"]["pass_completed"])
        self.assertEqual(tail["messages"], [])
        corrected = next(
            row
            for row in self.repository.frozen_message_rows(
                tuple(
                    row[0]
                    for row in self.service.read_messages(
                        mode="recent", conversation_id=self.group, limit=500
                    )["messages"]
                )
            )
            if row["source_message_id"] == "synthetic-range-0"
        )
        self.assertNotEqual(corrected["text"], "Synthetic canonical old-key correction")

        plan, changed = self.read_capture(
            {
                **arguments,
                "ack_delivery_id": tail["page"]["delivery_id"],
            }
        )
        self.assertIsNone(plan.after)
        rows = self.repository.frozen_message_rows(tuple(row[0] for row in changed["messages"]))
        wanted = {"synthetic-old-unseen", "synthetic-new-shard-old-key", "synthetic-range-0"}
        self.assertEqual({str(row["source_message_id"]) for row in rows}, wanted)
        for row in rows:
            self.assertLess(self.service._row_sort_key(row).as_tuple(), timeline.as_tuple())
            self.assertEqual(row["current_state"], "present")
        corrected = next(row for row in rows if row["source_message_id"] == "synthetic-range-0")
        self.assertEqual(corrected["text"], "Synthetic canonical old-key correction")
        with self.repository.database.connection() as connection:
            self.assertGreaterEqual(
                connection.execute(
                    "SELECT count(*) FROM message_observations WHERE message_id=?",
                    (corrected["message_id"],),
                ).fetchone()[0],
                2,
            )
        self.assertEqual(changed["markers"]["late_arrival"], list(range(len(rows))))
        self.assertEqual(
            self.repository.update_position("codex", self.group, "conversation", "*"), committed
        )
        self.assertFalse(changed["source_receipt"]["complete"])
        self.assertFalse(changed["source_receipt"]["updates_reconciliation"]["cross_page_snapshot"])
        _, final = self.read_capture(
            {
                **arguments,
                "ack_delivery_id": changed["page"]["delivery_id"],
            }
        )
        self.assertTrue(final["source_receipt"]["updates_reconciliation"]["pass_completed"])
        self.assertEqual(self.service.replica.fresh_updates_reconcile_position(self.group)[0], None)
        self.assertGreater(
            self.service.replica.fresh_updates_reconcile_position(self.group)[2], scan_pass
        )

    def test_missing_first_known_capture_id_cannot_skip_the_other_199(self) -> None:
        self.append_source(201)
        self.service.sync_source_once(batch_limit=500, conversation_limit=100)
        self.offline()
        arguments = {
            "mode": "updates",
            "conversation_id": self.group,
            "view": "fresh",
            "limit": 500,
            "projection": "compact",
        }
        plan = self.service.replica.prepare_fresh_message_capture(arguments)
        self.assertEqual(plan.operation, "verify")
        assert plan.updates_message_ids is not None
        self.assertEqual(len(plan.updates_message_ids), 200)
        first_id = plan.message_ids[0]
        for name in ("messages-1.db", "messages-2.db"):
            with closing(sqlite3.connect(self.source_root / name)) as connection:
                connection.execute("DELETE FROM messages WHERE source_message_id=?", (first_id,))
                connection.commit()
        watermark = self.repository.observation_watermark()
        plan, frozen = self.capture(arguments)
        callbacks = []
        with self.service.captured_provider(
            frozen,
            updates_message_ids=plan.updates_message_ids,
            before_commit=lambda _connection: callbacks.append("admitted"),
        ):
            self.assert_error(ErrorCode.SOURCE_INCOMPLETE, self.service.read_messages, **arguments)
        self.assertEqual(callbacks, [])
        self.assertEqual(self.repository.observation_watermark(), watermark)
        self.assertEqual(
            self.repository.update_position("codex", self.group, "conversation", "*"), 0
        )
        self.assertIsNone(
            self.repository.pending_delivery("codex", self.group, "conversation", "*")
        )
        current = self.repository.message_position_row(plan.updates_message_ids[0])
        assert current is not None
        self.assertEqual(current["current_state"], "present")
        self.assertEqual(
            self.service.replica.fresh_updates_reconcile_position(self.group), (None, 0, 0)
        )

    def test_two_windowdb_writers_publish_one_pending_and_reverse_ack_never_regresses(self) -> None:
        self.offline()
        self.service._ensure_update_reader_profile()
        services = [
            ReaderService(
                OfflineSource(self.provider),  # type: ignore[arg-type]
                WindowRepository(WindowDB(self.repository.database.path)),
                self.service.reader,
                self.service.token_codec,
                default_view="replica",
            )
            for _ in range(2)
        ]
        barrier = threading.Barrier(2)
        calls = [0, 0]
        pending_methods = [service.repository.pending_delivery for service in services]

        def race_pending(index):
            def pending(*args):
                row = pending_methods[index](*args)
                calls[index] += 1
                if calls[index] == 1:
                    self.assertIsNone(row)
                    barrier.wait(timeout=5)
                return row

            return pending

        arguments = {
            "mode": "updates",
            "conversation_id": self.group,
            "view": "replica",
            "limit": 2,
        }
        with (
            patch.object(services[0].repository, "pending_delivery", race_pending(0)),
            patch.object(services[1].repository, "pending_delivery", race_pending(1)),
            ThreadPoolExecutor(max_workers=2) as workers,
        ):
            futures = [
                workers.submit(
                    service.read_messages,
                    **arguments,
                    request_id=f"synthetic-raced-request-{index}",
                )
                for index, service in enumerate(services)
            ]
            pages = [future.result(timeout=10) for future in futures]
        self.assertEqual(pages[0], pages[1])
        self.assertGreaterEqual(min(calls), 2)
        with self.repository.database.connection() as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM reader_deliveries WHERE reader_id='codex' "
                    "AND conversation_id=? AND status='pending'",
                    (self.group,),
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM access_receipts WHERE tool_name=?",
                    (UPDATE_REQUEST_TOOL,),
                ).fetchone()[0],
                2,
            )
        for index, service in enumerate(services):
            self.assertEqual(
                service.read_messages(**arguments, request_id=f"synthetic-raced-request-{index}"),
                pages[0],
            )
        fresh_retry = {**arguments, "view": "fresh", "request_id": "synthetic-pending-fresh-retry"}
        self.assertTrue(self.service.local_only_tool_call("wechat_read_messages", fresh_retry))
        self.assertEqual(self.service.read_messages(**fresh_retry), pages[0])
        second = self.service.read_messages(
            **arguments, ack_delivery_id=pages[0]["page"]["delivery_id"]
        )
        third = self.service.read_messages(
            **arguments, ack_delivery_id=second["page"]["delivery_id"]
        )
        committed = self.repository.update_position("codex", self.group, "conversation", "*")
        timeline = self.repository.timeline_position("codex", self.group, "conversation", "*")
        timeline = dict(timeline) if timeline else None
        self.assertEqual(
            self.service.read_messages(
                **arguments, ack_delivery_id=pages[0]["page"]["delivery_id"]
            ),
            third,
        )
        self.assertEqual(
            self.repository.update_position("codex", self.group, "conversation", "*"), committed
        )
        current_timeline = self.repository.timeline_position(
            "codex", self.group, "conversation", "*"
        )
        self.assertEqual(dict(current_timeline) if current_timeline else None, timeline)

    def test_current_policy_principal_and_pause_override_durable_request_and_resource_replay(
        self,
    ) -> None:
        resources = self.tools.wechat_find_resources(conversation_ids=[self.group])["items"]
        resource_id = resources[0]["resource"]["resource_id"]
        resource_arguments = {
            "resource_id": resource_id,
            "mode": "metadata",
            "page": None,
            "start_line": None,
            "end_line": None,
            "max_bytes": 4096,
        }
        self.service.read_resource(**resource_arguments)
        self.service.read_resource(**{**resource_arguments, "mode": "original"})
        self.offline()
        request = {
            "mode": "updates",
            "conversation_id": self.group,
            "request_id": "synthetic-revocable-outcome",
            "limit": 2,
        }
        original = self.service.read_messages(**request)
        original_context = self.service.reader
        revoked = replace(original_context, policy=ReaderPolicy(mode="allowlist"))
        denied = ReaderService(
            OfflineSource(self.provider),  # type: ignore[arg-type]
            self.repository,
            revoked,
            self.service.token_codec,
            default_view="replica",
        )
        self.repository.upsert_reader(
            revoked.reader_id,
            revoked.display_name,
            revoked.policy.as_dict(),
            datetime.now(UTC).isoformat(),
        )
        self.assert_error(ErrorCode.POLICY_DENIED, denied.read_messages, **request)
        self.assert_error(ErrorCode.POLICY_DENIED, denied.read_resource, **resource_arguments)
        self.assert_error(
            ErrorCode.POLICY_DENIED,
            denied.read_resource,
            **{**resource_arguments, "mode": "original"},
        )
        paused = ReaderService(
            OfflineSource(self.provider),  # type: ignore[arg-type]
            self.repository,
            replace(original_context, paused=True),
            self.service.token_codec,
            default_view="replica",
        )
        self.assert_error(ErrorCode.SERVICE_PAUSED, paused.read_messages, **request)
        self.assert_error(ErrorCode.SERVICE_PAUSED, paused.read_resource, **resource_arguments)
        self.repository.upsert_reader(
            original_context.reader_id,
            original_context.display_name,
            original_context.policy.as_dict(),
            datetime.now(UTC).isoformat(),
        )
        self.assert_error(ErrorCode.QUERY_INVALID, self.service.read_messages, **request)
        self.assertEqual(self.delivery(original["page"]["delivery_id"])["status"], "expired")
        other = ReaderService(
            OfflineSource(self.provider),  # type: ignore[arg-type]
            self.repository,
            replace(original_context, reader_id="synthetic-other-reader"),
            self.service.token_codec,
            default_view="replica",
        )
        other_page = other.read_messages(**request)
        self.assertNotEqual(other_page["page"]["delivery_id"], original["page"]["delivery_id"])
        self.assert_error(
            ErrorCode.DELIVERY_ACK_INVALID,
            other.read_messages,
            **{**request, "request_id": None, "ack_delivery_id": original["page"]["delivery_id"]},
        )

    def test_policy_widening_restarts_reconciliation_without_rewinding_reader_ack(self) -> None:
        self.append_source(225)
        self.offline()
        arguments = {
            "mode": "updates",
            "conversation_id": self.group,
            "view": "fresh",
            "query": "synthetic-never-matches",
            "participant_ids": (self.seed["sender_id"],),
        }
        _, known = self.read_capture(arguments)
        plan, page = self.read_capture(
            {**arguments, "ack_delivery_id": known["page"]["delivery_id"]}
        )
        self.assertEqual(plan.operation, "range")
        self.assertEqual(plan.participant_filters, ())
        self.assertEqual(page["messages"], [])
        old_scan = self.service.replica.fresh_updates_reconcile_position(self.group)
        self.assertIsNotNone(old_scan[0])
        scope_kind, scope_key = cursor_scope(arguments["participant_ids"], arguments["query"])
        self.service.read_messages(
            **{**arguments, "view": "replica", "ack_delivery_id": page["page"]["delivery_id"]}
        )
        committed = self.repository.update_position("codex", self.group, scope_kind, scope_key)
        self.assertGreater(committed, 0)
        original_context = self.service.reader
        narrower = replace(
            original_context,
            policy=replace(
                original_context.policy,
                mode="allowlist",
                allowed_conversation_ids=frozenset({self.group}),
            ),
        )
        self.service.reader = narrower
        self.service._ensure_update_reader_profile()
        self.service.reader = original_context
        self.service._ensure_update_reader_profile()
        self.assertEqual(
            self.service.replica.fresh_updates_reconcile_position(self.group), (None, 0, 0)
        )
        self.assertEqual(
            self.repository.update_position("codex", self.group, scope_kind, scope_key), committed
        )
        next_plan = self.service.replica.prepare_fresh_message_capture(arguments)
        self.assertEqual(next_plan.operation, "range")
        self.assertIsNone(next_plan.after)
        self.assertEqual(next_plan.updates_reconcile_revision, 0)
        with patch.object(
            self.service, "_projection_inventory_epoch", return_value="synthetic-replacement-epoch"
        ):
            self.assertEqual(
                self.service.replica.fresh_updates_reconcile_position(self.group), (None, 0, 0)
            )
        # A wider scope first verifies admitted IDs the narrow scope never delivered.
        broad = self.service.replica.prepare_fresh_message_capture(
            {
                **arguments,
                "query": None,
                "participant_ids": (),
            }
        )
        self.assertEqual(broad.operation, "verify")
        self.assertTrue(broad.updates_message_ids)
        self.assertEqual(
            self.repository.update_position("codex", self.group, "conversation", "*"), 0
        )

    def test_reconciliation_revision_fences_an_origin_reset_after_a_complete_pass(self) -> None:
        self.offline()
        arguments = {
            "mode": "updates",
            "conversation_id": self.group,
            "view": "replica",
            "limit": 500,
        }
        for query in (None, "synthetic-distinct-empty-filter"):
            first = self.service.read_messages(**{**arguments, "query": query})
            self.service.read_messages(
                **{**arguments, "query": query, "ack_delivery_id": first["page"]["delivery_id"]}
            )
        fresh = {**arguments, "view": "fresh"}
        stale_plan, stale_capture = self.capture(fresh)
        self.assertEqual((stale_plan.after, stale_plan.updates_reconcile_revision), (None, 0))
        _, other_scope = self.read_capture({**fresh, "query": "synthetic-distinct-empty-filter"})
        self.assertTrue(other_scope["source_receipt"]["updates_reconciliation"]["pass_completed"])
        current = self.service.replica.fresh_updates_reconcile_position(self.group)
        self.assertEqual(current, (None, 1, 1))
        with self.service.captured_provider(
            stale_capture,
            updates_after=stale_plan.after,
            updates_reconcile_revision=stale_plan.updates_reconcile_revision,
        ):
            self.assert_error(ErrorCode.CURSOR_STALE, self.service.read_messages, **fresh)
        self.assertEqual(self.service.replica.fresh_updates_reconcile_position(self.group), current)
        self.assertIsNone(
            self.repository.pending_delivery("codex", self.group, "conversation", "*")
        )

    def test_capture_final_writer_failure_rolls_ack_scan_ingestion_and_request_outcome_back(
        self,
    ) -> None:
        self.append_source(225)
        self.offline()
        arguments = {
            "mode": "updates",
            "conversation_id": self.group,
            "view": "fresh",
            "limit": 500,
        }
        _, initial = self.read_capture(arguments)
        old_delivery = initial["page"]["delivery_id"]
        retry = {
            **arguments,
            "ack_delivery_id": old_delivery,
            "request_id": "synthetic-reconciliation-atomic-retry",
        }
        plan, frozen = self.capture(retry)
        self.assertEqual(plan.operation, "range")
        watermark = self.repository.observation_watermark()
        callbacks = []

        def fail_at_terminal(connection):
            self.assertIs(connection, self.repository.database._active_connection.get())
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM reader_deliveries WHERE delivery_id=?",
                    (old_delivery,),
                ).fetchone()[0],
                "acknowledged",
            )
            self.assertGreater(self.repository.observation_watermark(), watermark)
            self.assertEqual(
                self.service.replica.fresh_updates_reconcile_position(self.group)[1], 1
            )
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM access_receipts WHERE tool_name=?",
                    (UPDATE_REQUEST_TOOL,),
                ).fetchone()[0],
                1,
            )
            callbacks.append("terminal")
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED)

        with self.service.captured_provider(
            frozen,
            updates_after=plan.after,
            updates_reconcile_revision=plan.updates_reconcile_revision,
            before_commit=fail_at_terminal,
            after_commit=lambda: callbacks.append("released"),
        ):
            self.assert_error(
                ErrorCode.SOURCE_GENERATION_CHANGED, self.service.read_messages, **retry
            )
        self.assertEqual(callbacks, ["terminal"])
        self.assertEqual(self.repository.observation_watermark(), watermark)
        self.assertEqual(
            self.service.replica.fresh_updates_reconcile_position(self.group), (None, 0, 0)
        )
        self.assertEqual(self.delivery(old_delivery)["status"], "pending")
        self.assertEqual(
            self.repository.update_position("codex", self.group, "conversation", "*"), 0
        )
        with self.repository.database.connection() as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM access_receipts WHERE tool_name=?",
                    (UPDATE_REQUEST_TOOL,),
                ).fetchone()[0],
                0,
            )
        with self.service.captured_provider(
            frozen,
            updates_after=plan.after,
            updates_reconcile_revision=plan.updates_reconcile_revision,
            after_commit=lambda: callbacks.append("released"),
        ):
            committed = self.service.read_messages(**retry)
        self.assertEqual(callbacks, ["terminal", "released"])
        self.assertEqual(self.service.read_messages(**retry), committed)
        self.assertEqual(self.delivery(old_delivery)["status"], "acknowledged")

    def test_private_receipt_digest_is_keyed_and_query_never_enters_journal(self) -> None:
        codec = SignedTokenCodec(b"synthetic receipt purpose root secret")
        payload = {"query": "Synthetic dictionary phrase", "scope": [self.group]}
        token_before = codec.encode(payload)
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        digest = codec.private_digest("updates-request.v1", payload)
        self.assertNotEqual(digest, hashlib.sha256(canonical).hexdigest())
        self.assertNotEqual(digest, codec.private_digest("access-scope.v1", payload))
        self.assertNotEqual(
            digest,
            SignedTokenCodec(b"another synthetic purpose root secret").private_digest(
                "updates-request.v1", payload
            ),
        )
        self.assertEqual(codec.encode(payload), token_before)
        self.assertEqual(codec.decode(token_before), payload)
        self.offline()
        query = "Synthetic private phrase must remain transient"
        arguments = {
            "mode": "updates",
            "conversation_id": self.group,
            "query": query,
            "request_id": "synthetic-keyed-outcome",
        }
        with patch.object(
            self.service.token_codec,
            "private_digest",
            wraps=self.service.token_codec.private_digest,
        ) as private_digest:
            page = self.service.read_messages(**arguments)
        domain, request_payload = private_digest.call_args.args
        self.assertEqual(domain, "updates-request.v1")
        canonical = json.dumps(request_payload, sort_keys=True, separators=(",", ":")).encode()
        self.service.record_access_receipt(
            tool_name="wechat_search_messages",
            conversation_id=self.group,
            scope_kind="query",
            scope_values=(query,),
            result={"hits": [], "query": query},
            started_at=datetime.now(UTC).isoformat(),
        )
        with self.repository.database.connection() as connection:
            rows = connection.execute("SELECT * FROM access_receipts").fetchall()
        persisted = json.dumps([dict(row) for row in rows])
        self.assertNotIn(query, persisted)
        outcome = next(row for row in rows if row["tool_name"] == UPDATE_REQUEST_TOOL)
        self.assertNotEqual(outcome["scope_digest"], hashlib.sha256(canonical).hexdigest())
        self.assertEqual(
            outcome["scope_digest"],
            self.service.token_codec.private_digest(domain, request_payload),
        )
        audit = next(row for row in rows if row["tool_name"] == "wechat_search_messages")
        self.assertEqual(
            audit["scope_digest"],
            self.service.token_codec.private_digest(
                "access-scope.v1", {"reader": "codex", "scope": [query]}
            ),
        )
        delivery = self.delivery(page["page"]["delivery_id"])
        bytes_before = Path(delivery["payload_ref"]).read_bytes()
        self.assertEqual(self.service.read_messages(**arguments), page)
        self.assertEqual(Path(delivery["payload_ref"]).read_bytes(), bytes_before)

    def test_offline_resource_discovery_uses_only_current_resident_projection(self) -> None:
        self.offline()
        with patch.object(
            self.service,
            "_prepare_discovery_candidates",
            side_effect=AssertionError("resource source preparation touched"),
        ):
            result = self.tools.wechat_find_resources(conversation_ids=[self.group])
        self.assertTrue(result["items"])
        self.assertEqual(result["source_receipt"]["view"], "replica")
        self.assertEqual(result["source_receipt"]["coverage"]["conversation"], "resident_subset")

    def _resource_message(self) -> Any:
        return next(
            row
            for row in self.repository.materialized_message_rows(
                self.group,
                projection_epoch=self.service._projection_inventory_epoch(),
                observation_watermark=self.repository.observation_watermark(),
                direction="forward",
                limit=100,
            )
            if any(
                item["original_name"] == "notes.md"
                for item in self.repository.resources_for_message(str(row["message_id"]))
            )
        )

    def test_replica_resource_list_is_local_in_partial_resident_scopes(self) -> None:
        row = self._resource_message()
        message_id = str(row["message_id"])
        expected = self.service.list_resources(message_id)["resources"]
        self.assertTrue(expected)
        for mode in ("recent", "on_demand"):
            with self.subTest(mode=mode):
                self.offline(partial_mode=mode)
                with (
                    patch.object(
                        self.provider, "snapshot", side_effect=AssertionError("source snapshot")
                    ),
                    patch.object(
                        self.provider, "get_message", side_effect=AssertionError("source message")
                    ),
                ):
                    self.assertTrue(
                        self.service.local_only_tool_call(
                            "wechat_list_resources", {"message_id": message_id}
                        )
                    )
                    listed = self.tools.wechat_list_resources(message_id)
                self.assertEqual(listed["schema"], "sightglass.resource-list.v1")
                self.assertEqual(listed["resources"], expected)
                receipt = listed["source_receipt"]
                self.assertEqual(
                    (receipt["served_from"], receipt["view"]), ("window_db", "replica")
                )
                self.assertEqual(receipt["freshness"]["state"], "bounded_stale")
                self.assertFalse(receipt["freshness"]["live_refresh_confirmed"])
                self.assertFalse(receipt["complete"])
                self.assertEqual(receipt["coverage"]["conversation"], "resident_subset")
                self.assertNotIn(str(self.source_root), str(listed))
                self.assertNotIn(str(row["source_message_id"]), str(listed))
                for item in listed["resources"]:
                    self.assertEqual(item["source_message_id"], message_id)
                    self.assertNotIn("source_resource_key", item)
                    self.assertNotIn("resolver_json", item)

    def test_replica_resource_list_honors_current_corrections_and_active_resolver(self) -> None:
        assert self.context is not None
        row = self._resource_message()
        message_id = str(row["message_id"])
        original_items = self.repository.resources_for_message(message_id)
        original_resource_id = original_items[0]["resource_id"]
        with self.provider.snapshot() as snapshot:
            source = self.provider.get_message(
                self.context["source_account_key"], str(row["source_message_id"]), snapshot
            )
        assert source is not None
        changed = replace(
            source,
            resources=(replace(source.resources[0], original_name="synthetic-corrected.md"),),
        )
        self.repository.upsert_message(
            self.account,
            self.group,
            row["sender_id"],
            row["sender_membership_id"],
            changed,
            parse_message(changed),
            projection_epoch=self.service._projection_inventory_epoch(),
        )
        current = self.repository.message_position_row(message_id)
        assert current is not None
        self.assertGreater(current["current_observation_seq"], row["current_observation_seq"])
        self.offline()
        with patch.object(self.provider, "snapshot", side_effect=AssertionError("source snapshot")):
            listed = self.service.list_resources(message_id)
            self.assertEqual(listed["resources"][0]["resource_id"], original_resource_id)
            self.assertEqual(listed["resources"][0]["original_name"], "synthetic-corrected.md")
            self.assertEqual(
                listed["source_receipt"]["freshness"]["observation_watermark"],
                self.repository.observation_watermark(),
            )
            removed = replace(changed, resources=())
            self.repository.upsert_message(
                self.account,
                self.group,
                row["sender_id"],
                row["sender_membership_id"],
                removed,
                parse_message(removed),
                projection_epoch=self.service._projection_inventory_epoch(),
            )
            self.assertEqual(self.service.list_resources(message_id)["resources"], [])
        retained = self.repository.resource_context(original_resource_id)
        assert retained is not None
        self.assertFalse(json.loads(retained["resolver_json"])["active"])

    def test_remote_resource_list_dispatch_never_requests_capture_or_snapshot(self) -> None:
        assert self.context is not None
        message_id = str(self._resource_message()["message_id"])
        remote = RemoteCaptureProvider(
            RemoteCaptureSettings(
                source_instance_id="synthetic-resource-list-source",
                account_id=str(self.context["source_account_key"]),
                conversations=frozenset({str(self.context["source_conversation_id"])}),
                egress_revision="synthetic-resource-list-egress",
                stream_epoch="synthetic-resource-list-stream",
                edge_token_hash=hashlib.sha256(b"synthetic-resource-list-edge-token").hexdigest(),
                socket_path=self.repository.database.path.parent / "synthetic-edge.sock",
                origin=self.provider.descriptor,
            )
        )
        replica = ReaderService(
            remote,
            self.repository,
            self.service.reader,
            self.service.token_codec,
            default_view="replica",
        )
        core = CoreCapture(
            replica,
            SightglassConfig(
                data_dir=self.repository.database.path.parent,
                source_root=None,
                window_db_path=self.repository.database.path,
                socket_path=self.repository.database.path.parent / "synthetic-reader.sock",
                source_kind="remote-capture",
                source_instance_id=remote.settings.source_instance_id,
                reader_default_view="replica",
                activation_generation="synthetic-resource-list-generation",
            ),
        )
        # No broker listener or edge is started. This uses the actual Core dispatch
        # with a generated remote binding whose provider has no direct read RPC.
        with (
            patch.object(
                remote, "snapshot", side_effect=AssertionError("remote snapshot")
            ) as opened,
            patch.object(
                core, "_submit", side_effect=AssertionError("capture request")
            ) as submitted,
        ):
            self.assertTrue(
                replica.local_only_tool_call("wechat_list_resources", {"message_id": message_id})
            )
            listed = core.call(
                "wechat_list_resources",
                {"message_id": message_id},
                lambda: replica.list_resources(message_id),
            )
        opened.assert_not_called()
        submitted.assert_not_called()
        self.assertTrue(listed["resources"])
        self.assertEqual(listed["source_receipt"]["view"], "replica")
        self.assertEqual(
            replica._projection_inventory_epoch(), self.service._projection_inventory_epoch()
        )

    def test_replica_resource_list_excludes_unavailable_current_message_bodies(self) -> None:
        assert self.context is not None
        template = self._resource_message()
        with self.provider.snapshot() as snapshot:
            source = self.provider.get_message(
                self.context["source_account_key"], str(template["source_message_id"]), snapshot
            )
        assert source is not None
        identities = []
        for index in range(6):
            cloned = replace(source, source_message_id=f"synthetic-list-ineligible-{index}")
            identities.append(
                self.repository.upsert_message(
                    self.account,
                    self.group,
                    template["sender_id"],
                    template["sender_membership_id"],
                    cloned,
                    parse_message(cloned),
                    projection_epoch=self.service._projection_inventory_epoch(),
                )
            )
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET body_available=0 WHERE message_id=?", (identities[0],)
            )
            connection.execute(
                "INSERT INTO message_body_residency VALUES "
                "(?,?,'on_demand',0,'1999-01-01','2000-01-01T00:00:00Z')",
                (identities[1], self.group),
            )
            connection.execute(
                "INSERT INTO body_release_jobs SELECT message_id,current_observation_seq,0 "
                "FROM messages WHERE message_id=?",
                (identities[2],),
            )
            connection.execute(
                "UPDATE messages SET projection_epoch='synthetic-old-epoch' WHERE message_id=?",
                (identities[3],),
            )
            connection.execute(
                "UPDATE messages SET current_state='recalled' WHERE message_id=?", (identities[4],)
            )
            connection.execute(
                "UPDATE messages SET current_observation_seq=NULL WHERE message_id=?",
                (identities[5],),
            )
        self.offline()
        with patch.object(self.provider, "snapshot", side_effect=AssertionError("source snapshot")):
            for message_id in identities:
                with self.subTest(message_id=message_id):
                    self.assertTrue(self.repository.resources_for_message(message_id))
                    self.assert_error(
                        ErrorCode.SOURCE_INCOMPLETE,
                        self.service.list_resources,
                        message_id=message_id,
                    )
            self.assert_error(
                ErrorCode.MESSAGE_NOT_FOUND,
                self.service.list_resources,
                message_id="wxmsg_synthetic_unobserved_resource_owner",
            )

    def test_replica_resource_list_rechecks_policy_pause_and_metadata_capability(self) -> None:
        message_id = str(self._resource_message()["message_id"])
        self.offline()
        reader = self.service.reader
        original_policy = reader.policy
        with patch.object(self.provider, "snapshot", side_effect=AssertionError("source snapshot")):
            self.assertTrue(self.service.list_resources(message_id)["resources"])
            for policy in (
                replace(original_policy, denied_conversation_ids=frozenset({self.group})),
                replace(original_policy, resource_metadata=False),
                replace(original_policy, messages=False),
            ):
                reader.policy = policy
                self.assert_error(
                    ErrorCode.POLICY_DENIED, self.service.list_resources, message_id=message_id
                )
            reader.policy = original_policy
            reader.paused = True
            self.assert_error(
                ErrorCode.SERVICE_PAUSED, self.service.list_resources, message_id=message_id
            )
            reader.paused = False
            with self.repository.database.transaction() as connection:
                connection.execute(
                    "UPDATE source_conversation_state SET last_error_code="
                    "'duplicate_message_identity_conflict' WHERE conversation_id=?",
                    (self.group,),
                )
            self.assert_error(
                ErrorCode.SOURCE_INCOMPLETE, self.service.list_resources, message_id=message_id
            )

    def test_auto_resource_list_still_confirms_source_metadata(self) -> None:
        row = self._resource_message()
        message_id = str(row["message_id"])
        self.assertFalse(
            self.service.local_only_tool_call("wechat_list_resources", {"message_id": message_id})
        )
        with closing(sqlite3.connect(self.source_root / "messages-2.db")) as connection:
            source = connection.execute(
                "SELECT resources_json FROM messages WHERE source_message_id=?",
                (row["source_message_id"],),
            ).fetchone()
            assert source is not None
            resources = json.loads(source[0])
            resources[0]["original_name"] = "synthetic-fresh-source-name.md"
            connection.execute(
                "UPDATE messages SET resources_json=? WHERE source_message_id=?",
                (json.dumps(resources), row["source_message_id"]),
            )
            connection.commit()
        self.service.default_view = "replica"
        with patch.object(self.provider, "snapshot", side_effect=AssertionError("source snapshot")):
            admitted = self.service.list_resources(message_id)
        self.assertEqual(admitted["resources"][0]["original_name"], "notes.md")
        self.assertFalse(admitted["source_receipt"]["freshness"]["live_refresh_confirmed"])
        self.service.default_view = "auto"
        with patch.object(self.provider, "snapshot", wraps=self.provider.snapshot) as opened:
            listed = self.service.list_resources(message_id)
        self.assertEqual(opened.call_count, 1)
        self.assertEqual(listed["resources"][0]["original_name"], "synthetic-fresh-source-name.md")
        self.assertTrue(listed["source_receipt"]["complete"])
        self.assertEqual(listed["source_receipt"]["freshness"]["mode"], "live_source")
        self.assertTrue(listed["source_receipt"]["freshness"]["live_refresh_confirmed"])
        with patch.object(
            self.provider, "snapshot", side_effect=SightglassError(ErrorCode.SERVICE_UNAVAILABLE)
        ):
            self.assert_error(
                ErrorCode.SERVICE_UNAVAILABLE, self.service.list_resources, message_id=message_id
            )

    def test_actual_fresh_range_tie_boundary_and_overlap_survive_restart(self) -> None:
        expected = self.append_tied_source()
        self.offline()
        arguments = {
            "mode": "range",
            "conversation_id": self.group,
            "view": "fresh",
            "limit": 500,
            "time_after": self.moment.isoformat(),
            "direction": "forward",
        }
        _, first = self.read_capture(arguments)
        first_rows = self.repository.frozen_message_rows(tuple(row[0] for row in first["messages"]))
        first_ids = {str(row["source_message_id"]) for row in first_rows}
        self.assertEqual(first_ids, {f"synthetic-tied-{index:04d}" for index in range(200)})
        self.assertEqual(
            {(row["sort_primary"], row["sort_seq"], row["sort_tie"]) for row in first_rows},
            {(self.moment.isoformat(timespec="microseconds"), 17, 700_000)},
        )
        cursor = first["page"]["next_cursor"]
        self.assertTrue(cursor)
        self.restart_reader()
        plan, second = self.read_capture({**arguments, "cursor": cursor})
        assert plan.after is not None
        self.assertEqual(plan.after.source_message_id, "synthetic-tied-0199")
        self.assertEqual(plan.message_ids, ("synthetic-tied-0199",))
        self.assertEqual(plan.limit, 199)
        second_rows = self.repository.frozen_message_rows(
            tuple(row[0] for row in second["messages"])
        )
        second_ids = {str(row["source_message_id"]) for row in second_rows}
        self.assertFalse(first_ids & second_ids)
        self.assertEqual(first_ids | second_ids, expected)
        self.assertEqual(len(second_ids), 25)
        self.assertIsNone(second["page"]["next_cursor"])
        self.assertTrue(
            all(
                self.service._row_sort_key(row).as_tuple() > plan.after.as_tuple()
                for row in second_rows
            )
        )

    def test_legacy_auto_continues_a_refresh_source_cursor_without_view(self) -> None:
        self.append_source(225)
        self.assertEqual(self.service.default_view, "auto")
        arguments = {"mode": "recent", "conversation_id": self.group, "limit": 2}
        first = self.service.read_messages(**arguments, refresh=True)
        cursor = first["page"]["next_cursor"]
        payload = self.service.token_codec.decode(cursor)
        self.assertEqual((payload["kind"], payload["view"]), ("timeline", "fresh"))
        continuation = {**arguments, "cursor": cursor}
        self.assertFalse(self.service.local_only_tool_call("wechat_read_messages", continuation))
        next_page = self.service.read_messages(**continuation)
        rows = self.repository.frozen_message_rows(tuple(row[0] for row in next_page["messages"]))
        self.assertEqual(
            {str(row["source_message_id"]) for row in rows},
            {"synthetic-range-221", "synthetic-range-222"},
        )
        self.assertEqual(next_page["source_receipt"]["view"], "fresh")
        self.assert_error(
            ErrorCode.CURSOR_INVALID,
            self.service.read_messages,
            **{**continuation, "view": "replica"},
        )
        # Already-issued legacy v2 tokens have no view field (or the auto value).
        # Their signed scope/body format stays compatible with the default call.
        for legacy_view in (None, "auto"):
            legacy_payload = dict(payload)
            if legacy_view is None:
                legacy_payload.pop("view")
            else:
                legacy_payload["view"] = legacy_view
            legacy_cursor = self.service.token_codec.encode(legacy_payload)
            legacy_page = self.service.read_messages(**{**arguments, "cursor": legacy_cursor})
            self.assertEqual(legacy_page["messages"], next_page["messages"])
        self.service.default_view = "replica"
        self.assertTrue(self.service.local_only_tool_call("wechat_read_messages", continuation))
        self.assert_error(ErrorCode.CURSOR_INVALID, self.service.read_messages, **continuation)

    def test_fresh_local_errors_are_classified_before_source_ownership(self) -> None:
        self.offline()
        cases: tuple[tuple[dict[str, Any], ErrorCode], ...] = (
            (
                {
                    "mode": "context",
                    "anchor": "synthetic-invalid-anchor",
                    "before": 0,
                    "after": 0,
                    "limit": 1,
                    "view": "fresh",
                },
                ErrorCode.CURSOR_INVALID,
            ),
            (
                {"mode": "message", "message_id": "wxmsg_synthetic_unobserved", "view": "fresh"},
                ErrorCode.MESSAGE_NOT_FOUND,
            ),
            (
                {"mode": "updates", "conversation_id": self.group, "refresh": True},
                ErrorCode.QUERY_INVALID,
            ),
            ({"mode": "recent", "refresh": "true"}, ErrorCode.QUERY_INVALID),
            (
                {"mode": "recent", "cursor": "synthetic-invalid", "refresh": True},
                ErrorCode.QUERY_INVALID,
            ),
            (
                {
                    "mode": "message",
                    "message_id": "wxmsg_synthetic_unobserved",
                    "projection": None,
                    "include_resources": "indicator",
                    "view": "fresh",
                },
                ErrorCode.QUERY_INVALID,
            ),
        )
        for arguments, code in cases:
            with self.subTest(code=code):
                self.assertTrue(
                    self.service.local_only_tool_call("wechat_read_messages", arguments)
                )
                self.assert_error(code, self.service.read_messages, **arguments)
        valid = {"mode": "recent", "conversation_id": self.group, "view": "fresh"}
        self.assertFalse(self.service.local_only_tool_call("wechat_read_messages", valid))
        self.assertFalse(
            self.service.local_only_tool_call(
                "wechat_read_messages",
                {
                    **valid,
                    "participant_ids": None,
                    "projection": None,
                    "limit": 100,
                },
            )
        )
        self.assertTrue(
            self.service.local_only_tool_call(
                "wechat_read_messages",
                {
                    "mode": "recent",
                    "conversation_id": self.group,
                    "participant_ids": None,
                    "projection": "detail",
                    "limit": 2,
                    "view": "replica",
                    "response_profile": None,
                },
            )
        )
        self.service.reader = replace(self.service.reader, policy=ReaderPolicy(mode="allowlist"))
        self.assertTrue(self.service.local_only_tool_call("wechat_read_messages", valid))
        self.assert_error(ErrorCode.POLICY_DENIED, self.service.read_messages, **valid)

    def test_actual_reconciliation_tie_boundary_and_overlap_survive_restart(self) -> None:
        expected = self.append_tied_source()
        self.offline()
        arguments = {
            "mode": "updates",
            "conversation_id": self.group,
            "view": "fresh",
            "limit": 500,
        }
        _, known = self.read_capture(arguments)
        plan, first = self.read_capture(
            {
                **arguments,
                "ack_delivery_id": known["page"]["delivery_id"],
            }
        )
        self.assertEqual(plan.operation, "range")
        self.assertEqual(first["source_receipt"]["updates_reconciliation"]["page_messages"], 200)
        first_rows = self.repository.frozen_message_rows(tuple(row[0] for row in first["messages"]))
        first_ids = {str(row["source_message_id"]) for row in first_rows}
        self.assertTrue(first_ids < expected)
        self.assertEqual(
            {(row["sort_primary"], row["sort_seq"], row["sort_tie"]) for row in first_rows},
            {(self.moment.isoformat(timespec="microseconds"), 17, 700_000)},
        )
        boundary, revision, scan_pass = self.service.replica.fresh_updates_reconcile_position(
            self.group
        )
        assert boundary is not None
        self.assertEqual(boundary.source_message_id, max(first_ids))
        self.restart_reader()
        self.assertEqual(
            self.service.replica.fresh_updates_reconcile_position(self.group),
            (boundary, revision, scan_pass),
        )
        plan, second = self.read_capture(
            {
                **arguments,
                "ack_delivery_id": first["page"]["delivery_id"],
            }
        )
        self.assertEqual(plan.operation, "range")
        self.assertEqual(plan.after, boundary)
        second_rows = self.repository.frozen_message_rows(
            tuple(row[0] for row in second["messages"])
        )
        second_ids = {str(row["source_message_id"]) for row in second_rows}
        self.assertFalse(first_ids & second_ids)
        self.assertEqual(first_ids | second_ids, expected)
        self.assertTrue(
            all(
                self.service._row_sort_key(row).as_tuple() > boundary.as_tuple()
                for row in second_rows
            )
        )
        self.assertTrue(second["source_receipt"]["updates_reconciliation"]["pass_completed"])
        self.assertEqual(self.service.replica.fresh_updates_reconcile_position(self.group)[0], None)


if __name__ == "__main__":
    unittest.main()

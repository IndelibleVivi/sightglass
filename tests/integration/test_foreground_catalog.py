"""Generated foreground catalog capture, independent of body collection."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from unittest.mock import patch

from tests.integration import test_daily_recents as daily_fixture


class ForegroundCatalogTests(daily_fixture.DailyRecentsTests):
    def test_new_inbox_captures_catalog_without_collecting_bodies(self):
        self.release_bodies()
        before = self.repository.source_conversation_state(self.direct())
        watermark = self.repository.observation_watermark()
        with closing(sqlite3.connect(self.source_root / "catalog.db")) as connection:
            connection.execute(
                "UPDATE conversations SET title=?,last_message_at_utc=? "
                "WHERE source_conversation_id='conv_direct'",
                ("Synthetic refreshed contact", "2026-10-30T00:00:00+00:00"),
            )
            connection.commit()
        self.start_edge()
        operations = []
        execute = self.executor._execute

        def observe(request, snapshot):
            operations.append(request.operation)
            return execute(request, snapshot)

        with patch.object(self.executor, "_execute", side_effect=observe):
            page = self.call("wechat_read_inbox", kinds=["direct"], include_latest="text")
        self.assertEqual(page["items"][0]["title"], "Synthetic refreshed contact")
        self.assertEqual(page["source_receipt"]["view"], "fresh")
        self.assertTrue(page["source_receipt"]["freshness"]["live_refresh_confirmed"])
        self.assertEqual(operations, ["catalog"])
        self.assertEqual(self.repository.observation_watermark(), watermark)
        self.assertEqual(self.repository.source_conversation_state(self.direct()), before)
        self.assertEqual(page["items"][0]["latest_state"], "not_resident")
        self.assertIsNone(self.spool.pending())

    def test_catalog_discovery_and_preview_only_confirm_metadata(self):
        from dataclasses import replace

        self.start_edge()
        original = self.provider.list_conversations

        def catalog(account, snapshot):
            return [
                replace(item, unread_count=7)
                if item.source_conversation_id == "conv_direct"
                else item
                for item in original(account, snapshot)
            ]

        with patch.object(self.provider, "list_conversations", side_effect=catalog):
            page = self.call(
                "wechat_read_inbox",
                kinds=["direct"],
                unread_only=True,
                include_latest="text",
                response_profile="diagnostic",
            )
            discovery = self.call("wechat_find_conversations", query="Demo Direct")
        self.assertEqual(page["items"][0]["unread_count"], 7)
        self.assertIsNotNone(page["items"][0]["latest"])
        self.assertEqual(page["source_receipt"]["preview_freshness"], "bounded_stale")
        self.assertEqual(page["source_receipt"]["freshness_scope"], "catalog_metadata")
        self.assertEqual(discovery["source_receipt"]["view"], "fresh")
        self.assertTrue(discovery["candidates"])
        self.assertIsNone(self.spool.pending())

    def test_offline_replica_resident_and_local_errors_never_capture(self):
        from dataclasses import replace

        from sightglass.contracts.errors import ErrorCode

        self.start_edge()
        with patch.object(self.core, "_submit", side_effect=AssertionError("unexpected capture")):
            for name, arguments in (
                ("wechat_read_inbox", {"view": "replica"}),
                ("wechat_read_inbox", {"mode": "resident"}),
                ("wechat_find_conversations", {"query": "", "view": "replica"}),
            ):
                self.assertTrue(self.service.local_only_tool_call(name, arguments))
                self.assertEqual(self.call(name, **arguments)["source_receipt"]["view"], "replica")
            for arguments, code in (
                ({"account_id": "synthetic-foreign-account"}, ErrorCode.ACCOUNT_NOT_FOUND.value),
                ({"cursor": "synthetic-invalid-token"}, ErrorCode.CURSOR_INVALID.value),
                ({"include_latest": "invalid"}, ErrorCode.QUERY_INVALID.value),
            ):
                self.assertTrue(self.service.local_only_tool_call("wechat_read_inbox", arguments))
                self.assertEqual(self.call("wechat_read_inbox", **arguments)["code"], code)
            self.service.reader.paused = True
            self.assertTrue(self.service.local_only_tool_call("wechat_read_inbox", {}))
            self.assertEqual(self.call("wechat_read_inbox")["code"], ErrorCode.SERVICE_PAUSED.value)
            self.service.reader.paused = False
            self.service.reader.policy = replace(self.service.reader.policy, messages=False)
            self.assertTrue(self.service.local_only_tool_call("wechat_read_inbox", {}))
            self.assertEqual(self.call("wechat_read_inbox")["code"], ErrorCode.POLICY_DENIED.value)
        self.service.reader.policy = replace(self.service.reader.policy, messages=True)
        self.stop_edge()
        with self.core.broker._condition:
            self.assertTrue(
                self.core.broker._condition.wait_for(lambda: not self.core.broker.connected, 2)
            )
        with patch.object(self.core, "_submit", side_effect=AssertionError("offline capture")):
            self.assertEqual(self.call("wechat_read_inbox")["source_receipt"]["view"], "replica")

    def test_catalog_view_selection_is_frozen_and_failure_never_falls_back(self):
        from sightglass.contracts.errors import ErrorCode, SightglassError

        selected = self.service.bind_catalog_view("wechat_read_inbox", {})
        self.assertEqual(selected["view"], "replica")
        self.start_edge()
        self.assertTrue(self.service.local_only_tool_call("wechat_read_inbox", selected))
        self.assertEqual(
            self.call("wechat_read_inbox", **selected)["source_receipt"]["view"], "replica"
        )
        fresh = self.service.bind_catalog_view("wechat_read_inbox", {})
        self.assertEqual(fresh["view"], "fresh")
        self.stop_edge()
        with self.core.broker._condition:
            self.assertTrue(
                self.core.broker._condition.wait_for(lambda: not self.core.broker.connected, 2)
            )
        self.assertFalse(self.service.local_only_tool_call("wechat_read_inbox", fresh))
        with self.assertRaises(SightglassError) as caught:
            self.call("wechat_read_inbox", **fresh)
        self.assertEqual(caught.exception.code, ErrorCode.SERVICE_UNAVAILABLE)

    def test_signed_fresh_replica_and_legacy_catalog_cursors_keep_plane(self):
        from sightglass.contracts.errors import ErrorCode

        for name, base in (("wechat_read_inbox", {}), ("wechat_find_conversations", {"query": ""})):
            for plane in ("replica", "fresh"):
                if plane == "fresh" and not self.core.broker.connected:
                    self.edge_stop.clear()
                    self.start_edge()
                first = self.call(name, **base, limit=1, view=plane)
                cursor = first["page"]["next_cursor"]
                self.assertTrue(cursor)
                original_as_of = first["source_receipt"].get("fresh_as_of")
                self.stop_edge()
                with self.core.broker._condition:
                    self.assertTrue(
                        self.core.broker._condition.wait_for(
                            lambda: not self.core.broker.connected, 2
                        )
                    )
                with patch.object(
                    self.core, "_submit", side_effect=AssertionError("cursor capture")
                ):
                    second = self.call(name, **base, limit=1, cursor=cursor)
                    self.assertEqual(second["source_receipt"]["view"], plane)
                    self.assertEqual(second["source_receipt"].get("fresh_as_of"), original_as_of)
                    self.assertFalse(
                        second["source_receipt"]["freshness"]["live_refresh_confirmed"]
                    )
                    wrong = self.call(
                        name,
                        **base,
                        limit=1,
                        cursor=cursor,
                        view="replica" if plane == "fresh" else "fresh",
                    )
                    self.assertEqual(wrong["code"], ErrorCode.CURSOR_INVALID.value)
                    if plane == "replica":
                        payload = self.service.token_codec.decode(cursor)
                        payload["snapshot"].pop("view", None)
                        payload["snapshot"].pop("catalog_as_of", None)
                        legacy = self.service.token_codec.encode(payload)
                        self.assertEqual(
                            self.call(name, **base, limit=1, cursor=legacy)["source_receipt"][
                                "view"
                            ],
                            "replica",
                        )

    def test_catalog_admission_failure_rolls_back_and_exactly_rejects(self):
        from sightglass.contracts.errors import ErrorCode, SightglassError

        self.start_edge()
        account = self.repository.active_account_ids()[0]
        before = dict(self.repository.source_catalog_state(account))
        with closing(sqlite3.connect(self.source_root / "catalog.db")) as connection:
            connection.execute(
                "UPDATE conversations SET title='Synthetic rejected title' "
                "WHERE source_conversation_id='conv_direct'"
            )
            connection.commit()
        with patch.object(
            self.repository,
            "record_source_catalog_state",
            side_effect=SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED),
        ):
            result = self.call("wechat_read_inbox")
        self.assertEqual(result["code"], ErrorCode.SOURCE_GENERATION_CHANGED.value)
        self.assertEqual(dict(self.repository.source_catalog_state(account)), before)
        self.assertNotEqual(
            self.repository.conversation_context(self.direct())["current_title"],
            "Synthetic rejected title",
        )
        self.assertIsNone(self.spool.pending())
        position = self.core.journal.stream_position(
            self.settings.source_instance_id, self.settings.account_id
        )
        self.assertEqual(position.next_sequence, 2)

    def test_fresh_catalog_preserves_ceiling_policy_epoch_and_conflict_guards(self):
        from dataclasses import replace

        original = self.provider.list_conversations

        def catalog(account, snapshot):
            rows = original(account, snapshot)
            return [
                *rows,
                replace(
                    rows[0],
                    source_conversation_id="synthetic-outside-ceiling",
                    title="Synthetic forbidden ceiling title",
                ),
            ]

        self.service.reader.policy = replace(
            self.service.reader.policy, denied_conversation_ids=frozenset({self.group})
        )
        from sightglass.runtime.capture_core import CoreCapture

        self.core.close()
        self.core = CoreCapture(self.service, self.config)
        self.core.start()
        self.addCleanup(self.core.close)
        self.addCleanup(self.stop_edge)
        self.start_edge()
        with self.repository.database.transaction() as connection:
            connection.execute("UPDATE messages SET projection_epoch='synthetic-old-epoch'")
        with patch.object(self.provider, "list_conversations", side_effect=catalog):
            page = self.call("wechat_read_inbox", include_latest="text")
        self.assertEqual([item["conversation_id"] for item in page["items"]], [self.direct()])
        self.assertNotIn("latest", page["items"][0])
        self.assertNotIn("Synthetic forbidden ceiling title", str(page))
        self.repository.mark_source_conversation_attention(
            conversation_id=self.direct(),
            backfill_state="partial",
            error_code="duplicate_message_identity_conflict",
            observed_at="2026-10-30T00:00:00+00:00",
        )
        conflicted = self.call("wechat_read_inbox", include_latest="text")
        self.assertEqual(conflicted["items"][0]["latest_state"], "source_conflict")
        self.assertNotIn("latest", conflicted["items"][0])

    def test_successful_metadata_confirmation_does_not_invent_new_source_mtime(self):
        from contextlib import contextmanager
        from dataclasses import replace

        self.start_edge()
        session = self.provider.session

        @contextmanager
        def unchanged(scope):
            with session(scope) as snapshot:
                yield replace(snapshot, fresh_as_of="2020-01-01T00:00:00+00:00")

        with patch.object(self.provider, "session", side_effect=unchanged):
            first = self.call("wechat_read_inbox")
            second = self.call("wechat_read_inbox")
        for page in (first, second):
            self.assertTrue(page["source_receipt"]["freshness"]["live_refresh_confirmed"])
            self.assertEqual(page["source_receipt"]["fresh_as_of"], "2020-01-01T00:00:00+00:00")

    def test_daemon_freezes_catalog_choice_before_classifying_its_lane(self):
        from sightglass.contracts.errors import ErrorCode

        self.prepare_catalog_daemon()
        self.stop_edge()
        with self.core.broker._condition:
            self.assertTrue(
                self.core.broker._condition.wait_for(lambda: not self.core.broker.connected, 2)
            )
        classify = self.daemon._local_only_tool_call

        def reconnect(params):
            self.assertEqual(params["arguments"]["view"], "replica")
            self.edge_stop.clear()
            self.start_edge()
            return classify(params)

        with (
            patch.object(self.daemon, "_local_only_tool_call", side_effect=reconnect),
            patch.object(self.core, "_submit", side_effect=AssertionError("local capture")),
        ):
            page = self.client().call("tools.call", {"name": "wechat_read_inbox", "arguments": {}})
        self.assertEqual(page["source_receipt"]["view"], "replica")

        def disconnect(params):
            self.assertEqual(params["arguments"]["view"], "fresh")
            self.stop_edge()
            with self.core.broker._condition:
                self.assertTrue(
                    self.core.broker._condition.wait_for(lambda: not self.core.broker.connected, 2)
                )
            return classify(params)

        with patch.object(self.daemon, "_local_only_tool_call", side_effect=disconnect):
            failed = self.client().call(
                "tools.call", {"name": "wechat_read_inbox", "arguments": {}}
            )
        self.assertEqual(failed["code"], ErrorCode.SERVICE_UNAVAILABLE.value)

        self.edge_stop.clear()
        self.start_edge()
        first = self.client().call(
            "tools.call", {"name": "wechat_read_inbox", "arguments": {"limit": 1}}
        )
        with patch.object(
            self.daemon.source_worker,
            "foreground_enter",
            side_effect=AssertionError("invalid scope claimed source"),
        ):
            invalid = self.client().call(
                "tools.call",
                {
                    "name": "wechat_read_inbox",
                    "arguments": {
                        "limit": 1,
                        "cursor": first["page"]["next_cursor"],
                        "kinds": ["group"],
                    },
                },
            )
        self.assertEqual(invalid["code"], ErrorCode.CURSOR_INVALID.value)

    def test_catalog_deadline_never_returns_a_cached_success(self):
        from sightglass.contracts.errors import ErrorCode, SightglassError

        self.start_edge()
        before = self.repository.observation_watermark()
        selected = self.service.bind_catalog_view("wechat_read_inbox", {})
        with patch.object(
            self.core, "_submit", side_effect=SightglassError(ErrorCode.SERVICE_TIMEOUT)
        ):
            with self.assertRaises(SightglassError) as caught:
                self.call("wechat_read_inbox", **selected)
        self.assertEqual(caught.exception.code, ErrorCode.SERVICE_TIMEOUT)
        self.assertEqual(self.repository.observation_watermark(), before)

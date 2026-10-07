from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.operations import check_operation_budget, local_read_only_scope
from sightglass.runtime.lanes import RuntimeLanes
from sightglass.runtime.search_preparation import MAX_JOBS, SCHEMA, SearchPreparation
from sightglass.runtime.source_worker import SourceWorker
from sightglass.source.synthetic import create_synthetic_source
from sightglass.storage import StorageBudget, StorageSettings
from tests.fixtures.factory import build_test_stack
from tests.integration import test_m4_daemon as daemon_fixture
from tests.integration import test_scoped_search_preparation as scoped


class AsyncSearchPreparationTests(unittest.TestCase):
    # Reuse only the deterministic fixture helpers, not the synchronous test suite.
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "source"
        create_synthetic_source(self.root)
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            self.root, Path(self.temporary.name) / "state" / "window.db"
        )
        self.service.storage = StorageBudget(
            self.repository.database.path.parent,
            self.repository.database.path,
            StorageSettings(min_free_bytes=0),
        )
        self.repository.database.storage = self.service.storage
        self.tools.wechat_status()
        self.group = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        self.worker = SourceWorker(self.service)
        self.path = self.repository.database.path.with_name("search-preparation.json")
        self.manager = self._manager()
        scoped.ScopedSearchPreparationTests._append(
            cast(Any, self),
            "conv_group",
            4,
            prefix="async-needle",
            start=datetime(2026, 10, 3, tzinfo=UTC),
        )
        self.arguments: dict[str, Any] = {
            "query": "async-needle",
            "conversation_ids": [self.group],
            "limit": 1,
            "after": "2026-10-03T00:00:00Z",
            "before": "2026-10-03T00:01:00Z",
        }

    def _manager(self, binding: str = "synthetic-binding") -> SearchPreparation:
        return SearchPreparation(
            self.service,
            self.path,
            binding=binding,
            lanes=RuntimeLanes(),
            source_worker=self.worker,
        )

    def tearDown(self) -> None:
        self.assertTrue(self.manager.stop())
        self.tools.close()
        self.temporary.cleanup()

    def _ready(self, token: str) -> dict:
        self.manager.start()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            page = self.tools.wechat_search_messages(**self.arguments, reading_token=token)
            if page.get("schema") != SCHEMA or page["state"] not in {"preparing", "ready"}:
                return page
            time.sleep(0.01)
        self.fail("synthetic asynchronous search did not finish")

    def test_first_page_is_processing_without_source_or_false_empty_hits(self) -> None:
        with patch.object(
            self.provider, "snapshot", side_effect=AssertionError("foreground source")
        ):
            result = self.tools.wechat_search_messages(**self.arguments)
            duplicate = self.tools.wechat_search_messages(**self.arguments)
        self.assertEqual(result["schema"], SCHEMA)
        self.assertEqual(result["state"], "preparing")
        self.assertNotIn("hits", result)
        self.assertFalse(result["results_complete"])
        self.assertEqual(result["reading_token"], duplicate["reading_token"])
        self.assertTrue(self.service.local_only_tool_call("wechat_search_messages", self.arguments))
        disk = self.path.read_text()
        self.assertNotIn('"query":', disk)
        self.assertNotIn('"sender_query":', disk)
        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)

    def test_on_demand_sender_filter_stays_ephemeral_and_admits_no_other_speaker(self):
        from sightglass.residency.decisions import ResidencySettings
        from sightglass.residency.repository import ResidencyRepository

        ResidencyRepository(self.repository.database).set_settings(ResidencySettings())
        self.arguments["query"] = "Synthetic async-needle"
        self.arguments["sender_query"] = "demo_member_other"
        pending = self.tools.wechat_search_messages(**self.arguments)
        result = self._ready(pending["reading_token"])
        self.assertEqual(result["hits"], [])
        with self.repository.database.connection() as connection:
            self.assertFalse(connection.execute("SELECT 1 FROM messages").fetchone())
        disk = self.path.read_text()
        self.assertNotIn("demo_member_other", disk)
        self.assertNotIn("Synthetic async-needle", disk)

    def test_ready_result_uses_canonical_scan_and_signed_continuation(self) -> None:
        pending = self.tools.wechat_search_messages(**self.arguments)
        result = self._ready(pending["reading_token"])
        self.assertEqual(result["schema"], "sightglass.search-results.v2", result)
        self.assertEqual(len(result["hits"]), 1)
        self.assertTrue(result["source_receipt"]["search"]["preparation"]["performed"])
        cursor = result["page"]["next_cursor"]
        self.assertIsNotNone(cursor)
        with patch.object(
            self.service,
            "_prepare_search_candidates",
            side_effect=AssertionError("continuation preparation"),
        ):
            next_page = self.tools.wechat_search_messages(**self.arguments, cursor=cursor)
        self.assertEqual(next_page["schema"], result["schema"], next_page)
        self.assertNotEqual(result["hits"][0][0], next_page["hits"][0][0])

    def test_existing_cursor_parameter_consumes_preparation_token(self) -> None:
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        self._ready(token)
        result = self.tools.wechat_search_messages(**self.arguments, cursor=token)
        self.assertEqual(result["schema"], "sightglass.search-results.v2", result)

    def test_query_filter_limit_reader_and_binding_changes_fail_before_source(self) -> None:
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        with patch.object(
            self.provider, "snapshot", side_effect=AssertionError("invalid token source")
        ):
            changes: tuple[dict[str, Any], ...] = (
                {"query": "different"},
                {"limit": 2},
                {"before": "2026-10-04T00:00:00Z"},
                {"conversation_ids": []},
                {"participant_ids": ["synthetic-other"]},
            )
            for changed in changes:
                result = self.tools.wechat_search_messages(
                    **(self.arguments | changed), reading_token=token
                )
                self.assertEqual(result["code"], "CURSOR_INVALID", result)
            self.service.reader.reader_id = "different-reader"
            self.assertEqual(
                self.tools.wechat_search_messages(**self.arguments, reading_token=token)["code"],
                "CURSOR_INVALID",
            )
            self.service.reader.reader_id = "codex"
            self.manager.binding = "other-binding"
            self.assertEqual(
                self.tools.wechat_search_messages(**self.arguments, reading_token=token)["code"],
                "CURSOR_INVALID",
            )

    def test_expiry_is_explicit_and_no_source_access(self) -> None:
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        with patch(
            "sightglass.runtime.search_preparation.time.time", return_value=time.time() + 1000
        ):
            result = self.tools.wechat_search_messages(**self.arguments, reading_token=token)
        self.assertEqual(result["state"], "expired")
        self.assertNotIn("hits", result)

    def test_restart_preserves_token_and_recovers_running_job(self) -> None:
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        job = next(iter(self.manager._jobs.values()))
        job.update(state="running", attempts=1)
        self.manager._save()
        self.manager = self._manager()
        result = self._ready(token)
        self.assertEqual(result["schema"], "sightglass.search-results.v2", result)
        self.assertEqual(job["id"], next(iter(self.manager._jobs)))
        self.assertEqual(self.manager._jobs[job["id"]]["restart_count"], 1)

    def test_completed_first_page_does_not_hide_later_append_on_new_request(self) -> None:
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        self._ready(token)
        self.manager.stop()
        # A new first page must discover source append, rather than reusing the
        # completed job forever. The append is inside the ordinary recent window.
        scoped.ScopedSearchPreparationTests._append(
            cast(Any, self),
            "conv_group",
            1,
            prefix="async-needle-append",
            start=datetime(2026, 10, 3, 0, 0, 15, tzinfo=UTC),
        )
        result = self.tools.wechat_search_messages(**self.arguments)
        self.assertEqual(result["state"], "preparing")
        self.assertNotEqual(token, result["reading_token"])
        self.assertEqual(len(self.manager._jobs), 2)
        fresh = self._ready(result["reading_token"])
        self.assertEqual(fresh["source_receipt"]["search"]["preparation"]["message_count"], 5)

    def test_local_poll_ready_race_keeps_local_premise(self) -> None:
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        self._ready(token)
        with (
            local_read_only_scope(),
            patch.object(
                self.provider, "snapshot", side_effect=AssertionError("local poll touched source")
            ),
        ):
            result = self.tools.wechat_search_messages(**self.arguments, reading_token=token)
        self.assertEqual(result["state"], "ready")
        self.assertNotIn("hits", result)
        self.assertNotIn("preparation", result)
        self.assertNotIn("bindings", result)
        self.assertFalse(
            self.service.local_only_tool_call(
                "wechat_search_messages", self.arguments | {"reading_token": token}
            )
        )

    def test_stop_cancels_scan_and_restart_requeues_without_publishing(self) -> None:
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        entered = threading.Event()

        def stalled(*args, **kwargs):
            entered.set()
            while True:
                check_operation_budget()
                time.sleep(0.005)

        with patch.object(self.service, "_prepare_search_candidates", side_effect=stalled):
            self.manager.start()
            self.assertTrue(entered.wait(2))
            self.assertTrue(self.manager.stop())
        job = next(iter(self.manager._jobs.values()))
        self.assertEqual(job["state"], "preparing")
        self.assertNotIn("result", job)
        self.manager = self._manager()
        self.assertEqual(self._ready(token)["schema"], "sightglass.search-results.v2")

    def test_pause_and_policy_change_fence_old_tokens(self) -> None:
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        self.service.reader.paused = True
        self.manager = self._manager()
        self.assertEqual(
            self.tools.wechat_search_messages(**self.arguments, reading_token=token)["code"],
            "SERVICE_PAUSED",
        )
        self.service.reader.paused = False
        result = self.tools.wechat_search_messages(**self.arguments, reading_token=token)
        self.assertEqual(result["state"], "failed")
        self.assertEqual(result["error"]["code"], "SERVICE_PAUSED")
        from dataclasses import replace

        self.service.reader.policy = replace(
            self.service.reader.policy, denied_conversation_ids=frozenset({self.group})
        )
        result = self.tools.wechat_search_messages(**self.arguments, reading_token=token)
        self.assertEqual(result["code"], "CURSOR_INVALID")

    def test_generation_retries_are_bounded_and_failure_is_not_empty_result(self) -> None:
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        with patch.object(
            self.service,
            "_prepare_search_candidates",
            side_effect=SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED),
        ):
            self.manager.start()
            deadline = time.monotonic() + 4
            while time.monotonic() < deadline:
                result = self.tools.wechat_search_messages(**self.arguments, reading_token=token)
                if result["state"] == "failed":
                    break
                time.sleep(0.01)
        self.assertIn("state", result, result)
        self.assertEqual(result["state"], "failed", result)
        self.assertEqual(result["error"]["code"], "SOURCE_GENERATION_CHANGED")
        self.assertEqual(result["progress"]["attempt"], 3)
        self.assertNotIn("hits", result)

    def test_restart_keeps_completed_admissions_and_rescans_only_active_conversation(self) -> None:
        candidates = self.tools.wechat_find_conversations("")["candidates"]
        other = next(
            row["conversation_id"] for row in candidates if row["conversation_id"] != self.group
        )
        self.arguments["conversation_ids"] = [self.group, other]
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        job = next(iter(self.manager._jobs.values()))
        actual = self.provider.prepare_search_page
        calls = []

        def interrupted(account, conversation, **kwargs):
            calls.append(conversation)
            if len(calls) == 2:
                self.manager._stop.set()
                check_operation_budget()
            yield from actual(account, conversation, **kwargs)

        with patch.object(self.provider, "prepare_search_page", side_effect=interrupted):
            self.manager._attempt(job)
        self.assertEqual(job["checkpoint"]["done"], 1)
        first_completed = calls[0]
        self.manager = self._manager()
        resumed = []

        def record(account, conversation, **kwargs):
            resumed.append(conversation)
            yield from actual(account, conversation, **kwargs)

        with patch.object(self.provider, "prepare_search_page", side_effect=record):
            result = self._ready(token)
        self.assertEqual(result["schema"], "sightglass.search-results.v2", result)
        self.assertNotIn(first_completed, resumed)
        self.assertEqual(len(resumed), 1)

    def test_ready_token_fences_selected_replacement_but_survives_append(self) -> None:
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        self._ready(token)
        actual = self.provider.search_generation_binding

        def replaced(account, conversation, **kwargs):
            binding = actual(account, conversation, **kwargs)
            return tuple((key, value + "-replacement") for key, value in binding)

        with patch.object(self.provider, "search_generation_binding", side_effect=replaced):
            result = self.tools.wechat_search_messages(**self.arguments, reading_token=token)
        self.assertEqual(result["code"], "CURSOR_STALE", result)
        self.assertNotIn("hits", result)

    def test_failed_state_is_durable_when_ordinary_save_fails(self) -> None:
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        original = self.manager._save

        def pressure(*, maintenance=False):
            if not maintenance:
                raise SightglassError(ErrorCode.STORAGE_PRESSURE)
            return original(maintenance=True)

        with (
            patch.object(self.manager, "_save", side_effect=pressure),
            patch.object(
                self.service, "_prepare_search_candidates", side_effect=AssertionError("source")
            ),
        ):
            self.manager.start()
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                result = self.tools.wechat_search_messages(**self.arguments, reading_token=token)
                if result["state"] == "failed":
                    break
                time.sleep(0.01)
            self.manager.stop()
        self.assertEqual(result["state"], "failed", result)
        self.assertEqual(result["error"]["code"], "STORAGE_PRESSURE")
        self.manager = self._manager()
        result = self.tools.wechat_search_messages(**self.arguments, reading_token=token)
        self.assertEqual(result["state"], "failed", result)

    def test_uncommittable_terminal_state_is_pending_and_retries_only_metadata(self) -> None:
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        job = next(iter(self.manager._jobs.values()))
        with patch.object(
            self.manager, "_save", side_effect=SightglassError(ErrorCode.STORAGE_PRESSURE)
        ):
            self.manager._fail(job, SightglassError(ErrorCode.STORAGE_PRESSURE).as_dict())
            result = self.tools.wechat_search_messages(**self.arguments, reading_token=token)
        self.assertEqual(result["state"], "preparing")
        self.assertEqual(result["progress"]["phase"], "state_commit_pending")
        self.assertNotIn("error", result)
        with patch.object(
            self.service, "_prepare_search_candidates", side_effect=AssertionError("source")
        ):
            self.manager.start()
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                result = self.tools.wechat_search_messages(**self.arguments, reading_token=token)
                if result["state"] == "failed":
                    break
                time.sleep(0.01)
            self.manager.stop()
        self.assertEqual(result["state"], "failed", result)
        self.manager = self._manager()
        result = self.tools.wechat_search_messages(**self.arguments, reading_token=token)
        self.assertEqual(result["state"], "failed", result)

    def test_ready_token_scope_errors_stay_local_without_foreground_ownership(self) -> None:
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        self._ready(token)
        arguments: dict[str, Any] = self.arguments | {"reading_token": token, "query": "changed"}
        self.assertTrue(self.service.local_only_tool_call("wechat_search_messages", arguments))
        self.assertEqual(self.tools.wechat_search_messages(**arguments)["code"], "CURSOR_INVALID")
        wrong_kind = self.service.token_codec.encode({"kind": "search"})
        arguments["reading_token"] = wrong_kind
        self.assertTrue(self.service.local_only_tool_call("wechat_search_messages", arguments))
        self.assertEqual(self.tools.wechat_search_messages(**arguments)["code"], "CURSOR_INVALID")

    def test_capacity_and_storage_admission_bound_the_queue(self) -> None:
        for index in range(MAX_JOBS):
            changed: dict[str, Any] = {"query": f"q-{index}"}
            result = self.tools.wechat_search_messages(**(self.arguments | changed))
            self.assertEqual(result["state"], "preparing")
        changed = {"query": "overflow"}
        result = self.tools.wechat_search_messages(**(self.arguments | changed))
        self.assertEqual(result["code"], "SERVICE_BUSY")
        disk = json.loads(self.path.read_text())
        self.assertEqual(len(disk["jobs"]), MAX_JOBS)
        self.manager._jobs.clear()
        with patch.object(
            self.service.storage, "reserve", side_effect=SightglassError(ErrorCode.STORAGE_PRESSURE)
        ):
            result = self.tools.wechat_search_messages(**self.arguments)
        self.assertEqual(result["code"], "STORAGE_PRESSURE")
        self.assertEqual(self.manager._jobs, {})

    def test_omitted_limit_continuation_is_not_misclassified_local_only(self) -> None:
        # Reproduce the MCP integration bug: an explicit ``limit=None`` continuation
        # made ``local_request`` raise inside digest computation and fall back to
        # local-only, so a ready poll never reached the canonical result plane.
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        self._ready(token)
        continuation: dict[str, Any] = self.arguments | {"reading_token": token, "limit": None}
        self.assertFalse(self.service.local_only_tool_call("wechat_search_messages", continuation))
        # The classification must match the actual service default resolution.
        page = self.tools.wechat_search_messages(**continuation)
        self.assertEqual(page["schema"], "sightglass.search-results.v2", page)
        self.assertTrue(page["source_receipt"]["search"]["preparation"]["performed"])
        # The digest still binds scope: a changed limit is rejected, not silently
        # accepted under a recovered limit.
        changed_arguments: dict[str, Any] = self.arguments | {"limit": 7}
        changed = self.tools.wechat_search_messages(**changed_arguments, reading_token=token)
        self.assertEqual(changed["code"], ErrorCode.CURSOR_INVALID)

    def test_continuation_limit_resolves_original_limit_for_omitted_continuations(self) -> None:
        # Preparing poll token: a continuation that omits limit must recover the
        # exact limit the first request was bound to, never a caller default.
        token = self.tools.wechat_search_messages(**self.arguments)["reading_token"]
        self.assertEqual(self.manager.continuation_limit(token), self.arguments["limit"])
        self.assertEqual(
            self.service.continuation_limit(None, token, default=987), self.arguments["limit"]
        )
        self.assertEqual(self.service.continuation_limit(5, token, default=987), 5)
        # A different limit is a different digest scope and must not reuse this job.
        changed_arguments: dict[str, Any] = self.arguments | {"limit": 2}
        changed_result = self.tools.wechat_search_messages(**changed_arguments, reading_token=token)
        self.assertEqual(changed_result["code"], ErrorCode.CURSOR_INVALID)
        # Expiry uses stale-cursor semantics rather than a fabricated default.
        job = next(iter(self.manager._jobs.values()))
        job["expires_at"] = time.time() - 1
        with self.assertRaises(SightglassError) as expired:
            self.manager.continuation_limit(token)
        self.assertEqual(expired.exception.code, ErrorCode.CURSOR_STALE)
        # A malformed/foreign token fails closed as invalid.
        with self.assertRaises(SightglassError) as invalid:
            self.manager.continuation_limit("not-a-signed-token")
        self.assertEqual(invalid.exception.code, ErrorCode.CURSOR_INVALID)

    def test_continuation_limit_covers_discovery_continuation_token(self) -> None:
        # The same recovery serves a cold-discovery continuation token, which lives
        # in the same private job map and carries the discovery scope limit.
        self.service.residency.set(self.group, mode="on_demand")
        self.tools.wechat_search_messages(**self.arguments)
        discovery = self.tools.wechat_find_links(
            domains=["async-needle.example"], conversation_ids=[self.group], limit=1
        )
        token = discovery["reading_token"]
        self.assertEqual(self.manager.continuation_limit(token), 1)



class AsyncSearchDaemonTests(unittest.TestCase):
    # Reuse the actual IPC harness without collecting the original daemon suite twice.
    setUp = daemon_fixture.M4DaemonTests.setUp
    tearDown = daemon_fixture.M4DaemonTests.tearDown
    start_daemon = daemon_fixture.M4DaemonTests.start_daemon
    stop_daemon = daemon_fixture.M4DaemonTests.stop_daemon
    reader = daemon_fixture.M4DaemonTests.reader
    operator = daemon_fixture.M4DaemonTests.operator
    bridge = daemon_fixture.M4DaemonTests.bridge
    _group = daemon_fixture.M4DaemonTests._group
    daemon: Any

    def test_actual_ipc_processing_cursor_ready_and_restart(self) -> None:
        arguments: dict[str, Any] = {
            "query": "真人自拍",
            "conversation_ids": [self._group()],
            "limit": 1,
        }
        first = self.bridge.wechat_search_messages(**arguments)
        self.assertEqual(first["state"], "preparing", first)
        token = first["reading_token"]
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            result = self.bridge.wechat_search_messages(**arguments, cursor=token)
            if result.get("schema") != SCHEMA:
                break
            self.assertNotEqual(result["state"], "failed", result)
            time.sleep(0.01)
        self.assertEqual(result["schema"], "sightglass.search-results.v2", result)
        changed: dict[str, Any] = arguments | {"query": "different"}
        with patch.object(
            self.daemon.source_worker,
            "foreground_enter",
            side_effect=AssertionError("local error claimed source"),
        ):
            invalid = self.bridge.wechat_search_messages(**changed, cursor=token)
        self.assertEqual(invalid["code"], "CURSOR_INVALID", invalid)
        self.stop_daemon()
        self.start_daemon()
        result = self.bridge.wechat_search_messages(**arguments, cursor=token)
        self.assertEqual(result["schema"], "sightglass.search-results.v2", result)

    def test_operator_pause_cancels_job_and_resume_does_not_revive_token(self) -> None:
        group = self._group()
        entered = threading.Event()

        def stalled(*args, **kwargs):
            entered.set()
            while True:
                check_operation_budget()
                time.sleep(0.005)

        with patch.object(
            self.daemon.tools.service, "_prepare_search_candidates", side_effect=stalled
        ):
            result = self.bridge.wechat_search_messages(query="synthetic", conversation_ids=[group])
            token = result["reading_token"]
            self.assertTrue(entered.wait(2))
            self.assertTrue(self.operator.call("operator.pause")["paused"])
        self.assertEqual(
            self.bridge.wechat_search_messages(
                query="synthetic", conversation_ids=[group], cursor=token
            )["code"],
            "SERVICE_PAUSED",
        )
        self.operator.call("operator.resume")
        result = self.bridge.wechat_search_messages(
            query="synthetic", conversation_ids=[group], cursor=token
        )
        self.assertIn("state", result, result)
        self.assertEqual(result["state"], "failed", result)
        self.assertEqual(result["error"]["code"], "SERVICE_PAUSED")

    def test_discovery_first_request_and_poll_remain_local_during_source_work(self) -> None:
        from sightglass.residency.repository import ResidencyRepository
        from sightglass.runtime.search_preparation import DISCOVERY_SCHEMA

        group = self._group()
        ResidencyRepository(self.daemon.tools.service.repository.database).set(
            group, mode="on_demand"
        )
        entered = threading.Event()

        def stalled(*args, **kwargs):
            entered.set()
            while True:
                check_operation_budget()
                time.sleep(0.005)

        with patch.object(self.daemon.tools.service, "_prepare_discovery_candidates",
                          side_effect=stalled):
            first = self.bridge.wechat_find_links(
                domains=["synthetic.example"], conversation_ids=[group]
            )
            self.assertEqual(first["schema"], DISCOVERY_SCHEMA, first)
            self.assertTrue(entered.wait(2))
            with patch.object(self.daemon.source_worker, "foreground_enter",
                              side_effect=AssertionError("poll claimed source")):
                result = self.bridge.wechat_find_links(
                    domains=["synthetic.example"], conversation_ids=[group],
                    reading_token=first["reading_token"],
                )
                self.assertEqual(result["state"], "preparing", result)
            self.operator.call("operator.pause")

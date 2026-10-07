from __future__ import annotations

import json
import os
import socket
import stat
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

from sightglass.contracts.common import utc_now
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.mcp.bridge import DaemonReaderTools
from sightglass.operations import check_operation_budget
from sightglass.residency.decisions import ResidencySettings
from sightglass.runtime.config import ConfigStore, SightglassConfig
from sightglass.runtime.daemon import SightglassDaemon
from sightglass.runtime.ipc import (
    IPCClient,
    IPCError,
    IPCTimeoutError,
    IPCUnavailableError,
)
from sightglass.runtime.secrets import (
    OPERATOR_SECRET_ACCOUNT,
    READER_SECRET_ACCOUNT,
    MemorySecretStore,
    semantic_secret_account,
    token_hash,
)
from sightglass.semantic.settings import SemanticSettings
from sightglass.source.identity import opaque_id
from sightglass.source.synthetic import create_synthetic_source
from tests.integration import test_native_source_provider as native_fixture
from tests.unit.test_semantic_service import FakeBackend


class M4DaemonTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source_root = self.root / "source"
        create_synthetic_source(self.source_root)
        self.reader_token = "synthetic-reader-token"
        self.operator_token = "synthetic-operator-token"
        self.config = replace(
            SightglassConfig.create(self.root / "state", self.source_root),
            reader_token_hash=token_hash(self.reader_token),
            operator_token_hash=token_hash(self.operator_token),
        )
        self.config_store = ConfigStore(self.config.data_dir / "config.json")
        self.config_store.save(self.config)
        self.secrets = MemorySecretStore(
            {
                READER_SECRET_ACCOUNT: self.reader_token,
                OPERATOR_SECRET_ACCOUNT: self.operator_token,
            }
        )
        self.daemon: SightglassDaemon | None = None
        self.thread: threading.Thread | None = None
        self.start_daemon()

    def tearDown(self) -> None:
        self.stop_daemon()
        self.temp.cleanup()

    def start_daemon(self) -> None:
        self.daemon = SightglassDaemon(config_store=self.config_store, secret_store=self.secrets)
        self.daemon.tools.service.residency.set_settings(ResidencySettings(default_mode="keep"))
        self.thread = threading.Thread(
            target=self.daemon.serve_forever,
            kwargs={"install_signal_handlers": False},
            daemon=True,
        )
        self.thread.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.config.socket_path.exists():
                # bind creates the path before listen; a connection refused during
                # that narrow startup interval is not a running-service failure.
                try:
                    status = self.operator.call("daemon.status")
                except IPCUnavailableError:
                    pass
                else:
                    self.assertTrue(status["ready"])
                    return
            time.sleep(0.01)
        self.fail("synthetic daemon did not reach its IPC accept loop")

    def stop_daemon(self) -> None:
        if self.thread is None or not self.thread.is_alive():
            return
        self.operator.call("daemon.shutdown")
        self.thread.join(timeout=5)
        self.assertFalse(self.thread.is_alive())

    @property
    def reader(self) -> IPCClient:
        return IPCClient(config_store=self.config_store, secret_store=self.secrets, role="reader")

    @property
    def operator(self) -> IPCClient:
        return IPCClient(config_store=self.config_store, secret_store=self.secrets, role="operator")

    @property
    def bridge(self) -> DaemonReaderTools:
        return DaemonReaderTools(self.reader)

    def _group(self) -> str:
        return self.bridge.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]

    def test_reload_prepare_failure_preserves_active_runtime(self) -> None:
        assert self.daemon is not None
        self.bridge.wechat_find_conversations("Synthetic Group")
        previous = self.daemon.tools
        with patch(
            "sightglass.runtime.daemon.build_daemon_tools",
            side_effect=RuntimeError("synthetic prepare failure"),
        ):
            with self.assertRaises(RuntimeError):
                with self.daemon._state_gate.write():
                    self.daemon._reload(replace(self.config, paused=True))
        self.assertIs(self.daemon.tools, previous)
        self.assertEqual(self.config_store.load().paused, False)
        self.assertTrue(self.bridge.wechat_status()["ready"])

    def test_reload_start_failure_restores_persisted_config_and_reader_runtime(self) -> None:
        assert self.daemon is not None
        self.bridge.wechat_find_conversations("Synthetic Group")
        previous = self.daemon.tools
        original_start = self.daemon._start_runtime_workers
        attempts = 0

        def start():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("synthetic start failure")
            original_start()

        with patch.object(self.daemon, "_start_runtime_workers", side_effect=start):
            with self.assertRaises(RuntimeError):
                with self.daemon._state_gate.write():
                    self.daemon._reload(replace(self.config, paused=True))
        self.assertIs(self.daemon.tools, previous)
        self.assertEqual(self.config_store.load().paused, False)
        self.assertTrue(self.bridge.wechat_status()["ready"])
        self.assertFalse(self.bridge.wechat_find_links().get("ok") is False)

    def test_retrieval_operator_controls_and_reader_calls_use_local_lane(self) -> None:
        assert self.daemon is not None
        self.bridge.wechat_find_conversations("Synthetic Group")
        token = self._prepare_retrieval(concept="Synthetic", limit=1)
        with patch.object(
            self.daemon.source_worker,
            "foreground_enter",
            side_effect=AssertionError("retrieval acquired source ownership"),
        ):
            result = self.bridge.wechat_retrieve("Synthetic", limit=1, reading_token=token)
            self.assertEqual(result["schema"], "sightglass.retrieval-results.v1")
        with self.assertRaises(IPCError):
            self.reader.call("operator.retrieval.rebuild", {"kind": "links"})
        status = self.operator.call("operator.retrieval.explain")
        self.assertIn("ranking", status)
        generation = status["links"]["generation"]
        self.operator.call("operator.retrieval.rebuild", {"kind": "links"})
        self.assertGreater(
            self.operator.call("operator.retrieval.status")["links"]["generation"], generation
        )

    def test_enabled_semantic_reload_IPC_and_worker_lifecycle(self) -> None:
        assert self.daemon is not None
        group = self._group()
        self.bridge.wechat_read_messages(mode="recent", conversation_id=group, limit=20)
        conversation = self.daemon.tools.service.repository.conversation_context(group)
        assert conversation is not None
        account = conversation["account_id"]
        settings = SemanticSettings(
            enabled=True,
            external_data_authorized=True,
            cf_account_id="a" * 32,
            index_name="sightglass-synthetic-daemon",
            source_account_id=account,
            conversation_ids=(group,),
        )
        self.secrets.set(semantic_secret_account(settings.cf_account_id), "synthetic-CF-token")
        backend = FakeBackend()
        with patch("sightglass.runtime.service.CloudflareBackend", return_value=backend):
            with self.daemon._state_gate.write():
                self.daemon._reload(replace(self.config, semantic=settings))
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if self.operator.call("operator.retrieval.status")["semantic"]["state"] == "ready":
                    break
                time.sleep(0.01)
            status = self.operator.call("operator.retrieval.status")
            self.assertEqual(status["semantic"]["state"], "ready")
            self.assertTrue(status["semantic_worker"]["running"])
            token = self._prepare_retrieval(
                concept="Synthetic discussion", conversation_ids=[group],
                kinds=["message"], limit=1,
            )
            with patch.object(
                self.daemon.source_worker,
                "foreground_enter",
                side_effect=AssertionError("semantic retrieval took source ownership"),
            ):
                result = self.bridge.wechat_retrieve(
                    "Synthetic discussion", conversation_ids=[group], kinds=["message"], limit=1,
                    reading_token=token,
                )
            self.assertEqual(result["lanes"]["semantic"], "ready")
            self.assertTrue(backend.query_calls)
            old_worker = self.daemon.semantic_worker
            with self.daemon._state_gate.write():
                self.daemon._reload(replace(self.daemon.config, paused=True))
            self.assertFalse(old_worker.status()["running"])
            self.assertEqual(self.bridge.wechat_retrieve("Synthetic")["code"], "SERVICE_PAUSED")
            paused_worker = self.daemon.semantic_worker
            with self.daemon._state_gate.write():
                self.daemon._reload(
                    replace(self.daemon.config, semantic=replace(settings, enabled=False))
                )
            self.assertFalse(paused_worker.status()["running"])
            self.assertFalse(self.daemon.semantic_worker.status()["enabled"])

    def _prepare_retrieval(self, **arguments: Any) -> str | None:
        result = self.bridge.wechat_retrieve(**arguments)
        token = result.get("reading_token")
        deadline = time.monotonic() + 5
        while result.get("schema") == "sightglass.retrieval-preparation.v1":
            self.assertEqual(result["state"], "preparing", result)
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.01)
            result = self.bridge.wechat_retrieve(**arguments, reading_token=token)
        self.assertEqual(result["schema"], "sightglass.retrieval-results.v1", result)
        return token

    def test_general_health_requests_generation_state_without_history_counts(self) -> None:
        assert self.daemon is not None
        retrieval = self.daemon.tools.service.retrieval
        original = retrieval.status

        def bounded_status(**kwargs):
            self.assertIs(kwargs.get("include_counts"), False)
            return original(**kwargs)

        with patch.object(retrieval, "status", side_effect=bounded_status):
            status = self.operator.call("daemon.status")
            doctor = self.operator.call("operator.doctor")
        self.assertFalse(status["retrieval"]["statistics_collected"])
        self.assertIsNone(status["retrieval"]["links"]["pending_messages"])
        self.assertFalse(doctor["runtime"]["retrieval"]["statistics_collected"])
        exact = self.operator.call("operator.retrieval.status")
        self.assertTrue(exact["statistics_collected"])
        self.assertIsInstance(exact["links"]["pending_messages"], int)

    def test_private_config_socket_auth_roles_and_pause_are_enforced(self) -> None:
        rendered = self.config_store.path.read_text(encoding="utf-8")
        self.assertNotIn(self.reader_token, rendered)
        self.assertNotIn(self.operator_token, rendered)
        self.assertEqual(stat.S_IMODE(self.config_store.path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.config.data_dir.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(self.config.socket_path.stat().st_mode), 0o600)

        with self.assertRaises(IPCError):
            self.reader.call("operator.pause")
        wrong = MemorySecretStore(
            {READER_SECRET_ACCOUNT: "wrong", OPERATOR_SECRET_ACCOUNT: self.operator_token}
        )
        with self.assertRaises(IPCError):
            IPCClient(config_store=self.config_store, secret_store=wrong, role="reader").call(
                "daemon.status"
            )
        with self.assertRaisesRegex(RuntimeError, "reader credential"):
            SightglassDaemon(config_store=self.config_store, secret_store=wrong)

        self.operator.call("operator.pause")
        paused = self.bridge.wechat_find_conversations("Synthetic Group")
        self.assertEqual(paused["code"], "SERVICE_PAUSED")
        status = self.bridge.wechat_status()
        self.assertTrue(status["paused"])
        assert self.daemon is not None
        self.assertTrue(self.daemon.derived_worker.stop())
        # Exercise an uncancelled worker step without a competing background thread.
        self.daemon.derived_worker._stop.clear()
        links = self.daemon.tools.service.retrieval.links
        links.request_rebuild()
        before = links.state()
        for _ in range(2):
            self.assertFalse(self.daemon.derived_worker.run_once())
            self.assertEqual(links.state(), before)
        self.operator.call("operator.resume")
        self.assertTrue(self.bridge.wechat_find_conversations("Synthetic Group")["candidates"])

    def test_slow_reader_call_does_not_block_daemon_status(self) -> None:
        assert self.daemon is not None
        entered = threading.Event()
        release = threading.Event()
        result: dict[str, dict[str, object]] = {}
        original = self.daemon.tools.wechat_find_conversations

        def gated_find(*args, **kwargs):
            entered.set()
            if not release.wait(timeout=2):
                raise RuntimeError("test did not release the gated reader call")
            return original(*args, **kwargs)

        def run_reader() -> None:
            result["value"] = self.bridge.wechat_find_conversations("Synthetic Group")

        reader_thread = threading.Thread(target=run_reader)
        with patch.object(
            self.daemon.tools,
            "wechat_find_conversations",
            side_effect=gated_find,
        ):
            reader_thread.start()
            self.assertTrue(entered.wait(timeout=1))
            fast_operator = IPCClient(
                config_store=self.config_store,
                secret_store=self.secrets,
                role="operator",
                timeout=0.25,
            )
            try:
                status = fast_operator.call("daemon.status")
            finally:
                release.set()
                reader_thread.join(timeout=2)
        self.assertFalse(reader_thread.is_alive())
        self.assertEqual(status["schema"], "sightglass.daemon-status.v1")
        self.assertEqual(result["value"]["schema"], "sightglass.conversation-catalog.v2")

    def test_materialized_message_read_does_not_claim_foreground_source(self) -> None:
        assert self.daemon is not None
        group = self._group()
        self.daemon.tools.service.sync_source_once(initial_tail=100, conversation_limit=100)
        params = {
            "name": "wechat_read_messages",
            "arguments": {
                "mode": "recent",
                "conversation_id": group,
                "limit": 2,
                "projection": "detail",
            },
        }
        self.assertTrue(
            self.daemon.tools.service.local_only_tool_call(
                str(params["name"]), cast(dict[str, Any], params["arguments"])
            )
        )
        with patch.object(
            self.daemon.source_worker,
            "foreground_enter",
            side_effect=AssertionError("local read claimed foreground source ownership"),
        ) as foreground:
            page = self.daemon._dispatch("reader", "tools.call", params)
        foreground.assert_not_called()
        self.assertEqual(page["source_receipt"]["served_from"], "window_db")

    def test_local_read_errors_do_not_wait_for_foreground_source(self) -> None:
        assert self.daemon is not None
        self.daemon.source_worker.stop()
        native_fixture._sqlcipher()
        native = native_fixture.NativeSourceProviderTests(
            "test_live_descriptor_health_and_latest_message_read"
        )
        native_tools = None
        try:
            native.setUp()
            native_tools = native._reader_tools()
            with (
                patch.object(self.daemon, "tools", native_tools),
                patch.object(
                    self.daemon.source_worker,
                    "foreground_enter",
                    side_effect=SightglassError(ErrorCode.SERVICE_TIMEOUT),
                ) as foreground,
            ):
                for name, arguments, code in (
                    ("wechat_read_inbox", {"limit": 1}, "SOURCE_INCOMPLETE"),
                    (
                        "wechat_read_messages",
                        {
                            "mode": "context",
                            "anchor": "synthetic-invalid-anchor",
                            "before": 0,
                            "after": 0,
                            "limit": 1,
                        },
                        "CURSOR_INVALID",
                    ),
                    (
                        "wechat_read_messages",
                        {"mode": "message", "message_id": "wxmsg_synthetic_unobserved"},
                        "MESSAGE_NOT_FOUND",
                    ),
                    (
                        "wechat_read_messages",
                        {
                            "mode": "context",
                            "anchor": "synthetic-invalid-anchor",
                            "before": 0,
                            "after": 0,
                            "limit": 1,
                            "refresh": True,
                        },
                        "CURSOR_INVALID",
                    ),
                    (
                        "wechat_read_messages",
                        {
                            "mode": "message",
                            "message_id": "wxmsg_synthetic_unobserved",
                            "refresh": True,
                        },
                        "MESSAGE_NOT_FOUND",
                    ),
                    (
                        "wechat_read_messages",
                        {"mode": "updates", "refresh": True},
                        "QUERY_INVALID",
                    ),
                    (
                        "wechat_read_messages",
                        {"mode": "recent", "refresh": "true"},
                        "QUERY_INVALID",
                    ),
                    (
                        "wechat_read_messages",
                        {"mode": "recent", "cursor": "synthetic-invalid", "refresh": True},
                        "QUERY_INVALID",
                    ),
                ):
                    with self.subTest(code=code):
                        result = self.daemon._dispatch(
                            "reader", "tools.call", {"name": name, "arguments": arguments}
                        )
                        self.assertEqual(result["code"], code)
                conversation_id = native_tools.wechat_find_conversations("Fixture")["candidates"][
                    0
                ]["conversation_id"]
                native_tools.service.reader.policy = replace(
                    native_tools.service.reader.policy, allowed_conversation_ids=frozenset()
                )
                for refresh in (False, True):
                    result = self.daemon._dispatch(
                        "reader",
                        "tools.call",
                        {
                            "name": "wechat_read_messages",
                            "arguments": {
                                "mode": "recent",
                                "conversation_id": conversation_id,
                                "refresh": refresh,
                            },
                        },
                    )
                    self.assertEqual(result["code"], "POLICY_DENIED")
                foreground.assert_not_called()
        finally:
            if native_tools is not None:
                native_tools.close()
            if hasattr(native, "provider"):
                native.provider.close()
            if hasattr(native, "temp"):
                native.temp.cleanup()

    def test_refresh_message_read_claims_foreground_source(self) -> None:
        assert self.daemon is not None
        self.daemon.source_worker.stop()
        group = self._group()
        self.daemon.tools.service.sync_source_once(initial_tail=100, conversation_limit=100)
        arguments = {
            "mode": "recent",
            "conversation_id": group,
            "limit": 2,
            "projection": "detail",
            "refresh": True,
        }
        self.assertFalse(
            self.daemon.tools.service.local_only_tool_call("wechat_read_messages", arguments)
        )
        with patch.object(
            self.daemon.source_worker,
            "foreground_enter",
            wraps=self.daemon.source_worker.foreground_enter,
        ) as foreground:
            page = self.daemon._dispatch(
                "reader", "tools.call", {"name": "wechat_read_messages", "arguments": arguments}
            )
        foreground.assert_called_once()
        self.assertEqual(page["schema"], "sightglass.message-page.v1")
        self.assertNotEqual(page["source_receipt"].get("served_from"), "window_db")

    def test_tool_concurrency_is_bounded_with_retryable_busy_result(self) -> None:
        assert self.daemon is not None
        entered = threading.Condition()
        entered_count = 0
        release = threading.Event()
        original = self.daemon.tools.wechat_find_conversations

        def gated_find(*args, **kwargs):
            nonlocal entered_count
            with entered:
                entered_count += 1
                entered.notify_all()
            if not release.wait(timeout=2):
                raise RuntimeError("test did not release bounded reader calls")
            return original(*args, **kwargs)

        workers = [
            threading.Thread(
                target=self.bridge.wechat_find_conversations,
                args=(f"capacity-{index}",),
            )
            for index in range(3)
        ]
        with patch.object(
            self.daemon.tools,
            "wechat_find_conversations",
            side_effect=gated_find,
        ):
            for worker in workers:
                worker.start()
            with entered:
                ready = entered.wait_for(lambda: entered_count == 3, timeout=1)
            self.assertTrue(ready)
            fast_reader = DaemonReaderTools(
                IPCClient(
                    config_store=self.config_store,
                    secret_store=self.secrets,
                    role="reader",
                    timeout=0.25,
                )
            )
            try:
                busy = fast_reader.wechat_find_conversations("Synthetic Group")
            finally:
                release.set()
                for worker in workers:
                    worker.join(timeout=2)
        self.assertEqual(busy["schema"], "sightglass.error.v1")
        self.assertEqual(busy["code"], "SERVICE_BUSY")
        self.assertTrue(busy["retryable"])
        self.assertTrue(all(not worker.is_alive() for worker in workers))

    def test_saturated_source_lane_does_not_block_materialized_local_read(self) -> None:
        assert self.daemon is not None
        group = self._group()
        self.daemon.tools.service.sync_source_once(initial_tail=100, conversation_limit=100)
        entered = threading.Condition()
        entered_count = 0
        release = threading.Event()
        original = self.daemon.tools.wechat_find_conversations

        def gated_find(*args, **kwargs):
            nonlocal entered_count
            with entered:
                entered_count += 1
                entered.notify_all()
            if not release.wait(timeout=2):
                raise RuntimeError("test did not release saturated source calls")
            return original(*args, **kwargs)

        workers = [
            threading.Thread(
                target=self.bridge.wechat_find_conversations,
                args=(f"source-saturation-{index}",),
            )
            for index in range(3)
        ]
        with patch.object(
            self.daemon.tools,
            "wechat_find_conversations",
            side_effect=gated_find,
        ):
            for worker in workers:
                worker.start()
            with entered:
                self.assertTrue(entered.wait_for(lambda: entered_count == 3, timeout=1))
            status = self.operator.call("daemon.status")
            started = time.monotonic()
            try:
                page = self.bridge.wechat_read_messages(
                    mode="recent",
                    conversation_id=group,
                    limit=2,
                    projection="detail",
                )
                elapsed = time.monotonic() - started
            finally:
                release.set()
                for worker in workers:
                    worker.join(timeout=2)

        self.assertLess(elapsed, 0.25)
        self.assertEqual(page["source_receipt"]["served_from"], "window_db")
        self.assertTrue(status["work_lanes"]["lanes"]["source_read"]["saturated"])
        self.assertFalse(status["work_lanes"]["lanes"]["local_read"]["saturated"])
        self.assertTrue(all(not worker.is_alive() for worker in workers))

    def test_status_and_doctor_expose_content_free_runtime_observability(self) -> None:
        self.bridge.wechat_status("summary")
        status = self.operator.call("daemon.status")
        self.assertEqual(status["build"]["version"], "0.1.0.dev1")
        self.assertEqual(len(status["config_revision"]), 64)
        metrics = status["tool_metrics"]["tools"]["wechat_status"]
        self.assertGreaterEqual(metrics["success_count"], 1)
        self.assertIsNotNone(metrics["latency_ms"]["p50"])
        self.assertIn("wait_p95_ms", status["window_writer"])
        self.assertIn("foreground", status["source_worker"])
        self.assertNotIn(self.reader_token, str(status))
        self.assertNotIn(str(self.source_root), str(status))

        doctor = self.operator.call("operator.doctor")
        self.assertEqual(doctor["runtime"]["config_revision"], status["config_revision"])
        capabilities = self.bridge.wechat_status("capabilities")
        self.assertEqual(
            doctor["runtime"]["resource_processors"],
            capabilities["resource_processors"],
        )

    def test_mcp_summary_status_bypasses_saturated_tool_slots(self) -> None:
        assert self.daemon is not None
        entered = threading.Condition()
        entered_count = 0
        release = threading.Event()
        original = self.daemon.tools.wechat_find_conversations

        def gated_find(*args, **kwargs):
            nonlocal entered_count
            with entered:
                entered_count += 1
                entered.notify_all()
            if not release.wait(timeout=2):
                raise RuntimeError("test did not release saturated reader calls")
            return original(*args, **kwargs)

        workers = [
            threading.Thread(
                target=self.bridge.wechat_find_conversations,
                args=(f"distinct-{index}",),
            )
            for index in range(3)
        ]
        with patch.object(
            self.daemon.tools,
            "wechat_find_conversations",
            side_effect=gated_find,
        ):
            for worker in workers:
                worker.start()
            with entered:
                ready = entered.wait_for(lambda: entered_count == 3, timeout=1)
            self.assertTrue(ready)
            fast_status = DaemonReaderTools(
                IPCClient(
                    config_store=self.config_store,
                    secret_store=self.secrets,
                    role="reader",
                    timeout=0.25,
                )
            )
            started = time.monotonic()
            try:
                status = fast_status.wechat_status("summary", response_profile="diagnostic")
                elapsed = time.monotonic() - started
            finally:
                release.set()
                for worker in workers:
                    worker.join(timeout=2)
        self.assertLess(elapsed, 0.1)
        self.assertEqual(status["schema"], "sightglass.status.v1")
        self.assertEqual(status["runtime"]["operations"]["active_count"], 3)
        self.assertTrue(all(not worker.is_alive() for worker in workers))

    def test_mcp_summary_status_never_falls_back_to_source_health(self) -> None:
        assert self.daemon is not None
        service = self.daemon.tools.service
        with service._status_lock:  # noqa: SLF001 - exercise cold-cache failure path
            service._cached_status = None  # noqa: SLF001 - exercise cold-cache failure path
        with patch.object(
            service,
            "status",
            side_effect=AssertionError("fast status must not touch source health"),
        ):
            started = time.monotonic()
            status = self.bridge.wechat_status("summary")
            elapsed = time.monotonic() - started

        self.assertLess(elapsed, 0.1)
        self.assertEqual(status["schema"], "sightglass.status.v1")
        self.assertFalse(status["ready"])
        self.assertEqual(status["source"]["source_state"], "unknown")

    def test_identical_concurrent_calls_share_one_execution(self) -> None:
        assert self.daemon is not None
        entered = threading.Event()
        release = threading.Event()
        invocations = 0
        invocation_lock = threading.Lock()
        original = self.daemon.tools.wechat_find_conversations
        results: list[dict[str, object]] = []

        def gated_find(*args, **kwargs):
            nonlocal invocations
            with invocation_lock:
                invocations += 1
            entered.set()
            if not release.wait(timeout=2):
                raise RuntimeError("test did not release the shared reader call")
            return original(*args, **kwargs)

        def run_reader() -> None:
            results.append(self.bridge.wechat_find_conversations("Synthetic Group"))

        workers = [threading.Thread(target=run_reader) for _ in range(3)]
        with patch.object(
            self.daemon.tools,
            "wechat_find_conversations",
            side_effect=gated_find,
        ):
            for worker in workers:
                worker.start()
            self.assertTrue(entered.wait(timeout=1))
            time.sleep(0.05)
            release.set()
            for worker in workers:
                worker.join(timeout=2)

        self.assertEqual(invocations, 1)
        self.assertEqual(len(results), 3)
        self.assertTrue(all(item == results[0] for item in results))
        self.assertTrue(all(not worker.is_alive() for worker in workers))

    def test_daemon_deadline_stops_cooperative_work_and_recovers_without_restart(
        self,
    ) -> None:
        assert self.daemon is not None
        entered = threading.Event()

        def blocked_read(*_args, **_kwargs):
            entered.set()
            while True:
                check_operation_budget()
                time.sleep(0.005)

        with (
            patch(
                "sightglass.runtime.daemon.TOOL_OPERATION_TIMEOUT_SECONDS",
                0.05,
            ),
            patch.object(
                self.daemon.tools,
                "wechat_find_conversations",
                side_effect=blocked_read,
            ),
        ):
            timed_out = self.bridge.wechat_find_conversations("Synthetic Group")

        self.assertTrue(entered.is_set())
        self.assertEqual(timed_out["code"], "SERVICE_TIMEOUT")
        status = self.bridge.wechat_status("summary", response_profile="diagnostic")
        self.assertEqual(status["schema"], "sightglass.status.v1")
        self.assertEqual(status["runtime"]["operations"]["active_count"], 0)
        self.assertGreaterEqual(status["runtime"]["operations"]["timed_out_call_count"], 1)
        recovered = self.bridge.wechat_find_conversations("Synthetic Group")
        self.assertEqual(recovered["schema"], "sightglass.conversation-catalog.v2")

    def test_blocked_receipt_writer_does_not_delay_message_response(self) -> None:
        assert self.daemon is not None
        group_id = self._group()
        recorder = self.daemon.tools.receipt_recorder
        original_persist = recorder._persist  # type: ignore[attr-defined]
        receipt_entered = threading.Event()
        release_receipt = threading.Event()

        def blocked_persist(**values):
            receipt_entered.set()
            if not release_receipt.wait(timeout=2):
                raise RuntimeError("test did not release the receipt writer")
            original_persist(**values)

        with patch.object(recorder, "_persist", side_effect=blocked_persist):
            fast_reader = DaemonReaderTools(
                IPCClient(
                    config_store=self.config_store,
                    secret_store=self.secrets,
                    role="reader",
                    timeout=0.5,
                )
            )
            try:
                page = fast_reader.wechat_read_messages(
                    mode="recent",
                    conversation_id=group_id,
                    projection="compact",
                    limit=5,
                )
                self.assertTrue(receipt_entered.wait(timeout=0.25))
            finally:
                release_receipt.set()

        self.assertEqual(page["schema"], "sightglass.message-batch.v1")

    def test_bridge_projects_ipc_availability_failures_as_safe_errors(self) -> None:
        class FailingClient:
            def __init__(self, error: IPCError) -> None:
                self.error = error

            def call(self, _method, _params=None):
                raise self.error

        timed_out = DaemonReaderTools(
            cast(IPCClient, FailingClient(IPCTimeoutError("private timeout")))
        )
        timeout_result = timed_out.wechat_status()
        self.assertEqual(timeout_result["schema"], "sightglass.error.v1")
        self.assertEqual(timeout_result["code"], "SERVICE_TIMEOUT")
        self.assertTrue(timeout_result["retryable"])
        self.assertNotIn("private timeout", str(timeout_result))

        unavailable = DaemonReaderTools(
            cast(IPCClient, FailingClient(IPCUnavailableError("private unavailable")))
        )
        resource_result = unavailable.wechat_read_resource("wxres_unavailable")
        self.assertTrue(resource_result.isError)
        self.assertEqual(
            (resource_result.structuredContent or {})["code"],
            "SERVICE_UNAVAILABLE",
        )
        self.assertNotIn("private unavailable", str(resource_result))

    def test_ipc_response_timeout_is_distinct_from_service_unavailable(self) -> None:
        blocked_socket = self.root / "blocked.sock"
        blocked_config = replace(self.config, socket_path=blocked_socket)
        blocked_store = ConfigStore(self.root / "blocked-config.json")
        blocked_store.save(blocked_config)
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(blocked_socket))
        server.listen(1)
        accepted = threading.Event()
        release = threading.Event()

        def hold_response() -> None:
            connection, _address = server.accept()
            with connection:
                accepted.set()
                release.wait(timeout=1)

        server_thread = threading.Thread(target=hold_response)
        server_thread.start()
        client = IPCClient(
            config_store=blocked_store,
            secret_store=self.secrets,
            role="reader",
            timeout=0.05,
        )
        try:
            with self.assertRaises(IPCTimeoutError):
                client.call("daemon.status")
            self.assertTrue(accepted.is_set())
        finally:
            release.set()
            server.close()
            server_thread.join(timeout=1)
            blocked_socket.unlink(missing_ok=True)

        missing_config = replace(self.config, socket_path=self.root / "missing.sock")
        missing_store = ConfigStore(self.root / "missing-config.json")
        missing_store.save(missing_config)
        with self.assertRaises(IPCUnavailableError):
            IPCClient(
                config_store=missing_store,
                secret_store=self.secrets,
                role="reader",
                timeout=0.05,
            ).call("daemon.status")

    def test_pending_delivery_replays_exactly_after_daemon_restart(self) -> None:
        conversation_id = self._group()
        first = self.bridge.wechat_read_messages(
            mode="updates", conversation_id=conversation_id, limit=100
        )
        self.assertIsNotNone(first["page"]["delivery_id"])
        previous_instance = self.operator.call("daemon.status")["instance_id"]

        self.stop_daemon()
        self.start_daemon()
        current_instance = self.operator.call("daemon.status")["instance_id"]
        self.assertNotEqual(previous_instance, current_instance)
        replay = self.bridge.wechat_read_messages(
            mode="updates", conversation_id=conversation_id, limit=100
        )
        self.assertEqual(replay, first)

    def test_alias_merge_split_rebind_and_rollback_are_append_only(self) -> None:
        conversation_id = self._group()
        candidates = self.bridge.wechat_find_participants(conversation_id, "")["candidates"]
        database = self.daemon.database  # type: ignore[union-attr]
        with database.connection() as connection:
            non_self_ids = {
                str(row[0])
                for row in connection.execute(
                    "SELECT p.participant_id FROM participants p "
                    "JOIN participant_source_keys k ON k.participant_id = p.participant_id "
                    "WHERE p.is_self = 0 AND k.active = 1"
                )
            }
        selected = [
            candidate for candidate in candidates if candidate["participant_id"] in non_self_ids
        ]
        source_id = selected[0]["participant_id"]
        target_id = selected[1]["participant_id"]

        alias = self.operator.call(
            "operator.alias.set",
            {"participant_id": source_id, "alias": "玻璃糖", "reason": "synthetic test"},
        )
        found = self.bridge.wechat_find_participants(conversation_id, "玻璃糖")
        self.assertEqual(found["candidates"][0]["participant_id"], source_id)
        unset = self.operator.call("operator.alias.unset", {"participant_id": source_id})
        self.operator.call(
            "operator.correction.rollback", {"correction_id": unset["correction_id"]}
        )
        restored = self.bridge.wechat_find_participants(conversation_id, "玻璃糖")
        self.assertEqual(restored["candidates"][0]["label"], "玻璃糖")

        with database.connection() as connection:
            before_observations = int(
                connection.execute("SELECT COUNT(*) FROM message_observations").fetchone()[0]
            )
            before_participants = int(
                connection.execute("SELECT COUNT(*) FROM participants").fetchone()[0]
            )
        merged = self.operator.call(
            "operator.correction.merge",
            {
                "source_participant_id": source_id,
                "target_participant_id": target_id,
                "reason": "synthetic duplicate",
            },
        )
        with database.connection() as connection:
            self.assertIsNone(
                connection.execute(
                    "SELECT 1 FROM participants WHERE participant_id = ?", (source_id,)
                ).fetchone()
            )
            self.assertEqual(
                int(connection.execute("SELECT COUNT(*) FROM participants").fetchone()[0]),
                before_participants - 1,
            )
            self.assertEqual(
                int(connection.execute("SELECT COUNT(*) FROM message_observations").fetchone()[0]),
                before_observations,
            )
        self.operator.call(
            "operator.correction.split", {"merge_correction_id": merged["correction_id"]}
        )
        with database.connection() as connection:
            self.assertIsNotNone(
                connection.execute(
                    "SELECT 1 FROM participants WHERE participant_id = ?", (source_id,)
                ).fetchone()
            )
            key = connection.execute(
                """
                SELECT source_key_id FROM participant_source_keys
                WHERE participant_id = ? AND active = 1 LIMIT 1
                """,
                (source_id,),
            ).fetchone()
        self.assertIsNotNone(key)
        rebound = self.operator.call(
            "operator.correction.rebind",
            {"source_key_id": key[0], "target_participant_id": target_id},
        )
        self.operator.call(
            "operator.correction.rollback", {"correction_id": rebound["correction_id"]}
        )
        with database.connection() as connection:
            owner = connection.execute(
                "SELECT participant_id FROM participant_source_keys WHERE source_key_id = ?",
                (key[0],),
            ).fetchone()[0]
            ledger_count = int(
                connection.execute("SELECT COUNT(*) FROM identity_corrections").fetchone()[0]
            )
        self.assertEqual(owner, source_id)
        self.assertGreaterEqual(ledger_count, 6)
        self.assertTrue(alias["correction_id"].startswith("wxcorrection_"))

    def test_merge_uses_target_canonical_membership_across_conversations(self) -> None:
        conversation_id = self._group()
        candidates = self.bridge.wechat_find_participants(conversation_id, "")["candidates"]
        database = self.daemon.database  # type: ignore[union-attr]
        source_id = next(
            candidate["participant_id"]
            for candidate in candidates
            if candidate["label"] == "示例甲"
        )
        target_id = opaque_id("wxperson", "synthetic-merge-target")
        with database.transaction() as connection:
            source = connection.execute(
                "SELECT * FROM participants WHERE participant_id = ?", (source_id,)
            ).fetchone()
            connection.execute(
                """
                INSERT INTO participants(
                    participant_id, account_id, current_reader_label, is_self,
                    actor_kind, resolution_state, identity_confidence,
                    first_seen_at, last_seen_at
                ) VALUES (?, ?, NULL, 0, ?, ?, ?, ?, ?)
                """,
                (
                    target_id,
                    source["account_id"],
                    source["actor_kind"],
                    source["resolution_state"],
                    source["identity_confidence"],
                    source["first_seen_at"],
                    source["last_seen_at"],
                ),
            )
        merged = self.operator.call(
            "operator.correction.merge",
            {
                "source_participant_id": source_id,
                "target_participant_id": target_id,
            },
        )
        canonical_membership_id = opaque_id("wxmember", conversation_id, target_id)
        with database.connection() as connection:
            membership = connection.execute(
                """
                SELECT membership_id FROM conversation_members
                WHERE conversation_id = ? AND participant_id = ?
                """,
                (conversation_id, target_id),
            ).fetchone()
        self.assertEqual(membership["membership_id"], canonical_membership_id)

        # Re-hydrating source evidence must reuse the correction projection,
        # not collide with a source-derived noncanonical membership ID.
        self.assertTrue(self.bridge.wechat_find_participants(conversation_id, "示例甲"))
        self.operator.call(
            "operator.correction.split",
            {"merge_correction_id": merged["correction_id"]},
        )
        with database.connection() as connection:
            restored = connection.execute(
                "SELECT 1 FROM participants WHERE participant_id = ?", (source_id,)
            ).fetchone()
        self.assertIsNotNone(restored)

    def test_cache_cleanup_is_dry_run_by_default_and_apply_is_bounded(self) -> None:
        assert self.daemon is not None
        store = self.daemon.tools.service.resource_service.cache
        cached, local_path = store.put(b"orphan", mime_type="text/plain", origin="test")
        self.daemon.tools.service.repository.upsert_resource_object(
            object_digest=cached.digest,
            local_path_internal=local_path,
            mime_type=cached.mime_type,
            byte_size=len(cached.data),
            origin=cached.origin,
            observed_at=utc_now().isoformat(timespec="microseconds"),
        )
        preview = self.operator.call("operator.cache.cleanup", {"apply": False})
        self.assertFalse(preview["applied"])
        self.assertTrue(Path(local_path).exists())
        applied = self.operator.call("operator.cache.cleanup", {"apply": True})
        self.assertTrue(applied["applied"])
        self.assertEqual(applied["object_count"], 1)
        self.assertFalse(Path(local_path).exists())

    def test_storage_explain_is_operator_only_bounded_and_content_free(self) -> None:
        self.bridge.wechat_read_messages(mode="recent", conversation_id=self._group(), limit=2)
        result = self.operator.call(
            "operator.storage.explain",
            {"offset": 0, "limit": 5, "sample_size": 2},
        )
        self.assertEqual(result["schema"], "sightglass.storage-explain.v1")
        self.assertFalse(result["mutated"])
        self.assertLessEqual(len(result["files"]["files"]), 5)
        self.assertEqual(result["diagnostic"]["mode"], "quick")
        self.assertFalse(result["database"]["counts_available"])
        self.assertIsNone(result["database"]["message_count"])
        self.assertIsNone(result["database"]["observation_count"])
        quick = result
        result = self.operator.call(
            "operator.storage.explain",
            {"offset": 0, "limit": 5, "sample_size": 2, "deep": True},
        )
        self.assertEqual(result["diagnostic"]["mode"], "deep")
        self.assertEqual(result["diagnostic"]["state"], "complete")
        self.assertGreater(result["database"]["message_count"], 0)
        self.assertGreater(result["database"]["observation_count"], 0)
        history = result["history"]
        self.assertEqual(history["schema"], "sightglass.storage-history.v1")
        self.assertTrue(history["available"])
        self.assertIn("7d", history["windows"])
        self.assertIn("30d", history["windows"])
        rendered = json.dumps([quick, result], ensure_ascii=False)
        self.assertNotIn(str(self.config.data_dir), rendered)
        self.assertNotIn(str(self.config.source_settings_path), rendered)
        self.assertNotIn("Synthetic Group", rendered)
        with self.assertRaises(IPCError):
            self.reader.call(
                "operator.storage.explain",
                {"offset": 0, "limit": 5, "sample_size": 0},
            )

    def test_daemon_maintains_a_private_bounded_storage_history(self) -> None:
        path = self.config.data_dir / "storage-history.json"
        self.assertTrue(path.is_file())
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        entries = json.loads(path.read_text(encoding="utf-8"))["days"]
        self.assertEqual(len(entries), 1)
        status = self.operator.call("daemon.status")
        self.assertTrue(status["storage_history"]["available"])
        # A restart on the same UTC day replaces that day's snapshot, not duplicates it.
        self.stop_daemon()
        self.start_daemon()
        entries = json.loads(path.read_text(encoding="utf-8"))["days"]
        self.assertEqual(len(entries), 1)

    def test_process_identity_lock_rejects_a_second_daemon(self) -> None:
        second = SightglassDaemon(config_store=self.config_store, secret_store=self.secrets)
        with self.assertRaisesRegex(RuntimeError, "already owns"):
            second.serve_forever(install_signal_handlers=False)
        identity = json.loads(
            (self.config.socket_path.parent / "sightglassd.lock").read_text(encoding="utf-8")
        )
        self.assertEqual(identity["pid"], os.getpid())

    def test_operator_can_switch_account_scope_and_manage_the_denylist(self) -> None:
        assert self.daemon is not None
        group_id = self._group()
        direct_id = self.bridge.wechat_find_conversations("Demo Direct")["candidates"][0][
            "conversation_id"
        ]
        self.operator.call("operator.policy.allow", {"conversation_id": group_id})
        selected = self.operator.call("operator.policy.set", {"mode": "selected"})
        self.assertEqual(selected["mode"], "allowlist")
        self.assertEqual(selected["authorized_conversation_count"], 1)

        account = self.operator.call("operator.policy.set", {"mode": "account"})
        self.assertEqual(account["mode"], "all_except_denylist")
        self.assertEqual(account["authorized_conversation_count"], 2)

        self.operator.call("operator.policy.deny", {"conversation_id": direct_id})
        denied = self.operator.call("operator.policy.status")
        self.assertEqual(denied["deny_count"], 1)
        self.assertEqual(denied["authorized_conversation_count"], 1)
        denied_direct = self.bridge.wechat_read_messages(
            mode="recent", conversation_id=direct_id, limit=1
        )
        self.assertEqual(denied_direct["code"], "POLICY_DENIED")

        self.operator.call("operator.policy.clear_deny", {"conversation_id": direct_id})
        restored = self.operator.call("operator.policy.status")
        self.assertEqual(restored["deny_count"], 0)
        self.assertEqual(restored["authorized_conversation_count"], 2)


if __name__ == "__main__":
    unittest.main()

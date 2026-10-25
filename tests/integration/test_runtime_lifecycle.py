"""Generated IPC lifecycle regressions: owner joins versus post-stop SQL recovery."""

from __future__ import annotations

import json
import socket
import sqlite3
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from typing import Any
from unittest.mock import patch

from sightglass.runtime.config import ConfigStore
from sightglass.runtime.daemon import SightglassDaemon
from sightglass.runtime.edge_relay import EdgeSession, FramedStream
from sightglass.runtime.ipc import IPCError
from sightglass.runtime.secrets import (
    OPERATOR_SECRET_ACCOUNT,
    READER_SECRET_ACCOUNT,
    MemorySecretStore,
    token_hash,
)
from sightglass.storage import StorageSettings
from tests.integration import test_daemon_reliability as daemon_fixture
from tests.unit import test_capture_core as capture_fixture


class RuntimeLifecycleTests(unittest.TestCase):
    # Reuse the existing generated remote source/Core/edge fixture, never a config
    # or account belonging to an installation.
    setUp = capture_fixture.CoreCaptureTests.setUp
    stop_edge = capture_fixture.CoreCaptureTests.stop_edge
    start_daemon = daemon_fixture.DaemonReliabilityTests.start
    stop_daemon = daemon_fixture.DaemonReliabilityTests.stop
    client = daemon_fixture.DaemonReliabilityTests.client
    root: Any
    core: Any
    settings: Any
    config: Any
    executor: Any
    spool: Any
    token: Any
    edge_stop: Any
    edge_socket: Any
    edge_thread: Any
    daemon: Any
    thread: Any

    def start(self) -> None:
        self.core.close()
        settings_path = self.root / "enrollment" / "remote.json"
        self.settings.write(settings_path)
        self.config = replace(
            self.config, source_root=None, source_settings_path=settings_path,
            reader_token_hash=token_hash("synthetic-reader"),
            operator_token_hash=token_hash("synthetic-operator"),
            storage=StorageSettings(min_free_bytes=0),
        )
        self.store = ConfigStore(self.config.data_dir / "config.json")
        self.store.save(self.config)
        self.secrets = MemorySecretStore({
            READER_SECRET_ACCOUNT: "synthetic-reader",
            OPERATOR_SECRET_ACCOUNT: "synthetic-operator",
        })
        self.daemon = SightglassDaemon(config_store=self.store, secret_store=self.secrets)
        self.assertFalse(self.daemon.tools.receipt_writer_status()["alive"])
        self.core = self.daemon.capture_core
        self.thread, self.failures = None, []
        self.addCleanup(self.stop_daemon)
        self.addCleanup(self.stop_edge)
        self.start_daemon()
        self.connect_edge()

    def connect_edge(self) -> None:
        self.core = self.daemon.capture_core
        self.edge_stop.clear()
        connection = socket.socket(socket.AF_UNIX)
        connection.connect(str(self.settings.socket_path))
        self.edge_socket = connection

        def edge() -> None:
            try:
                with connection.makefile("rb") as reader, connection.makefile("wb") as writer:
                    EdgeSession(
                        self.executor, self.spool, token=self.token,
                        core_generation=self.config.activation_generation,
                    ).run(FramedStream(reader, writer), stop=self.edge_stop)
            except (OSError, RuntimeError, ValueError):
                pass  # A lifecycle transition deliberately disconnects this edge.

        self.edge_thread = threading.Thread(target=edge, daemon=True)
        self.edge_thread.start()
        with self.core.broker._condition:
            self.assertTrue(self.core.broker._condition.wait_for(
                lambda: self.core.broker.connected, 2))

    def diagnostic(self):
        return self.client("operator", timeout=1).call(
            "daemon.status", {"operations_only": True})["lifecycle"]

    def assert_quiet(self) -> None:
        for name in ("source_worker", "resource_worker", "voice_worker", "derived_worker",
                     "semantic_worker"):
            thread = getattr(getattr(self.daemon, name), "_thread", None)
            self.assertTrue(thread is None or not thread.is_alive(), name)
        self.assertFalse(self.core.broker.connected)
        self.assertFalse(self.daemon.tools.receipt_writer_status()["alive"])

    def fresh_read(self) -> None:
        result = self.client().call("tools.call", {
            "name": "wechat_read_messages", "arguments": {
                "conversation_id": self.group, "mode": "recent", "refresh": True,
                "limit": 1, "voice": "off", "include_resources": "none",
            },
        })
        self.assertEqual(result["source_receipt"]["view"], "fresh")
        self.assertTrue(result["messages"])

    group: str

    def test_sqlite_lease_failure_keeps_cause_and_fast_diagnostic_then_retry_recovers(self):
        self.start()
        old_tools = self.daemon.tools
        old_core = self.core
        # Keep an actual durable lease idle while a separate SQLite owner holds
        # BEGIN IMMEDIATE. Every daemon thread remains free to stop normally.
        self.daemon.resource_worker.stop()
        self.daemon.source_worker.stop()
        database = self.daemon.database
        with database.transaction() as connection:
            resource = connection.execute("SELECT resource_id FROM resources LIMIT 1").fetchone()[0]
            connection.execute(
                "INSERT INTO resource_jobs(job_id,resource_id,resource_revision,recipe_digest,"
                "recipe_json,state,owner_id,lease_expires_at,created_at,updated_at) "
                "VALUES(?,?,?,?,?,'leased','synthetic-owner',"
                "'2099-01-01T00:00:00+00:00','2026-01-01','2026-01-01')",
                ("synthetic-lifecycle-lease", resource, "synthetic-revision", "recipe", "{}"),
            )
        blocker = sqlite3.connect(database.path)
        self.addCleanup(blocker.close)
        blocker.execute("BEGIN IMMEDIATE")
        recovery_entered = threading.Event()
        recovery_owner_ident: int | None = None
        recover = self.daemon._claim_resource_leases
        open_connection = database.connection

        def observed_recovery():
            nonlocal recovery_owner_ident
            self.assert_quiet()  # Includes capture: the pre-fix path fails here.
            recovery_owner_ident = threading.get_ident()
            recovery_entered.set()
            return recover()

        def recovery_connection():
            # The event may precede recovery opening its first connection. Allow
            # that owner through to real SQLite; diagnostic IPC runs elsewhere.
            self.assertEqual(threading.get_ident(), recovery_owner_ident, "DB scan")
            return open_connection()

        with (
            patch.object(self.daemon, "_claim_resource_leases", side_effect=observed_recovery),
            patch.object(self.daemon, "_claim_voice_leases",
                         wraps=self.daemon._claim_voice_leases) as voice_recovery,
            patch.object(self.daemon, "_install_runtime") as install,
            patch.object(old_tools, "close", wraps=old_tools.close) as old_close,
            ThreadPoolExecutor(max_workers=1) as pool,
        ):
            pending = pool.submit(self.client("operator", timeout=20).call, "operator.pause")
            self.assertTrue(recovery_entered.wait(3))
            before = time.monotonic()
            # This IPC must not acquire the lifecycle/state locks or scan storage.
            with (
                patch.object(self.daemon, "status", side_effect=AssertionError("full status")),
                patch.object(database, "connection", side_effect=recovery_connection),
                patch.object(database, "storage_status", side_effect=AssertionError("aggregate")),
            ):
                active = self.diagnostic()
            self.assertLess(time.monotonic() - before, 1)
            self.assertEqual(active["state"], "reloading")
            self.assertTrue(active["attempts"][0]["owners_stopped"])
            self.assertEqual(active["attempts"][0]["steps"][-1]["state"], "running")
            with self.assertRaisesRegex(IPCError, "runtime workers could not be quiesced"):
                pending.result(15)
            install.assert_not_called()
            old_close.assert_not_called()
            self.assertEqual(voice_recovery.call_count, 2)
        failed = self.diagnostic()
        self.assertEqual(failed["state"], "degraded")
        self.assertEqual([a["attempt"] for a in failed["attempts"]], ["stop", "compensation"])
        for attempt in failed["attempts"]:
            self.assertTrue(attempt["owners_stopped"])
            self.assertFalse(attempt["leases_recovered"])
            errors = [step for step in attempt["steps"] if step["state"] == "failed"]
            self.assertEqual(len(errors), 1)
            self.assertEqual((errors[0]["phase"], errors[0]["step"]), ("recovery", "resource"))
            self.assertEqual(errors[0]["error_type"], "OperationalError")
            self.assertEqual(errors[0]["error_location"]["module"], "sightglass.model.db")
            self.assertEqual(errors[0]["error_location"]["function"], "transaction")
            self.assertIsInstance(errors[0]["error_location"]["line"], int)
        encoded = json.dumps(failed)
        for private in (str(database.path), "BEGIN", "locked", "synthetic-owner", "recipe"):
            self.assertNotIn(private, encoded)
        self.assertIs(self.daemon.tools, old_tools)
        self.assertFalse(self.daemon._lock_handle.closed)
        self.assert_quiet()
        blocker.rollback()
        # Explicit operator retry is required; a successful replacement must not
        # overlap the retired capture owner or any of its worker threads.
        self.stop_edge()
        self.client("operator").call("operator.resume")
        self.assertIsNot(self.daemon.capture_core, old_core)
        self.connect_edge()
        self.fresh_read()
        with self.daemon.database.connection() as connection:
            lease = connection.execute(
                "SELECT state,owner_id,fencing_token FROM resource_jobs WHERE job_id=?",
                ("synthetic-lifecycle-lease",),
            ).fetchone()
        self.assertNotEqual(lease[0], "leased")
        self.assertIsNone(lease[1])
        self.assertGreater(lease[2], 0)

    def test_thread_join_failure_stops_later_owners_and_blocks_all_recovery(self):
        self.start()
        resource_thread = self.daemon.resource_worker._thread
        self.assertTrue(resource_thread.is_alive())
        with (
            patch.object(self.daemon.resource_worker, "stop"),
            patch.object(self.daemon.voice_worker, "stop",
                         wraps=self.daemon.voice_worker.stop) as voice_stop,
            patch.object(self.daemon.source_worker, "stop",
                         wraps=self.daemon.source_worker.stop) as source_stop,
            patch.object(self.core, "close", wraps=self.core.close) as capture_close,
            patch.object(self.daemon, "_claim_resource_leases") as resource_recovery,
            patch.object(self.daemon, "_claim_voice_leases") as voice_recovery,
            patch.object(self.daemon, "_install_runtime") as install,
        ):
            with self.assertRaisesRegex(IPCError, "could not be quiesced"):
                self.client("operator").call("operator.pause")
            self.assertEqual(voice_stop.call_count, 2)
            self.assertEqual(source_stop.call_count, 2)
            self.assertEqual(capture_close.call_count, 2)
            resource_recovery.assert_not_called()
            voice_recovery.assert_not_called()
            install.assert_not_called()
        failed = self.diagnostic()
        for attempt in failed["attempts"]:
            self.assertFalse(attempt["owners_stopped"])
            errors = [s for s in attempt["steps"] if s["state"] == "failed"]
            self.assertEqual(len(errors), 1)
            self.assertEqual((errors[0]["phase"], errors[0]["step"]), ("stop", "resource"))
            self.assertTrue(errors[0]["thread_alive"])
            self.assertEqual(errors[0]["error_location"]["function"], "_require_worker_stopped")
            blocked = [s for s in attempt["steps"] if s["state"] == "blocked"]
            self.assertEqual([s["step"] for s in blocked], ["resource", "voice"])
            self.assertTrue(all(s["reason"] == "producers_not_quiesced" for s in blocked))
        self.stop_edge()
        self.client("operator").call("operator.resume")
        self.assertFalse(resource_thread.is_alive())
        self.connect_edge()
        self.fresh_read()

    def test_capture_join_failure_keeps_original_and_compensation_causes_separate(self):
        self.start()
        previous_tools = self.daemon.tools
        with (
            patch.object(self.core, "close", side_effect=[
                TimeoutError("SYNTHETIC_PRIVATE_FIRST"), OSError("SYNTHETIC_PRIVATE_SECOND"),
            ]),
            patch.object(self.daemon, "_claim_resource_leases") as resource_recovery,
            patch.object(self.daemon, "_claim_voice_leases") as voice_recovery,
            patch.object(self.daemon, "_install_runtime") as install,
            patch.object(previous_tools, "close", wraps=previous_tools.close) as old_close,
        ):
            with self.assertRaisesRegex(IPCError, "could not be quiesced"):
                self.client("operator").call("operator.pause")
            resource_recovery.assert_not_called()
            voice_recovery.assert_not_called()
            install.assert_not_called()
            old_close.assert_not_called()
        failed = self.diagnostic()
        self.assertEqual(failed["state"], "degraded")
        for attempt, expected in zip(failed["attempts"], ("TimeoutError", "OSError"), strict=True):
            self.assertFalse(attempt["owners_stopped"])
            errors = [s for s in attempt["steps"] if s["state"] == "failed"]
            self.assertEqual(len(errors), 1)
            self.assertEqual((errors[0]["phase"], errors[0]["step"]), ("stop", "capture"))
            self.assertEqual(errors[0]["error_type"], expected)
        self.assertNotIn("PRIVATE", json.dumps(failed))
        self.assertFalse(self.daemon._lock_handle.closed)
        self.stop_edge()
        self.client("operator").call("operator.resume")
        self.connect_edge()
        self.fresh_read()

    def test_shutdown_recovery_failure_retains_database_lock_and_memory_evidence(self):
        self.start()
        database = self.daemon.database
        with (
            patch.object(self.daemon, "_claim_resource_leases",
                         side_effect=sqlite3.OperationalError("SYNTHETIC_PRIVATE")),
            patch.object(self.daemon, "_claim_voice_leases",
                         wraps=self.daemon._claim_voice_leases) as voice_recovery,
            patch.object(self.daemon.tools, "close", wraps=self.daemon.tools.close) as close,
            patch.object(self.daemon, "_install_runtime") as install,
        ):
            self.assertTrue(self.client("operator").call("daemon.shutdown")["stopping"])
            self.thread.join(5)
            self.assertFalse(self.thread.is_alive())
            close.assert_not_called()
            install.assert_not_called()
            voice_recovery.assert_called_once()
        self.assert_quiet()
        self.assertFalse(self.daemon._runtime_closed)
        self.assertFalse(self.daemon._lock_handle.closed)
        with database.connection() as connection:
            self.assertEqual(connection.execute("SELECT 1").fetchone()[0], 1)
        # Explicit shutdown retains its existing closed-listener semantics. Its
        # evidence remains in memory, not externally queryable after serving exits.
        lifecycle = self.daemon._dispatch(
            "operator", "daemon.status", {"operations_only": True})["lifecycle"]
        self.assertEqual(lifecycle["state"], "degraded")
        attempt = lifecycle["attempts"][0]
        self.assertTrue(attempt["owners_stopped"])
        self.assertFalse(attempt["leases_recovered"])
        failure = next(s for s in attempt["steps"] if s["state"] == "failed")
        self.assertEqual((failure["phase"], failure["step"], failure["error_type"]),
                         ("recovery", "resource", "OperationalError"))
        self.assertNotIn("PRIVATE", json.dumps(lifecycle))
        self.daemon.shutdown()  # Same owner can finish once recovery succeeds.
        self.assertTrue(self.daemon._runtime_closed)
        self.assertIsNone(self.daemon._lock_handle)

    def test_blocked_receipts_fail_closed_then_explicit_retry_drains_without_overlap(self):
        self.start()
        writer = self.daemon.tools.receipt_recorder
        owner = writer._thread
        previous_tools = self.daemon.tools
        entered, release = threading.Event(), threading.Event()
        persist = writer._persist
        self.addCleanup(release.set)

        def blocked_persist(**values):
            entered.set()
            release.wait()
            return persist(**values)

        with patch.object(writer, "_persist", side_effect=blocked_persist):
            request = {"name": "wechat_find_conversations", "arguments": {"query": "Synthetic"}}
            self.assertIn("candidates", self.client().call("tools.call", request))
            self.assertTrue(entered.wait(2))
            self.assertIn("candidates", self.client().call("tools.call", request))
            self.assertEqual(writer.status()["pending_count"], 1)
            with (
                patch("sightglass.runtime.receipts.RECEIPT_STOP_TIMEOUT_SECONDS", 0.02),
                patch.object(self.daemon, "_claim_resource_leases") as resource_recovery,
                patch.object(self.daemon, "_claim_voice_leases") as voice_recovery,
                patch.object(self.daemon, "_install_runtime") as install,
                patch.object(previous_tools, "close", wraps=previous_tools.close) as close,
            ):
                with self.assertRaisesRegex(IPCError, "could not be quiesced"):
                    self.client("operator").call("operator.pause")
                resource_recovery.assert_not_called()
                voice_recovery.assert_not_called()
                install.assert_not_called()
                close.assert_not_called()
            failed = self.diagnostic()
            self.assertEqual(failed["state"], "degraded")
            for attempt in failed["attempts"]:
                self.assertFalse(attempt["owners_stopped"])
                errors = [step for step in attempt["steps"] if step["state"] == "failed"]
                self.assertEqual(len(errors), 1)
                self.assertEqual((errors[0]["phase"], errors[0]["step"]), ("stop", "receipts"))
                self.assertTrue(errors[0]["thread_alive"])
                self.assertEqual(errors[0]["error_location"]["module"],
                                 "sightglass.runtime.receipts")
                self.assertEqual(errors[0]["error_location"]["function"], "stop")
                self.assertEqual([step["state"] for step in attempt["steps"]
                                  if step["phase"] == "recovery"], ["blocked", "blocked"])
            self.assertIs(writer._thread, owner)
            self.assertTrue(owner.is_alive())
            self.assertFalse(self.daemon._lock_handle.closed)
            release.set()
            self.stop_edge()
            original_install = self.daemon._install_runtime

            def checked_install(*args):
                self.assertFalse(owner.is_alive())
                self.assertFalse(args[1].receipt_writer_status()["alive"])
                return original_install(*args)

            with patch.object(self.daemon, "_install_runtime", side_effect=checked_install):
                self.client("operator").call("operator.resume")
        self.assertFalse(owner.is_alive())
        self.assertEqual(writer.status()["persisted_count"], 2)
        self.assertEqual(writer.status()["pending_count"], 0)
        self.assertIsNot(self.daemon.tools.receipt_recorder._thread, owner)
        with self.daemon.database.connection() as connection:
            count = connection.execute("SELECT COUNT(*) FROM access_receipts WHERE tool_name=?",
                                       ("wechat_find_conversations",)).fetchone()[0]
        self.assertEqual(count, 2)
        self.connect_edge()
        self.fresh_read()

    def test_receipt_compensation_restarts_same_writer_only_after_its_queue_drains(self):
        self.start()
        previous_tools = self.daemon.tools
        writer = previous_tools.receipt_recorder
        owner = writer._thread
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        persist, stop = writer._persist, writer.stop
        attempts = 0

        def blocked_persist(**values):
            entered.set()
            release.wait()
            return persist(**values)

        def stop_and_release():
            nonlocal attempts
            attempts += 1
            try:
                return stop(timeout=0.01 if attempts == 1 else 2)
            finally:
                release.set()

        with patch.object(writer, "_persist", side_effect=blocked_persist):
            self.client().call("tools.call", {
                "name": "wechat_find_conversations", "arguments": {"query": "Synthetic"}})
            self.assertTrue(entered.wait(2))
            with patch.object(writer, "stop", side_effect=stop_and_release):
                with self.assertRaisesRegex(IPCError, "could not be quiesced"):
                    self.client("operator").call("operator.pause")
        self.assertEqual(attempts, 2)
        self.assertIs(self.daemon.tools, previous_tools)
        self.assertIs(self.daemon.tools.receipt_recorder, writer)
        self.assertFalse(owner.is_alive())
        self.assertIsNot(writer._thread, owner)
        self.assertTrue(writer.status()["alive"])
        self.assertEqual(writer.status()["persisted_count"], 1)
        self.assertEqual(writer.status()["pending_count"], 0)
        lifecycle = self.diagnostic()
        self.assertEqual(lifecycle["state"], "ready")
        self.assertFalse(lifecycle["attempts"][0]["owners_stopped"])
        self.assertTrue(lifecycle["attempts"][1]["owners_stopped"])
        self.assertTrue(lifecycle["attempts"][1]["leases_recovered"])
        self.stop_edge()
        self.connect_edge()
        self.fresh_read()

    def test_shutdown_blocked_receipts_retains_database_then_rejoins_and_drains(self):
        self.start()
        writer = self.daemon.tools.receipt_recorder
        database = self.daemon.database
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        persist = writer._persist

        def blocked_persist(**values):
            entered.set()
            release.wait()
            return persist(**values)

        with patch.object(writer, "_persist", side_effect=blocked_persist):
            self.client().call("tools.call", {
                "name": "wechat_find_conversations", "arguments": {"query": "Synthetic"}})
            self.assertTrue(entered.wait(2))
            with (
                patch("sightglass.runtime.receipts.RECEIPT_STOP_TIMEOUT_SECONDS", 0.02),
                patch.object(self.daemon, "_claim_resource_leases") as resource_recovery,
                patch.object(self.daemon, "_claim_voice_leases") as voice_recovery,
                patch.object(self.daemon.tools, "close", wraps=self.daemon.tools.close) as close,
            ):
                self.client("operator").call("daemon.shutdown")
                self.thread.join(5)
                self.assertFalse(self.thread.is_alive())
                resource_recovery.assert_not_called()
                voice_recovery.assert_not_called()
                close.assert_not_called()
            self.assertFalse(self.daemon._runtime_closed)
            self.assertFalse(self.daemon._lock_handle.closed)
            with database.connection() as connection:
                self.assertEqual(connection.execute("SELECT 1").fetchone()[0], 1)
            lifecycle = self.daemon._dispatch(
                "operator", "daemon.status", {"operations_only": True})["lifecycle"]
            self.assertEqual(lifecycle["state"], "degraded")
            receipt_step = next(step for step in lifecycle["attempts"][0]["steps"]
                                if step["step"] == "receipts")
            self.assertEqual(receipt_step["state"], "failed")
            self.assertTrue(receipt_step["thread_alive"])
            release.set()
            self.daemon.shutdown()
        self.assertFalse(writer.status()["alive"])
        self.assertEqual(writer.status()["persisted_count"], 1)
        self.assertTrue(self.daemon._runtime_closed)
        self.assertIsNone(self.daemon._lock_handle)
        with database.connection() as connection:
            count = connection.execute("SELECT COUNT(*) FROM access_receipts").fetchone()[0]
            self.assertEqual(count, 1)

    def test_candidate_status_receipt_stays_dormant_before_startup_recovery(self):
        self.start()
        original_recovery = self.daemon._claim_resource_leases
        seen = []

        def recovery():
            writer = self.daemon.tools.receipt_recorder
            self.assertFalse(writer.status()["alive"])
            before = writer.status()
            # This reader call intentionally bypasses the reload state gate and
            # can reach the newly installed tools before startup recovery returns.
            status = self.client().call("tools.call", {"name": "wechat_status"})
            self.assertIn("ready", status)
            after = writer.status()
            self.assertEqual(after["persisted_count"], before["persisted_count"])
            self.assertEqual(after["pending_count"], 0)
            self.assertEqual(after["dropped_count"], before["dropped_count"] + 1)
            self.assertFalse(after["alive"])
            seen.append(writer)
            return original_recovery()

        with patch.object(self.daemon, "_claim_resource_leases", side_effect=recovery):
            self.client("operator").call("operator.pause")
        self.assertEqual(len(seen), 2)
        self.assertIsNot(seen[0], seen[1])
        self.assertTrue(seen[1].status()["alive"])
        self.stop_edge()
        self.client("operator").call("operator.resume")
        self.connect_edge()
        self.fresh_read()

    def test_connected_pause_resume_preserves_two_phase_order_and_fresh_reads(self):
        self.start()
        self.fresh_read()
        recover = self.daemon._claim_resource_leases
        seen = []

        def observed_recovery():
            self.assert_quiet()
            seen.append(True)
            return recover()

        # Only post-stop recovery is observed; startup recovery occurs before
        # capture/producers start and uses the newly installed resource service.
        with patch.object(self.daemon, "_claim_resource_leases", side_effect=observed_recovery):
            self.client("operator").call("operator.pause")
        self.assertEqual(len(seen), 2)
        self.stop_edge()
        paused = self.diagnostic()
        self.assertEqual(paused["state"], "ready")
        self.assertTrue(paused["attempts"][0]["owners_stopped"])
        self.assertTrue(paused["attempts"][0]["leases_recovered"])
        self.client("operator").call("operator.resume")
        self.connect_edge()
        self.fresh_read()

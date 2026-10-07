from __future__ import annotations

import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.runtime.derived_worker import DerivedIndexWorker
from sightglass.runtime.semantic_worker import SemanticWorker
from sightglass.runtime.source_worker import SourceWorker
from sightglass.runtime.worker_diagnostics import clear_error, error_fields, record_error


def wait_until(test: unittest.TestCase, predicate, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    test.fail("synthetic worker condition was not reached")


class WorkerSchedulingTests(unittest.TestCase):
    def test_derived_idle_wake_fallback_and_stop_are_bounded(self) -> None:
        links = Mock()
        links.has_pending_backfill.return_value = False
        links.state.return_value = {"state": "ready"}
        links.database.storage = None
        links.backfill_batch.return_value = {"state": "ready", "processed": 2}
        worker = DerivedIndexWorker(links, SimpleNamespace(paused=False))  # type: ignore[arg-type]
        worker.start()
        try:
            wait_until(self, lambda: worker.status()["idle_cycle_count"] == 1)
            time.sleep(0.15)
            links.backfill_batch.assert_not_called()
            self.assertEqual(links.state.call_count, 1)
            links.state.return_value = {"state": "building"}
            worker.wake()
            wait_until(self, lambda: worker.status()["processed_messages"] == 2)
        finally:
            started = time.monotonic()
            self.assertTrue(worker.stop())
            self.assertLess(time.monotonic() - started, 0.5)

        links.state.return_value = {"state": "ready"}
        fallback = DerivedIndexWorker(
            links, SimpleNamespace(paused=False), poll_seconds=0.05  # type: ignore[arg-type]
        )
        fallback.start()
        try:
            wait_until(self, lambda: fallback.status()["idle_cycle_count"] == 1)
            links.state.return_value = {"state": "building"}
            wait_until(self, lambda: fallback.status()["processed_messages"] >= 2)
        finally:
            fallback.stop()

    def test_wake_during_queue_probe_is_retained_for_the_next_cycle(self) -> None:
        links = Mock()
        links.has_pending_backfill.return_value = False
        links.database.storage = None
        reader = SimpleNamespace(paused=False)
        worker = DerivedIndexWorker(links, reader)  # type: ignore[arg-type]
        first_probe = True

        def state():
            nonlocal first_probe
            if first_probe:
                first_probe = False
                worker.wake()  # committed rebuild during this already-read ready result
                return {"state": "ready"}
            return {"state": "building"}

        links.state.side_effect = state
        links.backfill_batch.return_value = {"state": "ready", "processed": 1}
        worker.start()
        try:
            wait_until(self, lambda: worker.status()["processed_messages"] >= 1)
            self.assertGreaterEqual(worker.status()["wake_count"], 1)
        finally:
            worker.stop()

    def test_original_derived_failure_is_safe_and_survives_later_ready(self) -> None:
        links = Mock()
        links.has_pending_backfill.return_value = False
        links.state.side_effect = ValueError("/private/synthetic-path account-id secret-message")
        worker = DerivedIndexWorker(links, SimpleNamespace(paused=False))  # type: ignore[arg-type]
        worker.start()
        try:
            wait_until(self, lambda: worker.status()["historical_error_count"] == 1)
            failed = worker.status()
            self.assertEqual(failed["last_error_code"], "INTERNAL_ERROR")
            self.assertEqual(failed["last_error_type"], "ValueError")
            self.assertEqual(failed["last_error_location"]["module"],
                             "sightglass.runtime.derived_worker")
            for sentinel in ("synthetic-path", "account-id", "secret-message", "args", "locals"):
                self.assertNotIn(sentinel, str(failed))
            links.state.side_effect = None
            links.state.return_value = {"state": "ready"}
            worker.wake()
            wait_until(self, lambda: worker.status()["last_error_code"] is None)
            self.assertEqual(worker.status()["historical_error_type"], "ValueError")
            self.assertEqual(worker.status()["historical_error_count"], 1)
        finally:
            worker.stop()

    def test_diagnostics_keep_original_cause_but_never_exception_arguments(self) -> None:
        state = {"last_error_code": None, **error_fields()}
        try:
            try:
                raise ValueError("private synthetic text")
            except ValueError as cause:
                raise SightglassError(ErrorCode.INTERNAL_ERROR) from cause
        except SightglassError as exc:
            record_error(state, exc, exc.code.value)
        self.assertEqual(state["last_error_type"], "ValueError")
        self.assertIsNone(state["last_error_location"])
        self.assertNotIn("private synthetic text", str(state))
        clear_error(state)
        self.assertIsNone(state["last_error_code"])
        self.assertEqual(state["historical_error_count"], 1)

    def test_diagnostics_never_stringify_hostile_exception_surfaces(self) -> None:
        sentinel = "private-synthetic-body-path-id"

        class SensitiveError(Exception):
            def __str__(self):
                raise AssertionError("worker diagnostics stringified an exception")

            def __repr__(self):
                raise AssertionError("worker diagnostics rendered an exception")

        sensitive = SensitiveError(sentinel)
        sensitive.add_note(sentinel)
        wrapped = SightglassError(ErrorCode.INTERNAL_ERROR)
        wrapped.__cause__ = sensitive
        grouped = ExceptionGroup(sentinel, [wrapped, sensitive])
        for failure in (sensitive, wrapped, grouped):
            state = {"last_error_code": None, **error_fields()}
            record_error(state, failure, "INTERNAL_ERROR")
            self.assertNotIn(sentinel, str(state))
            self.assertIsNone(state["last_error_location"])
            self.assertEqual(state["historical_error_count"], 1)

    def test_semantic_event_wake_preserves_independent_idle_lane(self) -> None:
        service = Mock()
        service.reader.paused = False
        service.settings.timeout_seconds = 0.1
        service.index_once.return_value = {"state": "ready", "scanned": 0}
        worker = SemanticWorker(service)
        links = Mock()
        links.has_pending_backfill.return_value = False
        links.state.return_value = {"state": "ready"}
        derived = DerivedIndexWorker(links, SimpleNamespace(paused=False))  # type: ignore[arg-type]
        worker.start()
        derived.start()
        try:
            wait_until(self, lambda: worker.status()["idle_cycle_count"] == 1)
            wait_until(self, lambda: derived.status()["idle_cycle_count"] == 1)
            derived.wake()
            wait_until(self, lambda: derived.status()["idle_cycle_count"] == 2)
            time.sleep(0.15)
            self.assertEqual(service.index_once.call_count, 1)
            worker.wake()
            wait_until(self, lambda: service.index_once.call_count == 2)
        finally:
            self.assertTrue(worker.stop())
            self.assertTrue(derived.stop())

    def test_source_on_demand_metadata_poll_is_quiet_and_idle_sweep_opens_no_source(self) -> None:
        service = Mock()
        service.provider.descriptor.supports_incremental = True
        service.provider.descriptor.source_mode = "synthetic"
        service.residency.settings.return_value = SimpleNamespace(default_mode="on_demand")
        service.sync_source_once.return_value = {
            "conversation_count": 0, "message_count": 0, "pending_conversation_count": 0,
        }
        service.process_backfill_once.return_value = {"state": "idle"}
        worker = SourceWorker(service, poll_interval_seconds=0.1)
        with patch("sightglass.runtime.source_worker.collecting_conversations", return_value=()):
            worker.start()
            try:
                wait_until(self, lambda: worker.status()["idle_cycle_count"] == 1)
                time.sleep(0.2)
                self.assertEqual(service.sync_source_once.call_count, 1)
                self.assertEqual(service.provider.maintain_idle_connections.call_count, 1)
                worker.foreground_enter()
                worker.foreground_exit()
                wait_until(self, lambda: service.sync_source_once.call_count == 2)
            finally:
                started = time.monotonic()
                worker.stop()
                self.assertLess(time.monotonic() - started, 0.5)

    def test_source_keep_recent_and_pending_catalog_retain_fast_cadence(self) -> None:
        service = Mock()
        service.provider.descriptor.supports_incremental = True
        service.provider.descriptor.source_mode = "synthetic"
        service.residency.settings.return_value = SimpleNamespace(default_mode="on_demand")
        service.reader.policy.permits.return_value = True
        worker = SourceWorker(service, poll_interval_seconds=0.1)
        with patch(
            "sightglass.runtime.source_worker.collecting_conversations", return_value=("conv",)
        ):
            self.assertEqual(worker._next_source_delay(), 0.1)
            service.reader.policy.permits.return_value = False
            self.assertEqual(worker._next_source_delay(), 30.0)
        service.residency.settings.return_value = SimpleNamespace(default_mode="recent")
        self.assertEqual(worker._next_source_delay(), 0.1)

    def test_source_diagnostic_reason_never_projects_private_detail(self) -> None:
        service = Mock()
        service.provider.descriptor.supports_incremental = True
        service.provider.descriptor.source_mode = "synthetic"
        service.sync_source_once.side_effect = SightglassError(
            ErrorCode.SOURCE_GENERATION_CHANGED, details={"reason": "/private/synthetic-account"}
        )
        worker = SourceWorker(service, poll_interval_seconds=30)
        worker.start()
        try:
            wait_until(self, lambda: worker.status()["historical_error_count"] == 1)
            self.assertIsNone(worker.status()["last_error_reason"])
            self.assertNotIn("synthetic-account", str(worker.status()))
        finally:
            worker.stop()

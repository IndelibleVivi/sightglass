from __future__ import annotations

import gc
import json
import sys
import threading
import time
import unittest
import weakref
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock, patch

from sightglass.operations import wait_for_event
from sightglass.runtime.daemon import (
    ACTIVE_STACK_FRAME_LIMIT,
    ACTIVE_STACK_WALK_LIMIT,
    SightglassDaemon,
    _InflightOperation,
)
from sightglass.runtime.lanes import RuntimeLanes
from sightglass.runtime.observability import ToolMetrics


class _SyntheticFrame:
    def __init__(
        self,
        module: str,
        function: str,
        back: _SyntheticFrame | None = None,
    ) -> None:
        self.f_globals = {
            "__name__": module,
            "synthetic_private_query": "SYNTHETIC_PRIVATE_CONTENT",
        }
        self.f_code = SimpleNamespace(
            co_name=function,
            co_filename="/synthetic/private/source.py",
        )
        self.f_lineno = 42
        self._back = back
        self.back_reads = 0

    @property
    def f_back(self) -> _SyntheticFrame | None:
        self.back_reads += 1
        return self._back

    @property
    def f_locals(self) -> Any:
        raise AssertionError("operation diagnostics must never inspect locals")


class RuntimeOperationStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        # A generated registry-only daemon has no config, source, secrets or database.
        self.daemon = SightglassDaemon.__new__(SightglassDaemon)
        self.daemon._operations_lock = threading.RLock()
        self.daemon._operations = {}
        self.daemon._joined_tool_calls = 0
        self.daemon._timed_out_tool_calls = 0
        self.daemon._metrics_lock = threading.Lock()
        self.daemon.total_bridge_calls = 0
        self.daemon.active_bridge_calls = 0
        self.daemon.lanes = RuntimeLanes()
        self.daemon.tool_metrics = ToolMetrics()
        forbidden = Mock(side_effect=AssertionError("diagnostics entered full runtime state"))
        self.daemon.tools = cast(Any, SimpleNamespace(service=SimpleNamespace(
            repository=SimpleNamespace(database=SimpleNamespace(connection=forbidden)),
        )))
        self.daemon._state_gate = cast(Any, SimpleNamespace(read=forbidden, write=forbidden))
        self.daemon.source_worker = cast(Any, SimpleNamespace(foreground_enter=forbidden))

    def _add_operation(self, ident: int = 123) -> None:
        self.daemon._operations["synthetic-operation"] = _InflightOperation(
            kind="wechat_search_messages",
            started_at=time.monotonic(),
            completed=threading.Event(),
            owner_thread_ident=ident,
        )

    def _status(self, *, include_stack: bool = False) -> dict[str, Any]:
        return self.daemon._dispatch(
            "operator", "daemon.status",
            {"operations_only": True, "include_stack": include_stack},
        )

    def test_operations_only_bypasses_full_status_database_lanes_and_gates(self) -> None:
        with (
            patch.object(self.daemon, "status", side_effect=AssertionError("full status")),
            patch.object(self.daemon.lanes, "status", side_effect=AssertionError("lane status")),
            patch.object(self.daemon.lanes, "held", side_effect=AssertionError("lane lease")),
            patch("sightglass.runtime.daemon.sys._current_frames",
                  side_effect=AssertionError("stack was not requested")),
        ):
            status = self._status()
        self.assertEqual(status["schema"], "sightglass.operation-status.v1")
        self.assertEqual(status["active_count"], 0)
        self.assertNotIn("active_stack", status)
        self.assertNotIn("active_phase", status)

    def test_diagnostic_flags_require_operator_and_explicit_stack_opt_in(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "operator role"):
            self.daemon._dispatch("reader", "daemon.status", {"operations_only": True})
        for arguments in (
            {"include_stack": True},
            {"operations_only": "true"},
            {"operations_only": True, "include_stack": 1},
            {"operations_only": True, "unexpected": True},
        ):
            with self.subTest(arguments=arguments), self.assertRaises(RuntimeError):
                self.daemon._dispatch("operator", "daemon.status", arguments)
        sentinel = {"schema": "synthetic-normal-status"}
        with patch.object(self.daemon, "status", return_value=sentinel) as ordinary:
            self.assertIs(self.daemon._dispatch("reader", "daemon.status", {}), sentinel)
        ordinary.assert_called_once_with()

    def test_default_active_status_never_samples_or_projects_stack(self) -> None:
        self._add_operation()
        with patch("sightglass.runtime.daemon.sys._current_frames",
                   side_effect=AssertionError("ordinary status sampled a stack")):
            status = self.daemon._operation_status()
        self.assertEqual(status["active_count"], 1)
        self.assertNotIn("active_stack", status)
        self.assertNotIn("active_phase", status)

    def test_idle_stack_does_not_take_a_thread_snapshot(self) -> None:
        with patch("sightglass.runtime.daemon.sys._current_frames",
                   side_effect=AssertionError("idle status sampled threads")):
            status = self._status(include_stack=True)
        self.assertEqual(status["active_stack"], [])
        self.assertIsNone(status["active_phase"])

    def test_stack_contains_only_eight_public_code_locations_from_oldest_owner(self) -> None:
        self._add_operation(123)
        self.daemon._operations["newer"] = _InflightOperation(
            kind="wechat_find_conversations",
            started_at=time.monotonic() + 1,
            completed=threading.Event(),
            owner_thread_ident=456,
        )
        frame = None
        for index in range(12):
            frame = _SyntheticFrame("sightglass.reader.service", f"public_stage_{index}", frame)
        frame = _SyntheticFrame("synthetic.private.module", "PRIVATE_FUNCTION", frame)
        with patch("sightglass.runtime.daemon.sys._current_frames", return_value={
            123: frame,
            456: _SyntheticFrame("sightglass.reader.service", "newer_operation"),
        }):
            status = self._status(include_stack=True)
        self.assertEqual(status["active_operation"], "wechat_search_messages")
        self.assertEqual(len(status["active_stack"]), ACTIVE_STACK_FRAME_LIMIT)
        self.assertEqual(status["active_phase"], status["active_stack"][0])
        self.assertEqual(status["active_phase"]["function"], "public_stage_11")
        self.assertTrue(all(set(item) == {"module", "function", "line"}
                            for item in status["active_stack"]))
        encoded = json.dumps(status)
        for excluded in ("PRIVATE", "private", "/", "owner_thread", "newer_operation"):
            self.assertNotIn(excluded, encoded)

    def test_stack_walk_is_bounded_when_all_frames_are_private(self) -> None:
        self._add_operation()
        chain = []
        frame = None
        for _ in range(ACTIVE_STACK_WALK_LIMIT + 20):
            frame = _SyntheticFrame("synthetic.private.module", "private_stage", frame)
            chain.append(frame)
        with patch("sightglass.runtime.daemon.sys._current_frames", return_value={123: frame}):
            status = self._status(include_stack=True)
        self.assertEqual(status["active_stack"], [])
        self.assertEqual(sum(item.back_reads for item in chain), ACTIVE_STACK_WALK_LIMIT)

    def test_frame_sampling_releases_registry_lock_and_does_not_retain_frames(self) -> None:
        self._add_operation()
        registry_lock = threading.Lock()
        references = []

        def snapshot():
            self.assertTrue(registry_lock.acquire(blocking=False))
            registry_lock.release()
            frame = _SyntheticFrame("sightglass.model.repositories", "synthetic_read")
            references.append(weakref.ref(frame))
            return {123: frame}

        with (
            patch.object(self.daemon, "_operations_lock", registry_lock),
            patch("sightglass.runtime.daemon.sys._current_frames", side_effect=snapshot),
        ):
            status = self._status(include_stack=True)
        self.assertEqual(status["active_phase"]["function"], "synthetic_read")
        gc.collect()
        self.assertIsNone(references[0]())

    def test_single_flight_stack_tracks_owner_and_joiner_never_rebinds_it(self) -> None:
        entered = threading.Event()
        joined = threading.Event()
        release = threading.Event()
        calls = []
        results = []

        def synthetic_tool(**arguments):
            calls.append(arguments)
            entered.set()
            wait_for_event(release)
            return {"schema": "synthetic-result"}

        def joining_wait(event):
            joined.set()
            wait_for_event(event)

        params = {"name": "wechat_find_conversations", "arguments": {
            "query": "SYNTHETIC_PRIVATE_CONTENT",
        }}
        owner = threading.Thread(target=lambda: results.append(self.daemon._dispatch_tool(params)))
        joiner = threading.Thread(target=lambda: results.append(self.daemon._dispatch_tool(params)))
        with (
            patch.object(
                self.daemon.tools, "wechat_find_conversations", synthetic_tool, create=True,
            ),
            patch("sightglass.runtime.daemon.wait_for_event", side_effect=joining_wait),
        ):
            owner.start()
            try:
                self.assertTrue(entered.wait(1))
                joiner.start()
                self.assertTrue(joined.wait(1))
                operation = next(iter(self.daemon._operations.values()))
                self.assertEqual(operation.owner_thread_ident, owner.ident)
                self.assertNotEqual(operation.owner_thread_ident, joiner.ident)
                status = self._status(include_stack=True)
                self.assertEqual(status["active_count"], 1)
                self.assertEqual(status["joined_call_count"], 1)
                self.assertEqual(status["active_phase"]["module"], "sightglass.operations")
                self.assertEqual(status["active_phase"]["function"], "wait_for_event")
                self.assertNotIn("SYNTHETIC_PRIVATE_CONTENT", json.dumps(status))
            finally:
                release.set()
                owner.join(2)
                if joiner.ident is not None:
                    joiner.join(2)
        self.assertFalse(owner.is_alive())
        self.assertFalse(joiner.is_alive())
        self.assertEqual(len(calls), 1)
        self.assertEqual(results, [{"schema": "synthetic-result"}] * 2)
        self.assertEqual(self.daemon.active_bridge_calls, 0)
        with patch.object(sys, "_current_frames", side_effect=AssertionError("owner remains")):
            idle = self._status(include_stack=True)
        self.assertEqual(idle["active_count"], 0)
        self.assertEqual(idle["active_stack"], [])


if __name__ == "__main__":
    unittest.main()

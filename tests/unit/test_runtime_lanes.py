from __future__ import annotations

import threading
import time
import unittest

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.operations import operation_budget
from sightglass.resources.coalesce import StageCoalescer
from sightglass.runtime.lanes import LaneLimits, RuntimeLanes, WorkClass
from sightglass.runtime.observability import ToolMetrics


class RuntimeLaneTests(unittest.TestCase):
    def test_source_saturation_does_not_consume_local_capacity(self) -> None:
        lanes = RuntimeLanes(LaneLimits(local_read=2, source_read=1))
        source = lanes.try_acquire(WorkClass.SOURCE_READ)
        self.assertIsNotNone(source)
        self.assertIsNone(lanes.try_acquire(WorkClass.SOURCE_READ))
        local = lanes.try_acquire(WorkClass.LOCAL_READ)
        self.assertIsNotNone(local)
        assert source is not None and local is not None
        local.release()
        source.release()
        status = lanes.status()
        self.assertEqual(status["lanes"]["source_read"]["busy_count"], 1)
        self.assertEqual(status["lanes"]["local_read"]["completed_count"], 1)

    def test_stage_joiner_timeout_does_not_cancel_the_owner(self) -> None:
        coalescer = StageCoalescer()
        entered = threading.Event()
        release = threading.Event()
        owner_result: list[str] = []

        def produce() -> str:
            entered.set()
            self.assertTrue(release.wait(timeout=2))
            return "ready"

        owner = threading.Thread(
            target=lambda: owner_result.append(coalescer.run("same", produce))
        )
        owner.start()
        self.assertTrue(entered.wait(timeout=1))
        started = time.monotonic()
        with self.assertRaises(SightglassError) as caught:
            with operation_budget(0.05):
                coalescer.run("same", lambda: "must-not-run")
        self.assertEqual(caught.exception.code, ErrorCode.SERVICE_TIMEOUT)
        self.assertLess(time.monotonic() - started, 0.5)
        release.set()
        owner.join(timeout=2)
        self.assertEqual(owner_result, ["ready"])
        self.assertEqual(coalescer.status()["joined_count"], 1)

    def test_tool_metrics_are_bounded_and_content_free(self) -> None:
        metrics = ToolMetrics()
        for duration in range(300):
            metrics.record(
                "wechat_read_resource",
                elapsed_ms=duration,
                result={"schema": "sightglass.resource-read.v1", "secret": "ignored"},
            )
        metrics.record(
            "wechat_read_resource",
            elapsed_ms=500,
            result={
                "schema": "sightglass.error.v1",
                "ok": False,
                "code": "SERVICE_TIMEOUT",
                "details": {"query": "must-not-be-retained"},
            },
        )

        status = metrics.status()
        tool = status["tools"]["wechat_read_resource"]
        self.assertEqual(tool["success_count"], 300)
        self.assertEqual(tool["error_count"], 1)
        self.assertEqual(tool["timeout_count"], 1)
        self.assertEqual(tool["sample_count"], 256)
        self.assertEqual(tool["latency_ms"]["max"], 500)
        self.assertNotIn("secret", str(status))
        self.assertNotIn("must-not-be-retained", str(status))


if __name__ == "__main__":
    unittest.main()

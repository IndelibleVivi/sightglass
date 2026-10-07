from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from sightglass.cli import _control_call, _parser
from sightglass.runtime.daemon import OPERATOR_METHODS, READER_METHODS, SightglassDaemon
from tests.unit import test_storage_history


class ObservationMaintenanceCLITests(unittest.TestCase):
    def test_inspect_forwards_bounded_continuation_only_to_operator(self) -> None:
        args = _parser().parse_args([
            "maintenance", "observations", "inspect", "--limit", "20",
            "--after-message-id", "wxmsg_synthetic_checkpoint",
        ])
        client = Mock()
        with patch("sightglass.cli._operator", return_value=client):
            _control_call(args, Mock())
        client.call.assert_called_once_with(
            "operator.maintenance.observations.inspect",
            {"limit": 20, "after_message_id": "wxmsg_synthetic_checkpoint"},
        )

    def test_repair_is_an_explicit_command_and_uses_its_durable_checkpoint(self) -> None:
        args = _parser().parse_args(["maintenance", "observations", "repair"])
        client = Mock()
        with patch("sightglass.cli._operator", return_value=client):
            _control_call(args, Mock())
        client.call.assert_called_once_with(
            "operator.maintenance.observations.repair", {"limit": 100}
        )
        for action in ("inspect", "repair"):
            method = f"operator.maintenance.observations.{action}"
            self.assertIn(method, OPERATOR_METHODS)
            self.assertNotIn(method, READER_METHODS)


class ObservationMaintenanceOperatorTests(unittest.TestCase):
    setUp = test_storage_history.StorageHistoryDaemonLifecycleTests.setUp
    daemon: SightglassDaemon

    def test_actual_inspector_stays_read_only_and_outside_reader_role(self) -> None:
        with patch.object(self.daemon.database, "transaction", side_effect=AssertionError("write")):
            result = self.daemon._dispatch(
                "operator", "operator.maintenance.observations.inspect", {"limit": 1}
            )
        self.assertEqual(result["schema"], "sightglass.observation-consistency.v1")
        self.assertEqual(result["mode"], "inspect")
        self.assertEqual(result["examined_count"], 0)
        self.assertTrue(result["complete"])
        with self.assertRaises(RuntimeError):
            self.daemon._dispatch("reader", "operator.maintenance.observations.inspect", {})

    def test_actual_repair_is_bounded_and_wakes_derivatives(self) -> None:
        with patch.object(self.daemon.derived_worker, "wake") as wake:
            result = self.daemon._dispatch(
                "operator", "operator.maintenance.observations.repair", {"limit": 1}
            )
        self.assertEqual(result["mode"], "repair")
        self.assertEqual(result["repaired_count"], 0)
        self.assertTrue(result["complete"])
        wake.assert_called_once_with()
        with self.assertRaises(RuntimeError):
            self.daemon._dispatch("reader", "operator.maintenance.observations.repair", {})


if __name__ == "__main__":
    unittest.main()

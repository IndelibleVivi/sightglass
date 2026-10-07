from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from sightglass.cli import (
    DAEMON_START_TIMEOUT_ENV,
    DAEMON_START_TIMEOUT_SECONDS,
    _daemon_start,
    _daemon_start_timeout,
    _daemon_stop,
)


class FakeClock:
    """Virtual monotonic clock so a bounded wait can be observed instantly."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class DaemonControlTests(unittest.TestCase):
    def test_stop_waits_past_the_worker_join_window(self) -> None:
        client = Mock()
        client.call.side_effect = [
            {"pid": 4242},
            {"stopping": True, "instance_id": "fixture"},
        ]
        store = Mock()
        running = iter((True, True, False, False))

        with (
            patch("sightglass.cli._operator", return_value=client),
            patch("sightglass.cli.process_is_running", side_effect=lambda _pid: next(running)),
            patch("sightglass.cli.time.monotonic", side_effect=(0.0, 8.5, 9.5, 10.5)),
            patch("sightglass.cli.time.sleep"),
        ):
            result = _daemon_stop(store)

        self.assertTrue(result["stopping"])


class DaemonStartTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.log_path = self.root / "sightglassd.log"
        self.clock = FakeClock()
        self.store = Mock()
        self.store.load.return_value = Mock(data_dir=self.root, socket_path=self.root / "s.sock")
        self.store.path = self.root / "config.json"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def start_with(self, client: Mock, process: Mock) -> dict:
        with (
            patch("sightglass.cli._operator", return_value=client),
            patch("sightglass.cli.subprocess.Popen", return_value=process) as popen,
            patch("sightglass.cli.time.monotonic", self.clock.monotonic),
            patch("sightglass.cli.time.sleep", self.clock.sleep),
        ):
            popen.return_value = process
            return _daemon_start(self.store)

    @staticmethod
    def client_ready_after(clock: FakeClock, ready_at: float, status: dict) -> Mock:
        def call(_method: str) -> dict:
            if clock.now < ready_at:
                raise RuntimeError("not ready yet")
            return status

        client = Mock()
        client.call.side_effect = call
        return client

    def test_already_running_daemon_is_returned_untouched(self) -> None:
        client = Mock()
        client.call.return_value = {"ready": True, "pid": 1}
        process = Mock()
        process.poll.return_value = None
        result = self.start_with(client, process)
        self.assertTrue(result["already_running"])
        process.terminate.assert_not_called()

    def test_slow_first_open_migration_is_not_killed(self) -> None:
        # A first start that upgrades a large window.db binds its socket only after the
        # private-state reconciliation finishes; readiness must tolerate that.
        status = {"ready": True, "pid": 4242, "window_db_schema_version": 4}
        client = self.client_ready_after(self.clock, 45.0, status)
        process = Mock()
        process.poll.return_value = None

        result = self.start_with(client, process)

        self.assertNotIn("already_running", result)
        self.assertEqual(result["pid"], 4242)
        self.assertGreaterEqual(self.clock.now, 45.0)
        process.terminate.assert_not_called()

    def test_unresponsive_start_is_still_bounded_and_terminated(self) -> None:
        client = Mock()
        client.call.side_effect = RuntimeError("never ready")
        process = Mock()
        process.poll.return_value = None

        with self.assertRaises(RuntimeError) as caught:
            self.start_with(client, process)

        process.terminate.assert_called_once()
        self.assertGreaterEqual(self.clock.now, DAEMON_START_TIMEOUT_SECONDS)
        self.assertLess(self.clock.now, DAEMON_START_TIMEOUT_SECONDS + 1.0)
        self.assertIn("did not become ready", str(caught.exception))
        self.assertIn(DAEMON_START_TIMEOUT_ENV, str(caught.exception))

    def test_daemon_that_exits_early_points_at_the_log(self) -> None:
        client = Mock()
        client.call.side_effect = RuntimeError("not ready")
        process = Mock()
        process.poll.return_value = 1

        with self.assertRaises(RuntimeError) as caught:
            self.start_with(client, process)

        self.assertIn("exited before becoming ready", str(caught.exception))
        self.assertIn(str(self.log_path), str(caught.exception))
        process.terminate.assert_not_called()

    def test_stale_lock_and_socket_do_not_block_readiness_polling(self) -> None:
        (self.root / "sightglassd.sock").write_text("stale")
        status = {"ready": True, "pid": 7}
        client = self.client_ready_after(self.clock, 0.2, status)
        process = Mock()
        process.poll.return_value = None
        result = self.start_with(client, process)
        self.assertEqual(result["pid"], 7)
        self.assertEqual(os.stat(self.log_path).st_mode & 0o777, 0o600)


class DaemonStartTimeoutTests(unittest.TestCase):
    def test_default_window(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop(DAEMON_START_TIMEOUT_ENV, None)
            self.assertEqual(_daemon_start_timeout(), DAEMON_START_TIMEOUT_SECONDS)

    def test_environment_override(self) -> None:
        with patch.dict(os.environ, {DAEMON_START_TIMEOUT_ENV: "300"}):
            self.assertEqual(_daemon_start_timeout(), 300.0)

    def test_invalid_override_falls_back_to_the_default(self) -> None:
        for value in ("", "0", "-5", "not-a-number"):
            environment = {DAEMON_START_TIMEOUT_ENV: value}
            with self.subTest(value=value), patch.dict(os.environ, environment):
                self.assertEqual(_daemon_start_timeout(), DAEMON_START_TIMEOUT_SECONDS)


if __name__ == "__main__":
    unittest.main()

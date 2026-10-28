"""Generated operator fixtures only: no actual SSH, Keychain, vault or services."""

from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import vault_gateway as gateway
from scripts import vault_recovery as recovery


def status(*, mounted=True, active=True):
    return {"schema": gateway.STATUS_SCHEMA, "mapped": mounted, "mounted": mounted,
            "deployment_ready": mounted, "services_active": active}


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)
        self.config = {"ssh_alias": "synthetic-vault", "keychain_helper": "/synthetic/helper",
                       "keychain_account": "synthetic-key-reference",
                       **{k: str(self.root / k) for k in ("receipt", "lock", "inhibit")}}
        self.controller = recovery.Recovery(self.config)

    def test_mounted_never_reads_key_or_restarts_services(self):
        for active, expected in ((True, "healthy"), (False, "services_stopped")):
            with patch.object(self.controller, "remote", return_value=status(active=active)) as rpc:
                with patch.object(self.controller, "run") as run:
                    result = self.controller.tick(100)
                    self.assertEqual(result["state"], expected)
                    run.assert_not_called()
                    self.assertEqual(rpc.call_args.args, (gateway.STATUS_FRAME,))

    def test_locked_recovers_with_exact_key_then_independent_confirmation(self):
        key = b"synthetic fixture key".ljust(32, b"!")
        with patch.object(self.controller, "remote", side_effect=[status(mounted=False),
                {"unlocked": True, "deployment_ready": True}, status()]) as rpc:
            with patch.object(self.controller, "run", return_value=
                              subprocess.CompletedProcess([], 0, key, b"")) as run:
                result = self.controller.tick(100)
        self.assertEqual(result["state"], "recovered")
        self.assertEqual(rpc.call_args_list[1].args, (b"R" + key,))
        self.assertEqual(run.call_args.args[0][1], "emit")
        contents = Path(self.config["receipt"]).read_bytes()
        self.assertNotIn(key, contents)
        self.assertNotIn(b"synthetic-key-reference", contents)
        self.assertEqual(Path(self.config["receipt"]).stat().st_mode & 0o777, 0o600)

    def test_locked_keychain_never_attempts_authorize_or_unlock(self):
        with patch.object(self.controller, "remote", return_value=status(mounted=False)) as rpc:
            with patch.object(self.controller, "run", return_value=
                              subprocess.CompletedProcess([], 1, b"", b"private error")) as run:
                result = self.controller.tick(100)
        self.assertEqual(result["state"], "keychain_unavailable")
        self.assertEqual(rpc.call_count, 1)
        self.assertNotIn("authorize", run.call_args.args[0])

    def test_probe_failure_does_not_read_key_and_backs_off(self):
        with patch.object(self.controller, "remote", side_effect=ValueError("private path")) as rpc:
            with patch.object(self.controller, "run") as key:
                first = self.controller.tick(100)
                self.assertEqual(self.controller.tick(120)["state"], "backoff")
                second = self.controller.tick(161)
                for i in range(8):
                    last = self.controller.tick(10_000 + i * 1000)
                key.assert_not_called()
        self.assertAlmostEqual(first["retry_after"], 160, places=2)
        self.assertAlmostEqual(second["retry_after"], 281, places=2)
        self.assertLessEqual(last["retry_after"] - last["checked_at"], 900)
        self.assertEqual(rpc.call_count, 10)
        self.assertNotIn("private path", Path(self.config["receipt"]).read_text())

    def test_unproved_or_foreign_status_does_not_read_key(self):
        for value in ({}, {**status(), "mounted": "true"},
                      {**status(), "mapped": False}, {**status(), "schema": "v0"}):
            with patch.object(self.controller, "remote", return_value=value):
                with patch.object(self.controller, "run") as key:
                    with self.assertRaises(ValueError):
                        self.controller.attempt()
                    key.assert_not_called()

    def test_unlock_success_is_not_healthy_until_post_probe(self):
        with patch.object(self.controller, "remote", side_effect=[status(mounted=False),
                {"unlocked": True, "deployment_ready": True}, status(active=False)]):
            with patch.object(self.controller, "run", return_value=
                              subprocess.CompletedProcess([], 0, b"k" * 32, b"")):
                self.assertEqual(self.controller.tick(100)["state"], "recovery_unconfirmed")

    def test_inhibit_skips_all_remote_and_keychain_work(self):
        Path(self.config["inhibit"]).touch()
        with patch.object(self.controller, "remote") as rpc:
            self.assertEqual(self.controller.tick(100)["state"], "inhibited")
            rpc.assert_not_called()

    def test_new_inhibit_during_status_or_helper_prevents_key_transfer(self):
        def inhibit_after_probe(_frame):
            Path(self.config["inhibit"]).touch()
            return status(mounted=False)
        with patch.object(self.controller, "remote", side_effect=inhibit_after_probe):
            with patch.object(self.controller, "run") as key:
                self.assertEqual(self.controller.attempt(), "inhibited")
                key.assert_not_called()
        Path(self.config["inhibit"]).unlink()
        def inhibit_after_key(_args):
            Path(self.config["inhibit"]).touch()
            return subprocess.CompletedProcess([], 0, b"k" * 32, b"")
        with patch.object(self.controller, "remote", return_value=status(mounted=False)) as rpc:
            with patch.object(self.controller, "run", side_effect=inhibit_after_key):
                self.assertEqual(self.controller.attempt(), "inhibited")
                self.assertEqual(rpc.call_count, 1)

    def test_mounted_without_ready_marker_is_not_healthy(self):
        with patch.object(self.controller, "remote", return_value=
                          {**status(), "deployment_ready": False}):
            with patch.object(self.controller, "run") as key:
                self.assertEqual(self.controller.attempt(), "deployment_not_ready")
                key.assert_not_called()

    def test_slow_failure_backoff_begins_at_completion(self):
        with patch.object(recovery.time, "monotonic", side_effect=[1.0, 126.0]):
            with patch.object(self.controller, "attempt", side_effect=TimeoutError):
                result = self.controller.tick(100)
        self.assertEqual(result["checked_at"], 225)
        self.assertEqual(result["retry_after"], 285)

    def test_concurrent_tick_does_not_start_second_owner(self):
        with open(self.config["lock"], "w") as lock:
            os.chmod(self.config["lock"], 0o600)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with patch.object(self.controller, "remote") as rpc:
                self.assertEqual(self.controller.tick(100)["state"], "already_running")
                rpc.assert_not_called()

    def test_ssh_is_fixed_pinned_noninteractive_without_shared_master(self):
        response = subprocess.CompletedProcess([], 0, json.dumps(status()).encode(), b"")
        with patch.object(self.controller, "run", return_value=response) as run:
            self.controller.status()
        args, frame = run.call_args.args
        self.assertEqual(frame, gateway.STATUS_FRAME)
        self.assertEqual(args[-1], "synthetic-vault")
        for option in ("BatchMode=yes", "StrictHostKeyChecking=yes", "ControlPath=none",
                       "ClearAllForwardings=yes", "PermitLocalCommand=no"):
            self.assertIn(option, args)

    def test_config_requires_private_state_and_no_ssh_options(self):
        path = self.root / "config.json"
        path.write_text(json.dumps(self.config))
        path.chmod(0o600)
        self.assertEqual(recovery.load_config(path), self.config)
        path.chmod(0o644)
        with self.assertRaises(ValueError):
            recovery.load_config(path)
        path.chmod(0o600)
        path.write_text(json.dumps({**self.config, "ssh_alias": "-oProxyCommand=evil"}))
        with self.assertRaises(ValueError):
            recovery.load_config(path)

    def test_state_aliases_rejected_and_old_tmp_inhibit_preserved(self):
        path = self.root / "config.json"
        path.write_text(json.dumps({**self.config, "inhibit": self.config["lock"]}))
        path.chmod(0o600)
        with self.assertRaisesRegex(ValueError, "overlapping"):
            recovery.load_config(path)
        self.config["inhibit"] = str(Path(self.config["receipt"]).with_suffix(".tmp"))
        Path(self.config["inhibit"]).touch()
        self.assertEqual(self.controller.tick(100)["state"], "inhibited")
        self.assertTrue(Path(self.config["inhibit"]).exists())


class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.image = self.root / "synthetic.luks"
        self.image.write_bytes(b"synthetic fixture")
        self.image.chmod(0o600)
        self.uuid = "11111111-1111-1111-1111-111111111111"
        self.config = {"image": str(self.image), "uuid": self.uuid,
                       "mapping": "synthetic-vault", "mount": str(self.root / "mount"),
                       "marker": str(self.root / "mount/ready"), "lock": str(self.root / "lock"),
                       "services": ["synthetic-core.service", "synthetic-tunnel.service"]}
        self.gateway = gateway.Gateway(self.config)

    def fake(self, *, mounted=False, image=None, uuid=None):
        def run(*args, key=None):
            code, output = 0, b""
            if args[:2] == ("cryptsetup", "luksUUID"):
                output = (uuid or self.uuid).encode()
            elif args[:2] == ("cryptsetup", "status"):
                output = f"  device: /dev/loop1\n  loop: {image or self.image}\n".encode()
            elif args[0] == "findmnt":
                code = 0 if mounted else 1
                output = b"/dev/mapper/synthetic-vault\n" if mounted else b""
            return subprocess.CompletedProcess(args, code, output, b"")
        return run

    def test_status_never_unlocks_or_starts_services(self):
        with patch.object(self.gateway, "run", side_effect=self.fake()) as run:
            result = self.gateway.status()
        self.assertFalse(result["mounted"])
        self.assertFalse(any(c.args[:2] == ("cryptsetup", "open") for c in run.call_args_list))
        self.assertFalse(any("start" in c.args for c in run.call_args_list))

    def test_foreign_mount_and_wrong_image_are_rejected(self):
        with patch.object(self.gateway, "run", side_effect=self.fake(mounted=True)):
            with self.assertRaisesRegex(ValueError, "foreign mount"):
                self.gateway.status()
        with patch.object(self.gateway, "run", side_effect=self.fake(uuid="wrong")):
            with self.assertRaisesRegex(ValueError, "wrong image"):
                self.gateway.status()

    def test_mapped_device_must_bind_exact_image_not_just_uuid(self):
        original = Path.exists
        def exists(path):
            return True if str(path) == "/dev/mapper/synthetic-vault" else original(path)
        with patch.object(Path, "exists", exists):
            with patch.object(self.gateway, "run", side_effect=self.fake(image="/foreign/image")):
                with self.assertRaisesRegex(ValueError, "wrong backing image"):
                    self.gateway.status()

    def test_exact_legacy_unlock_and_service_scope(self):
        with patch.object(self.gateway, "status", side_effect=[status(mounted=False), status()]):
            with patch.object(self.gateway, "checked", return_value=b"") as run:
                result = self.gateway.unlock(b"k" * 32)
        self.assertEqual(result, {"unlocked": True, "deployment_ready": True})
        self.assertEqual(run.call_args.args, ("systemctl", "start", *self.config["services"]))
        self.assertNotIn(b"k" * 32, run.call_args.args)

    def test_protocol_rejects_invalid_input_without_operations(self):
        for frame in (b"", b"x", b"x" * 33, b"status\n"):
            with patch.object(gateway, "Gateway") as cls:
                with self.assertRaises(ValueError):
                    gateway.serve(self.config, frame)
                cls.assert_not_called()
        with patch.object(gateway, "Gateway") as cls:
            cls.return_value.status.return_value = status()
            self.assertEqual(gateway.serve(self.config, gateway.STATUS_FRAME), status())
            cls.return_value.unlock.assert_not_called()

    def test_legacy_frame_retains_exact_32_bytes(self):
        key = bytes(range(32))
        with patch.object(gateway, "Gateway") as cls:
            gateway.serve(self.config, key)
            cls.return_value.unlock.assert_called_once_with(key)

    def test_automatic_frame_rechecks_mount_without_restarting_stopped_services(self):
        with patch.object(gateway, "Gateway") as cls:
            gateway.serve(self.config, b"R" + b"k" * 32)
            cls.return_value.unlock.assert_called_once_with(b"k" * 32, automatic=True)
        with patch.object(self.gateway, "status", return_value=status(active=False)):
            with patch.object(self.gateway, "checked") as run:
                self.gateway.unlock(b"k" * 32, automatic=True)
                run.assert_not_called()

    def test_gateway_lock_cannot_manufacture_deployment_marker(self):
        path = self.root / "config.json"
        path.write_text(json.dumps({**self.config, "marker": self.config["lock"]}))
        path.chmod(0o600)
        with self.assertRaisesRegex(ValueError, "overlapping"):
            gateway.load_config(path)

    def test_valid_config_and_marker_mount_boundary(self):
        path = self.root / "config.json"
        path.write_text(json.dumps(self.config))
        path.chmod(0o600)
        self.assertEqual(gateway.load_config(path), self.config)
        path.write_text(json.dumps({**self.config, "marker": str(self.root / "foreign")}))
        with self.assertRaisesRegex(ValueError, "mount boundary"):
            gateway.load_config(path)
        path.write_text(json.dumps({**self.config, "lock": str(self.root / "mount/lock")}))
        with self.assertRaisesRegex(ValueError, "mount boundary"):
            gateway.load_config(path)

    def test_deadline_kills_command_descendants_before_releasing_owner(self):
        marker = self.root / "should-not-appear"
        child = "import time,pathlib; time.sleep(.4); pathlib.Path(" + repr(str(marker))
        child += ").touch()"
        command = ("import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',"
                   + repr(child) + "]); time.sleep(5)")
        self.gateway.deadline = time.monotonic() + 0.15
        with self.assertRaises(subprocess.TimeoutExpired):
            self.gateway.run(sys.executable, "-c", command)
        time.sleep(0.5)
        self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()

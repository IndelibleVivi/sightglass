from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import zipfile
from contextlib import closing, redirect_stdout
from pathlib import Path
from unittest import mock

from sightglass.cli import main
from sightglass.runtime.config import ConfigStore, SightglassConfig
from sightglass.runtime.migration_state import (
    frozen_installation,
    relocate_received,
    stopped_installation,
    verify_relocated,
)
from sightglass.runtime.process_lock import acquire_runtime_lock, release_runtime_lock
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


class FrozenInstallationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        source = create_synthetic_source(self.root / "source")
        self.config = SightglassConfig.create(self.root / "old", source)
        self.config.data_dir.mkdir(mode=0o700)
        self.config.socket_path.parent.mkdir(mode=0o700, parents=True)
        _provider, repository, service, tools = build_test_stack(
            source,
            self.config.window_db_path,
            default_projection=None,
        )
        service.sync_source_once(initial_tail=200)
        self.repository, self.service = repository, service
        group = repository.conversation_id_for(
            repository.account_id_for("synthetic-account-demo"), "conv_group"
        )
        self.delivery = tools.wechat_read_messages(mode="updates", conversation_id=group)
        self.codec = service.token_codec
        secret = self.config.data_dir / "token-secret"
        secret.write_bytes(hashlib.sha256(b"synthetic-test-secret").digest())
        secret.chmod(0o600)

    def test_active_process_lock_blocks_freeze_before_any_checkpoint(self) -> None:
        lock = acquire_runtime_lock(self.config.socket_path.parent / "sightglassd.lock")
        try:
            with self.assertRaisesRegex(RuntimeError, "stop the full runtime"):
                with frozen_installation(self.config):
                    self.fail("active daemon namespace was frozen")
        finally:
            release_runtime_lock(lock)

    def test_linux_frozen_inventory_retains_only_the_declared_default_model_bundle(self) -> None:
        from tests.fixtures.linux_voice_helper import write_model_binding

        voice = self.config.data_dir / "voice"
        voice.mkdir(mode=0o700)
        helper = voice / "sightglass-whisper"
        helper.write_bytes(b"#!/bin/sh\n# Synthetic nonexecuted migration helper.\nexit 0\n")
        helper.chmod(0o700)
        model = write_model_binding(helper)
        unrelated = model.parent / "synthetic-unrelated-model.bin"
        unrelated.write_bytes(b"synthetic excluded model")
        unrelated.chmod(0o600)
        with mock.patch("sightglass.runtime.voice_setup._is_linux", return_value=True):
            with frozen_installation(self.config) as plan:
                names = {item.relative for item in plan.files}
        self.assertIn("voice/sightglass-whisper", names)
        self.assertIn("voice/models/model.json", names)
        self.assertIn("voice/models/ggml-small-q5_1.bin", names)
        self.assertNotIn("voice/models/synthetic-unrelated-model.bin", names)

    def test_exact_owned_inventory_relocation_and_pending_replay(self) -> None:
        unrelated = self.config.data_dir / "private-unrelated-export.json"
        unrelated.write_text('{"synthetic":"must not migrate"}')
        unrelated.chmod(0o600)
        with frozen_installation(self.config) as plan:
            paths = {item.relative for item in plan.files}
            self.assertIn("window.db", paths)
            self.assertIn("token-secret", paths)
            self.assertIn(f"deliveries/{self.delivery['page']['delivery_id']}.json", paths)
            self.assertNotIn(unrelated.name, paths)
            recovery = self.root / "recovery"
            candidate = self.root / "candidate"
            for target in (recovery, candidate):
                target.mkdir(mode=0o700)
                for member in paths:
                    destination = target / member
                    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    shutil.copyfile(self.config.data_dir / member, destination)
                    destination.chmod(0o600)
            relocate_received(
                candidate / "window.db", old_namespace=self.config.data_dir, new_namespace=candidate
            )
            receipt = verify_relocated(
                recovery / "window.db",
                candidate / "window.db",
                old_namespace=self.config.data_dir,
                new_namespace=candidate,
            )
        self.assertTrue(receipt["verified"])
        self.assertEqual(
            (candidate / "token-secret").read_bytes(),
            (self.config.data_dir / "token-secret").read_bytes(),
        )
        with closing(sqlite3.connect(candidate / "window.db")) as connection:
            identity, reference, digest, status = connection.execute(
                "SELECT delivery_id,payload_ref,payload_digest,status FROM reader_deliveries"
            ).fetchone()
        self.assertEqual(status, "pending")
        self.assertEqual(identity, self.delivery["page"]["delivery_id"])
        self.assertEqual(
            Path(reference).read_bytes(),
            (self.config.data_dir / "deliveries" / f"{identity}.json").read_bytes(),
        )
        self.assertEqual(hashlib.sha256(Path(reference).read_bytes()).hexdigest(), digest)
        with closing(sqlite3.connect(candidate / "window.db")) as connection:
            connection.execute("UPDATE messages SET text='synthetic corruption' WHERE rowid=1")
            connection.commit()
        with self.assertRaisesRegex(RuntimeError, "outside owned path"):
            verify_relocated(
                recovery / "window.db",
                candidate / "window.db",
                old_namespace=self.config.data_dir,
                new_namespace=candidate,
            )

    def test_reference_escape_and_missing_pending_fail_closed(self) -> None:
        with stopped_installation(self.config):
            with closing(sqlite3.connect(self.config.window_db_path)) as connection:
                connection.execute("UPDATE reader_deliveries SET payload_ref='/synthetic/outside'")
                connection.commit()
        with self.assertRaisesRegex(RuntimeError, "outside its canonical"):
            with frozen_installation(self.config):
                self.fail("escaped spool reference was exported")

    def test_operator_cli_streams_to_real_receiver_under_stopped_lock(self) -> None:
        store = ConfigStore(self.root / "config.json")
        store.save(self.config)
        identity = self.root / "synthetic-ssh-key"
        identity.write_bytes(b"synthetic-key-not-used-for-network")
        identity.chmod(0o600)
        target = self.root / "destination with spaces"
        output = self.root / "transfer-receipt.json"
        real_popen = subprocess.Popen
        seen = []

        def local_receiver(argv, **kwargs):
            seen.append(argv)
            with self.assertRaisesRegex(RuntimeError, "stop the full runtime"):
                with stopped_installation(self.config):
                    self.fail("sender released the stopped lock")
            command = shlex.split(argv[-1])
            self.assertEqual(
                command,
                [
                    sys.executable,
                    "-m",
                    "sightglass.runtime.migration",
                    "--destination",
                    str(target),
                ],
            )
            self.assertIn("StrictHostKeyChecking=yes", argv)
            return real_popen(command, **kwargs)

        stdout = io.StringIO()
        with (
            mock.patch(
                "sightglass.runtime.migration_state.subprocess.Popen", side_effect=local_receiver
            ),
            redirect_stdout(stdout),
        ):
            main(
                [
                    "--config",
                    str(store.path),
                    "migration",
                    "send",
                    "--host",
                    "synthetic-host",
                    "--identity-file",
                    str(identity),
                    "--remote-python",
                    sys.executable,
                    "--destination",
                    str(target),
                    "--output",
                    str(output),
                ]
            )
        self.assertEqual(len(seen), 1)
        receipt = json.loads(output.read_bytes())
        self.assertTrue(receipt["verified"])
        self.assertFalse(receipt["activated"])
        self.assertEqual(output.stat().st_mode & 0o777, 0o600)
        for member in receipt["manifest"]["files"]:
            self.assertEqual(
                (target / member["relative"]).read_bytes(),
                (self.config.data_dir / member["relative"]).read_bytes(),
            )
        public = json.loads(stdout.getvalue())
        self.assertNotIn("manifest", public)
        self.assertTrue(public["verified"])


class InstalledWheelPayloadTests(unittest.TestCase):
    """Cheap rejection tests; actual installation is the shared CI script gate."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        script = Path(__file__).resolve().parents[2] / "scripts/check-installed-wheel.py"
        specification = importlib.util.spec_from_file_location("installed_wheel_gate", script)
        assert specification is not None and specification.loader is not None
        self.gate = importlib.util.module_from_spec(specification)
        specification.loader.exec_module(self.gate)
        (self.root / "src/sightglass").mkdir(parents=True)
        (self.root / "src/sightglass/__init__.py").write_text("# synthetic package\n")
        (self.root / "LICENSE").write_text("synthetic license bytes\n")
        self.scripts = {name: "sightglass:main" for name in self.gate.ENTRYPOINTS}
        (self.root / "pyproject.toml").write_text(
            '[project]\nname="sightglass"\nversion="0.0.0"\nlicense="AGPL-3.0-only"\n'
            'license-files=["LICENSE"]\n[project.scripts]\n'
            + "\n".join(f'{name}="{value}"' for name, value in sorted(self.scripts.items()))
        )
        self.payload = {
            "sightglass/__init__.py": b"# synthetic package\n",
            "sightglass-0.0.0.dist-info/METADATA": (
                b"Name: sightglass\nVersion: 0.0.0\nLicense-Expression: AGPL-3.0-only\n"
                b"License-File: LICENSE\n\n"
            ),
            "sightglass-0.0.0.dist-info/licenses/LICENSE": b"synthetic license bytes\n",
            "sightglass-0.0.0.dist-info/entry_points.txt": (
                "[console_scripts]\n"
                + "\n".join(f"{name} = {value}" for name, value in sorted(self.scripts.items()))
            ).encode(),
        }

    def write_wheel(self):
        wheel = self.root / "synthetic.whl"
        with zipfile.ZipFile(wheel, "w") as archive:
            for name, body in self.payload.items():
                archive.writestr(name, body)
        return wheel

    def test_matching_payload_and_artifact_identity(self):
        wheel = self.write_wheel()
        result = self.gate.validate_wheel(wheel, self.root)
        self.assertEqual(result["package_source_files"], 1)
        self.assertEqual(set(result["entrypoints"]), self.gate.ENTRYPOINTS)
        self.assertEqual(result["wheel_sha256"],
                         hashlib.sha256(wheel.read_bytes()).hexdigest())

    def test_missing_or_stale_source_and_extra_payload_fail(self):
        original = dict(self.payload)
        for change in ("missing", "stale", "extra", "metadata-extra", "license", "entrypoint"):
            with self.subTest(change=change):
                self.payload = dict(original)
                if change == "missing":
                    del self.payload["sightglass/__init__.py"]
                elif change == "stale":
                    self.payload["sightglass/__init__.py"] = b"# stale\n"
                elif change == "extra":
                    self.payload["sightglass/private-runtime.json"] = b"{}"
                elif change == "metadata-extra":
                    self.payload["sightglass-0.0.0.dist-info/private-runtime.json"] = b"{}"
                elif change == "license":
                    self.payload["sightglass-0.0.0.dist-info/licenses/LICENSE"] = b"wrong license"
                else:
                    self.payload["sightglass-0.0.0.dist-info/entry_points.txt"] = (
                        b"[console_scripts]\nsightglassctl = sightglass:main\n"
                    )
                with self.assertRaises(RuntimeError):
                    self.gate.validate_wheel(self.write_wheel(), self.root)

    def test_python_and_operator_overrides_do_not_reach_probe(self):
        with mock.patch.dict(os.environ, {
            "PYTHONPATH": "synthetic-checkout", "PYTHONHOME": "synthetic-home",
            "VIRTUAL_ENV": "synthetic-dev", "SIGHTGLASS_CONFIG": "synthetic-operator",
        }):
            env = self.gate.clean_environment()
        for name in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV", "SIGHTGLASS_CONFIG"):
            self.assertNotIn(name, env)


if __name__ == "__main__":
    unittest.main()

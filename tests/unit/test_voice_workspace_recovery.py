"""Synthetic recovery checks: no real systemd, codec, account or model."""

from __future__ import annotations

import errno
import os
import shlex
import signal
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from sightglass.contracts.errors import SightglassError
from sightglass.storage import MIB, StorageBudget, StorageSettings
from sightglass.voice import workspace
from sightglass.voice.linux import DirectRunner, LinuxWhisperHelper


class VoiceWorkspaceRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.name = "sightglass-whisper-" + "d" * 32

    def test_setup_faults_close_every_opened_fd_and_remove_partial_marker(self):
        for operation in ("open", "flock", "write", "fsync"):
            for code in (errno.ENOSPC, errno.EMFILE):
                with self.subTest(operation=operation, errno=code):
                    opened = set()
                    real_open, real_close = os.open, os.close

                    def tracked_open(*args, **kwargs):
                        if operation == "open" and Path(args[0]).name == "lease":
                            raise OSError(code, "synthetic lease fault")
                        fd = real_open(*args, **kwargs)
                        opened.add(fd)
                        return fd

                    def tracked_close(fd):
                        opened.discard(fd)
                        return real_close(fd)

                    module = "fcntl" if operation == "flock" else "os"
                    original = getattr(workspace.fcntl if module == "fcntl" else os, operation)

                    def fail(*args, **kwargs):
                        # Root locking must still work; fail only the lease lock.
                        if operation == "flock" and stat.S_ISDIR(os.fstat(args[0]).st_mode):
                            return original(*args, **kwargs)
                        raise OSError(code, "synthetic lease fault")

                    with (
                        patch.object(workspace.os, "open", side_effect=tracked_open),
                        patch.object(workspace.os, "close", side_effect=tracked_close),
                    ):
                        if operation == "open":
                            with self.assertRaises(OSError):
                                with workspace.attempt(
                                    self.root / "linux-jobs", self.name, manager=None
                                ):
                                    self.fail("launched")
                        else:
                            with patch(
                                f"sightglass.voice.workspace.{module}.{operation}", side_effect=fail
                            ):
                                with self.assertRaises(OSError):
                                    with workspace.attempt(
                                        self.root / "linux-jobs", self.name, manager=None
                                    ):
                                        self.fail("launched")
                    self.assertEqual(opened, set())
                    self.assertEqual(list((self.root / "linux-jobs").iterdir()), [])

    def test_short_marker_write_is_not_a_replayable_job(self):
        with patch.object(workspace.os, "write", return_value=1):
            with self.assertRaises(OSError):
                with workspace.attempt(self.root / "linux-jobs", self.name, manager=None):
                    self.fail("launched")
        self.assertEqual(list((self.root / "linux-jobs").iterdir()), [])

    def test_readable_owned_output_reaps_only_after_live_lease_released(self):
        root = self.root / "linux-jobs"
        with workspace.attempt(root, self.name, manager="systemctl") as (path, _):
            output = path / "output.json"
            output.write_bytes(b"synthetic residual")
            output.chmod(0o644)
            stop = Mock()  # Even an inactive scope cannot bypass a locked live lease.
            self.assertEqual(workspace.reap(root, manager="systemctl", stop=stop), 0)
            stop.assert_not_called()
        stop.side_effect = RuntimeError("manager unavailable")
        with self.assertRaises(RuntimeError):
            workspace.reap(root, manager="systemctl", stop=stop)
        self.assertTrue(output.exists())
        budget = StorageBudget(
            self.root, self.root / "window.db", StorageSettings(8 * MIB, 16 * MIB, 0, MIB)
        )
        helper = LinuxWhisperHelper(
            self.root / "unused", language="en", runner=DirectRunner(), storage=budget
        )
        # Same manager is required: helper accounting must retain failed cleanup bytes.
        helper.runner = Mock(spec=DirectRunner)
        with self.assertRaises(SightglassError):
            helper.reap_workspaces(self.root)
        self.assertIn(output, workspace.inventory(root))
        stop.side_effect = None
        self.assertEqual(workspace.reap(root, manager="systemctl", stop=stop), 1)

    def test_budget_keeps_failed_cleanup_residual_then_releases_it(self):
        root = self.root / "linux-jobs"
        budget = StorageBudget(
            self.root, self.root / "window.db", StorageSettings(8 * MIB, 16 * MIB, 0, MIB)
        )
        helper = LinuxWhisperHelper(
            self.root / "unused", language="en", runner=DirectRunner(), storage=budget
        )
        with workspace.attempt(root, self.name, manager=None) as (path, _):
            output = path / "output.json"
            output.write_bytes(b"synthetic residual")
            output.chmod(0o644)
        with patch.object(workspace, "_remove", side_effect=OSError(errno.ENOSPC, "synthetic")):
            with self.assertRaises(SightglassError):
                helper.reap_workspaces(self.root)
        retained = budget.status()["accounted_bytes"]
        self.assertGreaterEqual(retained, len(b"synthetic residual"))
        self.assertEqual(helper.reap_workspaces(self.root), 1)
        self.assertEqual(budget.status()["accounted_bytes"], 0)

    def test_foreign_owner_and_external_alias_still_block_readable_output(self):
        root = self.root / "linux-jobs"
        with workspace.attempt(root, self.name, manager=None) as (path, _):
            output = path / "output.json"
            output.write_bytes(b"synthetic residual")
            output.chmod(0o644)
        with patch.object(workspace.os, "getuid", return_value=os.getuid() + 1):
            with self.assertRaises(RuntimeError):
                workspace.reap(root, manager=None, stop=None)
        external = self.root / "external"
        os.link(output, external)
        with self.assertRaises(RuntimeError):
            workspace.reap(root, manager=None, stop=None)
        self.assertTrue(output.exists())
        external.unlink()
        workspace.reap(root, manager=None, stop=None)

    def test_generated_wrapper_sigkill_twice_does_not_poison_next_attempt(self):
        cli = self.root / "synthetic-whisper"
        cli.write_text(
            "#!/usr/bin/env python3\nimport json,os,signal,sys\n"
            "if '--version' in sys.argv: print('whisper synthetic 1.9.5'); sys.exit(0)\n"
            "with open(os.path.join(os.path.dirname(__file__),'calls'),'a') as f:\n"
            "    f.write('called\\n')\n"
            "output=sys.argv[sys.argv.index('--output-file')+1]+'.json'\n"
            "with open(output,'w') as f: json.dump({'transcription':[]},f)\n"
            "assert os.stat(output).st_mode & 0o077 == 0\n"
            "os.kill(os.getppid(),signal.SIGKILL)\n"
        )
        cli.chmod(0o700)
        model = self.root / "ggml-synthetic-q5_1.bin"
        model.write_bytes(b"synthetic, never loaded")
        model.chmod(0o600)
        helper_path = self.root / "helper" / "sightglass-whisper"
        build = subprocess.run(
            [
                "bash",
                str(Path(__file__).resolve().parents[2] / "scripts/build-linux-voice-helper.sh"),
                "--whisper-cli",
                str(cli),
                "--model",
                str(model),
                "--source-tag",
                "synthetic",
                "--output",
                str(helper_path),
            ],
            capture_output=True,
            timeout=20,
        )
        self.assertEqual(build.returncode, 0, build.stderr)
        standalone = self.root / "standalone"
        standalone.mkdir(mode=0o700)
        wav = self.root / "synthetic.wav"
        wav.write_bytes(b"synthetic input, recognizer stub never decodes")
        wrapper = subprocess.run(
            [str(helper_path), "--pcm", str(wav)],
            umask=0o022,
            env={**os.environ, "TMPDIR": str(standalone)},
            capture_output=True,
            timeout=10,
        )
        self.assertEqual(wrapper.returncode, -signal.SIGKILL, wrapper.stderr)
        outputs = list(standalone.glob("*/out.json"))
        self.assertEqual(len(outputs), 1)
        self.assertEqual(stat.S_IMODE(outputs[0].stat().st_mode), 0o600)
        code = """import os,sys,signal,subprocess
from sightglass.voice import _linux_child as child
os.umask(0o022)
def decode(data, *, sample_rate, max_pcm_bytes, directory):
    path=directory+'/decoded.pcm'
    with open(path,'wb') as f: f.write(bytes(320))
    assert os.stat(path).st_mode & 0o077 == 0
    return path,320,160,'synthetic'
# macOS DirectRunner seam: Linux ulimit/systemd is separately validated.
# Keep the real child workspace/WAV and generated wrapper execution.
def recognize(arguments, wav, directory):
    result=subprocess.run([arguments.helper,'--pcm',wav,'--language',arguments.language])
    assert result.returncode == -signal.SIGKILL
    os.kill(os.getpid(),signal.SIGKILL)
child._decode_to_pcm=decode
child._run_whisper=recognize
child.main(sys.argv[3:])
"""
        driver = self.root / "driver"
        driver.write_text(
            "#!/bin/sh\nexec "
            + shlex.quote(sys.executable)
            + " -c "
            + shlex.quote(code)
            + ' "$@"\n'
        )
        driver.chmod(0o700)
        silk = self.root / "input.silk"
        silk.write_bytes(b"synthetic silk")
        silk.chmod(0o600)
        helper = LinuxWhisperHelper(
            helper_path, language="en", runner=DirectRunner(), python_executable=str(driver)
        )
        for _ in range(2):
            with self.assertRaises(SightglassError) as caught:
                helper.transcribe_silk(silk)
            self.assertNotEqual(
                caught.exception.details.get("reason"), "helper_workspace_cleanup_failed"
            )
            self.assertEqual(list((self.root / "linux-jobs").iterdir()), [])
        self.assertEqual((self.root / "calls").read_text().splitlines(), ["called"] * 3)

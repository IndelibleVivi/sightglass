"""Generated crash/concurrency and parser boundary regression evidence."""

from __future__ import annotations

import concurrent.futures
import io
import multiprocessing
import os
import sys
import tempfile
import unittest
import wave
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.resources.cache import ResourceObjectStore
from sightglass.resources.rich import DOCX_MIME, _html_details, _xml_root, extract_rich_text
from sightglass.runtime.voice_setup import build_voice_setup
from sightglass.storage import MIB, StorageBudget, StorageSettings
from sightglass.voice import workspace
from sightglass.voice.linux import DirectRunner, LinuxWhisperHelper
from tests.fixtures.linux_voice_helper import write_model_binding, write_stub_helper
from tests.unit.test_rich_resources import _docx, _zip


def _crash_publish(root: str) -> None:
    store = ResourceObjectStore(Path(root) / "window.db")
    rename = os.rename
    link = os.link

    def crash(source, target):
        rename(source, target)
        os._exit(0)

    def crash_link(source, target, **kwargs):
        link(source, target, **kwargs)
        os._exit(0)

    with (
        mock.patch("sightglass.resources.cache.os.rename", side_effect=crash),
        mock.patch("sightglass.resources.cache.os.link", side_effect=crash_link),
    ):
        store.put(b"synthetic object", mime_type="text/plain", origin="synthetic")


def _put(root: str) -> None:
    ResourceObjectStore(Path(root) / "window.db").put(
        b"synthetic object", mime_type="text/plain", origin="synthetic"
    )


def _crash_workspace(root: str) -> None:
    with workspace.attempt(Path(root), "sightglass-whisper-" + "c" * 32, manager="systemctl") as (
        path,
        _,
    ):
        with wave.open(str(path / "input.wav"), "wb") as output:
            output.setnchannels(1)
            output.setsampwidth(2)
            output.setframerate(16000)
            output.writeframes(bytes(320))
        (path / "input.wav").chmod(0o600)
        os._exit(17)


class ResourceReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = ResourceObjectStore(self.root / "window.db")

    def test_publish_crash_restart_and_concurrent_instances(self):
        process = multiprocessing.get_context("fork").Process(
            target=_crash_publish, args=(str(self.root),)
        )
        process.start()
        process.join(10)
        self.assertEqual(process.exitcode, 0)
        digest = self.store.digest(b"synthetic object")
        self.assertEqual((self.store.objects / digest).stat().st_nlink, 1)
        self.assertEqual(list(self.store.tmp.iterdir()), [])
        _put(str(self.root))
        # Exercise actual simultaneous publication rather than cache hits.
        (self.store.objects / digest).unlink()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(_put, [str(self.root)] * 32))
        (self.store.objects / digest).unlink()
        processes = [
            multiprocessing.get_context("fork").Process(target=_put, args=(str(self.root),))
            for _ in range(3)
        ]
        for process in processes:
            process.start()
        for process in processes:
            process.join(10)
            self.assertEqual(process.exitcode, 0)
        self.assertEqual((self.store.objects / digest).stat().st_nlink, 1)
        self.assertEqual(list(self.store.tmp.iterdir()), [])

    def test_publish_syncs_file_then_both_rename_directories_before_read(self):
        # Observe real syscalls and their ordering; this is not a power-loss test.
        events: list[str] = []
        identities = {
            (path.stat().st_dev, path.stat().st_ino): name
            for name, path in (("objects", self.store.objects), ("tmp", self.store.tmp))
        }
        real_fsync, real_rename, real_read = os.fsync, os.rename, self.store.read_path

        def sync(fd):
            metadata = os.fstat(fd)
            events.append(identities.get((metadata.st_dev, metadata.st_ino), "file"))
            real_fsync(fd)

        def rename(source, destination):
            self.assertEqual(events, ["file"])
            real_rename(source, destination)
            self.assertEqual(Path(destination).stat().st_nlink, 1)
            events.append("rename")

        def read(*args, **kwargs):
            events.append("read")
            return real_read(*args, **kwargs)

        with (
            mock.patch("sightglass.resources.cache.os.fsync", side_effect=sync),
            mock.patch("sightglass.resources.cache.os.rename", side_effect=rename),
            mock.patch.object(self.store, "read_path", side_effect=read),
        ):
            self.store.put(b"synthetic durability", mime_type="text/plain", origin="synthetic")
        self.assertEqual(events, ["file", "rename", "objects", "tmp", "read"])

    def test_directory_sync_failure_does_not_report_success_or_leave_alias(self):
        for failed_directory in (self.store.objects, self.store.tmp):
            with self.subTest(directory=failed_directory.name):
                data = ("synthetic failure " + failed_directory.name).encode()
                expected = failed_directory.stat()
                real_fsync = os.fsync
                synced: list[tuple[int, int]] = []

                def sync(fd):
                    metadata = os.fstat(fd)
                    identity = (metadata.st_dev, metadata.st_ino)
                    synced.append(identity)
                    if identity == (expected.st_dev, expected.st_ino):
                        raise OSError("synthetic directory fsync failure")
                    real_fsync(fd)

                with (
                    mock.patch("sightglass.resources.cache.os.fsync", side_effect=sync),
                    mock.patch.object(self.store, "read_path") as read,
                ):
                    with self.assertRaisesRegex(OSError, "synthetic directory fsync failure"):
                        self.store.put(data, mime_type="text/plain", origin="synthetic")
                read.assert_not_called()
                objects = self.store.objects.stat()
                self.assertIn((objects.st_dev, objects.st_ino), synced)
                self.assertEqual((self.store.objects / self.store.digest(data)).stat().st_nlink, 1)
                self.assertEqual(list(self.store.tmp.iterdir()), [])

    def test_aliases_and_corruption_remain_rejected(self):
        digest = self.store.digest(b"synthetic object")
        dest = self.store.objects / digest
        external = self.root / "outside"
        external.write_bytes(b"synthetic object")
        external.chmod(0o600)
        for alias in (lambda: os.link(external, dest), lambda: dest.symlink_to(external)):
            alias()
            with self.assertRaises(SightglassError):
                _put(str(self.root))
            self.assertEqual(external.read_bytes(), b"synthetic object")
            dest.unlink()
        _put(str(self.root))
        dest.write_bytes(b"corrupted object")
        with self.assertRaises(SightglassError) as caught:
            _put(str(self.root))
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)

    def test_xml_encoding_independent_dtd_and_early_budgets(self):
        for encoding in ("utf-8", "utf-16-le", "utf-16-be"):
            declaration = (
                '<?xml version="1.0" encoding="'
                + ("UTF-8" if encoding == "utf-8" else "UTF-16")
                + '"?>'
            )
            for content in (
                '<!DOCTYPE x [<!ENTITY e "EXPANSION">]><x>&e;</x>',
                '<!DOCTYPE x SYSTEM "file:///synthetic-never-read"><x/>',
            ):
                with self.subTest(encoding=encoding, content=content):
                    with self.assertRaises(SightglassError) as caught:
                        _xml_root((declaration + content).encode(encoding))
                    self.assertEqual(caught.exception.details["reason"], "xml_dtd_or_entity")
            root, nodes, depth = _xml_root((declaration + "<x><v>合成</v></x>").encode(encoding))
            self.assertEqual((root[0].text, nodes, depth), ("合成", 2, 2))
        # Truncated malformed tails prove budgets reject before the complete tree exists.
        for data, setting, reason in (
            (b"<x><a/><b/>", "MAX_XML_NODES", "xml_nodes"),
            (b"<x><a><b>", "MAX_XML_DEPTH", "xml_depth"),
        ):
            with mock.patch("sightglass.resources.rich." + setting, 2):
                with self.assertRaises(SightglassError) as caught:
                    _xml_root(data)
                self.assertEqual(caught.exception.details["reason"], reason)

    def test_html_void_nodes_do_not_accumulate_depth(self):
        for tag in ("br", "img", "meta", "input"):
            for suffix in (">", "/>"):
                text, count = _html_details(
                    (
                        "<html><body>visible"
                        + ("<" + tag + suffix) * 80
                        + "<script>hidden</script><style>hidden</style>end</body></html>"
                    ).encode()
                )
                self.assertEqual(text, "visible\nend")
                self.assertEqual(count, 84)
        with self.assertRaises(SightglassError) as caught:
            _html_details(b"<div>" * 65)
        self.assertEqual(caught.exception.details["reason"], "html_depth")

    def test_workspace_live_lease_restart_and_unsafe_files(self):
        root = self.root / "linux-jobs"
        name = "sightglass-whisper-" + "a" * 32
        with workspace.attempt(root, name, manager=None) as (path, lease):
            self.assertEqual(workspace.reap(root, manager=None, stop=None), 0)
            workspace.validate_child(path, lease)
            wav = path / "input.wav"
            wav.write_bytes(b"synthetic wav")
            wav.chmod(0o600)
        self.assertEqual(workspace.reap(root, manager=None, stop=None), 1)
        self.assertFalse(path.exists())
        for kind in ("hardlink", "symlink"):
            with workspace.attempt(root, name, manager=None) as (path, _):
                external = self.root / "outside"
                external.write_bytes(b"synthetic protected")
                external.chmod(0o600)
                if kind == "hardlink":
                    os.link(external, path / "input.wav")
                else:
                    (path / "input.wav").symlink_to(external)
            with self.assertRaises((RuntimeError, OSError)):
                workspace.reap(root, manager=None, stop=None)
            self.assertTrue(path.exists())
            self.assertEqual(external.read_bytes(), b"synthetic protected")
            (path / "input.wav").unlink()
            workspace.reap(root, manager=None, stop=None)

    def test_actual_child_wav_sigkill_is_cleaned_by_parent(self):
        helper_path = write_stub_helper(self.root, "pass\n")
        write_model_binding(helper_path)
        # Exercise the actual child main and WAV writer; only codec and recognizer
        # are synthetic seams, no model/source/runtime account is accessed.
        driver = self.root / "child-driver"
        driver.write_text(
            '''#!/bin/sh
exec "'''
            + sys.executable
            + """" -c '
import os, signal, sys
from sightglass.voice import _linux_child as child
def decode(data, *, sample_rate, max_pcm_bytes, directory):
    path = directory + "/decoded.pcm"
    with open(path, "wb") as out: out.write(bytes(320))
    os.chmod(path, 0o600)
    return path, 320, 160, "synthetic"
def recognize(arguments, wav, directory):
    assert open(wav, "rb").read(4) == b"RIFF"
    marker = os.path.join(os.path.dirname(arguments.helper), "child-wav-ready")
    with open(marker, "w") as out: out.write("RIFF")
    os.kill(os.getpid(), signal.SIGKILL)
child._decode_to_pcm = decode
child._run_whisper = recognize
child.main(sys.argv[3:])
' "$@"
"""
        )
        driver.chmod(0o700)
        silk = self.root / "input.silk"
        silk.write_bytes(b"synthetic silk")
        silk.chmod(0o600)
        helper = LinuxWhisperHelper(
            helper_path, language="en", runner=DirectRunner(), python_executable=str(driver)
        )
        with self.assertRaises(SightglassError):
            helper.transcribe_silk(silk)
        self.assertEqual((self.root / "child-wav-ready").read_text(), "RIFF")
        self.assertEqual(list((self.root / "linux-jobs").iterdir()), [])

    def test_workspace_reaper_requires_scope_death_and_exact_manager(self):
        root = self.root / "linux-jobs"
        name = "sightglass-whisper-" + "b" * 32
        with workspace.attempt(root, name, manager="systemctl") as (path, _):
            stop = mock.Mock()
            self.assertEqual(workspace.reap(root, manager="systemctl", stop=stop), 0)
            stop.assert_not_called()
        stop.side_effect = RuntimeError("scope alive")
        with self.assertRaises(RuntimeError):
            workspace.reap(root, manager="systemctl", stop=stop)
        self.assertTrue(path.exists())
        stop.assert_called_once_with(name + ".scope")
        with self.assertRaises(RuntimeError):
            workspace.reap(root, manager="other-manager", stop=stop)
        stop.side_effect = None
        self.assertEqual(workspace.reap(root, manager="systemctl", stop=stop), 1)

    def test_startup_reaps_killed_parent_even_with_voice_disabled(self):
        staging = self.root / "voice-work"
        staging.mkdir(mode=0o700)
        root = staging / "linux-jobs"
        process = multiprocessing.get_context("fork").Process(
            target=_crash_workspace, args=(str(root),)
        )
        process.start()
        process.join(10)
        self.assertEqual(process.exitcode, 17)
        config = SimpleNamespace(data_dir=self.root, voice_enabled=False)
        service = SimpleNamespace(
            repository=SimpleNamespace(database=SimpleNamespace(storage=None))
        )
        with (
            mock.patch("sightglass.runtime.voice_setup._is_linux", return_value=True),
            mock.patch("sightglass.voice.linux.SystemdRunner._cleanup_unit") as stop,
        ):
            setup = build_voice_setup(config, service)
        self.assertEqual(setup.blocked_reason, "voice_disabled")
        stop.assert_called_once_with("sightglass-whisper-" + "c" * 32 + ".scope")
        self.assertEqual(list(root.iterdir()), [])

    def test_office_xml_encodings(self):
        for encoding in ("utf-8", "utf-16-le", "utf-16-be"):
            with zipfile.ZipFile(io.BytesIO(_docx())) as archive:
                entries: list[tuple[str | zipfile.ZipInfo, bytes]] = [
                    (name, archive.read(name).decode().encode(encoding))
                    for name in archive.namelist()
                ]
            result = extract_rich_text(_zip(entries), DOCX_MIME, max_chars=10000)
            self.assertIn("alpha", str(result))
            malicious = _zip(
                [
                    (
                        name,
                        ('<!DOCTYPE x [<!ENTITY e "boom">]><x>&e;</x>').encode(encoding)
                        if name == "word/document.xml"
                        else data,
                    )
                    for name, data in entries
                ]
            )
            with self.assertRaises(SightglassError) as caught:
                extract_rich_text(malicious, DOCX_MIME, max_chars=10000)
            self.assertEqual(caught.exception.details["reason"], "xml_dtd_or_entity")

    def test_cas_capacity_failure_and_cache_hit_under_pressure(self):
        budget = StorageBudget(self.root, self.root / "window.db", StorageSettings(1, 2, 0, MIB))
        store = ResourceObjectStore(self.root / "window.db", storage=budget)
        with self.assertRaises(SightglassError) as caught:
            store.put(b"generated", mime_type="text/plain", origin="synthetic")
        self.assertEqual(caught.exception.code, ErrorCode.STORAGE_PRESSURE)
        self.assertEqual(list(store.tmp.iterdir()), [])
        budget.settings = StorageSettings(16 * MIB, 32 * MIB, 0, MIB)
        value, _ = store.put(b"generated", mime_type="text/plain", origin="synthetic")
        budget.settings = StorageSettings(1, 2, 0, MIB)
        self.assertEqual(
            store.put(b"generated", mime_type="text/plain", origin="synthetic")[0], value
        )


    def test_workspace_rejects_symlinked_staging_and_root(self):
        outside = self.root / "outside"
        outside.mkdir(mode=0o700)
        staging = self.root / "staging"
        staging.symlink_to(outside, target_is_directory=True)
        with self.assertRaises(RuntimeError):
            workspace.reap(staging / "linux-jobs", manager=None, stop=None)
        staging.unlink()
        staging.mkdir(mode=0o700)
        root = staging / "linux-jobs"
        root.symlink_to(outside, target_is_directory=True)
        with self.assertRaises(RuntimeError):
            workspace.reap(root, manager=None, stop=None)
        self.assertEqual(list(outside.iterdir()), [])

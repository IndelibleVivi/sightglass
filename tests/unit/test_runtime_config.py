from __future__ import annotations

import json
import os
import tempfile
import unittest
from argparse import Namespace
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from sightglass.cli import _timezone_command
from sightglass.runtime.config import (
    CONFIG_SCHEMA,
    CONFIG_SCHEMA_V1,
    ConfigStore,
    SightglassConfig,
)


class RuntimeConfigMigrationTests(unittest.TestCase):
    def test_reader_timezone_round_trip_and_legacy_default(self) -> None:
        config = SightglassConfig.create("/tmp/synthetic-state", "/tmp/synthetic-source")
        self.assertEqual((config.reader_id, config.reader_display_name), ("reader", "Reader"))
        value = replace(config, reader_timezone="America/New_York").as_dict()
        self.assertEqual(SightglassConfig.from_dict(value).reader_timezone, "America/New_York")
        del value["reader"]["timezone"]
        self.assertEqual(SightglassConfig.from_dict(value).reader_timezone, "Asia/Singapore")
        value["reader"]["timezone"] = "Synthetic/Invalid"
        with self.assertRaises(ValueError):
            SightglassConfig.from_dict(value)

    def test_operator_timezone_changes_only_stopped_config(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            store = ConfigStore(root / "state" / "config.json")
            config = SightglassConfig.create(root / "state", root / "source")
            store.save(config)
            args = Namespace(timezone_command="set", iana_timezone="Asia/Shanghai")
            result = _timezone_command(args, store)
            self.assertEqual(result["timezone"], "Asia/Shanghai")
            self.assertEqual(store.load().reader_timezone, "Asia/Shanghai")
            args.iana_timezone = "America/New_York"
            with patch(
                "sightglass.cli._daemon_must_be_stopped", side_effect=RuntimeError("running")
            ):
                with self.assertRaisesRegex(RuntimeError, "running"):
                    _timezone_command(args, store)
            self.assertEqual(store.load().reader_timezone, "Asia/Shanghai")

    def test_v1_synthetic_config_is_backed_up_and_migrated_to_v2(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data = root / "state"
            source = root / "source"
            run = data / "run"
            data.mkdir(mode=0o700)
            source.mkdir(mode=0o700)
            run.mkdir(mode=0o700)
            (source / "source.json").write_text("{}", encoding="utf-8")
            path = data / "config.json"
            value = {
                "schema": CONFIG_SCHEMA_V1,
                "paths": {
                    "data_dir": str(data),
                    "source_root": str(source),
                    "window_db": str(data / "window.db"),
                    "socket": str(run / "sightglassd.sock"),
                },
                "source_kind": "synthetic",
                "reader": {"reader_id": "demo_reader", "display_name": "Demo Reader"},
                "policy": {
                    "mode": "all_except_denylist",
                    "allowed": [],
                    "denied": [],
                },
                "paused": False,
                "auth": {
                    "reader_token_hash": "reader",
                    "operator_token_hash": "operator",
                },
            }
            path.write_text(json.dumps(value), encoding="utf-8")
            os.chmod(path, 0o600)

            config = ConfigStore(path).load()

            self.assertEqual(
                (config.reader_id, config.reader_display_name), ("demo_reader", "Demo Reader")
            )

            migrated = json.loads(path.read_text(encoding="utf-8"))
            backup = data / "config.v1.backup.json"
            self.assertEqual(migrated["schema"], CONFIG_SCHEMA)
            self.assertEqual(migrated["source"]["kind"], "synthetic")
            self.assertEqual(config.source_root, source.resolve())
            self.assertTrue(backup.is_file())
            self.assertEqual(json.loads(backup.read_text(encoding="utf-8")), value)
            self.assertEqual(backup.stat().st_mode & 0o777, 0o600)

    def test_policy_collections_require_a_list_of_non_empty_strings(self) -> None:
        def payload(allowed: object = "__missing__", denied: object = "__missing__"):
            policy: dict[str, object] = {"mode": "allowlist"}
            if allowed != "__missing__":
                policy["allowed"] = allowed
            if denied != "__missing__":
                policy["denied"] = denied
            return {
                "schema": CONFIG_SCHEMA,
                "paths": {
                    "data_dir": "/tmp/synthetic-state",
                    "window_db": "/tmp/synthetic-state/window.db",
                    "socket": "/tmp/synthetic-state/run/sightglassd.sock",
                },
                "source": {
                    "kind": "synthetic",
                    "instance_id": "synthetic-default",
                    "settings_path": "/tmp/synthetic-source/source.json",
                },
                "reader": {"reader_id": "reader", "display_name": "Reader"},
                "policy": policy,
                "auth": {"reader_token_hash": "", "operator_token_hash": ""},
            }

        # A well-formed list of strings is admitted and normalized.
        parsed = SightglassConfig.from_dict(payload(["conv-b", "conv-a", "conv-a"], []))
        self.assertEqual(parsed.allowed_conversation_ids, ("conv-a", "conv-b"))

        # A missing field defaults to empty; an explicit null is malformed, not empty.
        missing = SightglassConfig.from_dict(payload())
        self.assertEqual(missing.allowed_conversation_ids, ())
        self.assertEqual(missing.denied_conversation_ids, ())

        for malformed in (
            None,
            "conv-a",
            {"conv-a": True},
            7,
            [""],
            [1],
            [None],
            "conv-a,conv-b",
        ):
            with self.subTest(collection="allowed", malformed=malformed):
                with self.assertRaisesRegex(RuntimeError, "policy allowed"):
                    SightglassConfig.from_dict(payload(malformed, []))
            with self.subTest(collection="denied", malformed=malformed):
                with self.assertRaisesRegex(RuntimeError, "policy denied"):
                    SightglassConfig.from_dict(payload([], malformed))

    def test_save_fsyncs_the_parent_directory_after_rename(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            store = ConfigStore(root / "state" / "config.json")
            config = SightglassConfig.create(root / "state", root / "source")
            synced: list[Path] = []

            import sightglass.runtime.config as config_module

            real_fsync_directory = config_module._fsync_directory

            def recording_fsync(directory: Path) -> None:
                synced.append(Path(directory))
                real_fsync_directory(directory)

            with patch.object(config_module, "_fsync_directory", new=recording_fsync):
                store.save(config)

            self.assertIn(store.path.parent, synced)
            self.assertEqual(store.load().data_dir, config.data_dir)

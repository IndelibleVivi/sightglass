from __future__ import annotations

import json
import tempfile
import unittest
from argparse import Namespace
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

from sightglass.cli import _read_semantic_file, _semantic_command
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.runtime.config import ConfigStore, SightglassConfig
from sightglass.runtime.lanes import RuntimeLanes, WorkClass
from sightglass.runtime.semantic_worker import SemanticWorker
from sightglass.runtime.service import build_daemon_tools
from sightglass.semantic.settings import SemanticSettings
from sightglass.source.synthetic import create_synthetic_source


class SemanticRuntimeTests(unittest.TestCase):
    def test_legacy_config_defaults_to_disabled_and_never_reads_CF_secret(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = SightglassConfig.create(
                root / "state", create_synthetic_source(root / "source")
            )
            value = config.as_dict()
            del value["semantic"]
            legacy = SightglassConfig.from_dict(value)
            self.assertFalse(legacy.semantic.enabled)
            secrets = Mock()
            tools = build_daemon_tools(legacy, secret_store=secrets)
            try:
                secrets.get.assert_not_called()
                self.assertIsNone(tools.service.semantic)
                self.assertFalse((root / "state" / "semantic").exists())
            finally:
                tools.close()

    def test_enabled_missing_credential_degrades_without_losing_reader(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings = SemanticSettings(
                enabled=True,
                external_data_authorized=True,
                cf_account_id="a" * 32,
                index_name="sightglass-synthetic-unit",
                source_account_id="synthetic-account",
                conversation_ids=("synthetic-conversation",),
            )
            config = replace(
                SightglassConfig.create(root / "state", create_synthetic_source(root / "source")),
                semantic=settings,
            )
            secrets = Mock()
            secrets.get.side_effect = RuntimeError("missing")
            tools = build_daemon_tools(config, secret_store=secrets)
            try:
                self.assertEqual(tools.service.retrieval.semantic_status()["state"], "degraded")
                self.assertEqual(
                    tools.wechat_find_conversations("")["schema"],
                    "sightglass.conversation-catalog.v2",
                )
            finally:
                tools.close()

    def test_configure_requires_explicit_external_flag_and_stopped_daemon(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ConfigStore(root / "state" / "config.json")
            config = SightglassConfig.create(root / "state", root / "source")
            store.save(config)
            settings = {
                "enabled": True,
                "external_data_authorized": True,
                "cf_account_id": "a" * 32,
                "index_name": "sightglass-synthetic-unit",
                "source_account_id": "synthetic-account",
                "conversation_ids": ["synthetic-conversation"],
            }
            path = root / "settings.json"
            path.write_text(json.dumps(settings))
            path.chmod(0o600)
            args = Namespace(
                retrieval_command="configure-semantic",
                settings_file=str(path),
                authorize_external_data=False,
            )
            with self.assertRaisesRegex(RuntimeError, "authorize-external-data"):
                _semantic_command(args, store)
            self.assertFalse(store.load().semantic.enabled)
            args.authorize_external_data = True
            with patch(
                "sightglass.cli._daemon_must_be_stopped", side_effect=RuntimeError("running")
            ):
                with self.assertRaisesRegex(RuntimeError, "running"):
                    _semantic_command(args, store)
            self.assertFalse(store.load().semantic.enabled)
            self.assertFalse(_semantic_command(args, store)["activated"])
            self.assertTrue(store.load().semantic.enabled)
            _semantic_command(Namespace(retrieval_command="disable-semantic"), store)
            self.assertFalse(store.load().semantic.enabled)
            self.assertEqual(store.load().semantic.source_account_id, "synthetic-account")

    def test_semantic_sidecar_pressure_preserves_the_deterministic_reader(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings = SemanticSettings(
                enabled=True,
                external_data_authorized=True,
                cf_account_id="a" * 32,
                index_name="sightglass-synthetic-unit",
                source_account_id="synthetic-account",
                conversation_ids=("synthetic-conversation",),
            )
            config = replace(
                SightglassConfig.create(root / "state", create_synthetic_source(root / "source")),
                semantic=settings,
            )
            with patch(
                "sightglass.runtime.service.SemanticService",
                side_effect=SightglassError(ErrorCode.STORAGE_PRESSURE),
            ):
                tools = build_daemon_tools(config, secret_store=Mock(get=lambda _key: "synthetic"))
            try:
                self.assertEqual(
                    tools.service.retrieval.semantic_status()["reason"], "storage_pressure"
                )
                self.assertEqual(
                    tools.wechat_find_conversations("")["schema"],
                    "sightglass.conversation-catalog.v2",
                )
            finally:
                tools.close()

    def test_import_file_rejects_public_and_linked_tokens(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "token"
            path.write_text("synthetic-token")
            path.chmod(0o644)
            with self.assertRaisesRegex(RuntimeError, "owner-private"):
                _read_semantic_file(str(path))
            path.chmod(0o600)
            self.assertEqual(_read_semantic_file(str(path)), b"synthetic-token")
            alias = root / "link"
            alias.symlink_to(path)
            with self.assertRaises(OSError):
                _read_semantic_file(str(alias))

    def test_semantic_saturation_preserves_local_read_capacity(self):
        lanes = RuntimeLanes()
        semantic = [lanes.try_acquire(WorkClass.SEMANTIC_READ) for _ in range(2)]
        self.assertIsNone(lanes.try_acquire(WorkClass.SEMANTIC_READ))
        local = lanes.try_acquire(WorkClass.LOCAL_READ)
        self.assertIsNotNone(local)
        for lease in [*semantic, local]:
            assert lease is not None
            lease.release()

    def test_pending_publication_and_pause_do_not_spin_worker(self):
        service = Mock()
        service.reader.paused = False
        service.index_once.return_value = {"state": "pending", "processed": 32}
        worker = SemanticWorker(service)
        self.assertFalse(worker.run_once())
        service.index_once.return_value = {"state": "building", "processed": 0, "scanned": 32}
        self.assertTrue(worker.run_once())
        service.reader.paused = True
        service.index_once.reset_mock()
        self.assertFalse(worker.run_once())
        service.index_once.assert_not_called()

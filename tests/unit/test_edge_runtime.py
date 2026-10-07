from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from sightglass.contracts.capture import CaptureProtocolError, CaptureRequest
from sightglass.runtime.activation import write_activation
from sightglass.runtime.edge_runtime import EdgeSettings, build_executor, enroll_edge
from sightglass.source.capture import projection_origin_epoch
from sightglass.source.direct_wechat import SyntheticSourceProvider
from sightglass.source.synthetic import create_synthetic_source


class EdgeRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        source = create_synthetic_source(self.root / "source")
        self.settings = EdgeSettings(
            "synthetic",
            source / "source.json",
            "synthetic-instance",
            "synthetic-account-demo",
            frozenset({"conv_group"}),
            "synthetic-egress",
            projection_origin_epoch(SyntheticSourceProvider(source)),
            "synthetic-stream",
            self.root / "spool",
            "synthetic-host",
            self.root / "identity",
            "synthetic-edge-token",
            self.root / "activation",
            "synthetic-edge-owner",
            "synthetic-core-owner",
        )
        write_activation(
            self.settings.activation_path,
            generation=self.settings.activation_generation,
            state="active",
            role="edge",
            namespace=self.settings.spool_directory,
        )

    def test_enrollment_creates_only_bounded_edge_state_and_never_opens_window(self) -> None:
        with mock.patch("sightglass.model.db.WindowDB", side_effect=AssertionError("no WindowDB")):
            result = enroll_edge(self.settings)
        self.assertFalse(result["window_db_created"])
        self.assertTrue((self.settings.spool_directory / "edge-spool.sqlite").exists())
        self.assertFalse(any(self.root.rglob("window.db")))
        with self.assertRaises(CaptureProtocolError):
            enroll_edge(self.settings)

    def test_origin_change_and_revoked_role_refuse_before_spool_initialization(self) -> None:
        with self.assertRaisesRegex(CaptureProtocolError, "interpretation_epoch"):
            enroll_edge(replace(self.settings, origin_epoch="synthetic-changed"))
        self.assertFalse(self.settings.spool_directory.exists())
        write_activation(
            self.settings.activation_path,
            generation=self.settings.activation_generation,
            state="revoked",
            role="edge",
            namespace=self.settings.spool_directory,
            expected_generation=self.settings.activation_generation,
        )
        with self.assertRaisesRegex(RuntimeError, "no active"):
            enroll_edge(self.settings)
        self.assertFalse(self.settings.spool_directory.exists())

    def test_revocation_after_source_capture_prevents_sealed_publication(self) -> None:
        executor = build_executor(self.settings)
        original = executor.provider.session

        def revoked_session(scope):
            write_activation(
                self.settings.activation_path,
                generation=self.settings.activation_generation,
                state="revoked",
                role="edge",
                namespace=self.settings.spool_directory,
                expected_generation=self.settings.activation_generation,
            )
            return original(scope)

        with mock.patch.object(executor.provider, "session", side_effect=revoked_session):
            with self.assertRaisesRegex(RuntimeError, "no active"):
                executor.capture(
                    CaptureRequest(
                        "synthetic-request",
                        "recent",
                        self.settings.account_id,
                        "synthetic-policy",
                        conversation_source_id="conv_group",
                    ),
                    stream_epoch=self.settings.stream_epoch,
                    sequence=1,
                )

    def test_strict_private_settings_roundtrip_and_no_shell_rpc_parameters(self) -> None:
        path = self.root / "edge.json"
        path.write_text(json.dumps(self.settings.as_dict()))
        path.chmod(0o600)
        self.assertEqual(EdgeSettings.load(path), self.settings)
        for mutation in (
            {"source_kind": "remote-capture"},
            {"relay_host": "host;command"},
            {"activation_path": "relative"},
            {"shell_command": "arbitrary"},
        ):
            with (
                self.subTest(mutation=mutation),
                self.assertRaises((RuntimeError, CaptureProtocolError)),
            ):
                EdgeSettings.from_dict({**self.settings.as_dict(), **mutation})
        path.chmod(0o644)
        with self.assertRaises(RuntimeError):
            EdgeSettings.load(path)


if __name__ == "__main__":
    unittest.main()

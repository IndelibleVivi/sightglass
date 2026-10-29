from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sightglass.contracts.capture import CaptureProtocolError
from sightglass.runtime.activation import write_activation
from sightglass.runtime.edge import EdgeSpool
from sightglass.runtime.edge_runtime import (
    SOURCE_ACCESS_MODE,
    EdgeSettings,
    authorize_source_access,
    build_executor,
    source_access_scope_preview,
)
from sightglass.runtime.secrets import MemorySecretStore
from sightglass.runtime.source_access import HelperArtifact, ReceiverContext, derive_receiver_proof
from sightglass.source.capture import projection_origin_epoch
from sightglass.source.direct_wechat import SyntheticSourceProvider
from sightglass.source.macos_wechat.config import MacOSWeChatSettings
from sightglass.source.macos_wechat.keys import (
    new_source_account_binding_id,
    source_account_key,
)
from sightglass.source.synthetic import create_synthetic_source


def _private_directory(path: Path) -> Path:
    path.mkdir(parents=True, mode=0o700)
    os.chmod(path, 0o700)
    return path


def _helper(root: Path) -> HelperArtifact:
    app = _private_directory(root / "SourceAccess.app")
    contents = _private_directory(app / "Contents")
    executable_directory = _private_directory(contents / "MacOS")
    executable = executable_directory / "SightglassSourceAccess"
    payload = b"synthetic-helper"
    executable.write_bytes(payload)
    executable.chmod(0o700)
    (contents / "Info.plist").write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>CFBundleIdentifier</key><string>dev.sightglass.source-access</string>
<key>CFBundleExecutable</key><string>SightglassSourceAccess</string>
<key>CFBundlePackageType</key><string>APPL</string>
</dict></plist>
"""
    )
    os.chmod(contents / "Info.plist", 0o600)
    metadata = executable.stat()
    import hashlib

    return HelperArtifact(
        app_path=app,
        executable_path=executable,
        digest=hashlib.sha256(payload).hexdigest(),
        size=len(payload),
        revision=(
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mtime_ns,
            metadata.st_ctime_ns,
            0o700,
            metadata.st_uid,
        ),
    )


class EdgeSourceAccessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        host = mock.patch("sightglass.runtime.activation.host_identity", return_value="1" * 64)
        host.start()
        self.addCleanup(host.stop)
        all_accounts = _private_directory(self.root / "xwechat_files")
        self.account = _private_directory(all_accounts / "synthetic-account")
        self.source_root = self.account / "db_storage"
        (self.source_root / "contact").mkdir(parents=True)
        (self.source_root / "session").mkdir()
        (self.source_root / "contact" / "contact.db").write_bytes(b"contact")
        (self.source_root / "session" / "session.db").write_bytes(b"session")
        shared = all_accounts / "all_users" / "config" / "global_config"
        shared.parent.mkdir(parents=True)
        shared.write_bytes(b"shared")
        Path(f"{shared}.crc").write_bytes(b"meta")
        binding_id = new_source_account_binding_id()
        self.native = MacOSWeChatSettings(
            instance_id="synthetic-instance",
            source_root=self.source_root,
            keychain_account=f"source.{binding_id}.database-keys",
            source_account_binding_id=binding_id,
            source_account_key=source_account_key(binding_id),
            bundle_id="com.tencent.xinWeChat",
            version="4.1.13",
            build="269602",
            architecture="arm64",
            profile_id="wechat-macos-4.1.13-269602-arm64-v1",
        )
        self.private = _private_directory(self.root / "private")
        self.native_path = self.private / "source.json"
        self.native.save(self.native_path)
        synthetic = create_synthetic_source(self.root / "synthetic")
        self.synthetic = EdgeSettings(
            "synthetic",
            synthetic / "source.json",
            "synthetic-instance",
            "synthetic-account",
            frozenset({"conv_group"}),
            "synthetic-egress",
            projection_origin_epoch(SyntheticSourceProvider(synthetic)),
            "synthetic-stream",
            self.root / "synthetic-spool",
            "synthetic-host",
            self.root / "identity",
            "synthetic-token",
            self.root / "synthetic-activation",
            "synthetic-edge",
            "synthetic-core",
        )
        self.settings = EdgeSettings(
            "macos-wechat",
            self.native_path,
            self.native.instance_id,
            self.native.source_account_key,
            frozenset({"synthetic-conversation"}),
            "synthetic-egress",
            "synthetic-origin",
            "synthetic-stream",
            self.root / "spool",
            "synthetic-host",
            self.root / "identity",
            "synthetic-token",
            self.root / "activation",
            "synthetic-edge",
            "synthetic-core",
        )
        self.settings_path = self.private / "edge.json"
        self.settings_path.write_text(json.dumps(self.settings.as_dict()))
        self.settings_path.chmod(0o600)
        self.secrets = MemorySecretStore(
            {self.settings.capability_secret_account: "synthetic-edge-relay-token"}
        )
        write_activation(
            self.settings.activation_path,
            generation=self.settings.activation_generation,
            state="active",
            role="edge",
            namespace=self.settings.spool_directory,
        )
        EdgeSpool.initialize(
            self.settings.spool_directory,
            source_instance_id=self.settings.source_instance_id,
            account_id=self.settings.account_id,
            origin_epoch=self.settings.origin_epoch,
            stream_epoch=self.settings.stream_epoch,
        ).close()
        self.artifact = _helper(self.private)

    def test_existing_settings_compatibility_and_synthetic_refusal(self) -> None:
        self.assertIsNone(self.settings.source_access_helper_path)
        self.assertFalse(self.settings.source_access_shared_config)
        with self.assertRaisesRegex(RuntimeError, "macos-wechat"):
            self.synthetic.with_source_access(self.artifact.app_path, self.artifact.digest)

    def test_optional_fields_roundtrip_and_reject_incomplete_bindings(self) -> None:
        original = self.settings.as_dict()
        self.assertNotIn("source_access_helper_path", original)
        rebound = EdgeSettings.from_dict(
            {
                **original,
                "source_access_helper_path": str(self.artifact.app_path),
                "source_access_helper_digest": self.artifact.digest,
                "source_access_shared_config": True,
                "source_access_mode": SOURCE_ACCESS_MODE,
            }
        )
        self.assertEqual(EdgeSettings.from_dict(rebound.as_dict()), rebound)
        for mutation in (
            {"source_access_helper_digest": None},
            {"source_access_mode": None},
            {"source_access_shared_config": "yes"},
        ):
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                EdgeSettings.from_dict({**rebound.as_dict(), **mutation})

    def test_preview_binds_account_scope_without_exposing_paths(self) -> None:
        preview = source_access_scope_preview(self.settings, include_shared_config=True)
        self.assertEqual(preview["account_scope"], "configured_account")
        self.assertEqual(preview["database_subdirectory"], "db_storage")
        self.assertEqual(len(preview["shared_config"]), 2)
        self.assertNotIn(str(self.root), json.dumps(preview))
        with self.assertRaisesRegex(RuntimeError, "macos-wechat"):
            source_access_scope_preview(self.synthetic)

    def test_authorize_is_grant_only_and_preserves_both_before_images(self) -> None:
        edge_before = self.settings_path.read_bytes()
        native_before = self.native_path.read_bytes()
        with (
            mock.patch(
                "sightglass.runtime.edge_runtime.validate_helper",
                return_value=self.artifact,
            ),
            mock.patch(
                "sightglass.runtime.edge_runtime.helper_authorize",
                return_value=(mock.Mock(), self.artifact),
                create=True,
            ),
        ):
            report = authorize_source_access(
                self.settings_path,
                helper_path=self.artifact.app_path,
                secret_store=self.secrets,
            )
        self.assertTrue(report["granted"])
        self.assertFalse(report["bound"])
        self.assertTrue(report["settings_unchanged"])
        self.assertEqual(self.settings_path.read_bytes(), edge_before)
        self.assertEqual(self.native_path.read_bytes(), native_before)

    def test_bind_rejects_nonshadow_targets_before_secret_or_picker(self) -> None:
        before = self.settings_path.read_bytes()
        unsafe = self.private / "unsafe"
        unsafe.mkdir(mode=0o755)
        unsafe.chmod(0o755)
        cases: tuple[tuple[bool, Path | None], ...] = (
            (True, None),
            (True, self.settings_path),
            (False, self.private / "unused.json"),
            (True, unsafe / "edge.json"),
        )
        for bind, output in cases:
            with (
                self.subTest(bind=bind, output=output),
                mock.patch.object(self.secrets, "get") as secret,
                mock.patch("sightglass.runtime.edge_runtime.helper_authorize") as picker,
                self.assertRaises(RuntimeError),
            ):
                authorize_source_access(
                    self.settings_path,
                    helper_path=self.artifact.app_path,
                    secret_store=self.secrets,
                    bind=bind,
                    output_path=output,
                )
            secret.assert_not_called()
            picker.assert_not_called()
        self.assertEqual(self.settings_path.read_bytes(), before)
        self.assertEqual(unsafe.stat().st_mode & 0o777, 0o755)

    def test_authorize_rejects_mismatched_native_binding_before_secret_or_picker(self) -> None:
        value = self.native.as_dict()
        value["instance_id"] = "foreign-instance"
        self.native_path.write_text(json.dumps(value))
        with (
            mock.patch.object(self.secrets, "get") as secret,
            mock.patch("sightglass.runtime.edge_runtime.helper_authorize") as picker,
            self.assertRaisesRegex(RuntimeError, "native settings do not match"),
        ):
            authorize_source_access(
                self.settings_path,
                helper_path=self.artifact.app_path,
                secret_store=self.secrets,
            )
        secret.assert_not_called()
        picker.assert_not_called()

    def test_authorize_shadow_bind_does_not_touch_existing_edge_config(self) -> None:
        edge_before = self.settings_path.read_bytes()
        shadow = self.private / "edge-source-access.json"
        with (
            mock.patch(
                "sightglass.runtime.edge_runtime.validate_helper",
                return_value=self.artifact,
            ),
            mock.patch(
                "sightglass.runtime.edge_runtime.helper_authorize",
                return_value=(mock.Mock(), self.artifact),
                create=True,
            ),
        ):
            report = authorize_source_access(
                self.settings_path,
                helper_path=self.artifact.app_path,
                include_shared_config=True,
                output_path=shadow,
                bind=True,
                secret_store=self.secrets,
            )
        self.assertTrue(report["bound"])
        self.assertTrue(report["shadow"])
        self.assertEqual(self.settings_path.read_bytes(), edge_before)
        rebound = EdgeSettings.load(shadow)
        self.assertEqual(rebound.source_access_helper_digest, self.artifact.digest)
        self.assertTrue(rebound.source_access_shared_config)
        self.assertEqual(rebound.source_settings_path, self.native_path)

    def test_authorize_before_image_change_writes_no_binding(self) -> None:
        edge_before = self.settings_path.read_bytes()
        native_before = self.native_path.read_bytes()
        shadow = self.private / "edge-source-access.json"

        def mutate(*_args, **_kwargs):
            self.native_path.write_bytes(native_before + b"\n")
            return mock.Mock(), self.artifact

        with (
            mock.patch(
                "sightglass.runtime.edge_runtime.validate_helper",
                return_value=self.artifact,
            ),
            mock.patch(
                "sightglass.runtime.edge_runtime.helper_authorize",
                side_effect=mutate,
                create=True,
            ),
            self.assertRaisesRegex(RuntimeError, "source changed"),
        ):
            authorize_source_access(
                self.settings_path,
                helper_path=self.artifact.app_path,
                output_path=shadow,
                bind=True,
                secret_store=self.secrets,
            )
        self.assertFalse(shadow.exists())
        self.assertEqual(self.settings_path.read_bytes(), edge_before)

    def test_build_executor_leases_first_and_closes_on_provider_failure(self) -> None:
        leased = self.settings.with_source_access(self.artifact.app_path, self.artifact.digest)
        order: list[str] = []
        closed: list[bool] = []

        class Lease:
            def close(self) -> None:
                closed.append(True)

        def fail_provider(*_args, **_kwargs):
            raise CaptureProtocolError("source_access_capability_denied")

        with (
            mock.patch(
                "sightglass.runtime.edge_runtime.validate_helper",
                side_effect=lambda _path: order.append("helper") or self.artifact,
            ),
            mock.patch(
                "sightglass.runtime.edge_runtime.SourceAccessLease",
                side_effect=lambda *_args, **_kwargs: order.append("lease") or Lease(),
            ),
            mock.patch(
                "sightglass.runtime.edge_runtime.MacOSWeChatSourceProvider",
                side_effect=fail_provider,
            ),
            self.assertRaisesRegex(CaptureProtocolError, "capability_denied"),
        ):
            build_executor(leased, secret_store=self.secrets)
        self.assertEqual(order, ["helper", "lease"])
        self.assertEqual(closed, [True])

    def test_build_executor_binding_failure_releases_lease(self) -> None:
        leased = self.settings.with_source_access(self.artifact.app_path, self.artifact.digest)
        closed: list[bool] = []

        class Lease:
            def close(self) -> None:
                closed.append(True)

        changed = mock.Mock()
        changed.settings = mock.Mock(
            instance_id="other-instance",
            source_account_key=self.native.source_account_key,
        )
        with (
            mock.patch(
                "sightglass.runtime.edge_runtime.validate_helper",
                return_value=self.artifact,
            ),
            mock.patch(
                "sightglass.runtime.edge_runtime.SourceAccessLease",
                return_value=Lease(),
            ),
            mock.patch(
                "sightglass.runtime.edge_runtime.MacOSWeChatSourceProvider",
                return_value=changed,
            ),
            self.assertRaisesRegex(CaptureProtocolError, "account_binding_changed"),
        ):
            build_executor(leased, secret_store=self.secrets)
        self.assertEqual(closed, [True])

    def test_build_executor_derives_same_existing_secret_without_extra_reads(self) -> None:
        leased = self.settings.with_source_access(self.artifact.app_path, self.artifact.digest)
        read_accounts: list[str] = []

        class CountingStore:
            def get(self, account: str) -> str:
                read_accounts.append(account)
                return self_secrets.get(account)

            def set(self, account: str, value: str) -> None:
                del account, value
                raise AssertionError("source access must not write a secret")

        self_secrets = self.secrets
        fake_provider = mock.Mock()
        fake_provider.settings = self.native
        fake_provider.descriptor = SyntheticSourceProvider(self.root / "synthetic").descriptor
        proof_arguments: list[str] = []

        class Lease:
            def close(self) -> None:
                pass

        with (
            mock.patch(
                "sightglass.runtime.edge_runtime.validate_helper",
                return_value=self.artifact,
            ),
            mock.patch(
                "sightglass.runtime.edge_runtime.SourceAccessLease",
                side_effect=lambda *_args, **kwargs: (
                    proof_arguments.append(kwargs["receiver_proof"]) or Lease()
                ),
            ),
            mock.patch(
                "sightglass.runtime.edge_runtime.MacOSWeChatSourceProvider",
                return_value=fake_provider,
            ),
            mock.patch(
                "sightglass.source.capture.executor.projection_origin_epoch",
                return_value=leased.origin_epoch,
            ),
        ):
            executor = build_executor(leased, secret_store=CountingStore())
        expected = derive_receiver_proof(
            "synthetic-edge-relay-token",
            ReceiverContext(
                source_instance_id=leased.source_instance_id,
                account_id=leased.account_id,
                account_path=str(self.account),
            ),
        )
        self.assertEqual(read_accounts, [leased.capability_secret_account])
        self.assertEqual(proof_arguments, [expected])
        getattr(executor.provider, "close")()

    def test_missing_receiver_credential_fails_before_provider_construction(self) -> None:
        leased = self.settings.with_source_access(self.artifact.app_path, self.artifact.digest)
        with (
            mock.patch(
                "sightglass.runtime.edge_runtime.validate_helper",
                return_value=self.artifact,
            ),
            mock.patch(
                "sightglass.runtime.edge_runtime.MacOSWeChatSourceProvider",
            ) as provider,
            self.assertRaisesRegex(
                CaptureProtocolError, "source_access_receiver_credential_missing"
            ),
        ):
            build_executor(leased, secret_store=MemorySecretStore())
        provider.assert_not_called()


if __name__ == "__main__":
    unittest.main()

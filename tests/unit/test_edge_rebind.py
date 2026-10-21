from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from sightglass.contracts.capture import CaptureProtocolError
from sightglass.runtime.activation import write_activation
from sightglass.runtime.edge import EdgeSpool
from sightglass.runtime.edge_runtime import EdgeSettings, rebind_edge
from sightglass.runtime.secrets import MemorySecretStore
from sightglass.source.macos_wechat.config import MacOSWeChatSettings
from sightglass.source.macos_wechat.discovery import WeChatCandidate
from sightglass.source.macos_wechat.keys import new_source_account_binding_id, source_account_key


class EdgeRebindTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        host = mock.patch(
            "sightglass.runtime.activation.host_identity", return_value="1" * 64
        )
        host.start()
        self.addCleanup(host.stop)
        source_root = self.root / "native" / "synthetic-account" / "db_storage"
        (source_root / "contact").mkdir(parents=True)
        (source_root / "session").mkdir()
        (source_root / "message").mkdir()
        self.keys = {
            "contact/contact.db": "11" * 32,
            "session/session.db": "22" * 32,
            "message/message_0.db": "33" * 32,
        }
        self.verified_keys = {
            name: {"enc_key": key} for name, key in self.keys.items()
        }
        binding_id = new_source_account_binding_id()
        self.native_settings = MacOSWeChatSettings(
            instance_id="synthetic-native-instance",
            source_root=source_root.resolve(),
            keychain_account=f"source.{binding_id}.database-keys",
            source_account_binding_id=binding_id,
            source_account_key=source_account_key(binding_id),
            bundle_id="com.tencent.xinWeChat",
            version="4.1.13",
            build="269602",
            architecture="arm64",
            profile_id="wechat-macos-4.1.13-269602-arm64-v1",
        )
        self.private = self.root / "private"
        self.private.mkdir(mode=0o700)
        self.source_settings_path = self.private / "source.json"
        self.native_settings.save(self.source_settings_path)
        self.edge_settings = EdgeSettings(
            "macos-wechat",
            self.source_settings_path,
            self.native_settings.instance_id,
            self.native_settings.source_account_key,
            frozenset({"synthetic-conversation"}),
            "synthetic-egress",
            "synthetic-origin",
            "synthetic-stream",
            self.root / "spool",
            "synthetic-host",
            self.root / "identity",
            "synthetic-edge-token",
            self.root / "activation",
            "synthetic-edge-owner",
            "synthetic-core-owner",
        )
        self.edge_settings_path = self.private / "edge.json"
        self.edge_settings_path.write_text(json.dumps(self.edge_settings.as_dict()))
        self.edge_settings_path.chmod(0o600)
        write_activation(
            self.edge_settings.activation_path,
            generation=self.edge_settings.activation_generation,
            state="active", role="edge", namespace=self.edge_settings.spool_directory,
        )
        spool = EdgeSpool.initialize(
            self.edge_settings.spool_directory,
            source_instance_id=self.edge_settings.source_instance_id,
            account_id=self.edge_settings.account_id,
            origin_epoch=self.edge_settings.origin_epoch,
            stream_epoch=self.edge_settings.stream_epoch,
        )
        spool.close()
        self.secrets = MemorySecretStore()
        self.secrets.set(
            self.native_settings.keychain_account,
            json.dumps(
                self.verified_keys,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        self.candidate = WeChatCandidate(
            candidate_id=self.native_settings.instance_id,
            app_path=Path("/Applications/WeChat.app"),
            source_root=self.native_settings.source_root,
            bundle_id=self.native_settings.bundle_id,
            version="4.1.15",
            build="270115",
            architecture="arm64",
            running=True,
            profile_id="wechat-macos-4.1.15-270115-arm64-v1",
        )

    def test_same_account_rebind_preserves_identity_and_edge_configuration(self) -> None:
        before_edge = self.edge_settings.as_dict()
        before_native = json.loads(self.source_settings_path.read_bytes())
        before_native["synthetic_preserved_setting"] = {"value": 7}
        self.source_settings_path.write_text(json.dumps(before_native))
        spool_before = (self.edge_settings.spool_directory / "edge-spool.sqlite").read_bytes()
        with (
            mock.patch(
                "sightglass.source.macos_wechat.discovery.discover_configured_candidate",
                return_value=self.candidate,
            ),
            mock.patch(
                "sightglass.source.macos_wechat.keys.verify_key_map",
                return_value=self.verified_keys,
            ),
            mock.patch(
                "sightglass.model.db.WindowDB",
                side_effect=AssertionError("rebind must not open WindowDB"),
            ),
            mock.patch.object(
                self.secrets, "set", side_effect=AssertionError("rebind must not write secrets")
            ),
        ):
            result = rebind_edge(
                self.edge_settings_path,
                same_account=True,
                secret_store=self.secrets,
            )
        rebound = MacOSWeChatSettings.load(self.source_settings_path)
        self.assertEqual(result["profile_id"], self.candidate.profile_id)
        self.assertEqual(result["verified_database_count"], len(self.keys))
        self.assertEqual(rebound.instance_id, self.native_settings.instance_id)
        self.assertEqual(
            rebound.source_account_binding_id,
            self.native_settings.source_account_binding_id,
        )
        self.assertEqual(rebound.source_account_key, self.native_settings.source_account_key)
        self.assertEqual(rebound.keychain_account, self.native_settings.keychain_account)
        after_native = json.loads(self.source_settings_path.read_bytes())
        self.assertEqual(after_native.pop("candidate_binding"), {
            **before_native["candidate_binding"],
            "version": self.candidate.version, "build": self.candidate.build,
            "profile_id": self.candidate.profile_id,
        })
        before_native.pop("candidate_binding")
        self.assertEqual(after_native, before_native)
        self.assertEqual(self.edge_settings.as_dict(), before_edge)
        self.assertEqual(
            (self.edge_settings.spool_directory / "edge-spool.sqlite").read_bytes(), spool_before
        )
        self.assertTrue((self.edge_settings.spool_directory / "edge-spool.sqlite").exists())
        self.assertFalse(any(self.root.rglob("window.db")))

    def test_rebind_failures_leave_the_exact_before_image(self) -> None:
        before = self.source_settings_path.read_bytes()
        failures = (
            mock.patch(
                "sightglass.source.macos_wechat.discovery.discover_configured_candidate",
                return_value=replace(self.candidate, running=False),
            ),
            mock.patch(
                "sightglass.source.macos_wechat.keys.verify_key_map",
                side_effect=RuntimeError("verified WeChat key set is incomplete"),
            ),
            mock.patch(
                "sightglass.source.macos_wechat.discovery.discover_configured_candidate",
                return_value=replace(self.candidate, source_root=self.root / "foreign"),
            ),
        )
        for failure in failures:
            with failure, self.assertRaises(RuntimeError):
                rebind_edge(
                    self.edge_settings_path,
                    same_account=True,
                    secret_store=self.secrets,
                )
            self.assertEqual(self.source_settings_path.read_bytes(), before)

        with mock.patch.object(
            EdgeSpool,
            "__init__",
            side_effect=CaptureProtocolError("edge_spool_in_use_or_unsafe"),
        ):
            with self.assertRaises(CaptureProtocolError):
                rebind_edge(
                    self.edge_settings_path,
                    same_account=True,
                    secret_store=self.secrets,
                )
        self.assertEqual(self.source_settings_path.read_bytes(), before)

    def test_rebind_requires_same_account_and_installed_keys(self) -> None:
        before = self.source_settings_path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "same-account"):
            rebind_edge(
                self.edge_settings_path,
                same_account=False,
                secret_store=self.secrets,
            )
        self.assertEqual(self.source_settings_path.read_bytes(), before)

        missing = MemorySecretStore()
        with (
            mock.patch(
                "sightglass.source.macos_wechat.discovery.discover_configured_candidate",
                return_value=self.candidate,
            ),
            self.assertRaises(RuntimeError),
        ):
            rebind_edge(
                self.edge_settings_path,
                same_account=True,
                secret_store=missing,
            )
        self.assertEqual(self.source_settings_path.read_bytes(), before)


    def test_rebind_refuses_revoked_ownership_before_reading_credentials(self) -> None:
        before = self.source_settings_path.read_bytes()
        write_activation(
            self.edge_settings.activation_path,
            generation=self.edge_settings.activation_generation,
            state="revoked", role="edge", namespace=self.edge_settings.spool_directory,
            expected_generation=self.edge_settings.activation_generation,
        )
        with (
            mock.patch.object(
                self.secrets, "get", side_effect=AssertionError("no credential read")
            ),
            self.assertRaisesRegex(RuntimeError, "no active"),
        ):
            rebind_edge(self.edge_settings_path, same_account=True, secret_store=self.secrets)
        self.assertEqual(self.source_settings_path.read_bytes(), before)

    def test_rebind_preserves_concurrently_replaced_native_settings(self) -> None:
        changed = json.loads(self.source_settings_path.read_bytes())
        changed["synthetic_concurrent_setting"] = True
        replacement = (json.dumps(changed) + "\n").encode()

        def verified(*_args):
            self.source_settings_path.write_bytes(replacement)
            return self.verified_keys

        before = self.source_settings_path.read_bytes()
        with (
            mock.patch(
                "sightglass.source.macos_wechat.discovery.discover_configured_candidate",
                return_value=self.candidate,
            ),
            mock.patch(
                "sightglass.source.macos_wechat.keys.verify_key_map",
                side_effect=verified,
            ),
            self.assertRaisesRegex(RuntimeError, "changed before rebind"),
        ):
            rebind_edge(
                self.edge_settings_path,
                same_account=True,
                secret_store=self.secrets,
            )
        self.assertNotEqual(self.source_settings_path.read_bytes(), before)
        self.assertEqual(self.source_settings_path.read_bytes(), replacement)

    def test_full_key_proof_rejects_a_required_shard_appearing_during_verification(self) -> None:
        from sightglass.source.macos_wechat.keys import verify_key_map

        source_root = self.native_settings.source_root
        for relative in self.keys:
            (source_root / relative).touch()

        def page1_matches(_root, relative, _key):
            if relative == "message/message_0.db":
                (source_root / "message/message_1.db").touch()
            return True

        with (
            mock.patch("sightglass.source.macos_wechat.keys._page1_matches",
                       side_effect=page1_matches),
            self.assertRaisesRegex(RuntimeError, "inventory changed"),
        ):
            verify_key_map(source_root, self.verified_keys)

    def test_native_settings_reject_symlinks_and_shared_inodes(self) -> None:
        alias = self.private / "synthetic-alias.json"
        alias.symlink_to(self.source_settings_path)
        with self.assertRaises(RuntimeError):
            MacOSWeChatSettings.load(alias)
        alias.unlink()
        alias.hardlink_to(self.source_settings_path)
        with self.assertRaises(RuntimeError):
            MacOSWeChatSettings.load(self.source_settings_path)

if __name__ == "__main__":
    unittest.main()

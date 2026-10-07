from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from sightglass.source.macos_wechat.config import (
    SETTINGS_SCHEMA,
    MacOSWeChatSettings,
)
from sightglass.source.macos_wechat.discovery import discover_configured_candidate
from sightglass.source.macos_wechat.keys import (
    image_decoder_keychain_account,
    import_verified_key_file,
    new_source_account_binding_id,
    parse_image_decoder_key,
    read_image_decoder_key_file,
    source_account_key,
)


class NativeSourceConfigTests(unittest.TestCase):
    def test_configured_candidate_probe_validates_only_the_enrolled_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_root = Path(temporary) / "xwechat_files"
            source = data_root / "account" / "db_storage"
            (source / "contact").mkdir(parents=True)
            (source / "session").mkdir()
            (source / "contact" / "contact.db").touch()
            (source / "session" / "session.db").touch()
            context = SimpleNamespace(
                app_path=Path("/Applications/WeChat.app"),
                data_root=data_root.resolve(),
                bundle_id="com.tencent.xinWeChat",
                version="4.1.13",
                build="269602",
                architecture="arm64",
                running=True,
                profile_id="wechat-macos-4.1.13-269602-arm64-v1",
            )

            with mock.patch(
                "sightglass.source.macos_wechat.discovery._discovery_context",
                return_value=context,
            ):
                candidate = discover_configured_candidate(source)
                outside = discover_configured_candidate(
                    Path(temporary) / "other" / "db_storage"
                )

        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate.source_root, source.resolve())
        self.assertTrue(candidate.running)
        self.assertIsNone(outside)

    def test_image_key_enrollment_round_trips_an_account_bound_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "db_storage"
            source.mkdir()
            binding_id = new_source_account_binding_id()
            account = image_decoder_keychain_account(binding_id)
            settings = MacOSWeChatSettings(
                instance_id="wxsrc_fixture",
                source_root=source.resolve(),
                keychain_account=f"source.{binding_id}.database-keys",
                source_account_binding_id=binding_id,
                source_account_key=source_account_key(binding_id),
                bundle_id="com.tencent.xinWeChat",
                version="4.1.13",
                build="269602",
                architecture="arm64",
                profile_id="wechat-macos-4.1.13-269602-arm64-v1",
                image_keychain_account=account,
            )
            path = root / "private" / "source.json"
            settings.save(path)

            loaded = MacOSWeChatSettings.load(path)

            self.assertEqual(loaded.image_keychain_account, account)
            self.assertNotIn("0123456789abcdef", json.dumps(settings.as_dict()))

    def test_image_key_binding_rejects_a_foreign_account_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "db_storage"
            source.mkdir()
            binding_id = new_source_account_binding_id()
            other_binding = new_source_account_binding_id()
            path = root / "private" / "source.json"
            path.parent.mkdir(parents=True)
            path.write_text(
                json.dumps(
                    {
                        "schema": SETTINGS_SCHEMA,
                        "instance_id": "wxsrc_fixture",
                        "candidate_binding": {
                            "bundle_id": "com.tencent.xinWeChat",
                            "version": "4.1.13",
                            "build": "269602",
                            "architecture": "arm64",
                            "profile_id": "wechat-macos-4.1.13-269602-arm64-v1",
                        },
                        "source_root": str(source),
                        "keychain_account": f"source.{binding_id}.database-keys",
                        "source_account_binding_id": binding_id,
                        "source_account_key": source_account_key(binding_id),
                        "image_keychain_account": (
                            image_decoder_keychain_account(other_binding)
                        ),
                    }
                ),
                encoding="utf-8",
            )
            os.chmod(path, 0o600)

            with self.assertRaisesRegex(RuntimeError, "image key binding"):
                MacOSWeChatSettings.load(path)

    def test_image_decoder_key_requires_exactly_thirty_two_hex_characters(self) -> None:
        self.assertEqual(len(parse_image_decoder_key("A" * 32)), 16)
        self.assertEqual(parse_image_decoder_key(b"0" * 32 + b"\n"), bytes(16))
        for value in (
            "",
            "0" * 31,
            "0" * 33,
            "z" * 32,
            "0123456789abcdef0123456789abcde",
            b"\x00" * 16,
        ):
            with self.subTest(value=value):
                with self.assertRaises(RuntimeError):
                    parse_image_decoder_key(value)

    def test_image_decoder_key_file_requires_a_private_owner_only_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            key_file = root / "image.key"
            key_file.write_text("0" * 32, encoding="utf-8")
            os.chmod(key_file, 0o600)
            self.assertEqual(read_image_decoder_key_file(key_file), bytes(16))

            os.chmod(key_file, 0o644)
            with self.assertRaises(RuntimeError):
                read_image_decoder_key_file(key_file)

            os.chmod(key_file, 0o600)
            alias = root / "image-alias.key"
            os.link(key_file, alias)
            with self.assertRaises(RuntimeError):
                read_image_decoder_key_file(alias)

    def test_v2_round_trip_preserves_stable_account_and_profile_binding(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "db_storage"
            source.mkdir()
            binding_id = new_source_account_binding_id()
            settings = MacOSWeChatSettings(
                instance_id="wxsrc_fixture",
                source_root=source.resolve(),
                keychain_account=f"source.{binding_id}.database-keys",
                source_account_binding_id=binding_id,
                source_account_key=source_account_key(binding_id),
                bundle_id="com.tencent.xinWeChat",
                version="4.1.13",
                build="269602",
                architecture="arm64",
                profile_id="wechat-macos-4.1.13-269602-arm64-v1",
            )
            path = root / "private" / "source.json"

            settings.save(path)
            loaded = MacOSWeChatSettings.load(path)

            self.assertEqual(loaded, settings)
            saved = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(saved["schema"], SETTINGS_SCHEMA)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_v1_load_preserves_the_existing_account_key_for_reimport(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "db_storage"
            source.mkdir()
            path = root / "source.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": "sightglass.source.macos-wechat.v1",
                        "instance_id": "wxsrc_legacy",
                        "candidate_binding": {
                            "bundle_id": "com.tencent.xinWeChat",
                            "version": "4.1.13",
                            "build": "269602",
                            "architecture": "arm64",
                        },
                        "source_root": str(source),
                        "keychain_account": "source.legacy.database-keys",
                        "source_account_key": "legacy-account-key",
                    }
                ),
                encoding="utf-8",
            )
            os.chmod(path, 0o600)

            loaded = MacOSWeChatSettings.load(path)

            self.assertEqual(loaded.source_account_key, "legacy-account-key")
            self.assertEqual(
                loaded.source_account_binding_id, "legacy-legacy-account-key"
            )
            self.assertEqual(loaded.profile_id, "")

    def test_legacy_binding_gets_a_private_deterministic_image_key_account(self) -> None:
        binding = "legacy-legacy-account-key"

        first = image_decoder_keychain_account(binding)
        second = image_decoder_keychain_account(binding)

        self.assertEqual(first, second)
        self.assertTrue(first.startswith("source.wxlegacy_"))
        self.assertTrue(first.endswith(".image-decoder-key"))
        self.assertNotIn(binding, first)
        self.assertNotIn("legacy-account-key", first)

    def test_account_key_rejects_an_unscoped_or_empty_binding(self) -> None:
        for value in ("", "candidate-path", "wxbind_short"):
            with self.subTest(value=value):
                with self.assertRaises(RuntimeError):
                    source_account_key(value)

    def test_v2_rejects_a_mismatched_account_key(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "db_storage"
            source.mkdir()
            binding_id = new_source_account_binding_id()
            path = root / "source.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": SETTINGS_SCHEMA,
                        "instance_id": "wxsrc_fixture",
                        "candidate_binding": {
                            "bundle_id": "com.tencent.xinWeChat",
                            "version": "4.1.13",
                            "build": "269602",
                            "architecture": "arm64",
                            "profile_id": "wechat-macos-4.1.13-269602-arm64-v1",
                        },
                        "source_root": str(source),
                        "keychain_account": f"source.{binding_id}.database-keys",
                        "source_account_binding_id": binding_id,
                        "source_account_key": "not-derived-from-binding",
                    }
                ),
                encoding="utf-8",
            )
            os.chmod(path, 0o600)

            with self.assertRaisesRegex(RuntimeError, "inconsistent"):
                MacOSWeChatSettings.load(path)

    def test_key_import_rejects_a_file_that_changes_during_bounded_read(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "db_storage"
            source.mkdir()
            key_file = root / "keys.json"
            key_file.write_text("{}", encoding="utf-8")
            real_read = os.read
            changed = False

            def mutating_read(descriptor: int, amount: int) -> bytes:
                nonlocal changed
                chunk = real_read(descriptor, amount)
                if chunk and not changed:
                    changed = True
                    key_file.write_text('{"changed": true}', encoding="utf-8")
                return chunk

            with mock.patch(
                "sightglass.source.macos_wechat.keys.os.read",
                side_effect=mutating_read,
            ):
                with self.assertRaisesRegex(RuntimeError, "changed during import"):
                    import_verified_key_file(source, key_file)

    def test_key_import_rejects_a_hardlinked_input(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "db_storage"
            source.mkdir()
            original = root / "original.json"
            original.write_text("{}", encoding="utf-8")
            key_file = root / "keys.json"
            os.link(original, key_file)

            with self.assertRaisesRegex(RuntimeError, "private regular file"):
                import_verified_key_file(source, key_file)


if __name__ == "__main__":
    unittest.main()

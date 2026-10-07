from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from sightglass.cli import _parser, _source_image_key, main
from sightglass.runtime.config import ConfigStore
from sightglass.source.macos_wechat import keys as native_keys
from sightglass.source.macos_wechat.config import MacOSWeChatSettings

_FIXTURE_KEY = "0123456789abcdef0123456789abcdef"


class _StubSecrets:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def read(self, account: str) -> str:
        if account not in self.values:
            raise RuntimeError("native source keys are unavailable in Keychain")
        return self.values[account]

    def write(self, account: str, value: str) -> None:
        self.values[account] = value

    def delete(self, account: str) -> None:
        self.values.pop(account, None)


def _modules(secrets: _StubSecrets) -> dict[str, object]:
    return {
        "settings": MacOSWeChatSettings,
        "image_key_account": native_keys.image_decoder_keychain_account,
        "parse_image_key": native_keys.parse_image_decoder_key,
        "read_secret": secrets.read,
        "write_secret": secrets.write,
        "delete_secret": secrets.delete,
        "read_image_key_file": native_keys.read_image_decoder_key_file,
        "read_image_key_stream": native_keys.read_image_decoder_key_stream,
        "read_image_xor_key_file": native_keys.read_image_xor_key_file,
    }


class ImageKeyCliTests(unittest.TestCase):
    def test_optional_private_xor_material_round_trips_without_output(self) -> None:
        xor_file = self.root / "synthetic-xor.key"
        xor_file.write_text("35", encoding="ascii")
        xor_file.chmod(0o600)
        report = self._run(
            image_key_command="import", key_file=str(self.key_file), xor_key_file=str(xor_file)
        )
        account = native_keys.image_decoder_keychain_account(self.binding_id)
        stored = self.secrets.values[account]
        self.assertEqual(native_keys.parse_image_decoder_key(stored), bytes.fromhex(_FIXTURE_KEY))
        self.assertEqual(native_keys.parse_image_xor_key(stored), 0x35)
        self.assertNotIn("aes_key", json.dumps(report))
        self.assertNotIn(str(xor_file), json.dumps(report))
        xor_file.chmod(0o644)
        with self.assertRaises(RuntimeError):
            native_keys.read_image_xor_key_file(xor_file)

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        source = self.root / "db_storage"
        source.mkdir()
        self.binding_id = native_keys.new_source_account_binding_id()
        self.settings_path = self.root / "private" / "source.json"
        self.settings = MacOSWeChatSettings(
            instance_id="wxsrc_fixture",
            source_root=source.resolve(),
            keychain_account=f"source.{self.binding_id}.database-keys",
            source_account_binding_id=self.binding_id,
            source_account_key=native_keys.source_account_key(self.binding_id),
            bundle_id="com.tencent.xinWeChat",
            version="4.1.13",
            build="269602",
            architecture="arm64",
            profile_id="wechat-macos-4.1.13-269602-arm64-fixture",
        )
        self.settings.save(self.settings_path)
        self.key_file = self.root / "image.key"
        self.key_file.write_text(_FIXTURE_KEY, encoding="utf-8")
        os.chmod(self.key_file, 0o600)
        self.secrets = _StubSecrets()
        self.store = ConfigStore(self.root / "absent" / "config.json")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _args(self, **overrides: object) -> argparse.Namespace:
        values: dict[str, object] = {
            "image_key_command": "status",
            "settings": str(self.settings_path),
            "key_file": None,
            "stdin": False,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def _run(self, **overrides: object) -> dict[str, object]:
        with mock.patch("sightglass.cli._native_modules", return_value=_modules(self.secrets)):
            return _source_image_key(self._args(**overrides), self.store)

    def test_argv_cannot_carry_literal_key_material(self) -> None:
        with self.assertRaises(SystemExit):
            _parser().parse_args(["source", "image-key", "import", "--key", _FIXTURE_KEY])
        with self.assertRaises(SystemExit):
            _parser().parse_args(["source", "image-key", "import"])
        parsed = _parser().parse_args(
            ["source", "image-key", "import", "--key-file", "/tmp/example"]
        )
        self.assertFalse(hasattr(parsed, "key"))
        self.assertEqual(parsed.source_command, "image-key")

    def test_literal_key_argv_is_refused_without_echoing_it(self) -> None:
        for argv in (
            ["source", "image-key", "import", "--key", _FIXTURE_KEY],
            ["source", "image-key", "import", f"--key={_FIXTURE_KEY}"],
            ["source", "image-key", "import", "--key-file", "/tmp/nope", "--key", _FIXTURE_KEY],
        ):
            with self.subTest(argv=argv):
                stderr = io.StringIO()
                with contextlib.redirect_stderr(stderr):
                    with self.assertRaises(SystemExit) as caught:
                        main(argv)
                self.assertEqual(caught.exception.code, 2)
                self.assertNotIn(_FIXTURE_KEY, stderr.getvalue())
                self.assertIn("--key-file", stderr.getvalue())

    def test_import_enrolls_account_bound_identity_without_echoing_material(self) -> None:
        report = self._run(image_key_command="import", key_file=str(self.key_file))
        account = native_keys.image_decoder_keychain_account(self.binding_id)

        self.assertTrue(report["enrolled"])
        self.assertEqual(report["action"], "import")
        self.assertEqual(self.secrets.values[account], _FIXTURE_KEY)
        self.assertEqual(
            MacOSWeChatSettings.load(self.settings_path).image_keychain_account,
            account,
        )
        serialized = json.dumps(report)
        self.assertNotIn(_FIXTURE_KEY, serialized)
        self.assertNotIn(_FIXTURE_KEY[:8], serialized)
        self.assertNotIn("image.key", serialized)

    def test_status_and_remove_are_content_free(self) -> None:
        account = native_keys.image_decoder_keychain_account(self.binding_id)
        before = self._run(image_key_command="status")
        self.assertFalse(before["enrolled"])

        self._run(image_key_command="import", key_file=str(self.key_file))
        enrolled = self._run(image_key_command="status")
        self.assertTrue(enrolled["enrolled"])

        removed = self._run(image_key_command="remove")
        self.assertFalse(removed["enrolled"])
        self.assertTrue(removed["removed"])
        self.assertNotIn(account, self.secrets.values)
        self.assertIsNone(MacOSWeChatSettings.load(self.settings_path).image_keychain_account)
        for report in (before, enrolled, removed):
            serialized = json.dumps(report)
            self.assertNotIn(_FIXTURE_KEY, serialized)
            self.assertNotIn(_FIXTURE_KEY[:8], serialized)
            self.assertNotIn("keychain_account", report)
            self.assertNotIn("settings_path", report)

    def test_status_accepts_an_installed_legacy_account_binding(self) -> None:
        legacy_binding = "legacy-legacy-account-key"
        replace(
            self.settings,
            source_account_binding_id=legacy_binding,
            source_account_key="legacy-account-key",
        ).save(self.settings_path)

        report = self._run(image_key_command="status")

        self.assertFalse(report["enrolled"])
        self.assertTrue(report["account_bound"])
        serialized = json.dumps(report)
        self.assertNotIn(legacy_binding, serialized)
        self.assertNotIn("legacy-account-key", serialized)

    def test_import_rejects_wrong_key_material_without_storing_it(self) -> None:
        self.key_file.write_text("not-a-key", encoding="utf-8")
        os.chmod(self.key_file, 0o600)

        with self.assertRaises(RuntimeError):
            self._run(image_key_command="import", key_file=str(self.key_file))
        self.assertEqual(self.secrets.values, {})

    def test_import_rejects_world_readable_key_file(self) -> None:
        os.chmod(self.key_file, 0o644)

        with self.assertRaises(RuntimeError):
            self._run(image_key_command="import", key_file=str(self.key_file))
        self.assertEqual(self.secrets.values, {})


if __name__ == "__main__":
    unittest.main()

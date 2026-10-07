from __future__ import annotations

import os
import struct
import tempfile
import unittest
import zlib
from pathlib import Path
from unittest.mock import patch

from cryptography.hazmat.decrepit.ciphers.modes import CFB
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from sightglass.resources.sticker import derive_wechat_sticker_key
from sightglass.source.macos_wechat.config import MacOSWeChatSettings
from sightglass.source.macos_wechat.sticker_key import (
    _sticker_ciphertext_prefixes,
    load_sticker_decoder_key,
)


def _varint(value: int) -> bytes:
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def _string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return _varint(len(encoded)) + encoded


class NativeStickerKeyTests(unittest.TestCase):
    _PROFILE = "wechat-macos-4.1.13-269602-arm64-v1"
    _KEY_OFFSET = 0x84C6830

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.account_root = self.root / "xwechat_files" / "synthetic-account"
        self.source_root = self.account_root / "db_storage"
        self.source_root.mkdir(parents=True)
        self.global_config = (
            self.root / "xwechat_files" / "all_users" / "config" / "global_config"
        )
        self.global_config.parent.mkdir(parents=True)
        self.app_path = self.root / "WeChat.app"
        self.binary_path = self.app_path / "Contents" / "Resources" / "wechat.dylib"
        self.binary_path.parent.mkdir(parents=True)
        self.mmkv_key = b"synthetic-mmkv!!"
        self.uin = 12_345_678
        self.username = "synthetic-user"
        self.settings = MacOSWeChatSettings(
            instance_id="synthetic-instance",
            source_root=self.source_root,
            keychain_account="synthetic-keychain",
            source_account_binding_id="synthetic-binding",
            source_account_key="synthetic-account-key",
            bundle_id="com.tencent.xinWeChat",
            version="4.1.13",
            build="269602",
            architecture="arm64",
            profile_id=self._PROFILE,
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write_binary(self) -> None:
        slice_offset = 0x4000
        slice_size = self._KEY_OFFSET + len(self.mmkv_key)
        with self.binary_path.open("wb") as handle:
            handle.write(
                struct.pack(
                    ">IIiiIII", 0xCAFEBABE, 1, 0x0100000C, 0, slice_offset, slice_size, 14
                )
            )
            handle.seek(slice_offset)
            handle.write(struct.pack("<I", 0xFEEDFACF))
            handle.seek(slice_offset + self._KEY_OFFSET)
            handle.write(self.mmkv_key)

    def _write_mmkv(self, *, valid_crc: bool = True) -> None:
        entries = {
            b"mmkv_key_latest_login_uin": _varint(self.uin),
            b"mmkv_key_latest_login_username": _string(self.username),
            b"mmkv_key_user_name": _string(self.username),
            b"synthetic_unrelated_value": _string("must-not-affect-derivation"),
        }
        body = bytearray(_varint(0x200000))
        for key, value in entries.items():
            body.extend(_varint(len(key)))
            body.extend(key)
            body.extend(_varint(len(value)))
            body.extend(value)
        vector = bytes.fromhex("ffeeddccbbaa99887766554433221100")
        encryptor = Cipher(algorithms.AES(self.mmkv_key), CFB(vector)).encryptor()
        encrypted = encryptor.update(bytes(body)) + encryptor.finalize()
        self.global_config.write_bytes(b"\x00\x00\x00\x00" + encrypted)
        crc = zlib.crc32(encrypted) & 0xFFFFFFFF
        if not valid_crc:
            crc ^= 1
        Path(f"{self.global_config}.crc").write_bytes(
            struct.pack("<III16sI", crc, 4, 1, vector, len(encrypted)) + bytes(96)
        )

    def _write_sticker(self) -> bytes:
        sticker_key = derive_wechat_sticker_key(str(self.uin), self.username)
        plaintext = b"GIF89a" + b"synthetic account-bound sticker"
        padder = padding.PKCS7(128).padder()
        padded = padder.update(plaintext) + padder.finalize()
        encryptor = Cipher(
            algorithms.AES(sticker_key), modes.CBC(sticker_key)
        ).encryptor()
        ciphertext = encryptor.update(padded) + encryptor.finalize()
        digest = __import__("hashlib").md5(plaintext).hexdigest()  # noqa: S324
        target = self.account_root / "business" / "emoticon" / "Persist" / digest[:2]
        target.mkdir(parents=True)
        (target / digest).write_bytes(ciphertext)
        return sticker_key

    def test_derives_and_validates_the_selected_accounts_sticker_key(self) -> None:
        self._write_binary()
        self._write_mmkv()
        expected = self._write_sticker()

        self.assertEqual(
            load_sticker_decoder_key(self.settings, app_path=self.app_path),
            expected,
        )

    def test_corrupt_mmkv_snapshot_fails_closed(self) -> None:
        self._write_binary()
        self._write_mmkv(valid_crc=False)
        self._write_sticker()

        self.assertIsNone(
            load_sticker_decoder_key(self.settings, app_path=self.app_path)
        )

    def test_unvalidated_account_candidate_fails_closed(self) -> None:
        self._write_binary()
        self._write_mmkv()

        self.assertIsNone(
            load_sticker_decoder_key(self.settings, app_path=self.app_path)
        )

    def test_symlinked_mmkv_input_fails_closed(self) -> None:
        self._write_binary()
        self._write_mmkv()
        self._write_sticker()
        replacement = self.global_config.with_name("replacement")
        os.replace(self.global_config, replacement)
        os.symlink(replacement, self.global_config)

        self.assertIsNone(
            load_sticker_decoder_key(self.settings, app_path=self.app_path)
        )

    def _write_raw_entry(self, prefix: str, name: str) -> None:
        directory = (
            self.account_root / "business" / "emoticon" / "Persist" / prefix
        )
        directory.mkdir(parents=True, exist_ok=True)
        (directory / name).write_bytes(b"x" * 32)

    def test_scan_budget_counts_junk_and_never_enumerates_the_tail(self) -> None:
        for index in range(3):
            self._write_raw_entry("00", f"junk-{index}")
        self._write_raw_entry("01", "01" + "a" * 30)

        with patch(
            "sightglass.source.macos_wechat.sticker_key._MAX_STICKER_SCAN_ENTRIES", 3
        ):
            self.assertEqual(list(_sticker_ciphertext_prefixes(self.source_root)), [])

    def test_scan_budget_stops_before_tail_valid_entries(self) -> None:
        for index in range(4):
            self._write_raw_entry("00", f"00{index:030x}")

        with patch(
            "sightglass.source.macos_wechat.sticker_key._MAX_STICKER_SCAN_ENTRIES", 2
        ):
            self.assertEqual(
                len(list(_sticker_ciphertext_prefixes(self.source_root))), 2
            )

    def test_scan_never_follows_a_symlinked_prefix_directory(self) -> None:
        persist = self.account_root / "business" / "emoticon" / "Persist"
        persist.mkdir(parents=True)
        outside = self.root / "outside-prefix"
        outside.mkdir()
        (outside / ("00" + "a" * 30)).write_bytes(b"x" * 32)
        os.symlink(outside, persist / "00")

        self.assertEqual(list(_sticker_ciphertext_prefixes(self.source_root)), [])


if __name__ == "__main__":
    unittest.main()

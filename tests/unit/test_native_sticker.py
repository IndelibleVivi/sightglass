from __future__ import annotations

import hashlib
import os
import tempfile
import unittest
from pathlib import Path

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.source.macos_wechat.resources import NativeResourceResolver


class NativeStickerResolutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.account_root = Path(self.temp.name)
        self.source_root = self.account_root / "db_storage"
        self.source_root.mkdir()
        self.key = bytes.fromhex("00112233445566778899aabbccddeeff")
        self.plaintext = b"GIF89a" + b"synthetic local sticker"
        self.digest = hashlib.md5(self.plaintext).hexdigest()  # noqa: S324

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _resolver(self, *, key: bytes | None) -> NativeResourceResolver:
        return NativeResourceResolver(
            self.source_root,
            "synthetic-account-binding",
            reader_timezone="Asia/Singapore",
            sticker_decoder_key=key,
        )

    def _encrypt(self, plaintext: bytes) -> bytes:
        padder = padding.PKCS7(128).padder()
        padded = padder.update(plaintext) + padder.finalize()
        encryptor = Cipher(algorithms.AES(self.key), modes.CBC(self.key)).encryptor()
        return encryptor.update(padded) + encryptor.finalize()

    def _cache_path(self, digest: str | None = None) -> Path:
        selected = digest or self.digest
        directory = (
            self.account_root
            / "business"
            / "emoticon"
            / "Persist"
            / selected[:2]
        )
        directory.mkdir(parents=True, exist_ok=True)
        return directory / selected

    def _thumbnail_path(self, digest: str | None = None) -> Path:
        selected = digest or self.digest
        directory = (
            self.account_root
            / "business"
            / "emoticon"
            / "Thumb"
            / selected[:2]
        )
        directory.mkdir(parents=True, exist_ok=True)
        return directory / f"{selected}.thumb"

    def _resource(
        self,
        resolver: NativeResourceResolver,
        *,
        raw_content: str | None = None,
    ):
        resources = resolver.resources_for_message(
            source_message_id="synthetic-message",
            conversation_source_id="synthetic-conversation",
            local_id=47,
            create_time=1_725_000_047,
            local_type=47,
            raw_content=(
                raw_content
                if raw_content is not None
                else f'<msg><emoji md5="{self.digest}" /></msg>'
            ),
            packed_info_data=None,
        )
        self.assertEqual(len(resources), 1)
        return resources[0]

    def _read(self, resolver: NativeResourceResolver, resource):
        assert resource.source_resource_key is not None
        return resolver.read(resource.source_resource_key, max_bytes=64 * 1024)

    def test_decrypts_message_bound_local_sticker(self) -> None:
        self._cache_path().write_bytes(self._encrypt(self.plaintext))
        resolver = self._resolver(key=self.key)

        resource = self._resource(resolver)

        self.assertEqual(resource.kind, "sticker")
        self.assertEqual(resource.availability, "local_available")
        self.assertIsNone(resource.declared_hash)
        data, variant = self._read(resolver, resource)
        self.assertEqual(data, self.plaintext)
        self.assertEqual(variant, "original")

    def test_local_sticker_without_decoder_key_is_explicit(self) -> None:
        self._cache_path().write_bytes(self._encrypt(self.plaintext))
        resolver = self._resolver(key=None)

        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "key_missing")
        with self.assertRaises(SightglassError) as caught:
            self._read(resolver, resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)
        self.assertEqual(caught.exception.details["reason"], "sticker_decoder_key_missing")

    def test_missing_sticker_cache_entry_is_a_bounded_absence(self) -> None:
        resource = self._resource(self._resolver(key=self.key))

        self.assertEqual(resource.availability, "missing")

    def test_sticker_requires_a_valid_message_digest(self) -> None:
        resource = self._resource(
            self._resolver(key=self.key),
            raw_content='<msg><emoji md5="not-a-digest" /></msg>',
        )

        self.assertEqual(resource.availability, "metadata_only")
        self.assertIsNone(resource.source_resource_key)
        self.assertIsNone(resource.declared_hash)

    def test_sticker_cache_link_escape_fails_closed(self) -> None:
        outside = self.account_root / "outside"
        outside.write_bytes(self._encrypt(self.plaintext))
        os.symlink(outside, self._cache_path())

        resource = self._resource(self._resolver(key=self.key))

        self.assertEqual(resource.availability, "blocked_by_policy")

    def test_message_digest_is_a_locator_not_a_plaintext_integrity_claim(self) -> None:
        self._cache_path().write_bytes(self._encrypt(b"GIF89a different sticker"))
        resolver = self._resolver(key=self.key)
        resource = self._resource(resolver)

        data, variant = self._read(resolver, resource)

        self.assertEqual(data, b"GIF89a different sticker")
        self.assertEqual(variant, "original")

    def test_opaque_original_falls_back_to_message_bound_thumbnail(self) -> None:
        self._cache_path().write_bytes(self._encrypt(b"opaque inner sticker payload"))
        preview = b"\x89PNG\r\n\x1a\nsynthetic sticker thumbnail"
        self._thumbnail_path().write_bytes(self._encrypt(preview))
        resolver = self._resolver(key=self.key)

        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "preview_only")
        data, variant = self._read(resolver, resource)
        self.assertEqual(data, preview)
        self.assertEqual(variant, "thumbnail")

    def test_wxgf_container_is_a_supported_source_original(self) -> None:
        wxgf = b"wxgf synthetic header\x00\x00\x00\x01synthetic HEVC payload"
        self._cache_path().write_bytes(self._encrypt(wxgf))
        self._thumbnail_path().write_bytes(
            self._encrypt(b"\x89PNG\r\n\x1a\nsynthetic sticker thumbnail")
        )
        resolver = self._resolver(key=self.key)

        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "local_available")
        data, variant = self._read(resolver, resource)
        self.assertEqual(data, wxgf)
        self.assertEqual(variant, "original")

    def test_source_original_wins_when_thumbnail_is_also_present(self) -> None:
        self._cache_path().write_bytes(self._encrypt(self.plaintext))
        self._thumbnail_path().write_bytes(
            self._encrypt(b"\x89PNG\r\n\x1a\nsynthetic sticker thumbnail")
        )
        resolver = self._resolver(key=self.key)

        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "local_available")
        data, variant = self._read(resolver, resource)
        self.assertEqual(data, self.plaintext)
        self.assertEqual(variant, "original")

    def test_thumbnail_without_persist_entry_is_still_a_preview(self) -> None:
        preview = b"\xff\xd8\xffsynthetic sticker thumbnail"
        self._thumbnail_path().write_bytes(self._encrypt(preview))
        resolver = self._resolver(key=self.key)

        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "preview_only")
        data, variant = self._read(resolver, resource)
        self.assertEqual(data, preview)
        self.assertEqual(variant, "thumbnail")

    def test_opaque_sticker_without_thumbnail_stays_metadata_only(self) -> None:
        self._cache_path().write_bytes(self._encrypt(b"opaque inner sticker payload"))
        resolver = self._resolver(key=self.key)

        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "metadata_only")
        with self.assertRaises(SightglassError) as caught:
            self._read(resolver, resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_DECODE_FAILED)

    def test_thumbnail_link_escape_fails_closed(self) -> None:
        self._cache_path().write_bytes(self._encrypt(b"opaque inner sticker payload"))
        outside = self.account_root / "outside-thumbnail"
        outside.write_bytes(self._encrypt(b"\xff\xd8\xffoutside thumbnail"))
        os.symlink(outside, self._thumbnail_path())
        resolver = self._resolver(key=self.key)

        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "blocked_by_policy")
        with self.assertRaises(SightglassError) as caught:
            self._read(resolver, resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)


if __name__ == "__main__":
    unittest.main()

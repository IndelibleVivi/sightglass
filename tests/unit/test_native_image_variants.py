from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.source.macos_wechat.image_index import NativeImageEntry, packed_file_stem
from sightglass.source.macos_wechat.resources import NativeResourceResolver
from sightglass.source.synthetic import SYNTHETIC_IMAGE_KEY, _png_bytes, _v2_image_bytes

_READER_TIMEZONE = "Asia/Singapore"
_CONVERSATION_SOURCE_ID = "synthetic-conversation"
_CREATE_TIME = 1_725_000_031  # 2024-08 in Asia/Singapore
_MONTH = "2024-08"
_LOCAL_ID = 31
_XML_DIGEST = "f" * 32


class _FakeImageIndex:
    """Stand-in for the optional mapping databases; records the evidence it received."""

    def __init__(self, entry: NativeImageEntry | None = None) -> None:
        self.entry = entry
        self.calls: list[dict[str, Any]] = []

    def entry_for(self, **evidence: Any) -> NativeImageEntry | None:
        self.calls.append(evidence)
        return self.entry


class NativeImageResolutionTests(unittest.TestCase):
    """Native image payloads are resolved by mapping evidence, never by the message XML."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        # The native media tree lives beside the account's ``db_storage`` root,
        # which is exactly how the resolver walks it (``..`` from ``source_root``).
        self.media_root = Path(self.temp.name)
        self.source_root = self.media_root / "db_storage"
        self.source_root.mkdir(parents=True)
        self.image = _png_bytes()
        # The mapping hash names the stored file. It is deliberately unrelated to the
        # payload bytes and to the digest carried in the message XML.
        self.stem = "1" * 32
        self.attachment = self.image + b"stored attachment bytes"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _conversation_hash(self) -> str:
        return hashlib.md5(_CONVERSATION_SOURCE_ID.encode()).hexdigest()

    def _image_directory(self) -> Path:
        directory = (
            self.media_root
            / "msg"
            / "attach"
            / self._conversation_hash()
            / _MONTH
            / "Img"
        )
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _thumbnail_directory(self) -> Path:
        directory = (
            self.media_root
            / "cache"
            / _MONTH
            / "Message"
            / self._conversation_hash()
            / "Thumb"
        )
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _cache_thumbnail(self, data: bytes, *, local_id: int = _LOCAL_ID) -> Path:
        target = self._thumbnail_directory() / f"{local_id}_{_CREATE_TIME}_thumb.jpg"
        target.write_bytes(data)
        return target

    def _resolver(
        self,
        entry: NativeImageEntry | None = None,
        *,
        image_decoder_key: bytes | None = None,
        index: _FakeImageIndex | None = None,
    ) -> NativeResourceResolver:
        return NativeResourceResolver(
            self.source_root,
            "synthetic-account-binding",
            reader_timezone=_READER_TIMEZONE,
            image_decoder_key=image_decoder_key,
            image_index=index if index is not None else _FakeImageIndex(entry),
        )

    def _resource(self, resolver: NativeResourceResolver) -> Any:
        resources = resolver.resources_for_message(
            source_message_id="synthetic-message",
            conversation_source_id=_CONVERSATION_SOURCE_ID,
            local_id=_LOCAL_ID,
            create_time=_CREATE_TIME,
            local_type=3,
            raw_content=f'<msg><img md5="{_XML_DIGEST}" /></msg>',
            packed_info_data=None,
        )
        self.assertEqual(len(resources), 1)
        return resources[0]

    def _read(self, resolver: NativeResourceResolver, resource: Any) -> tuple[bytes, str]:
        assert resource.source_resource_key is not None
        return resolver.read(resource.source_resource_key, max_bytes=64 * 1024)

    def _mapped_entry(self) -> NativeImageEntry:
        return NativeImageEntry(
            directory=("msg", "attach", self._conversation_hash(), _MONTH, "Img"),
            stem=self.stem,
        )

    def test_message_xml_digest_is_never_used_as_a_stored_file_name(self) -> None:
        # A stored entry whose name happens to equal the message digest is not the
        # message's payload and must never be served for it.
        (self._image_directory() / f"{_XML_DIGEST}.dat").write_bytes(self.attachment)

        resource = self._resource(self._resolver())

        self.assertEqual(resource.kind, "image")
        self.assertEqual(resource.availability, "metadata_only")
        self.assertIsNone(resource.declared_hash)
        with self.assertRaises(SightglassError) as caught:
            self._read(self._resolver(), resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)
        self.assertEqual(caught.exception.details["reason"], "resource_missing")

    def test_mapping_receives_the_exact_message_position_evidence(self) -> None:
        index = _FakeImageIndex()

        self._resource(self._resolver(index=index))

        self.assertEqual(
            index.calls,
            [
                {
                    "conversation_source_id": _CONVERSATION_SOURCE_ID,
                    "local_id": _LOCAL_ID,
                    "month": _MONTH,
                    "xml_digest": _XML_DIGEST,
                }
            ],
        )

    def test_cache_thumbnail_is_resolved_for_the_exact_message_position(self) -> None:
        self._cache_thumbnail(b"\xff\xd8\xff cached preview")
        self._cache_thumbnail(b"\xff\xd8\xff another message", local_id=_LOCAL_ID + 1)

        resolver = self._resolver()
        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "preview_only")
        data, variant = self._read(resolver, resource)
        self.assertEqual(variant, "thumbnail")
        self.assertEqual(data, b"\xff\xd8\xff cached preview")

    def test_cache_thumbnail_link_escape_fails_closed(self) -> None:
        outside = self.media_root / "outside-thumb.jpg"
        outside.write_bytes(b"\xff\xd8\xff outside")
        directory = self._thumbnail_directory()
        os.symlink(outside, directory / f"{_LOCAL_ID}_{_CREATE_TIME}_thumb.jpg")

        resolver = self._resolver()
        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "blocked_by_policy")
        with self.assertRaises(SightglassError) as caught:
            self._read(resolver, resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)

    def test_mapped_full_entry_wins_over_mid_and_thumbnail(self) -> None:
        directory = self._image_directory()
        (directory / f"{self.stem}_h.dat").write_bytes(b"full resolution payload")
        (directory / f"{self.stem}.dat").write_bytes(b"mid resolution payload")
        (directory / f"{self.stem}_t.dat").write_bytes(b"\xff\xd8\xff thumbnail payload")
        self._cache_thumbnail(b"\xff\xd8\xff cached preview")

        resolver = self._resolver(self._mapped_entry())
        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "local_available")
        data, variant = self._read(resolver, resource)
        self.assertEqual(variant, "original")
        self.assertEqual(data, b"full resolution payload")

    def test_mapped_mid_entry_is_used_when_no_full_entry_exists(self) -> None:
        directory = self._image_directory()
        (directory / f"{self.stem}.dat").write_bytes(b"mid resolution payload")

        resolver = self._resolver(self._mapped_entry())
        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "local_available")
        data, variant = self._read(resolver, resource)
        self.assertEqual(variant, "original")
        self.assertEqual(data, b"mid resolution payload")

    def test_mapped_thumbnail_entry_is_only_a_preview(self) -> None:
        (self._image_directory() / f"{self.stem}_t.dat").write_bytes(b"thumbnail payload")

        resolver = self._resolver(self._mapped_entry())
        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "preview_only")
        data, variant = self._read(resolver, resource)
        self.assertEqual(variant, "thumbnail")
        self.assertEqual(data, b"thumbnail payload")

    def test_unclassifiable_preview_entries_with_distinct_bytes_fail_closed(self) -> None:
        directory = self._image_directory()
        (directory / f"{self.stem}_t.dat").write_bytes(self.image)
        (directory / f"{self.stem}_M.dat").write_bytes(self.image + b"\x00")

        resolver = self._resolver(self._mapped_entry())
        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "metadata_only")
        with self.assertRaises(SightglassError) as caught:
            self._read(resolver, resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)
        self.assertEqual(caught.exception.details["reason"], "resource_candidates_ambiguous")

    def test_unclassifiable_preview_entries_with_equivalent_bytes_reconcile(self) -> None:
        directory = self._image_directory()
        for suffix in ("_t.dat", "_M.dat", "_t_M.dat"):
            (directory / f"{self.stem}{suffix}").write_bytes(self.image)

        resolver = self._resolver(self._mapped_entry())
        resource = self._resource(resolver)

        # More than one entry makes the cheap descriptor refuse to select one, while
        # the read reconciles byte-equivalent entries exactly like file candidates.
        self.assertEqual(resource.availability, "metadata_only")
        data, variant = self._read(resolver, resource)
        self.assertEqual(variant, "thumbnail")
        self.assertEqual(data, self.image)

    def test_mapped_symlink_entry_fails_closed(self) -> None:
        outside = self.media_root / "outside.png"
        outside.write_bytes(self.image)
        os.symlink(outside, self._image_directory() / f"{self.stem}.dat")

        resolver = self._resolver(self._mapped_entry())
        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "blocked_by_policy")
        with self.assertRaises(SightglassError) as caught:
            self._read(resolver, resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)

    def test_mapped_hardlink_entry_fails_closed(self) -> None:
        outside = self.media_root / "outside.png"
        outside.write_bytes(self.image)
        os.link(outside, self._image_directory() / f"{self.stem}_h.dat")

        resolver = self._resolver(self._mapped_entry())
        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "blocked_by_policy")
        with self.assertRaises(SightglassError) as caught:
            self._read(resolver, resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)

    def test_mapping_outside_the_attach_store_is_refused(self) -> None:
        elsewhere = self.media_root / "msg" / "file" / _MONTH
        elsewhere.mkdir(parents=True)
        (elsewhere / f"{self.stem}.dat").write_bytes(self.attachment)
        entry = NativeImageEntry(
            directory=("msg", "file", _MONTH), stem=self.stem
        )

        resolver = self._resolver(entry)
        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "metadata_only")
        with self.assertRaises(SightglassError) as caught:
            self._read(resolver, resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)

    def test_mapping_with_an_escaping_directory_component_is_refused(self) -> None:
        entry = NativeImageEntry(directory=("msg", "attach", "..", _MONTH, "Img"), stem=self.stem)

        resolver = self._resolver(entry)
        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "metadata_only")
        with self.assertRaises(SightglassError) as caught:
            self._read(resolver, resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)

    def test_v2_mapped_entry_requires_and_then_uses_the_source_key(self) -> None:
        (self._image_directory() / f"{self.stem}_h.dat").write_bytes(
            _v2_image_bytes(self.image)
        )

        locked = self._resolver(self._mapped_entry())
        resource = self._resource(locked)
        self.assertEqual(resource.availability, "key_missing")
        with self.assertRaises(SightglassError) as caught:
            self._read(locked, resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)
        self.assertEqual(caught.exception.details["reason"], "image_decoder_key_missing")

        unlocked = self._resolver(
            self._mapped_entry(), image_decoder_key=SYNTHETIC_IMAGE_KEY
        )
        unlocked_resource = self._resource(unlocked)
        self.assertEqual(unlocked_resource.availability, "local_available")
        data, variant = self._read(unlocked, unlocked_resource)
        self.assertEqual(variant, "original")
        self.assertEqual(data, self.image)

    def test_locked_mapped_entry_serves_a_decodable_message_positioned_preview(self) -> None:
        # The mapped attach entry still needs its unenrolled V2 decoder key, so the
        # exact message-positioned preview answers instead. The original stays
        # explicitly unavailable and no ciphertext is ever returned.
        (self._image_directory() / f"{self.stem}_h.dat").write_bytes(
            _v2_image_bytes(self.image)
        )
        self._cache_thumbnail(b"\xff\xd8\xff cached preview")

        resolver = self._resolver(self._mapped_entry())
        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "preview_only")
        data, variant = self._read(resolver, resource)
        self.assertEqual(variant, "thumbnail")
        self.assertEqual(data, b"\xff\xd8\xff cached preview")

    def test_locked_mapped_entry_keeps_key_missing_without_a_decodable_preview(self) -> None:
        (self._image_directory() / f"{self.stem}_h.dat").write_bytes(
            _v2_image_bytes(self.image)
        )
        # A preview that needs the same unenrolled key is not a usable fallback.
        self._cache_thumbnail(_v2_image_bytes(self.image))

        resolver = self._resolver(self._mapped_entry())
        resource = self._resource(resolver)

        self.assertEqual(resource.availability, "key_missing")
        with self.assertRaises(SightglassError) as caught:
            self._read(resolver, resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)
        self.assertEqual(caught.exception.details["reason"], "image_decoder_key_missing")

    def test_descriptor_publishes_no_path_entry_name_or_message_digest(self) -> None:
        (self._image_directory() / f"{self.stem}_h.dat").write_bytes(self.attachment)

        resource = self._resource(self._resolver(self._mapped_entry()))
        descriptor = json.dumps(resource.as_dict(), ensure_ascii=False)

        self.assertTrue(str(resource.source_resource_key).startswith("nres1."))
        self.assertNotIn(str(self.media_root), descriptor)
        self.assertNotIn(self.stem, descriptor)
        self.assertNotIn(_XML_DIGEST, descriptor)
        self.assertNotIn(".dat", descriptor)
        self.assertNotIn("Thumb", descriptor)


class PackedInfoTests(unittest.TestCase):
    """The mapping envelope yields a file name only when it is unambiguous."""

    def test_one_nested_file_hash_is_extracted(self) -> None:
        stem = "0123456789abcdef" * 2
        envelope = bytes([0x12, 0x22, 0x0A, 0x20]) + stem.encode()

        self.assertEqual(packed_file_stem(envelope), stem)

    def test_several_distinct_file_hashes_stay_unmapped(self) -> None:
        first = bytes([0x12, 0x22, 0x0A, 0x20]) + (b"a" * 32)
        second = bytes([0x12, 0x22, 0x0A, 0x20]) + (b"b" * 32)

        self.assertIsNone(packed_file_stem(first + second))

    def test_a_payload_that_is_not_a_file_hash_stays_unmapped(self) -> None:
        self.assertIsNone(packed_file_stem(bytes([0x0A, 0x04]) + b"text"))
        self.assertIsNone(packed_file_stem(None))
        self.assertIsNone(packed_file_stem(b"x" * (512 * 1024 + 1)))


if __name__ == "__main__":
    unittest.main()

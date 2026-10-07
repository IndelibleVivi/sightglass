from __future__ import annotations

import hashlib
import json
import os
import struct
import unittest

from sightglass.model.observation_codec import (
    CODEC_RAW,
    CODEC_ZLIB,
    FORMAT_VERSION,
    MAGIC,
    ObservationCodecError,
    decode_observation_bytes,
    decode_observation_text,
    encode_observation,
    is_encoded_observation,
)

_HEADER = struct.Struct(">4sBBII")


def _header(blob: bytes) -> tuple[bytes, int, int, int, int]:
    return _HEADER.unpack_from(blob, 0)


class ObservationCodecTests(unittest.TestCase):
    def test_text_roundtrips_losslessly(self) -> None:
        payload = json.dumps(
            {"sender": {"surface_label": "示例甲"}, "text": "line one\nline two �1"},
            ensure_ascii=False,
        )
        blob = encode_observation(payload)
        self.assertTrue(is_encoded_observation(blob))
        self.assertEqual(decode_observation_text(blob), payload)

    def test_bytes_roundtrip_matches_sha256_of_uncompressed_payload(self) -> None:
        payload = ("repeat " * 200).encode("utf-8")
        blob = encode_observation(payload)
        decoded = decode_observation_bytes(blob)
        self.assertEqual(decoded, payload)
        self.assertEqual(
            hashlib.sha256(decoded).hexdigest(), hashlib.sha256(payload).hexdigest()
        )

    def test_incompressible_payload_uses_raw_codec_and_still_verifies(self) -> None:
        payload = os.urandom(1024)
        blob = encode_observation(payload)
        _magic, version, codec, length, _crc = _header(blob)
        self.assertEqual(version, FORMAT_VERSION)
        self.assertEqual(codec, CODEC_RAW)
        self.assertEqual(length, len(payload))
        self.assertEqual(decode_observation_bytes(blob), payload)

    def test_compressible_payload_uses_zlib_codec(self) -> None:
        payload = json.dumps({"text": "same " * 300}).encode("utf-8")
        _magic, _version, codec, _length, _crc = _header(encode_observation(payload))
        self.assertEqual(codec, CODEC_ZLIB)

    def test_legacy_text_rows_are_returned_verbatim(self) -> None:
        legacy = '{"sender":{"identity_keys":[]}}'
        self.assertEqual(decode_observation_text(legacy), legacy)
        self.assertFalse(is_encoded_observation(legacy))

    def test_memoryview_blobs_are_accepted(self) -> None:
        blob = encode_observation('{"a":1}')
        self.assertTrue(is_encoded_observation(memoryview(blob)))
        self.assertEqual(decode_observation_text(memoryview(blob)), '{"a":1}')

    def test_corrupt_payloads_fail_closed(self) -> None:
        blob = encode_observation('{"text":"hello world","padding":"' + "x" * 200 + '"}')
        mutations = {
            "bad magic": b"XXXX" + blob[4:],
            "truncated header": blob[:6],
            "bad version": blob[:4] + bytes([FORMAT_VERSION + 1]) + blob[5:],
            "bad codec": blob[:5] + bytes([0x7F]) + blob[6:],
            "bad length": blob[:6] + struct.pack(">I", 999_999) + blob[10:],
            "flipped body byte": blob[:-1] + bytes([blob[-1] ^ 0x01]),
            "corrupt crc": blob[:10] + struct.pack(">I", 0) + blob[14:],
            "truncated stream": blob[:-2],
            "trailing bytes": blob + b"junk",
            "undersized declared length": blob[:6] + struct.pack(">I", 1) + blob[10:],
        }
        for name, mutated in mutations.items():
            with self.subTest(name=name):
                self.assertNotEqual(mutated, blob)
                with self.assertRaises(ObservationCodecError):
                    decode_observation_text(mutated)

    def test_invalid_utf8_fails_text_decode(self) -> None:
        with self.assertRaises(ObservationCodecError):
            decode_observation_text(encode_observation(b"\xff"))

    def test_non_blob_non_text_value_is_rejected(self) -> None:
        with self.assertRaises(ObservationCodecError):
            decode_observation_text(1234)  # type: ignore[arg-type]

    def test_magic_prevents_accidental_double_encoding_mismatch(self) -> None:
        # A legacy JSON document never carries the canonical magic prefix.
        self.assertFalse(is_encoded_observation(b'{"m":"SGOC"}'))
        self.assertNotEqual(MAGIC, b'{"m"')


if __name__ == "__main__":
    unittest.main()

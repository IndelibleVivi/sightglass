from __future__ import annotations

import secrets
import struct
import unittest
import zlib

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.resources.v2 import V2_MAGIC, decode_wechat_v2_image


def png(pixel: bytes) -> bytes:
    def chunk(kind: bytes, body: bytes) -> bytes:
        return (
            struct.pack(">I", len(body))
            + kind
            + body
            + struct.pack(">I", zlib.crc32(kind + body) & 0xFFFFFFFF)
        )

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(b"\0" + pixel))
        + chunk(b"IEND", b"")
    )


def envelope(data: bytes, aes_key: bytes, xor: int) -> bytes:
    size = 32
    padding = 16 - size % 16
    encryptor = Cipher(algorithms.AES(aes_key), modes.ECB()).encryptor()
    encrypted = encryptor.update(data[:size] + bytes([padding]) * padding) + encryptor.finalize()
    tail = data[-16:]
    return (
        V2_MAGIC
        + struct.pack("<II", size, len(tail))
        + b"\0"
        + encrypted
        + data[size:-16]
        + bytes(value ^ xor for value in tail)
    )


class V2DecoderTests(unittest.TestCase):
    def test_account_specific_xor_restores_exact_png_bytes(self):
        key = secrets.token_bytes(16)
        xor = secrets.randbelow(255)
        if xor == 0x88:
            xor = 255
        source = png(b"\x10\x20\x30\xff")
        encoded = envelope(source, key, xor)
        self.assertEqual(decode_wechat_v2_image(encoded, key), source)
        with self.assertRaises(SightglassError):
            decode_wechat_v2_image(encoded, key, xor_key=(xor + 1) % 256)

    def test_black_and_transparent_images_are_valid_not_quality_rejected(self):
        key = secrets.token_bytes(16)
        for pixel in (b"\0\0\0\xff", b"\0\0\0\0"):
            source = png(pixel)
            self.assertEqual(decode_wechat_v2_image(envelope(source, key, 37), key), source)

    def test_truncated_tail_does_not_become_tolerant_gray_preview(self):
        key = secrets.token_bytes(16)
        encoded = envelope(png(b"\x10\x20\x30\xff"), key, 73)
        for truncated in (encoded[:-1], encoded[:-9]):
            with self.assertRaises(SightglassError) as caught:
                decode_wechat_v2_image(truncated, key)
            self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_DECODE_FAILED)

    def test_unknown_footer_requires_account_xor_material(self):
        key = secrets.token_bytes(16)
        source = b"wxgf" + bytes(range(76))
        encoded = envelope(source, key, 42)
        with self.assertRaises(SightglassError) as caught:
            decode_wechat_v2_image(encoded, key)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)
        self.assertEqual(decode_wechat_v2_image(encoded, key, xor_key=42), source)

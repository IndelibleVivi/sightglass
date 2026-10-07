from __future__ import annotations

import hashlib

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from sightglass.contracts.errors import ErrorCode, SightglassError

_AES_BLOCK_BYTES = 16
_FILE_XOR_KEY_SUFFIX = "EMOTICON"
WXGF_MAGIC = b"wxgf"


def derive_wechat_sticker_key(first: str, second: str) -> bytes:
    """Reproduce WeChat's in-memory FileXorKey derivation for one account."""

    material = f"{first}{second}{_FILE_XOR_KEY_SUFFIX}".encode()
    return hashlib.md5(material).digest()  # noqa: S324 - source-format compatibility


def decrypt_wechat_sticker_prefix(ciphertext: bytes, key: bytes) -> bytes:
    """Decrypt the first cache block without requiring the trailing padding block."""

    if len(key) != _AES_BLOCK_BYTES or len(ciphertext) < _AES_BLOCK_BYTES:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    try:
        decryptor = Cipher(algorithms.AES(key), modes.CBC(key)).decryptor()
        return decryptor.update(ciphertext[:_AES_BLOCK_BYTES])
    except ValueError as exc:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc


def is_wechat_sticker_image(data: bytes) -> bool:
    """Whether recovered sticker bytes begin with a supported local image format."""

    return (
        data.startswith(
            (b"\x89PNG\r\n\x1a\n", b"\xff\xd8\xff", b"GIF87a", b"GIF89a", WXGF_MAGIC)
        )
        or len(data) >= 12
        and data[:4] == b"RIFF"
        and data[8:12] == b"WEBP"
    )


def decrypt_wechat_sticker(ciphertext: bytes, key: bytes) -> bytes:
    """Decrypt one local WeChat sticker-cache payload.

    Current macOS WeChat encrypts the complete cache entry with AES-128-CBC,
    reusing the sixteen-byte FileXorKey as the IV, and applies PKCS#7 padding.
    The caller remains responsible for MIME sniffing the recovered bytes.
    """

    if (
        len(key) != _AES_BLOCK_BYTES
        or not ciphertext
        or len(ciphertext) % _AES_BLOCK_BYTES != 0
    ):
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    try:
        decryptor = Cipher(algorithms.AES(key), modes.CBC(key)).decryptor()
        padded = decryptor.update(ciphertext) + decryptor.finalize()
        unpadder = padding.PKCS7(_AES_BLOCK_BYTES * 8).unpadder()
        plaintext = unpadder.update(padded) + unpadder.finalize()
    except ValueError as exc:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc
    if not plaintext:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    return plaintext

from __future__ import annotations

import struct

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from sightglass.contracts.errors import ErrorCode, SightglassError

from .processors import MAX_IMAGE_DIMENSION, MAX_IMAGE_PIXELS

V2_MAGIC = b"\x07\x08V2\x08\x07"


def _is_supported_image(data: bytes) -> bool:
    return (
        data.startswith((b"\x89PNG\r\n\x1a\n", b"\xff\xd8\xff", b"GIF87a", b"GIF89a", b"wxgf"))
        or len(data) >= 12
        and data[:4] == b"RIFF"
        and data[8:12] == b"WEBP"
    )


def decode_wechat_v2_image(data: bytes, key: bytes, *, xor_key: int | None = None) -> bytes:
    """Decode the bounded local V2 image envelope used by current WeChat clients."""

    if not data.startswith(V2_MAGIC) or len(data) < 15 or len(key) != 16:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    try:
        aes_size = struct.unpack_from("<I", data, 6)[0]
        xor_size = struct.unpack_from("<I", data, 10)[0]
    except struct.error as exc:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc
    body = data[15:]
    aligned_aes_size = aes_size + (16 - (aes_size % 16))
    if aligned_aes_size > len(body) or xor_size > len(body) - aligned_aes_size:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    encrypted = body[:aligned_aes_size]
    try:
        decryptor = Cipher(algorithms.AES(key), modes.ECB()).decryptor()
        aes_part = (decryptor.update(encrypted) + decryptor.finalize())[:aes_size]
    except ValueError as exc:
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED) from exc
    if aes_part.startswith(b"\x89PNG\r\n\x1a\n") and len(aes_part) >= 24:
        width, height = struct.unpack_from(">II", aes_part, 16)
        if (
            width > MAX_IMAGE_DIMENSION
            or height > MAX_IMAGE_DIMENSION
            or width * height > MAX_IMAGE_PIXELS
        ):
            raise SightglassError(ErrorCode.RESOURCE_TOO_LARGE)
    raw_end = len(body) - xor_size
    raw_part = body[aligned_aes_size:raw_end]
    encrypted_tail = body[raw_end:]
    if xor_size:
        # Real native traces disprove a global constant: valid AES headers with a
        # wrong XOR tail decode into gray/black partial images under tolerant sips.
        # Known complete-format footers supply a per-payload proof of the mask.
        footer = (
            b"\xff\xd9"
            if aes_part.startswith(b"\xff\xd8\xff")
            else b"\x00\x00\x00\x00IEND\xaeB\x60\x82"
            if aes_part.startswith(b"\x89PNG\r\n\x1a\n")
            else b"\x00\x3b"
            if aes_part.startswith((b"GIF87a", b"GIF89a"))
            else None
        )
        if footer is not None and len(encrypted_tail) >= len(footer):
            masks = {
                left ^ right
                for left, right in zip(encrypted_tail[-len(footer) :], footer, strict=True)
            }
            if len(masks) != 1:
                raise SightglassError(
                    ErrorCode.RESOURCE_DECODE_FAILED, details={"reason": "v2_tail_integrity"}
                )
            inferred = masks.pop()
            if xor_key is not None and xor_key != inferred:
                raise SightglassError(
                    ErrorCode.RESOURCE_DECODE_FAILED, details={"reason": "v2_tail_integrity"}
                )
            xor_key = inferred
        if xor_key is None:
            raise SightglassError(
                ErrorCode.RESOURCE_UNAVAILABLE, details={"reason": "image_xor_key_missing"}
            )
        if type(xor_key) is not int or not 0 <= xor_key <= 255:
            raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    xor_part = bytes(value ^ int(xor_key or 0) for value in encrypted_tail)
    decoded = aes_part + raw_part + xor_part
    if not _is_supported_image(decoded):
        raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
    return decoded

"""Lossless, self-describing storage codec for ``message_observations`` payloads.

The observation envelope is the immutable evidence a source read produced for one
message. It is stored as a SQLite BLOB so the same bytes round-trip exactly:

    magic(4) | format_version(1) | codec(1) | raw_length(4) | crc32(4) | body

``payload_digest`` stays the SHA-256 of the *uncompressed* UTF-8 bytes, so
deduplication identity, ``observation_id`` derivation and generation verification
are unchanged by the encoding choice. Legacy rows remain plain ``TEXT`` JSON and
are returned untouched by :func:`decode_observation_text`; only new writes go
through the canonical encoder. Corruption fails closed with
:class:`ObservationCodecError` instead of being skipped.
"""

from __future__ import annotations

import json
import struct
import zlib
from typing import Any

MAGIC = b"SGOC"
FORMAT_VERSION = 1
CODEC_RAW = 0
CODEC_ZLIB = 1
# A released payload keeps identity evidence (original length + checksum) but the
# body copy is intentionally gone.  It is a distinct, explicit representation so
# every consumer can distinguish "released" from "corrupt".
CODEC_RELEASED = 2

# The released body carries a small, CRC-checked JSON *retained header* rather than
# a bare length.  It preserves the original sender identity keys and source
# envelope identity so identity corrections and consistency checks keep working
# without the (expired) message body copy.  The original full-payload SHA-256 stays
# the authoritative ``message_observations.payload_digest`` column value.
RELEASED_HEADER_VERSION = 1

# 2**32 bytes is far beyond any bounded observation payload; refuse to encode past
# it rather than silently truncating the length field.
MAX_PAYLOAD_BYTES = 0xFFFFFFFF

_HEADER = struct.Struct(">4sBBII")
_HEADER_SIZE = _HEADER.size


class ObservationCodecError(RuntimeError):
    """Raised when a stored observation payload is malformed or corrupt."""


class ObservationPayloadUnavailable(ObservationCodecError):
    """Raised when an observation body copy was intentionally released.

    The observation header (identity, sequence, digest) is intact and the retained
    header can still be decoded; only the redundant body copy is gone and can be
    rehydrated from the source on demand.
    """


def encode_observation(payload: str | bytes) -> bytes:
    """Encode one observation envelope into its canonical self-describing BLOB."""

    raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
    if len(raw) > MAX_PAYLOAD_BYTES:
        raise ObservationCodecError("observation payload exceeds the addressable length")
    compressed = zlib.compress(raw, 6)
    if len(compressed) < len(raw):
        codec, body = CODEC_ZLIB, compressed
    else:
        codec, body = CODEC_RAW, raw
    header = _HEADER.pack(
        MAGIC,
        FORMAT_VERSION,
        codec,
        len(raw),
        zlib.crc32(raw) & 0xFFFFFFFF,
    )
    return header + body


def _decode_envelope_header(value: object) -> tuple[int, int, int, int, bytes]:
    """Validate the canonical envelope header, returning its parsed fields.

    Raises :class:`ObservationCodecError` (never the availability subclass) for a
    malformed magic/version/length so callers can distinguish corruption from an
    intentional release.
    """

    if isinstance(value, memoryview):
        value = value.tobytes()
    if not isinstance(value, (bytes, bytearray)):
        raise ObservationCodecError("observation payload is neither text nor a BLOB")
    data = bytes(value)
    if len(data) < _HEADER_SIZE:
        raise ObservationCodecError("observation payload is shorter than its header")
    magic, version, codec, raw_length, checksum = _HEADER.unpack_from(data, 0)
    if magic != MAGIC:
        raise ObservationCodecError("observation payload has an unknown magic header")
    if version != FORMAT_VERSION:
        raise ObservationCodecError(f"unsupported observation format version {version}")
    return version, codec, int(raw_length), checksum, data[_HEADER_SIZE:]


def _released_body(body: bytes, length: int, checksum: int) -> bytes:
    try:
        decoder = zlib.decompressobj()
        raw = decoder.decompress(body, length + 1)
    except zlib.error as exc:
        raise ObservationCodecError("released header failed to decompress") from exc
    if (
        not decoder.eof
        or decoder.unused_data
        or decoder.unconsumed_tail
        or len(raw) != length
        or zlib.crc32(raw) & 0xFFFFFFFF != checksum
    ):
        raise ObservationCodecError("released header length/integrity mismatch")
    return raw


def observation_payload_state(value: object) -> str:
    """Classify a stored observation payload as ``full``/``released``/``corrupt``.

    Legacy ``TEXT`` rows and valid RAW/ZLIB BLOBs are ``full``.  A well-formed
    released header is ``released``.  Anything malformed (bad magic/version,
    truncated, bad CRC/length, trailing bytes) is ``corrupt`` -- never a
    destructive "released" shortcut.
    """

    if isinstance(value, str):
        return "full"
    try:
        _, codec, raw_length, checksum, body = _decode_envelope_header(value)
    except ObservationCodecError:
        return "corrupt"
    if codec == CODEC_RELEASED:
        try:
            raw = _released_body(body, raw_length, checksum)
            header = json.loads(raw.decode("utf-8"))
        except (ObservationCodecError, ValueError, UnicodeDecodeError):
            return "corrupt"
        valid = (
            isinstance(header, dict)
            and header.get("released_header_version") == RELEASED_HEADER_VERSION
            and type(header.get("original_bytes")) is int
            and 0 <= header["original_bytes"] <= MAX_PAYLOAD_BYTES
            and isinstance(header.get("retained"), dict)
        )
        return "released" if valid else "corrupt"
    if codec == CODEC_RAW:
        return (
            "full"
            if len(body) == raw_length and zlib.crc32(body) & 0xFFFFFFFF == checksum
            else "corrupt"
        )
    if codec == CODEC_ZLIB:
        try:
            decoder = zlib.decompressobj()
            raw = decoder.decompress(body, raw_length + 1)
        except zlib.error:
            return "corrupt"
        if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
            return "corrupt"
        return (
            "full"
            if len(raw) == raw_length and zlib.crc32(raw) & 0xFFFFFFFF == checksum
            else "corrupt"
        )
    return "corrupt"


def encode_released_observation(retained: dict, *, original_bytes: int) -> bytes:
    """Encode a released observation retaining only identity evidence.

    ``retained`` must be a JSON-serializable mapping of the original sender
    identity keys and source envelope identity.  The envelope is CRC-checked by the
    same canonical header; a malformed decode fails as corruption.
    """

    if not isinstance(retained, dict):
        raise ObservationCodecError("released header must be an object")
    payload = {
        "released_header_version": RELEASED_HEADER_VERSION,
        "original_bytes": int(original_bytes),
        "retained": retained,
    }
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    if len(body) > MAX_PAYLOAD_BYTES:
        raise ObservationCodecError("released header exceeds the addressable length")
    header = _HEADER.pack(
        MAGIC, FORMAT_VERSION, CODEC_RELEASED, len(body), zlib.crc32(body) & 0xFFFFFFFF
    )
    return header + zlib.compress(body, 6)


def build_released_header(value: object) -> dict[str, Any]:
    """Extract the identity evidence a released header must retain.

    Decodes the ORIGINAL observation envelope (legacy TEXT or full BLOB) and keeps
    exactly what other consumers still need without the body copy: the original
    sender identity keys (kind/value plus scope/provenance where present), the
    historical surface label, the outgoing flag, and the source envelope identity.
    It never consults the current canonical binding.
    """

    try:
        envelope = json.loads(decode_observation_bytes(value).decode("utf-8"))
    except (ObservationCodecError, ValueError, UnicodeDecodeError) as exc:
        raise ObservationCodecError("observation payload is unreadable") from exc
    if not isinstance(envelope, dict):
        raise ObservationCodecError("observation payload is not an object")
    retained: dict[str, Any] = {}
    sender = envelope.get("sender")
    if isinstance(sender, dict):
        keys = sender.get("identity_keys")
        if keys is not None and not isinstance(keys, list):
            raise ObservationCodecError("original observation has invalid sender keys")
        if isinstance(keys, list):
            retained_keys: list[dict[str, object]] = []
            for item in keys:
                if not isinstance(item, dict):
                    raise ObservationCodecError("original observation has invalid sender key")
                if item.get("kind") is None or item.get("value") is None:
                    raise ObservationCodecError("original observation has incomplete sender key")
                key: dict[str, object] = {
                    "kind": item.get("kind"),
                    "value": item.get("value"),
                }
                for extra in (
                    "stability",
                    "principal_eligible",
                    "scope_conversation_source_id",
                    "provenance",
                ):
                    if extra in item:
                        key[extra] = item[extra]
                retained_keys.append(key)
            retained["sender_identity_keys"] = retained_keys
        for field in ("surface_label", "is_outgoing"):
            if field in sender:
                retained[field] = sender[field]
    message = envelope.get("message")
    if isinstance(message, dict):
        retained["message_kind"] = message.get("kind")
    envelope_source = envelope.get("source_envelope")
    if isinstance(envelope_source, dict):
        retained["source_envelope"] = {
            key: envelope_source.get(key)
            for key in (
                "source_message_id",
                "source_conversation_id",
                "source_time_raw",
                "raw_payload_digest",
                "wechat_type",
                "sort_primary",
                "sort_seq",
                "sort_tie",
                "source_rowid",
                "source_generation_id",
                "sent_at_utc",
            )
            if key in envelope_source
        }
    return retained


def decode_released_header(value: object) -> dict | None:
    """Return the retained header of a released observation, else ``None``.

    Malformed payloads return ``None`` (corrupt), never a partial header.
    """

    if observation_payload_state(value) != "released":
        return None
    _, _, raw_length, checksum, body = _decode_envelope_header(value)
    try:
        return json.loads(_released_body(body, raw_length, checksum).decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None


def observation_payload_available(value: object) -> bool:
    """True when the observation *body copy* is physically present.

    Only a valid full (legacy TEXT / RAW / ZLIB) payload is available.  Released
    and corrupt payloads are both unavailable here; use
    :func:`observation_payload_state` to distinguish them when that matters.
    """

    return observation_payload_state(value) == "full"


def is_encoded_observation(value: object) -> bool:
    """True when ``value`` is a canonical encoded observation BLOB."""

    if isinstance(value, memoryview):
        value = value.tobytes()
    return isinstance(value, (bytes, bytearray)) and bytes(value[:4]) == MAGIC


def decode_observation_bytes(value: object) -> bytes:
    """Return the uncompressed UTF-8 bytes, verifying format and integrity."""

    if isinstance(value, str):
        # Legacy rows stored the raw JSON in the TEXT column; return it verbatim.
        return value.encode("utf-8")
    if isinstance(value, memoryview):
        value = value.tobytes()
    if not isinstance(value, (bytes, bytearray)):
        raise ObservationCodecError("observation payload is neither text nor a BLOB")
    data = bytes(value)
    if len(data) < _HEADER_SIZE:
        raise ObservationCodecError("observation payload is shorter than its header")
    magic, version, codec, raw_length, checksum = _HEADER.unpack_from(data, 0)
    if magic != MAGIC:
        raise ObservationCodecError("observation payload has an unknown magic header")
    if version != FORMAT_VERSION:
        raise ObservationCodecError(f"unsupported observation format version {version}")
    body = data[_HEADER_SIZE:]
    if codec == CODEC_RELEASED:
        # A malformed released marker is corruption, not an intentional absence.
        if observation_payload_state(data) != "released":
            raise ObservationCodecError("released observation header is corrupt")
        raise ObservationPayloadUnavailable(
            "observation body copy was released and can be rehydrated on demand"
        )
    if codec == CODEC_RAW:
        raw = body
    elif codec == CODEC_ZLIB:
        try:
            decoder = zlib.decompressobj()
            raw = decoder.decompress(body, raw_length + 1)
        except zlib.error as exc:
            raise ObservationCodecError("observation payload failed to decompress") from exc
        if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
            raise ObservationCodecError("observation payload is not one complete stream")
    else:
        raise ObservationCodecError(f"unsupported observation codec {codec}")
    if len(raw) != raw_length:
        raise ObservationCodecError("observation payload length does not match its header")
    if zlib.crc32(raw) & 0xFFFFFFFF != checksum:
        raise ObservationCodecError("observation payload failed its integrity check")
    return raw


def decode_observation_text(value: str | bytes | memoryview) -> str:
    """Return the observation envelope as text, accepting legacy ``TEXT`` rows."""

    try:
        return decode_observation_bytes(value).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ObservationCodecError("observation payload is not UTF-8") from exc

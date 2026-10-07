from __future__ import annotations

import hashlib
import json
import sqlite3
import struct
import zlib
from contextlib import closing
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from .direct_wechat import SYNTHETIC_SOURCE_SCHEMA

DEFAULT_OBSERVED_AT = "2026-09-13T10:00:00+00:00"
SYNTHETIC_IMAGE_KEY = bytes.fromhex("00112233445566778899aabbccddeeff")


def _png_bytes() -> bytes:
    def chunk(kind: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))
        )

    width = 3
    height = 2
    rows = b"".join(b"\x00" + bytes((255, 80 + row * 60, 120)) * width for row in range(height))
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(rows))
        + chunk(b"IEND", b"")
    )


def _v2_image_bytes(image: bytes) -> bytes:
    aes_size = min(32, len(image))
    padding_size = 16 - (aes_size % 16)
    padded = image[:aes_size] + bytes([padding_size]) * padding_size
    encryptor = Cipher(algorithms.AES(SYNTHETIC_IMAGE_KEY), modes.ECB()).encryptor()
    encrypted = encryptor.update(padded) + encryptor.finalize()
    xor_size = min(16, len(image) - aes_size)
    raw_end = len(image) - xor_size
    return (
        b"\x07\x08V2\x08\x07"
        + struct.pack("<II", aes_size, xor_size)
        + b"\x00"
        + encrypted
        + image[aes_size:raw_end]
        + bytes(value ^ 0x88 for value in image[raw_end:])
    )


def _pdf_bytes(pages: tuple[str, ...]) -> bytes:
    objects: list[bytes] = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    kids: list[str] = []
    for text in pages:
        page_number = len(objects) + 1
        stream_number = page_number + 1
        kids.append(f"{page_number} 0 R")
        escaped = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        stream = f"BT /F1 13 Tf 72 720 Td ({escaped}) Tj ET\n".encode("ascii")
        objects.append(
            (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                f"/Resources << /Font << /F1 3 0 R >> >> /Contents {stream_number} 0 R >>"
            ).encode("ascii")
        )
        objects.append(
            f"<< /Length {len(stream)} >>\nstream\n".encode("ascii") + stream + b"endstream"
        )
    objects[1] = f"<< /Type /Pages /Kids [{' '.join(kids)}] /Count {len(kids)} >>".encode("ascii")
    body = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for number, value in enumerate(objects, start=1):
        offsets.append(len(body))
        body.extend(f"{number} 0 obj\n".encode("ascii"))
        body.extend(value)
        body.extend(b"\nendobj\n")
    xref = len(body)
    body.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    body.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        body.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    body.extend(
        (f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n").encode(
            "ascii"
        )
    )
    return bytes(body)


def _create_catalog(path: Path, *, roster_complete: bool) -> None:
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(
            """
            CREATE TABLE conversations (
                source_conversation_id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                title TEXT NOT NULL,
                last_message_at_utc TEXT,
                roster_complete INTEGER NOT NULL
            );
            CREATE TABLE conversation_aliases (
                source_conversation_id TEXT NOT NULL,
                alias TEXT NOT NULL
            );
            CREATE TABLE principals (
                internal_id TEXT PRIMARY KEY,
                is_self INTEGER NOT NULL,
                actor_kind TEXT NOT NULL,
                account_nickname TEXT,
                contact_remark TEXT,
                public_handle TEXT
            );
            CREATE TABLE memberships (
                source_conversation_id TEXT NOT NULL,
                internal_id TEXT,
                source_membership_id TEXT,
                group_card TEXT,
                observed_at_utc TEXT NOT NULL
            );
            """
        )
        connection.executemany(
            "INSERT INTO conversations VALUES (?, ?, ?, ?, ?)",
            (
                (
                    "conv_group",
                    "group",
                    "Synthetic Group",
                    "2026-09-13T09:06:00+00:00",
                    int(roster_complete),
                ),
                (
                    "conv_direct",
                    "direct",
                    "Demo Direct",
                    "2026-09-13T09:04:00+00:00",
                    1,
                ),
            ),
        )
        connection.executemany(
            "INSERT INTO conversation_aliases VALUES (?, ?)",
            (("conv_group", "项目群"), ("conv_direct", "示例甲私聊")),
        )
        connection.executemany(
            "INSERT INTO principals VALUES (?, ?, ?, ?, ?, ?)",
            (
                (
                    "wxid_demo_owner",
                    1,
                    "person",
                    "Synthetic Owner Account",
                    "Synthetic Owner",
                    "demo_owner_handle",
                ),
                ("wxid_demo_member", 0, "person", "原账号昵称", "示例甲", "demo_member_old"),
                ("wxid_demo_member2", 0, "person", "示例甲", None, "demo_member_other"),
            ),
        )
        connection.executemany(
            "INSERT INTO memberships VALUES (?, ?, ?, ?, ?)",
            (
                (
                    "conv_group",
                    "wxid_demo_owner",
                    "group-self",
                    "Synthetic Owner群名片",
                    DEFAULT_OBSERVED_AT,
                ),
                (
                    "conv_group",
                    "wxid_demo_member",
                    "group-demo_member",
                    "群里的示例甲",
                    DEFAULT_OBSERVED_AT,
                ),
                (
                    "conv_group",
                    "wxid_demo_member2",
                    "group-demo_member2",
                    "示例甲",
                    DEFAULT_OBSERVED_AT,
                ),
                ("conv_direct", "wxid_demo_owner", "direct-self", None, DEFAULT_OBSERVED_AT),
                (
                    "conv_direct",
                    "wxid_demo_member",
                    "direct-demo_member",
                    None,
                    DEFAULT_OBSERVED_AT,
                ),
            ),
        )
        connection.commit()


def _create_shard(path: Path, rows: list[dict[str, Any]]) -> None:
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(
            """
            CREATE TABLE messages (
                source_message_id TEXT PRIMARY KEY,
                source_conversation_id TEXT NOT NULL,
                source_time_raw TEXT NOT NULL,
                sent_at_utc TEXT NOT NULL,
                observed_at_utc TEXT NOT NULL,
                sort_seq INTEGER NOT NULL,
                source_rowid INTEGER NOT NULL,
                wechat_type INTEGER NOT NULL,
                raw_content TEXT NOT NULL,
                is_outgoing INTEGER NOT NULL,
                sender_internal_id TEXT,
                sender_local_token TEXT,
                sender_surface_label TEXT,
                resources_json TEXT NOT NULL DEFAULT '[]'
            );
            """
        )
        connection.executemany(
            """
            INSERT INTO messages(
                source_message_id, source_conversation_id, source_time_raw,
                sent_at_utc, observed_at_utc, sort_seq, source_rowid,
                wechat_type, raw_content, is_outgoing, sender_internal_id,
                sender_local_token, sender_surface_label, resources_json
            ) VALUES (
                :source_message_id, :source_conversation_id, :source_time_raw,
                :sent_at_utc, :observed_at_utc, :sort_seq, :source_rowid,
                :wechat_type, :raw_content, :is_outgoing, :sender_internal_id,
                :sender_local_token, :sender_surface_label, :resources_json
            )
            """,
            rows,
        )
        connection.commit()


def _row(
    message_id: str,
    conversation_id: str,
    sent_at: str,
    sort_seq: int,
    rowid: int,
    wechat_type: int,
    content: str,
    *,
    outgoing: bool = False,
    sender: str | None = None,
    local_token: str | None = None,
    shown_as: str | None = None,
    resources: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "source_message_id": message_id,
        "source_conversation_id": conversation_id,
        "source_time_raw": sent_at,
        "sent_at_utc": sent_at,
        "observed_at_utc": DEFAULT_OBSERVED_AT,
        "sort_seq": sort_seq,
        "source_rowid": rowid,
        "wechat_type": wechat_type,
        "raw_content": content,
        "is_outgoing": int(outgoing),
        "sender_internal_id": sender,
        "sender_local_token": local_token,
        "sender_surface_label": shown_as,
        "resources_json": json.dumps(resources or [], ensure_ascii=False),
    }


def create_synthetic_source(
    root: str | Path,
    *,
    include_second_shard: bool = True,
    declare_second_shard: bool = True,
    catalog_complete: bool = True,
    roster_complete: bool = False,
) -> Path:
    target = Path(root).expanduser().resolve()
    target.mkdir(parents=True, exist_ok=True)
    resource_root = target / "resources"
    resource_root.mkdir(parents=True, exist_ok=True)
    image_payload = _png_bytes()
    pdf_payload = _pdf_bytes(
        (
            "Page one contains the synthetic overview.",
            "Page two explains stable resource identity.",
            "Page three says atomic publish keeps preview coherent.",
        )
    )
    text_payload = (
        "first line\nsecond line\natomic publish keeps cache coherent\n最后一行\n"
    ).encode()
    (resource_root / "image-004.bin").write_bytes(_v2_image_bytes(image_payload))
    (resource_root / "file-006.pdf").write_bytes(pdf_payload)
    (resource_root / "text-009.md").write_bytes(text_payload)
    _create_catalog(target / "catalog.db", roster_complete=roster_complete)

    shard_one = [
        _row(
            "source-msg-001",
            "conv_group",
            "2026-09-13T09:00:00+00:00",
            10,
            1,
            1,
            "wxid_demo_member:\n  保留  内部空白！\n第二行",
            sender="wxid_demo_member",
            shown_as="原账号昵称",
        ),
        _row(
            "source-msg-002",
            "conv_direct",
            "2026-09-13T09:01:00+00:00",
            10,
            2,
            1,
            "私聊 incoming",
            sender="wxid_demo_member",
            shown_as="示例甲",
        ),
        _row(
            "source-msg-003",
            "conv_group",
            "2026-09-13T09:02:00+00:00",
            10,
            3,
            1,
            "我发出的消息",
            outgoing=True,
            shown_as="Synthetic Owner Account",
        ),
        _row(
            "source-msg-004",
            "conv_group",
            "2026-09-13T09:03:00+00:00",
            10,
            4,
            3,
            "",
            sender="wxid_demo_member",
            shown_as="群里的示例甲",
            resources=[
                {
                    "source_ordinal": 0,
                    "kind": "image",
                    "source_resource_key": "image-004",
                    "mime_type": "image/jpeg",
                    "original_name": "photo.bin",
                    "declared_size": len(image_payload),
                    "declared_hash": hashlib.sha256(image_payload).hexdigest(),
                    "availability": "local_available",
                }
            ],
        ),
    ]
    shard_two = [
        _row(
            "source-msg-005",
            "conv_group",
            "2026-09-13T09:00:00+00:00",
            20,
            1,
            1,
            "wxid_demo_member2:\n同名另一个人",
            sender="wxid_demo_member2",
            shown_as="示例甲",
        ),
        _row(
            "source-msg-006",
            "conv_group",
            "2026-09-13T09:04:00+00:00",
            10,
            2,
            49,
            "<msg><appmsg><title>synthetic.pdf</title><type>6</type><des>fixture</des></appmsg></msg>",
            sender="wxid_outsider",
            shown_as="非好友成员",
            resources=[
                {
                    "source_ordinal": 0,
                    "kind": "file",
                    "source_resource_key": "file-006",
                    "mime_type": "application/pdf",
                    "original_name": "synthetic.pdf",
                    "declared_size": len(pdf_payload),
                    "declared_hash": hashlib.sha256(pdf_payload).hexdigest(),
                    "availability": "local_available",
                }
            ],
        ),
        _row(
            "source-msg-007",
            "conv_group",
            "2026-09-13T09:05:00+00:00",
            10,
            3,
            999,
            "opaque unsupported payload",
            sender="wxid_demo_member",
            shown_as="原账号昵称",
        ),
        _row(
            "source-msg-008",
            "conv_direct",
            "2026-09-13T09:04:00+00:00",
            10,
            4,
            1,
            "私聊 outgoing",
            outgoing=True,
            shown_as="Synthetic Owner Account",
        ),
        _row(
            "source-msg-009",
            "conv_group",
            "2026-09-13T09:06:00+00:00",
            10,
            5,
            1,
            "复制来的消息",
            shown_as="复制显示名",
            resources=[
                {
                    "source_ordinal": 0,
                    "kind": "file",
                    "source_resource_key": "text-009",
                    "mime_type": "text/markdown",
                    "original_name": "notes.md",
                    "declared_size": len(text_payload),
                    "declared_hash": hashlib.sha256(text_payload).hexdigest(),
                    "availability": "local_available",
                }
            ],
        ),
    ]
    _create_shard(target / "messages-1.db", shard_one)
    if include_second_shard:
        _create_shard(target / "messages-2.db", shard_two)
    shards = [
        {
            "logical_key": "message-shard-1",
            "file": "messages-1.db",
            "generation_id": "generation-1a",
        }
    ]
    if declare_second_shard:
        shards.append(
            {
                "logical_key": "message-shard-2",
                "file": "messages-2.db",
                "generation_id": "generation-2a",
            }
        )
    manifest = {
        "schema": SYNTHETIC_SOURCE_SCHEMA,
        "source_namespace": "synthetic-installation-alpha",
        "account": {
            "source_account_key": "synthetic-account-demo",
            "self_principal_key": "wxid_demo_owner",
            "display_name": "Synthetic Owner",
            "reader_timezone": "Asia/Singapore",
            "identity_confidence": "exact",
        },
        "catalog": {
            "file": "catalog.db",
            "complete": catalog_complete,
            "active_only": False,
        },
        "shards": shards,
        "resources": [
            {
                "source_resource_key": "image-004",
                "file": "resources/image-004.bin",
                "encoding": "wechat_v2",
                "image_aes_key": SYNTHETIC_IMAGE_KEY.hex(),
            },
            {"source_resource_key": "file-006", "file": "resources/file-006.pdf"},
            {"source_resource_key": "text-009", "file": "resources/text-009.md"},
        ],
    }
    (target / "source.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return target

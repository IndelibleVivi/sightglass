"""Deterministic voice-message additions for synthetic source fixtures.

The synthetic source declares no voice content, so voice-path tests add their own
message-bound voice resources here.  Everything written by this module is declared
by the fixture manifest, so the provider admits it as ordinary source evidence
instead of a hand-inserted row that re-admission would deactivate.

The default payload is a synthetic envelope that is deliberately *not* decodable: it
exercises admission, delivery, and cached sidecar paths without a real decoder.  Tests
that drive the real decode/recognize pipeline pass their own ``payload`` builder.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sightglass.source.synthetic import DEFAULT_OBSERVED_AT

VOICE_RESOURCE_PREFIX = "voice-fixture"
FIRST_VOICE_TIME = datetime(2026, 9, 13, 10, 0, tzinfo=UTC)


def silk_payload(index: int) -> bytes:
    return b"\x02#!SILK_V3" + bytes(range(48)) + index.to_bytes(2, "little")


def declare_voice_messages(
    root: Path,
    *,
    count: int,
    conversation_id: str = "conv_group",
    sender: str = "wxid_demo_member",
    shown_as: str = "群里的示例甲",
    available: bool = True,
    first_time: datetime = FIRST_VOICE_TIME,
    payload: Callable[[int], bytes] | None = None,
) -> list[str]:
    """Declare ``count`` voice messages as newest entries of one synthetic conversation."""

    build_payload = payload if payload is not None else silk_payload
    source_root = Path(root).expanduser().resolve()
    manifest_path = source_root / "source.json"
    manifest: dict[str, Any] = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows: list[tuple[object, ...]] = []
    declared: list[dict[str, Any]] = []
    message_ids: list[str] = []
    for index in range(count):
        key = f"{VOICE_RESOURCE_PREFIX}-{index:03d}"
        content = build_payload(index)
        relative = f"resources/{key}.bin"
        (source_root / relative).write_bytes(content)
        declared.append({"source_resource_key": key, "file": relative})
        sent_at = (first_time + timedelta(seconds=index)).isoformat()
        message_id = f"voice-fixture-msg-{index:03d}"
        message_ids.append(message_id)
        rows.append(
            (
                message_id,
                conversation_id,
                sent_at,
                sent_at,
                DEFAULT_OBSERVED_AT,
                index,
                20_000 + index,
                34,
                f'<msg><voicemsg length="{600 + index}" /></msg>',
                0,
                sender,
                None,
                shown_as,
                json.dumps(
                    [
                        {
                            "source_ordinal": 0,
                            "kind": "voice",
                            "source_resource_key": key,
                            "mime_type": "audio/silk",
                            "declared_size": len(content),
                            "declared_hash": hashlib.sha256(content).hexdigest(),
                            "availability": "local_available" if available else "metadata_only",
                        }
                    ],
                    ensure_ascii=False,
                ),
            )
        )
    manifest.setdefault("resources", []).extend(declared)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with closing(sqlite3.connect(source_root / "messages-1.db")) as connection:
        connection.executemany(
            """
            INSERT INTO messages(
                source_message_id, source_conversation_id, source_time_raw,
                sent_at_utc, observed_at_utc, sort_seq, source_rowid,
                wechat_type, raw_content, is_outgoing, sender_internal_id,
                sender_local_token, sender_surface_label, resources_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        connection.commit()
    if count:
        newest = (first_time + timedelta(seconds=count - 1)).isoformat()
        with closing(sqlite3.connect(source_root / "catalog.db")) as connection:
            connection.execute(
                """
                UPDATE conversations SET last_message_at_utc = ?
                WHERE source_conversation_id = ? AND (
                    last_message_at_utc IS NULL OR last_message_at_utc < ?
                )
                """,
                (newest, conversation_id, newest),
            )
            connection.commit()
    return message_ids

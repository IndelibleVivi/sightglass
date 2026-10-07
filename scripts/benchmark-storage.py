#!/usr/bin/env python
"""Reproducible storage benchmark for the observation codec and label fix.

Writes a synthetic ``window.db`` twice over the same mixed message distribution:

* ``new``    - canonical encoded observation BLOBs (``observation_codec``)
* ``legacy`` - the pre-codec plain ``TEXT`` observation JSON

Both stores contain byte-identical *uncompressed* observation payloads, so the
comparison isolates storage encoding. The script reports physical table/index/DB
pages, codec bytes per message, and read/correction decode timings.

Everything is synthetic and disposable; no real account, source, message, or key
is read or written. Data lands under ``.sightglass/storage-benchmark`` (ignored by
Git) unless ``--out`` is given.

Example:

    uv run --no-sync python scripts/benchmark-storage.py --messages 2000
    uv run --no-sync python scripts/benchmark-storage.py --messages 200000 --json
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sqlite3
import statistics
import sys
import time
from contextlib import closing
from pathlib import Path

from sightglass.contracts.identity import (
    LabelObservation,
    SourceAccount,
    SourceConversation,
    SourceIdentityKey,
    SourceParticipant,
)
from sightglass.contracts.messages import ParsedMessage, SourceMessage
from sightglass.contracts.resources import SourceResource
from sightglass.model.db import WindowDB
from sightglass.model.observation_codec import decode_observation_text
from sightglass.model.repositories import WindowRepository
from sightglass.runtime.corrections import CorrectionService

T0 = "2026-01-01T00:00:00.000000+00:00"
CONVERSATION_SOURCE_ID = "synthetic-benchmark-conversation"
PARTICIPANT_POOL = 24

_ASCII_WORDS = ("ack", "ship", "review", "benchmark", "release", "queue", "metric")
_CJK_WORDS = ("合成", "确认", "安排", "复核", "附件", "进度", "结论")


def _generation_counter(message_index: int) -> str:
    return f"synthetic-generation-{message_index // 500}"


def _long_text(rng: random.Random, target_chars: int) -> str:
    parts: list[str] = []
    length = 0
    while length < target_chars:
        word = rng.choice(_ASCII_WORDS) if rng.random() < 0.6 else rng.choice(_CJK_WORDS)
        parts.append(word)
        length += len(word) + 1
    return " ".join(parts)


def _build_payload(rng: random.Random, index: int) -> tuple[SourceMessage, ParsedMessage]:
    sender_index = index % PARTICIPANT_POOL
    sender_key = f"wxid_synth_{sender_index:03d}"
    surface_label = f"合成成员{sender_index:02d}"
    source_message_id = f"synth-{index:08d}"
    observed_at = T0
    sender_keys = (
        SourceIdentityKey(
            "internal_username",
            sender_key,
            "stable",
            True,
            "synthetic.benchmark.message-envelope",
        ),
    )
    sender_labels = (
        LabelObservation(
            label=surface_label,
            label_kind="message_surface",
            scope="message-surface",
            provenance="synthetic.benchmark.surface_label",
            observed_at_utc=observed_at,
            temporal_confidence="exact",
            observed_source_message_id=source_message_id,
        ),
    )
    variant = index % 6
    resources: tuple[SourceResource, ...] = ()
    structured: dict[str, object] = {}
    kind = "text"
    text: str | None = None
    if variant == 0:
        text = rng.choice(_ASCII_WORDS)
    elif variant == 1:
        text = "".join(rng.choice(_CJK_WORDS) for _ in range(rng.randint(8, 40)))
    elif variant == 2:
        text = _long_text(rng, rng.randint(600, 2400))
    elif variant == 3:
        kind = "image"
        resources = (
            SourceResource(
                source_ordinal=0,
                kind="image",
                source_resource_key=f"synthetic-image-{index:08d}",
                mime_type="image/jpeg",
                original_name=f"synthetic-{index:08d}.jpg",
                declared_size=rng.randint(80_000, 4_000_000),
                availability="metadata_only",
            ),
        )
    elif variant == 4:
        kind = "link"
        structured = {
            "link": {
                "title": f"合成链接标题 {index}",
                "description": _long_text(rng, rng.randint(40, 160)),
                "source_name": "example.invalid",
                "host": "example.invalid",
                "path": f"/synthetic/{index:08d}",
                "raw_url": (
                    f"https://example.invalid/synthetic/{index:08d}"
                    f"?token=synthetic-{index}#fragment"
                ),
            }
        }
    else:
        kind = "forwarded_chat"
        item_count = rng.randint(2, 5)
        structured = {
            "forwarded_chat": {
                "title": f"合成转发 {index}",
                "items": [
                    {
                        "display_name": f"转发成员{position}",
                        "text": _long_text(rng, rng.randint(20, 120)),
                    }
                    for position in range(item_count)
                ],
            }
        }
    source = SourceMessage(
        source_message_id=source_message_id,
        source_conversation_id=CONVERSATION_SOURCE_ID,
        conversation_kind="group",
        source_time_raw=str(1_760_000_000 + index),
        sent_at_utc=T0,
        observed_at_utc=observed_at,
        sort_seq=index + 1,
        source_rowid=index + 1,
        wechat_type=variant,
        raw_content=f"synthetic transport envelope {index} " + "x" * (index % 64),
        is_outgoing=(sender_index == 0),
        source_generation_id=_generation_counter(index),
        logical_shard_key="shard-0",
        sender_keys=sender_keys,
        sender_labels=sender_labels,
        sender_surface_label=surface_label,
        resources=resources,
    )
    parsed = ParsedMessage(
        kind=kind,
        text=text,
        structured=structured,
        resources=resources,
    )
    return source, parsed


def _build_store(path: Path, message_count: int, seed: int) -> WindowDB:
    database = WindowDB(path)
    repository = WindowRepository(database)
    account_id = repository.upsert_account(
        SourceAccount(
            source_namespace="synthetic",
            source_account_key=f"synthetic-benchmark-{seed}",
            self_principal_key="wxid_synth_self",
            display_name="Synthetic Self",
            reader_timezone="UTC",
        ),
        T0,
    )
    conversation_id = repository.upsert_conversation(
        account_id,
        SourceConversation(
            source_conversation_id=CONVERSATION_SOURCE_ID,
            kind="group",
            title="Synthetic Benchmark Group",
            roster_complete=True,
        ),
        T0,
    )
    indexed: list[tuple[str, str]] = []
    for position in range(PARTICIPANT_POOL):
        participant_id, membership_id = repository.index_participant(
            account_id,
            conversation_id,
            SourceParticipant(
                source_conversation_id=CONVERSATION_SOURCE_ID,
                identity_keys=(
                    SourceIdentityKey(
                        "internal_username",
                        f"wxid_synth_{position:03d}",
                        "stable",
                        True,
                        "synthetic.benchmark.catalog",
                    ),
                ),
                labels=(
                    LabelObservation(
                        label=f"合成成员{position:02d}",
                        label_kind="account_nickname",
                        scope="account",
                        provenance="synthetic.benchmark.catalog",
                        observed_at_utc=T0,
                        temporal_confidence="current_only",
                    ),
                ),
                account_labels_complete=True,
            ),
            T0,
        )
        indexed.append((participant_id, membership_id))
    rng = random.Random(seed)
    # Construction is not a production throughput benchmark. Keep one private
    # synthetic connection/transaction so repeated connection-close checkpoints
    # do not dominate the million-row physical-size experiment.
    with database.transaction() as connection:
        connection.execute("PRAGMA cache_size = -65536")
        for start in range(0, message_count, 500):
            for index in range(start, min(start + 500, message_count)):
                source, parsed = _build_payload(rng, index)
                participant_id, membership_id = indexed[index % PARTICIPANT_POOL]
                repository.upsert_message(
                    account_id, conversation_id, participant_id, membership_id, source, parsed
                )
    return database


def _convert_to_legacy_text(path: Path) -> None:
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute("PRAGMA cache_size = -65536")
        connection.execute("BEGIN")
        position = 0
        while rows := connection.execute(
            "SELECT observation_seq, parsed_json FROM message_observations "
            "WHERE observation_seq > ? ORDER BY observation_seq LIMIT 500",
            (position,),
        ).fetchall():
            connection.executemany(
                "UPDATE message_observations SET parsed_json = ? WHERE observation_seq = ?",
                [(decode_observation_text(row[1]), row[0]) for row in rows],
            )
            position = int(rows[-1][0])
        connection.commit()
    finally:
        connection.close()


def _physical(path: Path) -> dict[str, object]:
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute("PRAGMA journal_mode = DELETE")
        connection.execute("VACUUM")
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        observation_bytes = int(
            connection.execute(
                "SELECT COALESCE(SUM(length(CAST(parsed_json AS BLOB))), 0) "
                "FROM message_observations"
            ).fetchone()[0]
        )
        dbstat: dict[str, int] = {}
        if connection.execute(
            "SELECT 1 FROM pragma_compile_options WHERE compile_options = 'ENABLE_DBSTAT_VTAB'"
        ).fetchone():
            dbstat = {
                str(name): int(total)
                for name, total in connection.execute(
                    "SELECT name, SUM(pgsize) FROM dbstat GROUP BY name"
                )
            }
    finally:
        connection.close()
    return {
        "file_bytes": path.stat().st_size,
        "page_size": page_size,
        "page_count": page_count,
        "page_bytes": page_size * page_count,
        "observation_payload_bytes": observation_bytes,
        "dbstat": dbstat,
    }


def _timings(path: Path, keys: list[dict[str, str]], repeats: int) -> dict[str, float]:
    database = WindowDB(path)
    decode_samples: list[float] = []
    correction_samples: list[float] = []
    for _ in range(repeats):
        start = time.perf_counter()
        with database.connection() as connection:
            for row in connection.execute("SELECT parsed_json FROM message_observations"):
                json.loads(decode_observation_text(row[0]))
        decode_samples.append(time.perf_counter() - start)
    for _ in range(repeats):
        start = time.perf_counter()
        with database.connection() as connection:
            CorrectionService._message_ids_for_keys(connection, keys)
        correction_samples.append(time.perf_counter() - start)
    return {
        "decode_all_ms": statistics.median(decode_samples) * 1000.0,
        "correction_scan_ms": statistics.median(correction_samples) * 1000.0,
    }


def _report(store: str, messages: int, physical: dict[str, object]) -> dict[str, object]:
    dbstat = physical["dbstat"]
    return {
        "store": store,
        "messages": messages,
        "file_bytes": physical["file_bytes"],
        "page_bytes": physical["page_bytes"],
        "bytes_per_message": physical["file_bytes"] / messages,
        "observation_payload_bytes": physical["observation_payload_bytes"],
        "observation_payload_bytes_per_message": (physical["observation_payload_bytes"] / messages),
        "message_observations_bytes": dbstat.get("message_observations"),
        "message_observations_index_bytes": dbstat.get("message_observation_projection_identity"),
        "messages_table_bytes": dbstat.get("messages"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=2_000, help="synthetic message count")
    parser.add_argument("--seed", type=int, default=20260926)
    parser.add_argument("--repeats", type=int, default=3, help="timing repetitions per store")
    parser.add_argument("--out", type=Path, default=None, help="output directory")
    parser.add_argument("--keep", action="store_true", help="keep generated stores")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    args = parser.parse_args(argv)
    if args.messages <= 0:
        parser.error("--messages must be positive")
    if args.repeats <= 0:
        parser.error("--repeats must be positive")

    repo_root = Path(__file__).resolve().parents[1]
    out = args.out or (repo_root / ".sightglass" / "storage-benchmark")
    out = out.expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)
    os.chmod(out, 0o700)
    work = out / f"run-{int(time.time())}-{args.seed}"
    work.mkdir(mode=0o700)

    new_path = work / "new" / "window.db"
    legacy_path = work / "legacy" / "window.db"
    reports: dict[str, dict[str, object]] = {}
    try:
        _build_store(new_path, args.messages, args.seed)
        legacy_path.parent.mkdir(mode=0o700, parents=True)
        # Only this generated synthetic store is copied. Both measurements contain
        # identical rows and indexes; converting TEXT isolates the codec's effect.
        with (
            closing(sqlite3.connect(new_path)) as source,
            closing(sqlite3.connect(legacy_path)) as destination,
        ):
            source.backup(destination)
        os.chmod(legacy_path, 0o600)
        _convert_to_legacy_text(legacy_path)

        keys = [
            {
                "key_kind": "internal_username",
                "key_value": f"wxid_synth_{position:03d}",
            }
            for position in range(0, PARTICIPANT_POOL, 4)
        ]
        for name, path in (("new", new_path), ("legacy", legacy_path)):
            physical = _physical(path)
            reports[name] = _report(name, args.messages, physical)
            reports[name].update(_timings(path, keys, args.repeats))
    finally:
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)

    if args.json:
        print(json.dumps(reports, indent=2, sort_keys=True))
        return 0

    columns = (
        ("messages", "messages"),
        ("file_bytes", "file bytes"),
        ("bytes_per_message", "bytes/message"),
        ("observation_payload_bytes_per_message", "observation bytes/message"),
        ("message_observations_bytes", "observations table bytes"),
        ("message_observations_index_bytes", "observations index bytes"),
        ("decode_all_ms", "decode-all ms"),
        ("correction_scan_ms", "correction scan ms"),
    )
    header = f"{'metric':<34}" + "".join(f"{name:>16}" for name in ("new", "legacy"))
    print(header)
    print("-" * len(header))
    for key, label in columns:
        new_value = reports["new"].get(key)
        legacy_value = reports["legacy"].get(key)
        row = f"{label:<34}"
        for value in (new_value, legacy_value):
            if value is None:
                rendered = "n/a"
            elif isinstance(value, float):
                rendered = f"{value:,.1f}"
            else:
                rendered = f"{value:,}"
            row += f"{rendered:>16}"
        print(row)
    new_bytes = float(reports["new"]["bytes_per_message"])
    legacy_bytes = float(reports["legacy"]["bytes_per_message"])
    if legacy_bytes:
        print(
            f"\nbytes/message delta: {new_bytes - legacy_bytes:+,.1f} "
            f"({(new_bytes / legacy_bytes - 1.0) * 100:+.1f}%)"
        )
    if args.keep:
        print(f"\ngenerated stores kept at: {work}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

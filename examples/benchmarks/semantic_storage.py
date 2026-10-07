"""Offline synthetic semantic-sidecar geometry; no encoder, account or network."""

from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
import struct
import tempfile
import time
from contextlib import closing
from pathlib import Path

from sightglass.semantic.service import _SIDECAR_SCHEMA
from sightglass.semantic.settings import ACTIVE_DIMENSIONS, ACTIVE_MODEL, ACTIVE_RECIPE


def run(count: int = 100_000) -> dict:
    if count < 20:
        raise ValueError("the manifest lookup fixture needs at least 20 rows")
    with tempfile.TemporaryDirectory(prefix="sightglass-synthetic-semantic-storage-") as directory:
        path = Path(directory) / "index.db"
        vector = struct.pack(f"<{ACTIVE_DIMENSIONS}f", *([0.125] * ACTIVE_DIMENSIONS))
        namespace = "f" * 64
        epoch = "synthetic-projection-epoch"
        instant = "2026-09-20T00:00:00+00:00"
        with closing(sqlite3.connect(path)) as connection:
            path.chmod(0o600)
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript(_SIDECAR_SCHEMA)
            started = time.perf_counter()
            with connection:
                for offset in range(0, count, 1000):
                    connection.executemany(
                        "INSERT INTO semantic_entries VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        [
                            (
                                f"wxmsg_{index:032x}",
                                namespace,
                                f"{index:064x}",
                                "message",
                                index + 1,
                                index + 1,
                                epoch,
                                "synthetic-account",
                                f"wxconv_{index % 20:032x}",
                                f"{index % 20:064x}",
                                "a" * 64,
                                "text",
                                instant,
                                f"{index:064x}",
                                vector,
                                1,
                                1,
                                instant,
                            )
                            for index in range(offset, min(offset + 1000, count))
                        ],
                    )
            insertion_seconds = time.perf_counter() - started
            post_commit_bytes = sum(
                file.stat().st_size
                for file in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm"))
                if file.exists()
            )
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            lookup = (
                "SELECT * FROM semantic_entries WHERE published=1 AND generator=?"
                f" AND remote_id IN ({','.join('?' for _ in range(20))})"
            )
            lookup_ms = []
            all_exact = True
            for probe in range(50):
                indices = [(probe * 7919 + delta) % count for delta in range(20)]
                ids = [f"{index:064x}" for index in indices]
                started = time.perf_counter()
                rows = connection.execute(lookup, (1, *ids)).fetchall()
                lookup_ms.append((time.perf_counter() - started) * 1000)
                all_exact &= {row[2] for row in rows} == set(ids)
            plan = [row[3] for row in connection.execute("EXPLAIN QUERY PLAN " + lookup, (1, *ids))]
            published, payload = connection.execute(
                "SELECT COUNT(*),SUM(LENGTH(vector)) FROM semantic_entries WHERE published=1"
            ).fetchone()
            assert published == count and payload == count * ACTIVE_DIMENSIONS * 4
            assert all_exact and any("remote_id" in detail for detail in plan)
            physical = path.stat().st_size
        return {
            "schema": "sightglass.synthetic-semantic-storage.v1",
            "synthetic_only": True,
            "network_calls": 0,
            "embedding_or_quality_measurement": False,
            "model_geometry": ACTIVE_MODEL,
            "recipe_geometry": ACTIVE_RECIPE,
            "rows": count,
            "dimensions": ACTIVE_DIMENSIONS,
            "sqlite_version": sqlite3.sqlite_version,
            "raw_float32_bytes": payload,
            "checkpointed_database_bytes": physical,
            "post_commit_database_wal_shm_bytes": post_commit_bytes,
            "bytes_per_entry": physical / count,
            "insertion_seconds": round(insertion_seconds, 4),
            "lookup_batch": 20,
            "lookup_probes": len(lookup_ms),
            "lookup_median_ms": round(statistics.median(lookup_ms), 4),
            "all_exact_manifest_lookups": all_exact,
            "lookup_plan": plan,
            "scope": "local sidecar geometry and indexed lookup; no canonical admission or ANN",
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    args = parser.parse_args()
    print(json.dumps(run(args.rows), indent=2))


if __name__ == "__main__":
    main()

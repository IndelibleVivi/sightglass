"""Offline 100k canonical-fence/query benchmark with a bounded ANN double.

Fixtures are seeded directly, so this measures the real local admission and
reader path, not source ingestion, embeddings, publication or cloud ANN quality.
"""

from __future__ import annotations

import argparse
import json
import statistics
import struct
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from examples.benchmarks.cloudflare_retrieval import _reader_state, _stack
from sightglass.semantic.service import (
    SemanticService,
    _conversation_digest,
    _input_hash,
    _namespace,
    _remote_id,
    _sender_digest,
)
from sightglass.semantic.settings import ACTIVE_DIMENSIONS, ACTIVE_MODEL, SemanticSettings


class BoundedANN:
    """Return fixed generated matches; never encode, index or access the network."""

    def __init__(self) -> None:
        self.matches: list[dict[str, Any]] = []
        self.calls = 0

    def encode(self, texts):
        return (tuple([0.125] * ACTIVE_DIMENSIONS),) * len(texts)

    def query(self, vector, namespace, filter, top_k):
        self.calls += 1
        assert len(self.matches) <= top_k
        return self.matches

    def verify_index(self, *, required_metadata):
        raise AssertionError("direct-seeded benchmark never publishes")

    def upsert(self, rows):
        raise AssertionError("direct-seeded benchmark never publishes")

    def get_by_ids(self, ids):
        raise AssertionError("direct-seeded benchmark never publishes")


def run(count: int = 100_000, probes: int = 25) -> dict[str, Any]:
    if count < 100 or probes < 1:
        raise ValueError("use at least 100 rows and one probe")
    with tempfile.TemporaryDirectory(prefix="sightglass-synthetic-admission-") as temporary:
        root = Path(temporary)
        repository, reader, tools, corpus = _stack(root)
        account, group = corpus["account"], corpus["group"]
        epoch = reader._projection_inventory_epoch()
        with repository.database.connection() as connection:
            seed = dict(
                connection.execute(
                    "SELECT * FROM messages WHERE message_id=?",
                    (corpus["cases"][0]["message_ids"][0],),
                ).fetchone()
            )
        backend = BoundedANN()
        lane = SemanticService(
            repository,
            reader.reader,
            settings=SemanticSettings(
                enabled=True,
                external_data_authorized=True,
                cf_account_id="b" * 32,
                index_name="sightglass-synthetic-offline",
                source_account_id=account,
                conversation_ids=(group,),
            ),
            backend=backend,
            epoch_factory=reader._projection_inventory_epoch,
            sidecar_path=root / "semantic" / "index.db",
        )
        reader.semantic = lane
        namespace = _namespace(account, epoch, 1)
        vector = struct.pack(f"<{ACTIVE_DIMENSIONS}f", *([0.125] * ACTIVE_DIMENSIONS))
        conversation_digest = _conversation_digest(account, group)
        sender_digest = _sender_digest(account, seed["sender_id"])
        base = datetime(2026, 1, 1, tzinfo=UTC)
        watermark = repository.observation_watermark()
        picked: list[dict[str, Any]] = []
        started = time.perf_counter()
        try:
            with repository.database.transaction() as connection:
                columns = tuple(seed)
                sql = (
                    f"INSERT INTO messages ({','.join(columns)}) VALUES "
                    f"({','.join('?' for _ in columns)})"
                )
                observation = dict(
                    connection.execute(
                        "SELECT * FROM message_observations WHERE message_id=? LIMIT 1",
                        (seed["message_id"],),
                    ).fetchone()
                )
                observation_columns = tuple(observation)
                observation_sql = (
                    f"INSERT INTO message_observations ({','.join(observation_columns)}) VALUES "
                    f"({','.join('?' for _ in observation_columns)})"
                )
                sidecar = lane._db()
                sidecar.execute("BEGIN IMMEDIATE")
                for offset in range(0, count, 1000):
                    messages, observations, entries = [], [], []
                    for index in range(offset, min(offset + 1000, count)):
                        row = dict(seed)
                        identity = f"wxmsg_synthetic_scale_{index:032x}"
                        instant = (base + timedelta(seconds=index * 240)).isoformat()
                        seq = watermark + index + 1
                        row.update(
                            message_id=identity,
                            source_message_id=f"synthetic-scale-{index}",
                            source_time_raw=instant,
                            sent_at_utc=instant,
                            sort_primary=instant,
                            sort_seq=index,
                            sort_tie=index,
                            text="Synthetic scale fixture: unrelated generated body.",
                            search_text="Synthetic scale fixture: unrelated generated body.",
                            structured_json="{}",
                            first_observation_seq=seq,
                            current_observation_seq=seq,
                        )
                        messages.append(tuple(row[key] for key in columns))
                        observed = dict(observation)
                        observed.update(
                            observation_seq=seq,
                            observation_id=f"synthetic-scale-observation-{index}",
                            message_id=identity,
                        )
                        observations.append(tuple(observed[key] for key in observation_columns))
                        input_hash = _input_hash(row)
                        remote = _remote_id(namespace, identity, input_hash, seq)
                        entries.append(
                            (
                                identity,
                                namespace,
                                remote,
                                "message",
                                seq,
                                seq,
                                epoch,
                                account,
                                group,
                                conversation_digest,
                                sender_digest,
                                "text",
                                instant,
                                input_hash,
                                vector,
                                1,
                                1,
                                instant,
                            )
                        )
                        # Twenty far-apart rows exercise timeline lookup across the store.
                        if index in {count - 1 - delta * (count // 20) for delta in range(20)}:
                            picked.append(row | {"remote_id": remote})
                    connection.executemany(sql, messages)
                    connection.executemany(observation_sql, observations)
                    sidecar.executemany(
                        "INSERT INTO semantic_entries VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        entries,
                    )
                sidecar.execute(
                    "UPDATE semantic_state SET state='ready',coverage='complete',"
                    "indexed_count=?,indexed_bytes=? WHERE id=1",
                    (count, count * len(vector)),
                )
                sidecar.commit()
            seed_seconds = time.perf_counter() - started
            watermark = repository.observation_watermark()
            picked.sort(key=lambda row: row["message_id"])
            backend.matches = [
                {"id": row["remote_id"], "namespace": namespace, "score": 1 - index * 0.01}
                for index, row in enumerate(picked)
            ]
            # Apply three real canonical changes after the sidecar snapshot.
            with repository.database.transaction() as connection:
                connection.execute(
                    "UPDATE messages SET text='Synthetic correction' WHERE message_id=?",
                    (picked[0]["message_id"],),
                )
                connection.execute(
                    "UPDATE messages SET current_state='recalled' WHERE message_id=?",
                    (picked[1]["message_id"],),
                )
                connection.execute(
                    "UPDATE messages SET projection_epoch='synthetic-stale' WHERE message_id=?",
                    (picked[2]["message_id"],),
                )
            expected = {row["message_id"] for row in picked[3:]}
            before = _reader_state(repository)
            admission_ms = []
            for _ in range(probes):
                started = time.perf_counter()
                result = lane.query(
                    "synthetic residual concept",
                    account_id=account,
                    conversation_ids=(group,),
                    watermark=watermark,
                    epoch=epoch,
                    kinds=("message",),
                )
                admission_ms.append((time.perf_counter() - started) * 1000)
                assert result.receipt["state"] == "ready"
                assert set(result.message_ids) == expected
            started = time.perf_counter()
            result = tools.wechat_retrieve(
                "synthetic residual concept",
                account_id=account,
                conversation_ids=[group],
                kinds=["message"],
                limit=10,
            )
            reader_ms = (time.perf_counter() - started) * 1000
            focus = {
                identity
                for context in result["contexts"]
                for identity in context["focus_message_ids"]
            }
            assert focus and focus <= expected
            assert before == _reader_state(repository)
            sidecar_bytes = lane._sidecar_path.stat().st_size
            with repository.database.connection() as connection:
                canonical_rows = connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            return {
                "schema": "sightglass.synthetic-semantic-admission.v1",
                "synthetic_only": True,
                "network_calls": 0,
                "embedding_or_ann_quality_measurement": False,
                "model_geometry": ACTIVE_MODEL,
                "seeded_manifest_rows": count,
                "canonical_rows": canonical_rows,
                "direct_fixture_seed_seconds": round(seed_seconds, 4),
                "sidecar_database_bytes": sidecar_bytes,
                "ann_double_matches": len(picked),
                "canonical_admitted": len(expected),
                "rejected_current_input_state_epoch": 3,
                "query_probes": probes,
                "query_admission_median_ms": round(statistics.median(admission_ms), 4),
                "reader_default_page_ms": round(reader_ms, 4),
                "reader_contexts": len(result["contexts"]),
                "reader_focus_messages": len(focus),
                "canonical_ids_only": True,
                "reader_state_unchanged": True,
                "scope": "direct-seeded local manifest + canonical query/reader; "
                "no ingestion/publication/cloud ANN",
            }
        finally:
            tools.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--probes", type=int, default=25)
    args = parser.parse_args()
    print(json.dumps(run(args.rows, args.probes), indent=2))


if __name__ == "__main__":
    main()

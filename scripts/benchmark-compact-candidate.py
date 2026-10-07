#!/usr/bin/env python
"""100k real synthetic-provider → WindowDB → candidate → canonical recall proof.

Writes only to the explicitly selected synthetic workspace. Receipt includes exact
ID parity, actual episode churn, FTS merge, WAL/workspace peaks and process peak
RSS. No config, Keychain, account, installed runtime or network is opened.
"""

from __future__ import annotations

import argparse
import json
import resource
import sqlite3
import sys
import time
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sightglass.model.compact_candidate import build_candidate, freeze_input, verify_candidate
from sightglass.model.current_body import current_search_document
from sightglass.model.db import WindowDB
from sightglass.model.lexical import candidate_expression
from sightglass.model.observation_codec import decode_observation_bytes
from sightglass.model.repositories import WindowRepository
from sightglass.source.parser import parse_message
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


def strict_ids(repository: WindowRepository, query: str) -> list[str]:
    expression = candidate_expression(query)
    with repository.database.connection() as connection:
        return [
            row["message_id"]
            for row in connection.execute(
                "SELECT m.* FROM message_lexical f JOIN messages m "
                "ON m.rowid=f.rowid "
                "WHERE message_lexical MATCH ? AND m.body_available=1 AND "
                "m.current_state='present' "
                "ORDER BY m.message_id",
                (expression,),
            )
            if all(term in current_search_document(row).casefold()
                   for term in query.casefold().split())
        ]


def run(root: Path, count: int) -> dict:
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    source = create_synthetic_source(root / "source")
    with closing(sqlite3.connect(source / "messages-1.db")) as connection:
        connection.executemany(
            "INSERT INTO "
            "messages(source_message_id,source_conversation_id,source_time_raw,sent_at_utc,"
            "observed_at_utc,sort_seq,source_rowid,wechat_type,raw_content,is_outgoing,sender_internal_id,"
            "sender_surface_label,resources_json) VALUES (?,'conv_group',?,?,?,?,?,1,?,0,"
            "'wxid_demo_member','Synthetic benchmark sender','[]')",
            (
                (
                    f"synthetic-compact-{i:06d}",
                    *([(datetime(2026, 10, 1, tzinfo=UTC) + timedelta(seconds=i)).isoformat()] * 3),
                    i,
                    i + 1000,
                    "synthetic needle alpha" if i % 5 == 0 else "synthetic haystack beta",
                )
                for i in range(count)
            ),
        )
        connection.execute(
            "CREATE INDEX synthetic_bench_timeline ON messages("
            "source_conversation_id,sent_at_utc,sort_seq,source_rowid,source_message_id)"
        )
        connection.commit()
    window = root / "state" / "window.db"
    provider, repository, service, tools = build_test_stack(source, window)
    assert tools.wechat_status()["ready"]
    group = tools.wechat_find_conversations("Synthetic Group")["candidates"][0]["conversation_id"]
    context = repository.conversation_context(group)
    # The original FTS backend stores duplicate text; candidate replaces it.
    with repository.database.transaction() as connection:
        connection.execute("DROP TABLE message_lexical")
        connection.execute(
            "CREATE VIRTUAL TABLE message_lexical USING fts5(text,"
            "tokenize='trigram case_sensitive 1',detail=none)"
        )
    start = time.perf_counter()
    admitted = 0
    before = None
    churn = []
    with provider.snapshot() as snapshot:
        while True:
            page = provider.read_range(
                "synthetic-account-demo",
                "conv_group",
                after=before,
                before=None,
                direction="forward",
                limit=500,
                snapshot=snapshot,
            )
            if not page.messages:
                break
            with repository.database.transaction():
                service._ingest_messages(context, page.messages)
            admitted += len(page.messages)
            churn.extend(
                message
                for message in page.messages
                if message.source_message_id
                in {"synthetic-compact-000000", "synthetic-compact-000001"}
            )
            before = page.messages[-1].sort_key
            if not page.has_more_after:
                break
        # Real A→B→A episodes, not synthetic pointer edits.
        with repository.database.transaction():
            for message in churn:
                for text in ("synthetic correction needle beta", message.raw_content):
                    changed = replace(message, raw_content=text)
                    repository.upsert_message(
                        context["account_id"],
                        group,
                        None,
                        None,
                        changed,
                        parse_message(changed),
                        projection_epoch=service._projection_inventory_epoch(),
                    )
    with repository.database.transaction() as connection:
        for row in connection.execute(
            "SELECT observation_seq,parsed_json FROM message_observations"
        ):
            connection.execute(
                "UPDATE message_observations SET parsed_json=? WHERE observation_seq=?",
                (decode_observation_bytes(row[1]).decode(), row[0]),
            )
        connection.execute(
            "UPDATE sqlite_sequence SET seq=seq+1000 WHERE name='message_observations'"
        )
        connection.execute(
            "UPDATE derived_index_state SET state='ready' WHERE index_kind='lexical'"
        )
    expected = {
        query: strict_ids(repository, query)
        for query in ("needle alpha", "haystack beta", "correction needle")
    }
    freeze = freeze_input(
        window, root / "workspace", workspace_budget_bytes=2 * 1024**3, min_free_bytes=0
    )
    frozen, candidate = root / "workspace/frozen.db", root / "workspace/candidate.db"
    report = build_candidate(
        frozen, candidate, workspace_budget_bytes=2 * 1024**3, min_free_bytes=0
    )
    other = WindowRepository(WindowDB(candidate))
    actual = {query: strict_ids(other, query) for query in expected}
    assert expected == actual, "canonical literal AND ID parity failed"
    churn_wal = 0
    # Exercise correction/delete and explicit FTS merge on the installed backend.
    with other.database.transaction() as connection:
        rows = connection.execute(
            "SELECT rowid,* FROM messages ORDER BY rowid LIMIT 1000"
        ).fetchall()
        connection.executemany("DELETE FROM message_lexical WHERE rowid=?", [(r[0],) for r in rows])
        connection.executemany(
            "INSERT INTO message_lexical(rowid,text) VALUES (?,?)",
            [(r["rowid"], current_search_document(r).casefold()) for r in rows],
        )
        connection.execute(
            "INSERT INTO message_lexical(message_lexical,rank) VALUES ('merge',1000)"
        )
    wal = candidate.with_name(candidate.name + "-wal")
    churn_wal = wal.stat().st_size if wal.exists() else 0
    with other.database.connection() as connection:
        self_check = connection.execute("PRAGMA quick_check").fetchone()[0]
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    assert self_check == "ok"
    assert expected == {query: strict_ids(other, query) for query in expected}
    # Read/runtime writes preserved the original durable identities/observations.
    verified = verify_candidate(frozen, candidate)
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform != "darwin":
        rss *= 1024
    tools.close()
    return {
        "schema": "sightglass.compact-benchmark.v2",
        "generated_messages": count,
        "admitted_group_messages": admitted,
        "fixture_floor_reached": admitted >= count,
        "strict_id_parity": expected == actual,
        "match_counts": {q: len(ids) for q, ids in actual.items()},
        "episode_churn": "A-B-A",
        "fts_churn_rows": 1000,
        "fts_merge": True,
        "source_bytes": window.stat().st_size,
        "candidate_bytes": candidate.stat().st_size,
        "recovery_bytes": Path(freeze["recovery_artifact"]).stat().st_size,
        "peak_workspace_bytes": report["peak_workspace_bytes"],
        "peak_candidate_wal_bytes": max(report["peak_candidate_wal_bytes"], churn_wal),
        "peak_process_rss_bytes": rss,
        "elapsed_seconds": round(time.perf_counter() - start, 3),
        "verified": verified["verified"],
        "account_access": False,
        "network_access": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=100_000)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = run(args.workspace, args.messages)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()

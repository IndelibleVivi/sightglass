"""Synthetic-only 100k strict substring/tokenizer comparison; no configured runtime."""

from __future__ import annotations

import json
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from sightglass.model.lexical import candidate_expression

QUERIES = (
    "project",
    "lover",
    "ailover-atlas.example/a-b",
    "人",
    "人工",
    "人工智能",
    "Straße",
    "strasse",
    "Σίσυφος",
    "path query",
    "companion 人工",
    "absent-string",
)


def run(count: int = 100_000) -> dict:
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "synthetic.db"
        connection = sqlite3.connect(path)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE corpus(id INTEGER PRIMARY KEY, text TEXT)")
        connection.executemany(
            "INSERT INTO corpus VALUES (?,?)",
            ((i, f"Synthetic common project noise {i} 人造测试") for i in range(count)),
        )
        targets = [
            "人工智能 companion https://ailover-atlas.example/a-b/path?query=synthetic",
            "Straße STRASSE Σίσυφος",
            "other project file path and query",
            "人工 companion",
        ]
        connection.executemany(
            "INSERT INTO corpus VALUES (?,?)", ((count + i, text) for i, text in enumerate(targets))
        )
        connection.commit()
        base_bytes = path.stat().st_size
        rows = [(row[0], row[1].casefold()) for row in connection.execute("SELECT * FROM corpus")]
        result = {
            "schema": "sightglass.synthetic-lexical-benchmark.v1",
            "messages": len(rows),
            "sqlite": sqlite3.sqlite_version,
            "backends": {},
        }
        for backend, tokenizer in [
            ("unicode61", "unicode61"),
            ("trigram", "trigram case_sensitive 1"),
        ]:
            table = "index_" + backend
            started = time.perf_counter()
            detail = "none" if backend == "trigram" else "full"
            connection.execute(
                f"CREATE VIRTUAL TABLE {table} USING fts5("
                f"text, tokenize='{tokenizer}', detail={detail})"
            )
            connection.executemany(f"INSERT INTO {table}(rowid,text) VALUES (?,?)", rows)
            connection.commit()
            build_seconds = time.perf_counter() - started
            wal_bytes = path.with_name(path.name + "-wal").stat().st_size
            checks = {}
            for query in QUERIES:
                terms = query.casefold().split()
                reference = {key for key, text in rows if all(term in text for term in terms)}
                expr = (
                    candidate_expression(query)
                    if backend == "trigram"
                    else " AND ".join('"' + term.replace('"', '""') + '"' for term in terms)
                )
                times = []
                hits = set()
                raw = set()
                for _ in range(3):
                    started = time.perf_counter()
                    if expr is None:
                        candidates = rows
                    else:
                        candidates = connection.execute(
                            f"SELECT corpus.id,corpus.text FROM {table} "
                            f"JOIN corpus ON corpus.id={table}.rowid WHERE {table} MATCH ?",
                            (expr,),
                        ).fetchall()
                    raw = {key for key, _ in candidates}
                    hits = {
                        key
                        for key, text in candidates
                        if all(term in text.casefold() for term in terms)
                    }
                    times.append((time.perf_counter() - started) * 1000)
                checks[query] = {
                    "reference": len(reference),
                    "final_hits": len(hits),
                    "missing": len(reference - hits),
                    "false_positives": len(hits - reference),
                    "candidate_count": len(raw),
                    "median_ms": round(statistics.median(times), 3),
                    "fallback": expr is None,
                }
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            index_bytes = connection.execute(
                "SELECT SUM(pgsize) FROM dbstat WHERE name LIKE ?", (table + "%",)
            ).fetchone()[0]
            connection.close()
            connection = sqlite3.connect(path)
            connection.execute(f"INSERT INTO {table}({table}) VALUES ('integrity-check')")
            connection.commit()
            restart_ok = True
            expected_count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            connection.close()
            crashed = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "import os,sqlite3,sys; "
                    "c=sqlite3.connect(sys.argv[1]); c.execute('BEGIN IMMEDIATE'); "
                    "c.execute('DELETE FROM '+sys.argv[2]); os._exit(71)",
                    str(path),
                    table,
                ],
                check=False,
                capture_output=True,
                timeout=30,
            )
            if crashed.returncode != 71:
                raise RuntimeError("synthetic crash experiment failed")
            connection = sqlite3.connect(path)
            crash_count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if crash_count != expected_count:
                raise RuntimeError("interrupted rebuild lost the published lexical view")
            connection.execute(f"INSERT INTO {table}({table}) VALUES ('integrity-check')")
            connection.execute(f"DROP TABLE {table}")
            connection.commit()
            started = time.perf_counter()
            connection.execute(
                f"CREATE VIRTUAL TABLE {table} USING fts5("
                f"text, tokenize='{tokenizer}', detail={detail})"
            )
            connection.executemany(f"INSERT INTO {table}(rowid,text) VALUES (?,?)", rows)
            connection.commit()
            rebuild_seconds = time.perf_counter() - started
            rebuilt_count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if rebuilt_count != expected_count:
                raise RuntimeError("synthetic lexical rebuild count differs")
            connection.execute(f"DROP TABLE {table}")
            connection.commit()
            result["backends"][backend] = {
                "build_seconds": round(build_seconds, 3),
                "index_bytes": index_bytes,
                "canonical_bytes": base_bytes,
                "checks": checks,
                "restart_integrity": restart_ok,
                "uncommitted_crash_preserved_index": crash_count == expected_count,
                "drop_rebuild_count": rebuilt_count,
                "rebuild_seconds": round(rebuild_seconds, 3),
                "build_wal_bytes": wal_bytes,
            }
        connection.close()
        return result


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))

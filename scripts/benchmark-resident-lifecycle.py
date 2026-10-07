#!/usr/bin/env python
"""Generated resident-query/lifecycle comparison; never opens a configured account.

Run this same script against the before/after source roots with the same interpreter.
All databases live in an explicitly selected synthetic scratch directory. JSON output
contains counts/timings/process counters only; RSS is not whole-system memory pressure.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch


def process_sample() -> dict:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "cpu_seconds": round(usage.ru_utime + usage.ru_stime, 6),
        "peak_rss_bytes": int(usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024)),
        "block_input_operations": usage.ru_inblock,
        "block_output_operations": usage.ru_oublock,
        "open_fds": len(os.listdir("/dev/fd")) if Path("/dev/fd").is_dir() else None,
    }


def scale_probe(scratch: Path, count: int, resident: int) -> dict:
    from sightglass.model.db import WindowDB
    from sightglass.model.repositories import WindowRepository

    with tempfile.TemporaryDirectory(prefix="synthetic-scale-", dir=scratch) as temporary:
        database = WindowDB(Path(temporary) / "window.db")
        repository = WindowRepository(database)
        with database.transaction() as connection:
            connection.execute(
                "INSERT INTO accounts(account_id,source_namespace,identity_confidence,"
                "reader_timezone,current_display_name,first_seen_at,last_seen_at) VALUES "
                "('synthetic-account','synthetic-only','high','UTC','Synthetic account',"
                "'2026-01-01','2026-01-01')"
            )
            connection.execute(
                "INSERT INTO conversations(conversation_id,account_id,source_conversation_id,"
                "kind,current_title,first_seen_at,last_seen_at) VALUES ('synthetic-group',"
                "'synthetic-account','synthetic-group','group','Synthetic group',"
                "'2026-01-01','2026-01-01')"
            )
            connection.executemany(
                "INSERT INTO messages(message_id,account_id,conversation_id,source_message_id,"
                "source_time_raw,sent_at_utc,sort_primary,sort_seq,sort_tie,"
                "sender_label_snapshot_json,kind,structured_json,first_seen_at,last_seen_at,"
                "current_state,current_generation_id,projection_epoch,body_available) "
                "VALUES (?,'synthetic-account','synthetic-group',?,'2026-01-01','2026-01-01',"
                "'2026-01-01',?,0,'{}','text','{}','2026-01-01','2026-01-01','present',"
                "'synthetic-generation','synthetic-epoch',?)",
                ((f"synthetic-message-{i}", f"synthetic-source-{i}", i, int(i < resident))
                 for i in range(count)),
            )
            # Only the fixed resident set has a current observation. The larger
            # released skeleton history stays outside the ordinary read plane.
            for i in range(resident):
                sequence = connection.execute(
                    "INSERT INTO message_observations(observation_id,message_id,observed_at,"
                    "source_generation_id,state,payload_digest,parsed_json,parser_version) "
                    "VALUES (?,?,'2026-01-01','synthetic-generation','present',"
                    "'synthetic-scale-digest','{}','synthetic-scale-fixture')",
                    (f"synthetic-observation-{i}", f"synthetic-message-{i}"),
                ).lastrowid
                connection.execute(
                    "UPDATE messages SET first_observation_seq=?,current_observation_seq=?,"
                    "text='Synthetic resident body' WHERE message_id=?",
                    (sequence, sequence, f"synthetic-message-{i}"),
                )

        def measure(call):
            ticks = [0]
            with database.read_snapshot() as connection:
                def tick():
                    ticks[0] += 1
                    return 0
                connection.set_progress_handler(tick, 100)
                started = time.perf_counter()
                result = list(call())
                elapsed = time.perf_counter() - started
                connection.set_progress_handler(None, 0)
            receipt = {
                "returned": len(result), "vm_steps_upper_bound": (ticks[0] + 1) * 100,
                "seconds": round(elapsed, 6),
            }
            if result and hasattr(result[0], "keys") and "sort_seq" in result[0].keys():
                receipt["sort_seqs"] = [row["sort_seq"] for row in result]
            return receipt

        def warm_page(direction):
            return repository.materialized_message_rows(
                "synthetic-group", projection_epoch="synthetic-epoch",
                observation_watermark=resident, limit=5, direction=direction,
            )

        return {
            "durable_messages": count, "resident_messages": resident,
            "resident_conversations": measure(lambda: repository.resident_conversation_ids(
                "synthetic-account", "synthetic-epoch"
            )),
            "warm_candidate_page": measure(lambda: repository.search_candidate_window(
                ("synthetic-group",), projection_epoch="synthetic-epoch", limit=5
            )),
            "warm_forward_page": measure(lambda: warm_page("forward")),
            "warm_backward_page": measure(lambda: warm_page("backward")),
            "warm_bounds": measure(lambda: [value for value in
                repository.materialized_observation_bounds(
                    "synthetic-group", projection_epoch="synthetic-epoch",
                    observation_watermark=resident,
                ) if value is not None]),
            "warm_read_plane": measure(lambda: [True] if
                repository.has_materialized_read_plane("synthetic-epoch") else []),
            "warm_scope_present": measure(lambda: [True] if
                repository.has_materialized_messages(
                    "synthetic-epoch", conversation_id="synthetic-group"
                ) else []),
            "empty_inbox": measure(lambda: repository.inbox_rows(
                "synthetic-account", observation_seq=0
            )),
        }


def lifecycle_probe(scratch: Path) -> dict:
    from sightglass.model.db import WindowDB
    from sightglass.model.links import LinkRepository
    from sightglass.model.repositories import WindowRepository
    from sightglass.policy.readers import ReaderContext, ReaderPolicy
    from sightglass.reader.service import ReaderService
    from sightglass.residency.repository import ResidencyRepository
    from sightglass.resources.jobs import ResourceJobService
    from sightglass.runtime.resource_worker import ResourceWorker
    from sightglass.source.direct_wechat import DirectWeChatSourceProvider
    from sightglass.source.identity import SignedTokenCodec
    from sightglass.source.synthetic import create_synthetic_source

    with tempfile.TemporaryDirectory(prefix="synthetic-lifecycle-", dir=scratch) as temporary:
        root = Path(temporary)
        source = root / "source"
        create_synthetic_source(source)
        database = WindowDB(root / "state" / "window.db")
        repository = WindowRepository(database)
        service = ReaderService(
            DirectWeChatSourceProvider(source), repository,
            ReaderContext("synthetic-reader", "Synthetic Reader", ReaderPolicy(
                mode="all_except_denylist", identity_debug=True,
            )), SignedTokenCodec(hashlib.sha256(b"synthetic-test-secret").digest()),
        )
        group = service.find_conversations(query="Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        timings = {}
        def timed(name, call):
            started = time.perf_counter()
            result = call()
            timings[name] = round(time.perf_counter() - started, 6)
            return result
        timed("cold_read", lambda: service.read_messages(
            mode="recent", conversation_id=group, limit=10
        ))
        phase_samples = {"after_cold_read": process_sample()}
        timed("warm_read", lambda: service.read_messages(
            mode="recent", conversation_id=group, limit=10
        ))
        phase_samples["after_warm_read"] = process_sample()

        def counts():
            with database.connection() as connection:
                result = {
                    table: connection.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
                    for table in ("messages", "message_observations", "message_link_projection",
                                  "message_lexical_projection", "message_lexical")
                }
                result["available_bodies"] = connection.execute(
                    "SELECT COALESCE(SUM(body_available),0) FROM messages"
                ).fetchone()[0]
                result["three_equal_current_text_copies"] = connection.execute(
                    "SELECT COALESCE(SUM(text=search_text AND "
                    "text=json_extract(structured_json,'$.text')),0) FROM messages "
                    "WHERE kind='text' AND text IS NOT NULL"
                ).fetchone()[0]
                result["ordinary_text_copy_count"] = connection.execute(
                    "SELECT COALESCE(SUM(1 + COALESCE(search_text=text,0) + "
                    "COALESCE(json_extract(structured_json,'$.text')=text,0)),0) "
                    "FROM messages WHERE kind='text' AND text IS NOT NULL"
                ).fetchone()[0]
                return result

        before = counts()
        residency = ResidencyRepository(database)
        residency.release_stock(group, plan=residency.stock_preview(group)["plan"])
        after_release = counts()
        LinkRepository(database).backfill_batch(limit=100)
        after_backfill = counts()
        # Fresh WindowDB/derived repository exercises restart without a source read.
        database = WindowDB(database.path)
        links = LinkRepository(database)
        links.request_rebuild("lexical")
        links.backfill_batch(limit=100)
        after_restart_rebuild = counts()
        phase_samples["after_release_backfill_restart_rebuild"] = process_sample()
        jobs = ResourceJobService(WindowRepository(database))
        writer_before = database.writer_status()["wait_count"]
        recovered = sum(jobs.recover_expired_leases() for _ in range(10))
        direct_writer_count = database.writer_status()["wait_count"] - writer_before
        connection_count = [0]
        original_connection = database.connection

        @contextmanager
        def counted_connection():
            connection_count[0] += 1
            with original_connection() as connection:
                yield connection

        worker = ResourceWorker(service.resource_service, jobs)
        idle_before = process_sample()
        writer_before = database.writer_status()["wait_count"]
        idle_started = time.perf_counter()
        with patch.object(database, "connection", counted_connection):
            worker.start()
            try:
                time.sleep(2.05)
            finally:
                worker.stop()
        idle_after = process_sample()
        worker_status = worker.status().as_dict()
        assert not worker_status["running"] and worker_status["last_error_code"] is None
        return {
            "before_release": before, "after_release": after_release,
            "after_backfill": after_backfill, "after_restart_rebuild": after_restart_rebuild,
            "empty_resource_recovery": {
                "calls": 10, "recovered": recovered,
                "writer_acquisitions": direct_writer_count,
            }, "seconds": timings, "process_samples": phase_samples,
            "idle_resource_worker": {
                "elapsed_seconds": round(time.perf_counter() - idle_started, 6),
                "cpu_seconds": round(idle_after["cpu_seconds"] - idle_before["cpu_seconds"], 6),
                "writer_acquisitions": database.writer_status()["wait_count"] - writer_before,
                "connection_calls": connection_count[0],
                "process_before": idle_before, "process_after": idle_after,
                "completed_jobs": worker_status["completed_count"],
            },
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--label", default="candidate")
    args = parser.parse_args()
    sys.path.insert(0, str(args.source_root.resolve() / "src"))
    args.scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
    before = process_sample()
    receipt = {
        "schema": "sightglass.synthetic-lifecycle-benchmark.v1", "label": args.label,
        "scope": "generated only; no installation, account, network or busy-source proof",
        "lifecycle": lifecycle_probe(args.scratch),
        "scale": [scale_probe(args.scratch, count, resident)
                  for count in (10_000, 100_000) for resident in (0, 10)],
        "process_before": before, "process_after": process_sample(),
    }
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()

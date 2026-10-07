"""Explicit synthetic-only acceptance through the production semantic/reader path.

This source-checkout runner generates its own canonical database. It never loads
a Sightglass runtime config, source account or Keychain item. The only credentials
come from the explicitly named private benchmark config and existing Wrangler.
Remote mutations require the authorization flag and a dedicated synthetic index.
Readback-only mode blocks document encoding/upsert and resumes stored intents.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from examples.benchmarks.cloudflare_semantic import HARD_CASES, Cloudflare
from examples.benchmarks.semantic import CASES
from sightglass.mcp.tools import ReaderTools
from sightglass.model.db import WindowDB
from sightglass.model.repositories import WindowRepository
from sightglass.operations import operation_budget
from sightglass.policy.readers import ReaderContext, ReaderPolicy
from sightglass.reader.service import ReaderService
from sightglass.semantic.cloudflare import CloudflareBackend, HttpxTransport
from sightglass.semantic.service import SEMANTIC_RECIPE, SemanticService
from sightglass.semantic.settings import ACTIVE_MODEL, SemanticSettings
from sightglass.source.direct_wechat import SyntheticSourceProvider
from sightglass.source.identity import SignedTokenCodec
from sightglass.source.parser import parse_message
from sightglass.source.synthetic import create_synthetic_source

CORPUS_RECIPE = "sightglass.synthetic-canonical-semantic.v1"
AGGREGATE = (
    "aggregate_pair",
    "虚拟伙伴的跨平台入口",
    [
        "Synthetic: 这些收录了好些 AI companion 工具，可按类别挑选",
        "https://www.synthetic-companion-atlas.example/",
        "还有这个 https://synthetic-character-garden.example/companion/?token=synthetic#fragment",
    ],
)


class ObservedTransport(HttpxTransport):
    """Record operation counts only; no URL/token/body in the public receipt."""

    def __init__(self, *, readback_only: bool) -> None:
        super().__init__()
        self.block_publication = readback_only
        self.counts: dict[str, int] = {}
        self.http_status_counts: dict[str, int] = {}

    def request(self, method, url, *, headers, content, timeout):
        operation_url = url.split("?", 1)[0]
        kind = (
            "encode"
            if "/ai/run/" in operation_url
            else "upsert"
            if operation_url.endswith("/upsert")
            else "readback"
            if operation_url.endswith("/get_by_ids")
            else "query"
            if operation_url.endswith("/query")
            else "preflight"
        )
        if self.block_publication and kind in {"encode", "upsert"}:
            raise RuntimeError("readback-only mode cannot publish documents")
        self.counts[kind] = self.counts.get(kind, 0) + 1
        status, body = super().request(
            method, url, headers=headers, content=content, timeout=timeout
        )
        outcome = f"{kind}:{status}"
        self.http_status_counts[outcome] = self.http_status_counts.get(outcome, 0) + 1
        return status, body


def _stack(root: Path):
    marker = root / "corpus.json"
    if marker.exists():
        corpus = json.loads(marker.read_text())
        if corpus.get("recipe") != CORPUS_RECIPE:
            raise RuntimeError("benchmark corpus recipe differs")
        source = root / "source"
    else:
        if (root / "state" / "window.db").exists() or (root / "source").exists():
            raise RuntimeError("refuse an existing unmarked source/database")
        source = create_synthetic_source(root / "source")
        corpus = None
    provider = SyntheticSourceProvider(source)
    repository = WindowRepository(WindowDB(root / "state" / "window.db"))
    reader = ReaderContext(
        "synthetic-cf-acceptance", "Synthetic Reader", ReaderPolicy(mode="all_except_denylist")
    )
    service = ReaderService(
        provider,
        repository,
        reader,
        SignedTokenCodec(hashlib.sha256(CORPUS_RECIPE.encode()).digest()),
    )
    tools = ReaderTools(service)
    if corpus is not None:
        return repository, service, tools, corpus
    service.sync_source_once(initial_tail=100, conversation_limit=100)
    account = repository.active_account_ids()[0]
    group = next(row for row in repository.account_conversations(account) if row["kind"] == "group")
    group_id = str(group["conversation_id"])
    epoch = service._projection_inventory_epoch()
    seed = repository.materialized_message_rows(
        group_id,
        projection_epoch=epoch,
        observation_watermark=repository.observation_watermark(),
        limit=1,
        direction="forward",
    )[0]
    context = repository.conversation_context(group_id)
    with provider.snapshot() as snapshot:
        original = provider.get_message(
            context["source_account_key"], seed["source_message_id"], snapshot
        )
    assert original is not None
    cases = []
    base = datetime(2026, 9, 20, tzinfo=UTC)
    with repository.database.transaction():
        for case_index, (name, concept, texts) in enumerate([*CASES, *HARD_CASES, AGGREGATE]):
            ids = []
            for index, text in enumerate(texts):
                instant = base + timedelta(minutes=case_index * 20, seconds=index * 10)
                source_message = replace(
                    original,
                    source_message_id=f"synthetic-cf-canonical-{name}-{index}",
                    wechat_type=1,
                    raw_content=text,
                    sent_at_utc=instant.isoformat(),
                    observed_at_utc=(instant + timedelta(seconds=1)).isoformat(),
                    source_time_raw=instant.isoformat(),
                    source_rowid=50_000 + case_index * 100 + index,
                )
                ids.append(
                    repository.upsert_message(
                        account,
                        group_id,
                        seed["sender_id"],
                        seed["sender_membership_id"],
                        source_message,
                        parse_message(source_message),
                        projection_epoch=epoch,
                    )
                )
            cases.append({"name": name, "concept": concept, "message_ids": ids})
    corpus = {"recipe": CORPUS_RECIPE, "account": account, "group": group_id, "cases": cases}
    marker.write_text(json.dumps(corpus, ensure_ascii=False))
    os.chmod(marker, 0o600)
    return repository, service, tools, corpus


def _reader_state(repository: WindowRepository) -> dict[str, list[tuple]]:
    with repository.database.connection() as connection:
        return {
            table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table}")]
            for table in (
                "reader_timeline_cursors",
                "reader_update_cursors",
                "reader_deliveries",
                "voice_jobs",
                "resource_jobs",
            )
        }


def run(config: dict[str, Any], root: Path, *, readback_only: bool) -> dict[str, Any]:
    cf = Cloudflare(config)  # refuses a non-synthetic index before credential access
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    if readback_only and not (root / "corpus.json").exists():
        raise RuntimeError("readback-only requires an existing generated corpus")
    repository, reader, tools, corpus = _stack(root)
    transport = ObservedTransport(readback_only=readback_only)
    settings = SemanticSettings(
        enabled=True,
        external_data_authorized=True,
        cf_account_id=config["account_id"],
        index_name=config["index_name"],
        source_account_id=corpus["account"],
        conversation_ids=(corpus["group"],),
        timeout_seconds=8,
    )
    lane = SemanticService(
        repository,
        reader.reader,
        settings=settings,
        backend=CloudflareBackend(settings, cf.token, transport=transport),
        epoch_factory=reader._projection_inventory_epoch,
        sidecar_path=root / "semantic" / "index.db",
    )
    reader.semantic = lane
    before = _reader_state(repository)
    indexing = []
    started = time.perf_counter()
    try:
        for _ in range(8):
            with operation_budget(60):
                result = lane.index_once(limit=32)
            indexing.append(result)
            if result.get("failure") or not result.get("scanned"):
                break
        status = lane.status()
        report: dict[str, Any] = {
            "schema": "sightglass.synthetic-semantic-retrieval.v1",
            "synthetic_only": True,
            "model": ACTIVE_MODEL,
            "recipe": SEMANTIC_RECIPE,
            "corpus_recipe": CORPUS_RECIPE,
            "case_count": len(corpus["cases"]),
            "readback_only": readback_only,
            "index_batches": indexing,
            "status": status,
            "publication_seconds": round(time.perf_counter() - started, 4),
        }
        if status["state"] != "ready" or status["pending"]:
            report.update(complete=False, operation_counts=transport.counts)
            return report
        publication_counts = dict(transport.counts)
        transport.block_publication = False  # fixed synthetic query encoding is now allowed
        results = []
        for case in corpus["cases"]:
            query_started = time.perf_counter()
            result = tools.wechat_retrieve(
                case["concept"],
                account_id=corpus["account"],
                conversation_ids=[corpus["group"]],
                kinds=["message"],
                limit=10,
            )
            contexts = result.get("contexts", [])
            expected = set(case["message_ids"])
            first = contexts[0] if contexts else {}
            focus = set(first.get("focus_message_ids", []))
            all_focus = {identity for item in contexts for identity in item["focus_message_ids"]}
            row = {
                "case": case["name"],
                "semantic_state": result.get("lanes", {}).get("semantic"),
                "top_context_relevant": bool(expected & focus),
                "first_page_relevant": bool(expected & all_focus),
                "first_context_focus_purity": len(expected & focus) / max(1, len(focus)),
                "query_seconds": round(time.perf_counter() - query_started, 4),
                "canonical_ids_only": all(
                    repository.frozen_message_rows((identity,)) for identity in all_focus
                ),
            }
            if error := result.get("semantic_index_receipt", {}).get("error"):
                row["semantic_error"] = error
            if case["name"] == "aggregate_pair":
                relevant_context = next(
                    (
                        (position, item)
                        for position, item in enumerate(contexts, 1)
                        if expected & set(item["focus_message_ids"])
                    ),
                    (None, {}),
                )
                row["pair_context_position"] = relevant_context[0]
                row["both_original_links"] = {
                    "www.synthetic-companion-atlas.example",
                    "synthetic-character-garden.example",
                } <= {link["normalized_host"] for link in relevant_context[1].get("links", [])}
            results.append(row)
            if row["semantic_state"] != "ready":
                break
        report.update(
            complete=len(results) == len(corpus["cases"])
            and all(
                row["semantic_state"] == "ready"
                and row["first_page_relevant"]
                and row["canonical_ids_only"]
                for row in results
            )
            and bool(results[-1].get("both_original_links"))
            and before == _reader_state(repository),
            cases=results,
            publication_operation_counts=publication_counts,
            operation_counts=transport.counts,
            http_status_counts=transport.http_status_counts,
            reader_state_unchanged=before == _reader_state(repository),
            page_limit=10,
        )
        return report
    finally:
        tools.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--authorize-cloudflare-synthetic", action="store_true")
    parser.add_argument("--readback-only", action="store_true")
    args = parser.parse_args()
    if not args.authorize_cloudflare_synthetic:
        parser.error("requires explicit synthetic-only Cloudflare authorization")
    report = run(json.loads(args.config.read_text()), args.out, readback_only=args.readback_only)
    destination = args.out / "report.json"
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    os.chmod(destination, 0o600)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not report["complete"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()

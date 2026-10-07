"""Explicit synthetic-only Workers AI / Vectorize experiment, outside daemon code.

Requires operator authorization and a private config containing account_id,
index_name (must start sightglass-synthetic-), and an installed Wrangler CLI path.
Credentials stay in Wrangler and process memory. No configured Sightglass runtime
or account data is read. See docs/benchmarks/README.md for the external boundary.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import struct
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

from examples.benchmarks.semantic import CASES, build_messages, build_units, score_ranked
from sightglass.semantic.encoder import cosine

RECIPE = "sg-synthetic-cf-v1"
MODELS = {"bge-m3": "@cf/baai/bge-m3"}
QUERY_CACHE_TAG = (
    "Retrieve conversation messages and shared resources relevant to the search query."
)
HARD_CASES = [
    (
        "parallel_rail",
        "欧洲铁路旅行的通票和单程票怎么选",
        [
            "Synthetic: 我们沿着欧洲铁路旅行，交通预算需要先算清楚。",
            "连续乘车很多天选铁路通票，少量旅程比较各段票价。",
            "https://synthetic-rail.example/pass-comparison",
        ],
    ),
    (
        "parallel_vectors",
        "语义检索余弦近邻索引的配置",
        [
            "Synthetic: 这边讨论数据库索引，不是旅行路线。",
            "把文本编码成向量，用 cosine 相似度寻找最近的候选。",
            "https://synthetic-vector.example/index-setup",
        ],
    ),
    (
        "long_debate",
        "copyleft 是否禁止商业销售以及网络服务的公开源码要求",
        [
            "Synthetic: 先明确 copyleft 的商业使用和再分发是两个问题。",
            "收费销售并不自动违反这类许可证。",
            "关键是分发时仍要满足提供相应源代码等要求。",
            "如果提供网络服务，AGPL 还有与远程用户相关的条款。",
            "商业公司也可以遵守这些条件后销售软件。",
            "用户不能把公开代码直接理解成放弃版权。",
            "我们讨论的是软件，不是宣传图片的授权。",
            "许可义务要对照触发行为和具体版本。",
            "所以不要把收费和违法直接画等号。",
            "再分发和仅在内部使用的情形需要分开。",
            "网络交互的源码要求也要单独检查。",
            "这段讨论的结论是允许商业活动但保留适用的源码义务。",
        ],
    ),
    (
        "mixed_alias",
        "那个 HoloNest 角色聚合聊天项目的入口",
        [
            "Synthetic HoloNest / 全息巢把多个 AI companion 的会话放到一个工作区。",
            "它也叫角色花园；不同供应商的虚拟角色都能在同一入口使用。",
            "https://synthetic-holonest.example/",
        ],
    ),
]


def hard_messages() -> list[dict]:
    messages = []
    for case, (label, _, texts) in enumerate(HARD_CASES):
        conversation = 10 if case < 2 else 10 + case
        for index, text in enumerate(texts):
            messages.append(
                {
                    "id": f"hard-{case}-{index}",
                    "label": label,
                    "conversation": conversation,
                    "time": index * 10 + (5 if case == 1 else 0),
                    "text": text,
                    "reply_root": f"hard-{case}-0",
                }
            )
    return sorted(messages, key=lambda row: (row["conversation"], row["time"], row["id"]))


def reply_units(messages: list[dict]) -> list[dict]:
    """Explicit synthetic reply families, never inferred from time or label."""
    groups: dict[tuple, list[dict]] = {}
    for row in messages:
        groups.setdefault((row["conversation"], row["reply_root"]), []).append(row)
    return [
        {
            "members": tuple(row["id"] for row in rows),
            "labels": tuple(row["label"] for row in rows),
            "text": "\n".join(row["text"] for row in rows),
        }
        for rows in groups.values()
    ]


class Cloudflare:
    def __init__(self, config: dict) -> None:
        self.config = config
        if not config["index_name"].startswith("sightglass-synthetic-"):
            raise ValueError("use a dedicated sightglass-synthetic- index")
        self.token = self.load_token(refresh=True)
        self.base = "https://api.cloudflare.com/client/v4/accounts/" + urllib.parse.quote(
            config["account_id"], safe=""
        )
        self.vector_path = "/vectorize/v2/indexes/" + urllib.parse.quote(
            config["index_name"], safe=""
        )

    def load_token(self, *, refresh: bool = False) -> str:
        if refresh:
            subprocess.run(
                ["node", self.config["wrangler"], "whoami"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=60,
            )
        result = subprocess.run(
            ["node", self.config["wrangler"], "auth", "token", "--json"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=60,
        )
        value = json.loads(result.stdout)
        if value["type"] not in {"oauth", "api_token"} or not value["token"]:
            raise ValueError("Wrangler authentication unavailable")
        return value["token"]

    def request(self, path: str, body=None, *, multipart: bool = False):
        result = subprocess.run(
            ["node", str(Path(__file__).with_name("cloudflare_transport.mjs"))],
            input=json.dumps(
                {
                    "account_id": self.config["account_id"],
                    "token": self.token,
                    "path": path,
                    "body": body,
                    "multipart": multipart,
                }
            ).encode(),
            capture_output=True,
            timeout=70,
        )
        if result.returncode:
            raise RuntimeError("Cloudflare " + result.stderr.decode())
        return json.loads(result.stdout)

    def ensure_index(self, *, create: bool = True) -> None:
        indexes = self.request("/vectorize/v2/indexes")
        found = next((row for row in indexes if row["name"] == self.config["index_name"]), None)
        if found is None:
            if not create:
                raise RuntimeError("experiment index does not exist")
            found = self.request(
                "/vectorize/v2/indexes",
                {
                    "name": self.config["index_name"],
                    "description": "Sightglass synthetic-only semantic experiment",
                    "config": {"dimensions": 1024, "metric": "cosine"},
                },
            )
        if found["config"]["dimensions"] != 1024 or found["config"]["metric"] != "cosine":
            raise RuntimeError("experiment index configuration differs")

    def encode(self, model: str, texts: list[str], *, query: bool = False) -> list[tuple]:
        if model != MODELS["bge-m3"]:
            raise ValueError("only the selected BGE-M3 model is supported")
        body = {"text": texts}
        vectors = self.request("/ai/run/" + model, body)["data"]
        if len(vectors) != len(texts) or any(
            len(vector) != 1024 or not all(math.isfinite(x) for x in vector) or not any(vector)
            for vector in vectors
        ):
            raise RuntimeError("invalid embedding batch")
        return [struct.unpack("<1024f", struct.pack("<1024f", *vector)) for vector in vectors]

    def readback(self, expected: list[dict]) -> list[dict]:
        rows = []
        for start in range(0, len(expected), 20):
            batch = expected[start : start + 20]
            actual = self.request(
                self.vector_path + "/get_by_ids", {"ids": [x["id"] for x in batch]}
            )
            wanted = {row["id"]: row for row in batch}
            seen = set()
            for row in actual:
                identity = row["id"]
                if identity not in wanted or identity in seen:
                    raise RuntimeError("unexpected vector identity")
                seen.add(identity)
                target = wanted[identity]
                values = struct.unpack("<1024f", struct.pack("<1024f", *row["values"]))
                if (
                    row["metadata"] != target["metadata"]
                    or row["namespace"] != target["namespace"]
                    or values != tuple(target["values"])
                ):
                    raise RuntimeError("existing experiment vector differs; refusing overwrite")
            rows.extend(actual)
        return rows

    def publish(self, rows: list[dict], journal: Path) -> tuple[float, bool]:
        started = time.perf_counter()
        found = {row["id"] for row in self.readback(rows)}
        queued = json.loads(journal.read_text()) if journal.exists() else {}
        expected = {row["id"]: row["metadata"]["input"] for row in rows}
        if any(expected.get(identity) != digest for identity, digest in queued.items()):
            raise RuntimeError("queued experiment identity differs")
        missing = [row for row in rows if row["id"] not in found and row["id"] not in queued]
        if missing:
            # Record intent before the request. An interrupted/ambiguous submission
            # resumes by readback, never blindly submits the same paid mutation.
            queued.update({row["id"]: row["metadata"]["input"] for row in missing})
            journal.write_text(json.dumps(queued))
            journal.chmod(0o600)
            self.request(
                self.vector_path + "/upsert?unparsable-behavior=error",
                "\n".join(json.dumps(row) for row in missing) + "\n",
                multipart=True,
            )
        deadline = time.monotonic() + 300
        while len(self.readback(rows)) != len(rows):
            if time.monotonic() >= deadline:
                raise RuntimeError("vector readback incomplete; rerun readback with the same cache")
            time.sleep(3)
        return time.perf_counter() - started, not missing


def encoded_cache(
    cloud: Cloudflare,
    model: str,
    inputs: list[str],
    cache: Path,
    *,
    query: bool = False,
    cached_only: bool = False,
) -> tuple[list, float, bool, str]:
    identity = (
        [model, RECIPE, "query", QUERY_CACHE_TAG, inputs] if query else [model, RECIPE, inputs]
    )
    fingerprint = hashlib.sha256(json.dumps(identity).encode()).hexdigest()
    cache_file = cache / (fingerprint + ".json")
    if cache_file.exists():
        saved = json.loads(cache_file.read_text())
        return (
            saved["vectors"],
            saved["build_seconds"],
            True,
            fingerprint,
        )
    if cached_only:
        raise RuntimeError("embedding cache is incomplete; readback-only does not call Workers AI")
    started = time.perf_counter()
    vectors = []
    for offset in range(0, len(inputs), 16):
        vectors.extend(cloud.encode(model, inputs[offset : offset + 16], query=query))
    seconds = time.perf_counter() - started
    cache_file.write_text(json.dumps({"vectors": vectors, "build_seconds": seconds}))
    cache_file.chmod(0o600)
    return vectors, seconds, False, fingerprint


def run(config: dict, cache: Path, *, readback_only: bool = False) -> dict:
    cloud = Cloudflare(config)
    cloud.ensure_index(create=not readback_only)
    cache.mkdir(mode=0o700, parents=True, exist_ok=True)
    result = {
        "schema": "sightglass.synthetic-cloudflare-semantic.v1",
        "recipe": RECIPE,
        "data": "generated synthetic only",
        "dimensions": 1024,
        "production_semantic_lane": "disabled",
        "profiles": {},
        "topology_context_meaning": "30-second same-conversation temporal proxy (Apple baseline)",
        "reply_context_meaning": "explicit synthetic reply family (hard suite only)",
        "timing": "client wall time; embedding, publication/readback and query are separate",
    }
    planned, all_rows = [], []
    for tag, model in MODELS.items():
        profile = {}
        for suite, cases, messages in [
            ("baseline8", CASES, build_messages()),
            ("hard4", HARD_CASES, hard_messages()),
        ]:
            queries, query_seconds, query_cached, _ = encoded_cache(
                cloud,
                model,
                [case[1] for case in cases],
                cache,
                query=True,
                cached_only=readback_only,
            )
            kinds = ["message", "local_window", "topology_context"]
            if suite == "hard4":
                kinds.append("reply_context")
            for kind in kinds:
                units = (
                    reply_units(messages)
                    if kind == "reply_context"
                    else build_units(messages, kind)
                )
                vectors, build_seconds, cached, fingerprint = encoded_cache(
                    cloud, model, [unit["text"] for unit in units], cache, cached_only=readback_only
                )
                namespace = f"{RECIPE}-{tag}-{suite}-{kind}"
                rows = [
                    {
                        "id": f"{namespace}-{i}",
                        "namespace": namespace,
                        "values": vector,
                        "metadata": {"unit": i, "input": fingerprint},
                    }
                    for i, vector in enumerate(vectors)
                ]
                scores, exact_ranks = [], []
                started = time.perf_counter()
                for i, (label, _, _) in enumerate(cases):
                    exact = sorted(
                        range(len(units)),
                        key=lambda j: cosine(tuple(queries[i]), tuple(vectors[j])),
                        reverse=True,
                    )[:5]
                    exact_ranks.append(exact)
                    scores.append(score_ranked(label, exact, units, messages))
                local_ms = (time.perf_counter() - started) * 1000 / len(cases)
                key = f"{suite}/{kind}"
                profile[key] = {
                    "cases_count": len(cases),
                    "units": len(units),
                    "model": model,
                    "document_embedding_seconds": round(build_seconds, 4),
                    "document_cache_used": cached,
                    "query_cache_used": query_cached,
                    "query_embedding_ms_per_case_amortized": round(
                        query_seconds * 1000 / len(cases), 3
                    ),
                    "exact_local_cosine_ms_per_case": round(local_ms, 3),
                    "raw_float32_vector_bytes": len(units) * 1024 * 4,
                    "remote_physical_bytes": None,
                    "exact_cosine_recall_at_5": statistics.mean(
                        row["recall_at_5"] for row in scores
                    ),
                    "exact_cosine_first_page_correct": statistics.mean(
                        row["first_page_correct"] for row in scores
                    ),
                    "exact_cosine_cases": scores,
                    "vectorize_state": "pending_readback",
                }
                planned.append((tag, key, cases, messages, units, queries, rows, exact_ranks))
                all_rows.extend(rows)
                print(f"{tag} {key}: embedding complete", file=sys.stderr, flush=True)
        result["profiles"][tag] = profile
        (cache / "progress.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
    result["vector_rows"] = len(all_rows)
    if readback_only:
        started = time.perf_counter()
        visible = {row["id"] for row in cloud.readback(all_rows)}
        result["vectorize_publication"] = {
            "state": "full_readback_verified" if len(visible) == len(all_rows) else "incomplete",
            "visible_rows": len(visible),
            "expected_rows": len(all_rows),
            "seconds": round(time.perf_counter() - started, 4),
            "readback_only": True,
        }
    else:
        try:
            seconds, reused = cloud.publish(all_rows, cache / "queued.json")
        except RuntimeError as error:
            result["vectorize_publication"] = {"state": "incomplete", "reason": str(error)}
            return result
        visible = {row["id"] for row in all_rows}
        result["vectorize_publication"] = {
            "state": "full_readback_verified",
            "seconds": round(seconds, 4),
            "rows_reused": reused,
        }
    for tag, key, cases, messages, units, queries, rows, exact_ranks in planned:
        if any(row["id"] not in visible for row in rows):
            continue
        mapping = {row["id"]: i for i, row in enumerate(rows)}
        scores, latencies = [], []
        for i, (label, _, _) in enumerate(cases):
            started = time.perf_counter()
            matches = cloud.request(
                cloud.vector_path + "/query",
                {
                    "vector": queries[i],
                    "namespace": rows[0]["namespace"],
                    "topK": 5,
                    "returnMetadata": "all",
                },
            )["matches"]
            latencies.append((time.perf_counter() - started) * 1000)
            if not matches or any(
                row["id"] not in mapping or row["metadata"] != rows[mapping[row["id"]]]["metadata"]
                for row in matches
            ):
                raise RuntimeError(
                    "query has no candidates or failed local manifest identity fence"
                )
            ranked = [mapping[row["id"]] for row in matches]
            score = score_ranked(label, ranked, units, messages)
            score["ann_top5_set_matches_exact"] = set(ranked) == set(exact_ranks[i])
            canonical = {row["id"]: row for row in messages}
            members = [canonical[mid] for mid in units[ranked[0]]["members"]]
            score["manifest_recovery"] = len({row["conversation"] for row in members}) == 1
            scores.append(score)
        result["profiles"][tag][key].update(
            {
                "vectorize_state": "query_verified",
                "cases": scores,
                "vectorize_query_ms_median": round(statistics.median(latencies), 3),
                "recall_at_5": statistics.mean(row["recall_at_5"] for row in scores),
                "first_page_correct": statistics.mean(row["first_page_correct"] for row in scores),
            }
        )
        (cache / "progress.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
        print(f"{tag} {key}: query complete", file=sys.stderr, flush=True)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--cache", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--readback-only", action="store_true")
    arguments = parser.parse_args()
    try:
        report = run(
            json.loads(arguments.config.read_text()),
            arguments.cache,
            readback_only=arguments.readback_only,
        )
    except (RuntimeError, subprocess.SubprocessError) as error:
        print(f"Synthetic experiment stopped: {type(error).__name__}: {error}", file=sys.stderr)
        raise SystemExit(1) from None
    arguments.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    if report["vectorize_publication"]["state"] == "incomplete":
        print(
            "Vectorize readback incomplete; embedding evidence retained in output.", file=sys.stderr
        )
        raise SystemExit(2)

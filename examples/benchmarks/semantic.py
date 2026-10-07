"""Compare real local embeddings on synthetic message/window/topology units."""

from __future__ import annotations

import argparse
import json
import sqlite3
import struct
import tempfile
import time
from pathlib import Path

from sightglass.semantic.encoder import AppleSentenceEncoder, cosine

CASES = [
    (
        "projects",
        "聚合 AI companion 项目",
        [
            "把多个虚拟角色的对话入口放在一起，集中管理聊天记录。",
            "https://synthetic-garden.example/",
            "另一个平台也能整合不同厂商的聊天角色。",
        ],
    ),
    (
        "law",
        "跨境投资仲裁管辖权",
        [
            "外国企业与东道国争议应交给国际法庭。",
            "先确认双方的条约同意以及案件受理条件。",
            "国家豁免与投资争端机制的关系需要单独分析。",
        ],
    ),
    (
        "sleep",
        "缓解失眠的方法",
        [
            "晚上总是清醒到凌晨，第二天头很痛。",
            "睡前先放下手机，卧室灯光暗一些。",
            "咖啡下午就不要喝了。",
        ],
    ),
    (
        "travel",
        "坐火车去欧洲旅游",
        [
            "我们准备沿着铁路把几个申根城市串起来。",
            "订通票还是分段买票便宜？",
            "地图和车次整理在这个行程文件里。",
        ],
    ),
    (
        "vector",
        "embedding similarity search",
        [
            "向量表示可以把意思接近的文本放在附近。",
            "余弦距离用于近邻排序。",
            "本地索引避免把私人语料交给外部模型。",
        ],
    ),
    (
        "pronoun",
        "合同解除的条件",
        [
            "卖方迟延交货已经超过双方约定的期限。",
            "这种情况能不能终止交易？",
            "它造成了根本违约，买方可以发出书面通知。",
        ],
    ),
    (
        "image",
        "宣传海报的排版",
        [
            "这张图标题和正文挤在一起，视觉层次不清楚。",
            "[synthetic image resource]",
            "增大留白，字体使用同一套比例。",
        ],
    ),
    (
        "debate",
        "开源许可证限制商业使用",
        [
            "软件作者希望公开代码，但不希望公司直接卖副本。",
            "使用者需要看授权条款是否允许收费分发。",
            "另外文档与程序可以分别采用不同的许可。",
        ],
    ),
]


def build_messages() -> list[dict]:
    """The unchanged eight-case corpus shared by local and cloud experiments."""
    messages = []
    for case, (label, query, texts) in enumerate(CASES):
        for index, text in enumerate(texts):
            messages.append(
                {
                    "id": f"m{case}-{index}",
                    "label": label,
                    "conversation": case % 2,
                    "time": case * 300 + index * 10,
                    "text": text,
                }
            )
        # Interleaved same-conversation distraction is not automatically topic evidence.
        messages.append(
            {
                "id": f"noise{case}",
                "label": "noise",
                "conversation": case % 2,
                "time": case * 300 + 15,
                "text": f"今天食堂的新菜单很好吃，Synthetic unrelated {case}。",
            }
        )
    return messages


def build_units(messages: list[dict], kind: str) -> list[dict]:
    units = []
    for message in messages:
        radius = 0 if kind == "message" else 10 if kind == "local_window" else 30
        members = [
            row
            for row in messages
            if row["conversation"] == message["conversation"]
            and abs(row["time"] - message["time"]) <= radius
        ]
        key = tuple(row["id"] for row in members)
        if not any(unit["members"] == key for unit in units):
            units.append(
                {
                    "members": key,
                    "labels": tuple(row["label"] for row in members),
                    "text": "\n".join(row["text"] for row in members),
                }
            )
    return units


def score_ranked(label: str, ranked: list[int], units: list[dict], messages: list[dict]) -> dict:
    labels = [units[index]["labels"] for index in ranked[:5]]
    top = labels[0]
    return {
        "case": label,
        "recall_at_5": any(label in item for item in labels),
        "first_page_correct": label in top,
        "purity": round(top.count(label) / len(top), 3),
        "false_merge": len(set(top) - {"noise"}) > 1,
        "boundary_loss": len(
            {row["id"] for row in messages if row["label"] == label}
            - set(units[ranked[0]]["members"])
        ),
    }


def run(helper: Path) -> dict:
    messages = build_messages()
    result = {
        "schema": "sightglass.synthetic-semantic-benchmark.v1",
        "cases": len(CASES),
        "profiles": {},
    }
    for language in ("zh-Hans", "en"):
        encoder = AppleSentenceEncoder(helper, language=language)
        profile = {}
        query_vectors = encoder.encode(tuple(case[1] for case in CASES))
        for kind in ("message", "local_window", "topology_context"):
            units = build_units(messages, kind)
            started = time.perf_counter()
            batch = encoder.encode(tuple(unit["text"] for unit in units))
            build_seconds = time.perf_counter() - started
            with tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "vectors.db"
                connection = sqlite3.connect(path)
                connection.execute("CREATE TABLE vectors(id INTEGER PRIMARY KEY, vector BLOB)")
                connection.executemany(
                    "INSERT INTO vectors VALUES (?,?)",
                    (
                        (index, struct.pack(f"<{batch.dimensions}f", *vector))
                        for index, vector in enumerate(batch.vectors)
                    ),
                )
                connection.commit()
                scores = []
                started = time.perf_counter()
                for case, (label, _, _) in enumerate(CASES):
                    rows = connection.execute("SELECT * FROM vectors").fetchall()
                    ranked = sorted(
                        (
                            (
                                cosine(
                                    query_vectors.vectors[case],
                                    struct.unpack(f"<{batch.dimensions}f", blob),
                                ),
                                index,
                            )
                            for index, blob in rows
                        ),
                        reverse=True,
                    )
                    scores.append(
                        score_ranked(label, [index for _, index in ranked], units, messages)
                    )
                query_seconds = time.perf_counter() - started
                connection.close()
                size = path.stat().st_size
            profile[kind] = {
                "units": len(units),
                "model": batch.model,
                "revision": batch.revision,
                "dimensions": batch.dimensions,
                "build_seconds": round(build_seconds, 4),
                "query_ms_per_case": round(query_seconds * 1000 / len(CASES), 4),
                "sqlite_bytes": size,
                "bytes_per_unit": round(size / len(units), 2),
                "recall_at_5": sum(row["recall_at_5"] for row in scores) / len(CASES),
                "first_page_correct": sum(row["first_page_correct"] for row in scores) / len(CASES),
                "cases": scores,
            }
        result["profiles"][language] = profile
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--helper", required=True, type=Path)
    arguments = parser.parse_args()
    print(json.dumps(run(arguments.helper), ensure_ascii=False, indent=2))

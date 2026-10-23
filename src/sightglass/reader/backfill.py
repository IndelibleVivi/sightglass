"""Signed, job-local traversal state; source coverage and residency are independent."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from sightglass.contracts.common import SourceSortKey
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.source.identity import SignedTokenCodec

SCHEMA = "sightglass.backfill-progress.v1"
PERMANENT_REASONS = frozenset(
    {
        "backfill_account_changed",
        "backfill_epoch_changed",
        "backfill_cursor_invalid",
        "backfill_selected_generation_changed",
    }
)


def invalid(reason: str) -> SightglassError:
    return SightglassError(
        ErrorCode.SOURCE_GENERATION_CHANGED,
        "历史任务绑定已失效，请显式重新排队；原任务进度已保留。",
        retryable=False,
        details={"reason": reason, "action": "queue_backfill", "requeue_required": True},
    )


def scope(job: Any, target: Any, epoch: str) -> str:
    return hashlib.sha256(
        json.dumps(
            [
                SCHEMA,
                job["job_id"],
                job["account_id"],
                job["conversation_id"],
                job["requested_after"],
                job["requested_before"],
                job["max_messages"],
                target.source_account_key,
                target.source_conversation_id,
                epoch,
            ],
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


@dataclass(frozen=True)
class Progress:
    before: SourceSortKey | None = None
    generations: tuple[tuple[str, str], ...] = ()
    processed: int = 0

    def encode(self, codec: SignedTokenCodec, binding: str) -> str:
        return codec.encode(
            {
                "schema": SCHEMA,
                "scope": binding,
                "processed": self.processed,
                "before": list(self.before.as_tuple()) if self.before else None,
                "generations": [list(item) for item in self.generations],
            }
        )


def decode(codec: SignedTokenCodec, token: str, binding: str, processed: int) -> Progress:
    try:
        value = codec.decode(token)
        if (
            value.get("schema") != SCHEMA
            or value.get("scope") != binding
            or type(value.get("processed")) is not int
            or value["processed"] != processed
        ):
            raise ValueError
        position = value.get("before")
        before = None
        if position is not None:
            if (
                not isinstance(position, list)
                or len(position) != 4
                or not isinstance(position[0], str)
                or type(position[1]) is not int
                or type(position[2]) is not int
                or not isinstance(position[3], str)
            ):
                raise ValueError
            before = SourceSortKey(*position)
        raw = value.get("generations")
        if not isinstance(raw, list) or any(
            not isinstance(item, list)
            or len(item) != 2
            or any(not isinstance(part, str) or not part for part in item)
            for item in raw
        ):
            raise ValueError
        generations = tuple((item[0], item[1]) for item in raw)
        if len(dict(generations)) != len(generations) or (
            processed and (before is None or not generations)
        ):
            raise ValueError
        return Progress(before, generations, processed)
    except (SightglassError, ValueError, TypeError, KeyError) as exc:
        raise invalid("backfill_cursor_invalid") from exc

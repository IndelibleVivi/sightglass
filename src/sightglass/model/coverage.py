"""Durable validated source windows, independent of observed min/max positions."""

from __future__ import annotations

import json
from typing import Any

from sightglass.contracts.common import SourceSortKey

from .db import WindowDB


def position(key: SourceSortKey) -> str:
    return json.dumps(key.as_tuple(), separators=(",", ":"))


def key_from_position(value: str | None) -> SourceSortKey | None:
    return SourceSortKey(*json.loads(value)) if value else None


def state_frontier(state: Any) -> SourceSortKey | None:
    if state is None or not state["coverage_version"] or state["tail_sort_primary"] is None:
        return None
    return SourceSortKey(
        str(state["tail_sort_primary"]),
        int(state["tail_sort_seq"]),
        int(state["tail_sort_tie"]),
        str(state["tail_source_message_id"]),
    )


def _window_key(row: Any, side: str) -> SourceSortKey:
    return SourceSortKey(
        str(row[f"{side}_primary"]),
        int(row[f"{side}_seq"]),
        int(row[f"{side}_tie"]),
        str(row[f"{side}_message_id"]),
    )


def record_window(
    database: WindowDB,
    conversation_id: str,
    epoch: str,
    lower: SourceSortKey,
    upper: SourceSortKey,
    observed_at: str,
) -> None:
    """Union overlapping intervals, including an explicitly revalidated read anchor.

    Existing intervals are disjoint. A bounded source page can merge only the
    windows intersecting that page; unrelated history is never scanned or rewritten.
    """
    with database.transaction() as connection:
        rows = connection.execute(
            """SELECT * FROM source_read_windows
            WHERE conversation_id=? AND projection_epoch=?
              AND (lower_primary,lower_seq,lower_tie,lower_message_id) <= (?,?,?,?)
              AND (upper_primary,upper_seq,upper_tie,upper_message_id) >= (?,?,?,?)""",
            (conversation_id, epoch, *upper.as_tuple(), *lower.as_tuple()),
        ).fetchall()
        for row in rows:
            lower = min(lower, _window_key(row, "lower"), key=lambda key: key.as_tuple())
            upper = max(upper, _window_key(row, "upper"), key=lambda key: key.as_tuple())
            connection.execute(
                "DELETE FROM source_read_windows WHERE window_id=?", (row["window_id"],)
            )
        connection.execute(
            """INSERT INTO source_read_windows(
                conversation_id, projection_epoch,
                lower_primary,lower_seq,lower_tie,lower_message_id,
                upper_primary,upper_seq,upper_tie,upper_message_id,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (conversation_id, epoch, *lower.as_tuple(), *upper.as_tuple(), observed_at),
        )


def containing_window(
    database: WindowDB,
    conversation_id: str,
    epoch: str,
    focus: SourceSortKey,
) -> tuple[SourceSortKey, SourceSortKey] | None:
    with database.connection() as connection:
        # Seek the most recent lower boundary. Disjoint intervals guarantee it is
        # the sole possible containing window; no conversation-wide metadata scan.
        row = connection.execute(
            """SELECT * FROM source_read_windows
            WHERE conversation_id=? AND projection_epoch=?
              AND (lower_primary,lower_seq,lower_tie,lower_message_id) <= (?,?,?,?)
            ORDER BY lower_primary DESC,lower_seq DESC,lower_tie DESC,lower_message_id DESC
            LIMIT 1""",
            (conversation_id, epoch, *focus.as_tuple()),
        ).fetchone()
    if row is None:
        return None
    lower, upper = _window_key(row, "lower"), _window_key(row, "upper")
    return (lower, upper) if focus.as_tuple() <= upper.as_tuple() else None

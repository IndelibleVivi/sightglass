"""Casefolded trigram candidate recall; canonical literal checks remain authoritative."""

from __future__ import annotations

import hashlib
import sqlite3
from typing import Any

from sightglass.contracts.common import utc_now

from .current_body import current_search_document
from .resident_read import resident_body_predicate

LEXICAL_RECIPE = "sightglass.casefold-trigram.v1"


def candidate_expression(query: str) -> str | None:
    # A single trigram per eligible literal term is a necessary condition, never
    # evidence. Short-only queries retain the bounded timeline fallback.
    grams = {word[:3] for word in query.casefold().split() if len(word) >= 3 and "\0" not in word}
    return " AND ".join('"' + word.replace('"', '""') + '"' for word in sorted(grams)) or None


def publish_lexical(connection: sqlite3.Connection, row: Any, *, historical: bool) -> bool:
    # Compare-and-swap against the *current* row, not the caller's snapshot. A
    # direct caller may hold a prepared/stale row: re-read inside the writer and
    # require the same observation episode AND the same canonical body evidence.
    # The episode fence alone already rejects A->B->A (each state change appends a
    # new observation even when the payload repeats) and rehydration keeps its
    # original episode; the body-evidence fence rejects a row that lost or changed
    # its body between prepare and publish. The resident predicate rejects a body
    # released or temporarily expired in the same window.
    current = connection.execute(
        f"SELECT * FROM messages AS m WHERE {resident_body_predicate()} AND m.message_id=?",
        (row["message_id"],),
    ).fetchone()
    if current is None:
        return False
    # Exact episode comparison. An absent caller sequence fails closed instead of
    # coercing through ``int(None)``; a matching episode that rehydrated the same
    # content keeps the same sequence and still passes.
    expected_seq = row["current_observation_seq"]
    if expected_seq is None or current["current_observation_seq"] != expected_seq:
        return False
    text = current_search_document(current).casefold()
    if text != current_search_document(row).casefold():
        return False
    digest = hashlib.sha256(text.encode()).hexdigest()
    previous = connection.execute(
        "SELECT * FROM message_lexical_projection WHERE message_id=?", (row["message_id"],)
    ).fetchone()
    if (
        previous is not None
        and previous["source_observation_seq"] == current["current_observation_seq"]
        and previous["input_digest"] == digest
        and previous["recipe"] == LEXICAL_RECIPE
        and (
            current["current_state"] != "present"
            or connection.execute(
                "SELECT 1 FROM message_lexical WHERE rowid="
                "(SELECT rowid FROM messages WHERE message_id=?)",
                (row["message_id"],),
            ).fetchone() is not None
        )
    ):
        return True
    now = utc_now().isoformat(timespec="microseconds")
    connection.execute(
        "INSERT OR IGNORE INTO derived_index_state(index_kind, recipe, updated_at) "
        "VALUES ('lexical',?,?)",
        (LEXICAL_RECIPE, now),
    )
    connection.execute(
        "DELETE FROM message_lexical WHERE rowid=(SELECT rowid FROM messages WHERE message_id=?)",
        (row["message_id"],),
    )
    if current["current_state"] == "present":
        connection.execute(
            "INSERT INTO message_lexical(rowid,text) VALUES ((SELECT rowid FROM "
            "messages WHERE message_id=?),?)",
            (row["message_id"], text),
        )
    connection.execute(
        """INSERT INTO message_lexical_projection VALUES (?,?,?,?,?)
        ON CONFLICT(message_id) DO UPDATE SET source_observation_seq=
        excluded.source_observation_seq,
        input_digest=excluded.input_digest, recipe=excluded.recipe,
        updated_at=excluded.updated_at""",
        (row["message_id"], current["current_observation_seq"], digest, LEXICAL_RECIPE, now),
    )
    if historical or previous is not None:
        connection.execute(
            "UPDATE derived_index_state SET generation=generation+1, updated_at=? "
            "WHERE index_kind='lexical'",
            (now,),
        )
    return True

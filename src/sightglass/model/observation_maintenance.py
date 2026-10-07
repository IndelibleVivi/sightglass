"""Bounded, content-free inspection and restart-safe repair of observation pointers."""

from __future__ import annotations

import json
from typing import Any

from sightglass.contracts.common import utc_now
from sightglass.source.identity import opaque_id

from .current_body import message_view
from .db import WindowDB
from .links import prepare_links, publish_links
from .observation_codec import (
    ObservationCodecError,
    ObservationPayloadUnavailable,
    decode_observation_text,
    decode_released_header,
)


def _matches(row: Any, observation: Any) -> bool:
    if observation is None or observation["state"] != row["current_state"]:
        return False
    try:
        value = json.loads(decode_observation_text(observation["parsed_json"]))
    except ObservationPayloadUnavailable:
        # An intentionally released body copy is allowed ONLY when the message row
        # agrees that its body is gone (``body_available = 0``) and the retained
        # observation header still describes the same source episode.  If the row
        # claims ``body_available = 1`` while its current observation is released,
        # the projection is genuinely inconsistent: report a mismatch so the pass
        # never silently accepts it or appends a false episode repair.
        if int(row["body_available"] or 0) != 0:
            return False
        if observation["state"] != row["current_state"]:
            return False
        if str(observation["message_id"]) != str(row["message_id"]):
            return False
        header = decode_released_header(observation["parsed_json"])
        retained = header["retained"] if header else {}
        source = retained.get("source_envelope", {})
        return (
            retained.get("message_kind") == row["kind"]
            and source.get("source_message_id") == row["source_message_id"]
            and source.get("sent_at_utc") == row["sent_at_utc"]
            and source.get("sort_seq") == row["sort_seq"]
            and source.get("source_rowid") == row["sort_tie"]
        )
    except (ObservationCodecError, ValueError, TypeError):
        return False
    message = value.get("message")
    envelope = value.get("source_envelope")
    if not isinstance(message, dict) or not isinstance(envelope, dict):
        return False
    # Operator identity corrections intentionally change sender/member bindings.
    # Compare only source/body truth, never the current canonical identity binding.
    # ``message_view`` reconstructs the single current-body representation from
    # the row, so this holds for normalized rows and legacy v10 rows alike.
    return (
        message == message_view(row)
        and envelope.get("source_message_id") == row["source_message_id"]
        and envelope.get("sent_at_utc") == row["sent_at_utc"]
        and envelope.get("sort_seq") == row["sort_seq"]
        and envelope.get("source_rowid") == row["sort_tie"]
    )


def _rows(connection: Any, after: str | None, limit: int) -> list[Any]:
    return connection.execute(
        "SELECT * FROM messages WHERE message_id>? ORDER BY message_id LIMIT ?",
        (after or "", limit + 1),
    ).fetchall()


def _current(connection: Any, row: Any) -> Any:
    return connection.execute(
        "SELECT * FROM message_observations WHERE observation_seq=? AND message_id=?",
        (row["current_observation_seq"], row["message_id"]),
    ).fetchone()


def inspect_observation_consistency(
    database: WindowDB,
    *,
    after_message_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Read at most ``limit`` projections; no source access or persistent writes."""
    limit = max(1, min(int(limit), 500))
    with database.read_snapshot() as connection:
        candidates = _rows(connection, after_message_id, limit)
        rows = candidates[:limit]
        mismatches = sum(
            row["projection_epoch"] is not None and not _matches(row, _current(connection, row))
            for row in rows
        )
    return {
        "schema": "sightglass.observation-consistency.v1",
        "mode": "inspect",
        "examined_count": len(rows),
        "mismatch_count": mismatches,
        "complete": len(candidates) <= limit,
        "after_message_id": str(rows[-1]["message_id"]) if rows else after_message_id,
    }


def repair_observation_consistency(database: WindowDB, *, limit: int = 100) -> dict[str, Any]:
    """Repair one atomic batch and its checkpoint; preserve immutable history.

    At most 100 prior observations per inconsistent message are examined. When no
    matching evidence exists inside that bound, retire its local projection until
    a normal source admission can revalidate it. No guessed evidence is appended.
    """
    limit = max(1, min(int(limit), 500))
    repaired = invalidated = mismatches = 0
    now = utc_now().isoformat(timespec="microseconds")
    with database.transaction(maintenance=True) as connection:
        state = connection.execute(
            "SELECT * FROM observation_maintenance_state WHERE singleton=1"
        ).fetchone()
        after = state["after_message_id"] if state else None
        candidates = [] if state and state["complete"] else _rows(connection, after, limit)
        rows = candidates[:limit]
        for row in rows:
            if row["projection_epoch"] is None or _matches(row, _current(connection, row)):
                continue
            mismatches += 1
            history = connection.execute(
                "SELECT * FROM message_observations WHERE message_id=? "
                "ORDER BY observation_seq DESC LIMIT 100",
                (row["message_id"],),
            ).fetchall()
            matching = next((item for item in history if _matches(row, item)), None)
            if matching is None:
                connection.execute(
                    "UPDATE messages SET projection_epoch=NULL WHERE message_id=?",
                    (row["message_id"],),
                )
                connection.execute(
                    "DELETE FROM message_links WHERE message_id=?", (row["message_id"],)
                )
                connection.execute(
                    "DELETE FROM message_link_projection WHERE message_id=?", (row["message_id"],)
                )
                connection.execute(
                    (
                        "DELETE FROM message_lexical WHERE rowid=(SELECT rowid FROM messages "
                        "WHERE message_id=?)"
                    ),
                    (row["message_id"],),
                )
                connection.execute(
                    "DELETE FROM message_lexical_projection WHERE message_id=?",
                    (row["message_id"],),
                )
                connection.execute(
                    "UPDATE derived_index_state SET generation=generation+1, updated_at=?", (now,)
                )
                invalidated += 1
                continue
            payload = matching["parsed_json"]
            payload_bytes = (
                len(payload.encode("utf-8")) if isinstance(payload, str) else len(payload)
            )
            database.reserve_growth(
                payload_bytes + 2 * len(str(row["structured_json"]).encode("utf-8")) + 8192
            )
            inserted = connection.execute(
                """INSERT INTO message_observations(
                    observation_id, message_id, observed_at, source_generation_id,
                    state, payload_digest, parsed_json, parser_version, raw_payload_ref, reason_code
                ) VALUES (?,?,?,?,?,?,?,?,?,'projection_pointer_repair')""",
                (
                    opaque_id(
                        "wxobservation",
                        str(row["message_id"]),
                        "pointer-repair",
                        str(row["current_observation_seq"]),
                        str(matching["observation_seq"]),
                    ),
                    row["message_id"],
                    now,
                    matching["source_generation_id"],
                    matching["state"],
                    matching["payload_digest"],
                    matching["parsed_json"],
                    matching["parser_version"],
                    matching["raw_payload_ref"],
                ),
            )
            connection.execute(
                "UPDATE messages SET current_observation_seq=? WHERE message_id=?",
                (inserted.lastrowid, row["message_id"]),
            )
            repaired_row = connection.execute(
                "SELECT * FROM messages WHERE message_id=?", (row["message_id"],)
            ).fetchone()
            publish_links(connection, prepare_links(repaired_row), historical=True)
            repaired += 1
        after = str(rows[-1]["message_id"]) if rows else after
        complete = len(candidates) <= limit
        connection.execute(
            """INSERT INTO observation_maintenance_state(
                singleton, after_message_id, complete, revision, updated_at)
            VALUES (1,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET
            after_message_id=excluded.after_message_id, complete=excluded.complete,
            revision=observation_maintenance_state.revision+excluded.revision,
            updated_at=excluded.updated_at""",
            (after, int(complete), mismatches, now),
        )
    return {
        "schema": "sightglass.observation-consistency.v1",
        "mode": "repair",
        "examined_count": len(rows),
        "mismatch_count": mismatches,
        "repaired_count": repaired,
        "invalidated_count": invalidated,
        "complete": complete,
        "after_message_id": after,
    }

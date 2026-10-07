"""Version-fenced, rebuildable message-link projection and bounded backfill."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from typing import Any

from sightglass.contracts.common import utc_now
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.source.identity import opaque_id
from sightglass.source.links import LINK_EXTRACTION_VERSION, ExtractedLink, extract_links

from .current_body import canonical_structured_json, current_body_text, message_view
from .db import WindowDB
from .lexical import LEXICAL_RECIPE, publish_lexical
from .resident_read import body_is_eligible, resident_body_predicate


def input_digest(row: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            [row["text"], canonical_structured_json(row), row["current_state"]],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()

def needs_publication_predicate(alias: str = "m") -> str:
    """A resident row whose current derivative receipt is missing or stale.

    Ordinary admission publishes a row's link and lexical receipts directly, so
    the bounded background pass only has to pick up residents that never got a
    receipt (legacy v10 stock) or whose receipt predates the current recipe
    (explicit rebuild). A published resident is skipped, so an already-warm
    resident set costs one index walk and a couple of existence probes per batch
    instead of a re-publication.
    """

    return (
        f"NOT EXISTS (SELECT 1 FROM message_link_projection AS lp "
        f"WHERE lp.message_id={alias}.message_id "
        f"AND lp.source_observation_seq={alias}.current_observation_seq "
        f"AND lp.extraction_version=?) "
        f"OR NOT EXISTS (SELECT 1 FROM message_lexical_projection AS lq "
        f"WHERE lq.message_id={alias}.message_id "
        f"AND lq.source_observation_seq={alias}.current_observation_seq "
        f"AND lq.recipe=?) "
        f"OR NOT EXISTS (SELECT 1 FROM message_lexical "
        f"WHERE message_lexical.rowid={alias}.rowid)"
    )


@dataclass(frozen=True)
class PreparedLinks:
    message_id: str
    observation_seq: int
    first_observation_seq: int
    input_digest: str
    links: tuple[ExtractedLink, ...]
    complete: bool


def prepare_links(row: Any) -> PreparedLinks:
    # Reconstruct the canonical message so link extraction reads the one stored
    # body source plus the retained card/forwarded envelope, for normalized and
    # legacy rows alike.
    extracted, complete = extract_links(current_body_text(row), message_view(row))
    if row["current_state"] != "present":
        extracted = []
    return PreparedLinks(
        str(row["message_id"]),
        int(row["current_observation_seq"]),
        int(row["first_observation_seq"]),
        input_digest(row),
        tuple(extracted),
        complete,
    )


def publish_links(
    connection: sqlite3.Connection,
    prepared: PreparedLinks,
    *,
    historical: bool = False,
) -> bool:
    """Publish only against the captured version; caller owns the short transaction."""
    row = connection.execute(
        "SELECT * FROM messages WHERE message_id = ?",
        (prepared.message_id,),
    ).fetchone()
    if (
        row is None
        or row["current_observation_seq"] != prepared.observation_seq
        or (input_digest(row) != prepared.input_digest)
    ):
        return False
    # Recheck the one resident-body eligibility rule inside the short writer. A
    # row that lost its body (release, active release job, expired temporary
    # residency) between selection and commit must not regrow a derivative
    # receipt; returning False rolls the batch back so the next selection skips
    # it instead of manufacturing coverage for an empty skeleton.
    if not body_is_eligible(connection, prepared.message_id):
        return False
    # ``publish_lexical`` performs its own final CAS/expiry recheck against the
    # current row. If it rejects (a later release/expiry boundary, a stale
    # episode, or a changed body), do not write any link rows: the whole
    # publication must roll back together with the lexical decision.
    if not publish_lexical(connection, row, historical=historical):
        return False
    existing = connection.execute(
        "SELECT * FROM message_link_projection WHERE message_id = ?",
        (prepared.message_id,),
    ).fetchone()
    if existing is not None and (
        existing["source_observation_seq"] == prepared.observation_seq
        and existing["input_digest"] == prepared.input_digest
        and existing["extraction_version"] == LINK_EXTRACTION_VERSION
    ):
        return True
    now = utc_now().isoformat(timespec="microseconds")
    connection.execute(
        """INSERT OR IGNORE INTO derived_index_state(index_kind, recipe, updated_at)
        VALUES ('links', ?, ?)""",
        (LINK_EXTRACTION_VERSION, now),
    )
    connection.execute("DELETE FROM message_links WHERE message_id = ?", (prepared.message_id,))
    for link in prepared.links:
        fields = asdict(link)
        columns = tuple(fields)
        connection.execute(
            f"""INSERT INTO message_links(
                link_id, message_id, account_id, conversation_id, sent_at_utc,
                sort_seq, sort_tie, {",".join(columns)}, extraction_version,
                source_observation_seq, link_digest, created_at, updated_at
            ) VALUES ({",".join("?" for _ in range(len(columns) + 12))})""",
            (
                opaque_id("wxlink", prepared.message_id, link.source_path, str(link.ordinal)),
                prepared.message_id,
                row["account_id"],
                row["conversation_id"],
                row["sent_at_utc"],
                row["sort_seq"],
                row["sort_tie"],
                *fields.values(),
                LINK_EXTRACTION_VERSION,
                prepared.observation_seq,
                link.digest(),
                now,
                now,
            ),
        )
    connection.execute(
        """INSERT INTO message_link_projection(
            message_id, source_observation_seq, input_digest, extraction_version,
            complete, link_count, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(message_id) DO UPDATE SET
            source_observation_seq=excluded.source_observation_seq,
            input_digest=excluded.input_digest, extraction_version=excluded.extraction_version,
            complete=excluded.complete, link_count=excluded.link_count,
            updated_at=excluded.updated_at
        """,
        (
            prepared.message_id,
            prepared.observation_seq,
            prepared.input_digest,
            LINK_EXTRACTION_VERSION,
            int(prepared.complete),
            len(prepared.links),
            now,
        ),
    )
    # Ordinary new appends do not invalidate a cursor's older observation watermark.
    # Repairs of old rows and historical publication do change the published index.
    if historical or existing is not None:
        connection.execute(
            "UPDATE derived_index_state SET generation=generation+1, updated_at=? "
            "WHERE index_kind='links'",
            (now,),
        )
    return True


class LinkRepository:
    def __init__(self, database: WindowDB) -> None:
        self.database = database

    def state(self) -> dict[str, Any]:
        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT * FROM derived_index_state WHERE index_kind='links'"
            ).fetchone()
        return (
            dict(row)
            if row is not None
            else {
                "index_kind": "links",
                "recipe": LINK_EXTRACTION_VERSION,
                "generation": 1,
                "checkpoint_seq": 0,
                "state": "building",
                "updated_at": None,
            }
        )

    def has_pending_backfill(self) -> bool:
        """Read-only probe; a ready flag alone cannot hide a missing resident receipt."""
        return bool(self._backfill_candidates(1))

    def _backfill_candidates(self, limit: int) -> list[sqlite3.Row]:
        with self.database.connection() as connection:
            return connection.execute(
                f"""SELECT m.* FROM messages AS m INDEXED BY message_resident_timeline
                WHERE m.body_available=1 AND m.current_state='present'
                  AND {resident_body_predicate()}
                  AND m.first_observation_seq IS NOT NULL
                  AND m.current_observation_seq IS NOT NULL
                  AND ({needs_publication_predicate()})
                ORDER BY m.first_observation_seq, m.message_id LIMIT ?""",
                (
                    LINK_EXTRACTION_VERSION,
                    LEXICAL_RECIPE,
                    limit,
                ),
            ).fetchall()

    def backfill_batch(self, *, limit: int = 100) -> dict[str, Any]:
        state = self.state()
        bounded = max(1, min(200, int(limit)))
        # Drive the bounded pass from the resident set itself. The
        # ``message_resident_timeline`` partial index materialises exactly the
        # ``body_available=1`` rows, so a fixed small resident set never walks the
        # durable skeleton history (as an ordered ``message_derived_backfill``
        # scan would after a rebuild reset its checkpoint to zero). The
        # eligibility predicate keeps a released/expiring row out of the work
        # set, and ``needs_publication`` keeps already-published residents out so
        # an old receipt can never be mistaken for missing coverage.
        #
        # Selection is driven purely by ``needs_publication`` ordered by
        # ``first_observation_seq`` -- deliberately *not* fenced by the stored
        # checkpoint. That checkpoint is not the index's ordering key, so a
        # ``first_observation_seq > checkpoint`` fence prunes nothing while it
        # could permanently skip an eligible resident (e.g. a legacy row whose
        # receipt is missing) that happens to sit behind a checkpoint a previous
        # pass already advanced. Convergence is instead guaranteed because every
        # selected row is published, so the needing-publication set strictly
        # shrinks each batch until it is empty. The checkpoint remains a
        # monotonic progress/observability counter.
        rows = self._backfill_candidates(bounded)
        if not rows and state["state"] == "ready":
            return {"processed": 0, **state}
        prepared = tuple(prepare_links(row) for row in rows)
        now = utc_now().isoformat(timespec="microseconds")
        with self.database.transaction() as connection:
            connection.execute(
                """INSERT OR IGNORE INTO derived_index_state(index_kind, recipe, updated_at)
                VALUES ('links', ?, ?)""",
                (LINK_EXTRACTION_VERSION, now),
            )
            for item in prepared:
                if not publish_links(connection, item, historical=True):
                    # Roll the entire batch back, including the checkpoint. A subsequent
                    # bounded batch reads the new version rather than skipping the row.
                    raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
            checkpoint = prepared[-1].first_observation_seq if prepared else state["checkpoint_seq"]
            connection.execute(
                """UPDATE derived_index_state
                SET checkpoint_seq=MAX(checkpoint_seq, ?), state=?, recipe=?, updated_at=?
                WHERE index_kind='links'""",
                (
                    checkpoint,
                    "building" if len(rows) >= bounded else "ready",
                    LINK_EXTRACTION_VERSION,
                    now,
                ),
            )
            connection.execute(
                "UPDATE derived_index_state SET checkpoint_seq=MAX(checkpoint_seq,?), "
                "state=?, recipe=?, updated_at=? WHERE index_kind='lexical'",
                (checkpoint, "building" if len(rows) >= bounded else "ready", LEXICAL_RECIPE, now),
            )
        return {"processed": len(rows), **self.state()}

    def request_rebuild(self, kind: str = "links") -> None:
        # Rebuildable derivatives only; canonical history and reader state are retained.
        # Until each receipt is replaced, its recipe mismatch keeps it out of queries.
        now = utc_now().isoformat(timespec="microseconds")
        with self.database.transaction() as connection:
            if kind == "links":
                connection.execute(
                    "UPDATE message_link_projection SET extraction_version='rebuild-pending'"
                )
            elif kind == "lexical":
                connection.execute("UPDATE message_lexical_projection SET recipe='rebuild-pending'")
                connection.execute(
                    "UPDATE derived_index_state SET generation=generation+1, "
                    "state='building', checkpoint_seq=0 WHERE index_kind='lexical'"
                )
                connection.execute(
                    "UPDATE derived_index_state SET state='building', checkpoint_seq=0"
                    " WHERE index_kind='links'"
                )
                return
            else:
                raise ValueError("unsupported derivative kind")
            connection.execute(
                """INSERT INTO derived_index_state(index_kind, recipe, updated_at)
                VALUES ('links', ?, ?)
                ON CONFLICT(index_kind) DO UPDATE SET generation=generation+1,
                    checkpoint_seq=0, state='building', recipe=excluded.recipe,
                    updated_at=excluded.updated_at""",
                (LINK_EXTRACTION_VERSION, now),
            )

    def coverage(self, conversation_ids: tuple[str, ...], *, epoch: str, watermark: int) -> str:
        if not conversation_ids:
            return "complete"
        with self.database.connection() as connection:
            row = connection.execute(
                f"""SELECT 1 FROM messages AS m
                LEFT JOIN message_link_projection AS p ON p.message_id=m.message_id
                WHERE m.conversation_id IN ({",".join("?" for _ in conversation_ids)})
                  AND m.projection_epoch=? AND m.current_state='present'
                  AND m.first_observation_seq<=? AND m.current_observation_seq<=?
                  AND (p.message_id IS NULL OR p.source_observation_seq!=m.current_observation_seq
                       OR p.extraction_version!=? OR p.complete=0) LIMIT 1""",
                (*conversation_ids, epoch, watermark, watermark, LINK_EXTRACTION_VERSION),
            ).fetchone()
        return "partial" if row is not None else "complete"

    def links_for_messages(self, ids: tuple[str, ...]) -> list[sqlite3.Row]:
        if not ids:
            return []
        with self.database.connection() as connection:
            return connection.execute(
                f"""SELECT l.* FROM message_links AS l
                JOIN messages AS m ON m.message_id=l.message_id
                JOIN message_link_projection AS p ON p.message_id=m.message_id
                WHERE l.message_id IN ({",".join("?" for _ in ids)})
                  AND m.current_state='present'
                  AND l.source_observation_seq=m.current_observation_seq
                  AND p.extraction_version=? AND l.extraction_version=?
                ORDER BY l.message_id, l.source_path, l.ordinal""",
                (*ids, LINK_EXTRACTION_VERSION, LINK_EXTRACTION_VERSION),
            ).fetchall()

    def link_window(
        self,
        *,
        account_id: str,
        conversation_ids: tuple[str, ...],
        epoch: str,
        watermark: int,
        after: str | None,
        before: str | None,
        domains: tuple[str, ...] = (),
        participant_ids: tuple[str, ...] = (),
        position: tuple[str, int, int, str, str] | None = None,
        limit: int = 200,
    ) -> list[sqlite3.Row]:
        if not conversation_ids:
            return []
        clauses = [
            "l.account_id=?",
            "m.current_state='present'",
            resident_body_predicate(),
            "m.projection_epoch=?",
            "m.first_observation_seq<=?",
            "m.current_observation_seq<=?",
            "l.source_observation_seq=m.current_observation_seq",
            "l.extraction_version=?",
            "p.extraction_version=?",
            "c.visibility_state='active'",
            f"m.conversation_id IN ({','.join('?' for _ in conversation_ids)})",
        ]
        values: list[Any] = [
            account_id,
            epoch,
            watermark,
            watermark,
            LINK_EXTRACTION_VERSION,
            LINK_EXTRACTION_VERSION,
            *conversation_ids,
        ]
        for selected, column in ((domains, "l.normalized_host"), (participant_ids, "m.sender_id")):
            if selected:
                clauses.append(f"{column} IN ({','.join('?' for _ in selected)})")
                values.extend(selected)
        if after:
            clauses.append("l.sent_at_utc>=?")
            values.append(after)
        if before:
            clauses.append("l.sent_at_utc<?")
            values.append(before)
        if position:
            clauses.append(
                "(l.sent_at_utc,l.sort_seq,l.sort_tie,l.message_id,l.link_id)<(?,?,?,?,?)"
            )
            values.extend(position)
        values.append(limit)
        with self.database.connection() as connection:
            return connection.execute(
                f"""SELECT l.*, m.account_id, m.conversation_id, m.sender_id, m.sent_at_utc,
                    m.sort_seq, m.sort_tie, m.sender_membership_id,
                    c.kind AS conversation_kind, c.current_title AS conversation_title
                FROM message_links AS l INDEXED BY {
                    "message_links_host" if domains else "message_links_timeline"
                } JOIN messages AS m ON m.message_id=l.message_id
                JOIN conversations AS c ON c.conversation_id=m.conversation_id
                JOIN message_link_projection AS p ON p.message_id=m.message_id
                WHERE {" AND ".join(clauses)}
                ORDER BY l.sent_at_utc DESC,l.sort_seq DESC,l.sort_tie DESC,
                         l.message_id DESC,l.link_id DESC LIMIT ?""",
                values,
            ).fetchall()

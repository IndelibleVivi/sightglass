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

from .current_body import (
    CURRENT_BODY_VERSION,
    canonical_structured_json,
    current_body_text,
    message_view,
)
from .db import WindowDB
from .lexical import LEXICAL_RECIPE, publish_lexical
from .resident_read import body_is_eligible, resident_body_predicate

SCAN_KIND = "resident_derivative_scan"
SCAN_VERSION = "sightglass.resident-derivative-scan.v1"
SCAN_COLUMNS = "conversation_id,sort_primary,sort_seq,sort_tie,source_message_id"
ScanPosition = tuple[str, str, int, int, str]


def _scan_position(row: Any) -> ScanPosition:
    return tuple(row[k] for k in SCAN_COLUMNS.split(","))  # type: ignore[return-value]


def _scan_binding(row: Any) -> tuple[Any, ...] | None:
    return tuple(row) if row is not None else None


def _scan_envelope(position: ScanPosition | None, upper: ScanPosition | None) -> str:
    # This internal scan kind owns a versioned traversal recipe/envelope in the
    # existing generic state table. Published links/lexical recipe rows stay separate.
    return json.dumps({"version": SCAN_VERSION, "links": LINK_EXTRACTION_VERSION,
                       "lexical": LEXICAL_RECIPE, "body": CURRENT_BODY_VERSION,
                       "position": position, "upper": upper}, separators=(",", ":"))


@dataclass(frozen=True)
class _ScanPlan:
    binding: tuple[Any, ...] | None
    position: ScanPosition | None
    upper: ScanPosition | None
    complete: bool
    scanned: int
    rows: tuple[sqlite3.Row, ...]
    persisted_ready: bool


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

    Applied only after a bounded resident-position page has been selected.
    Ordinary admission publishes directly; this repairs legacy/rebuild holes
    without re-publishing already-current residents or scanning the entire stock.
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
        """Bounded read-only probe, including warm pages still needing traversal."""
        plan = self._scan_plan(100)
        return bool(plan.rows) or not plan.complete or not plan.persisted_ready

    def _scan_plan(self, limit: int) -> _ScanPlan:
        with self.database.connection() as connection:
            scan = connection.execute(
                "SELECT * FROM derived_index_state WHERE index_kind=?", (SCAN_KIND,)
            ).fetchone()
            position = upper = None
            if scan is not None and scan["state"] == "building":
                try:
                    envelope = json.loads(scan["recipe"])
                    candidate = envelope["position"]
                    end = envelope["upper"]
                    def valid(value: Any) -> bool:
                        return (isinstance(value, list) and len(value) == 5
                                and all(isinstance(value[i], str) for i in (0, 1, 4))
                                and all(type(value[i]) is int for i in (2, 3)))
                    if (valid(candidate) and valid(end)
                            and json.loads(_scan_envelope(tuple(candidate), tuple(end)))
                            == envelope):
                        position, upper = tuple(candidate), tuple(end)
                except (TypeError, ValueError, KeyError):
                    pass  # Invalid/old optimization state safely restarts its bounded sweep.
            if upper is None:
                last = connection.execute(
                    "SELECT " + SCAN_COLUMNS + " FROM messages "
                    "INDEXED BY message_resident_timeline WHERE body_available=1 "
                    "ORDER BY " + ",".join(c + " DESC" for c in SCAN_COLUMNS.split(","))
                    + " LIMIT 1"
                ).fetchone()
                upper = _scan_position(last) if last is not None else None
            positions: list[sqlite3.Row] = []
            if upper is not None:
                lower = f" AND ({SCAN_COLUMNS})>(?,?,?,?,?)" if position is not None else ""
                positions = connection.execute(
                    "SELECT rowid AS scan_rowid," + SCAN_COLUMNS + " FROM messages "
                    "INDEXED BY message_resident_timeline WHERE body_available=1 "
                    f"AND ({SCAN_COLUMNS})<=(?,?,?,?,?)" + lower
                    + " ORDER BY " + SCAN_COLUMNS + " LIMIT ?",
                    (*upper, *(position or ()), limit),
                ).fetchall()
            if not positions:
                return _ScanPlan(_scan_binding(scan), position, upper, True, 0, (),
                                 scan is None or scan["state"] == "ready")
            # LIMIT precedes every eligibility/receipt probe and every payload read.
            # Both dense warm stock and large released skeleton history stay bounded.
            rows = connection.execute(
                f"""SELECT m.* FROM messages AS m NOT INDEXED
                WHERE m.body_available=1 AND m.current_state='present'
                  AND m.rowid IN ({','.join('?' for _ in positions)})
                  AND {resident_body_predicate()}
                  AND m.first_observation_seq IS NOT NULL
                  AND m.current_observation_seq IS NOT NULL
                  AND ({needs_publication_predicate()})""",
                (
                    *(row["scan_rowid"] for row in positions),
                    LINK_EXTRACTION_VERSION,
                    LEXICAL_RECIPE,
                ),
            ).fetchall()
            end = _scan_position(positions[-1])
            return _ScanPlan(_scan_binding(scan), end, upper,
                             len(positions) < limit or end == upper, len(positions), tuple(rows),
                             scan is None or scan["state"] == "ready")

    def backfill_batch(self, *, limit: int = 100) -> dict[str, Any]:
        state = self.state()
        bounded = max(1, min(200, int(limit)))
        plan = self._scan_plan(bounded)
        if (not plan.rows and plan.complete and plan.persisted_ready
                and state["state"] == "ready"):
            return {"processed": 0, "scanned": plan.scanned, **state}
        prepared = tuple(prepare_links(row) for row in plan.rows)
        now = utc_now().isoformat(timespec="microseconds")
        with self.database.transaction() as connection:
            current = connection.execute(
                "SELECT * FROM derived_index_state WHERE index_kind=?", (SCAN_KIND,)
            ).fetchone()
            if _scan_binding(current) != plan.binding:
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
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
            checkpoint = max((item.first_observation_seq for item in prepared),
                             default=state["checkpoint_seq"])
            phase = "ready" if plan.complete else "building"
            # An ordinary warm verification sweep must not disable an already
            # published lexical index while traversing it. Explicit rebuilds and
            # recipe changes retain their building state until pass completion.
            published = ("ready" if plan.complete else state["state"]
                         if state["recipe"] == LINK_EXTRACTION_VERSION else "building")
            connection.execute(
                """UPDATE derived_index_state
                SET checkpoint_seq=MAX(checkpoint_seq, ?), state=?, recipe=?, updated_at=?
                WHERE index_kind='links'""",
                (
                    checkpoint,
                    published,
                    LINK_EXTRACTION_VERSION,
                    now,
                ),
            )
            connection.execute(
                "UPDATE derived_index_state SET checkpoint_seq=MAX(checkpoint_seq,?), "
                "state=CASE WHEN ? THEN 'ready' WHEN recipe!=? THEN 'building' ELSE state END, "
                "recipe=?, updated_at=? WHERE index_kind='lexical'",
                (checkpoint, plan.complete, LEXICAL_RECIPE, LEXICAL_RECIPE, now),
            )
            connection.execute(
                """INSERT INTO derived_index_state(index_kind,recipe,state,updated_at)
                VALUES (?,?,?,?) ON CONFLICT(index_kind) DO UPDATE SET
                recipe=excluded.recipe,state=excluded.state,updated_at=excluded.updated_at""",
                (SCAN_KIND, _scan_envelope(plan.position, plan.upper), phase, now),
            )
        return {"processed": len(prepared), "scanned": plan.scanned, **self.state(), "state": phase}

    def request_rebuild(self, kind: str = "links") -> None:
        # Rebuildable derivatives only; canonical history and reader state are retained.
        # Until each receipt is replaced, its recipe mismatch keeps it out of queries.
        now = utc_now().isoformat(timespec="microseconds")
        with self.database.transaction() as connection:
            # Separate reset generation: normal historical publication changes the
            # public index generation and must not reset its own traversal.
            connection.execute(
                """INSERT INTO derived_index_state(index_kind,recipe,state,updated_at)
                VALUES (?,?,'building',?) ON CONFLICT(index_kind) DO UPDATE SET
                generation=generation+1,recipe=excluded.recipe,state='building',updated_at=?""",
                (SCAN_KIND, _scan_envelope(None, None), now, now),
            )
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

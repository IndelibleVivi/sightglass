"""Canonical body ownership, bounded cache admission and exact release plans.

The message projection is the only cache. Retained observations keep identity
headers; leases bind exact message episodes. Traversal coverage is independent.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.observation_codec import (
    ObservationCodecError,
    build_released_header,
    decode_observation_bytes,
    encode_released_observation,
    observation_payload_state,
)

from .decisions import ResidencyDecision, ResidencyMode, ResidencySettings, decide_residency
from .models import ConversationResidency


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat(timespec="microseconds")


def _now() -> datetime:
    return datetime.now(UTC)


class ResidencyRepository:
    def __init__(self, database: Any) -> None:
        self.db = database

    def settings(self) -> ResidencySettings:
        with self.db.connection() as connection:
            row = connection.execute(
                "SELECT * FROM residency_settings WHERE singleton=1"
            ).fetchone()
        if row is None:
            return ResidencySettings()
        names = ResidencySettings.__dataclass_fields__
        return ResidencySettings(**{name: row[name] for name in names})

    def set_settings(self, settings: ResidencySettings) -> ResidencySettings:
        fields = settings.as_dict()
        names = tuple(fields)
        with self.db.transaction() as connection:
            connection.execute(
                f"INSERT INTO residency_settings(singleton,{','.join(names)},updated_at) "
                f"VALUES (1,{','.join('?' for _ in names)},?) ON CONFLICT(singleton) DO UPDATE SET "
                + ",".join(f"{name}=excluded.{name}" for name in (*names, "updated_at")),
                (*fields.values(), _iso(_now())),
            )
        return settings

    def get(self, conversation_id: str) -> ConversationResidency | None:
        with self.db.connection() as connection:
            row = connection.execute(
                "SELECT * FROM conversation_residency WHERE conversation_id=?", (conversation_id,)
            ).fetchone()
        return ConversationResidency.from_row(row) if row else None

    def effective(self, conversation_id: str, settings: ResidencySettings) -> ResidencyMode:
        entry = self.get(conversation_id)
        return entry.mode if entry else ResidencyMode(settings.default_mode)

    def resolve_decision(
        self, conversation_id: str, *, authorized: bool, settings: ResidencySettings | None = None
    ) -> ResidencyDecision:
        return decide_residency(
            conversation_id,
            self.get(conversation_id),
            settings or self.settings(),
            authorized=authorized,
        )

    def set(self, conversation_id: str, **settings: Any) -> ConversationResidency:
        return self.batch_set((conversation_id,), **settings)[0]

    def batch_set(
        self,
        conversation_ids: tuple[str, ...],
        *,
        mode: ResidencyMode | str,
        keep_backfill: bool = False,
        recent_window_days: int | None = None,
        recent_max_bytes: int | None = None,
        reason: str | None = None,
    ) -> list[ConversationResidency]:
        checked = ResidencySettings(
            default_mode=mode,
            **(
                {"recent_window_days": recent_window_days} if recent_window_days is not None else {}
            ),
            **({"recent_max_bytes": recent_max_bytes} if recent_max_bytes is not None else {}),
        )
        if type(keep_backfill) is not bool or (keep_backfill and checked.default_mode != "keep"):
            raise ValueError("historical backfill requires keep residency")
        ids = tuple(dict.fromkeys(conversation_ids))
        if not ids or len(ids) > 500:
            raise ValueError("select between 1 and 500 conversations")
        moment = _iso(_now())
        with self.db.transaction() as connection:
            for identity in ids:
                if not connection.execute(
                    "SELECT 1 FROM conversations WHERE conversation_id=?", (identity,)
                ).fetchone():
                    raise KeyError(identity)
            connection.executemany(
                "INSERT INTO conversation_residency VALUES (?,?,?,?,?,?,?,?) "
                "ON CONFLICT(conversation_id) DO UPDATE SET mode=excluded.mode, "
                "keep_backfill=excluded.keep_backfill, "
                "recent_window_days=excluded.recent_window_days, "
                "recent_max_bytes=excluded.recent_max_bytes,updated_at=excluded.updated_at, "
                "reason=excluded.reason",
                [
                    (
                        identity,
                        str(checked.default_mode),
                        int(keep_backfill),
                        recent_window_days,
                        recent_max_bytes,
                        moment,
                        moment,
                        reason,
                    )
                    for identity in ids
                ],
            )
        return [entry for identity in ids if (entry := self.get(identity)) is not None]

    def on_demand_conversation_ids(self, account_id: str) -> set[str]:
        with self.db.connection() as connection:
            return {
                str(row[0])
                for row in connection.execute(
                    "SELECT c.conversation_id FROM conversations c LEFT JOIN "
                    "conversation_residency r USING(conversation_id) WHERE c.account_id=? "
                    "AND COALESCE(r.mode,?)='on_demand'",
                    (account_id, str(self.settings().default_mode)),
                )
            }

    def historical_conversation_ids(self) -> tuple[str, ...]:
        with self.db.connection() as connection:
            return tuple(
                str(row[0])
                for row in connection.execute(
                    "SELECT conversation_id FROM conversation_residency WHERE mode='keep' "
                    "AND keep_backfill=1 ORDER BY conversation_id"
                )
            )

    def list(
        self,
        *,
        mode: ResidencyMode | str | None = None,
        limit: int = 200,
        cursor: str | None = None,
        sort: str = "bytes",
    ) -> tuple[list[dict[str, Any]], str | None]:
        if sort not in {"bytes", "conversation"}:
            raise ValueError("unsupported residency sort")
        limit = max(1, min(limit, 500))
        selected = self.settings()
        where: list[str] = []
        params: list[Any] = [str(selected.default_mode)]
        if mode is not None:
            where.append("effective_mode=?")
            params.append(str(ResidencyMode(mode)))
        if cursor:
            position = json.loads(cursor)
            if position["sort"] != sort:
                raise ValueError("residency cursor sort changed")
            if sort == "bytes":
                where.append("(resident_bytes < ? OR (resident_bytes=? AND conversation_id>?))")
                params.extend((position["bytes"], position["bytes"], position["id"]))
            else:
                where.append("conversation_id>?")
                params.append(position["id"])
        sql = """WITH entries AS (
            SELECT c.conversation_id,c.account_id,c.kind,c.current_title,
                   COALESCE(r.mode,?) AS effective_mode,r.keep_backfill,
                   r.mode AS override_mode,r.reason,r.updated_at,
                   r.recent_window_days,r.recent_max_bytes,
                   COALESCE((SELECT SUM(t.byte_size) FROM residency_totals t
                             WHERE t.conversation_id=c.conversation_id),0) AS resident_bytes,
                   COALESCE((SELECT SUM(t.message_count) FROM residency_totals t
                             WHERE t.conversation_id=c.conversation_id),0) AS resident_messages,
                   (SELECT m.sort_primary FROM messages m
                    WHERE m.conversation_id=c.conversation_id AND m.body_available=1
                    ORDER BY m.sort_primary,m.sort_seq,m.sort_tie,m.source_message_id LIMIT 1)
                    AS resident_after,
                   (SELECT m.sort_primary FROM messages m
                    WHERE m.conversation_id=c.conversation_id AND m.body_available=1
                    ORDER BY m.sort_primary DESC,m.sort_seq DESC,m.sort_tie DESC,
                             m.source_message_id DESC LIMIT 1) AS resident_before,
                   'partial' AS resident_coverage,
                   CASE WHEN r.conversation_id IS NULL THEN 0 ELSE 1 END AS explicit
            FROM conversations c LEFT JOIN conversation_residency r USING(conversation_id)
        ) SELECT * FROM entries"""
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += (
            " ORDER BY resident_bytes DESC,conversation_id"
            if sort == "bytes"
            else " ORDER BY conversation_id"
        ) + " LIMIT ?"
        params.append(limit + 1)
        with self.db.connection() as connection:
            rows = connection.execute(sql, params).fetchall()
        page = rows[:limit]
        next_cursor = None
        if len(rows) > limit:
            last = page[-1]
            next_cursor = json.dumps(
                {"sort": sort, "bytes": last["resident_bytes"], "id": last["conversation_id"]},
                separators=(",", ":"),
            )
        return [dict(row) for row in page], next_cursor

    def revision(self) -> int:
        with self.db.connection() as connection:
            return int(connection.execute("SELECT revision FROM residency_state").fetchone()[0])

    @staticmethod
    def _body_bytes(connection: sqlite3.Connection, message_id: str) -> int:
        row = connection.execute(
            "SELECT length(CAST(COALESCE(text,'') AS BLOB)) + "
            "length(CAST(COALESCE(search_text,'') AS BLOB)) + "
            "length(CAST(structured_json AS BLOB)) FROM messages WHERE message_id=?",
            (message_id,),
        ).fetchone()
        return int(row[0]) + int(
            connection.execute(
                "SELECT COALESCE(SUM(length(CAST(parsed_json AS BLOB))),0) "
                "FROM message_observations WHERE message_id=?",
                (message_id,),
            ).fetchone()[0]
        )

    def admission_owner(
        self,
        message_id: str,
        *,
        exists: bool,
        decision: ResidencyDecision,
        sent_at: str,
        now: datetime | None = None,
    ) -> str:
        with self.db.connection() as connection:
            row = connection.execute(
                "SELECT owner FROM message_body_residency WHERE message_id=?", (message_id,)
            ).fetchone()
        if row is not None and row[0] in {"protected", "keep"}:
            return str(row[0])
        if exists and row is None:
            return "protected"
        if decision.mode == ResidencyMode.KEEP:
            return "keep"
        if decision.mode == ResidencyMode.RECENT and datetime.fromisoformat(sent_at) >= (
            (now or _now()) - timedelta(days=decision.recent_window_days or 30)
        ):
            return "recent"
        return "on_demand"

    def record_admission(
        self,
        message_id: str,
        *,
        conversation_id: str,
        owner: str,
        decision: ResidencyDecision,
        sent_at: str,
    ) -> None:
        moment = _now()
        expires = None
        if owner == "on_demand":
            expires = _iso(moment + timedelta(seconds=decision.lease_ttl_seconds))
        elif owner == "recent":
            expires = _iso(
                datetime.fromisoformat(sent_at) + timedelta(days=decision.recent_window_days or 30)
            )
        with self.db.transaction() as connection:
            # A new bounded read can renew an expired temporary body. Exact manual
            # release of protected stock has no expiry and keeps its durable job.
            connection.execute(
                "DELETE FROM body_release_jobs WHERE message_id=? AND EXISTS (SELECT 1 "
                "FROM message_body_residency WHERE message_id=? AND expires_at<=?)",
                (message_id, message_id, _iso(moment)),
            )
            connection.execute(
                "DELETE FROM body_release_jobs WHERE message_id=? AND observation_seq!=("
                "SELECT current_observation_seq FROM messages WHERE message_id=?)",
                (message_id, message_id),
            )
            size = self._body_bytes(connection, message_id)
            connection.execute(
                "INSERT INTO message_body_residency VALUES (?,?,?,?,?,?) "
                "ON CONFLICT(message_id) DO UPDATE SET owner=excluded.owner, "
                "byte_size=excluded.byte_size,admitted_at=excluded.admitted_at, "
                "expires_at=excluded.expires_at",
                (message_id, conversation_id, owner, size, _iso(moment), expires),
            )

    def record_foreground_lease(
        self,
        *,
        conversation_id: str,
        scope_key: str,
        message_ids: tuple[str, ...],
        projection_epoch: str,
        settings: ResidencySettings,
        now: datetime | None = None,
    ) -> str:
        moment = now or _now()
        identity = hashlib.sha256((conversation_id + "\0" + scope_key).encode()).hexdigest()
        with self.db.transaction() as connection:
            rows = [
                connection.execute(
                    "SELECT current_observation_seq FROM messages WHERE message_id=?", (item,)
                ).fetchone()
                for item in message_ids
            ]
            connection.execute("DELETE FROM read_lease WHERE lease_id=?", (identity,))
            connection.execute(
                "INSERT INTO read_lease VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    identity,
                    conversation_id,
                    scope_key,
                    projection_epoch,
                    message_ids[0] if message_ids else None,
                    message_ids[-1] if message_ids else None,
                    len(message_ids),
                    sum(self._body_bytes(connection, item) for item in message_ids),
                    hashlib.sha256(json.dumps([row[0] for row in rows]).encode()).hexdigest(),
                    _iso(moment),
                    _iso(moment + timedelta(seconds=settings.lease_ttl_seconds)),
                ),
            )
            connection.executemany(
                "INSERT INTO read_lease_message VALUES (?,?,?)",
                [(identity, item, row[0]) for item, row in zip(message_ids, rows)],
            )
        return identity

    @staticmethod
    def _pinned(connection: sqlite3.Connection, message_id: str) -> bool:
        return bool(
            connection.execute(
                "SELECT 1 FROM resources r WHERE r.message_id=? AND ("
                "EXISTS(SELECT 1 FROM resource_jobs j WHERE j.resource_id=r.resource_id "
                "AND j.state IN ('pending','leased','running')) OR "
                "EXISTS(SELECT 1 FROM voice_jobs j WHERE j.resource_id=r.resource_id "
                "AND j.state IN ('pending','leased','running'))) LIMIT 1",
                (message_id,),
            ).fetchone()
            or connection.execute(
                "SELECT 1 FROM messages m JOIN reader_deliveries d "
                "ON d.conversation_id=m.conversation_id AND d.status='pending' "
                "WHERE m.message_id=? AND m.current_observation_seq>d.from_observation_seq "
                "AND m.current_observation_seq<=d.to_observation_seq LIMIT 1",
                (message_id,),
            ).fetchone()
        )

    # SQL mirror of ``_pinned`` used only to keep protected rows out of the bounded
    # reclaim candidate pool. A dropped predicate would starve releasable bodies
    # behind a long pinned prefix; ``_pinned`` stays the authoritative re-check.
    _PINNED_SQL = (
        "NOT EXISTS(SELECT 1 FROM resources r WHERE r.message_id=b.message_id AND ("
        "EXISTS(SELECT 1 FROM resource_jobs j WHERE j.resource_id=r.resource_id "
        "AND j.state IN ('pending','leased','running')) OR "
        "EXISTS(SELECT 1 FROM voice_jobs j WHERE j.resource_id=r.resource_id "
        "AND j.state IN ('pending','leased','running')))) "
        "AND NOT EXISTS(SELECT 1 FROM messages m JOIN reader_deliveries d "
        "ON d.conversation_id=m.conversation_id AND d.status='pending' "
        "WHERE m.message_id=b.message_id "
        "AND m.current_observation_seq>d.from_observation_seq "
        "AND m.current_observation_seq<=d.to_observation_seq)"
    )

    def _release_bodies(
        self,
        connection: sqlite3.Connection,
        message_ids: list[str],
        *,
        observation_limit: int = 500,
    ) -> tuple[int, list[str], int]:
        freed, remaining = 0, observation_limit
        completed: list[str] = []
        changed = False
        for identity in message_ids:
            message = connection.execute(
                "SELECT current_observation_seq,conversation_id FROM messages WHERE message_id=?",
                (identity,),
            ).fetchone()
            if self._pinned(connection, identity):
                continue
            job = connection.execute(
                "SELECT observation_seq,after_seq FROM body_release_jobs WHERE message_id=?",
                (identity,),
            ).fetchone()
            if job is not None and job[0] != message[0]:
                connection.execute("DELETE FROM body_release_jobs WHERE message_id=?", (identity,))
                continue
            connection.execute(
                "INSERT OR IGNORE INTO body_release_jobs VALUES (?,?,0)", (identity, message[0])
            )
            after = job[1] if job else 0
            before = self._body_bytes(connection, identity)
            # Historical copies go first; the current episode remains readable until completion.
            rows = connection.execute(
                "SELECT observation_seq,parsed_json FROM message_observations "
                "WHERE message_id=? AND observation_seq>? AND observation_seq!=? "
                "ORDER BY observation_seq LIMIT ?",
                (identity, after, message[0], remaining + 1),
            ).fetchall()
            partial = len(rows) > remaining
            selected = rows[:remaining]
            if not partial:
                current = connection.execute(
                    "SELECT observation_seq,parsed_json FROM message_observations WHERE "
                    "observation_seq=?",
                    (message[0],),
                ).fetchone()
                if current is not None:
                    if len(selected) >= remaining:
                        partial = True
                    else:
                        selected.append(current)
            for row in selected:
                state = observation_payload_state(row[1])
                if state == "corrupt":
                    raise ObservationCodecError("cannot release a corrupt observation")
                if state == "full":
                    raw = decode_observation_bytes(row[1])
                    marker = encode_released_observation(
                        build_released_header(row[1]), original_bytes=len(raw)
                    )
                    connection.execute(
                        "UPDATE message_observations SET parsed_json=? WHERE observation_seq=?",
                        (marker, row[0]),
                    )
                    changed = True
            remaining -= len(selected)
            if partial:
                if selected:
                    connection.execute(
                        "UPDATE body_release_jobs SET after_seq=? WHERE message_id=?",
                        (selected[-1][0], identity),
                    )
                connection.execute(
                    "UPDATE message_body_residency SET byte_size=? WHERE message_id=?",
                    (self._body_bytes(connection, identity), identity),
                )
                freed += max(0, before - self._body_bytes(connection, identity))
                break
            connection.execute(
                "DELETE FROM message_lexical WHERE rowid=(SELECT rowid FROM messages "
                "WHERE message_id=?)",
                (identity,),
            )
            for table in ("message_lexical_projection", "message_links", "message_link_projection"):
                connection.execute(f"DELETE FROM {table} WHERE message_id=?", (identity,))
            connection.execute(
                "UPDATE messages SET body_available=0,projection_epoch=NULL,"
                "text=NULL,search_text=NULL,structured_json='{}' WHERE message_id=?",
                (identity,),
            )
            connection.execute(
                "INSERT INTO message_body_residency VALUES (?,?, 'on_demand',0,?,NULL) "
                "ON CONFLICT(message_id) DO UPDATE SET "
                "byte_size=0,owner='on_demand',expires_at=NULL",
                (identity, message[1], _iso(_now())),
            )
            connection.execute("DELETE FROM body_release_jobs WHERE message_id=?", (identity,))
            completed.append(identity)
            changed = True
            # Cache leases are expendable; a cursor using one must rebaseline after eviction.
            connection.execute(
                "DELETE FROM read_lease WHERE lease_id IN (SELECT lease_id "
                "FROM read_lease_message WHERE message_id=?)",
                (identity,),
            )
            freed += max(0, before - self._body_bytes(connection, identity))
        if changed:
            connection.execute("UPDATE residency_state SET revision=revision+1")
            connection.execute(
                "UPDATE derived_index_state SET generation=generation+1 "
                "WHERE index_kind IN ('lexical','links')"
            )
        return freed, completed, observation_limit - remaining

    def release_expired_leases(
        self, *, limit: int = 200, now: datetime | None = None
    ) -> dict[str, Any]:
        moment = _iso(now or _now())
        with self.db.connection() as connection:
            needed = (
                connection.execute("SELECT 1 FROM body_release_jobs LIMIT 1").fetchone()
                or connection.execute(
                    "SELECT 1 FROM message_body_residency WHERE expires_at<=? LIMIT 1", (moment,)
                ).fetchone()
                or connection.execute(
                    "SELECT 1 FROM read_lease WHERE expires_at<=? LIMIT 1", (moment,)
                ).fetchone()
            )
        if not needed:
            return {
                "schema": "sightglass.residency-reclaim.v1",
                "released_messages": 0,
                "freed_bytes": 0,
                "reclaimed_bytes": 0,
                "reclaimed_leases": 0,
                "examined_messages": 0,
                "has_more": False,
            }
        released: list[str] = []
        with self.db.transaction(maintenance=True) as connection:
            rows = connection.execute(
                "SELECT b.message_id FROM message_body_residency b JOIN messages m "
                "USING(message_id) "
                "WHERE (b.expires_at<=? OR EXISTS(SELECT 1 FROM body_release_jobs j "
                "WHERE j.message_id=b.message_id)) AND NOT EXISTS(SELECT 1 FROM resources r "
                "WHERE r.message_id=b.message_id AND (EXISTS(SELECT 1 FROM resource_jobs j "
                "WHERE j.resource_id=r.resource_id AND j.state IN ('pending','leased','running')) "
                "OR EXISTS(SELECT 1 FROM voice_jobs j WHERE j.resource_id=r.resource_id "
                "AND j.state IN ('pending','leased','running')))) AND NOT EXISTS("
                "SELECT 1 FROM reader_deliveries d WHERE d.conversation_id=m.conversation_id "
                "AND d.status='pending' AND m.current_observation_seq>d.from_observation_seq "
                "AND m.current_observation_seq<=d.to_observation_seq) "
                "ORDER BY b.expires_at,b.message_id LIMIT ?",
                (moment, max(1, min(limit, 500))),
            ).fetchall()
            for row in rows:
                if not self._pinned(connection, row[0]):
                    released.append(str(row[0]))
            freed, completed, _spent = self._release_bodies(connection, released)
            leases = connection.execute(
                "SELECT lease_id FROM read_lease WHERE expires_at<=? ORDER BY expires_at LIMIT ?",
                (moment, limit),
            ).fetchall()
            connection.executemany(
                "DELETE FROM read_lease WHERE lease_id=?", [(row[0],) for row in leases]
            )
        return {
            "schema": "sightglass.residency-reclaim.v1",
            "released_messages": len(completed),
            "freed_bytes": freed,
            "reclaimed_bytes": freed,
            "reclaimed_leases": len(leases),
            "examined_messages": len(rows),
            "has_more": len(rows) == min(limit, 500) or len(completed) < len(released),
        }

    def reclaim_expired(self, *, limit: int = 200, now: datetime | None = None) -> dict[str, Any]:
        return self.release_expired_leases(limit=limit, now=now)

    def resident_bytes(self, conversation_id: str) -> int:
        with self.db.connection() as connection:
            return int(
                connection.execute(
                    "SELECT COALESCE(SUM(byte_size),0) FROM residency_totals "
                    "WHERE conversation_id=?",
                    (conversation_id,),
                ).fetchone()[0]
            )

    def enforce_caps(
        self, decision: ResidencyDecision, *, protect: tuple[str, ...], limit: int = 500
    ) -> None:
        settings = self.settings()
        caps = [
            (
                "conversation_id=? AND owner='on_demand'",
                (decision.conversation_id,),
                settings.lease_max_bytes,
            ),
            (
                "conversation_id=? AND owner='recent'",
                (decision.conversation_id,),
                decision.max_bytes if decision.mode == "recent" else settings.recent_max_bytes,
            ),
            ("owner IN ('recent','on_demand')", (), settings.global_max_bytes),
        ]
        with self.db.transaction() as connection:
            observation_budget = 500
            for predicate, params, cap in caps:
                total = int(
                    connection.execute(
                        "SELECT COALESCE(SUM(byte_size),0) "
                        "FROM residency_totals WHERE " + predicate,
                        params,
                    ).fetchone()[0]
                )
                if cap is None or total <= cap:
                    continue
                scope = predicate.replace("conversation_id", "b.conversation_id")
                # Only rows that still hold resident bytes can relieve pressure.
                # Zero-byte rows are retained durable bookkeeping: releasing one
                # frees nothing, so it must never consume the bounded candidate
                # pool or crowd out a real body behind a long released prefix.
                # Exclude protected and actively pinned rows before LIMIT too;
                # an arbitrarily long ineligible prefix must not consume this batch.
                protect_ids = tuple(dict.fromkeys(protect))
                protect_clause = (
                    " AND b.message_id NOT IN (" + ",".join("?" for _ in protect_ids) + ")"
                    if protect_ids
                    else ""
                )
                rows = connection.execute(
                    "SELECT b.message_id,b.byte_size FROM message_body_residency b JOIN messages m "
                    "USING(message_id) WHERE " + scope + " AND b.byte_size>0 AND "
                    + self._PINNED_SQL + protect_clause
                    + " ORDER BY m.sort_primary,m.sort_seq,m.sort_tie,b.message_id LIMIT ?",
                    (*params, *protect_ids, limit),
                ).fetchall()
                for row in rows:
                    if total <= cap or observation_budget == 0:
                        break
                    if self._pinned(connection, row[0]):
                        continue
                    _, completed, spent = self._release_bodies(
                        connection, [row[0]], observation_limit=observation_budget
                    )
                    observation_budget -= spent
                    if completed:
                        total -= int(row[1])
                if total > cap:
                    raise SightglassError(
                        ErrorCode.STORAGE_PRESSURE,
                        details={
                            "reason": "residency_capacity",
                            "required_bytes": total,
                            "limit_bytes": cap,
                            "bounded_batch": limit,
                        },
                    )

    def stock_preview(
        self, conversation_id: str, *, after: str | None = None, limit: int = 200
    ) -> dict[str, Any]:
        with self.db.connection() as connection:
            return self.preview_connection(connection, conversation_id, after=after, limit=limit)

    @classmethod
    def preview_connection(
        cls,
        connection: sqlite3.Connection,
        conversation_id: str,
        *,
        after: str | None = None,
        limit: int = 200,
        source_identity: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        limit = max(1, min(limit, 500))
        available = "body_available" in {
            row[1] for row in connection.execute("PRAGMA table_info(messages)")
        }
        predicate = "AND body_available=1 " if available else ""
        rows = connection.execute(
            "SELECT message_id,current_observation_seq FROM messages WHERE conversation_id=? "
            + predicate
            + "AND message_id>? ORDER BY message_id LIMIT ?",
            (conversation_id, after or "", limit + 1),
        ).fetchall()
        page = rows[:limit]
        entries = [
            {
                "message_id": row[0],
                "observation_seq": row[1],
                "pinned": cls._pinned(connection, row[0]),
                "body_bytes": cls._body_bytes(connection, row[0]),
            }
            for row in page
        ]
        has_state = connection.execute(
            "SELECT 1 FROM sqlite_schema WHERE name='residency_state'"
        ).fetchone()
        revision = (
            int(connection.execute("SELECT revision FROM residency_state").fetchone()[0])
            if has_state
            else 0
        )
        plan = {
            "conversation_id": conversation_id,
            "entries": entries,
            "next_cursor": page[-1][0] if len(rows) > limit else None,
            "residency_revision": revision,
        }
        if source_identity is not None:
            plan["source_identity"] = source_identity
        plan["plan_digest"] = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
        return {
            "schema": "sightglass.residency-release-preview.v1",
            "applied": False,
            "plan": plan,
            "examined_messages": len(page),
            "releasable_messages": sum(not entry["pinned"] for entry in entries),
            "body_bytes": sum(entry["body_bytes"] for entry in entries if not entry["pinned"]),
            "pinned_messages": sum(entry["pinned"] for entry in entries),
            "next_cursor": plan["next_cursor"],
            "has_more": len(rows) > limit,
            "filesystem_reclaim_bytes": None,
        }

    def release_stock(
        self, conversation_id: str, *, plan: dict[str, Any], limit: int = 200
    ) -> dict[str, Any]:
        checked = dict(plan)
        digest = checked.pop("plan_digest", None)
        if digest != hashlib.sha256(json.dumps(checked, sort_keys=True).encode()).hexdigest():
            raise ValueError("release plan digest changed")
        if checked["conversation_id"] != conversation_id or len(checked["entries"]) > min(
            limit, 500
        ):
            raise ValueError("release plan scope changed")
        released, pinned = [], 0
        with self.db.transaction(maintenance=True) as connection:
            revision = int(connection.execute("SELECT revision FROM residency_state").fetchone()[0])
            if revision != checked["residency_revision"]:
                raise ValueError("release preview is stale")
            for entry in checked["entries"]:
                row = connection.execute(
                    "SELECT conversation_id,current_observation_seq,body_available "
                    "FROM messages WHERE message_id=?",
                    (entry["message_id"],),
                ).fetchone()
                if (
                    row is None
                    or row[0] != conversation_id
                    or row[1] != entry["observation_seq"]
                    or not row[2]
                ):
                    raise ValueError("release preview message episode changed")
                if self._body_bytes(connection, entry["message_id"]) != entry["body_bytes"]:
                    raise ValueError("release preview body changed")
                if self._pinned(connection, entry["message_id"]):
                    pinned += 1
                elif not entry["pinned"]:
                    released.append(entry["message_id"])
            for identity in released:
                row = connection.execute(
                    "SELECT current_observation_seq FROM messages WHERE message_id=?", (identity,)
                ).fetchone()
                connection.execute(
                    "INSERT OR IGNORE INTO message_body_residency VALUES "
                    "(?,?,'on_demand',?,?,NULL)",
                    (
                        identity,
                        conversation_id,
                        self._body_bytes(connection, identity),
                        _iso(_now()),
                    ),
                )
                connection.execute(
                    "INSERT OR IGNORE INTO body_release_jobs VALUES (?,?,0)", (identity, row[0])
                )
            freed, completed, _spent = self._release_bodies(connection, released)
        return {
            "schema": "sightglass.residency-release.v1",
            "applied": True,
            "released_messages": len(completed),
            "pinned_messages": pinned,
            "freed_bytes": freed,
            "pending_messages": len(released) - len(completed),
            "next_cursor": checked["next_cursor"],
            "filesystem_reclaim_bytes": None,
        }

    def rebaseline(self, conversation_id: str, *, reason: str | None = None) -> dict[str, Any]:
        with self.db.transaction(maintenance=True) as connection:
            expired = connection.execute(
                "UPDATE reader_deliveries SET status='expired' "
                "WHERE conversation_id=? AND status='pending'",
                (conversation_id,),
            ).rowcount
            connection.execute("UPDATE residency_state SET revision=revision+1")
        return {
            "schema": "sightglass.residency-rebaseline.v1",
            "conversation_id": conversation_id,
            "expired_pending_deliveries": expired,
            "reason": reason,
            "observed_coverage_preserved": True,
        }

    def preview(self, *, now: datetime | None = None) -> dict[str, Any]:
        with self.db.connection() as connection:
            totals = [
                dict(row)
                for row in connection.execute(
                    "SELECT owner,SUM(byte_size) AS bytes,"
                    "SUM(message_count) AS messages FROM residency_totals GROUP BY owner"
                )
            ]
            leases = connection.execute(
                "SELECT expires_at>? AS live,COUNT(*) AS n,SUM(byte_size) AS bytes "
                "FROM read_lease GROUP BY live",
                (_iso(now or _now()),),
            ).fetchall()
        result = {
            "schema": "sightglass.residency.preview.v1",
            "settings": self.settings().as_dict(),
            "tracked_ownership": totals,
            "legacy_untracked_bytes": None,
            "mutated": False,
            "filesystem_reclaim_bytes": None,
        }
        for live, prefix in ((1, "live"), (0, "expired")):
            row = next((row for row in leases if row[0] == live), None)
            result[f"lease_{prefix}_count"] = int(row[1]) if row else 0
            result[f"lease_{prefix}_bytes"] = int(row[2] or 0) if row else 0
        return result

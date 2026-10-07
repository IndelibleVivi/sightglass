from __future__ import annotations

from typing import Any

from sightglass.model.db import WindowDB


class VoiceRepository:
    def __init__(self, database: WindowDB) -> None:
        self.database = database

    def one(self, query: str, parameters: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self.database.connection() as connection:
            row = connection.execute(query, parameters).fetchone()
            return dict(row) if row is not None else None

    def rows(self, query: str, parameters: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self.database.connection() as connection:
            return [dict(row) for row in connection.execute(query, parameters)]

    def account(self, account_id: str) -> dict[str, Any] | None:
        return self.one("SELECT * FROM accounts WHERE account_id = ?", (account_id,))

    def resource(self, resource_id: str) -> dict[str, Any] | None:
        return self.one(
            "SELECT r.*, m.account_id FROM resources r JOIN messages m USING(message_id) "
            "WHERE resource_id = ?", (resource_id,),
        )

    def batch(self, batch_id: str) -> dict[str, Any] | None:
        return self.one("SELECT * FROM voice_batches WHERE batch_id = ?", (batch_id,))

    def existing_batch(
        self, digest: str, binding: str | None, policy: str, now: str,
    ) -> dict[str, Any] | None:
        return self.one(
            "SELECT * FROM voice_batches WHERE selection_digest = ? "
            "AND account_binding_id IS ? AND voice_policy = ? "
            "AND state IN ('open', 'sealed', 'delivered') AND expires_at > ? "
            "ORDER BY created_at, batch_id LIMIT 1", (digest, binding, policy, now),
        )

    def insert_batch(self, values: dict[str, Any]) -> None:
        self._insert("voice_batches", values)

    def insert_item(self, values: dict[str, Any]) -> None:
        self._insert("voice_batch_items", values)

    def insert_job(self, values: dict[str, Any]) -> None:
        self._insert("voice_jobs", values)

    def insert_object(self, values: dict[str, Any]) -> None:
        self._insert("resource_objects", values, ignore=True)

    def _insert(self, table: str, values: dict[str, Any], *, ignore: bool = False) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                f"INSERT {'OR IGNORE ' if ignore else ''}INTO {table}"
                f"({','.join(values)}) VALUES ({','.join('?' for _ in values)})",
                tuple(values.values()),
            )

    def object(self, digest: str) -> dict[str, Any] | None:
        return self.one("SELECT * FROM resource_objects WHERE object_digest = ?", (digest,))

    def items(self, batch_id: str) -> list[dict[str, Any]]:
        return self.rows(
            "SELECT i.*, j.state AS job_state, j.result_digest, j.max_duration_ms, "
            "j.error_code "
            "FROM voice_batch_items i LEFT JOIN voice_jobs j USING(job_id) "
            "WHERE i.batch_id = ? ORDER BY i.ordinal", (batch_id,),
        )

    def update_item(self, batch_id: str, ordinal: int, **values: Any) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                f"UPDATE voice_batch_items SET {','.join(key + ' = ?' for key in values)} "
                "WHERE batch_id = ? AND ordinal = ?",
                (*values.values(), batch_id, ordinal),
            )

    def job(self, job_id: str) -> dict[str, Any] | None:
        return self.one("SELECT * FROM voice_jobs WHERE job_id = ?", (job_id,))

    def matching_job(
        self, account: str, resource: str, revision: str, recipe: str,
    ) -> dict[str, Any] | None:
        return self.one(
            "SELECT * FROM voice_jobs WHERE account_id = ? AND resource_id = ? "
            "AND resource_revision = ? AND recipe_digest = ? "
            "AND state IN ('ready', 'pending', 'leased', 'running', 'blocked') "
            "ORDER BY CASE state WHEN 'ready' THEN 0 ELSE 1 END, created_at DESC LIMIT 1",
            (account, resource, revision, recipe),
        )

    def active_budget(self, unknown_ms: int) -> tuple[int, int]:
        row = self.one(
            "SELECT count(*) AS n, coalesce(sum(coalesce(max_duration_ms, ?)), 0) AS ms "
            "FROM voice_jobs WHERE state IN ('pending', 'leased', 'running')", (unknown_ms,),
        )
        assert row is not None
        return int(row['n']), int(row['ms'])

    def step_budget(self, batch_id: str, step: int) -> tuple[int, int]:
        row = self.one(
            "SELECT count(*) AS n, coalesce(sum(j.max_duration_ms), 0) AS ms "
            "FROM voice_batch_items i JOIN voice_jobs j USING(job_id) "
            "WHERE i.batch_id = ? AND i.admission_step = ? AND i.state = 'admitted'",
            (batch_id, step),
        )
        assert row is not None
        return int(row['n']), int(row['ms'])

    def update_job(self, job_id: str, **values: Any) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                f"UPDATE voice_jobs SET {','.join(key + ' = ?' for key in values)} "
                "WHERE job_id = ?", (*values.values(), job_id),
            )

    def job_items(self, job_id: str) -> list[dict[str, Any]]:
        return self.rows("SELECT * FROM voice_batch_items WHERE job_id = ?", (job_id,))

    def expired_leases(self, now: str) -> list[dict[str, Any]]:
        return self.rows(
            "SELECT * FROM voice_jobs WHERE state IN ('leased', 'running') "
            "AND lease_expires_at <= ?", (now,),
        )

    def pending_jobs(self, limit: int, *, exclude: tuple[str, ...] = ()) -> list[dict[str, Any]]:
        excluded = " AND job_id NOT IN (" + ",".join("?" for _ in exclude) + ")" if exclude else ""
        return self.rows(
            "SELECT * FROM voice_jobs WHERE state = 'pending'" + excluded
            + " ORDER BY created_at, job_id LIMIT ?", (*exclude, int(limit)),
        )

    def next_lease_deadline(self) -> str | None:
        row = self.one(
            "SELECT MIN(lease_expires_at) AS deadline FROM voice_jobs "
            "WHERE state IN ('leased','running')"
        )
        return str(row["deadline"]) if row and row["deadline"] is not None else None

    def outstanding_leases(self) -> list[dict[str, Any]]:
        return self.rows(
            "SELECT * FROM voice_jobs WHERE state IN ('leased', 'running') "
            "ORDER BY created_at, job_id",
        )

    def batch_conversation_ids(self, batch_id: str) -> list[str]:
        return [
            str(row["conversation_id"])
            for row in self.rows(
                "SELECT DISTINCT m.conversation_id FROM voice_batch_items i "
                "JOIN messages m USING(message_id) WHERE i.batch_id = ? "
                "ORDER BY m.conversation_id", (batch_id,),
            )
        ]

    def add_event(
        self, batch: str, ordinal: int, job: str | None, kind: str, digest: str, now: str,
    ) -> None:
        self._insert("voice_batch_events", dict(
            batch_id=batch, item_ordinal=ordinal, job_id=job, kind=kind,
            result_digest=digest, created_at=now,
        ))

    def events(self, batch: str, after: int = 0, limit: int = 2) -> list[dict[str, Any]]:
        return self.rows(
            "SELECT e.*, i.message_id, i.resource_id FROM voice_batch_events e "
            "JOIN voice_batch_items i ON i.batch_id = e.batch_id "
            "AND i.ordinal = e.item_ordinal WHERE e.batch_id = ? AND e.event_id > ? "
            "ORDER BY e.event_id LIMIT ?", (batch, after, limit),
        )

    def event(self, batch: str, event_id: int) -> dict[str, Any] | None:
        return self.one(
            "SELECT * FROM voice_batch_events WHERE batch_id = ? AND event_id = ? "
            "AND item_ordinal IS NOT NULL", (batch, event_id),
        )

    def first_event(self, batch: str, ordinal: int) -> dict[str, Any]:
        row = self.one(
            "SELECT * FROM voice_batch_events WHERE batch_id = ? AND item_ordinal = ? "
            "ORDER BY event_id LIMIT 1", (batch, ordinal),
        )
        assert row is not None
        return row

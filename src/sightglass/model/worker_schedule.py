"""Read-only scheduling queries; no queue ownership or admission decisions."""

from __future__ import annotations

from typing import Any

from .db import WindowDB


def pending_resource_jobs(
    database: WindowDB, limit: int, *, exclude: tuple[str, ...] = ()
) -> list[dict[str, Any]]:
    excluded = " AND job_id NOT IN (" + ",".join("?" for _ in exclude) + ")" if exclude else ""
    with database.connection() as connection:
        return [
            dict(row) for row in connection.execute(
                "SELECT * FROM resource_jobs WHERE state='pending'" + excluded
                + " ORDER BY created_at, job_id LIMIT ?", (*exclude, int(limit)),
            )
        ]


def resource_lease_deadline(database: WindowDB) -> str | None:
    with database.connection() as connection:
        row = connection.execute(
            "SELECT MIN(lease_expires_at) FROM resource_jobs WHERE state IN ('leased','running')"
        ).fetchone()
    return str(row[0]) if row and row[0] is not None else None


def collecting_conversations(database: WindowDB, *, default_mode: str) -> tuple[str, ...]:
    with database.connection() as connection:
        return tuple(
            str(row[0]) for row in connection.execute(
                "SELECT c.conversation_id FROM conversations c LEFT JOIN conversation_residency r "
                "USING(conversation_id) WHERE COALESCE(r.mode,?) IN ('keep','recent')",
                (default_mode,),
            )
        )

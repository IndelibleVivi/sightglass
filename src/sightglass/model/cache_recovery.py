"""Frozen, read-only object bindings for explicit stopped CAS recovery."""
from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def frozen_object_bindings(
    database: Path,
) -> Iterator[Callable[[str], tuple[str, int] | None]]:
    connection = sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")

        def binding(digest: str) -> tuple[str, int] | None:
            row = connection.execute(
                "SELECT local_path_internal, byte_size FROM resource_objects WHERE object_digest=?",
                (digest,),
            ).fetchone()
            if row is None:
                return None
            # No DB row is rewritten or removed by recovery. Check the relevant
            # resource references rather than silently blessing corrupt bindings.
            for table in ("resource_bindings", "resource_derivations"):
                column = "object_digest" if table == "resource_bindings" else "derived_digest"
                invalid = connection.execute(
                    f"SELECT 1 FROM {table} AS b LEFT JOIN resources AS r USING(resource_id) "
                    f"WHERE b.{column}=? AND r.resource_id IS NULL LIMIT 1", (digest,),
                ).fetchone()
                if invalid is not None:
                    raise RuntimeError("legacy object has invalid resource bindings")
            return str(row[0]), int(row[1])

        yield binding
    finally:
        connection.close()

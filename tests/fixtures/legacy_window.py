"""Explicit historical DDL fixture upgrade; production WindowDB never calls this.

Retains pre-v10 migration rollback/space fixtures. SG-059 is tested separately
through a frozen schema9 input and the candidate builder. This helper touches
only unittest-generated databases, not a config or installed runtime.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

from sightglass.model.db import WindowDB
from sightglass.model.migrations import migrate_schema
from sightglass.model.schema import SCHEMA_VERSION


def migrate_fixture(path: Path, *, storage=None) -> WindowDB:
    # Match WindowDB's canonical namespace, including macOS /var -> /private/var.
    path = path.expanduser().resolve()
    backup = None
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if 0 < version < SCHEMA_VERSION:
            backup = migrate_schema(connection, path, version, storage=storage)
    database = WindowDB(path, storage=storage)
    database.migration_backup_path = backup
    return database

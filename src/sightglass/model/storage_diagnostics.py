"""Content-free capacity summaries and explicitly bounded deep SQLite diagnostics."""

from __future__ import annotations

import sqlite3
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.operations import (
    check_operation_budget,
    operation_budget,
    operation_cancelled,
    operation_expired,
)
from sightglass.storage import SQL_WRITE_RESERVE

from .observation_codec import encode_observation

if TYPE_CHECKING:
    from .db import WindowDB

PHASES = ("tables", "layout", "observations", "sample")
DEFAULT_DEADLINE_SECONDS = 10.0
MAX_DEADLINE_SECONDS = 25.0
MAX_SAMPLE_SIZE = 1_000


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _query(database: WindowDB, sql: str, parameters: tuple = ()) -> list[sqlite3.Row]:
    # A completed statement releases its read view before the next stage begins.
    check_operation_budget()
    with database.connection() as connection:
        connection.execute("PRAGMA query_only=ON")
        rows = connection.execute(sql, parameters).fetchall()
    check_operation_budget()
    return rows


def _table_count(database: WindowDB, name: str) -> int:
    return int(_query(database, f"SELECT COUNT(*) FROM {_identifier(name)}")[0][0])


def _headroom(status: dict[str, Any]) -> dict[str, Any]:
    if status.get("state") == "unmanaged":
        return {"available": False, "reason": "unmanaged"}
    limits = status["limits"]
    ordinary_floor = limits["min_free_bytes"] + limits["maintenance_reserve_bytes"]
    reserved = status["reserved_bytes"]
    required = ordinary_floor + reserved + SQL_WRITE_RESERVE
    return {
        "available": True,
        "ordinary_write_reserve_bytes": SQL_WRITE_RESERVE,
        "minimum_available_bytes_for_admission": required,
        "filesystem_shortfall_bytes": max(0, required - status["available_bytes"]),
        "budget_shortfall_bytes": max(0, SQL_WRITE_RESERVE - status["remaining_budget_bytes"]),
        "foreground_admission_allowed": status["admission_allowed"],
        "background_growth_allowed": status["background_growth_allowed"],
        "resume_condition": "both_filesystem_and_owned_budget_allow_admission",
        "scope": "one_base_SQL_admission_larger_payloads_require_additional_reservation",
    }


def diagnostic_deadline(value: Any) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not 0 < value <= MAX_DEADLINE_SECONDS
    ):
        raise ValueError("storage explain deadline_seconds must be greater than 0 and at most 25")
    return float(value)


def explain(
    database: WindowDB,
    *,
    offset: int,
    limit: int,
    sample_size: int,
    deep: bool,
    phase: str,
    after_object: str | None,
    deadline_seconds: float,
) -> dict[str, Any]:
    if type(deep) is not bool:
        raise ValueError("storage explain deep must be a boolean")
    if type(sample_size) is not int or not 0 <= sample_size <= MAX_SAMPLE_SIZE:
        raise ValueError(f"storage explain sample_size must be between 0 and {MAX_SAMPLE_SIZE}")
    if phase not in (*PHASES, "all"):
        raise ValueError(
            "storage explain phase must be all, tables, layout, observations or sample"
        )
    if after_object is not None and (not isinstance(after_object, str) or phase != "tables"):
        raise ValueError("storage explain after_object requires phase=tables")
    deadline_seconds = diagnostic_deadline(deadline_seconds)
    started = time.monotonic()
    with operation_budget(deadline_seconds):
        status = database.storage_status()
        files = (
            database.storage.explain(offset=offset, limit=limit, reconcile=False)
            if database.storage is not None
            else {"files": [], "next_offset": None, "accounting": "unmanaged"}
        )
        with database.connection() as connection:
            connection.execute("PRAGMA query_only=ON")
            header = {
                key: int(connection.execute(f"PRAGMA {pragma}").fetchone()[0])
                for key, pragma in (
                    ("schema_version", "user_version"),
                    ("page_size", "page_size"),
                    ("page_count", "page_count"),
                    ("freelist_page_count", "freelist_count"),
                )
            }
        detail: dict[str, Any] = {
            "schema": "sightglass.database-storage-explain.v2",
            **header,
            "page_bytes": header["page_size"] * header["page_count"],
            "freelist_bytes": header["page_size"] * header["freelist_page_count"],
            "counts_available": False,
            "objects": [],
            "message_count": None,
            "observation_count": None,
            "legacy_text_observation_count": None,
            "encoded_blob_observation_count": None,
            "snapshot_scope": "one_statement_per_phase_result",
        }
        progress: dict[str, Any] = {
            "mode": "deep" if deep else "quick",
            "state": "complete",
            "deadline_seconds": deadline_seconds,
            "completed_phases": [],
            "current_phase": None,
            "last_completed_object": None,
            "reason": None,
        }
        requested = PHASES if phase == "all" else (phase,)
        if deep:
            try:
                for selected in requested:
                    progress["current_phase"] = selected
                    check_operation_budget()
                    if selected == "tables":
                        rows = _query(
                            database,
                            """
                            SELECT name FROM sqlite_schema WHERE type='table'
                              AND name NOT LIKE 'sqlite_%' AND name > ? ORDER BY name
                        """,
                            (after_object or "",),
                        )
                        for row in rows:
                            name = str(row["name"])
                            count = _table_count(database, name)
                            detail["objects"].append(
                                {
                                    "name": name,
                                    "type": "table",
                                    "owner_table": name,
                                    "record_count": count,
                                    "record_count_source": "count_rows",
                                    "physical_bytes": None,
                                    "average_bytes_per_record": None,
                                }
                            )
                            progress["last_completed_object"] = name
                            if name == "messages":
                                detail["message_count"] = count
                        detail["counts_available"] = True
                    elif selected == "layout":
                        available = bool(
                            _query(
                                database,
                                """
                            SELECT 1 FROM pragma_compile_options
                            WHERE compile_options='ENABLE_DBSTAT_VTAB'
                        """,
                            )
                        )
                        detail["dbstat_available"] = available
                        if available:
                            rows = _query(
                                database,
                                """
                                SELECT s.name, s.type, s.tbl_name, d.pgsize, d.ncell
                                FROM dbstat AS d JOIN sqlite_schema AS s ON s.name=d.name
                                WHERE d.aggregate=TRUE AND s.name NOT LIKE 'sqlite_%'
                                ORDER BY s.type, s.name
                            """,
                            )
                            by_name = {item["name"]: item for item in detail["objects"]}
                            for row in rows:
                                item = by_name.get(row["name"])
                                if item is None:
                                    item = {
                                        "name": row["name"],
                                        "type": row["type"],
                                        "owner_table": row["tbl_name"],
                                        "record_count": int(row["ncell"])
                                        if row["type"] == "index"
                                        else None,
                                        "record_count_source": "dbstat_cells"
                                        if row["type"] == "index"
                                        else "not_requested",
                                    }
                                    detail["objects"].append(item)
                                size = int(row["pgsize"])
                                item["physical_bytes"] = size
                                count = item["record_count"]
                                item["average_bytes_per_record"] = (
                                    round(size / count, 2) if count else None
                                )
                            detail["categories"] = database._storage_categories(
                                detail["objects"], available=True
                            )
                    elif selected == "observations":
                        row = _query(
                            database,
                            """
                            SELECT COUNT(*) AS total,
                              COALESCE(SUM(typeof(parsed_json)='text'),0) AS legacy,
                              COALESCE(SUM(typeof(parsed_json)='blob'),0) AS encoded
                            FROM message_observations
                        """,
                        )[0]
                        detail.update(
                            observation_count=int(row["total"]),
                            legacy_text_observation_count=int(row["legacy"]),
                            encoded_blob_observation_count=int(row["encoded"]),
                        )
                    elif selected == "sample":
                        # Limit examined rows as well as emitted legacy payloads.
                        rows = _query(
                            database,
                            """
                            SELECT parsed_json FROM message_observations
                            ORDER BY observation_seq LIMIT ?
                        """,
                            (sample_size,),
                        )
                        legacy = [str(row[0]) for row in rows if isinstance(row[0], str)]
                        raw = sum(len(value.encode("utf-8")) for value in legacy)
                        encoded = 0
                        for value in legacy:
                            check_operation_budget()
                            encoded += len(encode_observation(value))
                        count = detail["legacy_text_observation_count"]
                        delta = (
                            round((encoded - raw) / len(legacy) * count)
                            if legacy and (count is not None)
                            else None
                        )
                        detail["legacy_compression_sample"] = {
                            "method": "legacy_within_oldest_bounded_observation_rows",
                            "requested_rows": sample_size,
                            "examined_rows": len(rows),
                            "sampled_rows": len(legacy),
                            "raw_payload_bytes": raw,
                            "encoded_payload_bytes": encoded,
                            "estimated_encoded_payload_delta_bytes": delta,
                            "estimated_payload_reduction_bytes": max(0, -delta)
                            if delta is not None
                            else None,
                            "estimate_scope": "payload_only_not_filesystem_reclaim",
                        }
                    check_operation_budget()
                    progress["completed_phases"].append(selected)
                    progress["current_phase"] = None
            except (sqlite3.DatabaseError, SightglassError) as exc:
                timed_out = operation_expired() or (
                    isinstance(exc, SightglassError) and exc.code == ErrorCode.SERVICE_TIMEOUT
                )
                if not timed_out:
                    raise
                progress["state"] = "partial"
                progress["reason"] = (
                    "operation_cancelled" if operation_cancelled() else "operation_deadline"
                )
        progress["elapsed_ms"] = max(0, round((time.monotonic() - started) * 1000))
        return {
            "schema": "sightglass.storage-explain.v1",
            "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "status": status,
            "headroom": _headroom(status),
            "files": files,
            "database": detail,
            "diagnostic": progress,
            "mutated": False,
        }

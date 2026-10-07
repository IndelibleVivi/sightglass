"""Logical cut evidence for a stopped, exact installation transfer.

The checkpointed DB, owned immutable payloads and receive high-water must describe
the same committed state. A stopped edge snapshot is mandatory for a remote core;
an unreceived pending batch must be resolved on its current owner before transfer.
This check performs no source reads, ACK, activation or body transformation.
"""

from __future__ import annotations

import hashlib
import json
from contextlib import closing
from pathlib import Path
from typing import Any

from sightglass.contracts.capture import CaptureProtocolError, CaptureStreamPosition
from sightglass.source.capture.codec import canonical_json, typed_value

from .capture_journal import (
    CAPTURE_BATCH_TOOL,
    CAPTURE_REQUEST_TOOL,
    CAPTURE_STREAM_TOOL,
    _identity,
)
from .config import SightglassConfig
from .migration import _regular, _revision
from .migration_state import _readonly

CUT_SCHEMA = "sightglass.installation-cut.v1"
_CUT_TABLES = (
    "reader_profiles",
    "reader_timeline_cursors",
    "reader_update_cursors",
    "reader_deliveries",
    "source_catalog_state",
    "source_conversation_state",
    "access_receipts",
    "sqlite_sequence",
)
_EDGE_KEYS = {
    "schema",
    "source_instance_id",
    "account_id",
    "origin_epoch",
    "stream_epoch",
    "next_sequence",
    "epoch_lost",
    "pending",
}


def _rows(connection: Any, table: str) -> dict[str, Any]:
    # The hash establishes exact cut identity; table/column contents stay private in DB.
    digest, count = hashlib.sha256(), 0
    for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid'):
        payload = canonical_json(
            [{"blob": value.hex()} if isinstance(value, bytes) else value for value in row]
        )
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
        count += 1
    return {"rows": count, "digest": digest.hexdigest()}


def _record(connection: Any, receipt_id: str, tool: str) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT warning_codes_json FROM access_receipts WHERE receipt_id=? AND tool_name=?",
        (receipt_id, tool),
    ).fetchone()
    if row is None:
        return None
    value = json.loads(row[0])
    if not isinstance(value, dict):
        raise CaptureProtocolError("corrupt_capture_journal")
    return value


def logical_installation_cut(
    config: SightglassConfig, *, edge_state: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Caller holds stopped_installation; the edge export requires its own spool lock."""
    before = _revision(_regular(config.window_db_path))
    with closing(_readonly(config.window_db_path)) as connection:
        connection.execute("BEGIN")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise RuntimeError("frozen installation has a foreign key violation")
        summaries = {table: _rows(connection, table) for table in _CUT_TABLES}
        remote_cut = None
        if config.source_kind == "remote-capture":
            from sightglass.contracts.capture import CaptureAck
            from sightglass.source.capture import projection_origin_epoch
            from sightglass.source.remote import RemoteCaptureProvider, RemoteCaptureSettings

            if (
                config.source_settings_path is None
                or edge_state is None
                or set(edge_state) != _EDGE_KEYS
            ):
                raise RuntimeError("remote migration requires a stopped edge state export")
            settings = RemoteCaptureSettings.load(config.source_settings_path)
            if (
                edge_state["schema"] != "sightglass.edge-recovery-state.v1"
                or edge_state["source_instance_id"] != settings.source_instance_id
                or edge_state["account_id"] != settings.account_id
                or edge_state["origin_epoch"]
                != projection_origin_epoch(RemoteCaptureProvider(settings))
                or edge_state["epoch_lost"] is not False
            ):
                raise RuntimeError("resolve edge loss/binding before freezing a remote core")
            stream = _record(
                connection,
                _identity("stream", settings.source_instance_id, settings.account_id),
                CAPTURE_STREAM_TOOL,
            )
            if stream is None:
                position = CaptureStreamPosition(settings.stream_epoch, 1)
            else:
                if (
                    stream.get("source_instance_id") != settings.source_instance_id
                    or stream.get("account_id") != settings.account_id
                ):
                    raise RuntimeError("frozen stream identity mismatch")
                position = typed_value(CaptureStreamPosition, stream.get("position"))
            if (
                edge_state["stream_epoch"] != position.stream_epoch
                or type(edge_state["next_sequence"]) is not int
                or edge_state["next_sequence"] != position.next_sequence
            ):
                raise RuntimeError("edge and core high-water do not describe one committed cut")
            pending = edge_state["pending"]
            if pending is not None:
                if (
                    not isinstance(pending, dict)
                    or set(pending) != {"sequence", "batch_id", "request_id", "digest"}
                    or type(pending["sequence"]) is not int
                    or pending["sequence"] + 1 != position.next_sequence
                ):
                    raise RuntimeError("resolve an unreceived pending edge batch before migration")
                raw_ack = _record(
                    connection, _identity("batch", pending["batch_id"]), CAPTURE_BATCH_TOOL
                )
                ack = typed_value(CaptureAck, raw_ack) if raw_ack is not None else None
                request = _record(
                    connection, _identity("request", pending["request_id"]), CAPTURE_REQUEST_TOOL
                )
                request_outcome = connection.execute(
                    "SELECT outcome FROM access_receipts WHERE receipt_id=? AND tool_name=?",
                    (_identity("request", pending["request_id"]), CAPTURE_REQUEST_TOOL),
                ).fetchone()
                if (
                    ack is None
                    or ack.stream_epoch != position.stream_epoch
                    or ack.sequence != pending["sequence"]
                    or ack.batch_id != pending["batch_id"]
                    or ack.request_id != pending["request_id"]
                    or ack.envelope_digest != pending["digest"]
                    or ack.terminal not in {"accepted", "rejected", "cancelled", "epoch_loss"}
                    or not ack.receipt_id
                    or request is None
                    or request.get("generation") != config.activation_generation
                    or request_outcome is None
                    or request_outcome[0] != ack.terminal
                ):
                    raise RuntimeError(
                        "pending edge identity lacks its exact durable core terminal"
                    )
            remote_cut = {
                "position": {
                    "stream_epoch": position.stream_epoch,
                    "next_sequence": position.next_sequence,
                },
                "edge_state": edge_state,
            }
        elif edge_state is not None:
            raise RuntimeError("a full local installation has no remote edge cut")
        result = {
            "schema": CUT_SCHEMA,
            "window_schema": connection.execute("PRAGMA user_version").fetchone()[0],
            "window_member": config.window_db_path.relative_to(config.data_dir).as_posix(),
            "owner_generation": config.activation_generation or None,
            "mode": config.source_kind,
            "committed_state": summaries,
            "remote": remote_cut,
        }
    if _revision(_regular(config.window_db_path)) != before:
        raise RuntimeError("frozen DB changed while constructing its logical cut")
    return result


def verify_committed_cut(database: Path, cut: dict[str, Any]) -> None:
    """Reject a byte-valid transfer whose durable states do not match its logical cut.

    Run before the two declared path relocations; their complete parity verifier then
    protects the relocation. No activation/config is imported from the frozen source.
    """
    if not isinstance(cut.get("committed_state"), dict) or set(cut["committed_state"]) != set(
        _CUT_TABLES
    ):
        raise RuntimeError("invalid committed installation state")
    with closing(_readonly(database)) as connection:
        connection.execute("BEGIN")
        if (
            connection.execute("PRAGMA user_version").fetchone()[0] != cut.get("window_schema")
            or connection.execute("PRAGMA foreign_key_check").fetchone() is not None
        ):
            raise RuntimeError("received installation cut schema/integrity differs")
        for table in _CUT_TABLES:
            if _rows(connection, table) != cut["committed_state"][table]:
                raise RuntimeError("received reader/receive state differs from its committed cut")

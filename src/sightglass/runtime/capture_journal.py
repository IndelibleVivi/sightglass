"""Ordered receive state in WindowDB's reserved internal receipt namespace.

This is durable state, not access-audit retention. It shares the reader admission
transaction and retains batch identity plus stream high-water after body cleanup.
"""

from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from typing import Any

from sightglass.contracts.capture import (
    CaptureAck,
    CaptureDocument,
    CaptureProtocolError,
    CaptureStreamPosition,
)
from sightglass.model.db import WindowDB
from sightglass.source.capture.codec import json_value, typed_value

CAPTURE_STREAM_TOOL = "_sightglass_capture_stream.v1"
CAPTURE_BATCH_TOOL = "_sightglass_capture_batch.v1"
CAPTURE_REQUEST_TOOL = "_sightglass_capture_request.v1"
CAPTURE_RECOVERY_TOOL = "_sightglass_capture_recovery.v1"


def _identity(kind: str, *values: str) -> str:
    # Identity for a compound private scope; no account identifiers appear in receipt IDs.
    return (
        "sgcp_"
        + hashlib.sha256(json.dumps([kind, *values], separators=(",", ":")).encode()).hexdigest()
    )


class WindowCaptureJournal:
    def __init__(self, database: WindowDB) -> None:
        self.database = database

    @contextmanager
    def transaction(self):
        with self.database.transaction() as connection:
            yield connection

    def _read(self, receipt_id: str, tool: str) -> dict[str, Any] | None:
        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT warning_codes_json FROM access_receipts WHERE receipt_id=? AND tool_name=?",
                (receipt_id, tool),
            ).fetchone()
        if row is None:
            return None
        try:
            value = json.loads(row[0])
        except (ValueError, TypeError) as exc:
            raise CaptureProtocolError("corrupt_capture_journal") from exc
        if not isinstance(value, dict):
            raise CaptureProtocolError("corrupt_capture_journal")
        return value

    def stream_position(
        self,
        source_instance_id: str,
        account_id: str,
    ) -> CaptureStreamPosition | None:
        value = self._read(_identity("stream", source_instance_id, account_id), CAPTURE_STREAM_TOOL)
        if value is None:
            return None
        if (
            value.get("source_instance_id") != source_instance_id
            or value.get("account_id") != account_id
        ):
            raise CaptureProtocolError("capture_stream_identity_conflict")
        position = typed_value(CaptureStreamPosition, value.get("position"))
        if not position.stream_epoch or not 1 <= position.next_sequence <= (1 << 63) - 1:
            raise CaptureProtocolError("corrupt_capture_journal")
        return position

    def batch_receipt(self, batch_id: str) -> CaptureAck | None:
        value = self._read(_identity("batch", batch_id), CAPTURE_BATCH_TOOL)
        return typed_value(CaptureAck, value) if value is not None else None

    def record_terminal(self, document: CaptureDocument, ack: CaptureAck) -> None:
        self.record_loss_or_terminal(
            document.origin.source_instance_id,
            document.request.account_id,
            ack,
            completed_at=document.receipt.sealed_at,
        )

    def _write(
        self,
        receipt_id: str,
        tool: str,
        metadata: Any,
        completed_at: str,
        *,
        digest: str | None = None,
        mutable: bool = False,
    ) -> None:
        # Always called inside the caller's outer writer. No independent commit.
        with self.database.connection() as connection:
            conflict = (
                " ON CONFLICT(receipt_id) DO UPDATE SET "
                "completed_at=excluded.completed_at,"
                "warning_codes_json=excluded.warning_codes_json"
                if mutable
                else ""
            )
            connection.execute(
                "INSERT INTO access_receipts(receipt_id,reader_id,tool_name,conversation_id,"
                "scope_kind,scope_digest,message_count,resource_count,bytes_returned,started_at,"
                "completed_at,outcome,warning_codes_json) "
                "VALUES(?,'_edge',?,NULL,'capture',?,0,0,0,?,?,'terminal',?)" + conflict,
                (
                    receipt_id,
                    tool,
                    digest,
                    completed_at,
                    completed_at,
                    json.dumps(metadata, sort_keys=True, separators=(",", ":")),
                ),
            )

    def record_position(
        self,
        source_instance_id: str,
        account_id: str,
        position: CaptureStreamPosition,
        *,
        completed_at: str,
    ) -> None:
        self._write(
            _identity("stream", source_instance_id, account_id),
            CAPTURE_STREAM_TOOL,
            {
                "source_instance_id": source_instance_id,
                "account_id": account_id,
                "position": json_value(position),
            },
            completed_at,
            mutable=True,
        )

    def record_loss_or_terminal(
        self,
        source_instance_id: str,
        account_id: str,
        ack: CaptureAck,
        *,
        completed_at: str,
    ) -> None:
        with self.database.transaction() as _connection:
            current = self.stream_position(source_instance_id, account_id)
            if current is not None and (
                current.stream_epoch != ack.stream_epoch or current.next_sequence != ack.sequence
            ):
                raise CaptureProtocolError("capture_stream_changed_before_commit")
            if not 1 <= ack.sequence < (1 << 63) - 1:
                raise CaptureProtocolError("capture_sequence_exhausted")
            self._write(
                _identity("batch", ack.batch_id),
                CAPTURE_BATCH_TOOL,
                json_value(ack),
                completed_at,
                digest=ack.envelope_digest,
            )
            self.record_position(
                source_instance_id,
                account_id,
                CaptureStreamPosition(ack.stream_epoch, ack.sequence + 1),
                completed_at=completed_at,
            )

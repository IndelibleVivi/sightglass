"""Explicit stopped-only loss plans; transport recovery never changes reader state.

The operator exports cached edge identity, prepares a private plan on the core,
then applies it under the daemon lock. An existing terminal ACK stays immutable.
Only an unreceived exact pending batch gains an epoch_loss terminal. The new
epoch and monotonic floor commit with the recovery receipt in WindowDB.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sightglass.contracts.capture import CaptureAck, CaptureProtocolError, CaptureStreamPosition
from sightglass.contracts.common import utc_now
from sightglass.source.capture.codec import canonical_json, json_value, typed_value

from .capture_journal import (
    CAPTURE_RECOVERY_TOOL,
    CAPTURE_REQUEST_TOOL,
    WindowCaptureJournal,
    _identity,
)
from .migration import _fsync_directory, _open_read, _private_directory, _regular, _revision

RECOVERY_SCHEMA = "sightglass.capture-recovery.v1"
MAX_SEQUENCE = (1 << 63) - 1
MAX_RECOVERY_BYTES = 512 * 1024


def read_private_json(path: Path) -> dict[str, Any]:
    _private_directory(path.parent)
    before = _revision(_regular(path))
    if before[2] > MAX_RECOVERY_BYTES:
        raise CaptureProtocolError("recovery_record_too_large")
    with _open_read(path) as handle:
        payload = handle.read(MAX_RECOVERY_BYTES + 1)
    if len(payload) > MAX_RECOVERY_BYTES or _revision(_regular(path)) != before:
        raise CaptureProtocolError("recovery_record_changed")
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise CaptureProtocolError("invalid_recovery_record")
    return value


def write_private_json(path: Path, value: dict[str, Any], *, expected: Any = None) -> None:
    _private_directory(path.parent)
    payload = canonical_json(value)
    if len(payload) > MAX_RECOVERY_BYTES:
        raise CaptureProtocolError("recovery_record_too_large")
    if expected is None:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    else:
        if canonical_json(read_private_json(path)) != canonical_json(expected):
            raise CaptureProtocolError("recovery_record_changed")
        descriptor, temporary = tempfile.mkstemp(prefix=".recovery-", dir=path.parent)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            Path(temporary).unlink(missing_ok=True)
    _fsync_directory(path.parent)


def _identifier(value: Any) -> str:
    if not isinstance(value, str) or not value or "\x00" in value or len(value.encode()) > 65_536:
        raise CaptureProtocolError("invalid_recovery_identity")
    return value


def _position(value: Any) -> CaptureStreamPosition:
    result = typed_value(CaptureStreamPosition, value)
    _identifier(result.stream_epoch)
    if type(result.next_sequence) is not int or not 1 <= result.next_sequence <= MAX_SEQUENCE:
        raise CaptureProtocolError("invalid_recovery_sequence")
    return result


def _pending(value: Any, stream_epoch: str) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != {
        "sequence",
        "batch_id",
        "request_id",
        "digest",
    }:
        raise CaptureProtocolError("invalid_recovery_pending")
    sequence = value["sequence"]
    if type(sequence) is not int or not 1 <= sequence < MAX_SEQUENCE:
        raise CaptureProtocolError("invalid_recovery_sequence")
    for name in ("batch_id", "request_id"):
        _identifier(value[name])
    if not isinstance(value["digest"], str) or not re.fullmatch(r"[a-f0-9]{64}", value["digest"]):
        raise CaptureProtocolError("invalid_recovery_digest")
    return {**value, "stream_epoch": stream_epoch}


@dataclass(frozen=True)
class CaptureRecoveryPlan:
    source_instance_id: str
    account_id: str
    origin_epoch: str
    activation_generation: str
    receipt_id: str
    previous: CaptureStreamPosition
    target: CaptureStreamPosition
    pending: dict[str, Any] | None

    def as_dict(self) -> dict[str, Any]:
        return {"schema": RECOVERY_SCHEMA, **json_value(self)}

    @classmethod
    def parse(cls, value: Any) -> CaptureRecoveryPlan:
        if (
            not isinstance(value, dict)
            or value.get("schema") != RECOVERY_SCHEMA
            or set(value) != set(cls.__dataclass_fields__) | {"schema"}
        ):
            raise CaptureProtocolError("invalid_recovery_plan")
        identities = {
            name: _identifier(value[name])
            for name in (
                "source_instance_id",
                "account_id",
                "origin_epoch",
                "activation_generation",
                "receipt_id",
            )
        }
        previous, target = _position(value["previous"]), _position(value["target"])
        pending = value["pending"]
        if pending is not None:
            if (
                not isinstance(pending, dict)
                or pending.get("stream_epoch") != previous.stream_epoch
            ):
                raise CaptureProtocolError("recovery_pending_epoch_conflict")
            pending = _pending(
                {key: item for key, item in pending.items() if key != "stream_epoch"},
                previous.stream_epoch,
            )
        floor = (
            max(previous.next_sequence, int(pending["sequence"]) + 1)
            if pending
            else (previous.next_sequence + 1)
        )
        if target.stream_epoch == previous.stream_epoch or target.next_sequence < floor:
            raise CaptureProtocolError("recovery_cannot_reset_sequence")
        return cls(**identities, previous=previous, target=target, pending=pending)


def prepare_recovery(
    journal: WindowCaptureJournal,
    *,
    source_instance_id: str,
    account_id: str,
    origin_epoch: str,
    activation_generation: str,
    enrolled_epoch: str,
    new_epoch: str,
    next_sequence: int,
    edge_state: dict[str, Any] | None,
) -> CaptureRecoveryPlan:
    position = journal.stream_position(source_instance_id, account_id) or CaptureStreamPosition(
        enrolled_epoch, 1
    )
    pending = None
    if edge_state is not None:
        if (
            set(edge_state)
            != {
                "schema",
                "source_instance_id",
                "account_id",
                "origin_epoch",
                "stream_epoch",
                "next_sequence",
                "epoch_lost",
                "pending",
            }
            or edge_state.get("schema") != "sightglass.edge-recovery-state.v1"
            or edge_state.get("source_instance_id") != source_instance_id
            or edge_state.get("account_id") != account_id
            or edge_state.get("origin_epoch") != origin_epoch
            or edge_state.get("stream_epoch") != position.stream_epoch
            or edge_state.get("epoch_lost") is not True
        ):
            raise CaptureProtocolError("recovery_edge_binding_conflict")
        pending = _pending(edge_state.get("pending"), position.stream_epoch)
        edge_next = edge_state.get("next_sequence")
        if (
            type(edge_next) is not int
            or not 1 <= edge_next <= MAX_SEQUENCE
            or next_sequence < edge_next
        ):
            raise CaptureProtocolError("recovery_cannot_reset_sequence")
    result = CaptureRecoveryPlan(
        source_instance_id,
        account_id,
        origin_epoch,
        activation_generation,
        "sgrecovery_" + uuid.uuid4().hex,
        position,
        CaptureStreamPosition(new_epoch, next_sequence),
        pending,
    )
    return CaptureRecoveryPlan.parse(result.as_dict())


def apply_recovery(journal: WindowCaptureJournal, plan: CaptureRecoveryPlan) -> dict[str, Any]:
    plan = CaptureRecoveryPlan.parse(plan.as_dict())
    receipt_key = _identity("recovery", plan.receipt_id)
    with journal.database.transaction(maintenance=True) as connection:
        recorded = journal._read(receipt_key, CAPTURE_RECOVERY_TOOL)
        if recorded is not None:
            if canonical_json(recorded.get("plan")) != canonical_json(plan.as_dict()):
                raise CaptureProtocolError("recovery_receipt_identity_conflict")
            return recorded
        current = journal.stream_position(plan.source_instance_id, plan.account_id)
        if (current or CaptureStreamPosition(plan.previous.stream_epoch, 1)) != plan.previous:
            raise CaptureProtocolError("recovery_stream_changed_before_commit")
        ack = None
        pending = plan.pending
        now = utc_now().isoformat()
        if pending is not None:
            ack = journal.batch_receipt(pending["batch_id"])
            if ack is not None:
                if (
                    ack.stream_epoch != pending["stream_epoch"]
                    or ack.sequence != pending["sequence"]
                    or ack.envelope_digest != pending["digest"]
                    or ack.request_id != pending["request_id"]
                    or ack.sequence + 1 != plan.previous.next_sequence
                    or ack.terminal not in {"accepted", "rejected", "cancelled", "epoch_loss"}
                    or not ack.receipt_id
                ):
                    raise CaptureProtocolError("recovery_batch_identity_conflict")
            else:
                if pending["sequence"] != plan.previous.next_sequence:
                    raise CaptureProtocolError("recovery_pending_out_of_order")
                request_key = _identity("request", pending["request_id"])
                request = journal._read(request_key, CAPTURE_REQUEST_TOOL)
                if (
                    request is None
                    or request.get("generation") != plan.activation_generation
                    or request.get("request", {}).get("request_id") != pending["request_id"]
                    or request.get("request", {}).get("account_id") != plan.account_id
                ):
                    raise CaptureProtocolError("recovery_request_not_authorized")
                ack = CaptureAck(
                    pending["stream_epoch"],
                    pending["sequence"],
                    pending["batch_id"],
                    pending["digest"],
                    pending["request_id"],
                    "epoch_loss",
                    plan.receipt_id,
                )
                journal.record_loss_or_terminal(
                    plan.source_instance_id, plan.account_id, ack, completed_at=now
                )
                connection.execute(
                    "UPDATE access_receipts SET outcome='epoch_loss',completed_at=? "
                    "WHERE receipt_id=? AND tool_name=?",
                    (now, request_key, CAPTURE_REQUEST_TOOL),
                )
        result = {
            "schema": RECOVERY_SCHEMA,
            "plan": plan.as_dict(),
            "applied": True,
            "ack": json_value(ack),
            "completed_at": now,
        }
        journal.record_position(
            plan.source_instance_id, plan.account_id, plan.target, completed_at=now
        )
        journal._write(receipt_key, CAPTURE_RECOVERY_TOOL, result, now)
        return result


def recover_core(config: Any, *, plan: CaptureRecoveryPlan) -> dict[str, Any]:
    """Apply one exact plan while the full runtime is stopped, then reconcile config.

    The DB receipt commits first. If the config write is interrupted, applying the same
    private plan again returns its durable receipt and completes the same config update.
    """
    from dataclasses import replace

    from sightglass.model.db import WindowDB
    from sightglass.source.capture import projection_origin_epoch
    from sightglass.source.remote import RemoteCaptureProvider, RemoteCaptureSettings

    from .activation import require_core_activation
    from .migration_state import stopped_installation

    if config.source_kind != "remote-capture" or config.source_settings_path is None:
        raise CaptureProtocolError("recovery_requires_remote_core")
    with stopped_installation(config):
        require_core_activation(config)
        settings = RemoteCaptureSettings.load(config.source_settings_path)
        if (
            plan.source_instance_id != settings.source_instance_id
            or plan.account_id != settings.account_id
            or plan.origin_epoch != projection_origin_epoch(RemoteCaptureProvider(settings))
            or plan.activation_generation != config.activation_generation
            or settings.stream_epoch not in {plan.previous.stream_epoch, plan.target.stream_epoch}
        ):
            raise CaptureProtocolError("recovery_plan_binding_conflict")
        journal = WindowCaptureJournal(
            WindowDB(config.window_db_path, write_guard=lambda: require_core_activation(config))
        )
        result = apply_recovery(journal, plan)
        if settings.stream_epoch != plan.target.stream_epoch:
            write_private_json(
                config.source_settings_path,
                replace(settings, stream_epoch=plan.target.stream_epoch).as_dict(),
                expected=settings.as_dict(),
            )
        return result


def plan_core_recovery(
    config: Any,
    *,
    new_epoch: str,
    next_sequence: int,
    edge_state: dict[str, Any] | None,
) -> CaptureRecoveryPlan:
    from sightglass.model.db import WindowDB
    from sightglass.source.capture import projection_origin_epoch
    from sightglass.source.remote import RemoteCaptureProvider, RemoteCaptureSettings

    from .activation import require_core_activation
    from .migration_state import stopped_installation

    if config.source_kind != "remote-capture" or config.source_settings_path is None:
        raise CaptureProtocolError("recovery_requires_remote_core")
    with stopped_installation(config):
        require_core_activation(config)
        settings = RemoteCaptureSettings.load(config.source_settings_path)
        journal = WindowCaptureJournal(
            WindowDB(config.window_db_path, write_guard=lambda: require_core_activation(config))
        )
        return prepare_recovery(
            journal,
            source_instance_id=settings.source_instance_id,
            account_id=settings.account_id,
            origin_epoch=projection_origin_epoch(RemoteCaptureProvider(settings)),
            activation_generation=config.activation_generation,
            enrolled_epoch=settings.stream_epoch,
            new_epoch=new_epoch,
            next_sequence=next_sequence,
            edge_state=edge_state,
        )


def inspect_edge_recovery(settings: Any) -> dict[str, Any]:
    from .edge import EdgeSpool

    settings.require_active()
    with EdgeSpool(
        settings.spool_directory,
        source_instance_id=settings.source_instance_id,
        account_id=settings.account_id,
        origin_epoch=settings.origin_epoch,
    ) as spool:
        return spool.recovery_state()


def recover_edge(settings_path: Path, receipt: dict[str, Any], *, whole_spool_lost: bool) -> None:
    """Finish an operator-exported durable core transition. Never invent a receipt.

    The receipt must come from the enrolled core's private stopped-only command over
    its authenticated operator channel. It is not a reader or wire ACK capability.
    """
    from dataclasses import replace

    from .edge import EdgeSpool
    from .edge_runtime import EdgeSettings

    settings = EdgeSettings.load(settings_path)
    settings.require_active()
    if (
        set(receipt) != {"schema", "plan", "applied", "ack", "completed_at"}
        or receipt.get("schema") != RECOVERY_SCHEMA
        or receipt.get("applied") is not True
    ):
        raise CaptureProtocolError("core_recovery_receipt_required")
    plan = CaptureRecoveryPlan.parse(receipt["plan"])
    if (
        plan.source_instance_id != settings.source_instance_id
        or plan.account_id != settings.account_id
        or plan.origin_epoch != settings.origin_epoch
        or plan.activation_generation != settings.core_generation
        or settings.stream_epoch not in {plan.previous.stream_epoch, plan.target.stream_epoch}
    ):
        raise CaptureProtocolError("recovery_edge_binding_conflict")
    if whole_spool_lost:
        if plan.pending is not None or receipt["ack"] is not None:
            raise CaptureProtocolError("whole_spool_loss_cannot_replace_known_pending")
        if settings.spool_directory.exists() and any(settings.spool_directory.iterdir()):
            spool = EdgeSpool(
                settings.spool_directory,
                source_instance_id=settings.source_instance_id,
                account_id=settings.account_id,
                origin_epoch=settings.origin_epoch,
            )
            if not spool.matches_transition(
                plan.previous.stream_epoch, plan.target, plan.receipt_id
            ):
                spool.close()
                raise CaptureProtocolError("whole_spool_loss_requires_a_new_empty_directory")
        else:
            spool = EdgeSpool.initialize(
                settings.spool_directory,
                source_instance_id=settings.source_instance_id,
                account_id=settings.account_id,
                origin_epoch=settings.origin_epoch,
                stream_epoch=plan.target.stream_epoch,
                next_sequence=plan.target.next_sequence,
                previous_epoch=plan.previous.stream_epoch,
                transition_receipt_id=plan.receipt_id,
            )
    else:
        spool = EdgeSpool(
            settings.spool_directory,
            source_instance_id=settings.source_instance_id,
            account_id=settings.account_id,
            origin_epoch=settings.origin_epoch,
        )
    with spool:
        if spool.stream_epoch == plan.previous.stream_epoch:
            state = spool.recovery_state()
            pending = _pending(state["pending"], plan.previous.stream_epoch)
            if pending is not None:
                if pending != plan.pending or receipt["ack"] is None:
                    raise CaptureProtocolError("recovery_pending_identity_changed")
                spool.acknowledge_epoch_loss(typed_value(CaptureAck, receipt["ack"]))
            elif receipt["ack"] is not None:
                # Retry after the pending deletion committed but before epoch transition.
                if not spool.matches_last_ack(typed_value(CaptureAck, receipt["ack"])):
                    raise CaptureProtocolError("recovery_terminal_identity_changed")
            spool.transition_epoch(
                new_epoch=plan.target.stream_epoch,
                next_sequence=plan.target.next_sequence,
                receipt_id=plan.receipt_id,
            )
        elif not spool.matches_transition(plan.previous.stream_epoch, plan.target, plan.receipt_id):
            raise CaptureProtocolError("recovery_edge_transition_changed")
        settings.require_active()
        if settings.stream_epoch != plan.target.stream_epoch:
            write_private_json(
                settings_path,
                replace(settings, stream_epoch=plan.target.stream_epoch).as_dict(),
                expected=settings.as_dict(),
            )

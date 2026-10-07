"""Atomic receive hooks. Storage belongs to the existing core transaction owner.

No receive sidecar is created here. The journal implementation must use the same
connection/transaction as message admission and reader ACK/delivery state.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Protocol

from sightglass.contracts.capture import (
    CaptureAck,
    CaptureDocument,
    CaptureExpectation,
    CaptureProtocolError,
    CaptureStreamPosition,
)

from .codec import SealedCapture, validate_capture
from .frozen import FrozenCaptureProvider


class ReceiveJournal(Protocol):
    """Implement using the caller's WindowDB connection; never independent commit.

    `record_terminal` atomically inserts the exact ACK and advances stream high-water
    to sequence+1. That state may not be pruned by ordinary access-receipt retention.
    A batch ID remains immutable even when earlier source body digests repeat.
    """

    def transaction(self) -> AbstractContextManager[object]: ...

    def stream_position(
        self, source_instance_id: str, account_id: str
    ) -> CaptureStreamPosition | None: ...

    def batch_receipt(self, batch_id: str) -> CaptureAck | None: ...

    def record_terminal(self, document: CaptureDocument, ack: CaptureAck) -> None: ...


@dataclass(frozen=True)
class PreparedReceive:
    envelope: SealedCapture
    document: CaptureDocument


def ack_matches(envelope: SealedCapture, ack: CaptureAck) -> bool:
    document = envelope.document()
    return (
        ack.stream_epoch == document.origin.stream_epoch
        and ack.sequence == document.origin.sequence
        and ack.batch_id == document.origin.batch_id
        and ack.envelope_digest == envelope.digest
        and ack.request_id == document.request.request_id
        and bool(ack.receipt_id)
        and ack.terminal
        in (
            {"accepted", "rejected", "cancelled"}
            if document.receipt.terminal == "complete"
            else {document.receipt.terminal}
        )
    )


class CaptureReceiver:
    def __init__(
        self,
        journal: ReceiveJournal,
        expected: CaptureExpectation,
        *,
        stream_epoch: str,
    ) -> None:
        if not stream_epoch:
            raise ValueError("explicit enrolled stream epoch is required")
        self.journal = journal
        self.expected = expected
        self.stream_epoch = stream_epoch

    def prepare(self, envelope: SealedCapture) -> PreparedReceive:
        # Freshness is checked after durable dedup below: a lost ACK must replay
        # even when its already-committed fresh receipt has subsequently expired.
        return PreparedReceive(
            envelope, validate_capture(envelope, self.expected, require_fresh=False)
        )

    def lookup_terminal(self, envelope: SealedCapture) -> CaptureAck | None:
        document = envelope.document()
        ack = self.journal.batch_receipt(document.origin.batch_id)
        if ack is not None and not ack_matches(envelope, ack):
            raise CaptureProtocolError("batch_identity_conflict")
        return ack

    def admit_in_transaction(
        self,
        prepared: PreparedReceive,
        admission_hook: Callable[[FrozenCaptureProvider, CaptureDocument], None] | None,
        *,
        receipt_id: str,
        reject: bool = False,
        now: str | None = None,
    ) -> CaptureAck:
        """Caller already holds its write transaction. Performs only local work.

        The returned ACK is provisional until that *outer* transaction commits.
        Only its after-commit callback may pass it to PreparedRemoteCapture.complete.
        """
        envelope, document = prepared.envelope, prepared.document
        duplicate = self.lookup_terminal(envelope)
        if duplicate is not None:
            return duplicate
        position = self.journal.stream_position(
            document.origin.source_instance_id, document.request.account_id
        )
        expected_sequence = position.next_sequence if position is not None else 1
        epoch = position.stream_epoch if position is not None else self.stream_epoch
        if document.origin.stream_epoch != epoch:
            raise CaptureProtocolError("epoch_transition_required")
        if document.origin.sequence != expected_sequence:
            raise CaptureProtocolError("capture_out_of_order")
        validate_capture(envelope, self.expected, require_fresh=not reject, now=now)
        if not receipt_id:
            raise CaptureProtocolError("durable_receipt_id_required")
        remote_terminal = document.receipt.terminal
        if remote_terminal != "complete":
            terminal = remote_terminal
        elif reject:
            terminal = "rejected"
        else:
            if admission_hook is None:
                raise CaptureProtocolError("admission_hook_required")
            admission_hook(FrozenCaptureProvider(envelope), document)
            terminal = "accepted"
        ack = CaptureAck(
            document.origin.stream_epoch,
            document.origin.sequence,
            document.origin.batch_id,
            envelope.digest,
            document.request.request_id,
            terminal,
            receipt_id,  # type: ignore[arg-type]
        )
        self.journal.record_terminal(document, ack)
        return ack

    def admit(
        self,
        prepared: PreparedReceive,
        admission_hook: Callable[[FrozenCaptureProvider, CaptureDocument], None] | None,
        *,
        receipt_id: str,
        reject: bool = False,
        now: str | None = None,
    ) -> CaptureAck:
        """Background convenience; ACK is returned only after journal commit."""
        with self.journal.transaction():
            ack = self.admit_in_transaction(
                prepared, admission_hook, receipt_id=receipt_id, reject=reject, now=now
            )
        return ack

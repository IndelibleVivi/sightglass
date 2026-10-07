"""Thin source-edge state: one finite durable sealed batch, no WindowDB."""

from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import stat
import threading
import uuid
from pathlib import Path

from sightglass.contracts.capture import (
    MAX_EDGE_SPOOL_BYTES,
    CaptureAck,
    CaptureProtocolError,
    CaptureStreamPosition,
)
from sightglass.source.capture.codec import SealedCapture, canonical_json, strict_json, typed_value
from sightglass.source.capture.receiver import ack_matches

_DB_NAME = "edge-spool.sqlite"
_PAYLOAD_NAME = "pending.capture"
_STAGING_NAME = "pending.staging"
_SQLITE_ALLOWANCE = 128 * 1024
_MAX_SEQUENCE = (1 << 63) - 1
EDGE_RECOVERY_STATE_SCHEMA = "sightglass.edge-recovery-state.v1"


def _regular_private(path: Path) -> None:
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise CaptureProtocolError("edge_file_not_private_regular")


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class EdgeSpool:
    """The spool epoch is enrolled explicitly; a missing DB never restarts at zero.

    SQLite holds the stream identity/next sequence and one pending file reference in
    one FULL-synchronous transaction. The immutable file is fsynced before that
    reference commits. Staging and SQLite journals count toward the same capacity.
    """

    @classmethod
    def initialize(
        cls,
        directory: Path,
        *,
        source_instance_id: str,
        account_id: str,
        origin_epoch: str,
        stream_epoch: str | None = None,
        next_sequence: int = 1,
        previous_epoch: str | None = None,
        transition_receipt_id: str | None = None,
        max_bytes: int = MAX_EDGE_SPOOL_BYTES,
    ) -> EdgeSpool:
        if not _SQLITE_ALLOWANCE < max_bytes <= MAX_EDGE_SPOOL_BYTES:
            raise ValueError("edge spool capacity must be bounded by 64 MiB")
        if type(next_sequence) is not int or not 1 <= next_sequence <= _MAX_SEQUENCE:
            raise CaptureProtocolError("invalid_sequence")
        if next_sequence != 1 and previous_epoch is None:
            raise CaptureProtocolError("epoch_transition_receipt_required")
        if previous_epoch is not None and (
            not previous_epoch
            or not transition_receipt_id
            or not stream_epoch
            or stream_epoch == previous_epoch
        ):
            raise CaptureProtocolError("epoch_transition_receipt_required")
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        metadata = directory.lstat()
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid():
            raise CaptureProtocolError("edge_directory_not_private")
        if any(directory.iterdir()):
            raise CaptureProtocolError("edge_spool_already_exists")
        os.chmod(directory, 0o700)
        descriptor = os.open(directory / _DB_NAME, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(descriptor)
        connection = sqlite3.connect(directory / _DB_NAME)
        try:
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript("""
                CREATE TABLE stream (
                    singleton INTEGER PRIMARY KEY CHECK (singleton=1),
                    source_instance_id TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    origin_epoch TEXT NOT NULL,
                    stream_epoch TEXT NOT NULL,
                    next_sequence INTEGER NOT NULL CHECK (next_sequence>=1),
                    epoch_lost INTEGER NOT NULL DEFAULT 0,
                    previous_epoch TEXT,
                    transition_receipt_id TEXT,
                    last_ack_json TEXT
                );
                CREATE TABLE pending (
                    singleton INTEGER PRIMARY KEY CHECK (singleton=1),
                    sequence INTEGER NOT NULL,
                    batch_id TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    file_bytes INTEGER NOT NULL
                );
                PRAGMA user_version=1;
            """)
            with connection:
                connection.execute(
                    "INSERT INTO stream(singleton,source_instance_id,account_id,origin_epoch,"
                    "stream_epoch,next_sequence,previous_epoch,transition_receipt_id) "
                    "VALUES(1,?,?,?,?,?,?,?)",
                    (
                        source_instance_id,
                        account_id,
                        origin_epoch,
                        stream_epoch or uuid.uuid4().hex,
                        next_sequence,
                        previous_epoch,
                        transition_receipt_id,
                    ),
                )
        finally:
            connection.close()
        _sync_directory(directory)
        return cls(
            directory,
            source_instance_id=source_instance_id,
            account_id=account_id,
            origin_epoch=origin_epoch,
            max_bytes=max_bytes,
        )

    def __init__(
        self,
        directory: Path,
        *,
        source_instance_id: str,
        account_id: str,
        origin_epoch: str,
        max_bytes: int = MAX_EDGE_SPOOL_BYTES,
    ) -> None:
        if not _SQLITE_ALLOWANCE < max_bytes <= MAX_EDGE_SPOOL_BYTES:
            raise ValueError("edge spool capacity must be bounded by 64 MiB")
        self.directory = directory
        self.max_bytes = max_bytes
        self._binding = (source_instance_id, account_id, origin_epoch)
        self._lock = threading.RLock()
        self._closed = False
        if not directory.exists() or not (directory / _DB_NAME).exists():
            raise CaptureProtocolError("epoch_loss_requires_explicit_recovery")
        directory_metadata = directory.lstat()
        if (
            not stat.S_ISDIR(directory_metadata.st_mode)
            or directory_metadata.st_uid != os.geteuid()
            or stat.S_IMODE(directory_metadata.st_mode) != 0o700
        ):
            raise CaptureProtocolError("edge_directory_not_private")
        _regular_private(directory / _DB_NAME)
        self._lock_fd = os.open(
            directory / "edge.lock",
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            _regular_private(directory / "edge.lock")
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, CaptureProtocolError) as exc:
            os.close(self._lock_fd)
            raise CaptureProtocolError("edge_spool_in_use_or_unsafe") from exc
        self._connection = sqlite3.connect(directory / _DB_NAME, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=DELETE")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute("PRAGMA temp_store=MEMORY")
        try:
            if self._connection.execute("PRAGMA user_version").fetchone()[0] != 1:
                raise CaptureProtocolError("edge_spool_version_mismatch")
            stream = self._state()
            if (
                stream["source_instance_id"],
                stream["account_id"],
                stream["origin_epoch"],
            ) != self._binding:
                raise CaptureProtocolError("edge_spool_binding_changed")
            self._recover_owned_files()
        except BaseException:
            self.close()
            raise

    def _state(self) -> sqlite3.Row:
        row = self._connection.execute("SELECT * FROM stream WHERE singleton=1").fetchone()
        if row is None:
            raise CaptureProtocolError("epoch_loss_requires_explicit_recovery")
        return row

    @property
    def stream_epoch(self) -> str:
        with self._lock:
            return str(self._state()["stream_epoch"])

    @property
    def next_sequence(self) -> int:
        with self._lock:
            return int(self._state()["next_sequence"])

    @property
    def epoch_lost(self) -> bool:
        with self._lock:
            return bool(self._state()["epoch_lost"])

    def recovery_state(self) -> dict[str, object]:
        """Return only bound cached identity, including when pending bytes are lost.

        This operator snapshot does not read payloads or establish a core terminal
        decision. The core must still verify its own immutable receipt/stream state.
        """
        with self._lock:
            if self._closed:
                raise CaptureProtocolError("edge_spool_closed")
            state = self._state()
            if (
                state["source_instance_id"],
                state["account_id"],
                state["origin_epoch"],
            ) != self._binding:
                raise CaptureProtocolError("edge_spool_binding_changed")
            if (
                any(
                    not isinstance(state[key], str) or not state[key]
                    for key in ("source_instance_id", "account_id", "origin_epoch", "stream_epoch")
                )
                or type(state["next_sequence"]) is not int
                or not 1 <= state["next_sequence"] <= _MAX_SEQUENCE
                or state["epoch_lost"] not in (0, 1)
            ):
                raise CaptureProtocolError("edge_recovery_state_corrupt")
            row = self._connection.execute("SELECT * FROM pending WHERE singleton=1").fetchone()
            pending: dict[str, object] | None = None
            if row is not None:
                if (
                    type(row["sequence"]) is not int
                    or row["sequence"] != state["next_sequence"] - 1
                    or not 1 <= row["sequence"] < _MAX_SEQUENCE
                    or any(
                        not isinstance(row[key], str) or not row[key]
                        for key in ("batch_id", "request_id", "digest")
                    )
                    or len(row["digest"]) != 64
                    or any(character not in "0123456789abcdef" for character in row["digest"])
                ):
                    raise CaptureProtocolError("edge_recovery_state_corrupt")
                pending = {
                    "sequence": row["sequence"],
                    "batch_id": row["batch_id"],
                    "request_id": row["request_id"],
                    "digest": row["digest"],
                }
            return {
                "schema": EDGE_RECOVERY_STATE_SCHEMA,
                "source_instance_id": state["source_instance_id"],
                "account_id": state["account_id"],
                "origin_epoch": state["origin_epoch"],
                "stream_epoch": state["stream_epoch"],
                "next_sequence": state["next_sequence"],
                "epoch_lost": bool(state["epoch_lost"]),
                "pending": pending,
            }

    def used_bytes(self) -> int:
        total = 0
        for path in self.directory.iterdir():
            _regular_private(path)
            total += path.stat().st_size
        return total

    def _recover_owned_files(self) -> None:
        pending = self._connection.execute("SELECT * FROM pending WHERE singleton=1").fetchone()
        # These are this spool's uncommitted/retired staging names. No source, CAS,
        # account, or arbitrary path is eligible for this private cleanup.
        staging = self.directory / _STAGING_NAME
        if staging.exists():
            _regular_private(staging)
            staging.unlink()
        payload = self.directory / _PAYLOAD_NAME
        if pending is None and payload.exists():
            _regular_private(payload)
            payload.unlink()
        if pending is not None:
            try:
                self.pending()
            except (CaptureProtocolError, OSError):
                with self._connection:
                    self._connection.execute("UPDATE stream SET epoch_lost=1 WHERE singleton=1")
        _sync_directory(self.directory)

    def pending(self) -> SealedCapture | None:
        with self._lock:
            row = self._connection.execute("SELECT * FROM pending WHERE singleton=1").fetchone()
            if row is None:
                return None
            path = self.directory / _PAYLOAD_NAME
            try:
                _regular_private(path)
                if path.stat().st_size != row["file_bytes"] or row["file_bytes"] > self.max_bytes:
                    raise CaptureProtocolError("pending_capture_size_mismatch")
                descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
                with os.fdopen(descriptor, "rb") as stream:
                    data = stream.read(int(row["file_bytes"]) + 1)
                envelope = SealedCapture.from_bytes(data)
                document = envelope.document()
                if (
                    envelope.digest != row["digest"]
                    or document.origin.batch_id != row["batch_id"]
                    or document.origin.sequence != row["sequence"]
                    or document.request.request_id != row["request_id"]
                    or document.origin.stream_epoch != self.stream_epoch
                ):
                    raise CaptureProtocolError("pending_capture_identity_mismatch")
                return envelope
            except OSError as exc:
                raise CaptureProtocolError("pending_capture_lost") from exc

    def store(self, envelope: SealedCapture) -> None:
        data = envelope.to_bytes()
        document = envelope.document()
        with self._lock:
            if self.epoch_lost:
                raise CaptureProtocolError("epoch_loss_requires_explicit_recovery")
            state = self._state()
            existing = self.pending()
            if existing is not None:
                if existing.to_bytes() == data:
                    return
                raise CaptureProtocolError("edge_pending_batch_must_be_acknowledged")
            if (
                document.origin.sequence != state["next_sequence"]
                or document.origin.stream_epoch != state["stream_epoch"]
                or document.origin.source_instance_id != state["source_instance_id"]
                or document.origin.origin_epoch != state["origin_epoch"]
                or document.request.account_id != state["account_id"]
            ):
                raise CaptureProtocolError("edge_batch_stream_mismatch")
            if state["next_sequence"] >= _MAX_SEQUENCE:
                raise CaptureProtocolError("edge_sequence_exhausted")
            if self.used_bytes() + len(data) + _SQLITE_ALLOWANCE > self.max_bytes:
                raise CaptureProtocolError("edge_spool_pressure")
            staging = self.directory / _STAGING_NAME
            descriptor = os.open(staging, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(staging, self.directory / _PAYLOAD_NAME)
                _sync_directory(self.directory)
                with self._connection:
                    self._connection.execute(
                        "INSERT INTO pending VALUES(1,?,?,?,?,?)",
                        (
                            document.origin.sequence,
                            document.origin.batch_id,
                            document.request.request_id,
                            envelope.digest,
                            len(data),
                        ),
                    )
                    self._connection.execute(
                        "UPDATE stream SET next_sequence=next_sequence+1 WHERE singleton=1"
                    )
            except BaseException:
                self._recover_owned_files()
                raise

    def acknowledge(self, ack: CaptureAck) -> None:
        with self._lock:
            envelope = self.pending()
            if envelope is None:
                prior = self._state()["last_ack_json"]
                if prior is not None and typed_value(CaptureAck, json.loads(str(prior))) == ack:
                    return
                raise CaptureProtocolError("edge_ack_without_pending_batch")
            if not ack_matches(envelope, ack):
                raise CaptureProtocolError("edge_ack_mismatch")
            ack_json = self._bounded_ack_json(ack)
            with self._connection:
                self._connection.execute("DELETE FROM pending WHERE singleton=1")
                self._connection.execute(
                    "UPDATE stream SET last_ack_json=? WHERE singleton=1",
                    (ack_json,),
                )
            payload = self.directory / _PAYLOAD_NAME
            _regular_private(payload)
            payload.unlink()
            _sync_directory(self.directory)

    def _bounded_ack_json(self, ack: CaptureAck) -> str:
        encoded = canonical_json(ack)
        # The normal terminal ACK fits the publication's reserved allowance.
        # A larger operator receipt must still leave room for its SQLite pages
        # and rollback journal before either durable state or bytes are released.
        reservation = max(_SQLITE_ALLOWANCE, 2 * len(encoded) + 65_536)
        if self.used_bytes() + reservation > self.max_bytes:
            raise CaptureProtocolError("edge_spool_pressure")
        return encoded.decode()

    def acknowledge_epoch_loss(self, ack: CaptureAck) -> None:
        """Explicit terminal handshake for corrupted/lost pending bytes.

        An operator may supply the exact already committed accepted/rejected/
        cancelled ACK after wire-ACK loss, preserving its immutable core receipt.
        When the core never admitted the batch, it must durably record epoch_loss
        instead. This API cannot manufacture core acceptance or source coverage.
        """
        with self._lock:
            row = self._connection.execute("SELECT * FROM pending WHERE singleton=1").fetchone()
            if (
                not self.epoch_lost
                or row is None
                or (
                    ack.terminal not in ("accepted", "rejected", "cancelled", "epoch_loss")
                    or ack.stream_epoch != self.stream_epoch
                    or type(ack.sequence) is not int
                    or ack.sequence != row["sequence"]
                    or ack.batch_id != row["batch_id"]
                    or ack.envelope_digest != row["digest"]
                    or ack.request_id != row["request_id"]
                    or not isinstance(ack.receipt_id, str)
                    or not ack.receipt_id
                )
            ):
                raise CaptureProtocolError("epoch_loss_ack_mismatch")
            ack_json = self._bounded_ack_json(ack)
            with self._connection:
                self._connection.execute("DELETE FROM pending WHERE singleton=1")
                self._connection.execute(
                    "UPDATE stream SET last_ack_json=? WHERE singleton=1",
                    (ack_json,),
                )
            self._recover_owned_files()

    def transition_epoch(self, *, new_epoch: str, next_sequence: int, receipt_id: str) -> None:
        """Apply an already agreed core/edge epoch-loss transition, never reset seq."""
        with self._lock:
            if (
                not self.epoch_lost
                or self.pending() is not None
                or not receipt_id
                or not new_epoch
                or new_epoch == self.stream_epoch
                or type(next_sequence) is not int
                or next_sequence > _MAX_SEQUENCE
                or next_sequence < self.next_sequence
            ):
                raise CaptureProtocolError("epoch_transition_not_authorized")
            with self._connection:
                self._connection.execute(
                    "UPDATE stream SET previous_epoch=stream_epoch,stream_epoch=?,next_sequence=?,"
                    "epoch_lost=0,transition_receipt_id=? WHERE singleton=1",
                    (new_epoch, next_sequence, receipt_id),
                )

    def matches_last_ack(self, ack: CaptureAck) -> bool:
        """Stopped operator retry after durable deletion, before the epoch transition."""
        with self._lock:
            value = self._state()["last_ack_json"]
            return value is not None and canonical_json(
                strict_json(value.encode())
            ) == canonical_json(ack)

    def matches_transition(
        self, previous_epoch: str, position: CaptureStreamPosition, receipt_id: str
    ) -> bool:
        """Recognize only the exact transition already applied by an operator retry."""
        with self._lock:
            state = self._state()
            return (
                state["previous_epoch"] == previous_epoch
                and state["stream_epoch"] == position.stream_epoch
                and state["next_sequence"] == position.next_sequence
                and state["transition_receipt_id"] == receipt_id
                and not state["epoch_lost"]
            )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._connection.close()
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            os.close(self._lock_fd)

    def __enter__(self) -> EdgeSpool:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

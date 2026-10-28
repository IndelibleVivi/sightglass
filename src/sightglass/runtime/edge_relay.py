"""Edge-originated fixed SSH stdio relay and private core capture broker."""

from __future__ import annotations

import hashlib
import hmac
import math
import os
import random
import re
import select
import selectors
import signal
import socket
import stat
import struct
import subprocess
import sys
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any, cast

from sightglass.contracts.capture import (
    CAPTURE_FINITE_REASONS,
    CAPTURE_VERSION,
    MAX_CAPTURE_METADATA_BYTES,
    MAX_CAPTURE_RESOURCE_BYTES,
    CaptureAck,
    CaptureExpectation,
    CaptureProtocolError,
    CaptureRequest,
)
from sightglass.operations import check_operation_budget
from sightglass.source.capture.codec import (
    SealedCapture,
    canonical_json,
    json_value,
    strict_json,
    typed_value,
)
from sightglass.source.capture.executor import CaptureExecutor
from sightglass.source.capture.frozen import FrozenCaptureProvider
from sightglass.source.capture.receiver import ack_matches

from .edge import EdgeSpool
from .worker_diagnostics import clear_error, record_error

MAX_RELAY_FRAME_BYTES = MAX_CAPTURE_METADATA_BYTES + MAX_CAPTURE_RESOURCE_BYTES + 64
RELAY_HANDSHAKE_TIMEOUT_SECONDS = 30.0
RELAY_IDLE_TIMEOUT_SECONDS = 60.0
RELAY_ACK_TIMEOUT_SECONDS = 180.0
RELAY_CANCEL_POLL_SECONDS = 0.05
RELAY_TRANSPORT_REVISION = 2
RELAY_CONTROL_TIMEOUT_SECONDS = 1.0
RELAY_PROXY_BUFFER_BYTES = 65_536
RELAY_PROXY_WRITE_TIMEOUT_SECONDS = 30.0
FIXED_EDGE_REMOTE_COMMAND = "sightglassctl edge-session"
_EDGE_FAILURE_REASONS = frozenset(
    {"capture_unsealed", "edge_spool_pressure", "edge_storage_unavailable"}
)
_RELAY_DIAGNOSTIC_REASONS = frozenset(
    {
        "capture_control_disconnected",
        "capture_finalization_pending",
        "capture_publication_pending",
        "edge_disconnected_before_capture",
        "edge_relay_frame_incomplete",
        "edge_relay_read_cancelled",
        "edge_relay_read_closed",
        "edge_relay_read_timeout",
        "edge_relay_write_cancelled",
        "edge_relay_write_closed",
        "edge_relay_write_incomplete",
        "edge_relay_write_timeout",
        "edge_stdio_proxy_disconnected",
        "edge_stdio_proxy_input_incomplete",
        "edge_stdio_proxy_input_timeout",
        "edge_stdio_proxy_output_incomplete",
        "edge_stdio_proxy_output_timeout",
        "relay_disconnected",
    }
)
_CONNECTION_PROTOCOL_REASONS = frozenset(
    {
        "edge_capability_rejected",
        "edge_failure_control_invalid",
        "edge_stream_already_connected",
        "pending_capture_must_replay_before_poll",
        "relay_control_not_object",
        "relay_frame_kind_invalid",
        "relay_frame_size_invalid",
        "relay_stream_epoch_mismatch",
        "request_capture_conflict",
        "unexpected_capture_failure",
        "unexpected_capture_order",
        "unknown_request_requires_authorized_recovery",
        "unsupported_edge_session_frame",
    }
) | CAPTURE_FINITE_REASONS


class RelayDisconnected(ConnectionError):
    pass


class CaptureWaitTimeout(TimeoutError):
    """The waiter detached; the durable edge batch remains pending."""


def edge_token_hash(token: str) -> str:
    return hashlib.sha256(b"sightglass-edge-capability\x00" + token.encode()).hexdigest()


def _stream_fileno(stream: IO[Any]) -> int | None:
    try:
        return stream.fileno()
    except (AttributeError, OSError, ValueError):
        return None

def _write_some(descriptor: int, pending: memoryview) -> int:
    """Write one chunk to a nonblocking descriptor, or report backpressure."""
    try:
        written = os.write(descriptor, pending[:RELAY_PROXY_BUFFER_BYTES])
    except (BlockingIOError, InterruptedError):
        return 0
    if written <= 0:
        raise RelayDisconnected("edge_stdio_proxy_output_incomplete")
    return written


def _stdio_selector_events(
    *,
    input_open: bool,
    input_pending: bool,
    connection_open: bool,
    output_pending: bool,
) -> dict[str, int]:
    """Selector masks for at most one read buffer per proxy direction."""
    events: dict[str, int] = {}
    if input_open and not input_pending:
        events["input"] = selectors.EVENT_READ
    if connection_open:
        connection_events = 0
        if not output_pending:
            connection_events |= selectors.EVENT_READ
        if input_pending:
            connection_events |= selectors.EVENT_WRITE
        if connection_events:
            events["connection"] = connection_events
    if output_pending:
        events["output"] = selectors.EVENT_WRITE
    return events


def _read_exact(
    stream: IO[Any],
    amount: int,
    *,
    stop: threading.Event | None = None,
    cancelled: threading.Event | None = None,
    timeout: float | None = None,
) -> bytes:
    deadline = time.monotonic() + timeout if timeout is not None else None
    chunks = []
    remaining = amount
    while remaining:
        if ((stop is not None and stop.is_set())
                or (cancelled is not None and cancelled.is_set())):
            raise RelayDisconnected("edge_relay_read_cancelled")
        if deadline is not None and time.monotonic() >= deadline:
            raise RelayDisconnected("edge_relay_read_timeout")
        descriptor = _stream_fileno(stream)
        if descriptor is not None:
            wait = RELAY_CANCEL_POLL_SECONDS
            if deadline is not None:
                wait = min(wait, max(0.0, deadline - time.monotonic()))
            try:
                os.set_blocking(descriptor, False)
                readable, _writable, _errors = select.select([descriptor], [], [], wait)
            except InterruptedError:
                continue
            except (OSError, ValueError) as exc:
                raise RelayDisconnected("edge_relay_read_closed") from exc
            if not readable:
                if deadline is not None and time.monotonic() >= deadline:
                    raise RelayDisconnected("edge_relay_read_timeout")
                continue
        try:
            # Bypass BufferedReader prefetch for real descriptors. select() only
            # observes the kernel fd; avoiding read-ahead consistently also keeps
            # the next frame's readiness aligned with that descriptor. Raw short
            # reads let each partial frame recheck stop and its absolute deadline.
            chunk = (
                os.read(descriptor, remaining)
                if descriptor is not None
                else stream.read(remaining)
            )
        except (BlockingIOError, InterruptedError):
            continue
        except (OSError, ValueError) as exc:
            raise RelayDisconnected("edge_relay_read_closed") from exc
        if not chunk:
            raise RelayDisconnected("edge_relay_frame_incomplete")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


class FramedStream:
    def __init__(
        self,
        reader: IO[Any],
        writer: IO[Any],
        *,
        stop: threading.Event | None = None,
        closers: tuple[Any, ...] = (),
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.stop = stop
        self._closers = closers
        self._cancelled = threading.Event()
        self._write_lock = threading.Lock()

    @property
    def cancelled(self) -> bool:
        return self._cancelled.is_set()

    def bind_stop(self, stop: threading.Event) -> None:
        self.stop = stop

    def cancel(self) -> None:
        self._cancelled.set()
        for closer in self._closers:
            try:
                closer.close()
            except (OSError, ValueError):
                pass

    def _check_send(self, deadline: float) -> None:
        if self.cancelled or (self.stop is not None and self.stop.is_set()):
            raise RelayDisconnected("edge_relay_write_cancelled")
        if time.monotonic() >= deadline:
            raise RelayDisconnected("edge_relay_write_timeout")

    def send(
        self, value: dict[str, Any] | SealedCapture,
        *, timeout: float = RELAY_ACK_TIMEOUT_SECONDS,
    ) -> None:
        deadline = time.monotonic() + max(0.0, timeout)
        self._check_send(deadline)
        payload = (
            b"C" + value.to_bytes()
            if isinstance(value, SealedCapture)
            else b"J" + canonical_json(value)
        )
        if len(payload) > MAX_RELAY_FRAME_BYTES:
            raise CaptureProtocolError("relay_frame_too_large")
        while True:
            self._check_send(deadline)
            if self._write_lock.acquire(
                timeout=min(RELAY_CANCEL_POLL_SECONDS, max(0.0, deadline - time.monotonic()))
            ):
                break
        try:
            try:
                descriptor = _stream_fileno(self.writer)
                if descriptor is not None:
                    # All framed fd I/O bypasses Python buffering. Nonblocking
                    # writes remain short even after readiness changes; select
                    # followed by a large blocking write can still hang forever.
                    os.set_blocking(descriptor, False)
                for chunk in (struct.pack("!I", len(payload)), payload):
                    view = memoryview(chunk)
                    while view:
                        self._check_send(deadline)
                        if descriptor is not None:
                            try:
                                _, writable, _ = select.select(
                                    [], [descriptor], [],
                                    min(RELAY_CANCEL_POLL_SECONDS,
                                        max(0.0, deadline - time.monotonic())),
                                )
                                if not writable:
                                    continue
                                written = os.write(descriptor, view[:65_536])
                            except (BlockingIOError, InterruptedError):
                                continue
                        else:
                            written = self.writer.write(view)
                        if written is None or written <= 0:
                            raise RelayDisconnected("edge_relay_write_incomplete")
                        view = view[written:]
                # Raw fd writes are already published; flushing BufferedWriter
                # here would reintroduce an uncancellable blocking operation.
                self._check_send(deadline)
                if descriptor is None:
                    self.writer.flush()
                    self._check_send(deadline)
            except (OSError, ValueError) as exc:
                if isinstance(exc, RelayDisconnected):
                    raise
                raise RelayDisconnected("edge_relay_write_closed") from exc
        finally:
            self._write_lock.release()

    def receive(
        self, *, timeout: float = RELAY_IDLE_TIMEOUT_SECONDS
    ) -> dict[str, Any] | SealedCapture:
        if self.cancelled:
            raise RelayDisconnected("edge_relay_read_cancelled")
        deadline = time.monotonic() + max(0.0, timeout)

        def read(amount: int) -> bytes:
            remaining = max(0.0, deadline - time.monotonic())
            return _read_exact(
                self.reader, amount, stop=self.stop, cancelled=self._cancelled, timeout=remaining
            )

        amount = struct.unpack("!I", read(4))[0]
        if not 2 <= amount <= MAX_RELAY_FRAME_BYTES:
            raise CaptureProtocolError("relay_frame_size_invalid")
        kind = read(1)
        payload = read(amount - 1)
        if kind == b"C":
            return SealedCapture.from_bytes(payload)
        if kind != b"J":
            raise CaptureProtocolError("relay_frame_kind_invalid")
        value = strict_json(payload)
        if not isinstance(value, dict):
            raise CaptureProtocolError("relay_control_not_object")
        return value


class PreparedRemoteCapture:
    """A local delivery ticket; complete performs no I/O and never waits.

    The caller supplies an ACK only after its existing WindowDB outer transaction
    commits. The network thread owns the wait and the eventual wire send.
    """

    def __init__(self, envelope: SealedCapture) -> None:
        self.envelope = envelope
        self.request = envelope.document().request
        self._done = threading.Event()
        self._lock = threading.Lock()
        self._ack: CaptureAck | None = None

    @property
    def provider(self) -> FrozenCaptureProvider:
        return FrozenCaptureProvider(self.envelope)

    @property
    def durable_ack(self) -> CaptureAck | None:
        with self._lock:
            return self._ack

    def complete(self, ack: CaptureAck) -> None:
        if not ack_matches(self.envelope, ack):
            raise CaptureProtocolError("ticket_ack_mismatch")
        with self._lock:
            if self._ack is not None and self._ack != ack:
                raise CaptureProtocolError("ticket_ack_conflict")
            self._ack = ack
            self._done.set()

    def wait_for_ack(self, timeout: float) -> CaptureAck | None:
        if not self._done.wait(timeout):
            return None
        return self.durable_ack


@dataclass
class _BrokerWork:
    request: CaptureRequest
    captured: threading.Event = field(default_factory=threading.Event)
    ticket: PreparedRemoteCapture | None = None
    failure: CaptureProtocolError | RelayDisconnected | None = None
    waiters: int = 0
    detached: bool = False
    recovering: bool = False
    deadline: float = 0.0
    cancel_requested: bool = False


class CaptureBroker:
    """One ordered edge stream; foreground/background callers use local tickets."""

    def __init__(
        self,
        expected: CaptureExpectation,
        *,
        token_hash: str,
        lookup_terminal: Callable[[SealedCapture], CaptureAck | None],
        recover: Callable[[PreparedRemoteCapture], None] | None = None,
        finalization_timeout: float = 30.0,
        max_pending_requests: int = 32,
        core_generation: str = "",
        ownership_guard: Callable[[], None] | None = None,
    ) -> None:
        if not re.fullmatch(r"[0-9a-f]{64}", token_hash):
            raise ValueError("a separate edge capability hash is required")
        self.expected = expected
        self.token_hash = token_hash
        self.lookup_terminal = lookup_terminal
        self.recover = recover
        self.core_generation = core_generation
        self.ownership_guard = ownership_guard
        self.finalization_timeout = finalization_timeout
        if not 1 <= max_pending_requests <= 200:
            raise ValueError("broker pending request bound is invalid")
        self.max_pending_requests = max_pending_requests
        self._condition = threading.Condition()
        self._queue: deque[_BrokerWork] = deque()
        self._by_request: dict[str, _BrokerWork] = {}
        self._inflight: _BrokerWork | None = None
        self._session_lock = threading.Lock()
        self._stop = threading.Event()
        self._closed = False
        self._connected = False
        self._connection_phase = "listening"
        self._diagnostics: dict[str, Any] = {
            "last_error_code": None,
            "last_error_type": None,
            "last_error_location": None,
            "historical_error_count": 0,
            "historical_error_code": None,
            "historical_error_type": None,
            "historical_error_location": None,
        }

    @property
    def connected(self) -> bool:
        with self._condition:
            return self._connected and not self._closed

    def diagnostics(self) -> dict[str, Any]:
        """Content-free queue/phase snapshot; performs no transport or recovery work."""
        with self._condition:
            work = self._inflight
            ticket = work.ticket if work is not None else None
            return {
                "queued": len(self._queue),
                "detached_queued": sum(work.detached for work in self._queue),
                "inflight": {
                    "operation": work.request.operation,
                    "captured": ticket is not None,
                    "detached": work.detached,
                    "recovering": work.recovering,
                    "waiters": work.waiters,
                    "durable_terminal": ticket is not None and ticket.durable_ack is not None,
                } if work is not None else None,
                "connection": {
                    "phase": self._connection_phase,
                    **self._diagnostics,
                },
            }

    def _set_connection_phase(self, phase: str) -> None:
        with self._condition:
            self._connection_phase = phase

    def _record_connection_error(self, exc: Exception) -> None:
        with self._condition:
            if (
                isinstance(exc, CaptureProtocolError)
                and exc.reason in _CONNECTION_PROTOCOL_REASONS
            ):
                code = exc.reason
            elif isinstance(exc, RelayDisconnected):
                reason = str(exc)
                code = reason if reason in _RELAY_DIAGNOSTIC_REASONS else "relay_disconnected"
            elif isinstance(exc, RuntimeError):
                code = "ownership_or_runtime_rejected"
            else:
                code = "connection_failed"
            record_error(self._diagnostics, exc, code)

    def submit(self, request: CaptureRequest, *, timeout: float = 120.0) -> PreparedRemoteCapture:
        from sightglass.contracts.capture import CaptureCeiling

        CaptureCeiling(
            self.expected.account_id, self.expected.conversations, self.expected.egress_revision
        ).authorize(request)
        if request.policy_revision != self.expected.policy_revision:
            raise CaptureProtocolError("request_policy_revision_mismatch")
        deadline = time.monotonic() + max(0.0, timeout)
        with self._condition:
            if self._closed:
                raise RelayDisconnected("capture_broker_closed")
            work = self._by_request.get(request.request_id)
            if work is None:
                if len(self._by_request) >= self.max_pending_requests:
                    raise CaptureProtocolError("capture_broker_queue_full")
                work = _BrokerWork(request, deadline=deadline)
                self._by_request[request.request_id] = work
                self._queue.append(work)
                self._condition.notify_all()
            elif work.request != request:
                raise CaptureProtocolError("request_identity_conflict")
            work.waiters += 1
            work.detached = False
            work.deadline = max(work.deadline, deadline)
        completed = False
        try:
            while True:
                check_operation_budget()
                remaining = max(0.0, deadline - time.monotonic())
                completed = work.captured.wait(min(0.05, remaining))
                if completed or remaining == 0:
                    break
        finally:
            with self._condition:
                work.waiters -= 1
                if not completed and work.waiters == 0:
                    work.detached = True
                    work.cancel_requested = True
                    # Selection and removal share this lock. An unissued request
                    # with no remaining callers must never start source work or
                    # keep a pending slot; an already selected request retains its
                    # exact late-terminal recovery without reissue or early ACK.
                    if (self._inflight is not work and work.ticket is None
                            and self._by_request.get(request.request_id) is work):
                        self._queue.remove(work)
                        del self._by_request[request.request_id]
                        self._condition.notify_all()
        if not completed:
            self._recover_work(work)
            # Do not fabricate a terminal ACK or release an in-flight source read.
            raise CaptureWaitTimeout("capture_waiter_timed_out_pending_work_retained")
        if work.failure is not None:
            raise work.failure
        if work.ticket is None:
            raise RelayDisconnected("capture_broker_closed")
        return work.ticket

    def abandon(self, ticket: PreparedRemoteCapture) -> None:
        """Mark an unfinished local caller for exact replay recovery; performs no I/O."""
        with self._condition:
            work = self._by_request.get(ticket.request.request_id)
            if work is not None and work.ticket is ticket and ticket.durable_ack is None:
                work.detached = True
                self._condition.notify_all()

    def _recover_work(self, work: _BrokerWork) -> None:
        with self._condition:
            ticket = work.ticket
            recover = self.recover
            if (
                not work.detached
                or work.recovering
                or ticket is None
                or ticket.durable_ack is not None
                or recover is None
            ):
                return
            work.recovering = True
        try:
            if self.ownership_guard is not None:
                self.ownership_guard()
            # The callback must resolve the exact durable request binding and may
            # complete only after its outer commit. It runs outside the broker lock.
            recover(ticket)
        finally:
            with self._condition:
                work.recovering = False
                self._condition.notify_all()

    def _next_work(self) -> _BrokerWork | None:
        with self._condition:
            if self._inflight is not None:
                if self._inflight.ticket is None or self._inflight.ticket.durable_ack is None:
                    return self._inflight
                self._inflight = None
            if not self._queue and not self._closed:
                self._condition.wait(1.0)
            if self._closed:
                return None
            if self._queue:
                self._inflight = self._queue.popleft()
            return self._inflight

    def _receive_capture(self, envelope: SealedCapture) -> PreparedRemoteCapture | CaptureAck:
        document = envelope.document()
        terminal = self.lookup_terminal(envelope)
        if terminal is not None:
            if not ack_matches(envelope, terminal):
                raise CaptureProtocolError("batch_identity_conflict")
            return terminal
        # Unknown captures can only enter through CoreCapture's exact durable request
        # recovery. Checking current policy here would strand valid old pending work
        # after a policy tightening before its terminal rejection can commit.
        if (
            document.request.account_id != self.expected.account_id
            or document.origin.source_instance_id != self.expected.source_instance_id
            or document.origin.origin_epoch != self.expected.origin_epoch
            or document.origin.egress_revision != self.expected.egress_revision
        ):
            raise CaptureProtocolError("capture_expectation_mismatch")
        with self._condition:
            work = self._by_request.get(document.request.request_id)
            if work is not None:
                if work.request != document.request:
                    raise CaptureProtocolError("capture_request_mismatch")
                if work.ticket is not None:
                    if work.ticket.envelope.digest != envelope.digest:
                        raise CaptureProtocolError("request_capture_conflict")
                elif self._inflight is not work:
                    raise CaptureProtocolError("unexpected_capture_order")
            else:
                if self.recover is None:
                    raise CaptureProtocolError("unknown_request_requires_authorized_recovery")
                if len(self._by_request) >= self.max_pending_requests:
                    raise CaptureProtocolError("capture_broker_queue_full")
                work = _BrokerWork(document.request, detached=True)
                self._by_request[document.request.request_id] = work
            if work.ticket is None:
                work.ticket = PreparedRemoteCapture(envelope)
            ticket = work.ticket
            work.captured.set()
            self._condition.notify_all()
            self._inflight = work
        self._recover_work(work)
        return ticket

    def _fail_uncaptured(self) -> None:
        """Detach disconnected callers without reissuing a possibly running read.

        A durable batch arriving later is recovered by its exact core request
        journal binding. Already returned tickets retain their immutable evidence.
        """
        with self._condition:
            for request_id, work in tuple(self._by_request.items()):
                if work.ticket is not None:
                    continue
                work.failure = RelayDisconnected("edge_disconnected_before_capture")
                work.detached = True
                work.captured.set()
                self._by_request.pop(request_id)
                if self._inflight is work:
                    self._inflight = None
            self._queue = deque(work for work in self._queue if work.failure is None)
            self._condition.notify_all()

    def _receive_failure(self, frame: dict[str, Any]) -> None:
        if (
            set(frame) != {"schema", "kind", "request_id", "reason"}
            or frame.get("schema") != CAPTURE_VERSION
            or frame.get("kind") != "failed"
            or not isinstance(frame["request_id"], str)
            or not isinstance(frame["reason"], str)
            or frame["reason"] not in _EDGE_FAILURE_REASONS
        ):
            raise CaptureProtocolError("edge_failure_control_invalid")
        with self._condition:
            work = self._inflight
            if (
                work is None
                or work.ticket is not None
                or work.request.request_id != frame["request_id"]
            ):
                raise CaptureProtocolError("unexpected_capture_failure")
            work.failure = CaptureProtocolError(frame["reason"])
            work.detached = True
            work.captured.set()
            self._by_request.pop(work.request.request_id)
            self._inflight = None
            self._condition.notify_all()

    def _wait_for_finalization(self, ticket: PreparedRemoteCapture) -> CaptureAck | None:
        deadline = time.monotonic() + self.finalization_timeout
        while True:
            if self._stop.is_set():
                return None
            ack = ticket.durable_ack
            if ack is not None:
                return ack
            with self._condition:
                work = self._by_request.get(ticket.request.request_id)
                abandoned = work is not None and work.ticket is ticket and work.detached
            if abandoned:
                assert work is not None
                # This transport owner is outside the cancelled caller's budget.
                # One exact recovery attempt may commit a terminal; an unfinished
                # attempt still retries only through the existing pending replay.
                self._recover_work(work)
                return ticket.durable_ack
            remaining = max(0.0, deadline - time.monotonic())
            if remaining == 0:
                return None
            ack = ticket.wait_for_ack(min(0.05, remaining))
            if ack is not None:
                return ack

    def serve_connection(self, channel: FramedStream) -> None:
        channel.bind_stop(self._stop)
        if not self._session_lock.acquire(blocking=False):
            raise CaptureProtocolError("edge_stream_already_connected")
        authenticated = False
        self._set_connection_phase("handshake")
        try:
            hello = channel.receive(timeout=RELAY_HANDSHAKE_TIMEOUT_SECONDS)
            hello_keys = {
                "schema",
                "kind",
                "role",
                "token",
                "source_instance_id",
                "account_id",
                "origin_epoch",
                "stream_epoch",
                "next_sequence",
                "core_generation",
            }
            transport_v2 = (
                isinstance(hello, dict)
                and type(hello.get("transport_revision")) is int
                and hello.get("transport_revision") == RELAY_TRANSPORT_REVISION
            )
            if transport_v2:
                hello_keys.add("transport_revision")
            if (
                not isinstance(hello, dict)
                or set(hello) != hello_keys
                or (
                    hello["schema"] != CAPTURE_VERSION
                    or hello["kind"] != "hello"
                    or hello["role"] != "edge"
                    or not isinstance(hello["token"], str)
                    or not hmac.compare_digest(edge_token_hash(hello["token"]), self.token_hash)
                    or hello["source_instance_id"] != self.expected.source_instance_id
                    or hello["account_id"] != self.expected.account_id
                    or hello["origin_epoch"] != self.expected.origin_epoch
                    or hello["core_generation"] != self.core_generation
                    or not isinstance(hello["stream_epoch"], str)
                    or not hello["stream_epoch"]
                    or type(hello["next_sequence"]) is not int
                    or hello["next_sequence"] < 1
                )
            ):
                raise CaptureProtocolError("edge_capability_rejected")
            authenticated = True
            if self.ownership_guard is not None:
                self.ownership_guard()
            channel.send(
                {
                    "schema": CAPTURE_VERSION,
                    "kind": "ready",
                    "core_generation": self.core_generation,
                    **({"transport_revision": RELAY_TRANSPORT_REVISION} if transport_v2 else {}),
                }
            )
            with self._condition:
                self._connected = True
                self._connection_phase = "connected"
                clear_error(self._diagnostics)
                self._condition.notify_all()
            while not self._closed:
                # The session loop itself is bounded by the edge acknowledgment
                # deadline; partial frames cannot pin a closed broker indefinitely.
                frame = channel.receive(timeout=RELAY_ACK_TIMEOUT_SECONDS)
                if self.ownership_guard is not None:
                    self.ownership_guard()
                if isinstance(frame, SealedCapture):
                    document = frame.document()
                    if document.origin.stream_epoch != hello["stream_epoch"]:
                        raise CaptureProtocolError("relay_stream_epoch_mismatch")
                    received = self._receive_capture(frame)
                    ack = (
                        received
                        if isinstance(received, CaptureAck)
                        else self._wait_for_finalization(received)
                    )
                    if ack is None:
                        assert isinstance(received, PreparedRemoteCapture)
                        self.abandon(received)
                        channel.send({"schema": CAPTURE_VERSION, "kind": "retry"})
                        return
                    if self.ownership_guard is not None:
                        self.ownership_guard()
                    channel.send({"schema": CAPTURE_VERSION, "kind": "ack", "ack": json_value(ack)})
                    with self._condition:
                        completed_work = self._by_request.pop(ack.request_id, None)
                        if self._inflight is completed_work:
                            self._inflight = None
                elif transport_v2 and isinstance(frame, dict) and frame.get("kind") == "working":
                    with self._condition:
                        work = self._inflight
                        if (set(frame) != {"schema", "kind", "request_id", "core_generation"}
                                or frame["schema"] != CAPTURE_VERSION
                                or frame["core_generation"] != self.core_generation
                                or work is None or work.ticket is not None
                                or frame["request_id"] != work.request.request_id):
                            raise CaptureProtocolError("unsupported_edge_session_frame")
                        cancel = work.cancel_requested or time.monotonic() >= work.deadline
                    channel.send({
                        "schema": CAPTURE_VERSION,
                        "kind": "cancel" if cancel else "continue",
                        "request_id": work.request.request_id,
                        "core_generation": self.core_generation,
                    }, timeout=RELAY_CONTROL_TIMEOUT_SECONDS)
                elif isinstance(frame, dict) and frame.get("kind") == "failed":
                    self._receive_failure(frame)
                elif frame == {"schema": CAPTURE_VERSION, "kind": "poll"}:
                    work = self._next_work()
                    if work is None:
                        channel.send(
                            {"schema": CAPTURE_VERSION, "kind": "idle", "retry_seconds": 1}
                        )
                    elif work.ticket is not None:
                        # Edge is responsible for replaying its pending batch first.
                        raise CaptureProtocolError("pending_capture_must_replay_before_poll")
                    else:
                        channel.send(
                            {
                                "schema": CAPTURE_VERSION,
                                "kind": "work",
                                "request": json_value(work.request),
                                **({"remaining_seconds": max(0.0, min(120.0,
                                    work.deadline - time.monotonic()))} if transport_v2 else {}),
                            }
                        )
                else:
                    raise CaptureProtocolError("unsupported_edge_session_frame")
        except Exception as exc:
            self._record_connection_error(exc)
            raise
        finally:
            if authenticated:
                self._fail_uncaptured()
            with self._condition:
                self._connected = False
                if self._closed:
                    self._connection_phase = "closed"
                elif self._diagnostics["last_error_code"] is not None:
                    self._connection_phase = "failed"
                else:
                    self._connection_phase = "disconnected"
                self._condition.notify_all()
            self._session_lock.release()

    def close(self) -> None:
        self._stop.set()
        with self._condition:
            self._closed = True
            self._connection_phase = "closed"
            for work in self._by_request.values():
                work.captured.set()
            self._condition.notify_all()


class CaptureBrokerServer:
    """Owner-only Unix listener. It serves edge capabilities, never MCP calls."""

    def __init__(self, broker: CaptureBroker, socket_path: Path) -> None:
        self.broker, self.socket_path = broker, socket_path
        self._socket: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._connections: set[socket.socket] = set()
        self._channels: set[FramedStream] = set()
        self._connection_threads: set[threading.Thread] = set()
        self._connections_lock = threading.Lock()
        self._socket_identity: tuple[int, int, int, int] | None = None

    @property
    def running(self) -> bool:
        thread = self._thread
        return self._socket is not None and thread is not None and thread.is_alive()

    def _socket_metadata(self) -> tuple[int, int, int, int]:
        metadata = self.socket_path.lstat()
        return (
            int(metadata.st_dev),
            int(metadata.st_ino),
            int(metadata.st_uid),
            int(stat.S_IMODE(metadata.st_mode)),
        )

    def _probe_socket(self) -> None:
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        probe.settimeout(0.25)
        try:
            probe.connect(str(self.socket_path))
        finally:
            probe.close()

    def _prepare_socket_path(self) -> None:
        path = self.socket_path
        if not (path.exists() or path.is_symlink()):
            return
        metadata = path.lstat()
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISSOCK(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise CaptureProtocolError("broker_socket_path_unsafe")
        before = self._socket_metadata()
        try:
            self._probe_socket()
        except (ConnectionRefusedError, FileNotFoundError):
            # A dead listener leaves an inode at this exact path. Re-check all stable
            # identity fields before removing it so a replacement cannot be stolen.
            if self._socket_metadata() != before:
                raise CaptureProtocolError("broker_socket_changed_during_probe")
            path.unlink()
            return
        except OSError as exc:
            raise CaptureProtocolError("broker_socket_probe_failed") from exc
        raise CaptureProtocolError("broker_socket_already_exists")

    def start(self) -> None:
        parent = self.socket_path.parent
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        metadata = parent.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise CaptureProtocolError("broker_directory_not_private")
        self._prepare_socket_path()
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(self.socket_path))
            self._socket_identity = self._socket_metadata()
            os.chmod(self.socket_path, 0o600)
            self._socket_identity = self._socket_metadata()
            listener.listen(2)
            listener.settimeout(0.2)
            self._socket = listener
            self._socket_identity = self._socket_metadata()
            self._thread = threading.Thread(
                target=self._serve, name="sightglass-capture-broker", daemon=True
            )
            self._thread.start()
        except BaseException:
            self._stop.set()
            self._socket = None
            self._thread = None
            listener.close()
            try:
                if (self._socket_identity is not None
                        and self._socket_metadata() == self._socket_identity):
                    self.socket_path.unlink()
            except OSError:
                pass
            self._socket_identity = None
            raise

    def _serve(self) -> None:
        listener = self._socket
        assert listener is not None
        while not self._stop.is_set():
            try:
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            with self._connections_lock:
                self._connections.add(connection)
                thread = threading.Thread(
                    target=self._connection, args=(connection,),
                    name="sightglass-capture-connection", daemon=True)
                self._connection_threads.add(thread)
            try:
                thread.start()
            except BaseException:
                with self._connections_lock:
                    self._connections.discard(connection)
                    self._connection_threads.discard(thread)
                connection.close()
                raise

    def _connection(self, connection: socket.socket) -> None:
        from .ipc import peer_effective_ids

        try:
            uid, _gid = peer_effective_ids(connection)
            if uid != os.geteuid():
                return
            # A healthy local capture may use the executor's 150-second ceiling.
            # Foreground waiters have their own shorter deadline; do not force a
            # connection failure while the edge is closing that bounded session.
            connection.settimeout(RELAY_ACK_TIMEOUT_SECONDS)
            with (
                connection.makefile("rb", buffering=0) as reader,
                connection.makefile("wb", buffering=0) as writer,
            ):
                channel = FramedStream(
                    cast(IO[Any], reader), cast(IO[Any], writer), closers=(connection,))
                with self._connections_lock:
                    self._channels.add(channel)
                try:
                    self.broker.serve_connection(channel)
                finally:
                    with self._connections_lock:
                        self._channels.discard(channel)
        except (RuntimeError, RelayDisconnected, OSError):
            pass  # Content-free fail closed. Pending sender bytes stay durable.
        finally:
            with self._connections_lock:
                self._connections.discard(connection)
                self._connection_threads.discard(threading.current_thread())
            connection.close()

    def close(self) -> None:
        self._stop.set()
        self.broker.close()
        if self._socket is not None:
            self._socket.close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            if self._thread.is_alive():
                raise RuntimeError("capture listener did not stop")
        with self._connections_lock:
            connections = tuple(self._connections)
            channels = tuple(self._channels)
            threads = tuple(self._connection_threads)
        for channel in channels:
            channel.cancel()
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        deadline = time.monotonic() + 3.0
        for thread in threads:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        if any(thread.is_alive() for thread in threads):
            raise RuntimeError("capture connections did not stop")
        if self._socket_identity is not None and self.socket_path.exists():
            try:
                current = self._socket_metadata()
            except OSError:
                current = None
            if current is not None and current == self._socket_identity:
                self.socket_path.unlink()
        self._socket = None
        self._thread = None
        self._socket_identity = None


class EdgeSession:
    def __init__(
        self,
        executor: CaptureExecutor,
        spool: EdgeSpool,
        *,
        token: str,
        core_generation: str = "",
        ownership_guard: Callable[[], None] | None = None,
    ) -> None:
        if not token:
            raise ValueError("a separate edge capability is required")
        self.executor, self.spool, self.token = executor, spool, token
        self.core_generation, self.ownership_guard = core_generation, ownership_guard

    def _require_owned(self) -> None:
        if self.ownership_guard is not None:
            self.ownership_guard()

    def run(self, channel: FramedStream, *, stop: threading.Event | None = None) -> None:
        stop = stop or threading.Event()
        channel.bind_stop(stop)
        if self.spool.epoch_lost:
            raise CaptureProtocolError("epoch_loss_requires_explicit_recovery")
        self._require_owned()
        channel.send(
            {
                "schema": CAPTURE_VERSION,
                "kind": "hello",
                "role": "edge",
                "token": self.token,
                "source_instance_id": self.executor.source_instance_id,
                "account_id": self.executor.ceiling.account_id,
                "origin_epoch": self.executor.origin_epoch,
                "stream_epoch": self.spool.stream_epoch,
                "next_sequence": self.spool.next_sequence,
                "transport_revision": RELAY_TRANSPORT_REVISION,
                "core_generation": self.core_generation,
            }
        )
        if channel.receive(timeout=RELAY_HANDSHAKE_TIMEOUT_SECONDS) != {
            "schema": CAPTURE_VERSION,
            "kind": "ready",
            "core_generation": self.core_generation,
            "transport_revision": RELAY_TRANSPORT_REVISION,
        }:
            raise CaptureProtocolError("edge_session_handshake_rejected")
        while not stop.is_set():
            self._require_owned()
            pending = self.spool.pending()
            if pending is not None:
                channel.send(pending)
                response = channel.receive(timeout=RELAY_ACK_TIMEOUT_SECONDS)
                if response == {"schema": CAPTURE_VERSION, "kind": "retry"}:
                    raise RelayDisconnected("capture_finalization_pending")
                if (
                    not isinstance(response, dict)
                    or set(response) != {"schema", "kind", "ack"}
                    or (response["schema"] != CAPTURE_VERSION or response["kind"] != "ack")
                ):
                    raise CaptureProtocolError("edge_terminal_ack_required")
                self._require_owned()
                self.spool.acknowledge(typed_value(CaptureAck, response["ack"]))
                continue
            channel.send({"schema": CAPTURE_VERSION, "kind": "poll"})
            response = channel.receive()
            if not isinstance(response, dict) or response.get("schema") != CAPTURE_VERSION:
                raise CaptureProtocolError("edge_work_envelope_invalid")
            if set(response) == {"schema", "kind", "retry_seconds"} and response["kind"] == "idle":
                delay = response["retry_seconds"]
                if type(delay) is not int or not 0 <= delay <= 5:
                    raise CaptureProtocolError("edge_idle_delay_invalid")
                stop.wait(delay)
            elif (set(response) == {"schema", "kind", "request", "remaining_seconds"}
                  and response["kind"] == "work"):
                request: CaptureRequest = typed_value(CaptureRequest, response["request"])
                try:
                    self._capture_controlled(
                        channel, request, response["remaining_seconds"], stop
                    )
                except (CaptureProtocolError, OSError) as exc:
                    # Do not manufacture a sealed capture or consume a sequence.
                    # If publication committed, disconnect so its exact bytes
                    # replay first; no unsealed failure may replace that batch.
                    if self.spool.pending() is not None:
                        raise RelayDisconnected("capture_publication_pending") from exc
                    reason = (
                        "edge_storage_unavailable"
                        if isinstance(exc, OSError)
                        else "edge_spool_pressure"
                        if exc.reason == "edge_spool_pressure"
                        else "capture_unsealed"
                    )
                    channel.send(
                        {
                            "schema": CAPTURE_VERSION,
                            "kind": "failed",
                            "request_id": request.request_id,
                            "reason": reason,
                        }
                    )
            else:
                raise CaptureProtocolError("unsupported_edge_work_frame")

    def _capture_controlled(
        self, channel: FramedStream, request: CaptureRequest,
        remaining: Any, stop: threading.Event,
    ) -> SealedCapture:
        # Relative seconds are interpreted on this host only. The source keeps its
        # own smaller configured cap; transport/native interpretation are independent.
        if (type(remaining) not in (int, float) or not math.isfinite(remaining)
                or not 0 <= remaining <= 120):
            raise CaptureProtocolError("edge_work_envelope_invalid")
        cancelled, done = threading.Event(), threading.Event()
        failures: list[BaseException] = []

        def control() -> None:
            try:
                while not done.is_set():
                    if stop.is_set():
                        cancelled.set()
                        return
                    channel.send({
                        "schema": CAPTURE_VERSION, "kind": "working",
                        "request_id": request.request_id,
                        "core_generation": self.core_generation,
                    }, timeout=RELAY_CONTROL_TIMEOUT_SECONDS)
                    reply = channel.receive(timeout=RELAY_CONTROL_TIMEOUT_SECONDS)
                    if (not isinstance(reply, dict)
                            or set(reply) != {"schema", "kind", "request_id", "core_generation"}
                            or reply["schema"] != CAPTURE_VERSION
                            or reply["request_id"] != request.request_id
                            or reply["core_generation"] != self.core_generation
                            or reply["kind"] not in {"continue", "cancel"}):
                        raise CaptureProtocolError("unsupported_edge_work_frame")
                    if reply["kind"] == "cancel":
                        cancelled.set()
                    done.wait(RELAY_CANCEL_POLL_SECONDS)
            except BaseException as exc:
                failures.append(exc)
                cancelled.set()

        owner = threading.Thread(target=control, name="capture-control", daemon=True)
        owner.start()
        try:
            # The normal executor seals cancellation, then the caller durably
            # spools it. Disconnect must never discard an already selected read.
            envelope = self.executor.capture(
                request, stream_epoch=self.spool.stream_epoch,
                sequence=self.spool.next_sequence, cancelled=cancelled,
                timeout_seconds=float(remaining),
            )
        finally:
            done.set()
            owner.join(timeout=2 * RELAY_CONTROL_TIMEOUT_SECONDS + .1)
        self._require_owned()
        self.spool.store(envelope)
        if owner.is_alive() or failures:
            raise RelayDisconnected("capture_control_disconnected")
        return envelope

    def reconnect(
        self,
        connect: Callable[[], Any],
        *,
        stop: threading.Event,
        initial_backoff: float = 1.0,
        max_backoff: float = 30.0,
    ) -> None:
        if not 0 < initial_backoff <= max_backoff <= 60:
            raise ValueError("edge reconnect backoff must be bounded by 60 seconds")
        backoff = initial_backoff
        while not stop.is_set():
            try:
                with connect() as channel:
                    self.run(channel, stop=stop)
                return
            except (RelayDisconnected, OSError):
                if stop.wait(random.uniform(0.8 * backoff, backoff)):
                    return
                backoff = min(max_backoff, max(initial_backoff, backoff * 2))


@dataclass(frozen=True)
class SSHRelayConnector:
    """No request can choose a host, executable, command, SQL, path or shell."""

    host_alias: str
    identity_file: Path
    ssh_binary: str = "ssh"

    def argv(self) -> list[str]:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,255}", self.host_alias):
            raise CaptureProtocolError("invalid_ssh_host_alias")
        return [
            self.ssh_binary,
            "-T",
            "-o",
            "BatchMode=yes",
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            "ClearAllForwardings=yes",
            "-o",
            "PermitLocalCommand=no",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=3",
            "-i",
            str(self.identity_file),
            "--",
            self.host_alias,
            FIXED_EDGE_REMOTE_COMMAND,
        ]

    @contextmanager
    def connect(self) -> Iterator[FramedStream]:
        process = subprocess.Popen(
            self.argv(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        assert process.stdin is not None and process.stdout is not None
        channel = FramedStream(
            process.stdout,
            process.stdin,
            closers=(process.stdin, process.stdout),
        )
        try:
            yield channel
        finally:
            channel.cancel()
            for stream in (process.stdin, process.stdout):
                try:
                    stream.close()
                except (OSError, ValueError):
                    pass
            # The SSH leader may have exited while a descendant still owns pipes
            # or transport state. The entire dedicated process group is ours.
            self._signal_process_tree(process, signal_number=signal.SIGTERM)
            try:
                process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                self._signal_process_tree(process, signal_number=signal.SIGKILL)
                process.wait()
            finally:
                self._signal_process_tree(process, signal_number=signal.SIGKILL)

    @staticmethod
    def _signal_process_tree(
        process: subprocess.Popen[bytes], *, signal_number: int
    ) -> None:
        try:
            os.killpg(process.pid, signal_number)
        except (ProcessLookupError, PermissionError):
            if signal_number == signal.SIGKILL:
                process.kill()


def run_core_stdio(
    socket_path: Path,
    *,
    reader: IO[Any] | None = None,
    writer: IO[Any] | None = None,
) -> None:
    """Fixed remote edge-session command: stdio <-> private core Unix socket.

    The command uses one selector-driven loop so a core socket close is not
    dependent on a reader blocked in stdin. The proxy is byte transparent and
    bounds each write wait by the existing relay deadline.
    """
    input_stream = reader or sys.stdin.buffer
    output_stream = writer or sys.stdout.buffer
    metadata = socket_path.lstat()
    if (
        not stat.S_ISSOCK(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise CaptureProtocolError("broker_socket_not_private")
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        connection.settimeout(RELAY_PROXY_WRITE_TIMEOUT_SECONDS)
        connection.connect(str(socket_path))
        _relay_stdio_descriptors(input_stream, output_stream, connection)
    finally:
        try:
            connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        connection.close()


def _relay_stdio_descriptors(
    input_stream: IO[Any], output_stream: IO[Any], connection: socket.socket
) -> None:
    """Relay descriptor streams through one deadline-aware selector loop."""
    input_fd = _stream_fileno(input_stream)
    output_fd = _stream_fileno(output_stream)
    if input_fd is None or output_fd is None:
        # The fixed SSH command always supplies pipes. Arbitrary Python streams
        # cannot be multiplexed with the socket without a blocked reader thread.
        raise CaptureProtocolError("edge_stdio_requires_file_descriptors")
    input_blocking = os.get_blocking(input_fd)
    output_blocking = os.get_blocking(output_fd)
    try:
        os.set_blocking(input_fd, False)
        os.set_blocking(output_fd, False)
        connection.setblocking(False)
        connection_fd = connection.fileno()
        with selectors.DefaultSelector() as selector:
            pending_input = bytearray()
            pending_output = bytearray()
            input_eof = False
            connection_eof = False
            input_deadline: float | None = None
            output_deadline: float | None = None
            while not connection_eof or pending_output:
                now = time.monotonic()
                if pending_input:
                    if input_deadline is None:
                        input_deadline = now + RELAY_PROXY_WRITE_TIMEOUT_SECONDS
                    if now >= input_deadline:
                        raise RelayDisconnected("edge_stdio_proxy_input_timeout")
                else:
                    input_deadline = None
                if pending_output:
                    if output_deadline is None:
                        output_deadline = now + RELAY_PROXY_WRITE_TIMEOUT_SECONDS
                    if now >= output_deadline:
                        raise RelayDisconnected("edge_stdio_proxy_output_timeout")
                else:
                    output_deadline = None

                masks = _stdio_selector_events(
                    input_open=not input_eof,
                    input_pending=bool(pending_input),
                    connection_open=not connection_eof,
                    output_pending=bool(pending_output),
                )
                descriptors = {
                    "input": input_fd,
                    "connection": connection_fd,
                    "output": output_fd,
                }
                desired = {
                    descriptors[label]: (events, label)
                    for label, events in masks.items()
                }

                registered = set(selector.get_map())
                for descriptor in registered - set(desired):
                    selector.unregister(descriptor)
                for descriptor, (events, label) in desired.items():
                    if descriptor in registered:
                        if selector.get_map()[descriptor].events != events:
                            selector.modify(descriptor, events, label)
                    else:
                        selector.register(descriptor, events, label)

                timeout = RELAY_CANCEL_POLL_SECONDS
                for deadline in (input_deadline, output_deadline):
                    if deadline is not None:
                        timeout = min(timeout, max(0.0, deadline - now))
                for key, mask in selector.select(timeout):
                    if key.data == "input":
                        try:
                            chunk = os.read(input_fd, RELAY_PROXY_BUFFER_BYTES)
                        except BlockingIOError:
                            continue
                        if chunk:
                            pending_input.extend(chunk)
                        else:
                            input_eof = True
                            if not pending_input:
                                try:
                                    connection.shutdown(socket.SHUT_WR)
                                except OSError:
                                    pass
                    elif key.data == "output":
                        written = _write_some(output_fd, memoryview(pending_output))
                        del pending_output[:written]
                    else:
                        if mask & selectors.EVENT_WRITE and pending_input:
                            try:
                                sent = connection.send(pending_input)
                            except (BlockingIOError, InterruptedError):
                                sent = 0
                            if sent > 0:
                                del pending_input[:sent]
                            if input_eof and not pending_input:
                                try:
                                    connection.shutdown(socket.SHUT_WR)
                                except OSError:
                                    pass
                        if mask & selectors.EVENT_READ:
                            try:
                                chunk = connection.recv(RELAY_PROXY_BUFFER_BYTES)
                            except (BlockingIOError, InterruptedError):
                                # Readable readiness can race with another consumer
                                # or an interrupt. Neither is a zero-length EOF.
                                continue
                            if chunk:
                                pending_output.extend(chunk)
                            else:
                                connection_eof = True
                                if not pending_output:
                                    break
    except (OSError, ValueError) as exc:
        if isinstance(exc, RelayDisconnected):
            raise
        raise RelayDisconnected("edge_stdio_proxy_disconnected") from exc
    finally:
        os.set_blocking(input_fd, input_blocking)
        os.set_blocking(output_fd, output_blocking)

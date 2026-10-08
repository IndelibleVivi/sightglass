"""Edge-originated fixed SSH stdio relay and private core capture broker."""

from __future__ import annotations

import hashlib
import hmac
import os
import random
import re
import socket
import stat
import struct
import subprocess
import sys
import threading
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

from sightglass.contracts.capture import (
    CAPTURE_VERSION,
    MAX_CAPTURE_METADATA_BYTES,
    MAX_CAPTURE_RESOURCE_BYTES,
    CaptureAck,
    CaptureExpectation,
    CaptureProtocolError,
    CaptureRequest,
)
from sightglass.source.capture.codec import (
    SealedCapture,
    canonical_json,
    json_value,
    strict_json,
    typed_value,
    validate_capture,
)
from sightglass.source.capture.executor import CaptureExecutor
from sightglass.source.capture.frozen import FrozenCaptureProvider
from sightglass.source.capture.receiver import ack_matches

from .edge import EdgeSpool

MAX_RELAY_FRAME_BYTES = MAX_CAPTURE_METADATA_BYTES + MAX_CAPTURE_RESOURCE_BYTES + 64
FIXED_EDGE_REMOTE_COMMAND = "sightglassctl edge-session"
_EDGE_FAILURE_REASONS = frozenset(
    {"capture_unsealed", "edge_spool_pressure", "edge_storage_unavailable"}
)


class RelayDisconnected(ConnectionError):
    pass


class CaptureWaitTimeout(TimeoutError):
    """The waiter detached; the durable edge batch remains pending."""


def edge_token_hash(token: str) -> str:
    return hashlib.sha256(b"sightglass-edge-capability\x00" + token.encode()).hexdigest()


def _read_exact(stream: IO[Any], amount: int) -> bytes:
    chunks = []
    remaining = amount
    while remaining:
        try:
            chunk = stream.read(remaining)
        except (OSError, ValueError) as exc:
            raise RelayDisconnected("edge_relay_read_closed") from exc
        if not chunk:
            raise RelayDisconnected("edge_relay_frame_incomplete")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


class FramedStream:
    def __init__(self, reader: IO[Any], writer: IO[Any]) -> None:
        self.reader = reader
        self.writer = writer
        self._write_lock = threading.Lock()

    def send(self, value: dict[str, Any] | SealedCapture) -> None:
        payload = (
            b"C" + value.to_bytes()
            if isinstance(value, SealedCapture)
            else b"J" + canonical_json(value)
        )
        if len(payload) > MAX_RELAY_FRAME_BYTES:
            raise CaptureProtocolError("relay_frame_too_large")
        with self._write_lock:
            try:
                for chunk in (struct.pack("!I", len(payload)), payload):
                    view = memoryview(chunk)
                    while view:
                        written = self.writer.write(view)
                        if written is None or written <= 0:
                            raise RelayDisconnected("edge_relay_write_incomplete")
                        view = view[written:]
                self.writer.flush()
            except (OSError, ValueError) as exc:
                raise RelayDisconnected("edge_relay_write_closed") from exc

    def receive(self) -> dict[str, Any] | SealedCapture:
        amount = struct.unpack("!I", _read_exact(self.reader, 4))[0]
        if not 2 <= amount <= MAX_RELAY_FRAME_BYTES:
            raise CaptureProtocolError("relay_frame_size_invalid")
        kind = _read_exact(self.reader, 1)
        payload = _read_exact(self.reader, amount - 1)
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
        self._closed = False
        self._connected = False

    @property
    def connected(self) -> bool:
        with self._condition:
            return self._connected and not self._closed

    def submit(self, request: CaptureRequest, *, timeout: float = 120.0) -> PreparedRemoteCapture:
        from sightglass.contracts.capture import CaptureCeiling

        CaptureCeiling(
            self.expected.account_id, self.expected.conversations, self.expected.egress_revision
        ).authorize(request)
        if request.policy_revision != self.expected.policy_revision:
            raise CaptureProtocolError("request_policy_revision_mismatch")
        with self._condition:
            if self._closed:
                raise RelayDisconnected("capture_broker_closed")
            work = self._by_request.get(request.request_id)
            if work is None:
                if len(self._by_request) >= self.max_pending_requests:
                    raise CaptureProtocolError("capture_broker_queue_full")
                work = _BrokerWork(request)
                self._by_request[request.request_id] = work
                self._queue.append(work)
                self._condition.notify_all()
            elif work.request != request:
                raise CaptureProtocolError("request_identity_conflict")
            work.waiters += 1
            work.detached = False
        completed = work.captured.wait(timeout)
        with self._condition:
            work.waiters -= 1
            if not completed and work.waiters == 0:
                work.detached = True
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
        validate_capture(envelope, self.expected, require_fresh=False)
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

    def serve_connection(self, channel: FramedStream) -> None:
        if not self._session_lock.acquire(blocking=False):
            raise CaptureProtocolError("edge_stream_already_connected")
        authenticated = False
        try:
            hello = channel.receive()
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
                }
            )
            with self._condition:
                self._connected = True
                self._condition.notify_all()
            while not self._closed:
                frame = channel.receive()
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
                        else received.wait_for_ack(self.finalization_timeout)
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
                            }
                        )
                else:
                    raise CaptureProtocolError("unsupported_edge_session_frame")
        finally:
            if authenticated:
                self._fail_uncaptured()
            with self._condition:
                self._connected = False
                self._condition.notify_all()
            self._session_lock.release()

    def close(self) -> None:
        with self._condition:
            self._closed = True
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
        self._connections_lock = threading.Lock()
        self._socket_identity: int | None = None

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
        if self.socket_path.exists() or self.socket_path.is_symlink():
            raise CaptureProtocolError("broker_socket_already_exists")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(self.socket_path))
        os.chmod(self.socket_path, 0o600)
        listener.listen(2)
        listener.settimeout(0.2)
        self._socket = listener
        self._socket_identity = self.socket_path.lstat().st_ino
        self._thread = threading.Thread(
            target=self._serve, name="sightglass-capture-broker", daemon=True
        )
        self._thread.start()

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
            threading.Thread(target=self._connection, args=(connection,), daemon=True).start()

    def _connection(self, connection: socket.socket) -> None:
        from .ipc import peer_effective_ids

        try:
            uid, _gid = peer_effective_ids(connection)
            if uid != os.geteuid():
                return
            # A healthy local capture may use the executor's 150-second ceiling.
            # Foreground waiters have their own shorter deadline; do not force a
            # connection failure while the edge is closing that bounded session.
            connection.settimeout(180.0)
            with connection.makefile("rb") as reader, connection.makefile("wb") as writer:
                self.broker.serve_connection(FramedStream(reader, writer))
        except (RuntimeError, RelayDisconnected, OSError):
            pass  # Content-free fail closed. Pending sender bytes stay durable.
        finally:
            with self._connections_lock:
                self._connections.discard(connection)
            connection.close()

    def close(self) -> None:
        self._stop.set()
        self.broker.close()
        if self._socket is not None:
            self._socket.close()
        with self._connections_lock:
            for connection in self._connections:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        if (
            self._socket_identity is not None
            and self.socket_path.exists()
            and self.socket_path.lstat().st_ino == self._socket_identity
        ):
            self.socket_path.unlink()


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
                "core_generation": self.core_generation,
            }
        )
        if channel.receive() != {
            "schema": CAPTURE_VERSION,
            "kind": "ready",
            "core_generation": self.core_generation,
        }:
            raise CaptureProtocolError("edge_session_handshake_rejected")
        while not stop.is_set():
            self._require_owned()
            pending = self.spool.pending()
            if pending is not None:
                channel.send(pending)
                response = channel.receive()
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
            elif set(response) == {"schema", "kind", "request"} and response["kind"] == "work":
                request: CaptureRequest = typed_value(CaptureRequest, response["request"])
                try:
                    envelope = self.executor.capture(
                        request,
                        stream_epoch=self.spool.stream_epoch,
                        sequence=self.spool.next_sequence,
                        cancelled=stop,
                    )
                    self._require_owned()
                    self.spool.store(envelope)
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
            self.argv(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )
        assert process.stdin is not None and process.stdout is not None
        try:
            yield FramedStream(process.stdout, process.stdin)
        finally:
            process.stdin.close()
            process.stdout.close()
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def run_core_stdio(
    socket_path: Path,
    *,
    reader: IO[Any] | None = None,
    writer: IO[Any] | None = None,
) -> None:
    """Fixed remote edge-session command: stdio <-> private core Unix socket."""
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
    connection.connect(str(socket_path))
    errors: list[BaseException] = []

    def upload() -> None:
        try:
            read_chunk = getattr(input_stream, "read1", input_stream.read)
            while True:
                chunk = read_chunk(65_536)
                if not chunk:
                    break
                connection.sendall(chunk)
        except (OSError, ValueError) as exc:
            errors.append(exc)
        finally:
            try:
                connection.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    thread = threading.Thread(target=upload, daemon=True)
    thread.start()
    try:
        while True:
            chunk = connection.recv(65_536)
            if not chunk:
                break
            output_stream.write(chunk)
            output_stream.flush()
    finally:
        connection.close()
    if errors:
        raise RelayDisconnected("edge_stdio_proxy_disconnected")

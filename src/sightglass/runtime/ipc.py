from __future__ import annotations

import ctypes
import hmac
import json
import os
import socket
import struct
import sys
import uuid
from pathlib import Path
from typing import Any

from .config import ConfigStore
from .secrets import (
    OPERATOR_SECRET_ACCOUNT,
    READER_SECRET_ACCOUNT,
    SecretStore,
    default_secret_store,
)

IPC_VERSION = 1
MAX_FRAME_BYTES = 16 * 1024 * 1024


class IPCError(RuntimeError):
    pass


class IPCUnavailableError(IPCError):
    pass


class IPCTimeoutError(IPCError):
    pass


def peer_effective_ids(connection: socket.socket) -> tuple[int, int]:
    if sys.platform == "linux":
        credentials = connection.getsockopt(
            socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("iII")
        )
        _pid, uid, gid = struct.unpack("iII", credentials)
        return uid, gid
    libc = ctypes.CDLL(None, use_errno=True)
    getpeereid = getattr(libc, "getpeereid", None)
    if getpeereid is None:
        raise IPCError("peer credential verification is unavailable")
    getpeereid.argtypes = [
        ctypes.c_int,
        ctypes.POINTER(ctypes.c_uint),
        ctypes.POINTER(ctypes.c_uint),
    ]
    getpeereid.restype = ctypes.c_int
    uid = ctypes.c_uint()
    gid = ctypes.c_uint()
    if getpeereid(connection.fileno(), ctypes.byref(uid), ctypes.byref(gid)) != 0:
        error_number = ctypes.get_errno()
        raise IPCError(os.strerror(error_number))
    return int(uid.value), int(gid.value)


def _receive_exact(connection: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise IPCError("IPC connection closed before the frame completed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def receive_frame(connection: socket.socket) -> dict[str, Any]:
    length = struct.unpack("!I", _receive_exact(connection, 4))[0]
    if length < 2 or length > MAX_FRAME_BYTES:
        raise IPCError("IPC frame size is invalid")
    try:
        value = json.loads(_receive_exact(connection, length).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IPCError("IPC frame is not strict UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise IPCError("IPC frame must contain a JSON object")
    return value


def send_frame(connection: socket.socket, value: dict[str, Any]) -> None:
    payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(payload) > MAX_FRAME_BYTES:
        raise IPCError("IPC response exceeds the frame budget")
    connection.sendall(struct.pack("!I", len(payload)) + payload)


def authenticated_role(
    supplied_token: str, *, reader_token_hash: str, operator_token_hash: str
) -> str | None:
    from .secrets import token_hash

    supplied_hash = token_hash(supplied_token)
    if reader_token_hash and hmac.compare_digest(supplied_hash, reader_token_hash):
        return "reader"
    if operator_token_hash and hmac.compare_digest(supplied_hash, operator_token_hash):
        return "operator"
    return None


class IPCClient:
    def __init__(
        self,
        *,
        config_store: ConfigStore | None = None,
        secret_store: SecretStore | None = None,
        role: str = "reader",
        timeout: float = 30.0,
    ) -> None:
        if role not in {"reader", "operator"}:
            raise ValueError("IPC role must be reader or operator")
        self.config_store = config_store or ConfigStore()
        self.secret_store = secret_store or default_secret_store()
        self.role = role
        self.timeout = timeout

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        config = self.config_store.load()
        account = READER_SECRET_ACCOUNT if self.role == "reader" else OPERATOR_SECRET_ACCOUNT
        token = self.secret_store.get(account)
        request_id = uuid.uuid4().hex
        request = {
            "version": IPC_VERSION,
            "id": request_id,
            "token": token,
            "method": method,
            "params": params or {},
        }
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(self.timeout)
        try:
            connection.connect(str(config.socket_path))
            send_frame(connection, request)
            response = receive_frame(connection)
        except TimeoutError as exc:
            raise IPCTimeoutError("Sightglass IPC request timed out") from exc
        except OSError as exc:
            raise IPCUnavailableError("Sightglass IPC service is unavailable") from exc
        finally:
            connection.close()
        if response.get("version") != IPC_VERSION or response.get("id") != request_id:
            raise IPCError("invalid sightglassd response envelope")
        if response.get("ok") is not True:
            error = response.get("error")
            message = error.get("message") if isinstance(error, dict) else None
            raise IPCError(str(message or "sightglassd rejected the request"))
        return response.get("result")


def socket_is_private(path: Path) -> bool:
    import stat

    metadata = path.lstat()
    return stat.S_ISSOCK(metadata.st_mode) and stat.S_IMODE(metadata.st_mode) == 0o600

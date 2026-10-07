"""Offline core source binding; native operations arrive only as sealed captures.

No provider method performs RPC. The core coordinator obtains one complete capture
before running the existing reader against its operation-local frozen provider.
"""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from sightglass.contracts.capture import CaptureProtocolError
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.source.base import SourceHealth, SourceProviderDescriptor
from sightglass.source.capture.codec import canonical_json, json_value, strict_json, typed_value

REMOTE_SETTINGS_SCHEMA = "sightglass.remote-capture.v1"


@dataclass(frozen=True)
class RemoteCaptureSettings:
    source_instance_id: str
    account_id: str
    conversations: frozenset[str]
    egress_revision: str
    stream_epoch: str
    edge_token_hash: str
    socket_path: Path
    origin: SourceProviderDescriptor

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": REMOTE_SETTINGS_SCHEMA,
            "source_instance_id": self.source_instance_id,
            "account_id": self.account_id,
            "conversations": sorted(self.conversations),
            "egress_revision": self.egress_revision,
            "stream_epoch": self.stream_epoch,
            "edge_token_hash": self.edge_token_hash,
            "socket_path": str(self.socket_path),
            "origin": json_value(self.origin),
        }

    @classmethod
    def load(cls, path: Path) -> RemoteCaptureSettings:
        parent = path.parent.lstat()
        if (not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid()
                or stat.S_IMODE(parent.st_mode) & 0o077):
            raise RuntimeError("capture settings directory is unsafe")
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            metadata = os.fstat(descriptor)
            if (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1
                    or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) & 0o077
                    or not 0 < metadata.st_size <= 512 * 1024):
                raise RuntimeError("capture settings are unsafe")
            value = strict_json(os.read(descriptor, 512 * 1024 + 1), max_bytes=512 * 1024)
        finally:
            os.close(descriptor)
        return cls.parse(value)

    @classmethod
    def parse(cls, value: Any) -> RemoteCaptureSettings:
        import re

        keys = {"schema", "source_instance_id", "account_id", "conversations",
                "egress_revision", "stream_epoch", "edge_token_hash", "socket_path", "origin"}
        if not isinstance(value, dict) or set(value) != keys:
            raise RuntimeError("invalid remote capture settings")
        if value["schema"] != REMOTE_SETTINGS_SCHEMA:
            raise RuntimeError("unsupported remote capture settings")
        for key in ("source_instance_id", "account_id", "egress_revision", "stream_epoch"):
            if (not isinstance(value[key], str) or not value[key]
                    or len(value[key].encode()) > 65_536 or "\x00" in value[key]):
                raise RuntimeError("invalid capture identity binding")
        conversations = value["conversations"]
        if (not isinstance(conversations, list) or len(conversations) > 10_000
                or any(not isinstance(item, str) or not item or "\x00" in item
                       for item in conversations)
                or len(set(conversations)) != len(conversations)):
            raise RuntimeError("invalid capture egress ceiling")
        if (not isinstance(value["edge_token_hash"], str)
                or not re.fullmatch(r"[a-f0-9]{64}", value["edge_token_hash"])):
            raise RuntimeError("invalid separate edge capability")
        socket_path = value["socket_path"]
        if (not isinstance(socket_path, str) or not Path(socket_path).is_absolute()
                or "\x00" in socket_path or ".." in Path(socket_path).parts):
            raise RuntimeError("invalid capture socket path")
        try:
            origin = typed_value(SourceProviderDescriptor, value["origin"])
        except CaptureProtocolError as exc:
            raise RuntimeError("invalid capture origin descriptor") from exc
        if origin.kind not in {"synthetic", "macos-wechat"}:
            raise RuntimeError("capture origin provider is unsupported")
        return cls(value["source_instance_id"], value["account_id"], frozenset(conversations),
                   value["egress_revision"], value["stream_epoch"], value["edge_token_hash"],
                   Path(socket_path), origin)

    def write(self, path: Path) -> None:
        """Enroll explicit private settings; never replace a live binding implicitly."""
        self.parse(self.as_dict())
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        metadata = path.parent.lstat()
        if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) & 0o077):
            raise RuntimeError("capture settings directory is unsafe")
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_json(self.as_dict()))
            handle.flush()
            os.fsync(handle.fileno())
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


class RemoteCaptureProvider:
    def __init__(self, settings: RemoteCaptureSettings) -> None:
        self.settings = settings
        self.origin_descriptor = settings.origin
        # Only assembly kind changes. Interpretation/epoch remains the origin's.
        self.descriptor = replace(settings.origin, kind="remote-capture", platform=("linux",))

    def health(self) -> SourceHealth:
        return SourceHealth(True, False, 1, "offline", "", "", "", {},
                            ("edge_confirmation_required",))

    @staticmethod
    def _unavailable(*args: Any, **kwargs: Any) -> Any:
        raise SightglassError(ErrorCode.SERVICE_UNAVAILABLE, retryable=True,
                              details={"reason": "edge_confirmation_required"})

    snapshot = session = list_accounts = list_conversations = get_conversation = _unavailable
    resolve_conversation = list_participants = resolve_participant = _unavailable
    read_recent = read_range = prepare_search_page = scan_discovery_page = _unavailable
    search_generation_binding = get_message = read_resource = _unavailable
    capture_resource_binding = _unavailable
    catalog_complete = active_conversations_only = _unavailable

    @staticmethod
    def close() -> None:
        pass

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class CachedObject:
    digest: str
    mime_type: str
    data: bytes
    origin: str


@dataclass(frozen=True)
class ResourceReadPayload:
    descriptor: dict[str, Any]
    data: bytes | None = None
    mime_type: str | None = None
    content_kind: Literal["image", "audio", "blob", "text"] | None = None
    text: str | None = None

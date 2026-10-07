from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

SourceResourceVariant = Literal["original", "thumbnail"]


@dataclass(frozen=True)
class SourceResource:
    source_ordinal: int
    kind: str
    source_resource_key: str | None = None
    mime_type: str | None = None
    original_name: str | None = None
    declared_size: int | None = None
    declared_hash: str | None = None
    availability: str = "metadata_only"

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_ordinal": self.source_ordinal,
            "kind": self.kind,
            "source_resource_key": self.source_resource_key,
            "mime_type": self.mime_type,
            "original_name": self.original_name,
            "declared_size": self.declared_size,
            "declared_hash": self.declared_hash,
            "availability": self.availability,
        }


@dataclass(frozen=True)
class SourceResourcePayload:
    """Bounded bytes read from one provider-owned source resource.

    ``variant`` names the source entry the bytes came from. ``original`` is the
    source original; ``thumbnail`` is a derived preview the source kept instead of
    an original. Read paths bind the exact variant and never present a derived
    entry as a source original.
    """

    source_resource_key: str
    data: bytes
    variant: SourceResourceVariant = "original"

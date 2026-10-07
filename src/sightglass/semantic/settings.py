"""Frozen configuration for the optional Cloudflare semantic candidate lane.

The lane is disabled unless an operator explicitly enables it *and* consents to
external data egress *and* names one exact source account and an explicit set of
conversations. Persisted settings never carry credentials; the Cloudflare API
token lives only in process memory (Keychain at the runtime boundary).
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass

CF_ACCOUNT_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
INDEX_NAME_PREFIX = "sightglass-"
# No model is configurable: the active embedding identity is fixed by the lane
# recipe so a shared dimension count can never be mistaken for model continuity.
ACTIVE_MODEL = "@cf/baai/bge-m3"
ACTIVE_DIMENSIONS = 1024
ACTIVE_METRIC = "cosine"
# The embedding recipe identity also participates in namespace/fence derivation so
# a dimension count alone can never be mistaken for model continuity.
ACTIVE_RECIPE = "sightglass.semantic.bge-m3.message.v2"


def _bounded_timeout(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("semantic timeout_seconds must be a number")
    result = float(value)
    if not 0.1 <= result <= 60.0:
        raise ValueError("semantic timeout_seconds must be between 0.1 and 60 seconds")
    return result


@dataclass(frozen=True)
class SemanticSettings:
    enabled: bool = False
    external_data_authorized: bool = False
    cf_account_id: str = ""
    index_name: str = ""
    source_account_id: str = ""
    conversation_ids: tuple[str, ...] = ()
    timeout_seconds: float = 8.0

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool or type(self.external_data_authorized) is not bool:
            raise ValueError("semantic flags must be boolean")
        conversations = tuple(self.conversation_ids)
        if len(dict.fromkeys(conversations)) != len(conversations):
            raise ValueError("semantic conversation_ids must be unique")
        if any(type(value) is not str or not value for value in conversations):
            raise ValueError("semantic conversation_ids must be non-empty strings")
        object.__setattr__(self, "conversation_ids", tuple(sorted(conversations)))
        object.__setattr__(self, "timeout_seconds", _bounded_timeout(self.timeout_seconds))
        for name in ("cf_account_id", "index_name", "source_account_id"):
            if type(getattr(self, name)) is not str:
                raise ValueError(f"semantic {name} must be a string")
        if not self.enabled:
            return
        if not self.external_data_authorized:
            raise ValueError("enabled semantic lane requires external data authorization")
        if not CF_ACCOUNT_ID_PATTERN.fullmatch(self.cf_account_id):
            raise ValueError("semantic cf_account_id must be 32 lowercase hex characters")
        if not self.index_name.startswith(INDEX_NAME_PREFIX) or len(self.index_name) <= len(
            INDEX_NAME_PREFIX
        ):
            raise ValueError(f"semantic index_name must start with {INDEX_NAME_PREFIX!r}")
        if not self.source_account_id:
            raise ValueError("enabled semantic lane requires one exact source account")
        if not conversations:
            raise ValueError("enabled semantic lane requires explicit conversation_ids")

    @classmethod
    def from_dict(cls, value: object) -> SemanticSettings:
        if not isinstance(value, dict):
            raise ValueError("semantic settings must be an object")
        if set(value) - set(cls.__dataclass_fields__):
            raise ValueError("unknown semantic setting")
        payload = dict(value)
        if "conversation_ids" in payload:
            raw = payload["conversation_ids"]
            if not isinstance(raw, (list, tuple)) or isinstance(raw, str):
                raise ValueError("semantic conversation_ids must be a list of strings")
            if any(type(item) is not str or not item for item in raw):
                raise ValueError("semantic conversation_ids must be non-empty strings")
            payload["conversation_ids"] = tuple(raw)
        for name in ("cf_account_id", "index_name", "source_account_id"):
            if name in payload and type(payload[name]) is not str:
                raise ValueError(f"semantic {name} must be a string")
        if payload.get("enabled") is not None and type(payload.get("enabled")) is not bool:
            raise ValueError("semantic enabled must be a boolean")
        if payload.get("enabled") is None and "enabled" in payload:
            raise ValueError("semantic enabled must be a boolean")
        if payload.get("external_data_authorized") is not None and (
            type(payload.get("external_data_authorized")) is not bool
        ):
            raise ValueError("semantic external_data_authorized must be a boolean")
        if "external_data_authorized" in payload and payload["external_data_authorized"] is None:
            raise ValueError("semantic external_data_authorized must be a boolean")
        return cls(**payload)

    def as_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["conversation_ids"] = list(self.conversation_ids)
        return value

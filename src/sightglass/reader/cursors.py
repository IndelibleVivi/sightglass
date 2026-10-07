from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from typing import Any

from sightglass.contracts.common import SourceSortKey, parse_aware_datetime, utc_now
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.source.identity import SignedTokenCodec, opaque_id

CURSOR_SCHEMA = "sightglass.cursor.v2"


def reader_binding(reader_id: str) -> str:
    return opaque_id("wxreader", reader_id)


def filter_digest(
    participant_ids: tuple[str, ...],
    query: str | None = None,
    time_after: str | None = None,
    time_before: str | None = None,
    system_policy: str = "include",
) -> str:
    value = {
        "participant_ids": sorted(set(participant_ids)),
        "query_digest": (
            hashlib.sha256(" ".join(query.casefold().split()).encode("utf-8")).hexdigest()
            if query
            else None
        ),
        "time_after": time_after,
        "time_before": time_before,
        "system_policy": system_policy,
    }
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def search_scope_digest(
    *,
    conversation_ids: tuple[str, ...],
    participant_ids: tuple[str, ...],
    query: str,
    after: str | None,
    before: str | None,
) -> str:
    value = {
        "conversation_ids": sorted(set(conversation_ids)),
        "participant_ids": sorted(set(participant_ids)),
        "query_digest": hashlib.sha256(query.casefold().encode("utf-8")).hexdigest(),
        "after": after,
        "before": before,
    }
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def cursor_scope(
    participant_ids: tuple[str, ...],
    query: str | None = None,
    time_after: str | None = None,
    time_before: str | None = None,
    system_policy: str = "include",
) -> tuple[str, str]:
    unique = tuple(sorted(set(participant_ids)))
    if (
        not unique
        and not query
        and not time_after
        and not time_before
        and system_policy == "include"
    ):
        return "conversation", "*"
    if (
        len(unique) == 1
        and not query
        and not time_after
        and not time_before
        and system_policy == "include"
    ):
        return "participant", unique[0]
    return "filter_set", filter_digest(unique, query, time_after, time_before, system_policy)


class TimelineCursorCodec:
    def __init__(self, codec: SignedTokenCodec) -> None:
        self.codec = codec

    def issue(
        self,
        *,
        reader_id: str,
        account_id: str,
        conversation_id: str,
        mode: str,
        direction: str,
        scope_kind: str,
        scope_key: str,
        row: Any,
        inventory_digest: str,
        generation_set_digest: str,
        dependency_generation_digest: str | None,
        projection_epoch: str,
        view: str = "auto",
    ) -> str:
        return self.codec.encode(
            {
                "schema": CURSOR_SCHEMA,
                "kind": "timeline",
                "view": view,
                "reader": reader_binding(reader_id),
                "account_id": account_id,
                "conversation_id": conversation_id,
                "mode": mode,
                "direction": direction,
                "scope_kind": scope_kind,
                "scope_key": scope_key,
                "position": {
                    "message_id": str(row["message_id"]),
                    "sort": [
                        str(row["sort_primary"]),
                        int(row["sort_seq"]),
                        int(row["sort_tie"]),
                    ],
                },
                "source": {
                    "inventory_digest": inventory_digest,
                    "generation_set_digest": generation_set_digest,
                    "dependency_generation_digest": dependency_generation_digest,
                    "projection_epoch": projection_epoch,
                },
                "issued_at": utc_now().isoformat(timespec="microseconds"),
            }
        )

    def verify(
        self,
        token: str,
        *,
        reader_id: str,
        account_id: str,
        conversation_id: str,
        mode: str,
        direction: str,
        scope_kind: str,
        scope_key: str,
        view: str = "auto",
    ) -> dict[str, Any]:
        payload = self.codec.decode(token)
        expected = {
            "schema": CURSOR_SCHEMA,
            "kind": "timeline",
            "reader": reader_binding(reader_id),
            "account_id": account_id,
            "conversation_id": conversation_id,
            "mode": mode,
            "direction": direction,
            "scope_kind": scope_kind,
            "scope_key": scope_key,
        }
        if any(payload.get(key) != value for key, value in expected.items()):
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        if payload.get("view", "auto") != view:
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        position = payload.get("position")
        source = payload.get("source")
        issued_at = payload.get("issued_at")
        if (
            not isinstance(position, dict)
            or not isinstance(position.get("message_id"), str)
            or not isinstance(position.get("sort"), list)
            or len(position["sort"]) != 3
            or not isinstance(source, dict)
            or not isinstance(source.get("inventory_digest"), str)
            or not isinstance(source.get("generation_set_digest"), str)
            or not isinstance(issued_at, str)
        ):
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        try:
            age = utc_now() - parse_aware_datetime(issued_at)
        except ValueError as exc:
            raise SightglassError(ErrorCode.CURSOR_INVALID) from exc
        if age > timedelta(days=30) or age < -timedelta(minutes=5):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        return payload

    def issue_materialized(
        self,
        *,
        reader_id: str,
        account_id: str,
        conversation_id: str,
        mode: str,
        direction: str,
        scope_kind: str,
        scope_key: str,
        row: Any,
        projection_epoch: str,
        observation_watermark: int,
        policy_revision: str,
        view: str = "auto",
    ) -> str:
        return self.codec.encode(
            {
                "schema": CURSOR_SCHEMA,
                "kind": "timeline-materialized",
                "view": view,
                "reader": reader_binding(reader_id),
                "account_id": account_id,
                "conversation_id": conversation_id,
                "mode": mode,
                "direction": direction,
                "scope_kind": scope_kind,
                "scope_key": scope_key,
                "position": {
                    "message_id": str(row["message_id"]),
                    "sort": [
                        str(row["sort_primary"]),
                        int(row["sort_seq"]),
                        int(row["sort_tie"]),
                    ],
                },
                "projection": {
                    "epoch": projection_epoch,
                    "observation_watermark": int(observation_watermark),
                    "policy_revision": policy_revision,
                },
                "issued_at": utc_now().isoformat(timespec="microseconds"),
            }
        )

    def verify_materialized(
        self,
        token: str,
        *,
        reader_id: str,
        account_id: str,
        conversation_id: str,
        mode: str,
        direction: str,
        scope_kind: str,
        scope_key: str,
        projection_epoch: str,
        policy_revision: str,
        view: str = "auto",
    ) -> dict[str, Any]:
        payload = self.codec.decode(token)
        expected = {
            "schema": CURSOR_SCHEMA,
            "kind": "timeline-materialized",
            "reader": reader_binding(reader_id),
            "account_id": account_id,
            "conversation_id": conversation_id,
            "mode": mode,
            "direction": direction,
            "scope_kind": scope_kind,
            "scope_key": scope_key,
        }
        if any(payload.get(key) != value for key, value in expected.items()):
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        if payload.get("view", "auto") != view:
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        position = payload.get("position")
        projection = payload.get("projection")
        issued_at = payload.get("issued_at")
        if (
            not isinstance(position, dict)
            or not isinstance(position.get("message_id"), str)
            or not isinstance(position.get("sort"), list)
            or len(position["sort"]) != 3
            or not isinstance(projection, dict)
            or projection.get("epoch") != projection_epoch
            or projection.get("policy_revision") != policy_revision
            or type(projection.get("observation_watermark")) is not int
            or int(projection["observation_watermark"]) < 0
            or not isinstance(issued_at, str)
        ):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        try:
            age = utc_now() - parse_aware_datetime(issued_at)
        except ValueError as exc:
            raise SightglassError(ErrorCode.CURSOR_INVALID) from exc
        if age > timedelta(days=30) or age < -timedelta(minutes=5):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        return payload


class SearchCursorCodec:
    def __init__(self, codec: SignedTokenCodec) -> None:
        self.codec = codec

    def issue(
        self,
        *,
        reader_id: str,
        account_id: str,
        scope_key: str,
        row: Any,
        inventory_digest: str,
        generation_set_digest: str,
        projection_epoch: str,
    ) -> str:
        return self.codec.encode(
            {
                "schema": CURSOR_SCHEMA,
                "kind": "search",
                "reader": reader_binding(reader_id),
                "account_id": account_id,
                "scope_key": scope_key,
                "position": {
                    "message_id": str(row["message_id"]),
                    "sort": [
                        str(row["sort_primary"]),
                        int(row["sort_seq"]),
                        int(row["sort_tie"]),
                        # The opaque message id is the final scan key, so the signed
                        # payload can resume a bounded candidate scan without a lookup
                        # and without ever exposing a source message id.
                        str(row["message_id"]),
                    ],
                },
                "source": {
                    "inventory_digest": inventory_digest,
                    "generation_set_digest": generation_set_digest,
                    "projection_epoch": projection_epoch,
                },
                "issued_at": utc_now().isoformat(timespec="microseconds"),
            }
        )

    def verify(
        self,
        token: str,
        *,
        reader_id: str,
        account_id: str,
        scope_key: str,
    ) -> dict[str, Any]:
        payload = self.codec.decode(token)
        expected = {
            "schema": CURSOR_SCHEMA,
            "kind": "search",
            "reader": reader_binding(reader_id),
            "account_id": account_id,
            "scope_key": scope_key,
        }
        if any(payload.get(key) != value for key, value in expected.items()):
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        position = payload.get("position")
        source = payload.get("source")
        issued_at = payload.get("issued_at")
        if (
            not isinstance(position, dict)
            or not isinstance(position.get("message_id"), str)
            or not isinstance(position.get("sort"), list)
            or len(position["sort"]) != 4
            or not isinstance(position["sort"][0], str)
            or type(position["sort"][1]) is not int
            or type(position["sort"][2]) is not int
            or position["sort"][3] != position["message_id"]
            or not isinstance(source, dict)
            or not isinstance(source.get("inventory_digest"), str)
            or not isinstance(source.get("generation_set_digest"), str)
            or not isinstance(issued_at, str)
        ):
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        try:
            age = utc_now() - parse_aware_datetime(issued_at)
        except ValueError as exc:
            raise SightglassError(ErrorCode.CURSOR_INVALID) from exc
        if age > timedelta(days=30) or age < -timedelta(minutes=5):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        return payload


class AccountCursorCodec:
    """Signed catalog/inbox/participant cursors bound to reader and snapshot."""

    def __init__(self, codec: SignedTokenCodec) -> None:
        self.codec = codec

    def issue(
        self,
        *,
        kind: str,
        reader_id: str,
        account_id: str,
        scope_key: str,
        policy_revision: str,
        position: list[Any],
        snapshot: dict[str, Any],
    ) -> str:
        if kind not in {"catalog", "inbox", "participants", "resources", "links", "retrieval",
                        "search-replica"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        return self.codec.encode(
            {
                "schema": CURSOR_SCHEMA,
                "kind": kind,
                "reader": reader_binding(reader_id),
                "account_id": account_id,
                "scope_key": scope_key,
                "policy_revision": policy_revision,
                "position": list(position),
                "snapshot": dict(snapshot),
                "issued_at": utc_now().isoformat(timespec="microseconds"),
            }
        )

    def verify(
        self,
        token: str,
        *,
        kind: str,
        reader_id: str,
        account_id: str,
        scope_key: str,
        policy_revision: str,
    ) -> dict[str, Any]:
        payload = self.codec.decode(token)
        expected = {
            "schema": CURSOR_SCHEMA,
            "kind": kind,
            "reader": reader_binding(reader_id),
            "account_id": account_id,
            "scope_key": scope_key,
            "policy_revision": policy_revision,
        }
        if any(payload.get(key) != value for key, value in expected.items()):
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        position = payload.get("position")
        snapshot = payload.get("snapshot")
        issued_at = payload.get("issued_at")
        if (
            not isinstance(position, list)
            or not position
            or not isinstance(snapshot, dict)
            or not isinstance(issued_at, str)
        ):
            raise SightglassError(ErrorCode.CURSOR_INVALID)
        try:
            age = utc_now() - parse_aware_datetime(issued_at)
        except ValueError as exc:
            raise SightglassError(ErrorCode.CURSOR_INVALID) from exc
        if age > timedelta(days=30) or age < -timedelta(minutes=5):
            raise SightglassError(ErrorCode.CURSOR_STALE)
        return payload


def source_sort_key(row: Any) -> SourceSortKey:
    return SourceSortKey(
        str(row["sort_primary"]),
        int(row["sort_seq"]),
        int(row["sort_tie"]),
        str(row["source_message_id"]),
    )

"""Materialized link discovery and canonical retrieval with an optional semantic lane."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from sightglass.contracts.common import parse_aware_datetime, to_utc_iso
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.current_body import current_search_document
from sightglass.model.lexical import LEXICAL_RECIPE, candidate_expression
from sightglass.model.links import LinkRepository
from sightglass.operations import check_operation_budget, operation_budget
from sightglass.source.identity import opaque_id
from sightglass.source.links import LINK_EXTRACTION_VERSION, hint_evidence, normalize_domain

from .cursors import source_sort_key
from .projections import CompactBodyBudgetAllocator
from .response_budget import active_response_budget, response_budget

if TYPE_CHECKING:
    from .service import ReaderService

RETRIEVAL_RECIPE = "sightglass.retrieval.rrf-context.v3"
LINK_CANDIDATE_BUDGET = 2_000
LEXICAL_CANDIDATE_BUDGET = 1_000
CONTEXT_RADIUS_MESSAGES = 8
CONTEXT_RADIUS_SECONDS = 180
MAX_CONTEXT_MESSAGES = 32
MAX_CONTEXT_LINKS = 32
_PUBLIC_DISCOVERY_FIELDS = frozenset({
    "performed", "kind", "complete", "conversation_count", "prepared_conversation_count",
    "message_count", "matched_message_count", "message_budget", "conversation_budget",
    "scanned_row_count", "scan_budget", "admitted_this_attempt",
    "unprepared_conversation_count", "pending_conversation_count",
    "remaining_conversation_ids", "unprepared_conversation_ids", "mode", "prepared_at",
})


@dataclass(frozen=True)
class RetrievalScope:
    account_id: str
    conversation_ids: tuple[str, ...]
    after: str | None
    before: str | None
    participant_ids: tuple[str, ...]
    epoch: str


class RetrievalService:
    def __init__(self, service: ReaderService) -> None:
        self.service = service
        self.repository = service.repository
        self.reader = service.reader
        self.links = LinkRepository(self.repository.database)

    def semantic_status(self) -> dict[str, Any]:
        if self.service.semantic is not None:
            return self.service.semantic.status()
        reason = self.service.semantic_unavailable_reason
        return {"state": "degraded" if reason else "disabled", "reason": reason}

    def _semantic_token(self) -> str:
        return (
            self.service.semantic.state_token()
            if self.service.semantic is not None else "disabled"
        )

    def _lexical_ready(self) -> bool:
        with self.repository.database.connection() as connection:
            row = connection.execute(
                "SELECT state FROM derived_index_state WHERE index_kind='lexical'"
            ).fetchone()
        return row is not None and row[0] == "ready"

    def readiness(self) -> dict[str, str]:
        """Published generation state for cold/cached health, without a history audit."""
        with self.repository.database.connection() as connection:
            rows = connection.execute(
                "SELECT index_kind, recipe, state FROM derived_index_state "
                "WHERE index_kind IN ('links', 'lexical')"
            ).fetchall()
        states = {row["index_kind"]: row for row in rows}
        link_row = states.get("links")
        lexical_row = states.get("lexical")
        links = (
            link_row["state"]
            if link_row is not None and link_row["recipe"] == LINK_EXTRACTION_VERSION
            else "building"
        )
        lexical = (
            lexical_row["state"]
            if lexical_row is not None and lexical_row["recipe"] == LEXICAL_RECIPE
            else "building"
        )
        return {
            "link_index": links,
            "lexical_index": lexical,
            "semantic_index": self.semantic_status()["state"],
            "retrieval": "ready" if links == lexical == "ready" else "degraded",
        }

    def status(
        self, *, detailed: bool = False, include_counts: bool = True
    ) -> dict[str, Any]:
        state = self.links.state()
        indexed = (None, None)
        pending = None
        lexical_count = None
        with self.repository.database.connection() as connection:
            lexical = connection.execute(
                "SELECT * FROM derived_index_state WHERE index_kind='lexical'"
            ).fetchone()
            if include_counts:
                indexed = connection.execute(
                    "SELECT COUNT(*), COALESCE(SUM(link_count),0) "
                    "FROM message_link_projection WHERE extraction_version=?",
                    (LINK_EXTRACTION_VERSION,),
                ).fetchone()
                pending = connection.execute(
                    "SELECT COUNT(*) FROM messages AS m LEFT JOIN message_link_projection AS p "
                    "ON p.message_id=m.message_id WHERE m.current_observation_seq IS NOT NULL AND "
                    "(p.message_id IS NULL OR p.source_observation_seq!=m.current_observation_seq "
                    "OR p.extraction_version!=?)",
                    (LINK_EXTRACTION_VERSION,),
                ).fetchone()[0]
                lexical_count = connection.execute(
                    "SELECT COUNT(*) FROM message_lexical_projection WHERE recipe=?",
                    (LEXICAL_RECIPE,),
                ).fetchone()[0]
        readiness = self.readiness()
        if include_counts and pending:
            readiness["retrieval"] = "degraded"
        result: dict[str, Any] = {
            "schema": "sightglass.retrieval-status.v1",
            "readiness": readiness,
            "statistics_collected": include_counts,
            "links": {
                **state,
                "indexed_messages": indexed[0],
                "link_count": indexed[1],
                "pending_messages": pending,
            },
            "recipe": RETRIEVAL_RECIPE,
            "semantic": self.semantic_status(),
            "lexical": {
                **(dict(lexical) if lexical else {"state": "building", "generation": 1}),
                "recipe": LEXICAL_RECIPE,
                "indexed_messages": lexical_count,
                "unsupported_query_fallback": "bounded_literal_scan",
            },
        }
        if detailed:
            result["ranking"] = {
                "fusion": "reciprocal_rank",
                "rank_constant": 60,
                "semantic_context": "best_candidate_rank",
                "structural_preferences": "secondary_tie_break",
                "link_cooccurrence": 0.01,
                "count_hint": "0.01/(1+absolute_count_difference)",
                "context_radius_messages": CONTEXT_RADIUS_MESSAGES,
                "context_radius_seconds": CONTEXT_RADIUS_SECONDS,
                "max_context_messages": MAX_CONTEXT_MESSAGES,
                "candidate_budget": LINK_CANDIDATE_BUDGET + LEXICAL_CANDIDATE_BUDGET,
            }
        return result

    def _scope(
        self,
        account_id: str | None,
        conversation_ids: tuple[str, ...],
        participant_ids: tuple[str, ...],
        after: str | None,
        before: str | None,
    ) -> RetrievalScope:
        self.reader.require_search()
        accounts = self.repository.active_account_ids()
        selected_account = account_id or (accounts[0] if len(accounts) == 1 else None)
        if selected_account is None or selected_account not in accounts:
            raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)
        available = {
            str(row["conversation_id"])
            for row in self.repository.account_conversations(selected_account)
        }
        selected = tuple(sorted(set(conversation_ids or tuple(available))))
        for value in conversation_ids:
            self.reader.authorize(value)
            if value not in available:
                raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        selected = tuple(value for value in selected if self.reader.policy.permits(value))
        degraded = self.repository.degraded_conversation_ids(
            selected_account,
            error_code="duplicate_message_identity_conflict",
        )
        selected = tuple(value for value in selected if value not in degraded)
        for participant in participant_ids:
            if self.repository.participant_account_id(participant) != selected_account:
                raise SightglassError(ErrorCode.PARTICIPANT_OUT_OF_SCOPE)
        try:
            after_utc = to_utc_iso(after) if after is not None else None
            before_utc = to_utc_iso(before) if before is not None else None
        except ValueError as exc:
            raise SightglassError(ErrorCode.QUERY_INVALID) from exc
        if after_utc is not None and before_utc is not None and after_utc >= before_utc:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        return RetrievalScope(
            selected_account,
            selected,
            after_utc,
            before_utc,
            tuple(sorted(set(participant_ids))),
            self.service._projection_inventory_epoch(),
        )

    def _discovery_request(
        self,
        *,
        kind: str,
        query: str,
        hints: tuple[str, ...],
        domains: tuple[str, ...],
        kinds: tuple[str, ...] = (),
        account_id: str | None,
        conversation_ids: tuple[str, ...],
        participant_ids: tuple[str, ...],
        after: str | None,
        before: str | None,
        cursor: str | None,
        reading_token: str | None,
        limit: int,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, bool]:
        """Route one cold link/retrieval call through bounded source preparation.

        Returns ``(envelope, facts, consumed_cursor)``. When ``envelope`` is not
        ``None`` the caller sends it back immediately. When it is ``None`` the caller
        runs the ordinary resident read; ``facts`` (possibly ``None``) are the
        request-local preparation receipt for that read, and ``consumed_cursor`` is
        true when the caller's ``cursor`` was a preparation token that must not be
        re-used as a materialized pagination cursor. No cross-request instance state
        is used, so concurrent calls cannot contaminate each other's receipts.
        """

        manager = self.service.search_preparation
        if manager is None:
            if reading_token:
                raise SightglassError(ErrorCode.CURSOR_INVALID)
            return None, None, False
        if cursor is not None and reading_token is not None:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        try:
            after_utc = to_utc_iso(after) if after else None
            before_utc = to_utc_iso(before) if before else None
        except ValueError as exc:
            raise SightglassError(ErrorCode.QUERY_INVALID) from exc
        poll_token = reading_token
        if reading_token is None and cursor is not None:
            # A preparation token may also be delivered through `cursor` by hosts
            # retaining an older retrieval catalog; a materialized cursor is not a
            # preparation token and keeps the resident continuation path.
            try:
                payload = self.service.token_codec.decode(cursor)
            except SightglassError:
                payload = None
            if payload is not None and payload.get("job_kind") == "discovery":
                poll_token = cursor
            else:
                return None, None, False
        if poll_token is None:
            scope = self._scope(account_id, conversation_ids, participant_ids, after, before)
            if self._resident_scope_complete(scope):
                return None, None, False
        result = manager.request_discovery(
            kind=kind,
            query=query,
            hints=hints,
            domains=domains,
            kinds=kinds,
            account_id=account_id,
            conversation_ids=conversation_ids,
            participant_ids=participant_ids,
            after_utc=after_utc,
            before_utc=before_utc,
            limit=limit,
            token=poll_token,
        )
        if result.get("state") in {"ready", "partial"}:
            # A ready or bounded-partial scan may proceed; carry the honest
            # preparation receipt so the read reports partial coverage and an
            # actionable continuation instead of silently claiming completeness.
            facts = result.get("preparation")
            if facts is not None:
                # Provider generations and physical resume positions belong only
                # to the private job. Signed pagination is readable by its holder.
                facts = {key: value for key, value in facts.items()
                         if key in _PUBLIC_DISCOVERY_FIELDS}
                facts["state"] = result.get("state")
                if result.get("continuation_token"):
                    facts["continuation_token"] = result["continuation_token"]
            return None, facts, reading_token is None and cursor is not None
        return result, None, False

    def _resident_scope_complete(self, scope: RetrievalScope) -> bool:
        """Whether the resident subset can answer without opening the source.

        A resident subset alone never proves complete source scope. Only a scope
        whose residency decision collects bodies continuously (``keep``/``recent``)
        *and* whose catalog/conversation coverage is complete *and* whose candidate
        bodies are all currently resident is authoritative. Under the default
        ``on_demand`` residency the local set is deliberately partial -- stock
        release deletes derived rows and bodies, and a never-admitted source has
        neither -- so both must be treated as cold and routed through bounded
        preparation.
        """

        if not scope.conversation_ids:
            return True
        for conversation_id in scope.conversation_ids:
            if not self.service._residency_decision(conversation_id).collect_bodies:
                return False
        catalog = self.repository.source_catalog_state(scope.account_id)
        if catalog is None or str(catalog["coverage_state"]) != "complete":
            return False
        for conversation_id in scope.conversation_ids:
            state = self.repository.source_conversation_state(conversation_id)
            if (
                state is None
                or state["backfill_state"] != "complete"
                or not state.get("coverage_version")
                or not state.get("history_complete")
                or not state.get("forward_complete")
            ):
                return False
        return self.repository.retrieval_candidate_resident(
            scope.conversation_ids, epoch=scope.epoch
        )

    def _snapshot(
        self,
        *,
        kind: str,
        scope: RetrievalScope,
        arguments: dict[str, Any],
        cursor: str | None,
        discovery_facts: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], str, list[Any] | None]:
        digest = hashlib.sha256(
            json.dumps(
                arguments,
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        generation = self.links.state()["generation"]
        with self.repository.database.connection() as connection:
            lexical = connection.execute(
                "SELECT generation FROM derived_index_state WHERE index_kind='lexical'"
            ).fetchone()
        lexical_generation = lexical[0] if lexical is not None else 1
        if cursor:
            payload = self.service.account_cursors.verify(
                cursor,
                kind=kind,
                reader_id=self.reader.reader_id,
                account_id=scope.account_id,
                scope_key=digest,
                policy_revision=self.service._materialized_cursor_revision(),
            )
            snapshot = payload["snapshot"]
            watermark = snapshot.get("observation_watermark")
            if (
                snapshot.get("epoch") != scope.epoch
                or snapshot.get("generation") != generation
                or kind == "retrieval"
                and snapshot.get("lexical_generation") != lexical_generation
                or kind == "retrieval"
                and snapshot.get("semantic_token") != self._semantic_token()
                or snapshot.get("recipe") != RETRIEVAL_RECIPE
                or snapshot.get("extraction") != LINK_EXTRACTION_VERSION
                or type(watermark) is not int
                or watermark < 0
            ):
                raise SightglassError(ErrorCode.CURSOR_STALE)
            if any(
                self.repository.materialized_snapshot_changed(
                    conversation,
                    projection_epoch=scope.epoch,
                    observation_watermark=watermark,
                )
                for conversation in scope.conversation_ids
            ):
                raise SightglassError(ErrorCode.CURSOR_STALE)
            return snapshot, digest, payload["position"]
        fresh: dict[str, Any] = {
            "epoch": scope.epoch,
            "generation": generation,
            "lexical_generation": lexical_generation,
            "lexical_recipe": LEXICAL_RECIPE,
            "semantic_token": self._semantic_token(),
            "recipe": RETRIEVAL_RECIPE,
            "extraction": LINK_EXTRACTION_VERSION,
            "observation_watermark": self.repository.observation_watermark(),
            "link_start": None,
            "lexical_start": None,
        }
        if discovery_facts is not None:
            # Freeze the source-preparation status in the signed snapshot so a later
            # continuation page keeps the same honest partial/coverage facts instead
            # of silently reverting to a complete-scope claim.
            fresh["discovery"] = discovery_facts
        return fresh, digest, None

    def _cursor(
        self,
        kind: str,
        scope: RetrievalScope,
        digest: str,
        snapshot: dict[str, Any],
        position: list[Any],
    ) -> str:
        return self.service.account_cursors.issue(
            kind=kind,
            reader_id=self.reader.reader_id,
            account_id=scope.account_id,
            scope_key=digest,
            policy_revision=self.service._materialized_cursor_revision(),
            position=position,
            snapshot=snapshot,
        )

    def _receipts(
        self,
        scope: RetrievalScope,
        snapshot: dict[str, Any],
        *,
        returned: int,
        examined: int,
        budget: int,
        has_more: bool,
        scan_partial: bool,
        discovery_facts: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        catalog = self.repository.source_catalog_state(scope.account_id)
        catalog_coverage = str(catalog["coverage_state"]) if catalog is not None else "unknown"
        states = [
            self.repository.source_conversation_state(value) for value in scope.conversation_ids
        ]
        history_complete = all(
            state is not None and state["backfill_state"] == "complete" for state in states
        )
        coverage = self.links.coverage(
            scope.conversation_ids, epoch=scope.epoch, watermark=snapshot["observation_watermark"]
        )
        warnings = ["materialized_projection_not_live"]
        if not history_complete:
            warnings.append("history_not_fully_indexed")
        if coverage != "complete":
            warnings.append("link_index_partial")
        if scan_partial:
            warnings.append("retrieval_scan_budget_exhausted")
        source_receipt: dict[str, Any] = {
            "complete": catalog_coverage == "complete" and history_complete,
            "returned_count": returned,
            "served_from": "window_db",
            "coverage": {
                "catalog": catalog_coverage,
                "conversation": "complete" if history_complete else "indexed",
            },
            "warnings": warnings,
            "freshness": {
                "state": "bounded_stale",
                "live_refresh_confirmed": False,
                "observation_watermark": snapshot["observation_watermark"],
            },
        }
        effective_discovery = (
            discovery_facts if discovery_facts is not None else snapshot.get("discovery")
        )
        if effective_discovery is not None:
            source_receipt["discovery_preparation"] = effective_discovery
            source_receipt["freshness"]["prepared_subset"] = True
            # Traversal completion spans independent validated read leases. It is
            # never proof of one current complete source snapshot or global absence.
            source_receipt["complete"] = False
            source_receipt["coverage"]["conversation"] = "observed_pages"
            partial = bool(
                effective_discovery.get("pending_conversation_count")
                or effective_discovery.get("unprepared_conversation_count")
                or effective_discovery.get("complete") is False
            )
            if partial:
                # A partial source preparation never proves complete scope even
                # when older backfill flags still read complete.
                source_receipt["complete"] = False
                source_receipt["coverage"]["conversation"] = "prepared_subset"
                source_receipt["warnings"].append("retrieval_source_preparation_partial")
                source_receipt["source_continuation"] = {
                    "available": True,
                    "remaining_conversation_ids": list(
                        effective_discovery.get("remaining_conversation_ids", ())
                    ),
                    "reading_token": effective_discovery.get("continuation_token"),
                    "instruction": (
                        "Repeat identical arguments with this continuation reading_token "
                        "to scan older rows; ordinary reading_token polls do not advance."
                    ),
                }
        return {
            "freshness": "materialized_observed",
            "source_receipt": source_receipt,
            "index_receipt": {
                "kind": "link",
                "generation": snapshot["generation"],
                "recipe": LINK_EXTRACTION_VERSION,
                "coverage": coverage,
            },
            "execution": {
                "state": "partial" if scan_partial else "complete",
                "stop_reason": "candidate_budget"
                if scan_partial
                else "output_limit"
                if has_more
                else None,
                "candidates_examined": examined,
                "candidate_budget": budget,
                "continuation_available": has_more,
            },
        }

    @staticmethod
    def _link_key(row: Any) -> tuple[str, int, int, str, str]:
        return (
            str(row["sent_at_utc"]),
            int(row["sort_seq"]),
            int(row["sort_tie"]),
            str(row["message_id"]),
            str(row["link_id"]),
        )

    def _link_projection(self, row: Any) -> dict[str, Any]:
        self.reader.authorize(str(row["conversation_id"]))
        label = (
            self.repository.preferred_labels_bulk(
                (
                    (
                        str(row["sender_id"]),
                        str(row["sender_membership_id"]) if row["sender_membership_id"] else None,
                    ),
                )
            )
            if row["sender_id"]
            else {}
        )
        participant = str(row["sender_id"]) if row["sender_id"] else None
        anchor = self.service.token_codec.encode(
            {
                "schema": 1,
                "account_id": str(row["account_id"]),
                "conversation_id": str(row["conversation_id"]),
                "message_id": str(row["message_id"]),
                "sort": [
                    str(row["sent_at_utc"]),
                    int(row["sort_seq"]),
                    int(row["sort_tie"]),
                    str(row["message_id"]),
                ],
            }
        )
        return {
            "link_id": str(row["link_id"]),
            "message_id": str(row["message_id"]),
            "raw_url": str(row["raw_url"]),
            "normalized_url": str(row["normalized_url"]),
            "normalized_host": str(row["normalized_host"]),
            "path": str(row["path"]),
            "source_kind": str(row["source_kind"]),
            "source_path": str(row["source_path"]),
            "ordinal": int(row["ordinal"]),
            "title": row["title"],
            "description": row["description"],
            "conversation": {
                "conversation_id": str(row["conversation_id"]),
                "title": str(row["conversation_title"]),
                "kind": str(row["conversation_kind"]),
            },
            "sender": {
                "participant_id": participant,
                "label": next(iter(label.values()))[0] if label else "未知发送者",
            },
            "sent_at": str(row["sent_at_utc"]),
            "context_anchor": anchor,
        }

    def find_links(
        self,
        *,
        query: str = "",
        account_id: str | None = None,
        conversation_ids: tuple[str, ...] = (),
        domains: tuple[str, ...] = (),
        hints: tuple[str, ...] = (),
        after: str | None = None,
        before: str | None = None,
        cursor: str | None = None,
        reading_token: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        limit = self.service.continuation_limit(limit, reading_token or cursor, default=20)
        bounded = self.reader.bound_limit(limit)
        if len(query) > 200 or len(hints) > 16 or any(len(hint) > 200 for hint in hints):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        normalized_domains = tuple(normalize_domain(value) for value in domains)
        if any(value is None for value in normalized_domains):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        selected_domains = tuple(sorted(set(str(value) for value in normalized_domains)))
        self.reader.require_search()
        self.service.residency.release_expired_leases()
        if cursor and reading_token:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        warm_page, discovery_facts, consumed_cursor = self._discovery_request(
            kind="links",
            query=query,
            hints=hints,
            domains=selected_domains,
            account_id=account_id,
            conversation_ids=conversation_ids,
            participant_ids=(),
            after=after,
            before=before,
            cursor=cursor,
            reading_token=reading_token,
            limit=bounded,
        )
        if warm_page is not None:
            return warm_page
        if consumed_cursor:
            cursor = None
        with self.repository.database.read_snapshot():
            scope = self._scope(account_id, conversation_ids, (), after, before)
            arguments = {
                "scope": scope.__dict__,
                "query": query,
                "hints": hints,
                "domains": selected_domains,
            }
            snapshot, digest, position = self._snapshot(
                kind="links",
                scope=scope,
                arguments=arguments,
                cursor=cursor,
                discovery_facts=discovery_facts,
            )
            if position is not None and len(position) != 5:
                raise SightglassError(ErrorCode.CURSOR_INVALID)
            start = tuple(position) if position else None
            rows = self.links.link_window(
                account_id=scope.account_id,
                conversation_ids=scope.conversation_ids,
                epoch=scope.epoch,
                watermark=snapshot["observation_watermark"],
                after=scope.after,
                before=scope.before,
                domains=selected_domains,
                position=start,
                limit=LINK_CANDIDATE_BUDGET + 1,  # type: ignore[arg-type]
            )
            items = []
            item_keys = []
            frontier = None
            examined = 0
            for row in rows[:LINK_CANDIDATE_BUDGET]:
                check_operation_budget()
                examined += 1
                frontier = self._link_key(row)
                evidence = hint_evidence(str(row["normalized_host"]), hints)
                text = "\n".join(
                    str(row[key] or "") for key in ("normalized_url", "title", "description")
                )
                if hints and not evidence or query and not self.service._text_matches(text, query):
                    continue
                item = self._link_projection(row)
                item["matched_by"] = list(
                    evidence or (("domain_exact",) if selected_domains else ("link",))
                )
                if (
                    len(json.dumps([*items, item], ensure_ascii=False)) + 8192
                    > self.reader.policy.max_compact_payload_chars
                ):
                    if not items:
                        raise SightglassError(ErrorCode.OUTPUT_BUDGET_EXCEEDED)
                    frontier = self._link_key(rows[examined - 2]) if examined > 1 else None
                    examined -= 1
                    break
                items.append(item)
                item_keys.append(self._link_key(row))
                if len(items) >= bounded:
                    break
            has_more = examined < len(rows)
            scan_partial = examined == LINK_CANDIDATE_BUDGET and has_more
            next_cursor = (
                self._cursor("links", scope, digest, snapshot, list(frontier))
                if (has_more and frontier is not None)
                else None
            )
            result = {
                "schema": "sightglass.link-search.v1",
                "items": items,
                "page": {"next_cursor": next_cursor, "has_more": has_more},
                **self._receipts(
                    scope,
                    snapshot,
                    returned=len(items),
                    examined=examined,
                    budget=LINK_CANDIDATE_BUDGET,
                    has_more=has_more,
                    scan_partial=scan_partial,
                    discovery_facts=discovery_facts,
                ),
            }
            transport = active_response_budget()
            if transport is not None:
                while len(items) > 1 and transport.size(result) > transport.available_bytes:
                    items.pop()
                    item_keys.pop()
                    result["page"] = {
                        "next_cursor": self._cursor("links", scope, digest, snapshot,
                                                    list(item_keys[-1])),
                        "has_more": True, "truncated": True,
                    }
                    result["source_receipt"]["returned_count"] = len(items)
                    result["execution"].update(stop_reason="output_budget",
                                               continuation_available=True)
                if transport.size(result) > transport.available_bytes:
                    # One full authorized URL is indivisible. Never shorten it or skip its item.
                    result["response_budget"] = {"soft_limit_bytes": transport.max_bytes,
                                                 "exception": "single_item"}
            self._bound_payload(result)
            return result

    def _bound_payload(self, result: dict[str, Any]) -> None:
        if (
            len(json.dumps(result, ensure_ascii=False))
            > self.reader.policy.max_compact_payload_chars
        ):
            raise SightglassError(ErrorCode.OUTPUT_BUDGET_EXCEEDED)

    def _context_rows(self, focus: Any, scope: RetrievalScope, watermark: int) -> list[Any]:
        conversation = str(focus["conversation_id"])
        self.reader.authorize(conversation)
        instant = parse_aware_datetime(str(focus["sent_at_utc"]))
        after = to_utc_iso(instant - timedelta(seconds=CONTEXT_RADIUS_SECONDS))
        before = to_utc_iso(instant + timedelta(seconds=CONTEXT_RADIUS_SECONDS))
        after = max(after, scope.after) if scope.after else after
        before = min(before, scope.before) if scope.before else before
        kwargs = {
            "projection_epoch": scope.epoch,
            "observation_watermark": watermark,
            "limit": CONTEXT_RADIUS_MESSAGES,
            "participant_ids": scope.participant_ids,
            "time_after_utc": after,
            "time_before_utc": before,
        }
        key = source_sort_key(focus)
        rows = self.repository.materialized_message_rows(
            conversation, direction="backward", before=key, **kwargs
        )
        rows += [focus]
        rows += self.repository.materialized_message_rows(
            conversation, direction="forward", after=key, **kwargs
        )
        # Dense chats can place adjacent project URLs beyond the body-neighbor limit.
        # Admit bounded link neighbors separately, still under the same canonical view.
        nearby_links = self.links.link_window(
            account_id=scope.account_id,
            conversation_ids=(conversation,),
            epoch=scope.epoch,
            watermark=watermark,
            after=after,
            before=before,
            participant_ids=scope.participant_ids,
            limit=MAX_CONTEXT_MESSAGES,
        )
        priority_ids = {str(focus["message_id"])}
        for message_id in dict.fromkeys(str(link["message_id"]) for link in nearby_links):
            linked = self.repository.materialized_message_rows(
                conversation,
                direction="forward",
                message_id=message_id,
                **kwargs,
            )
            rows += linked
            priority_ids.update(str(item["message_id"]) for item in linked)
        target_id = self._reply_target_id(focus, scope.account_id)
        if isinstance(target_id, str):
            target = self.repository.materialized_message_rows(
                conversation,
                direction="forward",
                message_id=target_id,
                **{**kwargs, "time_after_utc": scope.after, "time_before_utc": scope.before},
            )
            rows += target
            priority_ids.update(str(item["message_id"]) for item in target)
        unique = {str(row["message_id"]): row for row in rows}.values()
        selected = sorted(
            unique,
            key=lambda row: (
                str(row["message_id"]) != str(focus["message_id"]),
                str(row["message_id"]) not in priority_ids,
                abs((parse_aware_datetime(str(row["sent_at_utc"])) - instant).total_seconds()),
                self.service._search_keyset(row),
            ),
        )[:MAX_CONTEXT_MESSAGES]
        return sorted(selected, key=self.service._search_keyset)

    @staticmethod
    def _reply_target_id(row: Any, account_id: str) -> str | None:
        structured = json.loads(str(row["structured_json"]))
        source_id = structured.get("reply_target_source_message_id")
        if isinstance(source_id, str) and source_id:
            return opaque_id("wxmsg", account_id, source_id)
        reply = structured.get("reply")
        target = reply.get("target_message_id") if isinstance(reply, dict) else None
        return target if isinstance(target, str) else None

    def retrieve(
        self,
        *,
        concept: str,
        hints: tuple[str, ...] = (),
        account_id: str | None = None,
        conversation_ids: tuple[str, ...] = (),
        participant_ids: tuple[str, ...] = (),
        kinds: tuple[str, ...] = (),
        count_hint: int | None = None,
        after: str | None = None,
        before: str | None = None,
        cursor: str | None = None,
        reading_token: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        limit = self.service.continuation_limit(limit, reading_token or cursor, default=3)
        bounded = self.reader.bound_limit(limit)
        allowed_kinds = {"message", "link", "image", "file", "voice"}
        if (
            not concept.strip()
            or len(concept) > 500
            or len(hints) > 16
            or any(len(hint) > 200 for hint in hints)
            or not set(kinds) <= allowed_kinds
            or count_hint is not None
            and (type(count_hint) is not int or not 1 <= count_hint <= 128)
        ):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        self.reader.require_search()
        self.service.residency.release_expired_leases()
        if cursor and reading_token:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        warm_page, discovery_facts, consumed_cursor = self._discovery_request(
            kind="retrieval",
            query=concept,
            hints=hints,
            domains=(),
            kinds=tuple(sorted(set(kinds))),
            account_id=account_id,
            conversation_ids=conversation_ids,
            participant_ids=participant_ids,
            after=after,
            before=before,
            cursor=cursor,
            reading_token=reading_token,
            limit=bounded,
        )
        if warm_page is not None:
            return warm_page
        if consumed_cursor:
            cursor = None
        with self.repository.database.read_snapshot():
            scope = self._scope(account_id, conversation_ids, participant_ids, after, before)
            arguments = {
                "scope": scope.__dict__,
                "concept": concept,
                "hints": hints,
                "kinds": sorted(set(kinds)),
                "count_hint": count_hint,
            }
            # Candidate recall must use the same literal terms as final matching;
            # raw quote delimiters are query syntax, not required message bytes.
            lexical_queries = tuple(
                " ".join(self.service._query_parts(query)) for query in (concept, *hints)
            )
            snapshot, digest, position = self._snapshot(
                kind="retrieval",
                scope=scope,
                arguments=arguments,
                cursor=cursor,
                discovery_facts=discovery_facts,
            )
        # Remote recall owns no canonical SQLite transaction. Freeze its candidates in
        # the signed view so pagination never reruns a changing ANN ranking or query.
        if "semantic_ids" not in snapshot:
            semantic = self.service.semantic
            if semantic is not None:
                try:
                    with operation_budget(8.0):
                        candidates = semantic.query(
                            concept,
                            account_id=scope.account_id,
                            conversation_ids=scope.conversation_ids,
                            participant_ids=scope.participant_ids,
                            after=scope.after,
                            before=scope.before,
                            watermark=snapshot["observation_watermark"],
                            epoch=scope.epoch,
                            kinds=kinds,
                        )
                        check_operation_budget()
                except SightglassError as exc:
                    snapshot["semantic_ids"] = []
                    snapshot["semantic_receipt"] = {
                        "state": "degraded", "error": exc.code.value,
                        "coverage": "partial",
                    }
                else:
                    snapshot["semantic_ids"] = list(candidates.message_ids)
                    snapshot["semantic_receipt"] = candidates.receipt
            else:
                snapshot["semantic_ids"] = []
                snapshot["semantic_receipt"] = self.semantic_status()
        with self.repository.database.read_snapshot():
            # Correction, policy or rebuild during remote work cannot publish the
            # captured response as a current view. Ordinary later appends stay excluded.
            self._snapshot(
                kind="retrieval",
                scope=scope,
                arguments=arguments,
                cursor=self._cursor("retrieval", scope, digest, snapshot, position or ["batch"]),
                discovery_facts=discovery_facts,
            )
            if position == ["batch"]:
                position = None
            elif position is not None and (
                len(position) != 4
                or type(position[0]) is not int
                or type(position[1]) is not int
                or not all(isinstance(value, str) for value in position[2:])
            ):
                raise SightglassError(ErrorCode.CURSOR_INVALID)
            watermark = snapshot["observation_watermark"]
            kind_only_fallback = snapshot["semantic_receipt"].get("state") != "ready"
            link_start = tuple(snapshot["link_start"]) if snapshot.get("link_start") else None
            lexical_start = (
                tuple(snapshot["lexical_start"]) if snapshot.get("lexical_start") else None
            )
            link_rows = (
                self.links.link_window(
                    account_id=scope.account_id,
                    conversation_ids=scope.conversation_ids,
                    epoch=scope.epoch,
                    watermark=watermark,
                    after=scope.after,
                    before=scope.before,
                    participant_ids=scope.participant_ids,
                    position=link_start,
                    limit=LINK_CANDIDATE_BUDGET + 1,  # type: ignore[arg-type]
                )
                if not kinds or "link" in kinds
                else []
            )
            lexical_rows = self.repository.search_candidate_window(
                scope.conversation_ids,
                after_key=lexical_start,
                after_utc=scope.after,
                before_utc=scope.before,
                limit=LEXICAL_CANDIDATE_BUDGET + 1,
                participant_ids=scope.participant_ids,
                projection_epoch=scope.epoch,
                observation_watermark=watermark,
                lexical_queries=lexical_queries,
            )  # type: ignore[arg-type]
            consumed_links = min(len(link_rows), LINK_CANDIDATE_BUDGET)
            if len(link_rows) > consumed_links and consumed_links:
                boundary_id = link_rows[consumed_links - 1]["message_id"]
                if link_rows[consumed_links]["message_id"] == boundary_id:
                    while (
                        consumed_links
                        and link_rows[consumed_links - 1]["message_id"] == boundary_id
                    ):
                        consumed_links -= 1
            focus: dict[str, Any] = {}
            evidence: dict[str, set[str]] = {}
            ranks: dict[str, float] = {}
            semantic_ranks: dict[str, float] = {}

            def admit(row: Any, matched: tuple[str, ...], rank: int) -> None:
                message_id = str(row["message_id"])
                if not matched:
                    return
                focus[message_id] = row
                evidence.setdefault(message_id, set()).update(matched)
                contribution = 1 / (60 + rank)
                if matched == ("semantic",):
                    semantic_ranks[message_id] = contribution
                else:
                    ranks[message_id] = ranks.get(message_id, 0) + contribution

            structured_ids: dict[str, tuple[tuple[str, ...], int]] = {}
            for rank, row in enumerate(link_rows[:consumed_links], 1):
                matched = hint_evidence(str(row["normalized_host"]), hints)
                text = "\n".join(
                    str(row[key] or "") for key in ("normalized_url", "title", "description")
                )
                if self.service._text_matches(text, concept):
                    matched += ("lexical",)
                if not hints and "link" in kinds and kind_only_fallback:
                    matched += ("link_kind",)
                if matched:
                    structured_ids.setdefault(str(row["message_id"]), (matched, rank))
            for row in self.repository.frozen_message_rows(tuple(structured_ids)):
                matched, rank = structured_ids[str(row["message_id"])]
                admit(row, matched, rank)
            semantic_ids = (
                () if snapshot.get("semantic_consumed") else tuple(snapshot["semantic_ids"])
            )
            semantic_rows = {
                str(row["message_id"]): row
                for row in self.repository.frozen_message_rows(semantic_ids)
            }
            for rank, semantic_id in enumerate(semantic_ids, 1):
                row = semantic_rows.get(semantic_id)
                if row is None:
                    continue
                # The canonical store remains authoritative even for cached signed ANN
                # candidates. Hard predicates are reapplied in this final read view.
                if (
                    row["account_id"] == scope.account_id
                    and row["conversation_id"] in scope.conversation_ids
                    and self.reader.policy.permits(str(row["conversation_id"]))
                    and row["projection_epoch"] == scope.epoch
                    and row["current_state"] == "present"
                    and row["first_observation_seq"] is not None
                    and row["current_observation_seq"] is not None
                    and row["current_observation_seq"] <= watermark
                    and (not scope.participant_ids or row["sender_id"] in scope.participant_ids)
                    and (scope.after is None or row["sent_at_utc"] >= scope.after)
                    and (scope.before is None or row["sent_at_utc"] < scope.before)
                ):
                    self.reader.authorize(str(row["conversation_id"]))
                    admit(row, ("semantic",), rank)
            lexical_links: dict[str, list[Any]] = {}
            for link in self.links.links_for_messages(
                tuple(str(row["message_id"]) for row in lexical_rows)
            ):
                lexical_links.setdefault(str(link["message_id"]), []).append(link)
            for rank, row in enumerate(lexical_rows[:LEXICAL_CANDIDATE_BUDGET], 1):
                check_operation_budget()
                if (
                    row["projection_epoch"] != scope.epoch
                    or row["first_observation_seq"] is None
                    or row["current_observation_seq"] is None
                    or row["first_observation_seq"] > watermark
                    or row["current_observation_seq"] > watermark
                    or scope.participant_ids
                    and row["sender_id"] not in scope.participant_ids
                ):
                    continue
                owns_structured_match = (
                    any(
                        hint_evidence(str(link["normalized_host"]), hints)
                        or self.service._text_matches(
                            "\n".join(
                                str(link[field] or "")
                                for field in ("normalized_url", "title", "description")
                            ),
                            concept,
                        )
                        or not hints
                        and "link" in kinds
                        and kind_only_fallback
                        for link in lexical_links.get(str(row["message_id"]), ())
                    )
                    if not kinds or "link" in kinds
                    else False
                )
                if owns_structured_match and str(row["message_id"]) not in structured_ids:
                    continue
                text = current_search_document(row)
                if self.service._text_matches(text, concept) or any(
                    self.service._text_matches(text, hint) for hint in hints
                ):
                    admit(row, ("lexical",), rank)
                elif (
                    not hints
                    and kinds
                    and kind_only_fallback
                    and any(value in kinds for value in (str(row["kind"]), "message"))
                ):
                    admit(row, ("message_kind",), rank)
            if kinds and "message" not in kinds:
                linked = {
                    str(link["message_id"]) for link in self.links.links_for_messages(tuple(focus))
                }
                focus = {
                    key: row
                    for key, row in focus.items()
                    if row["kind"] in kinds or "link" in kinds and key in linked
                }
            contexts: list[dict[str, Any]] = []
            for message_id, row in focus.items():
                rows = self._context_rows(row, scope, watermark)
                ids = {str(item["message_id"]) for item in rows}
                overlapping = [
                    context
                    for context in contexts
                    if context["conversation_id"] == row["conversation_id"]
                    and context["ids"] & ids
                    and len(context["ids"] | ids) <= MAX_CONTEXT_MESSAGES
                ]
                if overlapping:
                    context = overlapping[0]
                    context["rows"].update({str(item["message_id"]): item for item in rows})
                    context["ids"].update(ids)
                    context["focus"].add(message_id)
                    for other in overlapping[1:]:
                        if len(context["ids"] | other["ids"]) > MAX_CONTEXT_MESSAGES:
                            continue
                        context["rows"].update(other["rows"])
                        context["ids"].update(other["ids"])
                        context["focus"].update(other["focus"])
                        contexts.remove(other)
                else:
                    contexts.append(
                        {
                            "conversation_id": str(row["conversation_id"]),
                            "rows": {str(item["message_id"]): item for item in rows},
                            "ids": ids,
                            "focus": {message_id},
                        }
                    )
            ranked = []
            for context in contexts:
                rows = sorted(context["rows"].values(), key=self.service._search_keyset)
                links = self.links.links_for_messages(tuple(context["ids"]))
                unique_links = {str(link["normalized_url"]) for link in links}
                matched = set().union(*(evidence[value] for value in context["focus"]))
                # A context receives its best ANN rank once. More weak semantic
                # candidates in a long discussion are not additional relevance.
                score = sum(ranks.get(value, 0) for value in context["focus"]) + max(
                    (semantic_ranks.get(value, 0) for value in context["focus"]), default=0
                )
                preference = 0.0
                if len(unique_links) >= 2:
                    matched.add("link_cooccurrence")
                    preference += 0.01
                if count_hint is not None:
                    preference += 0.01 / (1 + abs(len(unique_links) - count_hint))
                if len({item["sender_id"] for item in rows}) == 1:
                    matched.add("sender_continuity")
                reply_edges = [
                    {"from_message_id": str(item["message_id"]), "to_message_id": target}
                    for item in rows
                    if (target := self._reply_target_id(item, scope.account_id)) in context["ids"]
                ]
                if reply_edges:
                    matched.add("reply_edge")
                    preference += 0.01
                identifier = opaque_id(
                    "wxcontext",
                    scope.account_id,
                    context["conversation_id"],
                    *sorted(context["focus"]),
                )
                context.update(
                    rows_ordered=rows,
                    links=links,
                    matched=matched,
                    context_id=identifier,
                    score=round(score * 1_000_000),
                    reply_edges=reply_edges,
                    key=(
                        round(score * 1_000_000), round(preference * 1_000_000),
                        str(rows[-1]["sent_at_utc"]), identifier,
                    ),
                )
                if not position or context["key"] < tuple(position):
                    ranked.append(context)
            ranked.sort(key=lambda value: value["key"], reverse=True)
            selected = []
            row_count = 0
            for context in ranked[:bounded]:
                size = len(context["rows_ordered"])
                if selected and row_count + size > self.reader.policy.max_compact_messages_per_call:
                    break
                if size > self.reader.policy.max_compact_messages_per_call:
                    # Keep every focus row; narrow only proximity expansion under the row budget.
                    focus_rows = [
                        row
                        for row in context["rows_ordered"]
                        if row["message_id"] in context["focus"]
                    ]
                    if len(focus_rows) > self.reader.policy.max_compact_messages_per_call:
                        raise SightglassError(ErrorCode.OUTPUT_BUDGET_EXCEEDED)
                    context["rows_ordered"] = focus_rows
                    context["links"] = [
                        link for link in context["links"] if link["message_id"] in context["focus"]
                    ]
                    size = len(focus_rows)
                selected.append(context)
                row_count += size
            output = []
            account = self.repository.account_row(scope.account_id)
            assert account is not None
            for context in selected:
                rows = context["rows_ordered"]
                focus_ids = frozenset(context["focus"])
                compact = self.service.compact_projector.render(
                    self.service.compact_projector.prepare(
                        rows,
                        timezone_name=self.service._reader_timezone(account),
                        include_resource_indicators=self.reader.policy.resource_metadata,
                        focus_message_ids=focus_ids,
                        context_only_ids=frozenset(context["ids"] - focus_ids),
                        late_arrival_ids=frozenset(),
                    )
                )
                hydrated_links = []
                for link in context["links"][:MAX_CONTEXT_LINKS]:
                    message = context["rows"][str(link["message_id"])]
                    conversation = self.repository.conversation_row(context["conversation_id"])
                    assert conversation is not None
                    joined = {
                        **dict(link),
                        **{
                            key: message[key]
                            for key in (
                                "account_id",
                                "conversation_id",
                                "sender_id",
                                "sender_membership_id",
                                "sent_at_utc",
                                "sort_seq",
                                "sort_tie",
                            )
                        },
                        "conversation_title": conversation["current_title"],
                        "conversation_kind": conversation["kind"],
                    }
                    hydrated_links.append(self._link_projection(joined))
                resource_ids = tuple(context["ids"])
                resources = (
                    self.repository.resources_for_messages_bulk(resource_ids)
                    if (self.reader.policy.resource_metadata)
                    else {}
                )
                output.append(
                    {
                        "context_id": context["context_id"],
                        "conversation_id": context["conversation_id"],
                        "time_range": {
                            "after": str(rows[0]["sent_at_utc"]),
                            "before": str(rows[-1]["sent_at_utc"]),
                        },
                        "matched_by": sorted(context["matched"]),
                        "focus_message_ids": sorted(focus_ids),
                        "reply_edges": context["reply_edges"],
                        **compact,
                        "links": hydrated_links,
                        "link_projection": {
                            "complete": len(context["links"]) <= MAX_CONTEXT_LINKS,
                            "total_in_context": len(context["links"]),
                            "returned": len(hydrated_links),
                            "continuation_tool": "wechat_find_links"
                            if len(context["links"]) > MAX_CONTEXT_LINKS
                            else None,
                        },
                        "resources": [
                            {
                                "message_id": key,
                                "resources": [
                                    {
                                        field: value[field]
                                        for field in (
                                            "resource_id",
                                            "kind",
                                            "mime_type",
                                            "availability",
                                        )
                                    }
                                    for value in values
                                ],
                            }
                            for key, values in resources.items()
                            if values
                        ],
                        "context_anchor": self.service.token_codec.encode(
                            {
                                "schema": 1,
                                "account_id": scope.account_id,
                                "conversation_id": context["conversation_id"],
                                "message_id": str(rows[0]["message_id"]),
                                "sort": [
                                    str(rows[0]["sort_primary"]),
                                    int(rows[0]["sort_seq"]),
                                    int(rows[0]["sort_tie"]),
                                    str(rows[0]["message_id"]),
                                ],
                            }
                        ),
                        "topology": "same_conversation_proximity_not_topic_identity",
                    }
                )
            more_results = len(ranked) > len(selected)
            more_links = len(link_rows) > consumed_links
            more_lexical = len(lexical_rows) > LEXICAL_CANDIDATE_BUDGET
            has_more = more_results or more_links or more_lexical
            next_cursor = None
            if more_results:
                next_cursor = self._cursor(
                    "retrieval", scope, digest, snapshot, list(selected[-1]["key"])
                )
            elif more_links or more_lexical:
                continuation = {
                    **snapshot,
                    "semantic_consumed": True,
                    "link_start": list(self._link_key(link_rows[consumed_links - 1]))
                    if link_rows
                    else snapshot.get("link_start"),
                    "lexical_start": list(
                        self.service._search_keyset(
                            lexical_rows[min(len(lexical_rows), LEXICAL_CANDIDATE_BUDGET) - 1]
                        )
                    )
                    if lexical_rows
                    else snapshot.get("lexical_start"),
                }
                next_cursor = self._cursor("retrieval", scope, digest, continuation, ["batch"])
            result = {
                "schema": "sightglass.retrieval-results.v1",
                "concept": concept,
                "contexts": output,
                "page": {"next_cursor": next_cursor, "has_more": has_more},
                "lanes": {
                    "structured": "ready",
                    "lexical": "trigram_candidates"
                    if all(candidate_expression(query) for query in lexical_queries)
                    and self._lexical_ready()
                    else "bounded_literal_scan",
                    "semantic": snapshot["semantic_receipt"].get("state", "degraded"),
                    "ranking_scope": "candidate_batch",
                },
                **self._receipts(
                    scope,
                    snapshot,
                    returned=len(output),
                    examined=consumed_links + min(len(lexical_rows), LEXICAL_CANDIDATE_BUDGET),
                    budget=LINK_CANDIDATE_BUDGET + LEXICAL_CANDIDATE_BUDGET,
                    has_more=has_more,
                    scan_partial=more_links or more_lexical,
                    discovery_facts=discovery_facts,
                ),
                "lexical_index_receipt": {
                    "kind": "lexical",
                    "generation": snapshot["lexical_generation"],
                    "recipe": LEXICAL_RECIPE,
                    "coverage": "complete" if self._lexical_ready() else "partial",
                    "live_validation": False,
                },
                "semantic_index_receipt": snapshot["semantic_receipt"],
            }
            original_output = json.loads(json.dumps(output, ensure_ascii=False))
            semantic_receipt = snapshot["semantic_receipt"]
            if semantic_receipt.get("state") not in {"ready", "disabled"}:
                result["source_receipt"]["warnings"].append("semantic_unavailable")
            if semantic_receipt.get("coverage") == "partial":
                result["source_receipt"]["warnings"].append("semantic_index_partial")
            while output:
                for index in range(len(output)):
                    output[index] = json.loads(
                        json.dumps(original_output[index], ensure_ascii=False)
                    )
                fixed_result = {**result, "contexts": []}
                overhead = (
                    len(json.dumps(fixed_result, ensure_ascii=False, separators=(",", ":"))) + 4096
                )
                context_budget = (self.reader.policy.max_compact_payload_chars - overhead) // len(
                    output
                )
                transport = active_response_budget()
                context_bytes = None
                if transport is not None:
                    context_bytes = max(0, (transport.available_bytes
                                           - transport.size(fixed_result)) // len(output))
                try:
                    for context in output:
                        CompactBodyBudgetAllocator(
                            max_payload_chars=context_budget,
                            max_body_chars=self.reader.policy.max_compact_body_chars_per_message,
                            max_payload_bytes=context_bytes,
                        ).apply(context)
                    break
                except SightglassError as exc:
                    if (transport is not None and len(output) == 1
                            and exc.code == ErrorCode.OUTPUT_BUDGET_EXCEEDED
                            and exc.details.get("reason") == "fixed_envelope"):
                        # A single context's authorized URLs/IDs cannot be cut to
                        # satisfy a soft target. Keep the existing hard char bound.
                        output[0] = json.loads(json.dumps(original_output[0], ensure_ascii=False))
                        with response_budget(None):
                            CompactBodyBudgetAllocator(
                                max_payload_chars=context_budget,
                                max_body_chars=self.reader.policy.max_compact_body_chars_per_message,
                            ).apply(output[0])
                        result["response_budget"] = {
                            "soft_limit_bytes": transport.max_bytes,
                            "exception": "single_context",
                        }
                        break
                    if exc.code != ErrorCode.OUTPUT_BUDGET_EXCEEDED or len(output) == 1:
                        raise
                    output.pop()
                    selected.pop()
                    result["page"] = {
                        "next_cursor": self._cursor(
                            "retrieval", scope, digest, snapshot, list(selected[-1]["key"])
                        ),
                        "has_more": True,
                        "truncated": True,
                    }
                    result["execution"].update(
                        stop_reason="output_budget", continuation_available=True
                    )
                    result["source_receipt"]["returned_count"] = len(output)
            self._bound_payload(result)
            return result

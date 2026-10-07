from __future__ import annotations

import hashlib
import heapq
import json
import sqlite3
from contextlib import ExitStack, closing
from itertools import islice
from typing import Any
from urllib.parse import urlsplit

from sightglass.contracts.common import SourceSortKey
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.identity import (
    LabelObservation,
    SourceAccount,
    SourceConversation,
    SourceParticipant,
    SourceParticipantFilter,
)
from sightglass.contracts.messages import ParsedMessage, SourceMessage
from sightglass.contracts.resources import SourceResource
from sightglass.operations import check_operation_budget
from sightglass.source.identity import opaque_id
from sightglass.source.parser import PARSER_VERSION

from .coverage import containing_window, position
from .current_body import stored_search_text, stored_structured_json
from .db import WindowDB
from .lexical import LEXICAL_RECIPE, candidate_expression
from .links import prepare_links, publish_links
from .observation_codec import (
    ObservationCodecError,
    encode_observation,
    observation_payload_state,
)
from .resident_read import resident_body_predicate

_PRINCIPAL_KEY_KINDS = {"internal_username"}
_CONVERSATION_KEY_KINDS = {"conversation_sender_id", "source_membership_id"}


def normalize_label(value: str) -> str:
    return " ".join(str(value).casefold().split())


def resource_binding_fingerprint(resource: SourceResource) -> str:
    if resource.declared_hash:
        evidence: dict[str, Any] = {
            "declared_hash": resource.declared_hash.casefold(),
            "declared_size": resource.declared_size,
            "kind": resource.kind,
        }
    else:
        evidence = {
            "kind": resource.kind,
            "mime_type": resource.mime_type,
            "original_name": resource.original_name,
            "declared_size": resource.declared_size,
        }
    return hashlib.sha256(
        json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def message_search_fields(parsed: ParsedMessage) -> dict[str, str]:
    fields: dict[str, str] = {}
    if parsed.text:
        fields["text"] = parsed.text
    link = parsed.structured.get("link")
    if isinstance(link, dict):
        for key in ("title", "description", "source_name", "host", "path"):
            value = link.get(key)
            if isinstance(value, str) and value:
                fields[f"link.{key}"] = value
        url = link.get("raw_url")
        if isinstance(url, str) and url:
            try:
                parsed_url = urlsplit(url)
            except ValueError:
                parsed_url = None
            if parsed_url is not None:
                if parsed_url.hostname and "link.host" not in fields:
                    fields["link.host"] = parsed_url.hostname.casefold()
                if parsed_url.path and "link.path" not in fields:
                    fields["link.path"] = parsed_url.path
    return fields


def message_search_text(parsed: ParsedMessage) -> str | None:
    rendered = "\n".join(dict.fromkeys(message_search_fields(parsed).values()))
    return rendered or None


def lexical_index_covers_residents(
    connection: sqlite3.Connection,
    conversation_ids: tuple[str, ...],
    boundary: str,
    values: tuple[Any, ...],
) -> bool:
    """Bounded probe: does every in-scope resident carry a current lexical receipt?

    ``boundary``/``values`` are the same scope clauses the candidate seek uses
    (epoch/watermark/participant/time), so the probe answers exactly over the
    rows a narrowed recall would consider. It stops at the first uncovered
    resident, so a fully covered resident set pays one bounded index walk.

    A resident is uncovered when any of these holds, each of which would make a
    ``MATCH`` prefilter silently drop it: no observation episode
    (``current_observation_seq`` is NULL); no receipt, an outdated recipe, or a
    receipt trailing the observation version (``source_observation_seq``
    mismatch); or a current receipt with no ``message_lexical`` row, which a
    supported lifecycle never creates but which must still fall back rather than
    lose a hit. The probe is driven by ``message_resident_timeline`` so it never
    scans a released skeleton history.
    """

    for conversation_id in dict.fromkeys(conversation_ids):
        probe = connection.execute(
            f"""
            SELECT 1 FROM messages AS messages
                INDEXED BY message_resident_timeline
            WHERE messages.conversation_id = ?
              AND messages.current_state='present'
              AND {resident_body_predicate('messages')} {boundary}
              AND (messages.current_observation_seq IS NULL
                   OR NOT EXISTS (
                       SELECT 1 FROM message_lexical_projection AS lexical_projection
                       WHERE lexical_projection.message_id=messages.message_id
                         AND lexical_projection.recipe=?
                         AND lexical_projection.source_observation_seq=
                             messages.current_observation_seq)
                   OR NOT EXISTS (
                       SELECT 1 FROM message_lexical
                       WHERE message_lexical.rowid = messages.rowid))
            LIMIT 1
            """,
            (conversation_id, *values, LEXICAL_RECIPE),
        ).fetchone()
        if probe is not None:
            return False
    return True


class WindowRepository:
    def __init__(self, database: WindowDB) -> None:
        self.database = database

    @staticmethod
    def account_id_for(source_account_key: str) -> str:
        """Derive the account id without touching window.db."""

        return opaque_id("wxacct", source_account_key)

    @staticmethod
    def conversation_id_for(account_id: str, source_conversation_id: str) -> str:
        """Derive the conversation id without touching window.db."""

        return opaque_id("wxconv", account_id, source_conversation_id)

    def upsert_account(self, source: SourceAccount, observed_at: str) -> str:
        account_id = self.account_id_for(source.source_account_key)
        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO accounts(
                    account_id, source_namespace, source_account_key,
                    identity_confidence, reader_timezone, current_display_name,
                    first_seen_at, last_seen_at, active, account_binding_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    source_namespace = excluded.source_namespace,
                    identity_confidence = excluded.identity_confidence,
                    reader_timezone = excluded.reader_timezone,
                    current_display_name = excluded.current_display_name,
                    account_binding_id = COALESCE(
                        excluded.account_binding_id,
                        accounts.account_binding_id
                    ),
                    last_seen_at = excluded.last_seen_at,
                    active = 1
                """,
                (
                    account_id,
                    source.source_namespace,
                    source.source_account_key,
                    source.identity_confidence,
                    source.reader_timezone,
                    source.display_name,
                    observed_at,
                    observed_at,
                    source.account_binding_id,
                ),
            )
            self_participant_id = opaque_id(
                "wxperson",
                account_id,
                "principal",
                "internal_username",
                source.self_principal_key,
            )
            existing_self_key = connection.execute(
                """
                SELECT psk.key_value
                FROM participants AS p
                LEFT JOIN participant_source_keys AS psk
                  ON psk.participant_id = p.participant_id
                 AND psk.key_kind = 'internal_username'
                 AND psk.principal_eligible = 1
                 AND psk.active = 1
                WHERE p.account_id = ? AND p.is_self = 1
                """,
                (account_id,),
            ).fetchone()
            if existing_self_key is not None and str(existing_self_key[0] or "") != str(
                source.self_principal_key
            ):
                raise SightglassError(
                    ErrorCode.SOURCE_INCOMPLETE,
                    details={"warning_codes": ["account_self_identity_conflict"]},
                )
            connection.execute(
                """
                INSERT INTO participants(
                    participant_id, account_id, current_reader_label,
                    is_self, actor_kind, resolution_state, identity_confidence,
                    first_seen_at, last_seen_at
                ) VALUES (?, ?, NULL, 1, 'person', 'stable', ?, ?, ?)
                ON CONFLICT(participant_id) DO UPDATE SET
                    is_self = 1,
                    resolution_state = 'stable',
                    identity_confidence = excluded.identity_confidence,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    self_participant_id,
                    account_id,
                    source.identity_confidence,
                    observed_at,
                    observed_at,
                ),
            )
            self_key_id = opaque_id(
                "wxsourcekey",
                account_id,
                "internal_username",
                source.self_principal_key,
                "global",
            )
            connection.execute(
                """
                INSERT INTO participant_source_keys(
                    source_key_id, participant_id, account_id, key_kind,
                    key_value, scope_conversation_id, stability,
                    principal_eligible, provenance, first_observed_at,
                    last_observed_at, active
                ) VALUES (?, ?, ?, 'internal_username', ?, NULL, 'stable', 1,
                          'synthetic.account.self_principal_key', ?, ?, 1)
                ON CONFLICT(source_key_id) DO UPDATE SET
                    last_observed_at = excluded.last_observed_at,
                    active = 1
                """,
                (
                    self_key_id,
                    self_participant_id,
                    account_id,
                    source.self_principal_key,
                    observed_at,
                    observed_at,
                ),
            )
        return account_id

    def upsert_reader(
        self,
        reader_id: str,
        display_name: str,
        policy: dict[str, Any],
        observed_at: str,
        *,
        auth_token_hash: str | None = None,
    ) -> None:
        policy_json = json.dumps(policy, sort_keys=True)
        with self.database.transaction() as connection:
            previous = connection.execute(
                """
                SELECT policy_json, policy_revision
                FROM reader_profiles WHERE reader_id = ?
                """,
                (reader_id,),
            ).fetchone()
            if previous is not None and str(previous["policy_json"]) != policy_json:
                connection.execute(
                    """
                    UPDATE reader_deliveries
                    SET status = 'expired', expires_at = ?
                    WHERE reader_id = ? AND status = 'pending'
                    """,
                    (observed_at, reader_id),
                )
            policy_revision = (
                1
                if previous is None
                else int(previous["policy_revision"])
                + int(str(previous["policy_json"]) != policy_json)
            )
            connection.execute(
                """
                INSERT INTO reader_profiles(
                    reader_id, display_name, auth_token_hash, policy_json,
                    active, created_at, updated_at, policy_revision
                ) VALUES (?, ?, ?, ?, 1, ?, ?, ?)
                ON CONFLICT(reader_id) DO UPDATE SET
                    display_name = excluded.display_name,
                    auth_token_hash = COALESCE(
                        excluded.auth_token_hash,
                        reader_profiles.auth_token_hash
                    ),
                    policy_json = excluded.policy_json,
                    policy_revision = excluded.policy_revision,
                    active = 1,
                    updated_at = excluded.updated_at
                """,
                (
                    reader_id,
                    display_name,
                    auth_token_hash,
                    policy_json,
                    observed_at,
                    observed_at,
                    policy_revision,
                ),
            )

    def upsert_conversation(
        self, account_id: str, source: SourceConversation, observed_at: str
    ) -> str:
        conversation_id = self.conversation_id_for(account_id, source.source_conversation_id)
        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO conversations(
                    conversation_id, account_id, source_conversation_id,
                    kind, current_title, first_seen_at, last_seen_at,
                    last_message_at, visibility_state, roster_complete,
                    catalog_state, unread_count, catalog_observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?)
                ON CONFLICT(conversation_id) DO UPDATE SET
                    kind = excluded.kind,
                    current_title = excluded.current_title,
                    last_seen_at = excluded.last_seen_at,
                    last_message_at = excluded.last_message_at,
                    visibility_state = 'active',
                    roster_complete = excluded.roster_complete,
                    catalog_state = excluded.catalog_state,
                    unread_count = excluded.unread_count,
                    catalog_observed_at = excluded.catalog_observed_at
                """,
                (
                    conversation_id,
                    account_id,
                    source.source_conversation_id,
                    source.kind,
                    source.title,
                    observed_at,
                    observed_at,
                    source.last_message_at_utc,
                    int(source.roster_complete),
                    source.catalog_state,
                    max(0, int(source.unread_count)),
                    observed_at,
                ),
            )
            active_aliases = {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT alias FROM conversation_aliases
                    WHERE conversation_id = ? AND active = 1
                    """,
                    (conversation_id,),
                )
            }
            for alias in source.aliases:
                if alias in active_aliases:
                    continue
                alias_id = opaque_id("wxconvalias", conversation_id, alias, observed_at)
                connection.execute(
                    """
                    INSERT INTO conversation_aliases(
                        conversation_alias_id, conversation_id, alias,
                        normalized_alias, alias_kind, valid_from, valid_to,
                        source, active
                    ) VALUES (?, ?, ?, ?, 'source_alias', ?, NULL, 'synthetic.catalog', 1)
                    """,
                    (alias_id, conversation_id, alias, normalize_label(alias), observed_at),
                )
        return conversation_id

    @staticmethod
    def _source_key_scope(
        conversation_id: str, participant: SourceParticipant, key_index: int
    ) -> str | None:
        key = participant.identity_keys[key_index]
        return None if key.principal_eligible else conversation_id

    @staticmethod
    def _validate_identity_keys(participant: SourceParticipant) -> None:
        for key in participant.identity_keys:
            valid_principal = (
                key.principal_eligible
                and key.kind in _PRINCIPAL_KEY_KINDS
                and key.stability == "stable"
                and key.scope_conversation_source_id is None
            )
            valid_conversation = (
                not key.principal_eligible
                and key.kind in _CONVERSATION_KEY_KINDS
                and key.stability == "conversation_local"
                and key.scope_conversation_source_id == participant.source_conversation_id
            )
            if not key.value or not (valid_principal or valid_conversation):
                raise SightglassError(
                    ErrorCode.SOURCE_MESSAGE_DECODE_FAILED,
                    details={"warning_codes": ["identity_key_invalid"]},
                )

    def _existing_participant_ids(
        self,
        connection: sqlite3.Connection,
        account_id: str,
        conversation_id: str,
        participant: SourceParticipant,
    ) -> set[str]:
        found: set[str] = set()
        for index, key in enumerate(participant.identity_keys):
            scope = self._source_key_scope(conversation_id, participant, index)
            if scope is None:
                row = connection.execute(
                    """
                    SELECT participant_id FROM participant_source_keys
                    WHERE account_id = ? AND key_kind = ? AND key_value = ?
                      AND scope_conversation_id IS NULL AND active = 1
                    """,
                    (account_id, key.kind, key.value),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT participant_id FROM participant_source_keys
                    WHERE account_id = ? AND key_kind = ? AND key_value = ?
                      AND scope_conversation_id = ? AND active = 1
                    """,
                    (account_id, key.kind, key.value, scope),
                ).fetchone()
            if row is not None:
                found.add(str(row[0]))
        return found

    @staticmethod
    def _participant_identity_seed(
        conversation_id: str, participant: SourceParticipant
    ) -> tuple[object, ...]:
        eligible = [
            (key.kind, key.value) for key in participant.identity_keys if key.principal_eligible
        ]
        if eligible:
            return ("principal", *sorted(eligible)[0])
        scoped = [(key.kind, key.value) for key in participant.identity_keys]
        if scoped:
            return ("conversation", conversation_id, *sorted(scoped)[0])
        evidence_message = next(
            (
                label.observed_source_message_id
                for label in participant.labels
                if label.observed_source_message_id
            ),
            None,
        )
        return (
            "unresolved",
            conversation_id,
            participant.source_membership_id or evidence_message or "anonymous",
        )

    def index_participant(
        self,
        account_id: str,
        conversation_id: str,
        participant: SourceParticipant,
        observed_at: str,
    ) -> tuple[str, str]:
        self._validate_identity_keys(participant)
        with self.database.transaction() as connection:
            existing = self._existing_participant_ids(
                connection, account_id, conversation_id, participant
            )
            if len(existing) > 1:
                raise SightglassError(
                    ErrorCode.SOURCE_MESSAGE_DECODE_FAILED,
                    details={"warning_codes": ["identity_key_conflict"]},
                )
            participant_id = (
                next(iter(existing))
                if existing
                else opaque_id(
                    "wxperson",
                    account_id,
                    *self._participant_identity_seed(conversation_id, participant),
                )
            )
            connection.execute(
                """
                INSERT INTO participants(
                    participant_id, account_id, current_reader_label,
                    is_self, actor_kind, resolution_state, identity_confidence,
                    first_seen_at, last_seen_at
                ) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(participant_id) DO UPDATE SET
                    is_self = MAX(is_self, excluded.is_self),
                    actor_kind = excluded.actor_kind,
                    resolution_state = excluded.resolution_state,
                    identity_confidence = excluded.identity_confidence,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    participant_id,
                    account_id,
                    int(participant.is_self),
                    participant.actor_kind,
                    participant.resolution_state,
                    participant.identity_confidence,
                    observed_at,
                    observed_at,
                ),
            )
            for index, key in enumerate(participant.identity_keys):
                scope = self._source_key_scope(conversation_id, participant, index)
                key_id = opaque_id(
                    "wxsourcekey",
                    account_id,
                    key.kind,
                    key.value,
                    scope or "global",
                )
                connection.execute(
                    """
                    INSERT INTO participant_source_keys(
                        source_key_id, participant_id, account_id, key_kind,
                        key_value, scope_conversation_id, stability,
                        principal_eligible, provenance, first_observed_at,
                        last_observed_at, active
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                    ON CONFLICT(source_key_id) DO UPDATE SET
                        last_observed_at = excluded.last_observed_at,
                        active = 1
                    """,
                    (
                        key_id,
                        participant_id,
                        account_id,
                        key.kind,
                        key.value,
                        scope,
                        key.stability,
                        int(key.principal_eligible),
                        key.provenance,
                        observed_at,
                        observed_at,
                    ),
                )
            membership_id = opaque_id("wxmember", conversation_id, participant_id)
            connection.execute(
                """
                INSERT INTO conversation_members(
                    membership_id, conversation_id, participant_id,
                    source_membership_id, current_group_alias, resolution_state,
                    first_seen_at, last_seen_at, last_message_at
                ) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?)
                ON CONFLICT(membership_id) DO UPDATE SET
                    source_membership_id = COALESCE(
                        excluded.source_membership_id,
                        conversation_members.source_membership_id
                    ),
                    resolution_state = excluded.resolution_state,
                    last_seen_at = excluded.last_seen_at,
                    last_message_at = COALESCE(
                        excluded.last_message_at,
                        conversation_members.last_message_at
                    )
                """,
                (
                    membership_id,
                    conversation_id,
                    participant_id,
                    participant.source_membership_id,
                    participant.resolution_state,
                    observed_at,
                    observed_at,
                    participant.last_spoke_at_utc,
                ),
            )
            for label in participant.labels:
                observed_message_id = (
                    opaque_id("wxmsg", account_id, label.observed_source_message_id)
                    if label.observed_source_message_id
                    else None
                )
                if label.scope == "conversation":
                    self._observe_member_label(
                        connection,
                        membership_id,
                        label,
                        observed_message_id=observed_message_id,
                    )
                elif label.scope == "message-surface":
                    self._observe_participant_label(
                        connection,
                        participant_id,
                        label,
                        observed_message_id=observed_message_id,
                    )
                    self._observe_member_label(
                        connection,
                        membership_id,
                        label,
                        observed_message_id=observed_message_id,
                    )
                else:
                    self._observe_participant_label(
                        connection,
                        participant_id,
                        label,
                        observed_message_id=observed_message_id,
                    )
            if participant.account_labels_complete:
                present_account_kinds = {
                    label.label_kind for label in participant.labels if label.scope == "account"
                }
                for label_kind in (
                    "contact_remark",
                    "account_nickname",
                    "public_handle",
                ):
                    if label_kind in present_account_kinds:
                        continue
                    connection.execute(
                        """
                        UPDATE participant_labels
                        SET active = 0, valid_to = ?
                        WHERE participant_id = ? AND label_kind = ?
                          AND scope_kind = 'account' AND active = 1
                        """,
                        (observed_at, participant_id, label_kind),
                    )
            if participant.membership_labels_complete and not any(
                label.scope == "conversation" and label.label_kind == "group_card"
                for label in participant.labels
            ):
                connection.execute(
                    """
                    UPDATE conversation_member_labels
                    SET active = 0, valid_to = ?
                    WHERE membership_id = ? AND label_kind = 'group_card' AND active = 1
                    """,
                    (observed_at, membership_id),
                )
                active_alias = connection.execute(
                    """
                    SELECT label FROM conversation_member_labels
                    WHERE membership_id = ? AND label_kind = 'sightglass_alias' AND active = 1
                    ORDER BY observed_at DESC LIMIT 1
                    """,
                    (membership_id,),
                ).fetchone()
                connection.execute(
                    """
                    UPDATE conversation_members SET current_group_alias = ?
                    WHERE membership_id = ?
                    """,
                    (active_alias[0] if active_alias is not None else None, membership_id),
                )
        return participant_id, membership_id

    @staticmethod
    def _observe_participant_label(
        connection: sqlite3.Connection,
        participant_id: str,
        label: LabelObservation,
        *,
        observed_message_id: str | None,
    ) -> None:
        normalized = normalize_label(label.label)
        if label.label_kind != "message_surface":
            connection.execute(
                """
                UPDATE participant_labels
                SET active = 0, valid_to = ?
                WHERE participant_id = ? AND label_kind = ? AND scope_kind = ?
                  AND active = 1 AND normalized_label != ?
                """,
                (
                    label.observed_at_utc,
                    participant_id,
                    label.label_kind,
                    label.scope,
                    normalized,
                ),
            )
            # Same fact re-observed: refresh freshness in place instead of appending
            # a duplicate interval. A genuine value change (or an operator alias) has
            # a different normalized label or provenance and falls through to a new
            # row, which is what preserves A->B->A history.
            if observed_message_id is None:
                refreshed = connection.execute(
                    """
                    UPDATE participant_labels
                    SET label = ?,
                        observed_at = MAX(observed_at, ?)
                    WHERE participant_id = ? AND label_kind = ? AND scope_kind = ?
                      AND normalized_label = ? AND provenance = ?
                      AND observed_message_id IS NULL AND active = 1
                    """,
                    (
                        label.label,
                        label.observed_at_utc,
                        participant_id,
                        label.label_kind,
                        label.scope,
                        normalized,
                        label.provenance,
                    ),
                )
                if refreshed.rowcount > 0:
                    return
        label_id = opaque_id(
            "wxlabel",
            participant_id,
            label.label_kind,
            label.scope,
            label.label,
            label.observed_at_utc,
            observed_message_id or label.observed_source_message_id or "",
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO participant_labels(
                label_id, participant_id, label, normalized_label,
                label_kind, scope_kind, reader_id, observed_message_id,
                provenance, observed_at, valid_from, valid_to,
                temporal_confidence, active
            ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                label_id,
                participant_id,
                label.label,
                normalized,
                label.label_kind,
                label.scope,
                observed_message_id,
                label.provenance,
                label.observed_at_utc,
                label.valid_from_utc or label.observed_at_utc,
                label.valid_to_utc,
                label.temporal_confidence,
            ),
        )

    @staticmethod
    def _observe_member_label(
        connection: sqlite3.Connection,
        membership_id: str,
        label: LabelObservation,
        *,
        observed_message_id: str | None,
    ) -> None:
        normalized = normalize_label(label.label)
        refreshed = False
        if label.label_kind != "message_surface":
            connection.execute(
                """
                UPDATE conversation_member_labels
                SET active = 0, valid_to = ?
                WHERE membership_id = ? AND label_kind = ?
                  AND active = 1 AND normalized_label != ?
                """,
                (label.observed_at_utc, membership_id, label.label_kind, normalized),
            )
            if observed_message_id is None:
                refreshed = (
                    connection.execute(
                        """
                        UPDATE conversation_member_labels
                        SET label = ?,
                            observed_at = MAX(observed_at, ?)
                        WHERE membership_id = ? AND label_kind = ?
                          AND normalized_label = ? AND provenance = ?
                          AND observed_message_id IS NULL AND active = 1
                        """,
                        (
                            label.label,
                            label.observed_at_utc,
                            membership_id,
                            label.label_kind,
                            normalized,
                            label.provenance,
                        ),
                    ).rowcount
                    > 0
                )
        if not refreshed:
            label_id = opaque_id(
                "wxmemberlabel",
                membership_id,
                label.label_kind,
                label.label,
                label.observed_at_utc,
                observed_message_id or label.observed_source_message_id or "",
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO conversation_member_labels(
                    member_label_id, membership_id, label, normalized_label,
                    label_kind, observed_message_id, provenance, observed_at,
                    valid_from, valid_to, temporal_confidence, active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    label_id,
                    membership_id,
                    label.label,
                    normalized,
                    label.label_kind,
                    observed_message_id,
                    label.provenance,
                    label.observed_at_utc,
                    label.valid_from_utc or label.observed_at_utc,
                    label.valid_to_utc,
                    label.temporal_confidence,
                ),
            )
        if label.label_kind in {"group_card", "sightglass_alias"}:
            connection.execute(
                """
                UPDATE conversation_members SET current_group_alias = ?
                WHERE membership_id = ?
                """,
                (label.label, membership_id),
            )

    def upsert_message(
        self,
        account_id: str,
        conversation_id: str,
        participant_id: str | None,
        membership_id: str | None,
        source: SourceMessage,
        parsed: ParsedMessage,
        *,
        projection_epoch: str,
    ) -> str:
        message_id = opaque_id("wxmsg", account_id, source.source_message_id)
        parsed_value = {
            "kind": parsed.kind,
            "text": parsed.text,
            **parsed.structured,
            "resources": [resource.as_dict() for resource in parsed.resources],
        }
        # The immutable observation keeps the full message mapping. The canonical
        # ``messages`` row keeps one current-body copy: the body text lives only
        # in ``text``, and ``structured_json`` omits the duplicate.
        parsed_json = stored_structured_json(parsed)
        raw_digest = hashlib.sha256(source.raw_content.encode("utf-8")).hexdigest()
        observation_value = {
            "message": parsed_value,
            "sender": {
                "identity_keys": [
                    {
                        "kind": key.kind,
                        "value": key.value,
                        "stability": key.stability,
                        "principal_eligible": key.principal_eligible,
                        "scope_conversation_source_id": key.scope_conversation_source_id,
                        "provenance": key.provenance,
                    }
                    for key in source.sender_keys
                ],
                "surface_label": source.sender_surface_label,
                "is_outgoing": source.is_outgoing,
            },
            "source_envelope": {
                "source_message_id": source.source_message_id,
                "source_conversation_id": source.source_conversation_id,
                "source_time_raw": source.source_time_raw,
                "sent_at_utc": source.sent_at_utc,
                "sort_seq": source.sort_seq,
                "source_rowid": source.source_rowid,
                "wechat_type": source.wechat_type,
                "raw_payload_digest": raw_digest,
                "resources": [resource.as_dict() for resource in source.resources],
            },
        }
        observation_json = json.dumps(
            observation_value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        payload_digest = hashlib.sha256(observation_json.encode("utf-8")).hexdigest()
        # New observations are written as a self-describing BLOB. The digest above
        # stays over the uncompressed bytes, so identity and dedup are unchanged.
        observation_blob = encode_observation(observation_json)
        snapshot_value = {
            "shown_as": source.sender_surface_label,
            "shown_as_source": "message_surface" if source.sender_surface_label else None,
            "shown_as_temporal_confidence": "exact" if source.sender_surface_label else None,
            "is_self": source.is_outgoing,
        }
        with self.database.transaction() as connection:
            current_observation = connection.execute(
                "SELECT o.* FROM messages AS m LEFT JOIN message_observations AS o "
                "ON o.observation_seq=m.current_observation_seq WHERE m.message_id=?",
                (message_id,),
            ).fetchone()
            connection.execute(
                """
                INSERT INTO messages(
                    message_id, account_id, conversation_id, source_message_id,
                    source_time_raw, sent_at_utc, sort_primary, sort_seq,
                    sort_tie, sender_id, sender_membership_id,
                    sender_label_snapshot_json, kind, text, structured_json,
                    first_seen_at, last_seen_at, current_state,
                    current_generation_id, search_text, projection_epoch
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'present', ?, ?, ?)
                ON CONFLICT(account_id, source_message_id) DO UPDATE SET
                    conversation_id = excluded.conversation_id,
                    source_time_raw = excluded.source_time_raw,
                    sent_at_utc = excluded.sent_at_utc,
                    sort_primary = excluded.sort_primary,
                    sort_seq = excluded.sort_seq,
                    sort_tie = excluded.sort_tie,
                    sender_id = excluded.sender_id,
                    sender_membership_id = excluded.sender_membership_id,
                    sender_label_snapshot_json = excluded.sender_label_snapshot_json,
                    kind = excluded.kind,
                    text = excluded.text,
                    structured_json = excluded.structured_json,
                    search_text = excluded.search_text,
                    last_seen_at = excluded.last_seen_at,
                    current_state = excluded.current_state,
                    current_generation_id = excluded.current_generation_id,
                    projection_epoch = excluded.projection_epoch,
                    body_available = 1
                """,
                (
                    message_id,
                    account_id,
                    conversation_id,
                    source.source_message_id,
                    source.source_time_raw,
                    source.sent_at_utc,
                    source.sent_at_utc,
                    source.sort_seq,
                    source.source_rowid,
                    participant_id,
                    membership_id,
                    json.dumps(snapshot_value, ensure_ascii=False, sort_keys=True),
                    parsed.kind,
                    parsed.text,
                    parsed_json,
                    source.observed_at_utc,
                    source.observed_at_utc,
                    source.source_generation_id,
                    stored_search_text(parsed, rendered=message_search_text(parsed)),
                    projection_epoch,
                ),
            )
            observation_id = opaque_id(
                "wxobservation",
                message_id,
                source.source_generation_id,
                payload_digest,
                PARSER_VERSION,
                "present",
                str(current_observation["observation_seq"] if current_observation else 0),
            )
            inserted_observation = connection.execute(
                """
                INSERT INTO message_observations(
                    observation_id, message_id, observed_at,
                    source_generation_id, state, payload_digest,
                    parsed_json, parser_version, raw_payload_ref, reason_code
                )
                SELECT ?, ?, ?, ?, 'present', ?, ?, ?, NULL, NULL
                WHERE NOT EXISTS (
                    SELECT 1 FROM messages AS m JOIN message_observations AS o
                      ON o.observation_seq=m.current_observation_seq
                    WHERE m.message_id = ? AND o.state = 'present'
                      AND o.payload_digest = ? AND o.parser_version = ?
                )
                """,
                (
                    observation_id,
                    message_id,
                    source.observed_at_utc,
                    source.source_generation_id,
                    payload_digest,
                    observation_blob,
                    PARSER_VERSION,
                    message_id,
                    payload_digest,
                    PARSER_VERSION,
                ),
            )
            if not inserted_observation.rowcount and current_observation is not None:
                # The current episode already matches this content digest, so no new
                # observation is appended (identical re-read is not an update).  But
                # if its body copy was intentionally released, restore the full
                # payload in place: same row, same observation_seq, same digest and
                # same source episode, so this is a rehydration, not a new update.
                payload_state = observation_payload_state(current_observation["parsed_json"])
                if payload_state == "corrupt":
                    raise ObservationCodecError("current observation is corrupt")
                if payload_state == "released":
                    connection.execute(
                        "UPDATE message_observations SET parsed_json = ? WHERE observation_seq = ?",
                        (observation_blob, int(current_observation["observation_seq"])),
                    )
            first_observation = connection.execute(
                """
                SELECT MIN(observation_seq)
                FROM message_observations WHERE message_id = ?
                """,
                (message_id,),
            ).fetchone()
            if first_observation is None or first_observation[0] is None:
                raise RuntimeError("message admission did not retain an observation")
            if inserted_observation.rowcount:
                assert inserted_observation.lastrowid is not None
                current_sequence = int(inserted_observation.lastrowid)
            else:
                assert current_observation is not None
                current_sequence = int(current_observation["observation_seq"])
            connection.execute(
                """
                UPDATE messages
                SET first_observation_seq = ?, current_observation_seq = ?
                WHERE message_id = ?
                """,
                (int(first_observation[0]), current_sequence, message_id),
            )
            link_row = connection.execute(
                "SELECT * FROM messages WHERE message_id=?",
                (message_id,),
            ).fetchone()
            assert link_row is not None
            publish_links(connection, prepare_links(link_row))
            existing_resources = connection.execute(
                "SELECT * FROM resources WHERE message_id = ? ORDER BY resource_id",
                (message_id,),
            ).fetchall()
            existing_by_source_key = {
                str(row["source_resource_key"]): row
                for row in existing_resources
                if row["source_resource_key"]
            }
            existing_by_fingerprint: dict[str, sqlite3.Row] = {}
            existing_by_ordinal = {
                int(row["source_ordinal"]): row
                for row in existing_resources
                if row["source_resource_key"] is None and int(row["source_ordinal"]) >= 0
            }
            for index, row in enumerate(existing_resources):
                try:
                    resolver = json.loads(str(row["resolver_json"]))
                except json.JSONDecodeError:
                    resolver = {}
                fingerprint = resolver.get("binding_fingerprint")
                if row["source_resource_key"] is None and isinstance(fingerprint, str):
                    if fingerprint in existing_by_fingerprint:
                        raise SightglassError(
                            ErrorCode.SOURCE_MESSAGE_DECODE_FAILED,
                            details={"warning_codes": ["resource_binding_ambiguous"]},
                        )
                    existing_by_fingerprint[fingerprint] = row
                resolver["active"] = False
                ordinal = int(row["source_ordinal"])
                if row["source_resource_key"] is None:
                    ordinal = -1_000_000 - index
                connection.execute(
                    """
                    UPDATE resources SET source_ordinal = ?, resolver_json = ?
                    WHERE resource_id = ?
                    """,
                    (
                        ordinal,
                        json.dumps(resolver, sort_keys=True, separators=(",", ":")),
                        str(row["resource_id"]),
                    ),
                )

            current_fingerprints: set[str] = set()
            for resource in parsed.resources:
                fingerprint = resource_binding_fingerprint(resource)
                if resource.source_resource_key:
                    existing = existing_by_source_key.get(resource.source_resource_key)
                    resource_id = (
                        str(existing["resource_id"])
                        if existing is not None
                        else opaque_id("wxres", message_id, resource.source_resource_key)
                    )
                else:
                    if fingerprint in current_fingerprints:
                        raise SightglassError(
                            ErrorCode.SOURCE_MESSAGE_DECODE_FAILED,
                            details={"warning_codes": ["resource_binding_ambiguous"]},
                        )
                    existing = existing_by_fingerprint.get(fingerprint)
                    if existing is None:
                        existing = existing_by_ordinal.get(resource.source_ordinal)
                    resource_id = (
                        str(existing["resource_id"])
                        if existing is not None
                        else opaque_id("wxres", message_id, f"evidence:{fingerprint}")
                    )
                current_fingerprints.add(fingerprint)
                resolver_json = json.dumps(
                    {"active": True, "binding_fingerprint": fingerprint},
                    sort_keys=True,
                    separators=(",", ":"),
                )
                connection.execute(
                    """
                    INSERT INTO resources(
                        resource_id, message_id, source_resource_key,
                        source_ordinal, kind, mime_type, original_name,
                        declared_size, declared_hash, availability,
                        resolver_json, first_seen_at, last_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(resource_id) DO UPDATE SET
                        source_resource_key = excluded.source_resource_key,
                        source_ordinal = excluded.source_ordinal,
                        kind = excluded.kind,
                        mime_type = excluded.mime_type,
                        original_name = excluded.original_name,
                        declared_size = excluded.declared_size,
                        declared_hash = excluded.declared_hash,
                        availability = excluded.availability,
                        resolver_json = excluded.resolver_json,
                        last_seen_at = excluded.last_seen_at
                    """,
                    (
                        resource_id,
                        message_id,
                        resource.source_resource_key,
                        resource.source_ordinal,
                        resource.kind,
                        resource.mime_type,
                        resource.original_name,
                        resource.declared_size,
                        resource.declared_hash,
                        resource.availability,
                        resolver_json,
                        source.observed_at_utc,
                        source.observed_at_utc,
                    ),
                )
            for label in source.sender_labels:
                if participant_id is None or label.scope != "message-surface":
                    continue
                self._observe_participant_label(
                    connection,
                    participant_id,
                    label,
                    observed_message_id=message_id,
                )
                if membership_id is not None:
                    self._observe_member_label(
                        connection,
                        membership_id,
                        label,
                        observed_message_id=message_id,
                    )
            if membership_id is not None:
                connection.execute(
                    """
                    UPDATE conversation_members
                    SET last_message_at = CASE
                        WHEN last_message_at IS NULL OR last_message_at < ? THEN ?
                        ELSE last_message_at END,
                        last_seen_at = ?
                    WHERE membership_id = ?
                    """,
                    (
                        source.sent_at_utc,
                        source.sent_at_utc,
                        source.observed_at_utc,
                        membership_id,
                    ),
                )
            connection.execute(
                """
                UPDATE conversations
                SET last_message_at = CASE
                        WHEN last_message_at IS NULL OR last_message_at < ? THEN ?
                        ELSE last_message_at
                    END,
                    last_seen_at = CASE
                        WHEN last_seen_at < ? THEN ? ELSE last_seen_at
                    END
                WHERE conversation_id = ?
                """,
                (
                    source.sent_at_utc,
                    source.sent_at_utc,
                    source.observed_at_utc,
                    source.observed_at_utc,
                    conversation_id,
                ),
            )
        return message_id

    def account_row(self, account_id: str) -> sqlite3.Row | None:
        with self.database.connection() as connection:
            return connection.execute(
                "SELECT * FROM accounts WHERE account_id = ? AND active = 1",
                (account_id,),
            ).fetchone()

    def active_account_ids(self) -> tuple[str, ...]:
        with self.database.connection() as connection:
            rows = connection.execute(
                "SELECT account_id FROM accounts WHERE active = 1 ORDER BY account_id"
            ).fetchall()
        return tuple(str(row["account_id"]) for row in rows)

    def conversation_row(self, conversation_id: str) -> sqlite3.Row | None:
        with self.database.connection() as connection:
            return connection.execute(
                "SELECT * FROM conversations WHERE conversation_id = ?",
                (conversation_id,),
            ).fetchone()

    def conversation_context(self, conversation_id: str) -> sqlite3.Row | None:
        with self.database.connection() as connection:
            return connection.execute(
                """
                SELECT c.*, a.source_account_key, a.reader_timezone,
                       a.current_display_name AS account_display_name
                FROM conversations c
                JOIN accounts a USING(account_id)
                WHERE c.conversation_id = ? AND a.active = 1
                """,
                (conversation_id,),
            ).fetchone()

    def active_accounts(self) -> list[sqlite3.Row]:
        with self.database.connection() as connection:
            return connection.execute(
                "SELECT * FROM accounts WHERE active = 1 ORDER BY account_id"
            ).fetchall()

    def message_source_row(self, message_id: str) -> sqlite3.Row | None:
        with self.database.connection() as connection:
            return connection.execute(
                """
                SELECT
                    m.message_id,
                    m.account_id,
                    m.conversation_id,
                    m.source_message_id,
                    c.source_conversation_id
                FROM messages m JOIN conversations c USING(conversation_id)
                WHERE m.message_id = ?
                """,
                (message_id,),
            ).fetchone()

    def participant_source_filters(
        self, conversation_id: str, participant_ids: tuple[str, ...]
    ) -> tuple[SourceParticipantFilter, ...]:
        if not participant_ids:
            return ()
        placeholders = ",".join("?" for _ in participant_ids)
        with self.database.connection() as connection:
            member_rows = connection.execute(
                f"""
                SELECT participant_id FROM conversation_members
                WHERE conversation_id = ? AND participant_id IN ({placeholders})
                """,
                (conversation_id, *participant_ids),
            ).fetchall()
            if len(member_rows) != len(set(participant_ids)):
                raise SightglassError(ErrorCode.PARTICIPANT_OUT_OF_SCOPE)
            key_rows = connection.execute(
                f"""
                SELECT psk.key_kind, psk.key_value, psk.principal_eligible,
                       c.source_conversation_id AS scope_source_conversation_id
                FROM participant_source_keys AS psk
                LEFT JOIN conversations AS c
                  ON c.conversation_id = psk.scope_conversation_id
                WHERE psk.participant_id IN ({placeholders}) AND psk.active = 1
                """,
                participant_ids,
            ).fetchall()
            unresolved_rows = connection.execute(
                f"""
                SELECT source_membership_id FROM conversation_members
                WHERE conversation_id = ? AND participant_id IN ({placeholders})
                  AND source_membership_id LIKE 'message:%'
                """,
                (conversation_id, *participant_ids),
            ).fetchall()
        filters = {
            SourceParticipantFilter(
                key_kind=str(row["key_kind"]),
                key_value=str(row["key_value"]),
                principal_eligible=bool(row["principal_eligible"]),
                scope_conversation_source_id=(
                    str(row["scope_source_conversation_id"])
                    if row["scope_source_conversation_id"]
                    else None
                ),
            )
            for row in key_rows
        }
        filters.update(
            SourceParticipantFilter(
                source_message_id=str(row["source_membership_id"]).removeprefix("message:")
            )
            for row in unresolved_rows
        )
        return tuple(
            sorted(
                filters,
                key=lambda value: (
                    value.source_message_id or "",
                    value.key_kind or "",
                    value.key_value or "",
                    value.scope_conversation_source_id or "",
                ),
            )
        )

    def preferred_label(self, participant_id: str, membership_id: str | None) -> tuple[str, str]:
        with self.database.connection() as connection:
            choices: list[tuple[int, str, str]] = []
            if membership_id:
                for row in connection.execute(
                    """
                    SELECT label, label_kind FROM conversation_member_labels
                    WHERE membership_id = ? AND active = 1
                      AND label_kind IN ('sightglass_alias', 'group_card')
                    """,
                    (membership_id,),
                ):
                    rank = 0 if row["label_kind"] == "sightglass_alias" else 3
                    choices.append((rank, str(row["label"]), str(row["label_kind"])))
            for row in connection.execute(
                """
                SELECT label, label_kind FROM participant_labels
                WHERE participant_id = ? AND active = 1
                  AND label_kind IN (
                      'sightglass_alias', 'contact_remark',
                      'account_nickname', 'public_handle'
                  )
                """,
                (participant_id,),
            ):
                ranks = {
                    "sightglass_alias": 1,
                    "contact_remark": 2,
                    "account_nickname": 4,
                    "public_handle": 5,
                }
                choices.append(
                    (ranks[str(row["label_kind"])], str(row["label"]), str(row["label_kind"]))
                )
        if choices:
            _rank, label, source = min(choices, key=lambda value: (value[0], value[1]))
            return label, source
        return f"成员-{participant_id[-6:]}", "opaque_fallback"

    def preferred_labels_bulk(
        self, participant_memberships: tuple[tuple[str, str | None], ...]
    ) -> dict[tuple[str, str | None], tuple[str, str]]:
        pairs = tuple(dict.fromkeys(participant_memberships))
        if not pairs:
            return {}
        participant_ids = tuple(dict.fromkeys(participant_id for participant_id, _ in pairs))
        membership_ids = tuple(
            dict.fromkeys(membership_id for _, membership_id in pairs if membership_id is not None)
        )
        participant_choices: dict[str, list[tuple[int, str, str]]] = {}
        membership_choices: dict[str, list[tuple[int, str, str]]] = {}
        with self.database.connection() as connection:
            participant_rows = connection.execute(
                f"""
                SELECT participant_id, label, label_kind
                FROM participant_labels
                WHERE participant_id IN ({",".join("?" for _ in participant_ids)})
                  AND active = 1
                  AND label_kind IN (
                      'sightglass_alias', 'contact_remark',
                      'account_nickname', 'public_handle'
                  )
                """,
                participant_ids,
            ).fetchall()
            membership_rows = (
                connection.execute(
                    f"""
                    SELECT membership_id, label, label_kind
                    FROM conversation_member_labels
                    WHERE membership_id IN ({",".join("?" for _ in membership_ids)})
                      AND active = 1
                      AND label_kind IN ('sightglass_alias', 'group_card')
                    """,
                    membership_ids,
                ).fetchall()
                if membership_ids
                else ()
            )
        participant_ranks = {
            "sightglass_alias": 1,
            "contact_remark": 2,
            "account_nickname": 4,
            "public_handle": 5,
        }
        for row in participant_rows:
            kind = str(row["label_kind"])
            participant_choices.setdefault(str(row["participant_id"]), []).append(
                (participant_ranks[kind], str(row["label"]), kind)
            )
        for row in membership_rows:
            kind = str(row["label_kind"])
            membership_choices.setdefault(str(row["membership_id"]), []).append(
                (0 if kind == "sightglass_alias" else 3, str(row["label"]), kind)
            )
        result: dict[tuple[str, str | None], tuple[str, str]] = {}
        for participant_id, membership_id in pairs:
            choices = [*participant_choices.get(participant_id, ())]
            if membership_id is not None:
                choices.extend(membership_choices.get(membership_id, ()))
            if choices:
                _rank, label, source = min(choices, key=lambda value: (value[0], value[1]))
                result[(participant_id, membership_id)] = (label, source)
            else:
                result[(participant_id, membership_id)] = (
                    f"成员-{participant_id[-6:]}",
                    "opaque_fallback",
                )
        return result

    def participant_candidates(self, conversation_id: str, query: str) -> list[dict[str, Any]]:
        query_normalized = normalize_label(query)
        with self.database.connection() as connection:
            members = connection.execute(
                """
                SELECT cm.*, p.resolution_state, p.identity_confidence
                FROM conversation_members cm
                JOIN participants p USING(participant_id)
                WHERE cm.conversation_id = ?
                """,
                (conversation_id,),
            ).fetchall()
            participant_labels = connection.execute(
                """
                SELECT cm.participant_id, pl.*
                FROM conversation_members cm
                JOIN participant_labels pl USING(participant_id)
                WHERE cm.conversation_id = ?
                """,
                (conversation_id,),
            ).fetchall()
            member_labels = connection.execute(
                """
                SELECT cm.participant_id, cml.*
                FROM conversation_members cm
                JOIN conversation_member_labels cml USING(membership_id)
                WHERE cm.conversation_id = ?
                """,
                (conversation_id,),
            ).fetchall()
        p_labels: dict[str, list[sqlite3.Row]] = {}
        m_labels: dict[str, list[sqlite3.Row]] = {}
        for row in participant_labels:
            p_labels.setdefault(str(row["participant_id"]), []).append(row)
        for row in member_labels:
            m_labels.setdefault(str(row["participant_id"]), []).append(row)
        candidates: list[dict[str, Any]] = []
        for member in members:
            participant_id = str(member["participant_id"])
            labels = [*p_labels.get(participant_id, ()), *m_labels.get(participant_id, ())]
            matched = [
                row
                for row in labels
                if not query_normalized or query_normalized in str(row["normalized_label"])
            ]
            if query_normalized and not matched:
                continue
            label, label_source = self.preferred_label(participant_id, str(member["membership_id"]))
            current_layers: dict[str, str | None] = {
                "contact_remark": None,
                "account_nickname": None,
                "current_group_alias": None,
                "public_handle": None,
                "message_surface": None,
            }
            for row in labels:
                if not row["active"]:
                    continue
                kind = str(row["label_kind"])
                layer_key = "current_group_alias" if kind == "group_card" else kind
                if layer_key in current_layers:
                    current_layers[layer_key] = str(row["label"])
            best = min(
                matched or labels,
                key=lambda row: (
                    0 if normalize_label(str(row["label"])) == query_normalized else 1,
                    0 if row["active"] else 1,
                    str(row["observed_at"]),
                ),
                default=None,
            )
            matched_value = str(best["label"]) if best is not None else label
            matched_kind = str(best["label_kind"]) if best is not None else label_source
            matched_scope = (
                str(best["scope_kind"])
                if best is not None and "scope_kind" in best.keys()
                else "conversation"
            )
            temporal = str(best["temporal_confidence"]) if best is not None else "unknown"
            candidates.append(
                {
                    "participant_id": participant_id,
                    "membership_id": str(member["membership_id"]),
                    "label": label,
                    "label_source": label_source,
                    "labels": current_layers,
                    "matched": {
                        "value": matched_value,
                        "kind": matched_kind,
                        "scope": matched_scope,
                        "temporal_confidence": temporal,
                    },
                    "last_spoke_at": member["last_message_at"],
                    "resolution_state": str(member["resolution_state"]),
                    "identity_confidence": str(member["identity_confidence"]),
                }
            )
        return sorted(candidates, key=lambda row: (row["label"], row["participant_id"]))

    def participant_key_kinds(self, participant_id: str) -> list[str]:
        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT key_kind FROM participant_source_keys
                WHERE participant_id = ? AND active = 1
                ORDER BY key_kind
                """,
                (participant_id,),
            ).fetchall()
        return [str(row[0]) for row in rows]

    def participant_key_details(self, participant_id: str) -> list[dict[str, Any]]:
        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT key_kind, stability, principal_eligible, provenance,
                       CASE WHEN scope_conversation_id IS NULL
                            THEN 'global' ELSE 'conversation' END AS scope
                FROM participant_source_keys
                WHERE participant_id = ? AND active = 1
                ORDER BY key_kind, scope, provenance
                """,
                (participant_id,),
            ).fetchall()
        return [
            {
                "kind": str(row["key_kind"]),
                "stability": str(row["stability"]),
                "principal_eligible": bool(row["principal_eligible"]),
                "scope": str(row["scope"]),
                "provenance": str(row["provenance"]),
            }
            for row in rows
        ]

    def message_rows(
        self,
        conversation_id: str,
        *,
        limit: int,
        direction: str,
        participant_ids: tuple[str, ...] = (),
        time_after_utc: str | None = None,
        time_before_utc: str | None = None,
        message_id: str | None = None,
        message_ids: tuple[str, ...] = (),
    ) -> list[sqlite3.Row]:
        clauses = ["m.conversation_id = ?"]
        values: list[Any] = [conversation_id]
        if message_id:
            clauses.append("m.message_id = ?")
            values.append(message_id)
        if message_ids:
            clauses.append(f"m.message_id IN ({','.join('?' for _ in message_ids)})")
            values.extend(message_ids)
        if participant_ids:
            clauses.append(f"m.sender_id IN ({','.join('?' for _ in participant_ids)})")
            values.extend(participant_ids)
        if time_after_utc:
            clauses.append("m.sent_at_utc >= ?")
            values.append(time_after_utc)
        if time_before_utc:
            clauses.append("m.sent_at_utc < ?")
            values.append(time_before_utc)
        order = "ASC" if direction == "forward" else "DESC"
        if message_ids:
            # A non-empty ``message_ids`` set names globally unique primary keys, so this
            # read is a bounded point lookup. Keeping ``conversation_id = ?`` in the SQL
            # would instead let SQLite drive the query from ``message_timeline`` and scan a
            # very large conversation. Drop only that predicate from SQL, enforce the
            # conversation scope in memory, and keep the caller's exact tuple ordering.
            return self._message_rows_by_ids(
                conversation_id,
                message_id=message_id,
                message_ids=message_ids,
                limit=limit,
                direction=direction,
                participant_ids=participant_ids,
                time_after_utc=time_after_utc,
                time_before_utc=time_before_utc,
            )
        values.append(max(1, int(limit)))
        with self.database.connection() as connection:
            rows = connection.execute(
                f"""
                SELECT m.*, p.resolution_state AS sender_resolution_state,
                       p.identity_confidence AS sender_identity_confidence
                FROM messages AS m
                LEFT JOIN participants AS p ON p.participant_id = m.sender_id
                WHERE {" AND ".join(clauses)}
                ORDER BY m.sort_primary {order}, m.sort_seq {order},
                         m.sort_tie {order}, m.source_message_id {order}
                LIMIT ?
                """,
                values,
            ).fetchall()
        if direction == "backward":
            rows.reverse()
        return rows

    def materialized_message_rows(
        self,
        conversation_id: str,
        *,
        projection_epoch: str,
        observation_watermark: int,
        limit: int,
        direction: str,
        after: SourceSortKey | None = None,
        before: SourceSortKey | None = None,
        participant_ids: tuple[str, ...] = (),
        time_after_utc: str | None = None,
        time_before_utc: str | None = None,
        message_id: str | None = None,
        continuity_with: SourceSortKey | None = None,
    ) -> list[sqlite3.Row]:
        """Read one immutable materialized snapshot from the current projection.

        Rows first observed after ``observation_watermark`` are ordinary appends and are
        excluded. Rows that existed at the watermark but changed afterwards are detected
        separately by ``materialized_snapshot_changed`` so a continuation never silently
        drops a corrected row.
        """

        clauses = [
            "m.conversation_id = ?",
            "m.current_state = 'present'",
            resident_body_predicate(),
            "m.projection_epoch = ?",
            "m.first_observation_seq IS NOT NULL",
            "m.current_observation_seq IS NOT NULL",
            "m.first_observation_seq <= ?",
            "m.current_observation_seq <= ?",
        ]
        values: list[Any] = [
            conversation_id,
            projection_epoch,
            int(observation_watermark),
            int(observation_watermark),
        ]
        if continuity_with is not None:
            window = containing_window(
                self.database, conversation_id, projection_epoch, continuity_with
            )
            if window is None:
                return []
            clauses.append(
                "(m.sort_primary,m.sort_seq,m.sort_tie,m.source_message_id) >= (?,?,?,?)"
            )
            values.extend(window[0].as_tuple())
            clauses.append(
                "(m.sort_primary,m.sort_seq,m.sort_tie,m.source_message_id) <= (?,?,?,?)"
            )
            values.extend(window[1].as_tuple())
        if message_id is not None:
            clauses.append("m.message_id = ?")
            values.append(message_id)
        if participant_ids:
            clauses.append(f"m.sender_id IN ({','.join('?' for _ in participant_ids)})")
            values.extend(participant_ids)
        if time_after_utc is not None:
            clauses.append("m.sort_primary >= ? AND m.sent_at_utc >= ?")
            values.extend((time_after_utc, time_after_utc))
        if time_before_utc is not None:
            clauses.append("m.sort_primary < ? AND m.sent_at_utc < ?")
            values.extend((time_before_utc, time_before_utc))
        if after is not None:
            clauses.append(
                "(m.sort_primary, m.sort_seq, m.sort_tie, m.source_message_id) > (?, ?, ?, ?)"
            )
            values.extend(after.as_tuple())
        if before is not None:
            clauses.append(
                "(m.sort_primary, m.sort_seq, m.sort_tie, m.source_message_id) < (?, ?, ?, ?)"
            )
            values.extend(before.as_tuple())
        order = "ASC" if direction == "forward" else "DESC"
        values.append(max(1, int(limit)))
        # sort_primary is the canonical sent_at_utc (SourceMessage.sort_key).
        # Seek only resident bodies in canonical order. Released identities are
        # durable history, but must not enlarge a warm page or an empty probe.
        # Epoch/watermark/continuity predicates remain authoritative filters.
        index = " INDEXED BY message_resident_timeline" if message_id is None else ""
        with self.database.connection() as connection:
            rows = connection.execute(
                f"""
                SELECT m.*, p.resolution_state AS sender_resolution_state,
                       p.identity_confidence AS sender_identity_confidence
                FROM messages AS m{index}
                LEFT JOIN participants AS p ON p.participant_id = m.sender_id
                WHERE {" AND ".join(clauses)}
                ORDER BY m.sort_primary {order}, m.sort_seq {order},
                         m.sort_tie {order}, m.source_message_id {order}
                LIMIT ?
                """,
                values,
            ).fetchall()
        if direction == "backward":
            rows.reverse()
        return rows

    def materialized_snapshot_changed(
        self,
        conversation_id: str,
        *,
        projection_epoch: str,
        observation_watermark: int,
    ) -> bool:
        """Whether a row visible at a signed watermark changed after that watermark."""

        # Canonical writes append the observation before publishing its current
        # sequence. Drive from the observation PK range, not all old messages;
        # CROSS JOIN retains that order even without planner statistics. Later
        # appends have first_observation_seq > watermark and stay excluded.
        with self.database.connection() as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM message_observations AS o CROSS JOIN messages AS m
                ON m.message_id = o.message_id
                WHERE o.observation_seq > ?
                  AND m.current_observation_seq = o.observation_seq
                  AND m.conversation_id = ? AND m.projection_epoch = ?
                  AND m.first_observation_seq IS NOT NULL
                  AND m.first_observation_seq <= ?
                LIMIT 1
                """,
                (
                    int(observation_watermark),
                    conversation_id,
                    projection_epoch,
                    int(observation_watermark),
                ),
            ).fetchone()
        return row is not None

    def has_materialized_messages(
        self, projection_epoch: str, *, conversation_id: str | None = None
    ) -> bool:
        clauses = [
            "projection_epoch = ?",
            "current_state = 'present'",
            resident_body_predicate('messages'),
            "current_observation_seq IS NOT NULL",
        ]
        values: list[Any] = [projection_epoch]
        if conversation_id is not None:
            clauses.append("conversation_id = ?")
            values.append(conversation_id)
        with self.database.connection() as connection:
            row = connection.execute(
                f"SELECT 1 FROM messages INDEXED BY message_resident_timeline "
                f"WHERE {' AND '.join(clauses)} LIMIT 1",
                values,
            ).fetchone()
        return row is not None

    def materialized_observation_bounds(
        self,
        conversation_id: str,
        *,
        projection_epoch: str,
        observation_watermark: int,
    ) -> tuple[str | None, str | None]:
        """Bounds of admitted rows in this materialized snapshot, not old cache rows."""

        oldest = self.materialized_message_rows(
            conversation_id,
            projection_epoch=projection_epoch,
            observation_watermark=observation_watermark,
            limit=1,
            direction="forward",
        )
        newest = self.materialized_message_rows(
            conversation_id,
            projection_epoch=projection_epoch,
            observation_watermark=observation_watermark,
            limit=1,
            direction="backward",
        )
        if not oldest or not newest:
            return None, None
        return str(oldest[0]["sent_at_utc"]), str(newest[0]["sent_at_utc"])

    def has_materialized_read_plane(self, projection_epoch: str) -> bool:
        with self.database.connection() as connection:
            row = connection.execute(
                f"""
                SELECT 1
                FROM conversations AS c
                LEFT JOIN source_conversation_state AS s
                  ON s.conversation_id = c.conversation_id
                WHERE COALESCE(s.last_error_code, '') != ?
                  AND EXISTS (
                      SELECT 1
                      FROM messages AS m INDEXED BY message_resident_timeline
                      WHERE m.conversation_id = c.conversation_id
                        AND m.projection_epoch = ?
                        AND m.current_state = 'present'
                        AND {resident_body_predicate()}
                        AND m.current_observation_seq IS NOT NULL
                  )
                LIMIT 1
                """,
                (
                    "duplicate_message_identity_conflict",
                    projection_epoch,
                ),
            ).fetchone()
        return row is not None

    def has_resource_bindings(self) -> bool:
        with self.database.connection() as connection:
            return (
                connection.execute("SELECT 1 FROM resource_bindings LIMIT 1").fetchone() is not None
            )

    def identity_correction_revision(self) -> int:
        """Return the append-only correction ledger revision for cursor binding."""

        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(rowid), 0) FROM identity_corrections"
            ).fetchone()
        return int(row[0]) if row is not None else 0

    def _message_rows_by_ids(
        self,
        conversation_id: str,
        *,
        message_id: str | None,
        message_ids: tuple[str, ...],
        limit: int,
        direction: str,
        participant_ids: tuple[str, ...],
        time_after_utc: str | None,
        time_before_utc: str | None,
    ) -> list[sqlite3.Row]:
        """Resolve explicit message ids by primary key, then scope them in memory.

        ``message_id`` is globally unique, so ``WHERE m.message_id IN (...)`` is a bounded
        point lookup that cannot scan a conversation. The conversation predicate, any
        time window, and the requested limit are applied here to reproduce the public
        contract without letting a competing timeline predicate or ``ORDER BY`` drive the
        plan. Rows always come back in chronological ascending order.
        """

        ordered = tuple(dict.fromkeys(str(value) for value in message_ids))
        if message_id:
            # The singular filter ANDs with the set exactly as the legacy path did.
            ordered = tuple(value for value in ordered if value == message_id)
        if not ordered:
            return []
        clauses = [f"m.message_id IN ({','.join('?' for _ in ordered)})"]
        values: list[Any] = list(ordered)
        if participant_ids:
            clauses.append(f"m.sender_id IN ({','.join('?' for _ in participant_ids)})")
            values.extend(participant_ids)
        with self.database.connection() as connection:
            rows = connection.execute(
                f"""
                SELECT m.*, p.resolution_state AS sender_resolution_state,
                       p.identity_confidence AS sender_identity_confidence
                FROM messages AS m
                LEFT JOIN participants AS p ON p.participant_id = m.sender_id
                WHERE {" AND ".join(clauses)}
                """,
                values,
            ).fetchall()
        selected = [row for row in rows if str(row["conversation_id"]) == conversation_id]
        if time_after_utc:
            selected = [row for row in selected if str(row["sent_at_utc"]) >= time_after_utc]
        if time_before_utc:
            selected = [row for row in selected if str(row["sent_at_utc"]) < time_before_utc]
        selected.sort(
            key=lambda row: (
                str(row["sort_primary"]),
                int(row["sort_seq"]),
                int(row["sort_tie"]),
                str(row["source_message_id"]),
            )
        )
        bounded = max(1, int(limit))
        if direction == "forward":
            selected = selected[:bounded]
        else:
            selected = selected[len(selected) - bounded :] if bounded < len(selected) else selected
        return selected

    def resources_for_message(self, message_id: str) -> list[dict[str, Any]]:
        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT resource_id, kind, mime_type, original_name,
                       declared_size, declared_hash, availability, resolver_json
                FROM resources WHERE message_id = ? ORDER BY source_ordinal
                """,
                (message_id,),
            ).fetchall()
        projected: list[dict[str, Any]] = []
        for row in rows:
            resolver = json.loads(str(row["resolver_json"]))
            if resolver.get("active", True) is False:
                continue
            value = dict(row)
            value.pop("resolver_json", None)
            projected.append(value)
        return projected

    def resources_for_messages_bulk(
        self, message_ids: tuple[str, ...]
    ) -> dict[str, list[dict[str, Any]]]:
        selected = tuple(dict.fromkeys(message_ids))
        result: dict[str, list[dict[str, Any]]] = {message_id: [] for message_id in selected}
        if not selected:
            return result
        with self.database.connection() as connection:
            rows = connection.execute(
                f"""
                SELECT message_id, resource_id, kind, mime_type, original_name,
                       declared_size, declared_hash, availability, resolver_json
                FROM resources
                WHERE message_id IN ({",".join("?" for _ in selected)})
                ORDER BY message_id, source_ordinal
                """,
                selected,
            ).fetchall()
        for row in rows:
            resolver = json.loads(str(row["resolver_json"]))
            if resolver.get("active", True) is False:
                continue
            value = dict(row)
            message_id = str(value.pop("message_id"))
            value.pop("resolver_json", None)
            result[message_id].append(value)
        return result

    def voice_resources_for_messages(self, message_ids: tuple[str, ...]) -> list[dict[str, Any]]:
        """Return voice-bound resource evidence for delivered messages, in input order.

        Each entry carries the recorded binding fingerprint, which is the input revision
        the voice domain keys its cache and jobs on, plus whether any cached object
        binding exists for the resource.
        """

        selected = tuple(dict.fromkeys(message_ids))
        if not selected:
            return []
        entries: list[dict[str, Any]] = []
        with self.database.connection() as connection:
            for start in range(0, len(selected), 400):
                chunk = selected[start : start + 400]
                rows = connection.execute(
                    f"""
                    SELECT r.message_id, r.resource_id, r.kind, r.mime_type, r.availability,
                           r.source_ordinal, r.resolver_json, m.account_id,
                           EXISTS(SELECT 1 FROM resource_bindings b
                                  WHERE b.resource_id = r.resource_id) AS bound
                    FROM resources AS r
                    JOIN messages AS m USING(message_id)
                    WHERE r.kind = 'voice' AND r.message_id IN ({",".join("?" for _ in chunk)})
                    ORDER BY r.message_id, r.source_ordinal
                    """,
                    chunk,
                ).fetchall()
                for row in rows:
                    value = dict(row)
                    try:
                        resolver = json.loads(str(value.pop("resolver_json")))
                    except json.JSONDecodeError:
                        resolver = {}
                    if not isinstance(resolver, dict) or resolver.get("active", True) is False:
                        continue
                    fingerprint = resolver.get("binding_fingerprint")
                    value["binding_fingerprint"] = (
                        str(fingerprint) if isinstance(fingerprint, str) and fingerprint else None
                    )
                    value["bound"] = bool(value["bound"])
                    entries.append(value)
        order = {message_id: index for index, message_id in enumerate(selected)}
        entries.sort(key=lambda item: order[str(item["message_id"])])
        return entries

    def resource_counts_bulk(self, message_ids: tuple[str, ...]) -> dict[str, int]:
        selected = tuple(dict.fromkeys(message_ids))
        result = {message_id: 0 for message_id in selected}
        if not selected:
            return result
        with self.database.connection() as connection:
            rows = connection.execute(
                f"""
                SELECT message_id, resolver_json
                FROM resources
                WHERE message_id IN ({",".join("?" for _ in selected)})
                """,
                selected,
            ).fetchall()
        for row in rows:
            resolver = json.loads(str(row["resolver_json"]))
            if resolver.get("active", True) is not False:
                result[str(row["message_id"])] += 1
        return result

    def resource_context(self, resource_id: str) -> sqlite3.Row | None:
        with self.database.connection() as connection:
            return connection.execute(
                """
                SELECT r.*, m.account_id, m.conversation_id, m.source_message_id,
                       m.sender_id, m.sender_membership_id,
                       c.source_conversation_id, a.source_account_key,
                       a.reader_timezone
                FROM resources AS r
                JOIN messages AS m USING(message_id)
                JOIN conversations AS c USING(conversation_id)
                JOIN accounts AS a USING(account_id)
                WHERE r.resource_id = ?
                """,
                (resource_id,),
            ).fetchone()

    def find_resources(
        self,
        *,
        account_id: str,
        query: str,
        conversation_ids: tuple[str, ...],
        kinds: tuple[str, ...],
        format_families: tuple[str, ...],
        after: str | None,
        before: str | None,
        availability: tuple[str, ...],
        permitted_conversations: tuple[str, ...] | None,
        denied_conversations: tuple[str, ...],
        observation_watermark: int,
        position: tuple[str, int, int, int, str] | None,
        limit: int,
    ) -> list[sqlite3.Row]:
        clauses = [
            "m.account_id = ?",
            "m.current_state = 'present'",
            resident_body_predicate(),
            "m.first_observation_seq IS NOT NULL",
            "m.current_observation_seq IS NOT NULL",
            "m.first_observation_seq <= ?",
            "m.current_observation_seq <= ?",
        ]
        values: list[Any] = [
            account_id,
            int(observation_watermark),
            int(observation_watermark),
        ]
        if query:
            clauses.append("LOWER(COALESCE(r.original_name, '')) LIKE ? ESCAPE '\\'")
            escaped = query.casefold().replace("\\", "\\\\").replace("%", "\\%")
            escaped = escaped.replace("_", "\\_")
            values.append(f"%{escaped}%")
        for selected, column in (
            (conversation_ids, "m.conversation_id"),
            (kinds, "r.kind"),
            (availability, "r.availability"),
        ):
            if selected:
                clauses.append(f"{column} IN ({','.join('?' for _ in selected)})")
                values.extend(selected)
        if after is not None:
            clauses.append("m.sent_at_utc >= ?")
            values.append(after)
        if before is not None:
            clauses.append("m.sent_at_utc < ?")
            values.append(before)
        if permitted_conversations is not None:
            if not permitted_conversations:
                return []
            clauses.append(
                f"m.conversation_id IN ({','.join('?' for _ in permitted_conversations)})"
            )
            values.extend(permitted_conversations)
        if denied_conversations:
            clauses.append(
                f"m.conversation_id NOT IN ({','.join('?' for _ in denied_conversations)})"
            )
            values.extend(denied_conversations)
        detected_mime = """
            COALESCE(
                (
                    SELECT ro_detected.mime_type
                    FROM resource_bindings AS rb_detected
                    JOIN resource_objects AS ro_detected USING(object_digest)
                    WHERE rb_detected.resource_id = r.resource_id
                      AND rb_detected.variant IN ('original', 'thumbnail')
                    ORDER BY rb_detected.variant = 'original' DESC
                    LIMIT 1
                ),
                r.mime_type
            )
        """
        family_clauses: list[str] = []
        for family in format_families:
            if family == "image":
                family_clauses.append(
                    f"(r.kind IN ('image','sticker') OR {detected_mime} LIKE 'image/%')"
                )
            elif family == "audio":
                family_clauses.append(f"(r.kind = 'voice' OR {detected_mime} LIKE 'audio/%')")
            elif family == "video":
                family_clauses.append(f"(r.kind = 'video' OR {detected_mime} LIKE 'video/%')")
            elif family == "pdf":
                family_clauses.append(f"{detected_mime} = 'application/pdf'")
            elif family == "workbook":
                family_clauses.append(
                    f"({detected_mime} IN ('application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet','text/csv','text/tab-separated-values'))"
                )
            elif family == "presentation":
                family_clauses.append(
                    f"{detected_mime} = 'application/vnd.openxmlformats-officedocument."
                    "presentationml.presentation'"
                )
            elif family == "archive":
                family_clauses.append(f"{detected_mime} = 'application/zip'")
            elif family == "text":
                family_clauses.append(f"{detected_mime} LIKE 'text/%'")
            elif family == "office":
                family_clauses.append(f"{detected_mime} LIKE 'application/vnd.%'")
            elif family == "binary":
                family_clauses.append(f"{detected_mime} = 'application/octet-stream'")
        if family_clauses:
            clauses.append("(" + " OR ".join(family_clauses) + ")")
        if position is not None:
            clauses.append(
                "(m.sent_at_utc, m.sort_seq, m.sort_tie, r.source_ordinal, r.resource_id) "
                "< (?, ?, ?, ?, ?)"
            )
            values.extend(position)
        with self.database.connection() as connection:
            return connection.execute(
                f"""
                SELECT r.*, m.account_id, m.conversation_id, m.source_message_id,
                       m.sender_id, m.sender_membership_id, m.sent_at_utc,
                       m.sort_seq, m.sort_tie, c.kind AS conversation_kind,
                       c.current_title AS conversation_title,
                       p.current_reader_label AS sender_label,
                       a.source_account_key, a.reader_timezone,
                       c.source_conversation_id
                FROM messages AS m INDEXED BY message_resource_discovery_timeline
                JOIN resources AS r USING(message_id)
                JOIN conversations AS c USING(conversation_id)
                JOIN accounts AS a USING(account_id)
                LEFT JOIN participants AS p ON p.participant_id = m.sender_id
                WHERE {" AND ".join(clauses)}
                  AND COALESCE(json_extract(r.resolver_json, '$.active'), 1) != 0
                ORDER BY m.sent_at_utc DESC, m.sort_seq DESC, m.sort_tie DESC,
                         r.source_ordinal DESC, r.resource_id DESC
                LIMIT ?
                """,
                (*values, int(limit)),
            ).fetchall()

    def resource_binding(self, resource_id: str, variant: str) -> sqlite3.Row | None:
        with self.database.connection() as connection:
            return connection.execute(
                """
                SELECT ro.*, rb.variant
                FROM resource_bindings AS rb
                JOIN resource_objects AS ro USING(object_digest)
                WHERE rb.resource_id = ? AND rb.variant = ?
                """,
                (resource_id, variant),
            ).fetchone()

    def upsert_resource_object(
        self,
        *,
        object_digest: str,
        local_path_internal: str,
        mime_type: str,
        byte_size: int,
        origin: str,
        observed_at: str,
    ) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO resource_objects(
                    object_digest, local_path_internal, mime_type,
                    byte_size, origin, created_at, last_verified_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(object_digest) DO UPDATE SET
                    local_path_internal = excluded.local_path_internal,
                    mime_type = excluded.mime_type,
                    byte_size = excluded.byte_size,
                    last_verified_at = excluded.last_verified_at
                """,
                (
                    object_digest,
                    local_path_internal,
                    mime_type,
                    int(byte_size),
                    origin,
                    observed_at,
                    observed_at,
                ),
            )

    def bind_resource_object(
        self,
        *,
        resource_id: str,
        object_digest: str,
        variant: str,
        created_at: str,
    ) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO resource_bindings(resource_id, object_digest, variant, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(resource_id, variant) DO UPDATE SET
                    object_digest = excluded.object_digest,
                    created_at = excluded.created_at
                """,
                (resource_id, object_digest, variant, created_at),
            )

    # -- durable resource-derivation jobs ---------------------------------
    #
    # Long resource derivations are handed to a bounded daemon worker. The job row is
    # the durable truth: ``recipe_json`` carries the exact already-validated read
    # arguments, and the lease/owner/fencing columns decide which worker may publish
    # the derived binding. Nothing about a job is exposed through MCP.

    def insert_resource_job(
        self,
        *,
        job_id: str,
        resource_id: str,
        resource_revision: str,
        recipe_digest: str,
        recipe_json: str,
        created_at: str,
    ) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO resource_jobs(
                    job_id, resource_id, resource_revision, recipe_digest, recipe_json,
                    state, attempt, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                (
                    job_id,
                    resource_id,
                    resource_revision,
                    recipe_digest,
                    recipe_json,
                    created_at,
                    created_at,
                ),
            )

    def resource_job(self, job_id: str) -> sqlite3.Row | None:
        with self.database.connection() as connection:
            return connection.execute(
                "SELECT * FROM resource_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()

    def active_resource_job(
        self, resource_id: str, resource_revision: str, recipe_digest: str
    ) -> sqlite3.Row | None:
        with self.database.connection() as connection:
            return connection.execute(
                """
                SELECT * FROM resource_jobs
                WHERE resource_id = ? AND resource_revision = ? AND recipe_digest = ?
                  AND state IN ('pending', 'leased', 'running')
                ORDER BY created_at, job_id LIMIT 1
                """,
                (resource_id, resource_revision, recipe_digest),
            ).fetchone()

    def latest_resource_job(
        self, resource_id: str, resource_revision: str, recipe_digest: str
    ) -> sqlite3.Row | None:
        with self.database.connection() as connection:
            return connection.execute(
                """
                SELECT * FROM resource_jobs
                WHERE resource_id = ? AND resource_revision = ? AND recipe_digest = ?
                ORDER BY created_at DESC, job_id DESC LIMIT 1
                """,
                (resource_id, resource_revision, recipe_digest),
            ).fetchone()

    def expired_resource_job_leases(self, now: str) -> list[sqlite3.Row]:
        with self.database.connection() as connection:
            return connection.execute(
                """
                SELECT * FROM resource_jobs WHERE state IN ('leased', 'running')
                  AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?
                ORDER BY created_at, job_id
                """,
                (now,),
            ).fetchall()

    def outstanding_resource_job_leases(self) -> list[sqlite3.Row]:
        with self.database.connection() as connection:
            return connection.execute(
                """
                SELECT * FROM resource_jobs WHERE state IN ('leased', 'running')
                ORDER BY created_at, job_id
                """
            ).fetchall()

    def update_resource_job(self, job_id: str, **values: Any) -> None:
        if not values:
            return
        with self.database.transaction() as connection:
            connection.execute(
                f"UPDATE resource_jobs SET {','.join(key + ' = ?' for key in values)} "
                "WHERE job_id = ?",
                (*values.values(), job_id),
            )

    def resource_job_counts(self) -> dict[str, int]:
        with self.database.connection() as connection:
            rows = connection.execute(
                "SELECT state, COUNT(*) AS n FROM resource_jobs GROUP BY state"
            ).fetchall()
        counts = {
            state: 0
            for state in (
                "pending",
                "leased",
                "running",
                "ready",
                "failed",
                "blocked",
                "cancelled",
            )
        }
        counts.update({str(row["state"]): int(row["n"]) for row in rows})
        return counts

    def prune_resource_jobs(self, *, keep_terminal: int) -> None:
        """Bound terminal job rows without touching active or leased work."""

        with self.database.transaction() as connection:
            connection.execute(
                """
                DELETE FROM resource_jobs WHERE job_id IN (
                    SELECT job_id FROM resource_jobs
                    WHERE state IN ('ready', 'failed', 'blocked', 'cancelled')
                    ORDER BY updated_at DESC, job_id DESC
                    LIMIT -1 OFFSET ?
                )
                """,
                (int(keep_terminal),),
            )

    def observation_bounds(self, conversation_id: str) -> tuple[str | None, str | None]:
        # Two edge lookups on ``message_timeline`` (oldest and newest) instead of a
        # ``MIN/MAX(sent_at_utc)`` aggregate, which SQLite answered with a full scan of a
        # very large conversation. ``sort_primary`` carries ``sent_at_utc``, so the first
        # and last timeline rows give the same bounds.
        with self.database.connection() as connection:
            oldest = connection.execute(
                """
                SELECT sent_at_utc FROM messages
                WHERE conversation_id = ?
                ORDER BY sort_primary ASC, sort_seq ASC, sort_tie ASC,
                         source_message_id ASC
                LIMIT 1
                """,
                (conversation_id,),
            ).fetchone()
            newest = connection.execute(
                """
                SELECT sent_at_utc FROM messages
                WHERE conversation_id = ?
                ORDER BY sort_primary DESC, sort_seq DESC, sort_tie DESC,
                         source_message_id DESC
                LIMIT 1
                """,
                (conversation_id,),
            ).fetchone()
        if oldest is None or newest is None:
            return None, None
        return str(oldest[0]), str(newest[0])

    def timeline_cursor_count(self, conversation_id: str, scope_kind: str) -> int:
        with self.database.connection() as connection:
            return int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM reader_timeline_cursors
                    WHERE conversation_id = ? AND scope_kind = ?
                    """,
                    (conversation_id, scope_kind),
                ).fetchone()[0]
            )

    def account_conversations(self, account_id: str) -> list[sqlite3.Row]:
        with self.database.connection() as connection:
            return connection.execute(
                """
                SELECT * FROM conversations
                WHERE account_id = ? AND visibility_state = 'active'
                ORDER BY conversation_id
                """,
                (account_id,),
            ).fetchall()

    def record_source_catalog_state(
        self,
        *,
        account_id: str,
        inventory_epoch: str,
        coverage_state: str,
        observed_at: str,
        error_code: str | None = None,
        next_cursor_token: str | None = None,
    ) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                """
                UPDATE accounts SET source_inventory_epoch = ? WHERE account_id = ?
                """,
                (inventory_epoch, account_id),
            )
            connection.execute(
                """
                INSERT INTO source_catalog_state(
                    account_id, source_inventory_epoch, coverage_state,
                    next_cursor_token, scan_started_at, scan_completed_at,
                    last_observed_at, last_error_code, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    source_inventory_epoch = excluded.source_inventory_epoch,
                    coverage_state = excluded.coverage_state,
                    next_cursor_token = excluded.next_cursor_token,
                    scan_completed_at = excluded.scan_completed_at,
                    last_observed_at = excluded.last_observed_at,
                    last_error_code = excluded.last_error_code,
                    updated_at = excluded.updated_at
                """,
                (
                    account_id,
                    inventory_epoch,
                    coverage_state,
                    next_cursor_token,
                    observed_at,
                    observed_at,
                    observed_at,
                    error_code,
                    observed_at,
                ),
            )

    def source_catalog_state(self, account_id: str) -> sqlite3.Row | None:
        with self.database.connection() as connection:
            return connection.execute(
                "SELECT * FROM source_catalog_state WHERE account_id = ?",
                (account_id,),
            ).fetchone()

    def indexed_conversation_ids(
        self,
        account_id: str,
        *,
        inventory_epoch: str | None = None,
    ) -> set[str]:
        """Conversation ids carrying a tail for the requested projection epoch."""

        with self.database.connection() as connection:
            epoch_clause = " AND s.source_inventory_epoch = ?" if inventory_epoch else ""
            params: tuple[Any, ...] = (
                (
                    account_id,
                    inventory_epoch,
                )
                if inventory_epoch
                else (account_id,)
            )
            rows = connection.execute(
                f"""
                SELECT s.conversation_id
                FROM source_conversation_state AS s
                JOIN conversations AS c USING(conversation_id)
                WHERE c.account_id = ?
                {epoch_clause}
                """,
                params,
            ).fetchall()
        return {str(row["conversation_id"]) for row in rows}

    def current_catalog_conversation_ids(self, account_id: str) -> set[str]:
        """Conversation ids observed in the account's most recent complete catalog scan.

        A conversation is current only when its ``catalog_observed_at`` matches the
        account's ``source_catalog_state.last_observed_at``. Rows left over from an
        earlier observation (for example a session that dropped out of the latest
        complete catalog) stay persisted for history but are not part of the current
        projection-completion set.
        """

        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT c.conversation_id
                FROM conversations AS c
                JOIN source_catalog_state AS s ON s.account_id = c.account_id
                WHERE c.account_id = ?
                  AND c.catalog_observed_at IS NOT NULL
                  AND c.catalog_observed_at = s.last_observed_at
                """,
                (account_id,),
            ).fetchall()
        return {str(row["conversation_id"]) for row in rows}

    def degraded_conversation_ids(
        self,
        account_id: str,
        *,
        error_code: str,
    ) -> set[str]:
        """Conversation ids explicitly degraded with the exact attention error code."""

        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT s.conversation_id
                FROM source_conversation_state AS s
                JOIN conversations AS c USING(conversation_id)
                WHERE c.account_id = ? AND s.last_error_code = ?
                """,
                (account_id, error_code),
            ).fetchall()
        return {str(row["conversation_id"]) for row in rows}

    def source_conversation_tail_times(self, account_id: str) -> dict[str, str | None]:
        """Return admitted source-tail timestamps for one account's scheduler."""

        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT s.conversation_id, s.tail_sort_primary
                FROM source_conversation_state AS s
                JOIN conversations AS c USING(conversation_id)
                WHERE c.account_id = ?
                """,
                (account_id,),
            ).fetchall()
        return {
            str(row["conversation_id"]): (
                str(row["tail_sort_primary"]) if row["tail_sort_primary"] is not None else None
            )
            for row in rows
        }

    def source_shard_generations(self, account_id: str) -> dict[str, str]:
        """Return the last fully admitted physical generation for each source shard."""

        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT source_shard_key, source_generation_id
                FROM source_shard_state
                WHERE account_id = ?
                  AND availability_state = 'available'
                  AND source_generation_id IS NOT NULL
                """,
                (account_id,),
            ).fetchall()
        return {str(row["source_shard_key"]): str(row["source_generation_id"]) for row in rows}

    def mark_source_conversation_attention(
        self,
        *,
        conversation_id: str,
        backfill_state: str,
        error_code: str,
        observed_at: str,
    ) -> None:
        """Record a degraded per-conversation state without touching its resume tail.

        ``record_source_conversation_state`` owns the whole row and clears the tail when
        it is not re-observed; a conversation that was skipped needs the opposite, so this
        narrower writer updates only the observation state.
        """

        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO source_conversation_state(
                    conversation_id, backfill_state, last_error_code, updated_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(conversation_id) DO UPDATE SET
                    backfill_state = excluded.backfill_state,
                    last_error_code = excluded.last_error_code,
                    updated_at = excluded.updated_at
                """,
                (conversation_id, backfill_state, error_code, observed_at),
            )

    def record_source_conversation_state(
        self,
        *,
        conversation_id: str,
        inventory_epoch: str,
        tail: SourceSortKey | None,
        indexed_before: str | None,
        indexed_after: str | None,
        backfill_state: str,
        observed_at: str,
        error_code: str | None = None,
        coverage_version: int | None = None,
        contiguous_floor: SourceSortKey | None = None,
        history_complete: bool | None = None,
        forward_complete: bool | None = None,
    ) -> None:
        with self.database.transaction() as connection:
            previous = connection.execute(
                "SELECT * FROM source_conversation_state WHERE conversation_id=?",
                (conversation_id,),
            ).fetchone()
            version = (
                coverage_version
                if coverage_version is not None
                else int(previous["coverage_version"] if previous else 0)
            )
            history = (
                history_complete
                if history_complete is not None
                else bool(previous["history_complete"] if previous else False)
            )
            forward = (
                forward_complete
                if forward_complete is not None
                else bool(previous["forward_complete"] if previous else False)
            )
            floor = (
                position(contiguous_floor)
                if contiguous_floor
                else (
                    previous["contiguous_floor_position"]
                    if previous
                    and version
                    and previous["source_inventory_epoch"] == inventory_epoch
                    else None
                )
            )
            if backfill_state == "complete" and not (version and history and forward):
                backfill_state = "partial"
            connection.execute(
                """
                INSERT INTO source_conversation_state(
                    conversation_id, source_inventory_epoch, tail_cursor_token,
                    tail_generation_id, tail_sort_primary, tail_sort_seq,
                    tail_sort_tie, tail_source_message_id, tail_observed_at,
                    indexed_before, indexed_after, backfill_state,
                    last_error_code, updated_at, coverage_version,
                    contiguous_floor_position, history_complete, forward_complete
                ) VALUES (?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(conversation_id) DO UPDATE SET
                    source_inventory_epoch = excluded.source_inventory_epoch,
                    tail_sort_primary = excluded.tail_sort_primary,
                    tail_sort_seq = excluded.tail_sort_seq,
                    tail_sort_tie = excluded.tail_sort_tie,
                    tail_source_message_id = excluded.tail_source_message_id,
                    tail_observed_at = excluded.tail_observed_at,
                    indexed_before = excluded.indexed_before,
                    indexed_after = excluded.indexed_after,
                    backfill_state = excluded.backfill_state,
                    last_error_code = excluded.last_error_code,
                    updated_at = excluded.updated_at,
                    coverage_version = excluded.coverage_version,
                    contiguous_floor_position = excluded.contiguous_floor_position,
                    history_complete = excluded.history_complete,
                    forward_complete = excluded.forward_complete
                """,
                (
                    conversation_id,
                    inventory_epoch,
                    tail.sent_at_utc if tail else None,
                    tail.sort_seq if tail else None,
                    tail.source_rowid if tail else None,
                    tail.source_message_id if tail else None,
                    observed_at,
                    indexed_before,
                    indexed_after,
                    backfill_state,
                    error_code,
                    observed_at,
                    version,
                    floor,
                    int(history),
                    int(forward),
                ),
            )

    def record_source_shard_states(
        self,
        *,
        account_id: str,
        inventory_epoch: str,
        generation_by_shard: tuple[tuple[str, str], ...],
        observed_at: str,
    ) -> None:
        current_keys = tuple(shard_key for shard_key, _generation in generation_by_shard)
        with self.database.transaction() as connection:
            for shard_key, generation_id in generation_by_shard:
                connection.execute(
                    """
                    INSERT INTO source_shard_state(
                        account_id, source_shard_key, source_inventory_epoch,
                        source_generation_id, availability_state, cursor_token,
                        last_sort_primary, last_sort_seq, last_sort_tie,
                        last_source_message_id, discovered_at, last_verified_at,
                        last_error_code, updated_at
                    ) VALUES (?, ?, ?, ?, 'available', NULL, NULL, NULL, NULL,
                              NULL, ?, ?, NULL, ?)
                    ON CONFLICT(account_id, source_shard_key) DO UPDATE SET
                        source_inventory_epoch = excluded.source_inventory_epoch,
                        source_generation_id = excluded.source_generation_id,
                        availability_state = 'available',
                        last_verified_at = excluded.last_verified_at,
                        last_error_code = NULL,
                        updated_at = excluded.updated_at
                    """,
                    (
                        account_id,
                        shard_key,
                        inventory_epoch,
                        generation_id,
                        observed_at,
                        observed_at,
                        observed_at,
                    ),
                )
            if current_keys:
                placeholders = ",".join("?" for _value in current_keys)
                connection.execute(
                    f"""
                    UPDATE source_shard_state
                    SET source_inventory_epoch = ?, availability_state = 'missing',
                        last_error_code = 'SOURCE_INCOMPLETE', updated_at = ?
                    WHERE account_id = ?
                      AND source_shard_key NOT IN ({placeholders})
                    """,
                    (inventory_epoch, observed_at, account_id, *current_keys),
                )
            else:
                connection.execute(
                    """
                    UPDATE source_shard_state
                    SET source_inventory_epoch = ?, availability_state = 'missing',
                        last_error_code = 'SOURCE_INCOMPLETE', updated_at = ?
                    WHERE account_id = ?
                    """,
                    (inventory_epoch, observed_at, account_id),
                )

    def source_shard_state_counts(self, account_id: str) -> dict[str, int]:
        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT availability_state, COUNT(*) AS value
                FROM source_shard_state
                WHERE account_id = ?
                GROUP BY availability_state
                """,
                (account_id,),
            ).fetchall()
        return {str(row["availability_state"]): int(row["value"]) for row in rows}

    def source_conversation_state(self, conversation_id: str) -> dict[str, Any] | None:
        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT * FROM source_conversation_state WHERE conversation_id = ?",
                (conversation_id,),
            ).fetchone()
        if row is None:
            return None
        state = dict(row)
        # v8 min/max or a legacy complete flag supplies no contiguous proof.
        if state["backfill_state"] == "complete" and not (
            state["coverage_version"] and state["history_complete"] and state["forward_complete"]
        ):
            state["backfill_state"] = "partial"
        return state

    def retrieval_candidate_resident(
        self,
        conversation_ids: tuple[str, ...],
        *,
        epoch: str | None = None,
    ) -> bool:
        """Whether every in-scope row currently has a locally readable body.

        The derived link and lexical indexes are built from canonical rows and stay
        installed across body expiry, so recall can reference an observation whose
        body is no longer locally readable. This is a bounded existence probe: it
        answers ``False`` (source preparation required) as soon as any in-scope row
        is missing a currently-readable body -- including stock release, which clears
        ``projection_epoch`` and evicts bodies while older backfill flags may still
        read complete.

        A released row's NULL epoch and an old projection both make the scope
        cold; filtering either out before the existence check would hide missing
        coverage.
        """

        if not conversation_ids:
            return True
        placeholders = ",".join("?" for _ in conversation_ids)
        with self.database.connection() as connection:
            row = connection.execute(
                f"""SELECT 1 FROM messages AS m
                WHERE m.conversation_id IN ({placeholders})
                  AND m.current_state='present'
                  AND (m.body_available=0 OR m.current_observation_seq IS NULL
                       OR (? IS NOT NULL AND m.projection_epoch IS NOT ?) OR EXISTS(
                       SELECT 1 FROM body_release_jobs AS br
                       WHERE br.message_id=m.message_id) OR EXISTS(
                       SELECT 1 FROM message_body_residency AS rb
                       WHERE rb.message_id=m.message_id
                         AND rb.expires_at IS NOT NULL
                         AND julianday(rb.expires_at)<=julianday('now')))
                LIMIT 1""",
                (*conversation_ids, epoch, epoch),
            ).fetchone()
        return row is None

    def create_backfill_job(
        self,
        *,
        job_id: str,
        account_id: str,
        conversation_id: str,
        inventory_epoch: str,
        requested_after: str | None,
        requested_before: str | None,
        max_messages: int,
        created_at: str,
    ) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO source_backfill_jobs(
                    job_id, account_id, conversation_id, source_inventory_epoch,
                    requested_after, requested_before, max_messages,
                    processed_messages, cursor_token, state, created_at,
                    started_at, updated_at, completed_at, last_error_code
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, 'queued', ?, NULL, ?, NULL, NULL)
                """,
                (
                    job_id,
                    account_id,
                    conversation_id,
                    inventory_epoch,
                    requested_after,
                    requested_before,
                    int(max_messages),
                    created_at,
                    created_at,
                ),
            )

    def next_backfill_job(
        self, conversation_ids: tuple[str, ...] | None = None
    ) -> sqlite3.Row | None:
        if conversation_ids == ():
            return None
        scope = (
            " AND conversation_id IN (" + ",".join("?" for _ in conversation_ids) + ")"
            if conversation_ids is not None
            else ""
        )
        with self.database.connection() as connection:
            return connection.execute(
                f"""
                SELECT * FROM source_backfill_jobs
                WHERE state IN ('queued', 'running') {scope}
                ORDER BY CASE state WHEN 'running' THEN 0 ELSE 1 END,
                         updated_at, created_at
                LIMIT 1
                """,
                conversation_ids or (),
            ).fetchone()

    def update_backfill_job(
        self,
        job_id: str,
        *,
        state: str,
        processed_messages: int,
        updated_at: str,
        error_code: str | None = None,
    ) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                """
                UPDATE source_backfill_jobs
                SET state = ?, processed_messages = ?,
                    started_at = CASE
                        WHEN started_at IS NULL AND ? = 'running' THEN ?
                        ELSE started_at
                    END,
                    completed_at = CASE
                        WHEN ? IN ('completed', 'failed') THEN ?
                        ELSE completed_at
                    END,
                    last_error_code = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (
                    state,
                    int(processed_messages),
                    state,
                    updated_at,
                    state,
                    updated_at,
                    error_code,
                    updated_at,
                    job_id,
                ),
            )

    def set_backfills_paused(self, paused: bool, *, updated_at: str) -> int:
        with self.database.transaction() as connection:
            if paused:
                cursor = connection.execute(
                    """
                    UPDATE source_backfill_jobs SET state = 'paused', updated_at = ?
                    WHERE state IN ('queued', 'running')
                    """,
                    (updated_at,),
                )
            else:
                cursor = connection.execute(
                    """
                    UPDATE source_backfill_jobs SET state = 'queued', updated_at = ?
                    WHERE state = 'paused'
                    """,
                    (updated_at,),
                )
        return int(cursor.rowcount)

    def backfill_status_counts(self) -> dict[str, int]:
        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT state, COUNT(*) AS job_count,
                       COALESCE(SUM(processed_messages), 0) AS processed_messages
                FROM source_backfill_jobs GROUP BY state ORDER BY state
                """
            ).fetchall()
        return {str(row["state"]): int(row["job_count"]) for row in rows}

    def observation_watermark(self) -> int:
        """Highest observation sequence in the store, used as the inbox's initial as-of point.

        An inbox page is the per-conversation latest message *as of* one watermark
        (``inbox_rows`` keeps only observations with ``observation_seq <= watermark``).
        Any watermark at or above an account's own newest observation selects exactly that
        account's current view, because the row query is already scoped to the account, so
        the global maximum is a valid watermark for every account. Reading it is a single
        bounded primary-key lookup instead of one observation probe per message: the
        account-scoped form measured ~41 s against a 177k-message store, this one ~1 ms.
        """

        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(observation_seq), 0) FROM message_observations"
            ).fetchone()
        return int(row[0])

    def resident_conversation_ids(self, account_id: str, projection_epoch: str) -> set[str]:
        # Drive the resident set, not the durable account history. Enumerating the
        # account's conversations via ``conversation_catalog`` and probing each one
        # with an EXISTS seek into the leading ``conversation_id`` column of the
        # ``message_resident_timeline`` partial index keeps the work proportional to
        # the number of conversations plus the (bounded) resident rows. Selecting
        # ``DISTINCT conversation_id`` from ``messages`` by ``account_id`` instead
        # walked every durable skeleton in the account, which is linear in history
        # even when nothing is resident.
        with self.database.connection() as connection:
            return {
                str(row[0]) for row in connection.execute(
                    f"""SELECT c.conversation_id FROM conversations AS c
                    WHERE c.account_id=?
                      AND EXISTS (
                          SELECT 1 FROM messages AS m
                              INDEXED BY message_resident_timeline
                          WHERE m.conversation_id=c.conversation_id
                            AND m.account_id=c.account_id
                            AND m.projection_epoch=? AND m.current_state='present'
                            AND {resident_body_predicate()}
                      )""",
                    (account_id, projection_epoch),
                )
            }

    def inbox_rows(self, account_id: str, *, observation_seq: int) -> list[sqlite3.Row]:
        with self.database.connection() as connection:
            return connection.execute(
                f"""
                SELECT c.conversation_id, c.account_id, c.kind, c.current_title,
                       c.last_message_at, c.unread_count, c.catalog_state,
                       r.message_id AS latest_message_id,
                       r.sent_at_utc AS latest_sent_at,
                       r.kind AS latest_kind,
                       r.text AS latest_text,
                       r.sender_id AS latest_sender_id,
                       r.sender_label_snapshot_json AS latest_sender_snapshot,
                       p.current_reader_label AS latest_sender_label,
                       p.is_self AS latest_sender_is_self,
                       p.resolution_state AS latest_sender_resolution_state,
                       p.identity_confidence AS latest_sender_identity_confidence
                FROM conversations AS c
                JOIN messages AS r ON r.message_id = (
                    SELECT candidate.message_id
                    FROM messages AS candidate
                    WHERE candidate.conversation_id = c.conversation_id
                      AND candidate.current_state = 'present'
                      AND {resident_body_predicate('candidate')}
                      AND EXISTS (
                          SELECT 1 FROM message_observations AS observed
                          WHERE observed.message_id = candidate.message_id
                            AND observed.observation_seq <= ?
                      )
                    ORDER BY candidate.sort_primary DESC,
                             candidate.sort_seq DESC,
                             candidate.sort_tie DESC,
                             candidate.source_message_id DESC
                    LIMIT 1
                )
                LEFT JOIN participants AS p ON p.participant_id = r.sender_id
                WHERE c.account_id = ? AND c.visibility_state = 'active'
                ORDER BY r.sent_at_utc DESC, c.kind ASC, c.conversation_id ASC
                """,
                (int(observation_seq), account_id),
            ).fetchall()

    def current_message_ids(self, conversation_ids: tuple[str, ...]) -> set[str]:
        selected = tuple(dict.fromkeys(conversation_ids))
        if not selected:
            return set()
        with self.database.connection() as connection:
            rows = connection.execute(
                f"""
                SELECT message_id FROM messages
                WHERE conversation_id IN ({",".join("?" for _ in selected)})
                  AND current_state = 'present'
                  AND {resident_body_predicate('messages')}
                """,
                selected,
            ).fetchall()
        return {str(row[0]) for row in rows}

    def participant_account_id(self, participant_id: str) -> str | None:
        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT account_id FROM participants WHERE participant_id = ?",
                (participant_id,),
            ).fetchone()
        return str(row[0]) if row is not None else None

    def message_position_row(self, message_id: str) -> sqlite3.Row | None:
        with self.database.connection() as connection:
            return connection.execute(
                """
                SELECT m.*, p.resolution_state AS sender_resolution_state,
                       p.identity_confidence AS sender_identity_confidence
                FROM messages AS m
                LEFT JOIN participants AS p ON p.participant_id = m.sender_id
                WHERE m.message_id = ?
                """,
                (message_id,),
            ).fetchone()

    def frozen_message_rows(self, message_ids: tuple[str, ...]) -> list[sqlite3.Row]:
        """Re-read the frozen canonical rows of one admitted message set.

        Unlike ``message_rows`` this is not conversation-scoped: a search page can
        admit rows from several conversations and must project the exact version it
        just committed.
        """

        selected = tuple(dict.fromkeys(message_ids))
        if not selected:
            return []
        with self.database.connection() as connection:
            return connection.execute(
                f"""
                SELECT m.*, p.resolution_state AS sender_resolution_state,
                       p.identity_confidence AS sender_identity_confidence
                FROM messages AS m
                LEFT JOIN participants AS p ON p.participant_id = m.sender_id
                WHERE m.message_id IN ({",".join("?" for _ in selected)})
                  AND {resident_body_predicate()}
                """,
                selected,
            ).fetchall()

    def commit_timeline_position(
        self,
        *,
        reader_id: str,
        conversation_id: str,
        scope_kind: str,
        scope_key: str,
        row: Any,
        updated_at: str,
        seed_update_cursor: bool,
        admitted_message_ids: tuple[str, ...],
        observation_watermark: int | None = None,
    ) -> None:
        tie = json.dumps(
            [int(row["sort_seq"]), int(row["sort_tie"]), str(row["source_message_id"])],
            separators=(",", ":"),
        )
        # Advancing an already admitted page's bounded reader positions is
        # maintenance, not new source/derivative admission. An enclosing source
        # admission keeps its ordinary lease; a materialized read can use the
        # maintenance allowance without weakening the filesystem free floor.
        with self.database.transaction(maintenance=True) as connection:
            existing = connection.execute(
                """
                SELECT committed_sort_primary, committed_sort_tie
                FROM reader_timeline_cursors
                WHERE reader_id = ? AND conversation_id = ?
                  AND scope_kind = ? AND scope_key = ?
                """,
                (reader_id, conversation_id, scope_kind, scope_key),
            ).fetchone()
            candidate_key = (
                str(row["sort_primary"]),
                int(row["sort_seq"]),
                int(row["sort_tie"]),
                str(row["source_message_id"]),
            )
            should_advance = existing is None
            if existing is not None:
                existing_tie = json.loads(str(existing["committed_sort_tie"]))
                existing_key = (
                    str(existing["committed_sort_primary"]),
                    int(existing_tie[0]),
                    int(existing_tie[1]),
                    str(existing_tie[2]),
                )
                should_advance = candidate_key > existing_key
            if should_advance:
                connection.execute(
                    """
                    INSERT INTO reader_timeline_cursors(
                        reader_id, conversation_id, scope_kind, scope_key,
                        committed_sort_primary, committed_sort_tie,
                        committed_message_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(reader_id, conversation_id, scope_kind, scope_key)
                    DO UPDATE SET
                        committed_sort_primary = excluded.committed_sort_primary,
                        committed_sort_tie = excluded.committed_sort_tie,
                        committed_message_id = excluded.committed_message_id,
                        updated_at = excluded.updated_at
                    """,
                    (
                        reader_id,
                        conversation_id,
                        scope_kind,
                        scope_key,
                        str(row["sort_primary"]),
                        tie,
                        str(row["message_id"]),
                        updated_at,
                    ),
                )
            if not seed_update_cursor or not admitted_message_ids:
                return
            placeholders = ",".join("?" for _ in admitted_message_ids)
            observed = connection.execute(
                f"""
                SELECT MAX(observation_seq) FROM message_observations
                WHERE message_id IN ({placeholders})
                  AND (? IS NULL OR observation_seq <= ?)
                """,
                (*admitted_message_ids, observation_watermark, observation_watermark),
            ).fetchone()[0]
            if observed is not None:
                self._advance_update_cursor(
                    connection,
                    reader_id=reader_id,
                    conversation_id=conversation_id,
                    scope_kind=scope_kind,
                    scope_key=scope_key,
                    position=int(observed),
                    updated_at=updated_at,
                )

    @staticmethod
    def _advance_update_cursor(
        connection: sqlite3.Connection,
        *,
        reader_id: str,
        conversation_id: str,
        scope_kind: str,
        scope_key: str,
        position: int,
        updated_at: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO reader_update_cursors(
                reader_id, conversation_id, scope_kind, scope_key,
                committed_observation_seq, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(reader_id, conversation_id, scope_kind, scope_key)
            DO UPDATE SET
                committed_observation_seq = MAX(
                    reader_update_cursors.committed_observation_seq,
                    excluded.committed_observation_seq
                ),
                updated_at = excluded.updated_at
            """,
            (reader_id, conversation_id, scope_kind, scope_key, int(position), updated_at),
        )

    def timeline_position(
        self, reader_id: str, conversation_id: str, scope_kind: str, scope_key: str
    ) -> sqlite3.Row | None:
        with self.database.connection() as connection:
            return connection.execute(
                """
                SELECT * FROM reader_timeline_cursors
                WHERE reader_id = ? AND conversation_id = ?
                  AND scope_kind = ? AND scope_key = ?
                """,
                (reader_id, conversation_id, scope_kind, scope_key),
            ).fetchone()

    def update_position(
        self, reader_id: str, conversation_id: str, scope_kind: str, scope_key: str
    ) -> int:
        with self.database.connection() as connection:
            row = connection.execute(
                """
                SELECT committed_observation_seq FROM reader_update_cursors
                WHERE reader_id = ? AND conversation_id = ?
                  AND scope_kind = ? AND scope_key = ?
                """,
                (reader_id, conversation_id, scope_kind, scope_key),
            ).fetchone()
        return int(row[0]) if row is not None else 0

    def observation_rows_after(
        self,
        conversation_id: str,
        observation_seq: int,
        participant_ids: tuple[str, ...] = (),
    ) -> list[sqlite3.Row]:
        clauses = ["m.conversation_id = ?", "mo.observation_seq > ?"]
        values: list[Any] = [conversation_id, int(observation_seq)]
        if participant_ids:
            clauses.append(f"m.sender_id IN ({','.join('?' for _ in participant_ids)})")
            values.extend(participant_ids)
        with self.database.connection() as connection:
            return connection.execute(
                f"""
                SELECT mo.observation_seq, mo.observation_id,
                       m.*, p.resolution_state AS sender_resolution_state,
                       p.identity_confidence AS sender_identity_confidence
                FROM message_observations AS mo
                JOIN messages AS m USING(message_id)
                LEFT JOIN participants AS p ON p.participant_id = m.sender_id
                WHERE {" AND ".join(clauses)}
                ORDER BY mo.observation_seq ASC
                """,
                values,
            ).fetchall()

    def pending_delivery(
        self, reader_id: str, conversation_id: str, scope_kind: str, scope_key: str
    ) -> sqlite3.Row | None:
        with self.database.connection() as connection:
            return connection.execute(
                """
                SELECT * FROM reader_deliveries
                WHERE reader_id = ? AND conversation_id = ?
                  AND scope_kind = ? AND scope_key = ? AND status = 'pending'
                """,
                (reader_id, conversation_id, scope_kind, scope_key),
            ).fetchone()

    def delivery(self, delivery_id: str) -> sqlite3.Row | None:
        with self.database.connection() as connection:
            return connection.execute(
                "SELECT * FROM reader_deliveries WHERE delivery_id = ?",
                (delivery_id,),
            ).fetchone()

    def create_pending_delivery(
        self,
        *,
        delivery_id: str,
        reader_id: str,
        conversation_id: str,
        scope_kind: str,
        scope_key: str,
        from_observation_seq: int,
        to_observation_seq: int,
        payload_digest: str,
        payload_ref: str,
        projection_schema_version: str,
        created_at: str,
    ) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO reader_deliveries(
                    delivery_id, reader_id, conversation_id, scope_kind,
                    scope_key, from_observation_seq, to_observation_seq,
                    projection_schema_version, payload_digest, payload_ref,
                    status, created_at, acknowledged_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL, NULL)
                """,
                (
                    delivery_id,
                    reader_id,
                    conversation_id,
                    scope_kind,
                    scope_key,
                    int(from_observation_seq),
                    int(to_observation_seq),
                    projection_schema_version,
                    payload_digest,
                    payload_ref,
                    created_at,
                ),
            )

    def acknowledge_delivery(
        self,
        *,
        delivery_id: str,
        reader_id: str,
        conversation_id: str,
        scope_kind: str,
        scope_key: str,
        acknowledged_at: str,
    ) -> None:
        with self.database.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM reader_deliveries WHERE delivery_id = ?",
                (delivery_id,),
            ).fetchone()
            if row is None or any(
                str(row[key]) != expected
                for key, expected in (
                    ("reader_id", reader_id),
                    ("conversation_id", conversation_id),
                    ("scope_kind", scope_kind),
                    ("scope_key", scope_key),
                )
            ):
                raise SightglassError(ErrorCode.DELIVERY_ACK_INVALID)
            if str(row["status"]) == "acknowledged":
                return
            if str(row["status"]) != "pending":
                raise SightglassError(ErrorCode.DELIVERY_ACK_INVALID)
            self._advance_update_cursor(
                connection,
                reader_id=reader_id,
                conversation_id=conversation_id,
                scope_kind=scope_kind,
                scope_key=scope_key,
                position=int(row["to_observation_seq"]),
                updated_at=acknowledged_at,
            )
            connection.execute(
                """
                UPDATE reader_deliveries
                SET status = 'acknowledged', acknowledged_at = ?
                WHERE delivery_id = ? AND status = 'pending'
                """,
                (acknowledged_at, delivery_id),
            )

    def search_candidate_window(
        self,
        conversation_ids: tuple[str, ...],
        *,
        after_key: tuple[str, int, int, str] | None = None,
        after_utc: str | None = None,
        before_utc: str | None = None,
        participant_ids: tuple[str, ...] = (),
        projection_epoch: str | None = None,
        observation_watermark: int | None = None,
        lexical_queries: tuple[str, ...] = (),
        limit: int,
    ) -> list[sqlite3.Row]:
        """Merge bounded timeline positions, then fetch only the selected bodies.

        Both a text LIKE predicate and a multi-conversation IN/ORDER BY can scan
        full histories before LIMIT. Seek each conversation's existing timeline
        index separately; at most ``limit`` positions per conversation can enter
        the global first ``limit``. Query matching remains in the bounded reader
        scan, followed by current-source verification of each potential hit.
        """

        if not conversation_ids or int(limit) < 1:
            return []
        boundary = ""
        values: tuple[Any, ...] = ()
        # Coverage probe scope: the same epoch/time/participant/watermark filters
        # but *never* the pagination ``after_key``. The probe answers whether the
        # whole recall scope is index-covered; evaluating it with the page
        # boundary would hide an uncovered resident behind the page and wrongly
        # permit the MATCH prefilter on later pages.
        coverage_boundary = ""
        coverage_values: tuple[Any, ...] = ()
        if after_key is not None:
            boundary = "AND (sort_primary, sort_seq, sort_tie, message_id) > (?, ?, ?, ?)"
            values = after_key
        if after_utc is not None:
            clause = " AND sort_primary >= ? AND sent_at_utc >= ?"
            boundary += clause
            coverage_boundary += clause
            values += (after_utc, after_utc)
            coverage_values += (after_utc, after_utc)
        if before_utc is not None:
            clause = " AND sort_primary < ? AND sent_at_utc < ?"
            boundary += clause
            coverage_boundary += clause
            values += (before_utc, before_utc)
            coverage_values += (before_utc, before_utc)
        if participant_ids:
            clause = f" AND sender_id IN ({','.join('?' for _ in participant_ids)})"
            boundary += clause
            coverage_boundary += clause
            values += participant_ids
            coverage_values += participant_ids
        if projection_epoch is not None:
            clause = " AND projection_epoch = ?"
            boundary += clause
            coverage_boundary += clause
            values += (projection_epoch,)
            coverage_values += (projection_epoch,)
        # Search only ever returns a locally readable body, so drive the seek from
        # the resident partial index (which materialises exactly ``body_available=1``
        # in ``(conversation_id, sort_primary, sort_seq, sort_tie, source_message_id)``
        # order). The materialized timeline index leads with ``projection_epoch``,
        # so an explicit epoch seek then filters the resident predicate *after*
        # LIMIT: with no resident bodies it walked every skeleton in the epoch
        # (linear in durable history). The resident index keeps the seek bounded by
        # the conversation's resident rows while preserving the same tie order, and
        # the epoch/watermark/boundary clauses stay as remainder filters.
        timeline_index = "message_resident_timeline"
        if observation_watermark is not None:
            clause = " AND first_observation_seq <= ? AND current_observation_seq <= ?"
            boundary += clause
            coverage_boundary += clause
            values += (observation_watermark, observation_watermark)
            coverage_values += (observation_watermark, observation_watermark)

        def position(row: sqlite3.Row) -> tuple[str, int, int, str]:
            return (
                str(row["sort_primary"]),
                int(row["sort_seq"]),
                int(row["sort_tie"]),
                str(row["message_id"]),
            )

        with self.database.connection() as connection:
            # Several seeks must observe the same durable index view, just as the
            # former single SELECT did. Reuse an enclosing admission transaction.
            if not connection.in_transaction:
                connection.execute("BEGIN")
            expressions = [candidate_expression(query) for query in lexical_queries]
            lexical_ready = connection.execute(
                "SELECT state FROM derived_index_state WHERE index_kind='lexical'"
            ).fetchone()
            # Incomplete/rebuilding indexes cannot narrow recall. An unsupported OR
            # branch also requires fallback, otherwise it would silently disappear.
            # A "ready" state is necessary but not sufficient: a current resident
            # whose lexical receipt is missing, carries an outdated recipe, or
            # trails its observation version is not covered by the FTS rows, so a
            # MATCH prefilter would silently drop it. Only narrow when every
            # in-scope resident carries a current receipt; otherwise keep the
            # bounded canonical fallback.
            if (
                expressions
                and all(expressions)
                and lexical_ready is not None
                and lexical_ready[0] == "ready"
                and lexical_index_covers_residents(
                    connection, conversation_ids, coverage_boundary, coverage_values
                )
            ):
                expression = " OR ".join("(" + str(value) + ")" for value in expressions)
                boundary += (
                    " AND rowid IN "
                    "(SELECT rowid FROM message_lexical WHERE message_lexical MATCH ?)"
                )
                values += (expression,)
                boundary += (
                    " AND EXISTS (SELECT 1 FROM message_lexical_projection AS lexical_projection "
                    "WHERE lexical_projection.message_id=messages.message_id "
                    "AND lexical_projection.recipe=?)"
                )
                values += (LEXICAL_RECIPE,)
            with ExitStack() as cursors:
                windows = []
                for conversation_id in dict.fromkeys(conversation_ids):
                    check_operation_budget()
                    cursor = connection.execute(
                        f"""
                        SELECT message_id, sort_primary, sort_seq, sort_tie
                        FROM messages INDEXED BY {timeline_index}
                        WHERE conversation_id = ?
                          AND current_state='present'
                          AND {resident_body_predicate('messages')} {boundary}
                        ORDER BY sort_primary, sort_seq, sort_tie, message_id LIMIT ?
                        """,
                        (conversation_id, *values, int(limit)),
                    )
                    windows.append(cursors.enter_context(closing(cursor)))
                selected = list(islice(heapq.merge(*windows, key=position), int(limit)))
            if not selected:
                return []
            ids = tuple(str(row["message_id"]) for row in selected)
            placeholders = ",".join("?" for _ in ids)
            rows = connection.execute(
                f"""
                SELECT m.*, p.resolution_state AS sender_resolution_state,
                       p.identity_confidence AS sender_identity_confidence
                FROM messages AS m
                LEFT JOIN participants AS p ON p.participant_id = m.sender_id
                WHERE m.message_id IN ({placeholders})
                """,
                ids,
            ).fetchall()
            by_id = {str(row["message_id"]): row for row in rows}
            return [by_id[message_id] for message_id in ids]

    def record_access_receipt(
        self,
        *,
        receipt_id: str,
        reader_id: str,
        tool_name: str,
        conversation_id: str | None,
        scope_kind: str | None,
        scope_digest: str | None,
        message_count: int,
        resource_count: int,
        bytes_returned: int,
        started_at: str,
        completed_at: str,
        outcome: str,
        warning_codes: tuple[str, ...],
    ) -> None:
        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO access_receipts(
                    receipt_id, reader_id, tool_name, conversation_id,
                    scope_kind, scope_digest, message_count, resource_count,
                    bytes_returned, started_at, completed_at, outcome,
                    warning_codes_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    receipt_id,
                    reader_id,
                    tool_name,
                    conversation_id,
                    scope_kind,
                    scope_digest,
                    int(message_count),
                    int(resource_count),
                    int(bytes_returned),
                    started_at,
                    completed_at,
                    outcome,
                    json.dumps(list(warning_codes), separators=(",", ":")),
                ),
            )

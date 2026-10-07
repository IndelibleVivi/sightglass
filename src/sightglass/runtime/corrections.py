from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any

from sightglass.contracts.common import utc_now
from sightglass.model.observation_codec import (
    ObservationCodecError,
    ObservationPayloadUnavailable,
    decode_observation_text,
    decode_released_header,
)
from sightglass.model.repositories import normalize_label
from sightglass.source.identity import opaque_id


class CorrectionError(RuntimeError):
    pass


def _row_value(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _retained_identity_signatures(retained: dict | None) -> set[tuple[str, str]]:
    """Original sender identity signatures from a released observation header.

    The retained header stores the *original* observed keys exactly as the source
    reported them, so a merge/split/rebind that later changed the canonical binding
    cannot corrupt which messages a key-based correction must account for.
    """

    if not isinstance(retained, dict):
        return set()
    keys = retained.get("sender_identity_keys")
    if not isinstance(keys, list):
        return set()
    return {
        (str(item["kind"]), str(item["value"]))
        for item in keys
        if isinstance(item, dict)
        and item.get("kind") is not None
        and item.get("value") is not None
    }


class CorrectionService:
    """Operator-only identity projection mutations with an append-only ledger."""

    def __init__(self, connection_factory, *, operator_identity: str = "local-owner") -> None:
        self.connection_factory = connection_factory
        self.operator_identity = operator_identity

    @staticmethod
    def _correction_id() -> str:
        return f"wxcorrection_{uuid.uuid4().hex}"

    def _record(
        self,
        connection: sqlite3.Connection,
        *,
        action: str,
        subject: dict[str, Any],
        reason: str | None,
        supersedes: str | None = None,
    ) -> str:
        correction_id = self._correction_id()
        connection.execute(
            """
            INSERT INTO identity_corrections(
                correction_id, action, subject_json, reason, created_at,
                operator_identity, supersedes_correction_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                correction_id,
                action,
                json.dumps(subject, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                reason,
                utc_now().isoformat(timespec="microseconds"),
                self.operator_identity,
                supersedes,
            ),
        )
        return correction_id

    @staticmethod
    def _validate_alias(alias: str) -> str:
        value = " ".join(alias.split())
        if not value or len(value) > 120 or any(ord(character) < 32 for character in value):
            raise CorrectionError("alias must contain 1-120 printable characters")
        return value

    @staticmethod
    def _participant(connection: sqlite3.Connection, participant_id: str) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM participants WHERE participant_id = ?", (participant_id,)
        ).fetchone()
        if row is None:
            raise CorrectionError("participant does not exist")
        return row

    @staticmethod
    def _membership(
        connection: sqlite3.Connection, participant_id: str, conversation_id: str
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT * FROM conversation_members
            WHERE participant_id = ? AND conversation_id = ?
            """,
            (participant_id, conversation_id),
        ).fetchone()
        if row is None:
            raise CorrectionError("participant is not a member of that conversation")
        return row

    @staticmethod
    def _refresh_group_alias(connection: sqlite3.Connection, membership_id: str) -> None:
        row = connection.execute(
            """
            SELECT label FROM conversation_member_labels
            WHERE membership_id = ? AND active = 1
              AND label_kind IN ('sightglass_alias', 'group_card')
            ORDER BY CASE label_kind WHEN 'sightglass_alias' THEN 0 ELSE 1 END,
                     observed_at DESC, member_label_id DESC
            LIMIT 1
            """,
            (membership_id,),
        ).fetchone()
        connection.execute(
            "UPDATE conversation_members SET current_group_alias = ? WHERE membership_id = ?",
            (row[0] if row is not None else None, membership_id),
        )

    def alias_set(
        self,
        participant_id: str,
        alias: str,
        *,
        conversation_id: str | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        value = self._validate_alias(alias)
        now = utc_now().isoformat(timespec="microseconds")
        with self.connection_factory.transaction() as connection:
            self._participant(connection, participant_id)
            if conversation_id:
                membership = self._membership(connection, participant_id, conversation_id)
                membership_id = str(membership["membership_id"])
                previous = [
                    _row_value(row)
                    for row in connection.execute(
                        """
                        SELECT * FROM conversation_member_labels
                        WHERE membership_id = ? AND label_kind = 'sightglass_alias'
                          AND active = 1
                        """,
                        (membership_id,),
                    )
                ]
                connection.execute(
                    """
                    UPDATE conversation_member_labels
                    SET active = 0, valid_to = ?
                    WHERE membership_id = ? AND label_kind = 'sightglass_alias' AND active = 1
                    """,
                    (now, membership_id),
                )
                label_id = opaque_id("wxmemberlabel", membership_id, "operator", value, now)
                connection.execute(
                    """
                    INSERT INTO conversation_member_labels(
                        member_label_id, membership_id, label, normalized_label,
                        label_kind, observed_message_id, provenance, observed_at,
                        valid_from, valid_to, temporal_confidence, active
                    ) VALUES (?, ?, ?, ?, 'sightglass_alias', NULL,
                              'operator.local', ?, ?, NULL, 'exact', 1)
                    """,
                    (label_id, membership_id, value, normalize_label(value), now, now),
                )
                self._refresh_group_alias(connection, membership_id)
                subject = {
                    "scope": "conversation",
                    "participant_id": participant_id,
                    "conversation_id": conversation_id,
                    "membership_id": membership_id,
                    "new_label_id": label_id,
                    "previous": previous,
                }
            else:
                previous = [
                    _row_value(row)
                    for row in connection.execute(
                        """
                        SELECT * FROM participant_labels
                        WHERE participant_id = ? AND label_kind = 'sightglass_alias'
                          AND scope_kind = 'account' AND active = 1
                        """,
                        (participant_id,),
                    )
                ]
                connection.execute(
                    """
                    UPDATE participant_labels SET active = 0, valid_to = ?
                    WHERE participant_id = ? AND label_kind = 'sightglass_alias'
                      AND scope_kind = 'account' AND active = 1
                    """,
                    (now, participant_id),
                )
                label_id = opaque_id("wxlabel", participant_id, "operator", value, now)
                connection.execute(
                    """
                    INSERT INTO participant_labels(
                        label_id, participant_id, label, normalized_label,
                        label_kind, scope_kind, reader_id, observed_message_id,
                        provenance, observed_at, valid_from, valid_to,
                        temporal_confidence, active
                    ) VALUES (?, ?, ?, ?, 'sightglass_alias', 'account', NULL, NULL,
                              'operator.local', ?, ?, NULL, 'exact', 1)
                    """,
                    (label_id, participant_id, value, normalize_label(value), now, now),
                )
                subject = {
                    "scope": "account",
                    "participant_id": participant_id,
                    "new_label_id": label_id,
                    "previous": previous,
                }
            correction_id = self._record(
                connection, action="alias_set", subject=subject, reason=reason
            )
        return {"correction_id": correction_id, "action": "alias_set", "alias": value}

    def alias_unset(
        self,
        participant_id: str,
        *,
        conversation_id: str | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now().isoformat(timespec="microseconds")
        with self.connection_factory.transaction() as connection:
            self._participant(connection, participant_id)
            if conversation_id:
                membership = self._membership(connection, participant_id, conversation_id)
                membership_id = str(membership["membership_id"])
                rows = connection.execute(
                    """
                    SELECT * FROM conversation_member_labels
                    WHERE membership_id = ? AND label_kind = 'sightglass_alias' AND active = 1
                    """,
                    (membership_id,),
                ).fetchall()
                if not rows:
                    raise CorrectionError("no active conversation alias exists")
                connection.execute(
                    """
                    UPDATE conversation_member_labels SET active = 0, valid_to = ?
                    WHERE membership_id = ? AND label_kind = 'sightglass_alias' AND active = 1
                    """,
                    (now, membership_id),
                )
                self._refresh_group_alias(connection, membership_id)
                subject = {
                    "scope": "conversation",
                    "participant_id": participant_id,
                    "conversation_id": conversation_id,
                    "membership_id": membership_id,
                    "removed": [_row_value(row) for row in rows],
                }
            else:
                rows = connection.execute(
                    """
                    SELECT * FROM participant_labels
                    WHERE participant_id = ? AND label_kind = 'sightglass_alias'
                      AND scope_kind = 'account' AND active = 1
                    """,
                    (participant_id,),
                ).fetchall()
                if not rows:
                    raise CorrectionError("no active account alias exists")
                connection.execute(
                    """
                    UPDATE participant_labels SET active = 0, valid_to = ?
                    WHERE participant_id = ? AND label_kind = 'sightglass_alias'
                      AND scope_kind = 'account' AND active = 1
                    """,
                    (now, participant_id),
                )
                subject = {
                    "scope": "account",
                    "participant_id": participant_id,
                    "removed": [_row_value(row) for row in rows],
                }
            correction_id = self._record(
                connection, action="alias_unset", subject=subject, reason=reason
            )
        return {"correction_id": correction_id, "action": "alias_unset"}

    @staticmethod
    def _message_ids_for_keys(
        connection: sqlite3.Connection, keys: list[dict[str, Any]]
    ) -> set[str]:
        signatures = {(str(row["key_kind"]), str(row["key_value"])) for row in keys}
        if not signatures:
            return set()
        found: set[str] = set()
        for row in connection.execute(
            """
            SELECT mo.message_id, mo.parsed_json
            FROM message_observations AS mo
            JOIN (
                SELECT message_id, MAX(observation_seq) AS observation_seq
                FROM message_observations GROUP BY message_id
            ) latest USING(message_id, observation_seq)
            """
        ):
            try:
                payload = json.loads(decode_observation_text(row["parsed_json"]))
            except ObservationPayloadUnavailable:
                # The body copy was intentionally released, but the retained header
                # preserves the ORIGINAL observed sender identity keys -- never the
                # current canonical binding, which a merge/split/rebind may have
                # changed.  Decode those original keys directly.
                header = decode_released_header(row["parsed_json"])
                retained = header.get("retained") if isinstance(header, dict) else None
                observed = _retained_identity_signatures(retained)
                if observed & signatures:
                    found.add(str(row["message_id"]))
                continue
            except (ObservationCodecError, json.JSONDecodeError) as exc:
                # A corrupt observation cannot be silently skipped: it would hide
                # message evidence that an identity correction must account for.
                raise CorrectionError(
                    f"observation payload for {row['message_id']} is unreadable"
                ) from exc
            if not isinstance(payload, dict):
                raise CorrectionError(
                    f"observation payload for {row['message_id']} is not an object"
                )
            sender = payload.get("sender")
            observed = sender.get("identity_keys", []) if isinstance(sender, dict) else []
            if not isinstance(observed, list) or any(
                not isinstance(item, dict) for item in observed
            ):
                raise CorrectionError(
                    f"observation payload for {row['message_id']} has invalid sender keys"
                )
            observed_signatures = {
                (str(item.get("kind")), str(item.get("value"))) for item in observed
            }
            if observed_signatures & signatures:
                found.add(str(row["message_id"]))
        return found

    def merge(
        self,
        source_participant_id: str,
        target_participant_id: str,
        *,
        reason: str | None = None,
        action: str = "merge",
        supersedes: str | None = None,
    ) -> dict[str, Any]:
        if source_participant_id == target_participant_id:
            raise CorrectionError("merge source and target must differ")
        with self.connection_factory.transaction() as connection:
            source = self._participant(connection, source_participant_id)
            target = self._participant(connection, target_participant_id)
            if source["account_id"] != target["account_id"]:
                raise CorrectionError("participants from different accounts cannot merge")
            labels = connection.execute(
                "SELECT * FROM participant_labels WHERE participant_id = ?",
                (source_participant_id,),
            ).fetchall()
            keys = connection.execute(
                "SELECT * FROM participant_source_keys WHERE participant_id = ?",
                (source_participant_id,),
            ).fetchall()
            memberships = connection.execute(
                "SELECT * FROM conversation_members WHERE participant_id = ?",
                (source_participant_id,),
            ).fetchall()
            membership_ids = [str(row["membership_id"]) for row in memberships]
            member_labels: list[sqlite3.Row] = []
            if membership_ids:
                placeholders = ",".join("?" for _ in membership_ids)
                member_labels = connection.execute(
                    "SELECT * FROM conversation_member_labels "
                    f"WHERE membership_id IN ({placeholders})",
                    membership_ids,
                ).fetchall()
            messages = connection.execute(
                """
                SELECT message_id, sender_membership_id FROM messages
                WHERE sender_id = ?
                """,
                (source_participant_id,),
            ).fetchall()
            membership_targets: dict[str, dict[str, Any]] = {}
            for membership in memberships:
                source_membership_id = str(membership["membership_id"])
                existing = connection.execute(
                    """
                    SELECT * FROM conversation_members
                    WHERE conversation_id = ? AND participant_id = ?
                    """,
                    (membership["conversation_id"], target_participant_id),
                ).fetchone()
                if existing is None:
                    target_membership_id = opaque_id(
                        "wxmember", membership["conversation_id"], target_participant_id
                    )
                    target_membership = _row_value(membership)
                    target_membership["membership_id"] = target_membership_id
                    target_membership["participant_id"] = target_participant_id
                    self._insert_snapshot(
                        connection, "conversation_members", target_membership
                    )
                else:
                    target_membership_id = str(existing["membership_id"])
                connection.execute(
                    """
                    UPDATE conversation_member_labels SET membership_id = ?
                    WHERE membership_id = ?
                    """,
                    (target_membership_id, source_membership_id),
                )
                connection.execute(
                    """
                    UPDATE messages SET sender_membership_id = ?
                    WHERE sender_membership_id = ?
                    """,
                    (target_membership_id, source_membership_id),
                )
                connection.execute(
                    "DELETE FROM conversation_members WHERE membership_id = ?",
                    (source_membership_id,),
                )
                membership_targets[source_membership_id] = {
                    "target_membership_id": target_membership_id,
                    "target_existed": existing is not None,
                }
            connection.execute(
                "UPDATE participant_labels SET participant_id = ? WHERE participant_id = ?",
                (target_participant_id, source_participant_id),
            )
            connection.execute(
                "UPDATE participant_source_keys SET participant_id = ? WHERE participant_id = ?",
                (target_participant_id, source_participant_id),
            )
            connection.execute(
                "UPDATE messages SET sender_id = ? WHERE sender_id = ?",
                (target_participant_id, source_participant_id),
            )
            connection.execute(
                "DELETE FROM participants WHERE participant_id = ?", (source_participant_id,)
            )
            connection.execute(
                """
                UPDATE participants
                SET is_self = MAX(is_self, ?),
                    first_seen_at = MIN(first_seen_at, ?),
                    last_seen_at = MAX(last_seen_at, ?)
                WHERE participant_id = ?
                """,
                (
                    int(source["is_self"]),
                    str(source["first_seen_at"]),
                    str(source["last_seen_at"]),
                    target_participant_id,
                ),
            )
            subject = {
                "source_participant": _row_value(source),
                "target_participant_before": _row_value(target),
                "target_participant_id": target_participant_id,
                "labels": [_row_value(row) for row in labels],
                "keys": [_row_value(row) for row in keys],
                "memberships": [_row_value(row) for row in memberships],
                "member_labels": [_row_value(row) for row in member_labels],
                "messages": [_row_value(row) for row in messages],
                "membership_targets": membership_targets,
            }
            correction_id = self._record(
                connection,
                action=action,
                subject=subject,
                reason=reason,
                supersedes=supersedes,
            )
        return {"correction_id": correction_id, "action": action}

    @staticmethod
    def _insert_snapshot(connection: sqlite3.Connection, table: str, row: dict[str, Any]) -> None:
        columns = list(row)
        connection.execute(
            f"INSERT INTO {table}({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
            [row[column] for column in columns],
        )

    def _restore_merge(self, connection: sqlite3.Connection, subject: dict[str, Any]) -> None:
        source = subject["source_participant"]
        target_id = str(subject["target_participant_id"])
        source_id = str(source["participant_id"])
        target_before = subject["target_participant_before"]
        if connection.execute(
            "SELECT 1 FROM participants WHERE participant_id = ?", (source_id,)
        ).fetchone():
            raise CorrectionError("merge source already exists; correction was already reversed")
        if bool(source["is_self"]) and not bool(target_before["is_self"]):
            connection.execute(
                "UPDATE participants SET is_self = 0 WHERE participant_id = ?", (target_id,)
            )
        self._insert_snapshot(connection, "participants", source)
        for row in subject["labels"]:
            connection.execute(
                "UPDATE participant_labels SET participant_id = ? WHERE label_id = ?",
                (source_id, row["label_id"]),
            )
        for row in subject["keys"]:
            connection.execute(
                "UPDATE participant_source_keys SET participant_id = ? WHERE source_key_id = ?",
                (source_id, row["source_key_id"]),
            )
        for row in subject["memberships"]:
            self._insert_snapshot(connection, "conversation_members", row)
        for row in subject["member_labels"]:
            connection.execute(
                "UPDATE conversation_member_labels SET membership_id = ? WHERE member_label_id = ?",
                (row["membership_id"], row["member_label_id"]),
            )
        for row in subject["messages"]:
            connection.execute(
                """
                UPDATE messages SET sender_id = ?, sender_membership_id = ?
                WHERE message_id = ?
                """,
                (source_id, row["sender_membership_id"], row["message_id"]),
            )
        restored_message_ids = {str(row["message_id"]) for row in subject["messages"]}
        for message_id in self._message_ids_for_keys(connection, subject["keys"]):
            position = connection.execute(
                "SELECT conversation_id FROM messages WHERE message_id = ?",
                (message_id,),
            ).fetchone()
            if position is None:
                continue
            membership = connection.execute(
                """
                SELECT membership_id FROM conversation_members
                WHERE conversation_id = ? AND participant_id = ?
                """,
                (position["conversation_id"], source_id),
            ).fetchone()
            connection.execute(
                """
                UPDATE messages SET sender_id = ?, sender_membership_id = ?
                WHERE message_id = ? AND sender_id = ?
                """,
                (
                    source_id,
                    membership["membership_id"] if membership else None,
                    message_id,
                    target_id,
                ),
            )
            restored_message_ids.add(message_id)
        if restored_message_ids:
            placeholders = ",".join("?" for _ in restored_message_ids)
            for membership in subject["memberships"]:
                target = subject["membership_targets"][str(membership["membership_id"])]
                connection.execute(
                    """
                    UPDATE conversation_member_labels SET membership_id = ?
                    WHERE membership_id = ? AND observed_message_id IN ("""
                    + placeholders
                    + ")",
                    (
                        membership["membership_id"],
                        target["target_membership_id"],
                        *sorted(restored_message_ids),
                    ),
                )
        for target in subject["membership_targets"].values():
            if target["target_existed"]:
                continue
            target_membership_id = str(target["target_membership_id"])
            still_referenced = connection.execute(
                """
                SELECT 1 FROM messages WHERE sender_membership_id = ?
                UNION ALL
                SELECT 1 FROM conversation_member_labels WHERE membership_id = ?
                LIMIT 1
                """,
                (target_membership_id, target_membership_id),
            ).fetchone()
            if still_referenced is None:
                connection.execute(
                    "DELETE FROM conversation_members WHERE membership_id = ?",
                    (target_membership_id,),
                )
        connection.execute(
            """
            UPDATE participants SET is_self = ?, first_seen_at = ?, last_seen_at = ?
            WHERE participant_id = ?
            """,
            (
                target_before["is_self"],
                target_before["first_seen_at"],
                target_before["last_seen_at"],
                target_id,
            ),
        )

    def split(self, merge_correction_id: str, *, reason: str | None = None) -> dict[str, Any]:
        return self._reverse_merge(merge_correction_id, action="split", reason=reason)

    def _reverse_merge(
        self, correction_id: str, *, action: str, reason: str | None
    ) -> dict[str, Any]:
        with self.connection_factory.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM identity_corrections WHERE correction_id = ?",
                (correction_id,),
            ).fetchone()
            if row is None or str(row["action"]) not in {"merge", "rollback"}:
                raise CorrectionError("split requires a merge correction")
            if connection.execute(
                "SELECT 1 FROM identity_corrections WHERE supersedes_correction_id = ?",
                (correction_id,),
            ).fetchone():
                raise CorrectionError("correction was already superseded")
            subject = json.loads(str(row["subject_json"]))
            self._restore_merge(connection, subject)
            new_id = self._record(
                connection,
                action=action,
                subject={"reversed_action": str(row["action"]), "original": subject},
                reason=reason,
                supersedes=correction_id,
            )
        return {"correction_id": new_id, "action": action}

    def rebind(
        self,
        source_key_id: str,
        target_participant_id: str,
        *,
        reason: str | None = None,
    ) -> dict[str, Any]:
        with self.connection_factory.transaction() as connection:
            target = self._participant(connection, target_participant_id)
            key = connection.execute(
                "SELECT * FROM participant_source_keys WHERE source_key_id = ? AND active = 1",
                (source_key_id,),
            ).fetchone()
            if key is None:
                raise CorrectionError("active source key does not exist")
            source_id = str(key["participant_id"])
            if source_id == target_participant_id:
                raise CorrectionError("source key is already bound to that participant")
            source = self._participant(connection, source_id)
            if source["account_id"] != target["account_id"]:
                raise CorrectionError("source key cannot cross accounts")
            key_value = _row_value(key)
            message_ids = sorted(self._message_ids_for_keys(connection, [key_value]))
            messages = []
            for message_id in message_ids:
                row = connection.execute(
                    """
                    SELECT message_id, sender_id, sender_membership_id, conversation_id
                    FROM messages WHERE message_id = ? AND sender_id = ?
                    """,
                    (message_id, source_id),
                ).fetchone()
                if row is not None:
                    messages.append(_row_value(row))
            connection.execute(
                "UPDATE participant_source_keys SET participant_id = ? WHERE source_key_id = ?",
                (target_participant_id, source_key_id),
            )
            for message in messages:
                target_membership = connection.execute(
                    """
                    SELECT membership_id FROM conversation_members
                    WHERE conversation_id = ? AND participant_id = ?
                    """,
                    (message["conversation_id"], target_participant_id),
                ).fetchone()
                connection.execute(
                    """
                    UPDATE messages SET sender_id = ?, sender_membership_id = ?
                    WHERE message_id = ?
                    """,
                    (
                        target_participant_id,
                        target_membership["membership_id"] if target_membership else None,
                        message["message_id"],
                    ),
                )
            subject = {
                "key": key_value,
                "target_participant_id": target_participant_id,
                "messages": messages,
            }
            correction_id = self._record(
                connection, action="source_key_rebind", subject=subject, reason=reason
            )
        return {"correction_id": correction_id, "action": "source_key_rebind"}

    def rollback(self, correction_id: str, *, reason: str | None = None) -> dict[str, Any]:
        with self.connection_factory.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM identity_corrections WHERE correction_id = ?",
                (correction_id,),
            ).fetchone()
            if row is None:
                raise CorrectionError("correction does not exist")
            if connection.execute(
                "SELECT 1 FROM identity_corrections WHERE supersedes_correction_id = ?",
                (correction_id,),
            ).fetchone():
                raise CorrectionError("correction was already superseded")
            action = str(row["action"])
            subject = json.loads(str(row["subject_json"]))
            if action == "merge":
                self._restore_merge(connection, subject)
            elif action == "split":
                original = subject["original"]
                source_id = str(original["source_participant"]["participant_id"])
                target_id = str(original["target_participant_id"])
                # The outer transaction is re-entrant through WindowDB.
                return self.merge(
                    source_id,
                    target_id,
                    reason=reason,
                    action="rollback",
                    supersedes=correction_id,
                )
            elif action == "source_key_rebind":
                key = subject["key"]
                target_id = str(subject["target_participant_id"])
                source_id = str(key["participant_id"])
                connection.execute(
                    "UPDATE participant_source_keys SET participant_id = ? WHERE source_key_id = ?",
                    (source_id, key["source_key_id"]),
                )
                for message in subject["messages"]:
                    connection.execute(
                        """
                        UPDATE messages SET sender_id = ?, sender_membership_id = ?
                        WHERE message_id = ?
                        """,
                        (
                            message["sender_id"],
                            message["sender_membership_id"],
                            message["message_id"],
                        ),
                    )
                restored_ids = {str(message["message_id"]) for message in subject["messages"]}
                for message_id in self._message_ids_for_keys(connection, [key]) - restored_ids:
                    position = connection.execute(
                        "SELECT conversation_id FROM messages WHERE message_id = ?",
                        (message_id,),
                    ).fetchone()
                    membership = connection.execute(
                        """
                        SELECT membership_id FROM conversation_members
                        WHERE conversation_id = ? AND participant_id = ?
                        """,
                        (position["conversation_id"], source_id),
                    ).fetchone()
                    connection.execute(
                        """
                        UPDATE messages SET sender_id = ?, sender_membership_id = ?
                        WHERE message_id = ? AND sender_id = ?
                        """,
                        (
                            source_id,
                            membership["membership_id"] if membership else None,
                            message_id,
                            target_id,
                        ),
                    )
            elif action in {"alias_set", "alias_unset"}:
                self._rollback_alias(connection, action, subject)
            else:
                raise CorrectionError("that correction action cannot be rolled back")
            new_id = self._record(
                connection,
                action="rollback",
                subject={"reversed_action": action, "original": subject},
                reason=reason,
                supersedes=correction_id,
            )
        return {"correction_id": new_id, "action": "rollback"}

    def _rollback_alias(
        self, connection: sqlite3.Connection, action: str, subject: dict[str, Any]
    ) -> None:
        conversation_scope = subject["scope"] == "conversation"
        table = "conversation_member_labels" if conversation_scope else "participant_labels"
        id_column = "member_label_id" if conversation_scope else "label_id"
        if action == "alias_set":
            connection.execute(
                f"UPDATE {table} SET active = 0, valid_to = ? WHERE {id_column} = ?",
                (utc_now().isoformat(timespec="microseconds"), subject["new_label_id"]),
            )
            previous = subject["previous"]
        else:
            previous = subject["removed"]
        for row in previous:
            connection.execute(
                f"UPDATE {table} SET active = ?, valid_to = ? WHERE {id_column} = ?",
                (row["active"], row["valid_to"], row[id_column]),
            )
        if conversation_scope:
            self._refresh_group_alias(connection, str(subject["membership_id"]))

    def list(self, *, limit: int = 50) -> list[dict[str, Any]]:
        if limit < 1 or limit > 200:
            raise CorrectionError("correction list limit must be between 1 and 200")
        with self.connection_factory.connection() as connection:
            rows = connection.execute(
                """
                SELECT correction_id, action, reason, created_at,
                       operator_identity, supersedes_correction_id
                FROM identity_corrections
                ORDER BY created_at DESC, correction_id DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [_row_value(row) for row in rows]

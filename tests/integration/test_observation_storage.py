from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from sightglass.contracts.identity import (
    LabelObservation,
    SourceAccount,
    SourceConversation,
    SourceIdentityKey,
    SourceParticipant,
)
from sightglass.model.db import WindowDB
from sightglass.model.observation_codec import (
    decode_observation_bytes,
    decode_observation_text,
    is_encoded_observation,
)
from sightglass.model.repositories import WindowRepository
from sightglass.runtime.corrections import CorrectionError, CorrectionService
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


class ObservationStorageTests(unittest.TestCase):
    """Observation codec and label freshness invariants over synthetic sources."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "source"
        self.window = Path(self.temp.name) / "state" / "window.db"
        create_synthetic_source(self.root)
        self.provider, self.repository, self.service, self.tools = build_test_stack(
            self.root, self.window
        )
        self.assertTrue(self.tools.wechat_status()["ready"])
        self.group_id = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _read_group(self) -> dict:
        return self.tools.wechat_read_messages(
            mode="recent", conversation_id=self.group_id, limit=100
        )

    def _counts(self) -> dict[str, int]:
        with self.repository.database.connection() as connection:
            return {
                table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in (
                    "messages",
                    "message_observations",
                    "participant_labels",
                    "conversation_member_labels",
                )
            }

    def test_new_observations_are_self_describing_blobs_with_faithful_digest(self) -> None:
        self._read_group()
        with self.repository.database.connection() as connection:
            rows = connection.execute(
                "SELECT payload_digest, typeof(parsed_json) AS storage, parsed_json "
                "FROM message_observations"
            ).fetchall()
        self.assertTrue(rows)
        for row in rows:
            self.assertEqual(row["storage"], "blob")
            self.assertTrue(is_encoded_observation(row["parsed_json"]))
            raw = decode_observation_bytes(row["parsed_json"])
            # The digest stays over the uncompressed bytes, so identity and dedup
            # evidence are unchanged by the storage encoding.
            self.assertEqual(hashlib.sha256(raw).hexdigest(), str(row["payload_digest"]))
            payload = json.loads(raw)
            self.assertIn("message", payload)
            self.assertIn("source_envelope", payload)
            self.assertIn("identity_keys", payload["sender"])

    def test_repeated_scan_does_not_append_observations_or_labels(self) -> None:
        self._read_group()
        baseline = self._counts()
        self._read_group()
        self._read_group()
        self.assertEqual(self._counts(), baseline)

    def test_message_surface_labels_stay_message_scoped(self) -> None:
        self._read_group()
        baseline = self._counts()
        with self.repository.database.connection() as connection:
            distinct_messages = int(
                connection.execute(
                    "SELECT COUNT(DISTINCT observed_message_id) FROM participant_labels "
                    "WHERE label_kind = 'message_surface'"
                ).fetchone()[0]
            )
            for table, column in (
                ("participant_labels", "label_id"),
                ("conversation_member_labels", "member_label_id"),
            ):
                rows = connection.execute(
                    f"SELECT observed_message_id FROM {table} WHERE label_kind = 'message_surface'"
                ).fetchall()
                self.assertTrue(rows)
                self.assertTrue(all(row[0] for row in rows))
                self.assertEqual(len({str(row[0]) for row in rows}), len(rows))
        self.assertGreater(distinct_messages, 1)
        # A second scan re-observes the same messages without appending rows.
        self._read_group()
        self.assertEqual(self._counts(), baseline)

    def test_corrupt_observation_fails_identity_correction(self) -> None:
        self._read_group()
        with self.repository.database.connection() as connection:
            key = connection.execute(
                "SELECT key_kind, key_value FROM participant_source_keys "
                "WHERE principal_eligible = 1 AND active = 1 LIMIT 1"
            ).fetchone()
            observation = connection.execute(
                "SELECT observation_id, parsed_json FROM message_observations LIMIT 1"
            ).fetchone()
        self.assertIsNotNone(key)
        self.assertIsNotNone(observation)
        # Truncate a real encoded observation inside its header so the codec fails
        # closed instead of returning a partially readable payload.
        corrupted = bytes(observation["parsed_json"])[:6]
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE message_observations SET parsed_json = ? WHERE observation_id = ?",
                (corrupted, str(observation["observation_id"])),
            )
        try:
            with self.repository.database.connection() as connection:
                with self.assertRaises(CorrectionError):
                    CorrectionService._message_ids_for_keys(
                        connection,
                        [{"key_kind": str(key["key_kind"]), "key_value": str(key["key_value"])}],
                    )
        finally:
            with self.repository.database.transaction() as connection:
                connection.execute(
                    "UPDATE message_observations SET parsed_json = ? WHERE observation_id = ?",
                    (observation["parsed_json"], str(observation["observation_id"])),
                )
        # A healthy ledger still resolves identities after the corruption is removed.
        with self.repository.database.connection() as connection:
            found = CorrectionService._message_ids_for_keys(
                connection,
                [{"key_kind": str(key["key_kind"]), "key_value": str(key["key_value"])}],
            )
        self.assertTrue(found)

    def test_legacy_text_observation_rows_still_resolve_identity(self) -> None:
        self._read_group()
        with self.repository.database.connection() as connection:
            message_id = str(
                connection.execute(
                    "SELECT message_id FROM message_observations LIMIT 1"
                ).fetchone()[0]
            )
            key = connection.execute(
                "SELECT key_kind, key_value FROM participant_source_keys "
                "WHERE principal_eligible = 1 AND active = 1 LIMIT 1"
            ).fetchone()
            payload = json.loads(
                decode_observation_text(
                    connection.execute(
                        "SELECT parsed_json FROM message_observations WHERE message_id = ?",
                        (message_id,),
                    ).fetchone()[0]
                )
            )
            payload["sender"]["identity_keys"] = [
                {
                    "kind": str(key["key_kind"]),
                    "value": str(key["key_value"]),
                    "stability": "stable",
                    "principal_eligible": True,
                    "scope_conversation_source_id": None,
                    "provenance": "synthetic.legacy",
                }
            ]
        with self.repository.database.transaction() as connection:
            # Rewrite the row as legacy plain TEXT JSON (no codec header).
            connection.execute(
                "UPDATE message_observations SET parsed_json = ? WHERE message_id = ?",
                (json.dumps(payload, ensure_ascii=False), message_id),
            )
            found = CorrectionService._message_ids_for_keys(
                connection,
                [{"key_kind": str(key["key_kind"]), "key_value": str(key["key_value"])}],
            )
        self.assertIn(message_id, found)


class LabelFreshnessTests(unittest.TestCase):
    """Participant and member labels refresh in place and keep true A->B->A history."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = WindowDB(Path(self.temp.name) / "state" / "window.db")
        self.repository = WindowRepository(self.database)
        account_id = self.repository.upsert_account(
            SourceAccount(
                source_namespace="synthetic",
                source_account_key="demo-account",
                self_principal_key="wxid_demo_owner",
                display_name="Synthetic Owner",
                reader_timezone="UTC",
            ),
            "2026-09-26T00:00:00+00:00",
        )
        self.conversation_id = self.repository.upsert_conversation(
            account_id,
            SourceConversation(
                source_conversation_id="conv_group",
                kind="group",
                title="Synthetic Group",
                roster_complete=True,
            ),
            "2026-09-26T00:00:00+00:00",
        )
        self.account_id = account_id
        self.participant_id, self.membership_id = self._index("示例甲", "2026-09-26T00:00:00+00:00")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _index(self, label: str, observed_at: str) -> tuple[str, str]:
        participant = SourceParticipant(
            source_conversation_id="conv_group",
            identity_keys=(
                SourceIdentityKey(
                    "internal_username", "wxid_demo_member", "stable", True, "synthetic"
                ),
            ),
            labels=(
                LabelObservation(
                    label=label,
                    label_kind="contact_remark",
                    scope="account",
                    provenance="synthetic.catalog.principals",
                    observed_at_utc=observed_at,
                    temporal_confidence="current_only",
                ),
                LabelObservation(
                    label=f"{label}群名片",
                    label_kind="group_card",
                    scope="conversation",
                    provenance="synthetic.catalog.memberships",
                    observed_at_utc=observed_at,
                    temporal_confidence="current_only",
                ),
            ),
            account_labels_complete=True,
        )
        return self.repository.index_participant(
            self.account_id, self.conversation_id, participant, observed_at
        )

    def _rows(self, table: str) -> list[dict]:
        with self.database.connection() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    f"SELECT * FROM {table} WHERE label_kind IN "
                    "('contact_remark', 'group_card') ORDER BY observed_at, rowid"
                )
            ]

    def test_unchanged_relabel_only_refreshes_freshness(self) -> None:
        self._index("示例甲", "2026-09-26T00:00:05+00:00")
        self._index("示例甲", "2026-09-26T00:00:09+00:00")
        participant_rows = self._rows("participant_labels")
        member_rows = self._rows("conversation_member_labels")
        self.assertEqual(len(participant_rows), 1)
        self.assertEqual(len(member_rows), 1)
        self.assertEqual(participant_rows[0]["observed_at"], "2026-09-26T00:00:09+00:00")
        self.assertEqual(member_rows[0]["observed_at"], "2026-09-26T00:00:09+00:00")
        # The interval still starts at the first observation.
        self.assertEqual(participant_rows[0]["valid_from"], "2026-09-26T00:00:00+00:00")
        self.assertEqual(participant_rows[0]["active"], 1)
        self.assertEqual(member_rows[0]["active"], 1)

    def test_case_only_relabel_refreshes_raw_label_without_duplicating(self) -> None:
        self._index("EXAMPLE label", "2026-09-26T00:00:04+00:00")
        self._index("example label", "2026-09-26T00:00:08+00:00")
        participant_rows = [
            row for row in self._rows("participant_labels") if row["label_kind"] == "contact_remark"
        ]
        # setUp's "示例甲" interval was closed by the value change, so two intervals
        # remain: the replaced one and a single refreshed case-only relabel.
        active = [row for row in participant_rows if row["active"]]
        self.assertEqual(len(participant_rows), 2)
        self.assertEqual(len(active), 1)
        # Normalized-equal text keeps one interval while the current raw label stays truthful.
        self.assertEqual(active[0]["label"], "example label")
        self.assertEqual(active[0]["observed_at"], "2026-09-26T00:00:08+00:00")

    def test_value_change_keeps_a_b_a_intervals(self) -> None:
        self._index("示例乙", "2026-09-26T00:00:02+00:00")
        self._index("示例甲", "2026-09-26T00:00:06+00:00")
        participant_rows = [
            row for row in self._rows("participant_labels") if row["label_kind"] == "contact_remark"
        ]
        member_rows = [
            row
            for row in self._rows("conversation_member_labels")
            if row["label_kind"] == "group_card"
        ]
        self.assertEqual([row["label"] for row in participant_rows], ["示例甲", "示例乙", "示例甲"])
        self.assertEqual(
            [row["label"] for row in member_rows], ["示例甲群名片", "示例乙群名片", "示例甲群名片"]
        )
        for rows in (participant_rows, member_rows):
            self.assertEqual([row["active"] for row in rows], [0, 0, 1])
            self.assertIsNone(rows[2]["valid_to"])
            self.assertIsNotNone(rows[0]["valid_to"])
            self.assertIsNotNone(rows[1]["valid_to"])


if __name__ == "__main__":
    unittest.main()

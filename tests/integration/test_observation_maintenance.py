"""Legacy pointer recovery preserves immutable episodes, identity and storage floor."""

from __future__ import annotations

import unittest
from dataclasses import replace
from unittest.mock import patch

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.observation_maintenance import (
    inspect_observation_consistency,
    repair_observation_consistency,
)
from sightglass.storage import MIB, StorageBudget, StorageSettings
from tests.integration.test_reading_correctness_sequences import _ReadingFixture


class ObservationMaintenanceTests(_ReadingFixture):
    # Reuse fixture helpers without inheriting the unrelated sequence tests.
    def _broken_pointer(self, number: int) -> tuple[int, int]:
        source = self._source(number)
        row = self._message_row(self._id(number))
        first = int(row["current_observation_seq"])
        self._upsert(replace(source, raw_content="Synthetic changed body"))
        second = int(self._message_row(self._id(number))["current_observation_seq"])
        self._upsert(source)
        # Recreate the stock v8 corruption: current projection A, observation B.
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET current_observation_seq=? WHERE message_id=?",
                (second, self._id(number)),
            )
        return first, second

    def test_inspection_is_read_only_and_batches_repair_survive_restart(self) -> None:
        self._append(1, 4, text="Synthetic original body")
        self.service.sync_source_once(initial_tail=100)
        first, second = self._broken_pointer(2)
        self._broken_pointer(3)
        cursor = self._page(mode="recent", limit=1)["page"]["next_cursor"]
        with self.repository.database.connection() as connection:
            count = connection.execute("SELECT COUNT(*) FROM message_observations").fetchone()[0]
            connection.execute(
                (
                    "UPDATE messages SET sender_id=(SELECT participant_id FROM "
                    "participants WHERE is_self=1 LIMIT 1) WHERE message_id=?"
                ),
                (self._id(2),),
            )
            connection.commit()
            corrected_sender = connection.execute(
                "SELECT sender_id FROM messages WHERE message_id=?", (self._id(2),)
            ).fetchone()[0]
        after = None
        mismatch = examined = 0
        while True:
            receipt = inspect_observation_consistency(
                self.repository.database, after_message_id=after, limit=1
            )
            examined += receipt["examined_count"]
            mismatch += receipt["mismatch_count"]
            self.assertLessEqual(receipt["examined_count"], 1)
            after = receipt["after_message_id"]
            if receipt["complete"]:
                break
        self.assertGreater(examined, 4)
        self.assertEqual(mismatch, 2)
        with self.repository.database.connection() as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM message_observations").fetchone()[0], count
            )
            self.assertIsNone(
                connection.execute("SELECT 1 FROM observation_maintenance_state").fetchone()
            )
        repaired = 0
        while True:
            receipt = repair_observation_consistency(self.repository.database, limit=1)
            repaired += receipt["repaired_count"]
            self.assertLessEqual(receipt["examined_count"], 1)
            self._restart()
            if receipt["complete"]:
                break
        self.assertEqual(repaired, 2)
        self.assertEqual(
            inspect_observation_consistency(self.repository.database)["mismatch_count"], 0
        )
        row = self._message_row(self._id(2))
        self.assertEqual(row["sender_id"], corrected_sender)
        self.assertGreater(row["current_observation_seq"], second)
        with self.repository.database.connection() as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM message_observations").fetchone()[0],
                count + 2,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT payload_digest FROM message_observations WHERE observation_seq=?",
                    (first,),
                ).fetchone()[0],
                connection.execute(
                    "SELECT payload_digest FROM message_observations WHERE observation_seq=?",
                    (row["current_observation_seq"],),
                ).fetchone()[0],
            )
            self.assertEqual(
                connection.execute(
                    "SELECT source_observation_seq FROM message_link_projection WHERE message_id=?",
                    (self._id(2),),
                ).fetchone()[0],
                row["current_observation_seq"],
            )
        with self.assertRaises(SightglassError) as caught:
            self._page(mode="recent", limit=1, cursor=cursor)
        self.assertEqual(caught.exception.code, ErrorCode.CURSOR_STALE)
        self.assertEqual(
            repair_observation_consistency(self.repository.database)["examined_count"], 0
        )

    def test_unsupported_evidence_invalidates_projection_and_existing_cursor(self) -> None:
        self._append(1, 4)
        self.service.sync_source_once(initial_tail=100)
        with self.repository.database.transaction() as connection:
            connection.execute(
                (
                    "UPDATE messages SET text='Synthetic unsupported', structured_json=? "
                    "WHERE message_id=?"
                ),
                ('{"kind":"text","text":"Synthetic unsupported","resources":[]}', self._id(2)),
            )
        cursor = self._page(mode="recent", limit=1)["page"]["next_cursor"]
        receipt = repair_observation_consistency(self.repository.database)
        self.assertEqual(receipt["invalidated_count"], 1)
        self.assertIsNone(self._message_row(self._id(2))["projection_epoch"])
        with self.assertRaises(SightglassError) as caught:
            self._page(mode="recent", limit=1, cursor=cursor)
        self.assertEqual(caught.exception.code, ErrorCode.CURSOR_STALE)
        self._upsert(self._source(2))
        self.assertIsNotNone(self._message_row(self._id(2))["projection_epoch"])

    def test_failed_batch_and_physical_floor_do_not_advance_checkpoint(self) -> None:
        self._append(1, 4)
        self.service.sync_source_once(initial_tail=100)
        self._broken_pointer(2)
        before = self.repository.observation_watermark()
        with patch(
            "sightglass.model.observation_maintenance.publish_links",
            side_effect=RuntimeError("Synthetic interrupted repair"),
        ):
            with self.assertRaises(RuntimeError):
                repair_observation_consistency(self.repository.database)
        self.assertEqual(self.repository.observation_watermark(), before)
        with self.repository.database.connection() as connection:
            self.assertIsNone(
                connection.execute("SELECT 1 FROM observation_maintenance_state").fetchone()
            )
        self.repository.database.storage = StorageBudget(
            self.window.parent, self.window, StorageSettings(64 * MIB, 128 * MIB, 1 << 60, 8 * MIB)
        )
        with self.assertRaises(SightglassError) as caught:
            repair_observation_consistency(self.repository.database)
        self.assertEqual(caught.exception.code, ErrorCode.STORAGE_PRESSURE)
        self.repository.database.storage = None
        self.assertEqual(
            repair_observation_consistency(self.repository.database)["repaired_count"], 1
        )


if __name__ == "__main__":
    unittest.main()

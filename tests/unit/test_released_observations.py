"""Canonical released-observation lifecycle regressions.

Exercises the observation codec, the real ``WindowRepository`` admission path and
``CorrectionService`` against an admitted synthetic catalog.  Every fixture is
unmistakably synthetic.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from sightglass.model.observation_codec import (
    ObservationCodecError,
    ObservationPayloadUnavailable,
    build_released_header,
    decode_observation_bytes,
    decode_observation_text,
    decode_released_header,
    encode_observation,
    encode_released_observation,
    observation_payload_available,
    observation_payload_state,
)
from sightglass.model.observation_maintenance import _matches
from sightglass.runtime.corrections import CorrectionService, _retained_identity_signatures
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


def _full_envelope(*, key_value: str, text: str, surface_label: str = "Synthetic") -> str:
    return json.dumps(
        {
            "message": {"kind": "text", "text": text},
            "sender": {
                "identity_keys": [
                    {
                        "kind": "internal_username",
                        "value": key_value,
                        "stability": "stable",
                        "principal_eligible": True,
                        "provenance": "message_sender",
                    }
                ],
                "surface_label": surface_label,
                "is_outgoing": False,
            },
            "source_envelope": {
                "source_message_id": f"src-{key_value}",
                "sent_at_utc": "2026-01-01T00:00:00+00:00",
                "sort_seq": 1,
                "source_rowid": 1,
            },
        },
        ensure_ascii=False,
        sort_keys=True,
    )


class ReleasedCodecTests(unittest.TestCase):
    def test_released_header_roundtrips_and_classifies(self) -> None:
        full = encode_observation(_full_envelope(key_value="u1", text="hi"))
        self.assertEqual(observation_payload_state(full), "full")
        self.assertTrue(observation_payload_available(full))
        retained = build_released_header(full)
        released = encode_released_observation(retained, original_bytes=len(full))
        self.assertEqual(observation_payload_state(released), "released")
        self.assertFalse(observation_payload_available(released))
        header = decode_released_header(released)
        assert header is not None
        keys = _retained_identity_signatures(header["retained"])
        self.assertEqual(keys, {("internal_username", "u1")})
        with self.assertRaises(ObservationPayloadUnavailable):
            decode_observation_bytes(released)

    def test_malformed_released_marker_is_corrupt_not_released(self) -> None:
        released = encode_released_observation({"retained": {}}, original_bytes=5)
        for malformed in (
            released[:-1],  # truncated
            released + b"trailing",
            b"XXXX" + released[4:],
            bytes([released[0], released[1], released[2], released[3], 9]) + released[5:],
        ):
            self.assertEqual(observation_payload_state(malformed), "corrupt")
            with self.assertRaises(ObservationCodecError):
                decode_observation_bytes(malformed)
        corrupted = bytearray(released)
        corrupted[-1] ^= 0xFF
        self.assertEqual(observation_payload_state(bytes(corrupted)), "corrupt")

    def test_legacy_text_still_lossless_and_available(self) -> None:
        legacy = _full_envelope(key_value="u2", text="老文本")
        self.assertEqual(observation_payload_state(legacy), "full")
        self.assertTrue(observation_payload_available(legacy))
        self.assertEqual(decode_observation_text(legacy), legacy)
        retained = build_released_header(legacy)
        self.assertEqual(_retained_identity_signatures(retained), {("internal_username", "u2")})

    def test_retained_header_preserves_scope_and_provenance(self) -> None:
        payload = json.dumps(
            {
                "message": {"kind": "text", "text": "x"},
                "sender": {
                    "identity_keys": [
                        {
                            "kind": "conversation_sender_id",
                            "value": "v",
                            "scope_conversation_source_id": "conv-1",
                            "provenance": "roster_row",
                            "principal_eligible": False,
                        }
                    ],
                    "surface_label": "L",
                    "is_outgoing": True,
                },
                "source_envelope": {"source_message_id": "s"},
            }
        )
        retained = build_released_header(encode_observation(payload))
        self.assertEqual(retained["sender_identity_keys"][0]["provenance"], "roster_row")
        self.assertEqual(
            retained["sender_identity_keys"][0]["scope_conversation_source_id"], "conv-1"
        )
        self.assertEqual(retained["surface_label"], "L")
        self.assertTrue(retained["is_outgoing"])

    def test_release_does_not_discard_malformed_original_identity_evidence(self):
        for keys in ("malformed", [17], [{"kind": "internal_username"}]):
            full = encode_observation(json.dumps({"sender": {"identity_keys": keys}}))
            with self.assertRaises(ObservationCodecError):
                build_released_header(full)


class ReleasedObservationAdmissionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "source"
        self.window = Path(self.temp.name) / "state" / "window.db"
        create_synthetic_source(self.root)
        _, self.repository, self.service, self.tools = build_test_stack(
            self.root, self.window, residency_default="keep"
        )
        self.assertTrue(self.tools.wechat_status()["ready"])
        self.service.sync_source_once(initial_tail=100, conversation_limit=100)
        self.database = self.repository.database
        self.epoch = self.service._projection_inventory_epoch()
        from sightglass.source.parser import parse_message

        self.parse_message = parse_message
        with self.database.connection() as connection:
            self.message = connection.execute(
                "SELECT * FROM messages WHERE current_observation_seq IS NOT NULL LIMIT 1"
            ).fetchone()
        context = self.repository.conversation_context(str(self.message["conversation_id"]))
        assert context is not None
        with self.service.provider.snapshot() as snapshot:
            self.original = self.service.provider.get_message(
                context["source_account_key"],
                str(self.message["source_message_id"]),
                snapshot,
            )
        assert self.original is not None

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _readmit(self, source_message, *, projection_epoch=None):
        from sightglass.source.parser import parse_message

        return self.repository.upsert_message(
            str(self.message["account_id"]),
            str(self.message["conversation_id"]),
            self.message["sender_id"],
            self.message["sender_membership_id"],
            source_message,
            parse_message(source_message),
            projection_epoch=projection_epoch or self.epoch,
        )

    def _release(self, message_id: str) -> None:
        with self.database.transaction() as connection:
            row = connection.execute(
                "SELECT observation_seq, parsed_json FROM message_observations "
                "WHERE observation_seq = (SELECT current_observation_seq FROM messages "
                "WHERE message_id = ?)",
                (message_id,),
            ).fetchone()
            released = encode_released_observation(
                build_released_header(row[1]), original_bytes=len(row[1])
            )
            connection.execute(
                "UPDATE message_observations SET parsed_json = ? WHERE observation_seq = ?",
                (released, int(row[0])),
            )
            connection.execute(
                "UPDATE messages SET body_available = 0 WHERE message_id = ?",
                (message_id,),
            )

    def _current_payload(self, message_id: str) -> bytes:
        with self.database.connection() as connection:
            return connection.execute(
                "SELECT parsed_json FROM message_observations WHERE observation_seq = "
                "(SELECT current_observation_seq FROM messages WHERE message_id = ?)",
                (message_id,),
            ).fetchone()[0]

    def _state(self, message_id: str):
        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT current_observation_seq, body_available FROM messages WHERE message_id = ?",
                (message_id,),
            ).fetchone()
            count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM message_observations WHERE message_id = ?",
                    (message_id,),
                ).fetchone()[0]
            )
        return int(row[0]), int(row[1]), count

    def test_rehydration_same_digest_is_not_a_new_episode(self) -> None:
        message_id = str(self.message["message_id"])
        self._release(message_id)
        seq_before, _, count_before = self._state(message_id)
        # Re-admit the identical source content through the real admission path.
        self._readmit(self.original)
        seq_after, body_after, count_after = self._state(message_id)
        self.assertEqual(seq_after, seq_before)
        self.assertEqual(count_after, count_before)
        self.assertEqual(body_after, 1)
        restored = self._current_payload(message_id)
        self.assertTrue(observation_payload_available(restored))

    def test_genuine_change_is_a_new_episode(self) -> None:
        from dataclasses import replace

        message_id = str(self.message["message_id"])
        seq_a, _, _ = self._state(message_id)
        original = self.original
        assert original is not None
        changed = replace(
            original,
            raw_content=str(original.raw_content) + " changed",
            sent_at_utc="2027-01-01T00:00:00+00:00",
        )
        self._readmit(changed)
        seq_b, _, _ = self._state(message_id)
        self.assertNotEqual(seq_a, seq_b, "changed content must append an episode")
        # Re-admitting the original content again is a third A->B->A episode.
        self._readmit(self.original)
        seq_a2, _, count = self._state(message_id)
        self.assertNotEqual(seq_a2, seq_b)
        self.assertGreaterEqual(count, 3)

    def test_corrections_use_original_retained_keys(self) -> None:
        message_id = str(self.message["message_id"])
        self._release(message_id)
        retained = decode_released_header(self._current_payload(message_id))
        assert retained is not None
        keys = _retained_identity_signatures(retained["retained"])
        self.assertTrue(keys)
        corrections = CorrectionService(self.database)
        key_kind, key_value = next(iter(keys))
        with self.database.transaction() as connection:
            found = corrections._message_ids_for_keys(
                connection, [{"key_kind": key_kind, "key_value": key_value}]
            )
        self.assertIn(message_id, found)

    def test_corrections_original_keys_survive_rebind(self) -> None:
        message_id = str(self.message["message_id"])
        self._release(message_id)
        retained = decode_released_header(self._current_payload(message_id))
        assert retained is not None
        original_keys = _retained_identity_signatures(retained["retained"])
        # Move the canonical binding elsewhere; the retained original keys must
        # still match, proving the correction does not depend on current binding.
        with self.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET sender_id = NULL WHERE message_id = ?",
                (message_id,),
            )
        corrections = CorrectionService(self.database)
        key_kind, key_value = next(iter(original_keys))
        with self.database.transaction() as connection:
            found = corrections._message_ids_for_keys(
                connection, [{"key_kind": key_kind, "key_value": key_value}]
            )
        self.assertIn(message_id, found)

    def test_maintenance_intentional_absence_vs_lying_row(self) -> None:
        message_id = str(self.message["message_id"])
        self._release(message_id)
        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT * FROM messages WHERE message_id = ?", (message_id,)
            ).fetchone()
            observation = connection.execute(
                "SELECT * FROM message_observations WHERE observation_seq = ?",
                (int(row["current_observation_seq"]),),
            ).fetchone()
        self.assertTrue(_matches(row, observation))
        with self.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET body_available = 1 WHERE message_id = ?",
                (message_id,),
            )
        with self.database.connection() as connection:
            lying = connection.execute(
                "SELECT * FROM messages WHERE message_id = ?", (message_id,)
            ).fetchone()
        self.assertFalse(_matches(lying, observation))

    def test_corrupt_observation_is_not_treated_as_released(self) -> None:
        message_id = str(self.message["message_id"])
        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT * FROM messages WHERE message_id = ?", (message_id,)
            ).fetchone()
            observation = connection.execute(
                "SELECT * FROM message_observations WHERE observation_seq = ?",
                (int(row["current_observation_seq"]),),
            ).fetchone()

        class _Corrupt:
            def __init__(self, base, payload):
                self._base = base
                self._payload = payload

            def __getitem__(self, key):
                return self._payload if key == "parsed_json" else self._base[key]

        self.assertFalse(_matches(row, _Corrupt(observation, b"corrupt-bytes")))


if __name__ == "__main__":
    unittest.main()

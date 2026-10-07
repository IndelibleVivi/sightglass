from __future__ import annotations

import unittest
from dataclasses import FrozenInstanceError

from sightglass.contracts.voice import VoiceCoverage, VoiceTranscriptItem, VoiceTranscriptPage


class VoiceContractTests(unittest.TestCase):
    def test_page_shape_and_immutable_rows(self) -> None:
        item = VoiceTranscriptItem(
            "synthetic-msg", "synthetic-resource", 0, "ready", "Synthetic", None
        )
        page = VoiceTranscriptPage(
            "synthetic-token", (item,), VoiceCoverage(selected=1, ready=1),
            True, True, False, "1", "2026-09-19T00:00:00+00:00",
        )
        payload = page.as_dict()
        self.assertEqual(payload["schema"], "sightglass.voice-page.v2")
        self.assertEqual(
            payload["fields"],
            ["message_id", "resource_id", "ordinal", "state", "text", "error_code"],
        )
        self.assertEqual(payload["items"], [item.as_row()])
        self.assertEqual(len(payload["coverage"]), 8)
        self.assertEqual(payload["derivation"], {"kind": "derived_transcript"})
        with self.assertRaises(FrozenInstanceError):
            setattr(item, "text", "changed")

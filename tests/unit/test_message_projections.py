from __future__ import annotations

import json
import unittest
from typing import Any

from sightglass.reader.projections import DetailMessageProjector
from sightglass.source.identity import SignedTokenCodec


class _StubRepository:
    def __init__(self, row: dict[str, Any]) -> None:
        self.row = row

    def preferred_labels_bulk(self, pairs: Any) -> dict[Any, tuple[str, str]]:
        return {pair: ("Fixture Sender", "source_surface") for pair in pairs}

    def resources_for_messages_bulk(self, message_ids: Any) -> dict[str, list[Any]]:
        return {}


def _row(structured: dict[str, Any], *, kind: str = "link") -> dict[str, Any]:
    return {
        "message_id": "wxmsg_fixture",
        "account_id": "wxacct_fixture",
        "conversation_id": "wxconv_fixture",
        "sender_id": "wxperson_fixture",
        "sender_membership_id": None,
        "sender_label_snapshot_json": json.dumps({"shown_as": None, "is_self": False}),
        "structured_json": json.dumps(structured),
        "sort_primary": "2026-09-01T00:00:00.000000+00:00",
        "sort_seq": 1,
        "sort_tie": 1,
        "sent_at_utc": "2026-09-01T00:00:00.000000+00:00",
        "kind": kind,
        "text": "Article title",
        "current_state": "present",
        "last_seen_at": "2026-09-01T00:00:01.000000+00:00",
        "current_generation_id": "wxgeneration_fixture",
        "source_message_id": "wxsource_fixture",
        "sender_resolution_state": "stable",
        "sender_identity_confidence": "exact",
    }


def _project(structured: dict[str, Any], *, kind: str = "link") -> dict[str, Any]:
    projector = DetailMessageProjector(
        _StubRepository(_row(structured, kind=kind)),  # type: ignore[arg-type]
        SignedTokenCodec(b"projection-fixture-secret-32bytes"),
    )
    return projector.project(
        [_row(structured, kind=kind)],
        timezone_name="Asia/Singapore",
        include_resources=False,
        focus_ids=(),
    )[0]


class DetailLinkProjectionTests(unittest.TestCase):
    def test_detail_link_omits_raw_url_credentials_query_and_fragment(self) -> None:
        projected = _project(
            {
                "link": {
                    "app_type": 5,
                    "title": "Article title",
                    "description": "Article description",
                    "source_name": "Example Source",
                    "raw_url": "https://user:pass@Example.COM:8443/a/b?token=SECRET#frag",
                    "scheme": "https",
                    "host": "example.com",
                    "path": "/a/b",
                    "display_url": "example.com/a/b",
                    "fetched": False,
                }
            }
        )

        link = projected["link"]
        self.assertEqual(link["scheme"], "https")
        self.assertEqual(link["host"], "example.com")
        self.assertEqual(link["path"], "/a/b")
        self.assertEqual(link["display_url"], "example.com/a/b")
        self.assertEqual(link["source_name"], "Example Source")
        self.assertEqual(link["app_type"], 5)
        self.assertNotIn("raw_url", link)
        serialized = json.dumps(projected)
        for forbidden in ("user:pass", "SECRET", "#frag", "?", "8443", "fetched"):
            self.assertNotIn(forbidden, serialized)

    def test_detail_link_derives_normalized_parts_from_legacy_raw_url(self) -> None:
        projected = _project(
            {
                "link": {
                    "title": "Legacy row",
                    "raw_url": "http://Example.invalid/legacy/path?secret=1#frag",
                }
            }
        )

        link = projected["link"]
        self.assertEqual(link["scheme"], "http")
        self.assertEqual(link["host"], "example.invalid")
        self.assertEqual(link["path"], "/legacy/path")
        self.assertEqual(link["display_url"], "example.invalid/legacy/path")
        serialized = json.dumps(projected)
        for forbidden in ("secret", "#frag", "?"):
            self.assertNotIn(forbidden, serialized)

    def test_detail_forwarded_chat_passes_bounded_items_only(self) -> None:
        projected = _project(
            {
                "forwarded_chat": {
                    "title": "Group transcript",
                    "description": "forwarded history",
                    "declared_count": 12,
                    "total_count": 12,
                    "returned_count": 1,
                    "truncated": True,
                    "parse_state": "parsed",
                    "items": [
                        {
                            "sender": "Person 0",
                            "sent_at_text": "2026-09-01 10:00",
                            "kind": "text",
                            "text": "body 0",
                        }
                    ],
                }
            },
            kind="forwarded_chat",
        )

        forwarded = projected["forwarded_chat"]
        self.assertEqual(forwarded["returned_count"], 1)
        self.assertTrue(forwarded["truncated"])
        self.assertEqual(forwarded["items"][0]["text"], "body 0")

    def test_detail_absent_link_stays_null(self) -> None:
        projected = _project({})

        self.assertIsNone(projected["link"])
        self.assertIsNone(projected["forwarded_chat"])


if __name__ == "__main__":
    unittest.main()

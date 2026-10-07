from __future__ import annotations

import json
import unittest

from sightglass.contracts.messages import SourceMessage
from sightglass.model.repositories import message_search_fields
from sightglass.source.parser import parse_message, public_link


def _message(content: str, *, wechat_type: int = 49) -> SourceMessage:
    return SourceMessage(
        source_message_id="wxmsg_fixture",
        source_conversation_id="synthetic-contact",
        conversation_kind="direct",
        source_time_raw="1725000000",
        sent_at_utc="2024-08-30T06:40:00.000000+00:00",
        observed_at_utc="2024-08-30T06:41:00.000000+00:00",
        sort_seq=1,
        source_rowid=1,
        wechat_type=wechat_type,
        raw_content=content,
        is_outgoing=False,
        source_generation_id="fixture-generation",
        logical_shard_key="message/message_0.db",
    )


def _recordinfo(count: int, *, malformed: bool = False) -> str:
    items = "".join(
        (
            f'<dataitem datatype="1"><sourcename>Person {index}</sourcename>'
            f"<sourcetime>2026-09-01 10:0{index % 10}</sourcetime>"
            f"<datatitle>title {index}</datatitle>"
            f"<datadesc>body {index}</datadesc></dataitem>"
        )
        for index in range(count)
    )
    if malformed:
        return '<recordinfo><title>broken</title><datalist count="3">' + items
    return (
        "<recordinfo><title>Group transcript</title><desc>forwarded history</desc>"
        f'<datalist count="{count}">{items}</datalist></recordinfo>'
    )


def _forwarded_chat_message(record_xml: str, *, cdata: bool = True) -> str:
    inner = f"<![CDATA[{record_xml}]]>" if cdata else record_xml
    return (
        "<msg><appmsg><title>Group transcript</title><type>19</type>"
        f"<des>forwarded history</des><recorditem>{inner}</recorditem></appmsg></msg>"
    )


def _link_message(url: str) -> str:
    return (
        "<msg><appmsg><title>Article title</title><des>Article description</des>"
        f"<type>5</type><url>{url}</url>"
        "<sourcedisplayname>Example Source</sourcedisplayname></appmsg></msg>"
    )


class ForwardedChatParsingTests(unittest.TestCase):
    def test_forwarded_chat_items_are_ordered_bounded_and_counted(self) -> None:
        parsed = parse_message(_message(_forwarded_chat_message(_recordinfo(12))))

        self.assertEqual(parsed.kind, "forwarded_chat")
        forwarded = parsed.structured["forwarded_chat"]
        self.assertEqual(forwarded["title"], "Group transcript")
        self.assertEqual(forwarded["description"], "forwarded history")
        self.assertEqual(forwarded["declared_count"], 12)
        self.assertEqual(forwarded["total_count"], 12)
        self.assertEqual(forwarded["returned_count"], 8)
        self.assertTrue(forwarded["truncated"])
        self.assertEqual(len(forwarded["items"]), 8)
        self.assertEqual(
            [item["text"] for item in forwarded["items"]],
            [f"body {index}" for index in range(8)],
        )
        self.assertEqual(forwarded["items"][0]["sender"], "Person 0")
        self.assertEqual(forwarded["items"][0]["sent_at_text"], "2026-09-01 10:00")
        self.assertEqual(forwarded["items"][0]["kind"], "text")

    def test_forwarded_chat_accepts_escaped_entity_record_item(self) -> None:
        escaped = (
            _recordinfo(2)
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        parsed = parse_message(
            _message(_forwarded_chat_message(escaped, cdata=False))
        )

        forwarded = parsed.structured["forwarded_chat"]
        self.assertEqual(forwarded["returned_count"], 2)
        self.assertFalse(forwarded["truncated"])

    def test_forwarded_chat_malformed_record_fails_closed(self) -> None:
        parsed = parse_message(
            _message(_forwarded_chat_message(_recordinfo(3, malformed=True)))
        )

        forwarded = parsed.structured["forwarded_chat"]
        self.assertEqual(parsed.kind, "forwarded_chat")
        self.assertEqual(forwarded["items"], [])
        self.assertEqual(forwarded["returned_count"], 0)
        self.assertTrue(forwarded["truncated"])
        self.assertEqual(forwarded["parse_state"], "unreadable")

    def test_forwarded_chat_oversized_record_fails_closed(self) -> None:
        oversized = _recordinfo(1).replace(
            "<desc>forwarded history</desc>",
            "<desc>" + "x" * (512 * 1024) + "</desc>",
        )
        parsed = parse_message(_message(_forwarded_chat_message(oversized)))

        forwarded = parsed.structured["forwarded_chat"]
        self.assertEqual(forwarded["items"], [])
        self.assertTrue(forwarded["truncated"])
        self.assertEqual(forwarded["parse_state"], "unreadable")

    def test_forwarded_chat_projection_stays_content_only(self) -> None:
        record = (
            "<recordinfo><title>Group transcript</title>"
            '<datalist count="1"><dataitem datatype="1" dataid="wxid_secret">'
            "<sourcename>Person</sourcename>"
            "<sourcetime>2026-09-01 10:00</sourcetime>"
            "<datatitle>title</datatitle><datadesc>body</datadesc>"
            '<dataurl>https://example.invalid/a?b=c#d</dataurl>'
            "</dataitem></datalist></recordinfo>"
        )
        parsed = parse_message(_message(_forwarded_chat_message(record)))
        from sightglass.source.parser import public_forwarded_chat

        raw_url = parsed.structured["forwarded_chat"]["items"][0]["link"]["raw_url"]
        self.assertEqual(raw_url, "https://example.invalid/a?b=c#d")
        serialized = json.dumps(public_forwarded_chat(parsed.structured["forwarded_chat"]))

        for forbidden in (
            "wxid_secret",
            "dataurl",
            "https://",
            "/Users/",
            "?b=c",
            "#d",
        ):
            self.assertNotIn(forbidden, serialized)


class LinkProjectionTests(unittest.TestCase):
    def test_finder_share_is_projected_as_a_safe_link(self) -> None:
        parsed = parse_message(
            _message(
                "<msg><appmsg><title>Finder clip</title><des>Clip description</des>"
                "<type>51</type><url>https://example.invalid/finder?token=secret</url>"
                "<finderFeed><nickname>Finder creator</nickname></finderFeed>"
                "</appmsg></msg>"
            )
        )

        self.assertEqual(parsed.kind, "link")
        projected = public_link(parsed.structured["link"])
        assert projected is not None
        self.assertEqual(projected["app_type"], 51)
        self.assertEqual(projected["display_url"], "example.invalid/finder")
        self.assertNotIn("token", json.dumps(projected))

    def test_public_link_removes_credentials_query_and_fragment(self) -> None:
        parsed = parse_message(
            _message(
                _link_message(
                    "https://user:pass@Example.COM:8443/a/b?token=SECRET#frag"
                )
            )
        )
        projected = public_link(parsed.structured["link"])

        self.assertIsNotNone(projected)
        assert projected is not None
        self.assertEqual(projected["scheme"], "https")
        self.assertEqual(projected["host"], "example.com")
        self.assertEqual(projected["path"], "/a/b")
        self.assertEqual(projected["display_url"], "example.com/a/b")
        self.assertEqual(projected["title"], "Article title")
        self.assertEqual(projected["description"], "Article description")
        self.assertEqual(projected["source_name"], "Example Source")
        serialized = json.dumps(projected)
        for forbidden in ("user:pass", "token", "SECRET", "#frag", "?"):
            self.assertNotIn(forbidden, serialized)

    def test_public_link_rejects_non_mapping_values(self) -> None:
        self.assertIsNone(public_link(None))
        self.assertIsNone(public_link("https://example.invalid"))

    def test_public_link_keeps_port_free_display_without_raw_url(self) -> None:
        parsed = parse_message(_message(_link_message("https://example.invalid/a")))
        projected = public_link(parsed.structured["link"])

        assert projected is not None
        self.assertNotIn("raw_url", projected)
        self.assertEqual(projected["display_url"], "example.invalid/a")

    def test_search_fields_use_normalized_host_and_path_without_query(self) -> None:
        parsed = parse_message(
            _message(
                _link_message(
                    "https://user:pass@Example.COM:8443/a/b?token=SECRET#frag"
                )
            )
        )
        fields = message_search_fields(parsed)

        self.assertEqual(fields["link.host"], "example.com")
        self.assertEqual(fields["link.path"], "/a/b")
        self.assertEqual(fields["link.title"], "Article title")
        self.assertEqual(fields["link.source_name"], "Example Source")
        serialized = json.dumps(fields)
        for forbidden in ("user:pass", "SECRET", "#frag", "?", "token"):
            self.assertNotIn(forbidden, serialized)


if __name__ == "__main__":
    unittest.main()

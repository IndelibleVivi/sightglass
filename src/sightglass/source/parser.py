from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET
from typing import Any
from urllib.parse import urlsplit

from sightglass.contracts.messages import ParsedMessage, SourceMessage
from sightglass.source.message_identity import native_message_token

PARSER_VERSION = "sightglass.wechat-parser.v2"

_MAX_RECORDITEM_BYTES = 256 * 1024
_MAX_FORWARDED_ITEMS = 8
_MAX_FORWARDED_TEXT_CHARS = 200
_MAX_FORWARDED_LABEL_CHARS = 120
_MAX_LINK_TEXT_CHARS = 300
_SCHEME = re.compile(r"[a-z][a-z0-9+.\-]{0,31}")


_SIMPLE_KINDS = {
    1: "text",
    3: "image",
    34: "voice",
    42: "contact_card",
    43: "video",
    47: "sticker",
    48: "location",
    10000: "system",
    10002: "recalled",
}


def _remove_verified_group_prefix(message: SourceMessage) -> str:
    content = message.raw_content
    if message.conversation_kind != "group" or message.is_outgoing:
        return content
    eligible = [key.value for key in message.sender_keys if key.value]
    for value in eligible:
        prefix = f"{value}:\n"
        if content.startswith(prefix):
            return content[len(prefix) :]
    return content


def _xml_text(root: ET.Element, path: str) -> str | None:
    value = root.findtext(path)
    return value if value is not None else None


def _bounded_text(value: Any, limit: int) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text[:limit] if text else None


def _recordinfo_element(root: ET.Element) -> ET.Element | None:
    """Bounded inner record document for an appmsg forwarded chat (type 19).

    The transcript arrives either as nested XML, as CDATA, or as entity-escaped
    text. Anything absent, oversized, or unparseable fails closed to ``None`` so the
    caller projects an explicitly incomplete structure instead of partial guesses.
    """
    nested = root.find(".//appmsg//recordinfo")
    if nested is not None:
        return nested
    for element in root.findall(".//appmsg//recorditem"):
        text = "".join(element.itertext())
        candidates = [text]
        if "<recordinfo" not in text:
            unescaped = html.unescape(text)
            if unescaped != text:
                candidates.append(unescaped)
        for candidate in candidates:
            if "<recordinfo" not in candidate:
                continue
            if len(candidate.encode("utf-8", errors="ignore")) > _MAX_RECORDITEM_BYTES:
                return None
            start = candidate.find("<recordinfo")
            end = candidate.rfind("</recordinfo>")
            if end >= start:
                candidate = candidate[start : end + len("</recordinfo>")]
            try:
                return ET.fromstring(candidate)
            except ET.ParseError:
                continue
    return None


def _recordinfo_total(record: ET.Element) -> int:
    datalist = record.find(".//datalist")
    if datalist is None:
        return 0
    try:
        return max(0, int(datalist.attrib.get("count") or 0))
    except ValueError:
        return 0


def _forwarded_item(dataitem: ET.Element) -> dict[str, Any] | None:
    sender = _bounded_text(_xml_text(dataitem, "sourcename"), _MAX_FORWARDED_LABEL_CHARS)
    sent_at_text = _bounded_text(_xml_text(dataitem, "sourcetime"), _MAX_FORWARDED_LABEL_CHARS)
    title = _bounded_text(_xml_text(dataitem, "datatitle"), _MAX_FORWARDED_TEXT_CHARS)
    description = _bounded_text(_xml_text(dataitem, "datadesc"), _MAX_FORWARDED_TEXT_CHARS)
    data_format = _bounded_text(_xml_text(dataitem, "datafmt"), 32)
    datatype = (dataitem.attrib.get("datatype") or "").strip()
    raw_url = _xml_text(dataitem, "dataurl") or _xml_text(dataitem, "weburl")
    if raw_url:
        kind = "link"
    elif data_format or datatype == "8":
        kind = "file"
    elif title or description:
        kind = "text"
    else:
        kind = "unknown"
    if (
        sender is None
        and sent_at_text is None
        and title is None
        and description is None
        and data_format is None
        and raw_url is None
    ):
        return None
    result = {
        "sender": sender,
        "sent_at_text": sent_at_text,
        "kind": kind,
        "text": description or title or "",
    }
    if raw_url:
        result["link"] = {"raw_url": raw_url, "title": title, "description": description}
    return result


def public_forwarded_chat(value: Any) -> dict[str, Any] | None:
    """Keep newly parsed inner raw URLs private on ordinary message projection."""
    if not isinstance(value, dict):
        return None
    result = dict(value)
    result["items"] = [
        {**item, "link": public_link(item["link"])} if "link" in item else dict(item)
        for item in value.get("items", [])
        if isinstance(item, dict)
    ]
    return result


def _forwarded_chat(root: ET.Element, title: str | None, description: str | None) -> dict[str, Any]:
    bounded_title = _bounded_text(title, _MAX_LINK_TEXT_CHARS)
    bounded_description = _bounded_text(description, _MAX_LINK_TEXT_CHARS)
    record = _recordinfo_element(root)
    if record is None:
        return {
            "title": bounded_title,
            "description": bounded_description,
            "declared_count": 0,
            "total_count": 0,
            "returned_count": 0,
            "items": [],
            "truncated": True,
            "parse_state": "unreadable",
        }
    total_count = _recordinfo_total(record)
    items: list[dict[str, Any]] = []
    for dataitem in record.findall(".//dataitem"):
        if len(items) >= _MAX_FORWARDED_ITEMS:
            break
        item = _forwarded_item(dataitem)
        if item is not None:
            items.append(item)
    returned_count = len(items)
    return {
        "title": bounded_title,
        "description": bounded_description,
        "declared_count": total_count,
        "total_count": total_count,
        "returned_count": returned_count,
        "items": items,
        "truncated": returned_count < total_count or total_count == 0,
        "parse_state": "parsed",
    }


def _normalized_parts(link: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
    scheme = link.get("scheme")
    host = link.get("host")
    path = link.get("path")
    if (
        not isinstance(scheme, str)
        or not isinstance(host, str)
        or not (isinstance(path, str) or path is None)
    ):
        raw_url = link.get("raw_url")
        if isinstance(raw_url, str) and raw_url:
            try:
                parsed_url = urlsplit(raw_url)
            except ValueError:
                parsed_url = None
            if parsed_url is not None:
                scheme = parsed_url.scheme
                host = parsed_url.hostname
                path = parsed_url.path or None
    normalized_scheme = (
        scheme.casefold()
        if isinstance(scheme, str) and _SCHEME.fullmatch(scheme.casefold())
        else None
    )
    normalized_host = host.casefold() if isinstance(host, str) and host else None
    normalized_path = path if isinstance(path, str) and path else None
    return normalized_scheme, normalized_host, normalized_path


def public_link(link: Any) -> dict[str, Any] | None:
    """MCP-visible link projection: no credentials, query, fragment, or raw URL."""
    if not isinstance(link, dict):
        return None
    scheme, host, path = _normalized_parts(link)
    app_type = link.get("app_type")
    return {
        "title": _bounded_text(link.get("title"), _MAX_LINK_TEXT_CHARS),
        "description": _bounded_text(link.get("description"), _MAX_LINK_TEXT_CHARS),
        "source_name": _bounded_text(link.get("source_name"), _MAX_LINK_TEXT_CHARS),
        "app_type": app_type if isinstance(app_type, int) else None,
        "scheme": scheme,
        "host": host,
        "path": path,
        "display_url": f"{host}{path or ''}" if host else None,
    }


def _parse_app_message(content: str, resources, message: SourceMessage) -> ParsedMessage:
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return ParsedMessage(
            kind="unknown",
            text="[暂不支持的消息类型]",
            structured={"wechat_type": 49, "raw_payload_available": True},
            resources=resources,
            derivation_text_kind="placeholder",
        )
    app_type_text = _xml_text(root, ".//appmsg/type") or "0"
    try:
        app_type = int(app_type_text)
    except ValueError:
        app_type = 0
    title = _xml_text(root, ".//appmsg/title")
    description = _xml_text(root, ".//appmsg/des")
    source_name = _xml_text(root, ".//appmsg/sourcedisplayname")
    url = _xml_text(root, ".//appmsg/url")
    if app_type == 57:
        reply_source = None
        server_id = _xml_text(root, ".//refermsg/svrid")
        if message.source_message_id.startswith("nmsg_") and server_id is not None:
            if re.fullmatch(r"[1-9][0-9]{0,18}", server_id) and int(server_id) < (1 << 63):
                reply_source = native_message_token(
                    message.source_conversation_id, "server", (int(server_id),)
                )
        return ParsedMessage(
            kind="reply",
            text=title or "",
            structured={
                "reply_target_source_message_id": reply_source,
                "reply": {
                    "target_message_id": None,
                    "quoted_sender": _xml_text(root, ".//refermsg/displayname"),
                    "quoted_text": _xml_text(root, ".//refermsg/content"),
                    "resolved": False,
                }
            },
            resources=resources,
        )
    if app_type == 6:
        return ParsedMessage(
            kind="file",
            text=title,
            structured={"description": description},
            resources=resources,
        )
    if app_type in {4, 5, 33, 36, 51}:
        kind = "mini_program" if app_type in {33, 36} else "link"
        try:
            parsed_url = urlsplit(url or "")
            scheme = parsed_url.scheme.casefold() or None
            host = parsed_url.hostname.casefold() if parsed_url.hostname else None
            path = parsed_url.path or None
        except ValueError:
            scheme = None
            host = None
            path = None
        display_url = f"{host}{path or ''}" if host is not None else (path or None)
        return ParsedMessage(
            kind=kind,
            text=title,
            structured={
                "link": {
                    "app_type": app_type,
                    "title": title,
                    "description": description,
                    "source_name": source_name,
                    "raw_url": url,
                    "scheme": scheme,
                    "host": host,
                    "path": path,
                    "display_url": display_url,
                    "fetched": False,
                }
            },
            resources=resources,
        )
    if app_type == 19:
        return ParsedMessage(
            kind="forwarded_chat",
            text=_bounded_text(title, _MAX_LINK_TEXT_CHARS),
            structured={"forwarded_chat": _forwarded_chat(root, title, description)},
            resources=resources,
        )
    return ParsedMessage(
        kind="unknown",
        text="[暂不支持的消息类型]",
        structured={"wechat_type": 49, "appmsg_type": app_type, "raw_payload_available": True},
        resources=resources,
        derivation_text_kind="placeholder",
    )


def parse_message(message: SourceMessage) -> ParsedMessage:
    content = _remove_verified_group_prefix(message)
    if message.wechat_type == 49:
        return _parse_app_message(content, message.resources, message)
    kind = _SIMPLE_KINDS.get(message.wechat_type)
    if kind is None:
        return ParsedMessage(
            kind="unknown",
            text="[暂不支持的消息类型]",
            structured={
                "wechat_type": message.wechat_type,
                "raw_payload_available": True,
            },
            resources=message.resources,
            derivation_text_kind="placeholder",
        )
    if kind in {"image", "sticker", "contact_card", "video", "voice", "location"}:
        # These raw fields are transport envelopes, not verified source-visible captions.
        text = None
    else:
        text = content
    return ParsedMessage(kind=kind, text=text, resources=message.resources)

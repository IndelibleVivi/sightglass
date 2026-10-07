"""Local-only URL extraction and bounded hostname candidate evidence."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import urlsplit, urlunsplit

LINK_EXTRACTION_VERSION = "sightglass.links.v1"
MAX_LINKS_PER_MESSAGE = 128
MAX_URL_CHARS = 8_192
_URL = re.compile(r"https?://[^\s<>\"'`，。！？；、]+", re.IGNORECASE)


@dataclass(frozen=True)
class ExtractedLink:
    source_path: str
    ordinal: int
    raw_url: str
    normalized_url: str
    scheme: str
    normalized_host: str
    path: str
    query_text: str
    fragment_text: str
    title: str | None
    description: str | None
    source_kind: str

    def digest(self) -> str:
        return hashlib.sha256(
            json.dumps(asdict(self), sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()


def normalize_url(value: str) -> dict[str, str] | None:
    raw = value.strip().rstrip(".,;:!?，。！？；、。】」』”’）")
    for left, right in (("(", ")"), ("[", "]"), ("{", "}")):
        while raw.endswith(right) and raw.count(right) > raw.count(left):
            raw = raw[:-1]
    if not raw or len(raw) > MAX_URL_CHARS:
        return None
    try:
        parsed = urlsplit(raw)
        scheme = parsed.scheme.casefold()
        if scheme not in {"http", "https"} or not parsed.hostname:
            return None
        host = parsed.hostname.rstrip(".").encode("idna").decode("ascii").casefold()
        if any(character.isspace() for character in host):
            return None
        port = parsed.port
        authority = f"[{host}]" if ":" in host else host
        if port is not None and (scheme, port) not in {("http", 80), ("https", 443)}:
            authority += f":{port}"
        if "@" in parsed.netloc:
            authority = parsed.netloc.rsplit("@", 1)[0] + "@" + authority
        normalized = urlunsplit((scheme, authority, parsed.path, parsed.query, parsed.fragment))
    except (ValueError, UnicodeError):
        return None
    return {
        "raw_url": raw,
        "normalized_url": normalized,
        "scheme": scheme,
        "normalized_host": host,
        "path": parsed.path,
        "query_text": parsed.query,
        "fragment_text": parsed.fragment,
    }


def extract_links(text: str | None, structured: dict[str, Any]) -> tuple[list[ExtractedLink], bool]:
    links: list[ExtractedLink] = []
    complete = True

    def collect(
        value: Any,
        path: str,
        kind: str,
        *,
        card: dict[str, Any] | None = None,
        explicit: bool = False,
    ) -> None:
        nonlocal complete
        if not isinstance(value, str):
            return
        matches = [value] if explicit else (match.group() for match in _URL.finditer(value))
        for ordinal, candidate in enumerate(matches):
            normalized = normalize_url(candidate)
            if normalized is None:
                complete = False
                continue
            if len(links) >= MAX_LINKS_PER_MESSAGE:
                complete = False
                return
            metadata = card or {}
            links.append(
                ExtractedLink(
                    source_path=path,
                    ordinal=ordinal,
                    source_kind=kind,
                    title=metadata.get("title") if isinstance(metadata.get("title"), str) else None,
                    description=(
                        metadata.get("description")
                        if isinstance(metadata.get("description"), str)
                        else None
                    ),
                    **normalized,
                )
            )

    collect(text, "text", "text")
    card = structured.get("link")
    if isinstance(card, dict):
        collect(card.get("raw_url"), "link.raw_url", "link_card", card=card, explicit=True)
        for field in ("title", "description", "source_name"):
            collect(card.get(field), f"link.{field}", "link_card", card=card)
    forwarded = structured.get("forwarded_chat")
    if isinstance(forwarded, dict):
        if forwarded.get("truncated"):
            complete = False
        for index, item in enumerate(forwarded.get("items", [])):
            if not isinstance(item, dict):
                continue
            prefix = f"forwarded_chat.items[{index}]"
            collect(item.get("text"), f"{prefix}.text", "forwarded_text")
            inner = item.get("link")
            if isinstance(inner, dict):
                collect(
                    inner.get("raw_url"),
                    f"{prefix}.link.raw_url",
                    "forwarded_link",
                    card=inner,
                    explicit=True,
                )
    return links, complete


def hostname_forms(host: str) -> tuple[str, ...]:
    tokens = re.findall(r"[a-z0-9]+", host.casefold())
    if tokens and tokens[0] == "www":
        tokens = tokens[1:]
    stem = tokens[:-1] if len(tokens) > 1 else tokens
    return tuple(dict.fromkeys([*stem, " ".join(stem), "-".join(stem), "".join(stem)]))


def normalize_domain(value: str) -> str | None:
    parsed = normalize_url(value if "://" in value else f"https://{value}")
    return parsed["normalized_host"] if parsed is not None else None


def _one_edit(left: str, right: str) -> bool:
    if abs(len(left) - len(right)) > 1:
        return False
    if len(left) == len(right):
        return sum(a != b for a, b in zip(left, right, strict=True)) <= 1
    if len(left) > len(right):
        left, right = right, left
    index = next((i for i, pair in enumerate(zip(left, right)) if pair[0] != pair[1]), len(left))
    return left[index:] == right[index + 1 :]


def hint_evidence(host: str, hints: tuple[str, ...]) -> tuple[str, ...]:
    evidence: list[str] = []
    forms = hostname_forms(host)
    for hint in hints:
        domain = normalize_domain(hint)
        if domain == host:
            evidence.append("hostname_hint_exact")
            continue
        query_forms = hostname_forms(domain) if domain else tuple(hint.casefold().split())
        if any(value and value in forms for value in query_forms):
            evidence.append("hostname_hint_normalized")
        elif any(
            len(left) >= 4
            and len(right) >= 4
            and (
                _one_edit(left.rstrip("s"), right.rstrip("s"))
                or (
                    min(len(left), len(right)) >= 6
                    and (left.startswith(right) or right.startswith(left))
                )
            )
            for left in query_forms
            for right in forms
        ):
            evidence.append("hostname_hint_fuzzy")
    return tuple(dict.fromkeys(evidence))

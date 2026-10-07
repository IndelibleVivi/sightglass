"""One canonical current-body representation for an admitted message.

An admitted message previously stored its ordinary text three times: the
``messages.text`` body column, a ``search_text`` document, and a ``text`` field
embedded inside ``structured_json``. Only one of those is the current-body
source of truth; the others were either exact duplicates or a *superset* that
only added independently meaningful card fields.

This module owns the single normalized representation and the shared readers
that reconstruct the full message mapping:

* ``stored_structured_json`` writes the card/reply/forwarded envelope **without**
  re-embedding the body text, because ``messages.text`` already holds it.
* ``stored_search_text`` keeps the joined card search document only when it adds
  something beyond the body; an exact duplicate of the body is stored as NULL so
  ``search_text or text`` remains the canonical recall document.
* ``message_view`` reconstructs ``{"kind","text",**structured,"resources"}`` for
  every reader (literal matching, links, renderer, semantic recipe and
  observation consistency) from either a normalized row or a legacy v10 row,
  which is why no startup bulk rewrite is required.

New writes are normalized; existing v10 rows stay readable through the same
``message_view`` reconstruction and are never rewritten at startup.
"""

from __future__ import annotations

import json
from typing import Any

from sightglass.contracts.messages import ParsedMessage

CURRENT_BODY_VERSION = "sightglass.current-body.v1"

class LegacyBodyConflict(ValueError):
    """A legacy row's embedded ``structured_json.text`` contradicts its body column.

    Normalization must never silently drop a conflicting value: the caller either
    fails closed or retains the row with clear evidence.
    """

def stored_structured_json(parsed: ParsedMessage) -> str:
    """The current-body structured envelope with the body text omitted.

    Card, reply, forwarded and resource fields are retained verbatim; only the
    ordinary body text is dropped because :attr:`messages.text` is its single
    stored source.
    """

    value = {
        "kind": parsed.kind,
        **parsed.structured,
        "resources": [resource.as_dict() for resource in parsed.resources],
    }
    return json.dumps(value, ensure_ascii=False, sort_keys=True)

def stored_search_text(parsed: ParsedMessage, *, rendered: str | None) -> str | None:
    """Keep the card search document only when it is not just the body copy."""

    if rendered is None or rendered == parsed.text:
        return None
    return rendered

def current_body_text(row: Any) -> str | None:
    """The single stored current-body source."""

    return row["text"]

def current_search_document(row: Any) -> str:
    """The canonical recall document: card search text, else the body.

    Byte-identical to the previous ``search_text or text or ""`` expression: an
    empty ``search_text`` is falsy and falls through to the body, and an absent
    body yields ``""``. An exact-duplicate ``search_text`` is stored as NULL, so
    a single owner produces the same document.
    """

    return str(row["search_text"] or row["text"] or "")

def card_search_values(row: Any) -> tuple[str, ...]:
    """Independently meaningful card fields still stored in ``structured_json``.

    Top-level ``title``/``description`` remain the semantic recipe's card inputs
    after the duplicate body text is omitted.
    """

    structured = row["structured_json"]
    if not structured:
        return ()
    parsed = json.loads(str(structured))
    if not isinstance(parsed, dict):
        return ()
    return tuple(
        value
        for field in ("title", "description")
        if isinstance(value := parsed.get(field), str) and value.strip()
    )

def message_view(row: Any) -> dict[str, Any]:
    """Reconstruct the canonical message mapping from a stored row.

    Works for both normalized rows (no ``text`` in ``structured_json``) and
    legacy v10 rows (which still embed it): the stored body column always wins,
    so the reconstruction is byte-identical to the on-write ``parsed_value``.
    """

    structured = row["structured_json"]
    value: dict[str, Any] = {}
    if structured:
        parsed = json.loads(str(structured))
        if isinstance(parsed, dict):
            value.update(parsed)
    value["kind"] = row["kind"]
    value["text"] = row["text"]
    value.setdefault("resources", [])
    return value

def normalized_columns(row: Any) -> dict[str, str | None]:
    """Representation-only normalization of one stored row.

    Returns the ``structured_json``/``search_text`` values that drop the exact
    duplicate body representations. The body column and every non-body field are
    preserved. Only an *exact* ``structured_json.text`` equal to ``row["text"]``
    is removed; a conflicting legacy value raises :class:`LegacyBodyConflict`
    rather than losing data. ``search_text`` is nulled only when it is an exact
    duplicate of the body.
    """

    structured = row["structured_json"]
    value: dict[str, Any] = {}
    if structured:
        try:
            parsed = json.loads(str(structured))
        except (ValueError, TypeError):
            raise LegacyBodyConflict("invalid structured_json") from None
        if not isinstance(parsed, dict):
            raise LegacyBodyConflict("structured_json is not a message mapping")
        value = parsed
    if "text" in value:
        embedded = value.get("text")
        if embedded != row["text"]:
            raise LegacyBodyConflict(
                "structured_json.text conflicts with the body column"
            )
        value = {key: item for key, item in value.items() if key != "text"}
        normalized_structured: str | None = json.dumps(value, ensure_ascii=False, sort_keys=True)
    else:
        normalized_structured = None if structured is None else str(structured)
    search_text = row["search_text"]
    # NULL only an exact duplicate of the body. Never coerce an absent body to
    # "" to widen the transformation, and keep a distinct NULL/empty column.
    if search_text is not None and row["text"] is not None and search_text == row["text"]:
        search_text = None
    return {"structured_json": normalized_structured, "search_text": search_text}

def canonical_structured_json(row: Any) -> str:
    """Representation-invariant structured content for a stable identity digest.

    Drops an embedded ``text`` field that duplicates the body column so a legacy
    row and its normalized candidate hash identically. A conflicting embedded
    value is *not* dropped (``normalized_columns`` owns that failure); it stays
    here so the digest still reflects the distinct content.
    """

    structured = row["structured_json"]
    if not structured:
        return "{}"
    parsed = json.loads(str(structured))
    if not isinstance(parsed, dict):
        return str(structured)
    if parsed.get("text") == row["text"] and "text" in parsed:
        parsed = {key: item for key, item in parsed.items() if key != "text"}
        return json.dumps(parsed, ensure_ascii=False, sort_keys=True)
    return str(structured)

"""One resident-body eligibility rule shared by reads, selection and publication.

The same premise gates three callers so they can never disagree about which rows
own a locally readable body:

* ordinary local body reads (``resident_body_predicate`` embeds it in SQL),
* bounded background/derivative candidate *selection* (a released skeleton must
  be skipped so traversal progresses without manufacturing coverage), and
* final link/lexical *publication* (rechecked inside the short writer so a row
  that lost its body between selection and commit can never regrow an empty
  derivative receipt).
"""

from __future__ import annotations

import sqlite3


def resident_body_predicate(alias: str = "m") -> str:
    return (
        f"{alias}.body_available=1 AND NOT EXISTS (SELECT 1 FROM body_release_jobs AS br "
        f"WHERE br.message_id={alias}.message_id) AND NOT EXISTS (SELECT 1 "
        f"FROM message_body_residency AS rb WHERE rb.message_id={alias}.message_id "
        "AND julianday(rb.expires_at)<=julianday('now'))"
    )

def body_is_eligible(connection: sqlite3.Connection, message_id: str) -> bool:
    """Python mirror of :func:`resident_body_predicate` for one exact row.

    Used at publication time so a rechecked row cannot publish a derivative once
    its body has been released or its temporary residency has expired, even when
    the selection query could not observe that yet.
    """

    return (
        connection.execute(
            f"SELECT 1 FROM messages AS m WHERE {resident_body_predicate()} "
            "AND m.message_id=?",
            (message_id,),
        ).fetchone()
        is not None
    )

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from sightglass.model.lexical import LEXICAL_RECIPE
from sightglass.source.synthetic import create_synthetic_source
from tests.fixtures.factory import build_test_stack


class SearchCandidateWindowTests(unittest.TestCase):
    def _mixed_history(
        self, *, history_epoch_current: bool
    ) -> tuple[Any, tuple[str, ...], str, int]:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        source = create_synthetic_source(root / "source")
        _provider, repository, service, tools = build_test_stack(
            source, root / "state" / "window.db"
        )
        self.addCleanup(tools.close)
        ids = tuple(
            item["conversation_id"] for item in tools.wechat_find_conversations("")["candidates"]
        )
        for conversation in ids:
            tools.wechat_read_messages(mode="recent", conversation_id=conversation, limit=20)
        epoch = service._projection_inventory_epoch()
        watermark = repository.observation_watermark()
        with repository.database.transaction() as connection:
            columns = [str(row[1]) for row in connection.execute("PRAGMA table_info(messages)")]
            for index, conversation in enumerate(ids):
                for current, count in ((False, 20_000), (True, 10)):
                    substitutions = {
                        "message_id": f"'synthetic-{'current' if current else 'history'}-{index}-' "
                        "|| printf('%05d', n)",
                        "source_message_id": f"'synthetic-source-{index}-' "
                        + ("|| printf('%05d', 100-n)" if current else "|| printf('%05d', n+1000)"),
                        "source_time_raw": "'synthetic fixture instant'",
                        "sent_at_utc": "'2021-01-01T00:00:00.000000+00:00'"
                        if current
                        else "'2020-01-01T00:00:00.000000+00:00'",
                        "sort_primary": "'2021-01-01T00:00:00.000000+00:00'"
                        if current
                        else "'2020-01-01T00:00:00.000000+00:00'",
                        "sort_seq": "17" if current else "n",
                        "sort_tie": "23" if current else "0",
                        "projection_epoch": "seed.projection_epoch"
                        if current or history_epoch_current
                        else "'synthetic-retired-epoch'",
                        # A retired-epoch body is released stock: the lifecycle
                        # clears its body (``body_available=0``) when its epoch is
                        # retired. Current-epoch history stays a resident body so
                        # the epoch-partition seek can be exercised.
                        "body_available": "seed.body_available"
                        if current or history_epoch_current
                        else "0",
                        "text": "CASE WHEN n % 2 = 0 THEN 'synthetic needle' "
                        "ELSE 'synthetic haystack' END"
                        if current
                        else "'synthetic haystack'",
                        "search_text": "CASE WHEN n % 2 = 0 THEN 'synthetic needle' "
                        "ELSE 'synthetic haystack' END"
                        if current
                        else "'synthetic haystack'",
                    }
                    projection = ",".join(
                        substitutions.get(name, f"seed.{name}") for name in columns
                    )
                    connection.execute(
                        f"""
                        WITH RECURSIVE positions(n) AS (
                            SELECT 0 UNION ALL SELECT n+1 FROM positions WHERE n < ?
                        ), seed AS (
                            SELECT * FROM messages WHERE conversation_id=? AND sender_id IS NOT NULL
                              AND projection_epoch=?
                            LIMIT 1
                        )
                        INSERT INTO messages({",".join(columns)})
                        SELECT {projection} FROM seed CROSS JOIN positions
                        """,
                        (count - 1, conversation, epoch),
                    )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM messages WHERE message_id LIKE 'synthetic-current-%' "
                    "AND projection_epoch=?", (epoch,),
                ).fetchone()[0],
                20,
            )
            sender = connection.execute(
                "SELECT sender_id FROM messages WHERE message_id='synthetic-current-0-00000'"
            ).fetchone()[0]
            alternate = connection.execute(
                "SELECT participant_id,membership_id FROM conversation_members "
                "WHERE conversation_id=? AND participant_id<>? LIMIT 1", (ids[0], sender),
            ).fetchone()
            assert alternate is not None
            connection.execute(
                "UPDATE messages SET sender_id=?,sender_membership_id=? "
                "WHERE message_id='synthetic-current-0-00002'", tuple(alternate),
            )
            connection.execute(
                "UPDATE messages SET first_observation_seq=? "
                "WHERE message_id='synthetic-current-0-00008'",
                (watermark + 1,),
            )
            connection.execute(
                "UPDATE messages SET current_observation_seq=? "
                "WHERE message_id='synthetic-current-1-00008'",
                (watermark + 1,),
            )
            connection.execute(
                "UPDATE messages SET current_state='recalled' "
                "WHERE message_id='synthetic-current-0-00009'"
            )
        return repository, ids, epoch, watermark

    def _bounded_window(
        self, repository: Any, ids: tuple[str, ...], **kwargs: Any
    ) -> tuple[Any, int]:
        original_connect = repository.database.connect
        steps = 0

        def bounded_connect():
            connection = original_connect()

            def progress() -> int:
                nonlocal steps
                steps += 100
                # A tiny current candidate set and PK evidence checks fit easily;
                # scanning/materializing a 40,000-row history does not.
                return int(steps > 30_000)

            connection.set_progress_handler(progress, 100)
            return connection

        with patch.object(repository.database, "connect", side_effect=bounded_connect):
            rows = repository.search_candidate_window(ids, **kwargs)
        return rows, steps

    def _reference(
        self, repository: Any, ids: tuple[str, ...], epoch: str, watermark: int
    ) -> list[Any]:
        with repository.database.connection() as connection:
            rows = connection.execute(
                "SELECT m.*, p.resolution_state AS sender_resolution_state, "
                "p.identity_confidence AS sender_identity_confidence FROM messages AS m "
                "LEFT JOIN participants AS p ON p.participant_id=m.sender_id "
                "WHERE m.projection_epoch=?",
                (epoch,),
            ).fetchall()
        return sorted(
            (
                dict(row)
                for row in rows
                if row["conversation_id"] in ids
                and row["current_state"] == "present"
                and row["body_available"] == 1
                and row["first_observation_seq"] is not None
                and row["current_observation_seq"] is not None
                and row["first_observation_seq"] <= watermark
                and row["current_observation_seq"] <= watermark
            ),
            key=lambda row: (
                row["sort_primary"],
                row["sort_seq"],
                row["sort_tie"],
                row["message_id"],
            ),
        )

    def test_current_epoch_seeks_partition_and_preserves_message_id_ties_and_watermark(
        self,
    ) -> None:
        repository, ids, epoch, watermark = self._mixed_history(history_epoch_current=False)
        reference = self._reference(repository, ids, epoch, watermark)
        first, steps = self._bounded_window(
            repository,
            ids,
            projection_epoch=epoch,
            observation_watermark=watermark,
            limit=3,
        )
        self.assertEqual([dict(row) for row in first], reference[:3])
        self.assertLess(steps, 30_000)
        boundary = tuple(
            first[-1][name] for name in ("sort_primary", "sort_seq", "sort_tie", "message_id")
        )
        second, _ = self._bounded_window(
            repository,
            ids,
            projection_epoch=epoch,
            observation_watermark=watermark,
            after_key=boundary,
            limit=3,
        )
        self.assertEqual([dict(row) for row in second], reference[3:6])
        late_boundary = tuple(
            reference[7][name] for name in ("sort_primary", "sort_seq", "sort_tie", "message_id")
        )
        late, _ = self._bounded_window(
            repository,
            ids,
            projection_epoch=epoch,
            observation_watermark=watermark,
            after_key=late_boundary,
            limit=3,
        )
        self.assertEqual([dict(row) for row in late], reference[8:11])
        self.assertTrue(all(row["projection_epoch"] == epoch for row in (*first, *second)))
        self.assertNotEqual(
            [row["source_message_id"] for row in first],
            sorted(row["source_message_id"] for row in first),
            "message-ID ordering must survive an index whose last key is source ID",
        )

    def test_ready_lexical_projection_uses_pk_without_repeated_recipe_scan_and_keeps_fallback(
        self,
    ) -> None:
        repository, ids, epoch, watermark = self._mixed_history(history_epoch_current=True)
        with repository.database.transaction() as connection:
            connection.execute("DELETE FROM message_lexical")
            connection.execute("DELETE FROM message_lexical_projection")
            connection.execute(
                "INSERT INTO message_lexical(rowid,text) "
                "SELECT rowid,COALESCE(search_text,text,'') FROM messages"
            )
            connection.execute(
                "INSERT INTO message_lexical_projection(message_id,source_observation_seq, "
                "input_digest,recipe,updated_at) SELECT message_id,current_observation_seq, "
                "'synthetic-unused-digest',?,'2026-01-01T00:00:00+00:00' FROM messages",
                (LEXICAL_RECIPE,),
            )
            connection.execute(
                "UPDATE message_lexical_projection SET recipe='synthetic-retired-recipe' "
                "WHERE message_id='synthetic-current-0-00000'"
            )
        reference = [
            row
            for row in self._reference(repository, ids, epoch, watermark)
            if "2021-01-01" in row["sent_at_utc"]
        ]
        participant = reference[0]["sender_id"]
        reference = [row for row in reference if row["sender_id"] == participant]
        # ``synthetic-current-0-00000`` carries a retired lexical recipe, so its
        # resident coverage is incomplete. A ready index must then fall back to
        # the bounded canonical scan rather than let the MATCH prefilter silently
        # drop that uncovered current resident.
        for state, queries, narrowed in (
            ("ready", ("needle",), False),
            ("building", ("needle",), False),
            ("rebuilding", ("needle",), False),
            ("ready", ("needle", "x"), False),
        ):
            with self.subTest(state=state, queries=queries):
                with repository.database.transaction() as connection:
                    connection.execute(
                        "UPDATE derived_index_state SET state=? WHERE index_kind='lexical'",
                        (state,),
                    )
                expected = [
                    row
                    for row in reference
                    if not narrowed
                    or "needle" in row["text"]
                ]
                rows, steps = self._bounded_window(
                    repository,
                    ids,
                    projection_epoch=epoch,
                    observation_watermark=watermark,
                    after_utc="2021-01-01T00:00:00+00:00",
                    before_utc="2022-01-01T00:00:00+00:00",
                    lexical_queries=queries,
                    participant_ids=(participant,),
                    limit=3,
                )
                self.assertEqual([dict(row) for row in rows], expected[:3])
                self.assertLess(steps, 30_000)
                boundary = tuple(
                    rows[-1][name]
                    for name in ("sort_primary", "sort_seq", "sort_tie", "message_id")
                )
                next_rows, _ = self._bounded_window(
                    repository,
                    ids,
                    projection_epoch=epoch,
                    observation_watermark=watermark,
                    after_utc="2021-01-01T00:00:00+00:00",
                    before_utc="2022-01-01T00:00:00+00:00",
                    lexical_queries=queries,
                    participant_ids=(participant,),
                    after_key=boundary,
                    limit=3,
                )
                self.assertEqual([dict(row) for row in next_rows], expected[3:6])

        # Restore every receipt to the current recipe; now the ready index covers
        # all in-scope residents and the MATCH accelerator may narrow again.
        with repository.database.transaction() as connection:
            connection.execute(
                "UPDATE message_lexical_projection SET recipe=? WHERE recipe=?",
                (LEXICAL_RECIPE, "synthetic-retired-recipe"),
            )
            connection.execute(
                "UPDATE derived_index_state SET state='ready' WHERE index_kind='lexical'"
            )
        covered_expected = [row for row in reference if "needle" in row["text"]]
        covered, covered_steps = self._bounded_window(
            repository,
            ids,
            projection_epoch=epoch,
            observation_watermark=watermark,
            after_utc="2021-01-01T00:00:00+00:00",
            before_utc="2022-01-01T00:00:00+00:00",
            lexical_queries=("needle",),
            participant_ids=(participant,),
            limit=3,
        )
        self.assertEqual([dict(row) for row in covered], covered_expected[:3])
        self.assertLess(covered_steps, 30_000)

    def test_account_window_seeks_each_timeline_without_sorting_full_histories(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = create_synthetic_source(root / "source")
            _provider, repository, _service, tools = build_test_stack(
                source, root / "state" / "window.db"
            )
            self.addCleanup(tools.close)
            candidates = tools.wechat_find_conversations("")["candidates"]
            ids = tuple(item["conversation_id"] for item in candidates)
            self.assertEqual(len(ids), 2)
            for conversation in ids:
                tools.wechat_read_messages(mode="recent", conversation_id=conversation, limit=20)
            with repository.database.transaction() as connection:
                columns = [str(row[1]) for row in connection.execute("PRAGMA table_info(messages)")]
                for index, conversation in enumerate(ids):
                    substitutions = {
                        "message_id": f"'synthetic-window-{index}-' || printf('%05d', n)",
                        "source_message_id": f"'synthetic-source-{index}-' || printf('%05d', n)",
                        "sort_primary": "'2020-01-01T00:00:00.000000+00:00'",
                        "sort_seq": "n",
                        "sort_tie": "0",
                    }
                    projection = ",".join(
                        substitutions.get(name, f"seed.{name}") for name in columns
                    )
                    connection.execute(
                        f"""
                        WITH RECURSIVE positions(n) AS (
                            SELECT 0 UNION ALL SELECT n + 1 FROM positions WHERE n < 19999
                        ), seed AS (SELECT * FROM messages WHERE conversation_id = ? LIMIT 1)
                        INSERT INTO messages ({",".join(columns)})
                        SELECT {projection} FROM seed CROSS JOIN positions
                        """,
                        (conversation,),
                    )

            def bounded_connect():
                connection = original_connect()
                calls = 0

                def progress() -> int:
                    nonlocal calls
                    calls += 1
                    # Two bounded index seeks need far fewer instructions; an
                    # account-wide IN/ORDER BY scan over 40,000 rows exceeds this.
                    return int(calls > 200)

                connection.set_progress_handler(progress, 1_000)
                return connection

            original_connect = repository.database.connect
            with patch.object(repository.database, "connect", side_effect=bounded_connect):
                first = repository.search_candidate_window(ids, limit=3)
                frontier = first[-1]
                after = (
                    str(frontier["sort_primary"]),
                    int(frontier["sort_seq"]),
                    int(frontier["sort_tie"]),
                    str(frontier["message_id"]),
                )
                second = repository.search_candidate_window(ids, after_key=after, limit=3)
                late = repository.search_candidate_window(
                    ids,
                    after_key=(
                        "2020-01-01T00:00:00.000000+00:00",
                        19998, 0, "synthetic-window-1-19998",
                    ),
                    limit=2,
                )
            self.assertEqual(
                [row["message_id"] for row in first + second],
                [f"synthetic-window-{index}-{n:05d}" for n in range(3) for index in range(2)],
            )
            self.assertEqual(
                [row["message_id"] for row in late],
                ["synthetic-window-0-19999", "synthetic-window-1-19999"],
            )

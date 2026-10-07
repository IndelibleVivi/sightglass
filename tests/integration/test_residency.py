"""Actual projection lifecycle and reader/operator boundary regressions."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.observation_codec import observation_payload_state
from sightglass.residency.decisions import ResidencySettings
from sightglass.residency.repository import ResidencyRepository
from sightglass.runtime import control
from sightglass.source.synthetic import _create_shard, _row, create_synthetic_source
from tests.fixtures.factory import build_test_stack


class ResidencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "source"
        self.window = Path(self.temp.name) / "state" / "window.db"
        create_synthetic_source(self.root)
        _, self.repository, self.service, self.tools = build_test_stack(
            self.root, self.window, residency_default=None
        )
        self.assertTrue(self.tools.wechat_status()["ready"])
        self.group = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        self.store = ResidencyRepository(self.repository.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def read(self, **extra):
        return self.service.read_messages(
            mode="recent", conversation_id=self.group, limit=10, **extra
        )

    def rows(self):
        with self.repository.database.connection() as connection:
            return connection.execute(
                "SELECT * FROM messages WHERE conversation_id=? "
                "ORDER BY sort_primary,sort_seq,sort_tie",
                (self.group,),
            ).fetchall()

    def release(self):
        preview = self.store.stock_preview(self.group)
        return self.store.release_stock(self.group, plan=preview["plan"])

    def test_default_idle_opens_no_source_bodies_and_does_not_refill(self):
        def forbidden(*args, **kwargs):
            self.fail("idle on-demand opened a body read")

        self.service.provider.read_recent = forbidden
        self.service.provider.read_range = forbidden
        for _ in range(2):
            self.service.sync_source_once(conversation_limit=100)
        self.assertEqual(self.rows(), [])

    def test_foreground_cache_repeat_and_idempotent_actual_bytes(self):
        first = self.read()
        before = self.store.resident_bytes(self.group)
        self.assertGreater(before, 0)
        original = self.service.provider.read_recent

        def forbidden(*args, **kwargs):
            self.fail("canonical repeat should be local")

        self.service.provider.read_recent = forbidden
        second = self.read()
        self.assertEqual(
            [(m["message_id"], m.get("text")) for m in first["messages"]],
            [(m["message_id"], m.get("text")) for m in second["messages"]],
        )
        self.assertEqual(before, self.store.resident_bytes(self.group))

        self.service.provider.read_recent = original
        self.read(refresh=True)
        self.assertEqual(before, self.store.resident_bytes(self.group))

    def test_search_scan_does_not_admit_nonmatching_on_demand_bodies(self):
        result = self.service.search_messages(
            query="synthetic-nonexistent-needle", conversation_ids=(self.group,)
        )
        self.assertEqual(result["hits"], [])
        self.assertEqual(result["source_receipt"]["search"]["preparation"]["message_count"], 0)
        self.assertEqual(self.rows(), [])
        with self.repository.database.connection() as connection:
            self.assertFalse(connection.execute("SELECT 1 FROM read_lease").fetchone())
        result = self.service.search_messages(query="保留", conversation_ids=(self.group,))
        self.assertEqual(len(result["hits"]), 1)
        self.assertEqual(len(self.rows()), 1)

    def test_sender_scope_does_not_admit_other_speakers(self):
        candidates = self.service.find_participants(
            conversation_id=self.group, query="demo_member_old"
        )
        selected = candidates["candidates"][0]["participant_id"]
        self.service.search_messages(
            query="",
            conversation_ids=(self.group,),
            participant_ids=(selected,),
            after="2010-01-01T00:00:00+00:00",
            before="2030-01-01T00:00:00+00:00",
        )
        self.assertGreater(len(self.rows()), 0)
        self.assertTrue(all(row["sender_id"] == selected for row in self.rows()))

    def test_backfill_is_keep_only_and_disabled_job_does_not_starve_selected_job(self):
        self.store.set(self.group, mode="recent")
        self.assertEqual(
            self.service.queue_backfill(conversation_id=self.group)["queued_job_count"], 0
        )
        self.store.set(self.group, mode="keep")
        first = self.service.queue_backfill(conversation_id=self.group)["job_ids"][0]
        self.store.set(self.group, mode="on_demand")
        other = next(
            item["conversation_id"]
            for item in self.tools.wechat_find_conversations("")["candidates"]
            if item["conversation_id"] != self.group
        )
        self.store.set(other, mode="keep")
        selected = self.service.queue_backfill(conversation_id=other)["job_ids"][0]
        self.service.process_backfill_once(batch_limit=10)
        with self.repository.database.connection() as connection:
            old = connection.execute(
                "SELECT processed_messages FROM source_backfill_jobs WHERE job_id=?", (first,)
            ).fetchone()[0]
            new = connection.execute(
                "SELECT processed_messages FROM source_backfill_jobs WHERE job_id=?", (selected,)
            ).fetchone()[0]
        self.assertEqual(old, 0)
        self.assertGreater(new, 0)

    def test_expiry_releases_all_body_copies_preserves_episode_and_identity(self):
        self.read()
        before = self.rows()
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE message_body_residency SET expires_at=?",
                ((datetime.now(UTC) - timedelta(days=2)).isoformat(),),
            )
        result = self.store.reclaim_expired()
        self.assertGreater(result["freed_bytes"], 0)
        after = self.rows()
        self.assertEqual(
            [row["message_id"] for row in before], [row["message_id"] for row in after]
        )
        self.assertEqual(
            [row["current_observation_seq"] for row in before],
            [row["current_observation_seq"] for row in after],
        )
        self.assertTrue(all(not row["body_available"] for row in after))
        with self.repository.database.connection() as connection:
            for row in connection.execute("SELECT parsed_json FROM message_observations"):
                self.assertEqual(observation_payload_state(row[0]), "released")
        self.service.sync_source_once(conversation_limit=100)
        self.assertTrue(all(not row["body_available"] for row in self.rows()))
        self.read(refresh=True)
        self.assertEqual(
            [row["current_observation_seq"] for row in before],
            [row["current_observation_seq"] for row in self.rows()],
        )
        self.assertGreater(self.store.resident_bytes(self.group), 0)

    def test_mode_change_and_re_read_never_demote_keep_stock(self):
        self.store.set(self.group, mode="keep")
        self.read()
        self.store.set(self.group, mode="on_demand")
        self.read(refresh=True)
        with self.repository.database.connection() as connection:
            owners = {
                row[0] for row in connection.execute("SELECT owner FROM message_body_residency")
            }
        self.assertEqual(owners, {"keep"})
        self.store.reclaim_expired(now=datetime.now(UTC) + timedelta(days=365))
        self.assertTrue(all(row["body_available"] for row in self.rows()))

    def test_release_is_exact_plan_and_preserves_observed_coverage(self):
        self.read()
        state = self.repository.source_conversation_state(self.group)
        plan = self.store.stock_preview(self.group)["plan"]
        with self.assertRaises(ValueError):
            self.store.release_stock(self.group, plan=plan | {"conversation_id": "wrong"})
        released = self.store.release_stock(self.group, plan=plan)
        self.assertGreater(released["released_messages"], 0)
        self.assertEqual(state, self.repository.source_conversation_state(self.group))
        with self.assertRaises(ValueError):
            self.store.release_stock(self.group, plan=plan)

    def test_release_preview_stales_on_changed_episode(self):
        self.read()
        plan = self.store.stock_preview(self.group)["plan"]
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET current_observation_seq=current_observation_seq+1 "
                "WHERE message_id=?",
                (plan["entries"][0]["message_id"],),
            )
        with self.assertRaisesRegex(ValueError, "episode changed"):
            self.store.release_stock(self.group, plan=plan)
        self.assertTrue(all(row["body_available"] for row in self.rows()))

    def test_batch_is_atomic_and_listing_covers_implicit_conversations(self):
        with self.assertRaises(KeyError):
            self.store.batch_set((self.group, "missing"), mode="keep")
        self.assertIsNone(self.store.get(self.group))
        seen, cursor = [], None
        while True:
            rows, cursor = self.store.list(limit=1, cursor=cursor)
            seen.extend(row["conversation_id"] for row in rows)
            if cursor is None:
                break
        with self.repository.database.connection() as connection:
            expected = {
                row[0] for row in connection.execute("SELECT conversation_id FROM conversations")
            }
        self.assertEqual(set(seen), expected)
        self.assertEqual(len(seen), len(set(seen)))

    def test_global_and_conversation_cap_reject_oversized_page_atomically(self):
        self.store.set_settings(ResidencySettings(lease_max_bytes=1, global_max_bytes=1))
        with self.assertRaises(SightglassError) as caught:
            self.read()
        self.assertEqual(caught.exception.code, ErrorCode.STORAGE_PRESSURE)
        self.assertEqual(self.rows(), [])

    def test_recent_cap_rotates_to_newest_using_actual_bytes(self):
        self.store.set_settings(
            ResidencySettings(default_mode="recent", recent_max_bytes=1800, recent_window_days=3650)
        )
        self.service.sync_source_once(conversation_limit=100, initial_tail=100)
        rows = self.rows()
        self.assertTrue(rows)
        retained = [row for row in rows if row["body_available"]]
        self.assertTrue(retained)
        self.assertEqual(retained[-1]["message_id"], rows[-1]["message_id"])
        self.assertLessEqual(self.store.resident_bytes(self.group), 1800)
        before = self.store.resident_bytes(self.group)
        self.service.sync_source_once(conversation_limit=100)
        self.assertEqual(self.store.resident_bytes(self.group), before)

    def test_operator_apply_requires_exact_preview_and_rebaseline_keeps_traversal(self):
        self.read()
        with self.assertRaises(ValueError):
            control.residency_release(
                self.repository.database, conversation_id=self.group, apply=True
            )
        plan = control.residency_release(self.repository.database, conversation_id=self.group)[
            "plan"
        ]
        self.assertTrue(
            control.residency_release(
                self.repository.database, conversation_id=self.group, apply=True, plan=plan
            )["applied"]
        )
        state = self.repository.source_conversation_state(self.group)
        control.residency_rebaseline(self.repository.database, conversation_id=self.group)
        self.assertEqual(state, self.repository.source_conversation_state(self.group))

    def test_settings_reject_coercion_and_unbounded_cache(self):
        for value in (True, "60", 60.5, None, 0):
            with self.assertRaises(ValueError):
                ResidencySettings(lease_max_bytes=value)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            control.residency_configure(self.repository.database, settings={"unknown": 1})
        self.assertEqual(json.dumps(self.store.settings().as_dict()) != "", True)

    def test_many_observation_release_continues_after_repository_restart(self):
        self.read()
        target = self.rows()[0]
        with self.repository.database.transaction() as connection:
            for number in range(600):
                connection.execute(
                    "INSERT INTO message_observations(observation_id,message_id,observed_at,"
                    "source_generation_id,state,payload_digest,parsed_json,parser_version) "
                    "SELECT ?,message_id,observed_at,source_generation_id,state,payload_digest,"
                    "parsed_json,parser_version FROM message_observations WHERE observation_seq=?",
                    (f"synthetic-release-episode-{number}", target["current_observation_seq"]),
                )
        approved = self.release()
        self.assertGreater(approved["pending_messages"], 0)
        with self.repository.database.connection() as connection:
            current = connection.execute(
                "SELECT parsed_json FROM message_observations WHERE observation_seq=?",
                (target["current_observation_seq"],),
            ).fetchone()[0]
            self.assertEqual(observation_payload_state(current), "full")
        # The current payload remains physically intact for the bounded job, but
        # ordinary candidate reads cannot continue serving a released body.
        self.assertEqual(self.repository.frozen_message_rows((target["message_id"],)), [])
        restarted = ResidencyRepository(self.repository.database)
        result = restarted.reclaim_expired()
        self.assertGreater(result["released_messages"], 0)
        self.assertTrue(all(not row["body_available"] for row in self.rows()))
        with self.repository.database.connection() as connection:
            self.assertFalse(connection.execute("SELECT 1 FROM body_release_jobs").fetchone())

    def test_exact_pending_replay_and_resource_voice_dependencies_survive_release(self):
        self.read()
        rows = self.rows()
        pinned = rows[0]
        moment = datetime.now(UTC).isoformat()
        self.repository.create_pending_delivery(
            delivery_id="synthetic-residency-delivery",
            reader_id=self.service.reader.reader_id,
            conversation_id=self.group,
            scope_kind="conversation",
            scope_key=self.group,
            from_observation_seq=pinned["current_observation_seq"] - 1,
            to_observation_seq=pinned["current_observation_seq"],
            payload_digest="synthetic-digest",
            payload_ref="synthetic-immutable-spool",
            projection_schema_version="synthetic",
            created_at=moment,
        )
        with self.repository.database.connection() as connection:
            resources = connection.execute(
                "SELECT * FROM resources WHERE message_id IN "
                "(SELECT message_id FROM messages WHERE conversation_id=?) LIMIT 2",
                (self.group,),
            ).fetchall()
        self.assertEqual(len(resources), 2)
        first, second = resources
        self.repository.insert_resource_job(
            job_id="synthetic-residency-resource-job",
            resource_id=first["resource_id"],
            resource_revision="synthetic-revision",
            recipe_digest="synthetic-recipe",
            recipe_json="{}",
            created_at=moment,
        )
        from sightglass.voice.repository import VoiceRepository

        VoiceRepository(self.repository.database).insert_job(
            {
                "job_id": "synthetic-residency-voice-job",
                "account_id": rows[0]["account_id"],
                "resource_id": second["resource_id"],
                "resource_revision": "synthetic-revision",
                "recipe_digest": "synthetic-voice-recipe",
                "recipe_json": "{}",
                "created_at": moment,
                "updated_at": moment,
            }
        )
        approved = self.release()
        self.assertGreater(approved["pinned_messages"], 0)
        wanted = {pinned["message_id"], first["message_id"], second["message_id"]}
        retained = {row["message_id"] for row in self.rows() if row["body_available"]}
        self.assertEqual(retained, wanted)
        delivery = self.repository.delivery("synthetic-residency-delivery")
        assert delivery is not None
        self.assertEqual(delivery["payload_ref"], "synthetic-immutable-spool")
        self.assertEqual(delivery["status"], "pending")
        self.assertEqual(
            self.store.reclaim_expired(now=datetime.now(UTC) + timedelta(days=100))[
                "released_messages"
            ],
            0,
        )


class _ResidencyCapHarness(unittest.TestCase):
    """Shared fixture/helpers for the bounded cap reclaim regressions."""

    ROW_COUNT = 520

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name) / "source"
        create_synthetic_source(
            root, include_second_shard=False, declare_second_shard=False
        )
        (root / "messages-1.db").unlink()
        _create_shard(
            root / "messages-1.db",
            [
                _row(
                    f"synthetic-cap-{index:04d}",
                    "conv_group",
                    (datetime(2026, 9, 1, tzinfo=UTC) + timedelta(seconds=index)).isoformat(),
                    index,
                    index + 1,
                    1,
                    "wxid_demo_member:\nsynthetic cache payload "
                    + f"{index:04d} "
                    + "x" * 100,
                    sender="wxid_demo_member",
                    shown_as="Synthetic member",
                )
                for index in range(self.ROW_COUNT)
            ],
        )
        _, self.repository, self.service, self.tools = build_test_stack(
            root, Path(self.temp.name) / "state" / "window.db", residency_default="keep"
        )
        self.assertTrue(self.tools.wechat_status()["ready"])
        self.group = self.tools.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]
        self.store = ResidencyRepository(self.repository.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def rows(self):
        with self.repository.database.connection() as connection:
            return connection.execute(
                "SELECT * FROM messages WHERE conversation_id=? "
                "ORDER BY sort_primary,sort_seq,sort_tie",
                (self.group,),
            ).fetchall()

    def _admit_all_bodies(self) -> list[str]:
        """Admit the whole conversation, then return resident ids in read order."""

        self.service.sync_source_once(conversation_limit=100, initial_tail=500)
        self.service.queue_backfill(conversation_id=self.group)
        for _ in range((self.ROW_COUNT + 499) // 500 + 1):
            self.service.process_backfill_once(batch_limit=500)
        with self.repository.database.connection() as connection:
            return [
                row[0]
                for row in connection.execute(
                    "SELECT message_id FROM messages WHERE conversation_id=? "
                    "AND body_available=1 ORDER BY sort_primary,sort_seq,sort_tie",
                    (self.group,),
                ).fetchall()
            ]

    def _release_all(self) -> None:
        while True:
            preview = self.store.stock_preview(self.group, limit=500)
            if not preview["examined_messages"]:
                break
            self.store.release_stock(self.group, plan=preview["plan"], limit=500)

class ResidencyCapProgressTests(_ResidencyCapHarness):
    """Bounded cap reclaim must reach real bodies behind retained/released prefixes."""

    ROW_COUNT = 520

    def test_cap_reclaim_skips_released_zero_byte_prefix(self):
        # Reproduce the reported bug: an already released durable prefix owns the
        # bounded candidate pool while the only evictable body sorts behind it.
        admitted = self._admit_all_bodies()
        self.assertEqual(len(admitted), 520)
        self._release_all()
        self.store.set(self.group, mode="on_demand")
        self.service.read_messages(
            mode="message", message_id=admitted[-1], projection="detail", limit=1
        )
        with self.repository.database.connection() as connection:
            prefix = connection.execute(
                "SELECT b.byte_size,m.body_available FROM message_body_residency b "
                "JOIN messages m USING(message_id) WHERE b.conversation_id=? "
                "ORDER BY m.sort_primary,m.sort_seq,m.sort_tie,b.message_id LIMIT 500",
                (self.group,),
            ).fetchall()
            retained = [
                row[0]
                for row in connection.execute(
                    "SELECT message_id FROM messages WHERE conversation_id=? "
                    "AND body_available=1",
                    (self.group,),
                ).fetchall()
            ]
        # The first 500 candidate rows are all durable zero-byte bookkeeping.
        self.assertEqual(len(prefix), 500)
        self.assertTrue(all(row[0] == 0 and not row[1] for row in prefix))
        self.assertEqual(retained, [admitted[-1]])
        cap = self.store.resident_bytes(self.group) + 32
        self.store.set_settings(
            ResidencySettings(lease_max_bytes=cap, global_max_bytes=cap * 10)
        )
        before = self.store.resident_bytes(self.group)
        # A second body read must succeed by evicting the older cached body even
        # though 500 zero-byte rows sort ahead of it in the reclaim order.
        body = self.service.read_messages(
            mode="message", message_id=admitted[-2], projection="detail", limit=1
        )
        self.assertEqual(len(body["messages"]), 1)
        self.assertLessEqual(self.store.resident_bytes(self.group), cap)
        self.assertNotEqual(self.store.resident_bytes(self.group), before)
        with self.repository.database.connection() as connection:
            zero = connection.execute(
                "SELECT COUNT(*) FROM message_body_residency WHERE conversation_id=? "
                "AND byte_size=0",
                (self.group,),
            ).fetchone()[0]
            bookkeeping = connection.execute(
                "SELECT COUNT(*) FROM message_body_residency WHERE conversation_id=?",
                (self.group,),
            ).fetchone()[0]
        # Released durable bookkeeping is retained, never deleted to make room:
        # 520 rows survive and exactly one (the current body) holds bytes.
        self.assertEqual(bookkeeping, 520)
        self.assertEqual(zero, 519)

    def test_cap_reclaim_progresses_past_protected_prefix(self):
        ids = self._admit_all_bodies()
        self.assertGreaterEqual(len(ids), 500)
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE message_body_residency SET owner='on_demand',expires_at=NULL "
                "WHERE conversation_id=? AND byte_size>0",
                (self.group,),
            )
        # The cap is only satisfiable by evicting rows past a protected prefix
        # longer than the bounded batch, so a single-page scan would starve. The
        # cap sits below the normalized resident total (the duplicate current-body
        # copies are no longer stored) but above the protected 470-row prefix.
        self.store.set_settings(
            ResidencySettings(lease_max_bytes=290_000, global_max_bytes=290_000)
        )
        before = self.store.resident_bytes(self.group)
        self.store.enforce_caps(
            self.store.resolve_decision(self.group, authorized=True),
            protect=tuple(ids[:470]),
            limit=500,
        )
        after = self.store.resident_bytes(self.group)
        self.assertLess(after, before)
        self.assertLessEqual(after, 290_000)
        with self.repository.database.connection() as connection:
            survivors = {
                row[0]
                for row in connection.execute(
                    "SELECT message_id FROM message_body_residency WHERE "
                    "conversation_id=? AND byte_size>0",
                    (self.group,),
                ).fetchall()
            }
        # The protected prefix is untouched while past-prefix bodies were evicted.
        self.assertTrue(set(ids[:470]).issubset(survivors))

    def test_cap_reclaim_failure_rolls_back_and_retry_succeeds(self):
        ids = self._admit_all_bodies()
        self.assertGreaterEqual(len(ids), 500)
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE message_body_residency SET owner='on_demand',expires_at=NULL "
                "WHERE conversation_id=? AND byte_size>0",
                (self.group,),
            )
        before_rows = self.rows()
        before_state = self.repository.source_conversation_state(self.group)
        revision = self.store.revision()
        self.store.set_settings(
            ResidencySettings(lease_max_bytes=1_000, global_max_bytes=1_000)
        )
        # Protecting every releasable body makes the bound genuinely unsatisfiable;
        # the failure must roll back all partial eviction work.
        with self.assertRaises(SightglassError) as caught:
            self.store.enforce_caps(
                self.store.resolve_decision(self.group, authorized=True),
                protect=tuple(ids),
                limit=500,
            )
        self.assertEqual(caught.exception.code, ErrorCode.STORAGE_PRESSURE)
        self.assertEqual(self.store.revision(), revision)
        self.assertEqual(
            [row["message_id"] for row in self.rows()],
            [row["message_id"] for row in before_rows],
        )
        self.assertTrue(all(row["body_available"] for row in self.rows()))
        # A retry that can make progress succeeds and preserves identity/episode.
        self.store.set_settings(
            ResidencySettings(lease_max_bytes=409_000, global_max_bytes=409_000)
        )
        self.store.enforce_caps(
            self.store.resolve_decision(self.group, authorized=True),
            protect=(),
            limit=500,
        )
        self.assertLessEqual(self.store.resident_bytes(self.group), 409_000)
        after_rows = self.rows()
        self.assertEqual(
            [row["message_id"] for row in before_rows],
            [row["message_id"] for row in after_rows],
        )
        self.assertEqual(
            [row["current_observation_seq"] for row in before_rows],
            [row["current_observation_seq"] for row in after_rows],
        )
        self.assertEqual(self.repository.source_conversation_state(self.group), before_state)


class ResidencyCapStarvationTests(_ResidencyCapHarness):
    """A protected/pinned prefix longer than any single-scan budget must not starve.

    The default bounded reclaim candidate pool can hold at most one batch worth of
    rows per pass. If a protected or pinned prefix re-occupies that pool on every
    call, releasable bodies behind it are permanently unreachable. Eligibility is
    filtered in SQL, so the pool only ever contains genuinely reclaimable rows.
    """

    ROW_COUNT = 2_600

    def test_reclaim_reaches_bodies_behind_prefix_longer_than_scan_budget(self) -> None:
        ids = self._admit_all_bodies()
        self.assertGreaterEqual(len(ids), 2_500)
        with self.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE message_body_residency SET owner='on_demand',expires_at=NULL "
                "WHERE conversation_id=? AND byte_size>0",
                (self.group,),
            )
            seqs = {
                row[0]: row[1]
                for row in connection.execute(
                    "SELECT message_id,current_observation_seq FROM messages "
                    "WHERE conversation_id=?",
                    (self.group,),
                ).fetchall()
            }
        # Protect far more rows than limit*4; a single fixed scan window would never
        # reach the releasable tail. Pin a separate interior slice through a real
        # pending delivery so both the SQL eligibility filter and ``_pinned`` see it.
        protected = ids[:2_400]
        pinned_slice = ids[2_400:2_450]
        pinned_set = set(pinned_slice)
        self.repository.create_pending_delivery(
            delivery_id="synthetic-starvation-delivery",
            reader_id=self.service.reader.reader_id,
            conversation_id=self.group,
            scope_kind="conversation",
            scope_key=self.group,
            from_observation_seq=min(seqs[item] for item in pinned_slice) - 1,
            to_observation_seq=max(seqs[item] for item in pinned_slice),
            payload_digest="synthetic-digest",
            payload_ref="synthetic-immutable-spool",
            projection_schema_version="synthetic",
            created_at=datetime.now(UTC).isoformat(),
        )
        # A cap above the protected+pinned floor but below the full resident total,
        # so it is only satisfiable by evicting part of the eligible tail.
        with self.repository.database.connection() as connection:
            floor = connection.execute(
                "SELECT COALESCE(SUM(b.byte_size),0) FROM message_body_residency b "
                "WHERE b.conversation_id=? AND b.message_id IN ("
                + ",".join("?" for _ in (*protected, *pinned_slice))
                + ")",
                (self.group, *protected, *pinned_slice),
            ).fetchone()[0]
        before = self.store.resident_bytes(self.group)
        self.assertGreater(before, floor)
        cap = floor + (before - floor) // 2
        self.store.set_settings(
            ResidencySettings(lease_max_bytes=cap, global_max_bytes=cap)
        )
        self.store.enforce_caps(
            self.store.resolve_decision(self.group, authorized=True),
            protect=tuple(protected),
            limit=500,
        )
        after = self.store.resident_bytes(self.group)
        self.assertLess(after, before)
        self.assertLessEqual(after, cap)
        with self.repository.database.connection() as connection:
            survivors = {
                row[0]
                for row in connection.execute(
                    "SELECT message_id FROM message_body_residency WHERE "
                    "conversation_id=? AND byte_size>0",
                    (self.group,),
                ).fetchall()
            }
        # Both the protected prefix and the pinned slice are preserved.
        self.assertTrue(set(protected).issubset(survivors))
        self.assertTrue(pinned_set.issubset(survivors))


if __name__ == "__main__":
    unittest.main()

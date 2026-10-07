from __future__ import annotations

import json
import os
import stat
import tempfile
import threading
import unittest
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from sightglass.model.db import WindowDB
from sightglass.runtime.config import ConfigStore, SightglassConfig
from sightglass.runtime.daemon import SightglassDaemon
from sightglass.runtime.secrets import (
    OPERATOR_SECRET_ACCOUNT,
    READER_SECRET_ACCOUNT,
    MemorySecretStore,
    token_hash,
)
from sightglass.runtime.storage_history import (
    STORAGE_HISTORY_FILENAME,
    STORAGE_HISTORY_SCHEMA,
    DailySnapshot,
    StorageHistory,
    StorageHistoryError,
    capture_daily_snapshot,
    next_utc_midnight_delay,
)
from sightglass.source.synthetic import create_synthetic_source
from sightglass.storage import MIB, StorageBudget, StorageSettings

COMPONENT_KEYS = (
    "database",
    "database_sidecars",
    "migration_backups",
    "delivery_spool",
    "resource_objects_and_tmp",
    "staging",
    "other_owned_files",
)


def make_snapshot(
    day: str,
    *,
    database: int,
    resource: int = 0,
    other: int = 0,
    messages: int = 0,
    observations: int = 0,
) -> DailySnapshot:
    components: dict[str, int] = dict.fromkeys(COMPONENT_KEYS, 0)
    components["database"] = database
    components["resource_objects_and_tmp"] = resource
    components["other_owned_files"] = other
    return DailySnapshot(
        day=day,
        captured_at=f"{day}T00:00:00+00:00",
        accounted_bytes=sum(components.values()),
        components=components,
        message_count=messages,
        observation_count=observations,
    )


def day_at(day: str) -> datetime:
    return datetime.fromisoformat(f"{day}T12:00:00+00:00")


class StorageHistorySidecarTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.settings = StorageSettings(4 * MIB, 8 * MIB, 0, 2 * MIB)
        self.free = patch("sightglass.storage._free", return_value=1024 * MIB)
        self.free.start()
        self.addCleanup(self.free.stop)
        self.addCleanup(self.temporary.cleanup)
        self.budget = StorageBudget(self.root, self.root / "window.db", self.settings)
        self.history = StorageHistory(self.root, self.budget)

    def test_sidecar_is_private_regular_atomic_and_budget_tracked(self) -> None:
        self.history.record(make_snapshot("2026-09-01", database=60))
        self.assertTrue(self.history.path.is_file())
        self.assertFalse(self.history.path.is_symlink())
        self.assertEqual(stat.S_IMODE(self.history.path.stat().st_mode), 0o600)
        self.assertEqual(
            [item.name for item in self.root.iterdir()],
            [STORAGE_HISTORY_FILENAME],
        )
        accounted = self.budget.status(reconcile=True)["accounted_bytes"]
        metadata = self.history.path.stat()
        self.assertEqual(accounted, max(metadata.st_size, metadata.st_blocks * 512))
        explained = self.budget.explain(limit=10)
        roles = {item["relative_path"]: item["recognized_role"] for item in explained["files"]}
        self.assertEqual(roles[STORAGE_HISTORY_FILENAME], "storage_history")

    def test_record_replaces_same_day_and_bounds_retention(self) -> None:
        for index in range(70):
            day = (datetime(2026, 1, 1, tzinfo=UTC).toordinal() + index)
            name = datetime.fromordinal(day).date().isoformat()
            self.history.record(make_snapshot(name, database=index))
        self.history.record(make_snapshot("2026-03-11", database=999))
        block = self.history.history(limits=self.settings.as_dict(), now=day_at("2026-03-11"))
        self.assertTrue(block["available"])
        self.assertEqual(block["day_count"], 64)
        self.assertEqual(block["retention_days"], 64)
        self.assertEqual(block["captured_days"][-1], "2026-03-11")
        self.assertEqual(len(block["captured_days"]), 64)
        # 2026-03-11 was already recorded; a second record replaces, never duplicates.
        raw = json.loads(self.history.path.read_text(encoding="utf-8"))
        days = [entry["day"] for entry in raw["days"]]
        self.assertEqual(len(days), len(set(days)))
        self.assertEqual(days[-1], "2026-03-11")
        self.assertEqual(raw["days"][-1]["accounted_bytes"], 999)

    def test_exact_7_and_30_day_deltas_and_missing_baseline(self) -> None:
        self.history.record(
            make_snapshot("2026-09-01", database=800, resource=100, other=50,
                          messages=10, observations=20)
        )
        self.history.record(
            make_snapshot("2026-09-08", database=1_200, resource=300, other=80,
                          messages=14, observations=26)
        )
        self.history.record(
            make_snapshot("2026-09-28", database=2_400, resource=1_200, other=1_400,
                          messages=30, observations=60)
        )
        block = self.history.history(limits=self.settings.as_dict(), now=day_at("2026-09-28"))
        self.assertTrue(block["available"])
        self.assertTrue(block["as_of_is_current_day"])
        seven = block["windows"]["7d"]
        self.assertFalse(seven["available"])
        self.assertEqual(seven["baseline_day"], "2026-09-21")
        # The 7-day baseline day is absent, so the 8-day-old row must not stand in.
        self.assertEqual(seven["reason"], "baseline_day_absent")

        self.history.record(
            make_snapshot("2026-09-21", database=1_000, resource=500, other=500,
                          messages=10, observations=20)
        )
        block = self.history.history(
            limits={"soft_limit_bytes": 8_000, "hard_limit_bytes": 11_000},
            now=day_at("2026-09-28"),
        )
        seven = block["windows"]["7d"]
        self.assertTrue(seven["available"])
        self.assertEqual(seven["interval_days"], 7)
        self.assertEqual(
            seven["deltas"],
            {
                "accounted_bytes": 3_000,
                "database_bytes": 1_400,
                "other_owned_bytes": 900,
                "resource_bytes": 700,
                "message_count": 20,
                "observation_count": 40,
            },
        )
        self.assertEqual(seven["bytes_per_new_message"], 150.0)
        self.assertEqual(seven["average_accounted_bytes_per_day"], 428.57)
        self.assertTrue(seven["growing"])
        self.assertEqual(seven["estimated_days_to_soft_limit"], 7.0)
        self.assertEqual(seven["estimated_days_to_hard_limit"], 14.0)
        self.assertFalse(block["windows"]["30d"]["available"])
        self.assertEqual(block["windows"]["30d"]["reason"], "baseline_day_absent")
        self.assertEqual(block["windows"]["30d"]["baseline_day"], "2026-08-29")

    def test_declining_growth_omits_limit_estimates(self) -> None:
        self.history.record(
            make_snapshot("2026-09-21", database=2_000, messages=30)
        )
        self.history.record(
            make_snapshot("2026-09-28", database=1_000, messages=30)
        )
        block = self.history.history(
            limits={"soft_limit_bytes": 10_000, "hard_limit_bytes": 20_000},
            now=day_at("2026-09-28"),
        )
        seven = block["windows"]["7d"]
        self.assertFalse(seven["growing"])
        self.assertIsNone(seven["bytes_per_new_message"])
        self.assertIsNone(seven["estimated_days_to_soft_limit"])
        self.assertIsNone(seven["estimated_days_to_hard_limit"])

    def test_corrupt_symlink_and_insecure_permissions_fail_closed(self) -> None:
        self.history.path.write_bytes(b"{not valid json")
        os.chmod(self.history.path, 0o600)
        before = self.history.path.read_bytes()
        with self.assertRaises(StorageHistoryError) as caught:
            self.history.record(make_snapshot("2026-09-28", database=1))
        self.assertEqual(caught.exception.reason, "history_corrupt")
        self.assertEqual(self.history.path.read_bytes(), before)
        blocked = self.history.history(now=day_at("2026-09-28"))
        self.assertFalse(blocked["available"])
        self.assertEqual(blocked["reason"], "history_corrupt")
        self.assertFalse(blocked["windows"]["7d"]["available"])

    def test_symlink_target_is_never_read_or_replaced(self) -> None:
        target = self.root / "target.json"
        target.write_text('{"secret": "synthetic-must-not-change"}', encoding="utf-8")
        self.history.path.symlink_to(target)
        with self.assertRaises(StorageHistoryError) as caught:
            self.history.record(make_snapshot("2026-09-28", database=1))
        self.assertEqual(caught.exception.reason, "history_symlink")
        self.assertTrue(self.history.path.is_symlink())
        self.assertEqual(
            target.read_text(encoding="utf-8"), '{"secret": "synthetic-must-not-change"}'
        )
        self.assertFalse(self.history.status()["available"])

    def test_insecure_permissions_fail_closed(self) -> None:
        self.history.record(make_snapshot("2026-09-28", database=1))
        os.chmod(self.history.path, 0o644)
        block = self.history.history(now=day_at("2026-09-28"))
        self.assertFalse(block["available"])
        self.assertEqual(block["reason"], "history_insecure_permissions")
        self.assertEqual(self.history.status()["reason"], "history_insecure_permissions")

    def _write_valid_then_mutate(self, mutate) -> bytes:
        self.history.record(make_snapshot("2026-09-28", database=1))
        payload = json.loads(self.history.path.read_text(encoding="utf-8"))
        mutate(payload)
        raw = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.history.path.write_bytes(raw)
        os.chmod(self.history.path, 0o600)
        return raw

    def test_hardlinked_sidecar_fails_closed_and_is_preserved(self) -> None:
        self.history.record(make_snapshot("2026-09-28", database=1))
        alias = self.root / "storage-history-alias.json"
        os.link(self.history.path, alias)
        before = self.history.path.read_bytes()
        block = self.history.history(now=day_at("2026-09-28"))
        self.assertFalse(block["available"])
        self.assertEqual(block["reason"], "history_hardlinked")
        with self.assertRaises(StorageHistoryError) as caught:
            self.history.record(make_snapshot("2026-09-28", database=2))
        self.assertEqual(caught.exception.reason, "history_hardlinked")
        # Both links keep their original bytes: the shared inode is never rewritten.
        self.assertEqual(self.history.path.read_bytes(), before)
        self.assertEqual(alias.read_bytes(), before)

    def test_unexpected_top_level_entry_and_component_keys_fail_closed(self) -> None:
        for mutate in (
            lambda payload: payload.__setitem__("extra", "synthetic"),
            lambda payload: payload["days"][0].__setitem__("extra", "synthetic"),
            lambda payload: payload["days"][0]["components"].__setitem__(
                "extra_component", 1
            ),
        ):
            with self.subTest(mutate=mutate):
                self.history.path.unlink(missing_ok=True)
                before = self._write_valid_then_mutate(mutate)
                block = self.history.history(now=day_at("2026-09-28"))
                self.assertFalse(block["available"])
                self.assertEqual(block["reason"], "history_corrupt")
                self.assertEqual(self.history.path.read_bytes(), before)

    def test_component_total_mismatch_and_bad_captured_at_fail_closed(self) -> None:
        def bump_accounted(payload) -> None:
            payload["days"][0]["accounted_bytes"] += 1

        def naive_captured_at(payload) -> None:
            payload["days"][0]["captured_at"] = "2026-09-28T00:00:00"

        def other_day_captured_at(payload) -> None:
            payload["days"][0]["captured_at"] = "2026-09-27T00:00:00+00:00"

        for mutate in (bump_accounted, naive_captured_at, other_day_captured_at):
            with self.subTest(mutate=mutate):
                self.history.path.unlink(missing_ok=True)
                before = self._write_valid_then_mutate(mutate)
                block = self.history.history(now=day_at("2026-09-28"))
                self.assertFalse(block["available"])
                self.assertEqual(block["reason"], "history_corrupt")
                self.assertEqual(self.history.path.read_bytes(), before)

    def test_duplicate_day_entries_are_rejected_not_last_wins(self) -> None:
        def duplicate_day(payload) -> None:
            payload["days"].append(dict(payload["days"][0]))

        before = self._write_valid_then_mutate(duplicate_day)
        block = self.history.history(now=day_at("2026-09-28"))
        self.assertFalse(block["available"])
        self.assertEqual(block["reason"], "history_corrupt")
        with self.assertRaises(StorageHistoryError) as caught:
            self.history.record(make_snapshot("2026-09-28", database=2))
        self.assertEqual(caught.exception.reason, "history_corrupt")
        self.assertEqual(self.history.path.read_bytes(), before)

    def test_direct_snapshot_construction_rejects_invalid_fields(self) -> None:
        zero_components: dict[str, int] = dict.fromkeys(COMPONENT_KEYS, 0)
        base = {
            "day": "2026-09-28",
            "captured_at": "2026-09-28T00:00:00+00:00",
            "accounted_bytes": 0,
            "components": zero_components,
            "message_count": 0,
            "observation_count": 0,
        }
        for field in ("accounted_bytes", "message_count", "observation_count"):
            for bad in (-1, True, "1", 1.5, None):
                with self.subTest(field=field, bad=bad):
                    with self.assertRaises(ValueError):
                        DailySnapshot(**{**base, field: bad})
        for component in COMPONENT_KEYS:
            components = dict.fromkeys(COMPONENT_KEYS, 0)
            components[component] = -1
            with self.subTest(component=component):
                with self.assertRaises(ValueError):
                    DailySnapshot(**{**base, "components": components})

    def test_history_output_contains_no_content_or_paths(self) -> None:
        self.history.record(
            make_snapshot("2026-09-21", database=1, messages=1, observations=2)
        )
        self.history.record(
            make_snapshot("2026-09-28", database=4, messages=3, observations=6)
        )
        block = self.history.history(limits=self.settings.as_dict(), now=day_at("2026-09-28"))
        rendered = json.dumps(block, ensure_ascii=False)
        self.assertNotIn(str(self.root), rendered)
        for forbidden in ("body", "label", "filename", "credential"):
            self.assertNotIn(forbidden, rendered.lower())
        self.assertEqual(block["schema"], STORAGE_HISTORY_SCHEMA)

    def test_write_failure_is_closed_and_observable(self) -> None:
        self.budget.settings = StorageSettings(1, 2, MIB, MIB)
        with patch("sightglass.storage._free", return_value=1):
            with self.assertRaises(StorageHistoryError) as caught:
                self.history.record(make_snapshot("2026-09-28", database=1))
        self.assertEqual(caught.exception.reason, "history_write_failed")
        self.assertFalse(self.history.path.exists())

    def test_reserve_is_used_from_the_maintenance_allowance(self) -> None:
        with patch.object(self.budget, "reserve", wraps=self.budget.reserve) as reserve:
            self.history.record(make_snapshot("2026-09-28", database=1))
        self.assertTrue(reserve.call_args.kwargs["maintenance"])

    def test_capture_snapshot_reads_budget_and_exact_counts(self) -> None:
        database = WindowDB(self.root / "window.db", storage=self.budget)
        snapshot = capture_daily_snapshot(database, now=day_at("2026-09-28"))
        self.assertEqual(snapshot.day, "2026-09-28")
        self.assertEqual(snapshot.message_count, 0)
        self.assertEqual(snapshot.observation_count, 0)
        self.assertEqual(set(snapshot.components), set(COMPONENT_KEYS))
        self.assertGreater(snapshot.components["database"], 0)

    def test_next_utc_midnight_delay_is_positive_and_bounded(self) -> None:
        delay = next_utc_midnight_delay(datetime(2026, 9, 28, 23, 59, 0, tzinfo=UTC))
        self.assertGreaterEqual(delay, 1.0)
        self.assertLessEqual(delay, 24 * 60 * 60)


class StorageHistoryDaemonLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.addCleanup(self.temporary.cleanup)
        self.source_root = self.root / "source"
        create_synthetic_source(self.source_root)
        self.reader_token = "synthetic-reader-token"
        self.operator_token = "synthetic-operator-token"
        self.config = replace(
            SightglassConfig.create(self.root / "state", self.source_root),
            reader_token_hash=token_hash(self.reader_token),
            operator_token_hash=token_hash(self.operator_token),
        )
        self.store = ConfigStore(self.config.data_dir / "config.json")
        self.store.save(self.config)
        self.secrets = MemorySecretStore(
            {
                READER_SECRET_ACCOUNT: self.reader_token,
                OPERATOR_SECRET_ACCOUNT: self.operator_token,
            }
        )
        self.daemon = SightglassDaemon(config_store=self.store, secret_store=self.secrets)
        self.addCleanup(self.daemon.tools.close)
        self.addCleanup(self.daemon._stop_storage_history)

    def test_start_captures_history_stop_completes_and_explain_is_operator_only(self) -> None:
        path = self.config.data_dir / STORAGE_HISTORY_FILENAME
        self.assertFalse(path.exists())
        self.daemon._start_storage_history()
        self.assertTrue(path.is_file())
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertTrue(self.daemon.storage_history_state["available"])

        # A second start on the same UTC day replaces rather than duplicating.
        self.daemon._start_storage_history()
        raw = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(len(raw["days"]), 1)

        result = self.daemon._dispatch(
            "operator", "operator.storage.explain", {"offset": 0, "limit": 5, "sample_size": 1}
        )
        self.assertEqual(result["schema"], "sightglass.storage-explain.v1")
        history = result["history"]
        self.assertEqual(history["schema"], STORAGE_HISTORY_SCHEMA)
        self.assertTrue(history["available"])
        self.assertIn("7d", history["windows"])
        self.assertIn("30d", history["windows"])
        rendered = json.dumps(result, ensure_ascii=False)
        self.assertNotIn(str(self.config.data_dir), rendered)

        with self.assertRaises(RuntimeError):
            self.daemon._dispatch("reader", "operator.storage.explain", {"offset": 0, "limit": 5})

        recorder = self.daemon._storage_history_recorder
        self.assertIsNotNone(recorder)
        assert recorder is not None
        thread = recorder.thread
        self.assertIsNotNone(thread)
        self.assertTrue(self.daemon._stop_storage_history())
        self.assertIsNone(self.daemon._storage_history_recorder)
        assert thread is not None
        self.assertFalse(thread.is_alive())
        self.assertTrue(self.daemon.status()["storage_history"]["available"])

    def test_operator_explain_reads_persisted_history_without_any_write(self) -> None:
        self.daemon._start_storage_history()
        path = self.config.data_dir / STORAGE_HISTORY_FILENAME
        persisted = path.read_bytes()
        with (
            patch.object(
                StorageHistory, "record", side_effect=AssertionError("explain must not write")
            ),
            patch.object(
                SightglassDaemon,
                "_capture_storage_history_day",
                side_effect=AssertionError("explain must not capture"),
            ),
        ):
            result = self.daemon._dispatch(
                "operator", "operator.storage.explain", {"offset": 0, "limit": 5, "sample_size": 0}
            )
        self.assertFalse(result["mutated"])
        self.assertEqual(result["history"]["schema"], STORAGE_HISTORY_SCHEMA)
        self.assertTrue(result["history"]["available"])
        self.assertEqual(path.read_bytes(), persisted)

    def test_history_failure_state_does_not_break_daemon_status(self) -> None:
        path = self.config.data_dir / STORAGE_HISTORY_FILENAME
        path.write_bytes(b"corrupt")
        os.chmod(path, 0o600)
        self.daemon._capture_storage_history_day()
        state = self.daemon.status()["storage_history"]
        self.assertFalse(state["available"])
        self.assertEqual(state["reason"], "history_corrupt")
        # The corrupt sidecar is preserved, never silently overwritten.
        self.assertEqual(path.read_bytes(), b"corrupt")

    def test_missing_tables_are_reported_as_capture_failure(self) -> None:
        with patch(
            "sightglass.runtime.daemon.capture_daily_snapshot",
            side_effect=RuntimeError("synthetic"),
        ):
            self.daemon._capture_storage_history_day()
        self.assertEqual(self.daemon.storage_history_state["reason"], "capture_failed")

    def test_write_failure_is_observable_through_content_free_state(self) -> None:
        history = self.daemon.storage_history
        assert history is not None
        with patch.object(
            history, "record", side_effect=StorageHistoryError("history_write_failed")
        ):
            self.daemon._capture_storage_history_day()
        state = self.daemon.storage_history_state
        self.assertFalse(state["available"])
        self.assertEqual(state["reason"], "history_write_failed")
        self.assertFalse((self.config.data_dir / STORAGE_HISTORY_FILENAME).exists())

    def test_timed_out_recorder_is_not_revived_or_duplicated_then_is_reaped(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        real_capture = capture_daily_snapshot

        def gated(database, *, now=None):
            if threading.current_thread().name == "sightglass-storage-history":
                entered.set()
                release.wait(timeout=5)
            return real_capture(database, now=now)

        self.addCleanup(release.set)
        with (
            patch("sightglass.runtime.daemon.capture_daily_snapshot", side_effect=gated),
            patch("sightglass.runtime.daemon.next_utc_midnight_delay", return_value=0.0),
            patch("sightglass.runtime.daemon.STORAGE_HISTORY_STOP_TIMEOUT_SECONDS", 0.05),
        ):
            self.daemon._start_storage_history()
            self.assertTrue(entered.wait(timeout=5))
            recorder = self.daemon._storage_history_recorder
            assert recorder is not None
            thread = recorder.thread
            assert thread is not None
            # The blocked capture outlives the join timeout: stop reports failure and
            # does not clear the generation, so a later start cannot revive it.
            self.assertFalse(self.daemon._stop_storage_history())
            self.assertIs(self.daemon._storage_history_recorder, recorder)
            with self.assertRaises(RuntimeError):
                self.daemon._start_storage_history()
            self.assertIs(self.daemon._storage_history_recorder, recorder)
            background = [
                item
                for item in threading.enumerate()
                if item.name == "sightglass-storage-history"
            ]
            self.assertEqual(len(background), 1)
            self.assertIs(background[0], thread)
            # Releasing the capture lets the single generation finish and be reaped.
            release.set()
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            self.assertTrue(self.daemon._stop_storage_history())
            self.assertIsNone(self.daemon._storage_history_recorder)
            self.assertFalse(
                any(item.name == "sightglass-storage-history" for item in threading.enumerate())
            )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from sightglass.model.db import WindowDB
from sightglass.model.storage_diagnostics import _query, _table_count
from sightglass.operations import operation_budget
from sightglass.runtime.daemon import SightglassDaemon
from sightglass.storage import MIB, StorageBudget, StorageSettings
from tests.unit import test_storage_history


class StorageDiagnosticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.free = patch("sightglass.storage._free", return_value=1024 * MIB)
        self.free.start()
        self.addCleanup(self.free.stop)
        self.budget = StorageBudget(
            self.root, self.root / "window.db", StorageSettings(4 * MIB, 8 * MIB, 0, 2 * MIB)
        )
        self.database = WindowDB(self.root / "window.db", storage=self.budget)

    def _expensive_count(self, database: WindowDB, _name: str) -> int:
        rows = _query(
            database,
            """
            WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000)
            SELECT SUM(x) FROM n
        """,
        )
        return int(rows[0][0])

    def test_default_capacity_summary_never_counts_or_reconciles_full_inventory(self) -> None:
        sql: list[str] = []
        connect = self.database.connect

        def traced_connect():
            connection = connect()
            connection.set_trace_callback(sql.append)
            return connection

        with (
            patch.object(self.database, "connect", side_effect=traced_connect),
            patch.object(self.budget, "reconcile", side_effect=AssertionError("full scan")),
        ):
            result = self.database.storage_explain(limit=1, sample_size=0)
        self.assertEqual(result["diagnostic"]["mode"], "quick")
        self.assertFalse(result["database"]["counts_available"])
        self.assertIsNone(result["database"]["message_count"])
        self.assertFalse(
            any(
                "COUNT(" in statement.upper() or "dbstat" in statement or "parsed_json" in statement
                for statement in sql
            )
        )
        self.assertEqual(result["files"]["inventory_source"], "tracked_files")
        self.assertFalse(result["mutated"])

    def test_deep_deadline_preserves_completed_table_and_releases_read_view(self) -> None:
        with self.database.transaction() as connection:
            connection.execute("CREATE TABLE aaa_synthetic(value INTEGER)")
            connection.execute("CREATE TABLE aab_synthetic(value INTEGER)")
            connection.execute("INSERT INTO aaa_synthetic VALUES(1)")

        def count(database: WindowDB, name: str) -> int:
            return (
                self._expensive_count(database, name)
                if name == "aab_synthetic"
                else (_table_count(database, name))
            )

        started = time.monotonic()
        with patch("sightglass.model.storage_diagnostics._table_count", side_effect=count):
            result = self.database.storage_explain(deep=True, phase="tables", deadline_seconds=0.03)
        self.assertLess(time.monotonic() - started, 2)
        progress = result["diagnostic"]
        self.assertEqual(progress["state"], "partial")
        self.assertEqual(progress["reason"], "operation_deadline")
        self.assertEqual(progress["last_completed_object"], "aaa_synthetic")
        self.assertEqual(result["database"]["objects"][0]["record_count"], 1)
        with self.database.connection() as connection:
            self.assertEqual(connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0], 0)
        resumed = self.database.storage_explain(
            deep=True, phase="tables", after_object="aaa_synthetic"
        )
        self.assertEqual(resumed["diagnostic"]["state"], "complete")
        self.assertNotIn("aaa_synthetic", [row["name"] for row in resumed["database"]["objects"]])

    def test_bounded_sample_does_not_scan_past_encoded_prefix_for_legacy_rows(self) -> None:
        # A shadow, schema-compatible disposable table lets this check avoid unrelated
        # canonical message admission while exercising the real diagnostic SQL.
        with self.database.transaction() as connection:
            connection.execute("DROP TABLE message_observations")
            connection.execute("""
                CREATE TABLE message_observations(observation_seq INTEGER PRIMARY KEY,
                                                  parsed_json BLOB)
            """)
            connection.executemany(
                "INSERT INTO message_observations VALUES(?,?)",
                [(i, b"encoded") for i in range(1, 101)],
            )
            connection.execute(
                "INSERT INTO message_observations VALUES(101, ?)", ('{"synthetic":"legacy"}',)
            )
        result = self.database.storage_explain(deep=True, phase="sample", sample_size=10)
        sample = result["database"]["legacy_compression_sample"]
        self.assertEqual(sample["examined_rows"], 10)
        self.assertEqual(sample["sampled_rows"], 0)

    def test_growth_shortfall_reports_both_floors_without_relaxing_limits(self) -> None:
        with patch("sightglass.storage._free", return_value=MIB):
            result = self.database.storage_explain()
        headroom = result["headroom"]
        self.assertFalse(headroom["foreground_admission_allowed"])
        self.assertEqual(headroom["filesystem_shortfall_bytes"], 2 * MIB)
        self.assertEqual(result["status"]["limits"]["maintenance_reserve_bytes"], 2 * MIB)

    def test_nested_operation_budget_cancels_deep_with_partial_result(self) -> None:
        cancelled = threading.Event()
        started = threading.Event()

        def count(database: WindowDB, name: str) -> int:
            started.set()
            return self._expensive_count(database, name)

        timer = threading.Thread(target=lambda: (started.wait(2), cancelled.set()))
        timer.start()
        with (
            operation_budget(None, cancelled=cancelled),
            patch("sightglass.model.storage_diagnostics._table_count", side_effect=count),
        ):
            result = self.database.storage_explain(deep=True, phase="tables")
        timer.join(2)
        self.assertEqual(result["diagnostic"]["reason"], "operation_cancelled")


class StorageDiagnosticDisconnectTests(unittest.TestCase):
    setUp = test_storage_history.StorageHistoryDaemonLifecycleTests.setUp
    daemon: SightglassDaemon

    def test_actual_operator_path_cancels_sqlite_when_client_disconnects(self) -> None:
        client, server = socket.socketpair()
        self.addCleanup(client.close)
        self.addCleanup(server.close)
        started = threading.Event()
        result: list[dict] = []
        errors: list[Exception] = []

        def count(database: WindowDB, _name: str) -> int:
            started.set()
            rows = _query(
                database,
                """
                WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000)
                SELECT SUM(x) FROM n
            """,
            )
            return int(rows[0][0])

        def dispatch() -> None:
            try:
                result.append(
                    self.daemon._dispatch(
                        "operator",
                        "operator.storage.explain",
                        {"deep": True, "phase": "tables"},
                        connection=server,
                    )
                )
            except Exception as exc:
                errors.append(exc)

        with patch("sightglass.model.storage_diagnostics._table_count", side_effect=count):
            worker = threading.Thread(target=dispatch)
            worker.start()
            self.assertTrue(started.wait(2))
            # General status continues to respond while the deep SQL is in flight.
            self.assertIn("storage", self.daemon.status())
            client.close()
            worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(result[0]["diagnostic"]["reason"], "operation_cancelled")
        self.assertFalse(
            any(
                t.name == "sightglass-storage-cancel" and t.is_alive()
                for t in threading.enumerate()
            )
        )

    def test_quick_diagnostic_is_still_operator_only(self) -> None:
        with self.assertRaises(RuntimeError):
            self.daemon._dispatch("reader", "operator.storage.explain", {})


if __name__ == "__main__":
    unittest.main()

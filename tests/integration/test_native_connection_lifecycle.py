from __future__ import annotations

import importlib
import os
import shutil
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from sightglass.contracts.errors import SightglassError
from sightglass.operations import operation_budget
from sightglass.source.base import SourceScope
from sightglass.source.macos_wechat import provider as native
from tests.integration import test_native_source_provider as native_fixture


class NativeConnectionLifecycleTests(unittest.TestCase):
    """Real encrypted fixture handles; no installed account or runtime access."""

    def setUp(self) -> None:
        self.fixture = native_fixture.NativeSourceProviderTests(methodName="runTest")
        self.fixture.setUp()
        self.provider = self.fixture.provider
        self.relative = "message/message_0.db"
        self.sqlite = importlib.import_module("sqlcipher3.dbapi2")

    def tearDown(self) -> None:
        self.fixture.tearDown()

    def test_reuse_and_idle_limit_only_retire_unused_handles(self) -> None:
        with self.provider._connect(self.relative) as first:
            first.execute("SELECT 1").fetchone()
        with self.provider._connect(self.relative) as again:
            self.assertIs(first, again)
        with patch.object(native, "NATIVE_IDLE_CONNECTION_LIMIT", 2):
            with self.provider._connect(self.relative) as active:
                for index in range(12):
                    relative = f"message/message_{index + 1}.db"
                    shutil.copy2(
                        self.fixture.source / self.relative, self.fixture.source / relative
                    )
                    self.provider._keys[relative] = self.provider._keys[self.relative]
                    with self.provider._connect(relative):
                        pass
                    status = self.provider.connection_cache_status()
                    self.assertLessEqual(status["idle_handles"], 2)
                    self.assertEqual(status["active_or_waiting_users"], 1)
                    active.execute("SELECT 1").fetchone()
            self.assertLessEqual(self.provider.connection_cache_status()["shared_handles"], 2)

    def test_same_path_replacement_retires_old_after_active_release(self) -> None:
        source = self.fixture.source / self.relative
        with self.provider._connect(self.relative) as old:
            replacement = source.with_suffix(".replacement")
            shutil.copy2(source, replacement)
            replacement.replace(source)
            with self.provider._connect(self.relative) as new:
                self.assertIsNot(old, new)
                self.assertEqual(self.provider.connection_cache_status()["retiring_handles"], 1)
                old.execute("SELECT 1").fetchone()
            old.execute("SELECT 1").fetchone()
        self.assertEqual(self.provider.connection_cache_status()["retiring_handles"], 0)
        with self.assertRaises(self.sqlite.ProgrammingError):
            old.execute("SELECT 1")

    def test_repeated_identity_replacement_keeps_handle_and_fd_set_bounded(self) -> None:
        source = self.fixture.source / self.relative
        with self.provider._connect(self.relative):
            pass
        before_fds = len(os.listdir("/dev/fd"))
        for _ in range(40):
            with self.provider._connect(self.relative) as old:
                replacement = source.with_suffix(".replacement")
                shutil.copy2(source, replacement)
                replacement.replace(source)
                with self.provider._connect(self.relative) as current:
                    current.execute("SELECT 1").fetchone()
                    old.execute("SELECT 1").fetchone()
            status = self.provider.connection_cache_status()
            self.assertEqual(status["shared_handles"], 1)
            self.assertEqual(status["retiring_handles"], 0)
            self.assertEqual(status["active_or_waiting_users"], 0)
            with self.assertRaises(self.sqlite.ProgrammingError):
                old.execute("SELECT 1")
        self.assertLessEqual(len(os.listdir("/dev/fd")), before_fds + 2)

    def test_idle_age_sweep_does_not_open_source(self) -> None:
        with self.provider._connect(self.relative) as connection:
            pass
        slot = next(iter(self.provider._connection_cache.values()))
        slot.last_used -= native.NATIVE_IDLE_CONNECTION_SECONDS + 1
        with patch.object(self.provider, "_connection_identity", side_effect=AssertionError):
            self.assertEqual(self.provider.maintain_idle_connections(), 1)
        with self.assertRaises(self.sqlite.ProgrammingError):
            connection.execute("SELECT 1")

    def test_scoped_handles_are_counted_and_closed_even_on_error(self) -> None:
        scope = SourceScope(
            kind="conversation", account_id=self.fixture.account_key,
            conversation_source_id=self.fixture.conversation,
        )
        with self.provider.session(scope), self.provider._connect(self.relative) as connection:
            self.assertEqual(self.provider.connection_cache_status()["scoped_handles"], 1)
            self.assertEqual(self.provider.connection_cache_status()["shared_handles"], 0)
            connection.execute("SELECT 1").fetchone()
        self.assertEqual(self.provider.connection_cache_status()["scoped_handles"], 0)
        with self.assertRaises(self.sqlite.ProgrammingError):
            connection.execute("SELECT 1")
        with self.assertRaisesRegex(ValueError, "synthetic failure"):
            with self.provider.session(scope), self.provider._connect(self.relative):
                self.assertEqual(self.provider.connection_cache_status()["scoped_handles"], 1)
                raise ValueError("synthetic failure")
        self.assertEqual(self.provider.connection_cache_status()["scoped_handles"], 0)

    def test_close_preserves_active_user_and_revokes_reserved_waiter(self) -> None:
        with self.provider._connect(self.relative) as connection:
            waiter, _ = self.provider._connection_slot(self.relative, auxiliary=False)
            self.assertEqual(waiter.users, 2)
            self.provider.close()
            connection.execute("SELECT 1").fetchone()
            # Let the waiter try acquiring after the active holder has released.
        with self.assertRaises(SightglassError):
            self.provider._acquire_connection(waiter)
        self.assertEqual(waiter.users, 0)
        self.assertTrue(waiter.closed)
        self.assertEqual(self.provider.connection_cache_status()["retiring_handles"], 0)

    def test_cancelled_waiter_releases_its_reservation(self) -> None:
        cancelled = threading.Event()
        with self.provider._connect(self.relative) as connection:
            slot, _ = self.provider._connection_slot(self.relative, auxiliary=False)
            with operation_budget(1, cancelled=cancelled):
                cancelled.set()
                with self.assertRaises(SightglassError):
                    self.provider._acquire_connection(slot)
            self.assertEqual(slot.users, 1)
            connection.execute("SELECT 1").fetchone()
        self.assertEqual(slot.users, 0)

    def test_cancelled_builder_clears_build_and_closes_handle(self) -> None:
        cancelled = threading.Event()
        handles = []

        def connect(*args, **kwargs):
            handle = self.sqlite.connect(*args, **kwargs)
            handles.append(handle)
            cancelled.set()
            return handle

        adapter = SimpleNamespace(
            connect=connect, Row=self.sqlite.Row, DatabaseError=self.sqlite.DatabaseError
        )
        with operation_budget(1, cancelled=cancelled), patch.object(
            native.importlib, "import_module", return_value=adapter
        ), self.assertRaises(SightglassError):
            with self.provider._connect(self.relative):
                self.fail("cancelled builder published")
        self.assertEqual(self.provider.connection_cache_status()["building_handles"], 0)
        self.assertEqual(self.provider.connection_cache_status()["shared_handles"], 0)
        with self.assertRaises(self.sqlite.ProgrammingError):
            handles[0].execute("SELECT 1")

    def test_slow_retired_close_does_not_hold_cache_registry(self) -> None:
        entered = threading.Event()
        finish = threading.Event()
        inspected = threading.Event()
        with self.provider._connect(self.relative):
            pass
        slot = next(iter(self.provider._connection_cache.values()))
        slot.last_used -= native.NATIVE_IDLE_CONNECTION_SECONDS + 1
        original = slot.connection

        def close():
            entered.set()
            self.assertTrue(finish.wait(3))
            original.close()

        slot.connection = SimpleNamespace(close=close)
        sweeper = threading.Thread(target=self.provider.maintain_idle_connections)
        sweeper.start()
        self.assertTrue(entered.wait(3))

        def inspect():
            self.provider.connection_cache_status()
            inspected.set()

        reader = threading.Thread(target=inspect)
        reader.start()
        try:
            self.assertTrue(inspected.wait(1), "close held the registry mutex")
        finally:
            finish.set()
            sweeper.join(3)
            reader.join(3)
        self.assertFalse(sweeper.is_alive())
        with self.assertRaises(self.sqlite.ProgrammingError):
            original.execute("SELECT 1")

    def _gated_builder(self, *, close_during_build: bool) -> None:
        entered = threading.Event()
        finish = threading.Event()
        handles: list = []
        errors: list[BaseException] = []
        successes: list[int] = []

        def connect(*args, **kwargs):
            handle = self.sqlite.connect(*args, **kwargs)
            handles.append(handle)
            entered.set()
            self.assertTrue(finish.wait(3))
            return handle

        adapter = SimpleNamespace(
            connect=connect, Row=self.sqlite.Row, DatabaseError=self.sqlite.DatabaseError
        )

        def read() -> None:
            try:
                with self.provider._connect(self.relative) as connection:
                    successes.append(connection.execute("SELECT 1").fetchone()[0])
            except BaseException as exc:
                errors.append(exc)

        with patch.object(native.importlib, "import_module", return_value=adapter):
            first = threading.Thread(target=read)
            first.start()
            self.assertTrue(entered.wait(3))
            second = threading.Thread(target=read)
            second.start()
            if close_during_build:
                self.provider.close()
            finish.set()
            first.join(3)
            second.join(3)
            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())
        self.assertEqual(len(handles), 1)
        self.assertEqual(self.provider.connection_cache_status()["building_handles"], 0)
        if close_during_build:
            self.assertEqual(len(errors), 2)
            self.assertTrue(all(isinstance(error, SightglassError) for error in errors))
            with self.assertRaises(self.sqlite.ProgrammingError):
                handles[0].execute("SELECT 1")
        else:
            self.assertEqual(errors, [])
            self.assertEqual(successes, [1, 1])

    def test_concurrent_build_is_single_flight(self) -> None:
        self._gated_builder(close_during_build=False)

    def test_close_during_build_does_not_publish_or_leak(self) -> None:
        self._gated_builder(close_during_build=True)

    def test_close_during_scoped_build_closes_unpublished_handle(self) -> None:
        entered = threading.Event()
        finish = threading.Event()
        handles: list = []
        errors: list[BaseException] = []

        def connect(*args, **kwargs):
            handle = self.sqlite.connect(*args, **kwargs)
            handles.append(handle)
            entered.set()
            self.assertTrue(finish.wait(3))
            return handle

        adapter = SimpleNamespace(
            connect=connect, Row=self.sqlite.Row, DatabaseError=self.sqlite.DatabaseError
        )

        def build():
            try:
                self.provider._open_scoped_connection(self.relative, auxiliary=False)
            except BaseException as error:
                errors.append(error)

        with patch.object(native.importlib, "import_module", return_value=adapter):
            builder = threading.Thread(target=build)
            builder.start()
            try:
                self.assertTrue(entered.wait(3))
                self.provider.close()
            finally:
                finish.set()
                builder.join(3)
        self.assertFalse(builder.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], SightglassError)
        self.assertEqual(self.provider.connection_cache_status()["scoped_handles"], 0)
        with self.assertRaises(self.sqlite.ProgrammingError):
            handles[0].execute("SELECT 1")

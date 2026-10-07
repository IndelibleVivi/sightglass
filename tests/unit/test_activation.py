from __future__ import annotations

import shutil
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from sightglass.model.db import WindowDB
from sightglass.runtime.activation import read_activation, require_core_activation, write_activation
from sightglass.runtime.config import SightglassConfig


class ActivationTests(unittest.TestCase):
    def test_revocation_during_writer_rolls_back_and_blocks_later_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            config = replace(
                SightglassConfig.create(root / "data", root),
                activation_generation="synthetic-owner",
                activation_path=root / "owner",
            )
            write_activation(
                root / "owner",
                generation="synthetic-owner",
                state="active",
                role="core",
                namespace=config.window_db_path,
            )
            database = WindowDB(
                config.window_db_path, write_guard=lambda: require_core_activation(config)
            )
            with database.transaction() as connection:
                connection.execute("CREATE TABLE synthetic_activation(value TEXT)")
            with self.assertRaisesRegex(RuntimeError, "no active core"):
                with database.transaction() as connection:
                    connection.execute("INSERT INTO synthetic_activation VALUES('one')")
                    write_activation(
                        root / "owner",
                        generation="synthetic-owner",
                        state="revoked",
                        role="core",
                        namespace=config.window_db_path,
                        expected_generation="synthetic-owner",
                    )
            with database.connection() as connection:
                self.assertIsNone(
                    connection.execute("SELECT value FROM synthetic_activation").fetchone()
                )
            with self.assertRaisesRegex(RuntimeError, "no active core"):
                with database.transaction():
                    self.fail("revoked writer was admitted")

    def test_revoked_old_host_cannot_restart_and_target_needs_new_generation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            config = SightglassConfig.create(root / "data", root)
            old = replace(
                config, activation_generation="synthetic-old", activation_path=root / "old.json"
            )
            target = replace(
                config,
                source_kind="remote-capture",
                reader_default_view="replica",
                activation_generation="synthetic-new",
                activation_path=root / "new.json",
            )
            assert old.activation_path is not None and target.activation_path is not None
            write_activation(
                old.activation_path,
                generation="synthetic-old",
                state="active",
                role="core",
                namespace=old.window_db_path,
            )
            require_core_activation(old)
            write_activation(
                old.activation_path,
                generation="synthetic-old",
                state="revoked",
                role="core",
                namespace=old.window_db_path,
                expected_generation="synthetic-old",
            )
            with self.assertRaisesRegex(RuntimeError, "no active core"):
                require_core_activation(old)
            with self.assertRaises(RuntimeError):
                require_core_activation(target)
            write_activation(
                target.activation_path,
                generation="synthetic-new",
                state="active",
                role="core",
                predecessor="synthetic-old",
                namespace=target.window_db_path,
                counter=2,
            )
            require_core_activation(target)
            with self.assertRaises(RuntimeError):
                require_core_activation(replace(target, activation_generation="synthetic-old"))

    def test_remote_core_cannot_use_legacy_unfenced_activation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = SightglassConfig.create(temporary, temporary)
            require_core_activation(config)
            with self.assertRaisesRegex(RuntimeError, "explicit activation"):
                require_core_activation(replace(config, source_kind="remote-capture"))

    def test_generation_transition_requires_expected_current_owner(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            path = root / "activation.json"
            write_activation(
                path,
                generation="synthetic-one",
                state="active",
                role="core",
                namespace=root / "data" / "window.db",
            )
            with self.assertRaises(RuntimeError):
                write_activation(
                    path,
                    generation="synthetic-two",
                    state="active",
                    role="core",
                    namespace=root / "data" / "window.db",
                )
            with self.assertRaises(RuntimeError):
                write_activation(
                    path,
                    generation="synthetic-two",
                    state="active",
                    role="core",
                    expected_generation="synthetic-wrong",
                    namespace=root / "data" / "window.db",
                )

    def test_recovery_clone_cannot_reuse_namespace_host_or_missing_credential(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            config = replace(
                SightglassConfig.create(root / "active", root),
                activation_generation="synthetic-live",
                activation_path=root / "grant",
            )
            write_activation(
                root / "grant",
                generation="synthetic-live",
                state="active",
                role="core",
                namespace=config.window_db_path,
            )
            require_core_activation(config)
            clone = replace(
                config, data_dir=root / "recovery", window_db_path=root / "recovery" / "window.db"
            )
            with self.assertRaisesRegex(RuntimeError, "host/namespace"):
                WindowDB(clone.window_db_path, write_guard=lambda: require_core_activation(clone))
            self.assertFalse(clone.window_db_path.exists())
            with mock.patch("sightglass.runtime.activation.host_identity", return_value="0" * 64):
                with self.assertRaisesRegex(RuntimeError, "host/namespace"):
                    require_core_activation(config)
            shutil.copyfile(root / "grant", root / "copied-grant")
            (root / "copied-grant").chmod(0o600)
            with self.assertRaisesRegex(RuntimeError, "credential is unavailable"):
                require_core_activation(replace(config, activation_path=root / "copied-grant"))

    def test_revoked_generation_cannot_reactivate_and_successor_increases_counter(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            path, namespace = root / "grant", root / "data" / "window.db"
            write_activation(
                path, generation="synthetic-one", state="active", role="core", namespace=namespace
            )
            write_activation(
                path,
                generation="synthetic-one",
                state="revoked",
                role="core",
                namespace=namespace,
                expected_generation="synthetic-one",
            )
            with self.assertRaisesRegex(RuntimeError, "new generation"):
                write_activation(
                    path,
                    generation="synthetic-one",
                    state="active",
                    role="core",
                    namespace=namespace,
                    expected_generation="synthetic-one",
                )
            with self.assertRaisesRegex(RuntimeError, "predecessor"):
                write_activation(
                    path,
                    generation="synthetic-two",
                    state="active",
                    role="core",
                    namespace=namespace,
                    expected_generation="synthetic-one",
                )
            write_activation(
                path,
                generation="synthetic-two",
                state="active",
                role="core",
                namespace=namespace,
                expected_generation="synthetic-one",
                predecessor="synthetic-one",
            )
            self.assertEqual(read_activation(path)["counter"], 2)

    def test_namespace_symlink_is_rejected_before_activation_is_written(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            namespace = root / "data"
            namespace.mkdir()
            alias = root / "alias"
            alias.symlink_to(namespace, target_is_directory=True)
            with self.assertRaisesRegex(RuntimeError, "symlink"):
                write_activation(
                    root / "grant",
                    generation="synthetic-owner",
                    state="active",
                    role="core",
                    namespace=alias / "window.db",
                )
            self.assertFalse((root / "grant").exists())


if __name__ == "__main__":
    unittest.main()

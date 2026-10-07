"""Actual atomic runtime/config/DB selection, launcher and crash cuts."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import subprocess
import sys
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from sightglass.runtime.config import ConfigStore, SightglassConfig
from sightglass.runtime.paired import activate_pair, prepare_pair, recover_selection, selected_pair
from sightglass.runtime.voice_setup import build_voice_setup, resolve_helper_path
from sightglass.storage import StorageSettings
from tests.integration.test_compact_candidate import CompactFixture


class PairedRuntimeTests(CompactFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.pairs = self.window.parent / "pairs"
        self.config_path = self.window.parent / "config.json"
        self.config = replace(
            SightglassConfig.create(self.window.parent, self.window.parent.parent / "source"),
            storage=StorageSettings(min_free_bytes=0),
        )
        ConfigStore(self.config_path).save(self.config)
        self.freeze()
        from sightglass.model.compact_candidate import build_candidate

        build_candidate(self.frozen, self.candidate, **self.kwargs)

    def prepare(self, candidate=False, **extra):
        return prepare_pair(
            self.pairs,
            config_path=self.config_path,
            runtime_python=Path(sys.executable),
            candidate_path=self.candidate if candidate else None,
            frozen_path=self.frozen if candidate else None,
            **(self.kwargs | extra),
        )

    def _default_helper(self, *, mode=0o700, content=None):
        self.config = replace(self.config, voice_enabled=True, voice_helper_path="")
        ConfigStore(self.config_path).save(self.config)
        helper = resolve_helper_path(self.config)
        helper.parent.mkdir(mode=0o700, exist_ok=True)
        helper.write_bytes(content if content is not None else (
            b"#!/bin/sh\n# Synthetic nonexecuted paired helper fixture.\nexit 0\n"
        ))
        helper.chmod(mode)
        return helper

    def _voice_readiness(self, config):
        with (
            patch("sightglass.runtime.voice_setup.SilkDecoder.available", return_value=True),
            patch("sightglass.runtime.voice_setup.SilkDecoder.version", return_value="synthetic"),
        ):
            return build_voice_setup(config, self.service).readiness

    def _assert_selected(self, pair_id):
        selected = selected_pair(self.pairs)
        assert selected is not None
        self.assertEqual(selected["pair_id"], pair_id)

    def test_default_helper_stays_ready_independent_and_rollback_preserves_original(self):
        helper = self._default_helper(mode=0o500)
        content = helper.read_bytes()
        self.assertTrue(self._voice_readiness(self.config)["ready"])
        old = self.prepare()
        activate_pair(self.pairs, old["pair_id"], expected_current=None)
        for candidate in (False, True):
            with self.subTest(candidate=candidate):
                new = self.prepare(candidate=candidate, copy_current=not candidate)
                config = ConfigStore(Path(new["config_path"])).load()
                copied = resolve_helper_path(config)
                self.assertEqual(config.voice_helper_path, "")
                self.assertEqual(copied.parent.parent, Path(new["database_path"]).parent)
                self.assertEqual(copied.read_bytes(), content)
                self.assertNotEqual(copied.stat().st_ino, helper.stat().st_ino)
                self.assertEqual(copied.stat().st_nlink, 1)
                self.assertEqual(stat.S_IMODE(copied.stat().st_mode), 0o500)
                self.assertEqual(copied.stat().st_uid, os.getuid())
                self.assertEqual(stat.S_IMODE(copied.parent.stat().st_mode), 0o700)
                self.assertTrue(self._voice_readiness(config)["ready"])
                activate_pair(self.pairs, new["pair_id"], expected_current=old["pair_id"])
                # Fixture-only cleanup of the new namespace cannot damage rollback.
                copied.unlink()
                activate_pair(self.pairs, old["pair_id"], expected_current=new["pair_id"])
                self.assertEqual(helper.read_bytes(), content)
                self.assertTrue(self._voice_readiness(self.config)["ready"])

    def test_explicit_external_helper_stays_an_explicit_reference(self):
        helper = self._default_helper()
        external = self.window.parent.parent / "external-helper"
        external.write_bytes(helper.read_bytes())
        external.chmod(0o700)
        self.config = replace(self.config, voice_helper_path=str(external))
        ConfigStore(self.config_path).save(self.config)
        for candidate in (False, True):
            with self.subTest(candidate=candidate):
                new = self.prepare(candidate=candidate, copy_current=not candidate)
                config = ConfigStore(Path(new["config_path"])).load()
                self.assertEqual(config.voice_helper_path, str(external))
                self.assertEqual(resolve_helper_path(config), external)
                self.assertFalse((config.data_dir / "voice" / helper.name).exists())
                self.assertFalse(any(item.get("kind") == "default_voice_helper"
                                     for item in new["state_files"]))
                self.assertTrue(self._voice_readiness(config)["ready"])

    def test_missing_default_helper_preserves_the_existing_blocked_state(self):
        self.config = replace(self.config, voice_enabled=True)
        ConfigStore(self.config_path).save(self.config)
        before = self._voice_readiness(self.config)
        self.assertFalse(before["ready"])
        self.assertEqual(before["blocked_reason"], "helper_missing")
        current = None
        for candidate in (False, True):
            with self.subTest(candidate=candidate):
                new = self.prepare(candidate=candidate, copy_current=not candidate)
                config = ConfigStore(Path(new["config_path"])).load()
                self.assertFalse(resolve_helper_path(config).exists())
                self.assertEqual(self._voice_readiness(config)["blocked_reason"], "helper_missing")
                activate_pair(self.pairs, new["pair_id"], expected_current=current)
                current = new["pair_id"]

    def test_unsafe_default_helper_fails_closed_without_selecting_a_partial_pair(self):
        helper = self._default_helper()
        old = self.prepare()
        activate_pair(self.pairs, old["pair_id"], expected_current=None)
        original = helper.read_bytes()
        for kind in ("symlink", "hardlink", "public", "nonexecuting", "empty", "directory"):
            with self.subTest(kind=kind):
                helper.unlink()
                if kind == "symlink":
                    helper.symlink_to(self.window)
                elif kind == "directory":
                    helper.mkdir(mode=0o700)
                else:
                    helper.write_bytes(b"" if kind == "empty" else original)
                    helper.chmod(0o755 if kind == "public" else 0o600 if kind == "nonexecuting"
                                 else 0o700)
                    if kind == "hardlink":
                        os.link(helper, helper.with_name("synthetic-hardlink"))
                with self.assertRaisesRegex(RuntimeError, "voice helper"):
                    self.prepare(copy_current=True)
                self._assert_selected(old["pair_id"])
                if kind == "hardlink":
                    helper.with_name("synthetic-hardlink").unlink()
                if kind == "directory":
                    helper.rmdir()
                    helper.write_bytes(original)
                elif helper.is_symlink():
                    helper.unlink()
                    helper.write_bytes(original)
                helper.chmod(0o700)
        helper.parent.chmod(0o755)
        with self.assertRaisesRegex(RuntimeError, "voice helper directory"):
            self.prepare(candidate=True)
        helper.parent.chmod(0o700)
        # Inject only the helper's foreign-owner metadata; directories stay owned.
        original_lstat = Path.lstat

        def foreign_owner(path):
            metadata = original_lstat(path)
            if path == helper:
                values = list(metadata)
                values[4] = os.getuid() + 1
                return os.stat_result(values)
            return metadata

        with patch.object(Path, "lstat", foreign_owner):
            with self.assertRaisesRegex(RuntimeError, "voice helper"):
                self.prepare(copy_current=True)
        self._assert_selected(old["pair_id"])

    def test_default_helper_source_and_stage_tamper_fence_first_activation(self):
        helper = self._default_helper()
        original = helper.read_bytes()
        old = self.prepare()
        activate_pair(self.pairs, old["pair_id"], expected_current=None)
        for candidate in (False, True):
            for side in ("source", "stage", "stage_mode", "stage_inode", "stage_symlink",
                         "stage_hardlink", "configuration"):
                with self.subTest(candidate=candidate, side=side):
                    helper.write_bytes(original)
                    new = self.prepare(candidate=candidate, copy_current=not candidate)
                    staged = resolve_helper_path(ConfigStore(Path(new["config_path"])).load())
                    if side == "source":
                        helper.write_bytes(original + b"# synthetic source tamper\n")
                    elif side == "stage":
                        staged.write_bytes(original + b"# synthetic stage tamper\n")
                    elif side == "stage_mode":
                        staged.chmod(0o600)
                    elif side == "stage_symlink":
                        staged.unlink()
                        staged.symlink_to(helper)
                    elif side == "stage_hardlink":
                        os.link(staged, staged.with_name("synthetic-staged-hardlink"))
                    elif side == "configuration":
                        config_path = Path(new["config_path"])
                        changed = ConfigStore(config_path).load()
                        ConfigStore(config_path).save(
                            replace(changed, voice_helper_path=str(helper))
                        )
                    else:
                        staged.unlink()
                        staged.write_bytes(original)
                        staged.chmod(0o700)
                    with self.assertRaisesRegex(RuntimeError, "voice helper"):
                        activate_pair(self.pairs, new["pair_id"], expected_current=old["pair_id"])
                    self._assert_selected(old["pair_id"])

    def test_missing_helper_created_after_preparation_requires_a_new_preparation(self):
        self.config = replace(self.config, voice_enabled=True)
        ConfigStore(self.config_path).save(self.config)
        old = self.prepare()
        activate_pair(self.pairs, old["pair_id"], expected_current=None)
        for side in ("source", "stage"):
            with self.subTest(side=side):
                new = self.prepare(copy_current=True)
                config = ConfigStore(Path(new["config_path"])).load()
                target = resolve_helper_path(self.config if side == "source" else config)
                target.parent.mkdir(mode=0o700, exist_ok=True)
                target.write_bytes(b"#!/bin/sh\n# Synthetic late helper.\nexit 0\n")
                target.chmod(0o700)
                with self.assertRaisesRegex(RuntimeError, "voice helper private state changed"):
                    activate_pair(self.pairs, new["pair_id"], expected_current=old["pair_id"])
                target.unlink()
                self._assert_selected(old["pair_id"])

    def test_helper_copy_budget_and_free_floor_failures_do_not_publish_a_partial_pair(self):
        from sightglass.model.compact_candidate import CompactCandidateError, _capacity

        helper = self._default_helper(content=b"# Synthetic helper budget bytes\n" * 4096)
        original = helper.read_bytes()
        old = self.prepare()
        activate_pair(self.pairs, old["pair_id"], expected_current=None)
        original_open = Path.open

        def copy_failure(path, *args, **kwargs):
            if path.name == helper.name and args and args[0] == "xb":
                # Simulate a write interrupted after creating a partial helper.
                with original_open(path, *args, **kwargs) as writer:
                    os.fchmod(writer.fileno(), 0o700)
                    writer.write(original[:10])
                raise OSError("synthetic helper copy failure")
            return original_open(path, *args, **kwargs)

        with patch.object(Path, "open", copy_failure):
            with self.assertRaisesRegex(OSError, "synthetic helper copy failure"):
                self.prepare(copy_current=True)
        used = _capacity(self.pairs, budget=64 * 1024**2, min_free=0)
        budget = used + self.window.stat().st_size + 16 * 1024
        with self.assertRaisesRegex(CompactCandidateError, "budget/free floor exceeded"):
            self.prepare(copy_current=True, workspace_budget_bytes=budget)
        with patch("sightglass.model.compact_candidate._free",
                   side_effect=[1024**3, len(original) + 4096 + 1024 - 1]):
            with self.assertRaisesRegex(CompactCandidateError, "budget/free floor exceeded"):
                self.prepare(copy_current=True, min_free_bytes=1024)
        self.assertEqual(helper.read_bytes(), original)
        self._assert_selected(old["pair_id"])
        incomplete = [path for path in self.pairs.iterdir()
                      if path.is_dir() and not (path / "pair.json").exists()]
        self.assertEqual(len(incomplete), 3)
        self.assertEqual([path.read_bytes() for directory in incomplete
                          for path in (directory / "voice" / helper.name,)
                          if path.exists()], [original[:10]])
        for path in incomplete:
            with self.assertRaises(FileNotFoundError):
                activate_pair(self.pairs, path.name, expected_current=old["pair_id"])

    def test_default_helper_survives_staging_and_selection_crash_recovery(self):
        helper = self._default_helper()
        old = self.prepare()
        activate_pair(self.pairs, old["pair_id"], expected_current=None)

        def stage_cut(phase):
            if phase == "state_copied":
                raise RuntimeError("synthetic helper stage cut")

        with self.assertRaisesRegex(RuntimeError, "helper stage cut"):
            self.prepare(copy_current=True, fault=stage_cut)
        self._assert_selected(old["pair_id"])
        for candidate in (False, True):
            new = self.prepare(candidate=candidate, copy_current=not candidate)

            def selection_cut(phase):
                if phase == "selection_published":
                    raise RuntimeError("synthetic helper selection cut")

            with self.assertRaisesRegex(RuntimeError, "helper selection cut"):
                activate_pair(self.pairs, new["pair_id"], expected_current=old["pair_id"],
                              fault=selection_cut)
            self.assertEqual(recover_selection(self.pairs)["selected"], new["pair_id"])
            config = ConfigStore(Path(new["config_path"])).load()
            self.assertTrue(self._voice_readiness(config)["ready"])
            self.assertEqual(resolve_helper_path(config).read_bytes(), helper.read_bytes())
            activate_pair(self.pairs, old["pair_id"], expected_current=new["pair_id"])

    def test_selects_both_and_real_launcher_uses_selected_config(self):
        old, new = self.prepare(), self.prepare(candidate=True)
        activate_pair(self.pairs, old["pair_id"], expected_current=None)
        activate_pair(self.pairs, new["pair_id"], expected_current=old["pair_id"])
        selected = selected_pair(self.pairs)
        assert selected is not None
        self.assertEqual(selected["database_path"], new["database_path"])
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "sightglass.runtime.paired",
                "--root",
                str(self.pairs),
                "ctl",
                "storage",
                "compact",
                "preview",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        report = json.loads(result.stdout)
        self.assertFalse(report["mutated"])
        self.assertEqual(selected["schema_version"], 10)
        self.assertEqual(report["database_bytes"], Path(new["database_path"]).stat().st_size)
        # The old DB/config still exists and is a valid explicit rollback target.
        activate_pair(self.pairs, old["pair_id"], expected_current=new["pair_id"])
        selected = selected_pair(self.pairs)
        assert selected is not None
        self.assertEqual(selected["database_path"], str(self.window.resolve()))
        self.assertTrue(Path(new["database_path"]).exists())

    def test_crash_before_and_after_atomic_selection_never_splits_the_pair(self):
        old, new = self.prepare(), self.prepare(candidate=True)
        activate_pair(self.pairs, old["pair_id"], expected_current=None)
        for phase in ("selection_prepared", "selection_published"):

            def interrupted(at):
                if at == phase:
                    raise RuntimeError("synthetic selection crash")

            with self.assertRaises(RuntimeError):
                activate_pair(
                    self.pairs, new["pair_id"], expected_current=old["pair_id"], fault=interrupted
                )
            wanted = old if phase == "selection_prepared" else new
            selected = selected_pair(self.pairs)
            assert selected is not None
            self.assertEqual(selected["pair_id"], wanted["pair_id"])
            self.assertEqual(recover_selection(self.pairs)["selected"], wanted["pair_id"])
        selected = selected_pair(self.pairs)
        assert selected is not None
        self.assertEqual(selected["runtime_python"], new["runtime_python"])

    def test_staging_crash_keeps_old_selection_and_no_partial_pair_is_selectable(self):
        old = self.prepare()
        activate_pair(self.pairs, old["pair_id"], expected_current=None)

        def interrupted(phase):
            if phase == "stage_copied":
                raise RuntimeError("synthetic stage crash")

        with self.assertRaises(RuntimeError):
            self.prepare(candidate=True, fault=interrupted)
        selected = selected_pair(self.pairs)
        assert selected is not None
        self.assertEqual(selected["pair_id"], old["pair_id"])
        incomplete = [
            path
            for path in self.pairs.iterdir()
            if path.is_dir() and not (path / "pair.json").exists()
        ]
        self.assertEqual(len(incomplete), 1)
        with self.assertRaises(FileNotFoundError):
            activate_pair(self.pairs, incomplete[0].name, expected_current=old["pair_id"])

    def test_unpaired_database_schema_is_refused(self):
        import sqlite3

        with closing(sqlite3.connect(self.window)) as connection:
            connection.execute("PRAGMA user_version=9")
        with self.assertRaisesRegex(RuntimeError, "schema mismatch"):
            self.prepare()

    def test_namespace_replay_cas_and_tokens_remain_independent_after_cleanup(self):
        self._namespace_replay(candidate=True)

    def test_current_schema_relocation_preserves_replay_cas_and_tokens(self):
        self._namespace_replay(candidate=False)

    def test_parent_path_alias_keeps_relocated_replay_references_canonical(self):
        alias = self.window.parent.parent / "synthetic-state-alias"
        alias.symlink_to(self.window.parent, target_is_directory=True)
        self.pairs = alias / "pairs"
        self._namespace_replay(candidate=False)

    def _namespace_replay(self, *, candidate):
        from sightglass.model.compact_candidate import build_candidate, freeze_input
        from sightglass.reader.deliveries import DeliveryPayloadStore
        from sightglass.resources.cache import ResourceObjectStore
        from sightglass.source.identity import SignedTokenCodec, load_or_create_token_secret

        secret = load_or_create_token_secret(self.window)
        token = SignedTokenCodec(secret).encode({"synthetic": "pending cursor"})
        spool = DeliveryPayloadStore(self.window)
        payload = {"synthetic": "exact replay", "token": token}
        reference, digest = spool.write("synthetic-paired-delivery", payload)
        with self.repository.database.connection() as connection:
            message = connection.execute("SELECT * FROM messages LIMIT 1").fetchone()
        self.repository.create_pending_delivery(
            delivery_id="synthetic-paired-delivery",
            reader_id=self.service.reader.reader_id,
            conversation_id=message["conversation_id"],
            scope_kind="conversation",
            scope_key=message["conversation_id"],
            from_observation_seq=0,
            to_observation_seq=message["current_observation_seq"],
            payload_digest=digest,
            payload_ref=reference,
            projection_schema_version="synthetic-paired-projection",
            created_at="synthetic",
        )
        objects = ResourceObjectStore(self.window)
        obj, object_path = objects.put(
            b"synthetic exact CAS bytes",
            mime_type="text/plain",
            origin="source_original",
        )
        with self.repository.database.transaction() as connection:
            connection.execute(
                "INSERT INTO resource_objects VALUES (?,?,?,?,?,?,NULL)",
                (
                    obj.digest,
                    object_path,
                    "text/plain",
                    len(obj.data),
                    "source_original",
                    "synthetic",
                ),
            )
        self.workspace = self.window.parent.parent / "namespace-compact"
        self.frozen = self.workspace / "frozen.db"
        self.candidate = self.workspace / "candidate.db"
        freeze_input(self.window, self.workspace, **self.kwargs)
        build_candidate(self.frozen, self.candidate, **self.kwargs)
        old, new = self.prepare(), self.prepare(candidate=candidate, copy_current=not candidate)
        activate_pair(self.pairs, old["pair_id"], expected_current=None)
        activate_pair(self.pairs, new["pair_id"], expected_current=old["pair_id"])
        database = Path(new["database_path"])
        with closing(sqlite3.connect(database)) as connection:
            new_ref = connection.execute("SELECT payload_ref FROM reader_deliveries").fetchone()[0]
            new_object = connection.execute(
                "SELECT local_path_internal FROM resource_objects"
            ).fetchone()[0]
        self.assertEqual(DeliveryPayloadStore(database).read(new_ref, digest), payload)
        self.assertEqual(load_or_create_token_secret(database), secret)
        self.assertEqual(
            SignedTokenCodec(load_or_create_token_secret(database)).decode(token),
            {"synthetic": "pending cursor"},
        )
        self.assertEqual(
            ResourceObjectStore(database)
            .read_path(
                Path(new_object),
                expected_digest=obj.digest,
                expected_size=len(obj.data),
                mime_type="text/plain",
                origin="source_original",
            )
            .data,
            obj.data,
        )
        self.assertNotEqual(os.stat(new_ref).st_ino, os.stat(reference).st_ino)
        Path(new_ref).unlink()
        Path(new_object).unlink()
        activate_pair(self.pairs, old["pair_id"], expected_current=new["pair_id"])
        self.assertEqual(spool.read(reference, digest), payload)
        self.assertEqual(Path(object_path).read_bytes(), obj.data)
        self.assertEqual(load_or_create_token_secret(self.window), secret)

    def test_changed_staged_secret_is_rejected_before_selection(self):
        from sightglass.source.identity import load_or_create_token_secret

        load_or_create_token_secret(self.window)
        new = self.prepare(candidate=True)
        secret = Path(new["database_path"]).with_name("token-secret")
        secret.write_bytes(b"synthetic changed token secret" * 2)
        with self.assertRaisesRegex(RuntimeError, "private state changed"):
            activate_pair(self.pairs, new["pair_id"], expected_current=None)
        self.assertIsNone(selected_pair(self.pairs))

    def test_current_relocation_fences_source_and_stage_changes(self):
        from sightglass.model.compact_candidate import verify_candidate

        old = self.prepare()
        activate_pair(self.pairs, old["pair_id"], expected_current=None)
        new = self.prepare(copy_current=True)
        verify_candidate(
            self.window, Path(new["database_path"]),
            state_relocation=(self.window.parent, Path(new["database_path"]).parent),
        )
        with closing(sqlite3.connect(self.window)) as connection:
            connection.execute("UPDATE accounts SET current_display_name='Synthetic changed'")
            connection.commit()
        with self.assertRaisesRegex(RuntimeError, "active database changed"):
            activate_pair(self.pairs, new["pair_id"], expected_current=old["pair_id"])
        fresh = self.prepare(copy_current=True)
        with closing(sqlite3.connect(fresh["database_path"])) as connection:
            connection.execute("UPDATE accounts SET current_display_name='Synthetic tampered'")
            connection.commit()
        with self.assertRaisesRegex(RuntimeError, "staged candidate changed"):
            activate_pair(self.pairs, fresh["pair_id"], expected_current=old["pair_id"])
        selected = selected_pair(self.pairs)
        assert selected is not None
        self.assertEqual(selected["pair_id"], old["pair_id"])

    def test_current_relocation_refuses_wal_and_mixed_candidate(self):
        with self.assertRaisesRegex(RuntimeError, "cannot also select"):
            self.prepare(candidate=True, copy_current=True)
        with closing(sqlite3.connect(self.window)) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("UPDATE accounts SET current_display_name='Synthetic pending WAL'")
            connection.commit()
            with self.assertRaisesRegex(RuntimeError, "uncheckpointed WAL"):
                self.prepare(copy_current=True)

    def test_current_relocation_crash_keeps_old_pair(self):
        old = self.prepare()
        activate_pair(self.pairs, old["pair_id"], expected_current=None)

        def interrupted(phase):
            if phase == "state_copied":
                raise RuntimeError("synthetic relocation cut")

        with self.assertRaisesRegex(RuntimeError, "relocation cut"):
            self.prepare(copy_current=True, fault=interrupted)
        selected = selected_pair(self.pairs)
        assert selected is not None
        self.assertEqual(selected["pair_id"], old["pair_id"])
        new = self.prepare(copy_current=True)

        def selection_cut(phase):
            if phase == "selection_published":
                raise RuntimeError("synthetic selection cut")

        with self.assertRaisesRegex(RuntimeError, "selection cut"):
            activate_pair(
                self.pairs, new["pair_id"], expected_current=old["pair_id"], fault=selection_cut,
            )
        self.assertEqual(recover_selection(self.pairs)["selected"], new["pair_id"])


if __name__ == "__main__":
    unittest.main()

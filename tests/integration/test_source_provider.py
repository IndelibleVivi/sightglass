from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.db import WindowDB
from sightglass.model.repositories import WindowRepository
from sightglass.source.direct_wechat import DirectWeChatSourceProvider
from sightglass.source.synthetic import create_synthetic_source


class DirectSourceProviderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "source"
        create_synthetic_source(self.root)
        self.provider = DirectWeChatSourceProvider(self.root)

    def tearDown(self):
        self.temp.cleanup()

    def _copy_message_to_second_shard(
        self, source_message_id: str, *, replacements: dict[str, object] | None = None
    ) -> None:
        with closing(sqlite3.connect(self.root / "messages-1.db")) as source:
            source.row_factory = sqlite3.Row
            row = source.execute(
                "SELECT * FROM messages WHERE source_message_id = ?",
                (source_message_id,),
            ).fetchone()
        assert row is not None
        values = dict(row)
        values.update(replacements or {})
        columns = tuple(values)
        with closing(sqlite3.connect(self.root / "messages-2.db")) as target:
            target.execute(
                f"INSERT INTO messages({','.join(columns)}) "
                f"VALUES ({','.join('?' for _ in columns)})",
                tuple(values[column] for column in columns),
            )
            target.commit()

    def test_health_reports_complete_multi_shard_inventory_without_paths(self):
        health = self.provider.health()
        self.assertTrue(health.complete)
        self.assertEqual(health.shard_counts["present"], 2)
        self.assertEqual(health.shard_counts["missing"], 0)
        self.assertNotIn(str(self.root), json.dumps(health.as_dict()))

    def test_missing_expected_shard_fails_closed(self):
        missing_root = Path(self.temp.name) / "missing"
        create_synthetic_source(
            missing_root,
            include_second_shard=False,
            declare_second_shard=True,
        )
        provider = DirectWeChatSourceProvider(missing_root)
        health = provider.health()
        self.assertFalse(health.complete)
        self.assertEqual(health.shard_counts["missing"], 1)
        with self.assertRaises(SightglassError) as caught:
            with provider.snapshot():
                pass
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)

    def test_generation_change_during_snapshot_aborts_read(self):
        manifest_path = self.root / "source.json"
        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                manifest["shards"][0]["generation_id"] = "generation-1b"
                manifest_path.write_text(
                    json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                self.provider.read_recent("synthetic-account-demo", "conv_group", 100, snapshot)
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)

    def test_snapshot_binds_manifest_identity_semantics(self):
        manifest_path = self.root / "source.json"
        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                manifest["account"]["self_principal_key"] = "wxid_replacement"
                manifest_path.write_text(
                    json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                self.provider.list_accounts(snapshot)
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)

    def test_snapshot_detects_wal_only_mutation(self):
        shard = self.root / "messages-1.db"
        connection = sqlite3.connect(shard)
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA wal_autocheckpoint = 0")
            with self.assertRaises(SightglassError) as caught:
                with self.provider.snapshot() as snapshot:
                    connection.execute(
                        """
                        UPDATE messages SET raw_content = 'changed in WAL'
                        WHERE source_message_id = 'source-msg-001'
                        """
                    )
                    connection.commit()
                    self.provider.read_recent("synthetic-account-demo", "conv_group", 100, snapshot)
            self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)
        finally:
            connection.close()

    def test_multi_shard_same_second_order_uses_sort_seq(self):
        with self.provider.snapshot() as snapshot:
            page = self.provider.read_recent("synthetic-account-demo", "conv_group", 100, snapshot)
        source_ids = [message.source_message_id for message in page.messages]
        self.assertLess(source_ids.index("source-msg-001"), source_ids.index("source-msg-005"))

    def test_full_same_second_tie_uses_rowid_then_source_id(self):
        with closing(sqlite3.connect(self.root / "messages-2.db")) as connection:
            template = connection.execute(
                "SELECT * FROM messages WHERE source_message_id = 'source-msg-005'"
            ).fetchone()
            assert template is not None
            columns = [item[1] for item in connection.execute("PRAGMA table_info(messages)")]
            for source_id in ("source-msg-aa", "source-msg-zz"):
                values = dict(zip(columns, template, strict=True))
                values.update(
                    {
                        "source_message_id": source_id,
                        "raw_content": source_id,
                        "sort_seq": 30,
                        "source_rowid": 99,
                    }
                )
                connection.execute(
                    f"INSERT INTO messages({','.join(columns)}) "
                    f"VALUES ({','.join('?' for _ in columns)})",
                    tuple(values[column] for column in columns),
                )
            connection.commit()
        with self.provider.snapshot() as snapshot:
            page = self.provider.read_recent("synthetic-account-demo", "conv_group", 100, snapshot)
        source_ids = [message.source_message_id for message in page.messages]
        self.assertLess(source_ids.index("source-msg-aa"), source_ids.index("source-msg-zz"))

    def test_identical_cross_shard_overlap_is_deduplicated_before_paging(self):
        self._copy_message_to_second_shard("source-msg-001", replacements={"source_rowid": 101})
        with self.provider.snapshot() as snapshot:
            page = self.provider.read_recent("synthetic-account-demo", "conv_group", 100, snapshot)
        source_ids = [message.source_message_id for message in page.messages]
        self.assertEqual(source_ids.count("source-msg-001"), 1)

    def test_conflicting_cross_shard_duplicate_fails_closed(self):
        self._copy_message_to_second_shard(
            "source-msg-001",
            replacements={"source_rowid": 101, "raw_content": "conflicting body"},
        )
        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot():
                pass
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)
        self.assertIn(
            "duplicate_message_identity_conflict",
            caught.exception.details["warning_codes"],
        )

    def test_conflicting_outgoing_sender_evidence_fails_closed(self):
        with closing(sqlite3.connect(self.root / "messages-1.db")) as connection:
            connection.execute(
                """
                UPDATE messages SET sender_internal_id = 'wxid_demo_member'
                WHERE source_message_id = 'source-msg-003'
                """
            )
            connection.commit()
        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                self.provider.read_recent("synthetic-account-demo", "conv_group", 100, snapshot)
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_MESSAGE_DECODE_FAILED)

    def test_outgoing_group_and_direct_messages_resolve_to_account_self(self):
        with self.provider.snapshot() as snapshot:
            group = self.provider.read_recent("synthetic-account-demo", "conv_group", 100, snapshot)
            direct = self.provider.read_recent(
                "synthetic-account-demo", "conv_direct", 100, snapshot
            )
        group_outgoing = next(
            item for item in group.messages if item.source_message_id == "source-msg-003"
        )
        direct_outgoing = next(
            item for item in direct.messages if item.source_message_id == "source-msg-008"
        )
        for message in (group_outgoing, direct_outgoing):
            self.assertTrue(message.is_outgoing)
            self.assertEqual(message.sender_keys[0].value, "wxid_demo_owner")
            self.assertTrue(message.sender_keys[0].principal_eligible)

    def test_non_contact_sender_is_discovered_from_message_envelope(self):
        with self.provider.snapshot() as snapshot:
            participants = self.provider.list_participants(
                "synthetic-account-demo", "conv_group", snapshot
            )
        outsider = [
            item
            for item in participants
            if any(key.value == "wxid_outsider" for key in item.identity_keys)
        ]
        self.assertEqual(len(outsider), 1)
        self.assertEqual(outsider[0].resolution_state, "stable")

    def test_source_path_move_does_not_change_account_identity(self):
        moved = Path(self.temp.name) / "moved-source"
        shutil.copytree(self.root, moved)
        moved_provider = DirectWeChatSourceProvider(moved)
        with self.provider.snapshot() as first_snapshot:
            first = self.provider.list_accounts(first_snapshot)[0]
        with moved_provider.snapshot() as second_snapshot:
            second = moved_provider.list_accounts(second_snapshot)[0]
        self.assertEqual(first.source_account_key, second.source_account_key)
        self.assertNotEqual(self.provider.source_root, moved_provider.source_root)
        repository = WindowRepository(
            WindowDB(Path(self.temp.name) / "path-move-state" / "window.db")
        )
        first_id = repository.upsert_account(first, first_snapshot.fresh_as_of)
        second_id = repository.upsert_account(second, second_snapshot.fresh_as_of)
        self.assertEqual(first_id, second_id)
        with repository.database.connection() as connection:
            account_count = int(connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0])
        self.assertEqual(account_count, 1)

    def test_non_synthetic_root_is_not_admitted(self):
        other = Path(self.temp.name) / "other"
        other.mkdir()
        provider = DirectWeChatSourceProvider(other)
        self.assertEqual(provider.health().source_state, "not_configured")
        with self.assertRaises(SightglassError) as caught:
            with provider.snapshot():
                pass
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_NOT_CONFIGURED)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import os
import sqlite3
import tempfile
import time
import unittest
from datetime import UTC, datetime

from sightglass.contracts.common import render_in_timezone, to_utc_iso
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.identity import SourceIdentityKey
from sightglass.contracts.messages import SourceMessage
from sightglass.model.db import WindowDB
from sightglass.model.schema import SCHEMA_VERSION
from sightglass.source.identity import SignedTokenCodec, load_or_create_token_secret
from sightglass.source.parser import parse_message


class ContractTests(unittest.TestCase):
    def test_explicit_time_contract_rejects_naive_values(self):
        with self.assertRaises(ValueError):
            to_utc_iso("2026-09-13T09:00:00")
        self.assertEqual(
            to_utc_iso("2026-09-13T17:00:00+08:00"),
            "2026-09-13T09:00:00.000000+00:00",
        )

    def test_reader_timezone_is_independent_from_process_timezone(self):
        original = os.environ.get("TZ")
        try:
            os.environ["TZ"] = "America/Los_Angeles"
            if hasattr(time, "tzset"):
                time.tzset()
            first = render_in_timezone("2026-09-13T16:30:00+00:00", "Asia/Singapore")
            os.environ["TZ"] = "Europe/London"
            if hasattr(time, "tzset"):
                time.tzset()
            second = render_in_timezone("2026-09-13T16:30:00+00:00", "Asia/Singapore")
        finally:
            if original is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = original
            if hasattr(time, "tzset"):
                time.tzset()
        self.assertEqual(first, second)
        self.assertTrue(first.startswith("2026-09-14T00:30:00+08:00"))

    def test_anchor_hmac_rejects_tampering(self):
        codec = SignedTokenCodec(b"0123456789abcdef0123456789abcdef")
        token = codec.encode({"conversation_id": "wxconv_a", "message_id": "wxmsg_a"})
        body, signature = token.split(".")
        tampered = ("A" if body[0] != "A" else "B") + body[1:] + "." + signature
        with self.assertRaises(SightglassError) as caught:
            codec.decode(tampered)
        self.assertEqual(caught.exception.code, ErrorCode.CURSOR_INVALID)

    def test_per_install_token_secret_is_private_stable_and_not_global(self):
        with tempfile.TemporaryDirectory() as temporary:
            first_path = os.path.join(temporary, "first", "window.db")
            second_path = os.path.join(temporary, "second", "window.db")
            WindowDB(first_path)
            WindowDB(second_path)
            first = load_or_create_token_secret(first_path)
            self.assertEqual(load_or_create_token_secret(first_path), first)
            self.assertNotEqual(load_or_create_token_secret(second_path), first)
            secret_path = os.path.join(os.path.dirname(first_path), "token-secret")
            self.assertEqual(os.stat(secret_path).st_mode & 0o777, 0o600)

    def test_group_sender_prefix_is_removed_without_touching_body(self):
        message = SourceMessage(
            source_message_id="source-1",
            source_conversation_id="group-1",
            conversation_kind="group",
            source_time_raw="1",
            sent_at_utc="2026-09-13T09:00:00+00:00",
            observed_at_utc="2026-09-13T10:00:00+00:00",
            sort_seq=1,
            source_rowid=1,
            wechat_type=1,
            raw_content="wxid_sender:\n  exact  text\n",
            is_outgoing=False,
            source_generation_id="generation",
            logical_shard_key="shard",
            sender_keys=(
                SourceIdentityKey(
                    "internal_username",
                    "wxid_sender",
                    "stable",
                    True,
                    "test",
                ),
            ),
        )
        self.assertEqual(parse_message(message).text, "  exact  text\n")

    def test_unknown_message_type_is_retained(self):
        message = SourceMessage(
            source_message_id="source-unknown",
            source_conversation_id="group-1",
            conversation_kind="group",
            source_time_raw="1",
            sent_at_utc="2026-09-13T09:00:00+00:00",
            observed_at_utc="2026-09-13T10:00:00+00:00",
            sort_seq=1,
            source_rowid=1,
            wechat_type=999,
            raw_content="opaque",
            is_outgoing=False,
            source_generation_id="generation",
            logical_shard_key="shard",
        )
        parsed = parse_message(message)
        self.assertEqual(parsed.kind, "unknown")
        self.assertEqual(parsed.text, "[暂不支持的消息类型]")
        self.assertEqual(parsed.structured["wechat_type"], 999)

    def test_error_contract_is_versioned_and_privacy_safe(self):
        value = SightglassError(
            ErrorCode.SOURCE_INCOMPLETE,
            details={"warning_codes": ["source_shard_missing"]},
        ).as_dict()
        self.assertEqual(value["schema"], "sightglass.error.v1")
        self.assertEqual(value["code"], "SOURCE_INCOMPLETE")
        self.assertNotIn("/Users/", str(value))


class WindowSchemaTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.temp.name, "private", "window.db")
        self.database = WindowDB(self.path)

    def tearDown(self):
        self.temp.cleanup()

    def test_window_db_current_schema_permissions_and_pragmas(self):
        self.assertEqual(self.database.schema_version, SCHEMA_VERSION)
        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)
        self.assertEqual(os.stat(os.path.dirname(self.path)).st_mode & 0o777, 0o700)
        with self.database.connection() as connection:
            self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
            self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")

    def test_existing_non_private_parent_is_rejected_without_chmod(self):
        public_parent = os.path.join(self.temp.name, "public")
        os.mkdir(public_parent, mode=0o755)
        os.chmod(public_parent, 0o755)
        with self.assertRaises(RuntimeError):
            WindowDB(os.path.join(public_parent, "window.db"))
        self.assertEqual(os.stat(public_parent).st_mode & 0o777, 0o755)

    def test_nullable_source_key_scope_uses_partial_unique_indexes(self):
        now = datetime.now(UTC).isoformat()
        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO accounts(
                    account_id, source_namespace, source_account_key,
                    identity_confidence, reader_timezone, current_display_name,
                    first_seen_at, last_seen_at, active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                ("a", "ns", "key", "exact", "UTC", "A", now, now),
            )
            for conv in ("c1", "c2"):
                connection.execute(
                    """
                    INSERT INTO conversations(
                        conversation_id, account_id, source_conversation_id,
                        kind, current_title, first_seen_at, last_seen_at,
                        last_message_at, visibility_state, roster_complete
                    ) VALUES (?, 'a', ?, 'group', ?, ?, ?, NULL, 'active', 1)
                    """,
                    (conv, conv, conv, now, now),
                )
            for person in ("p1", "p2"):
                connection.execute(
                    """
                    INSERT INTO participants
                    VALUES (?, 'a', NULL, 0, 'person', 'stable', 'exact', ?, ?)
                    """,
                    (person, now, now),
                )
            base = ("a", "internal_username", "same", "stable", 1, "test", now, now, 1)
            connection.execute(
                """
                INSERT INTO participant_source_keys
                VALUES ('k1', 'p1', ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)
                """,
                base,
            )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO participant_source_keys
                    VALUES ('k2', 'p2', ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)
                    """,
                    base,
                )
        with self.database.transaction() as connection:
            scoped = ("a", "conversation_sender_id", "local", "stable", 0, "test", now, now, 1)
            connection.execute(
                """
                INSERT INTO participant_source_keys
                VALUES ('s1', 'p1', ?, ?, ?, 'c1', ?, ?, ?, ?, ?, ?)
                """,
                scoped,
            )
            connection.execute(
                """
                INSERT INTO participant_source_keys
                VALUES ('s2', 'p2', ?, ?, ?, 'c2', ?, ?, ?, ?, ?, ?)
                """,
                scoped,
            )

    def test_pending_delivery_is_unique_per_scope_only_while_pending(self):
        indexes = {}
        with self.database.connection() as connection:
            for row in connection.execute("PRAGMA index_list(reader_deliveries)"):
                indexes[str(row[1])] = bool(row[4])
        self.assertTrue(indexes["reader_deliveries_one_pending"])


if __name__ == "__main__":
    unittest.main()

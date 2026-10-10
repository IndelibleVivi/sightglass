from __future__ import annotations

import hashlib
import importlib
import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

import zstandard
from mcp.types import AudioContent, EmbeddedResource, ImageContent, TextResourceContents

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.identity import SourceParticipantFilter
from sightglass.mcp.tools import ReaderTools
from sightglass.model.db import WindowDB
from sightglass.model.repositories import WindowRepository
from sightglass.operations import operation_expired
from sightglass.policy.readers import ReaderContext, ReaderPolicy
from sightglass.reader.service import ReaderService
from sightglass.runtime.source_worker import SourceWorker
from sightglass.source.base import SourceScope
from sightglass.source.identity import SignedTokenCodec, opaque_id
from sightglass.source.macos_wechat.config import MacOSWeChatSettings
from sightglass.source.macos_wechat.discovery import WeChatCandidate
from sightglass.source.macos_wechat.keys import (
    encode_key_map,
    image_decoder_keychain_account,
    import_verified_key_file,
    new_source_account_binding_id,
    source_account_key,
)
from sightglass.source.macos_wechat.provider import MacOSWeChatSourceProvider
from sightglass.source.synthetic import SYNTHETIC_IMAGE_KEY, _png_bytes, _v2_image_bytes
from tests.fixtures.factory import enable_keep_residency


def _sqlcipher() -> Any:
    try:
        return importlib.import_module("sqlcipher3.dbapi2")
    except ImportError as exc:  # pragma: no cover - platform dependency absence
        raise unittest.SkipTest("sqlcipher3 is not installed") from exc


class NativeSourceProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / "db_storage"
        (self.source / "contact").mkdir(parents=True)
        (self.source / "session").mkdir()
        (self.source / "message").mkdir()
        self.keys = {
            "contact/contact.db": "11" * 32,
            "session/session.db": "22" * 32,
            "message/message_0.db": "33" * 32,
            "message/media_0.db": "44" * 32,
        }
        self.conversation = "synthetic-contact"
        self._create_databases()
        self.key_file = self.root / "keys.json"
        self.key_file.write_text(
            json.dumps({name: {"enc_key": key} for name, key in self.keys.items()}),
            encoding="utf-8",
        )
        imported = import_verified_key_file(self.source, self.key_file)
        self.assertEqual(set(imported), set(self.keys))
        self.imported_keys = imported
        self.account_binding_id = new_source_account_binding_id()
        self.account_key = source_account_key(self.account_binding_id)
        self.settings_path = self.root / "private" / "source.json"
        self.settings = MacOSWeChatSettings(
            instance_id="wxsrc_fixture",
            source_root=self.source.resolve(),
            keychain_account="source.fixture.database-keys",
            source_account_binding_id=self.account_binding_id,
            source_account_key=self.account_key,
            bundle_id="com.tencent.xinWeChat",
            version="4.1.13",
            build="269602",
            architecture="arm64",
            profile_id="fixture-profile",
        )
        self.settings.save(self.settings_path)
        self.candidate = WeChatCandidate(
            candidate_id="wxsrc_fixture",
            app_path=Path("/Applications/WeChat.app"),
            source_root=self.source.resolve(),
            bundle_id=self.settings.bundle_id,
            version=self.settings.version,
            build=self.settings.build,
            architecture=self.settings.architecture,
            running=True,
            profile_id="fixture-profile",
        )
        self.provider = MacOSWeChatSourceProvider(
            self.settings_path,
            secret_loader=lambda _account: encode_key_map(imported),
            candidate_discovery=lambda: (self.candidate,),
        )

    def tearDown(self) -> None:
        self.provider.close()
        self.temp.cleanup()

    def _connect_new(self, relative: str) -> Any:
        sqlite = _sqlcipher()
        path = self.source / relative
        connection = sqlite.connect(path)
        connection.execute(f'''PRAGMA key = "x'{self.keys[relative]}'"''')
        return connection

    def _create_databases(self) -> None:
        contact = self._connect_new("contact/contact.db")
        try:
            contact.execute("CREATE TABLE contact(username TEXT, nick_name TEXT, remark TEXT)")
            contact.execute(
                "INSERT INTO contact VALUES (?, ?, ?)",
                (self.conversation, "Fixture Contact", "Fixture Remark"),
            )
            contact.commit()
        finally:
            contact.close()

        session = self._connect_new("session/session.db")
        try:
            session.execute(
                "CREATE TABLE SessionTable("
                "username TEXT, unread_count INTEGER, last_timestamp INTEGER)"
            )
            session.execute(
                "INSERT INTO SessionTable VALUES (?, ?, ?)",
                (self.conversation, 0, 1_725_000_002),
            )
            session.commit()
        finally:
            session.close()

        message = self._connect_new("message/message_0.db")
        table = "Msg_" + hashlib.md5(self.conversation.encode()).hexdigest()
        try:
            message.execute(
                f"""
                CREATE TABLE [{table}](
                    local_id INTEGER,
                    server_id INTEGER,
                    local_type INTEGER,
                    sort_seq INTEGER,
                    real_sender_id INTEGER,
                    create_time INTEGER,
                    status INTEGER,
                    message_content BLOB,
                    WCDB_CT_message_content INTEGER,
                    packed_info_data BLOB
                )
                """
            )
            message.execute(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (1, 101, 1, 1, 0, 1_725_000_001, 0, "first message", 0, None),
            )
            compressed = zstandard.ZstdCompressor().compress(b"latest message")
            message.execute(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (2, 102, 1, 2, 0, 1_725_000_002, 2, compressed, 4, None),
            )
            message.commit()
        finally:
            message.close()

        media = self._connect_new("message/media_0.db")
        try:
            media.execute("CREATE TABLE Name2Id(user_name TEXT UNIQUE)")
            media.execute(
                "CREATE TABLE VoiceInfo("
                "chat_name_id INTEGER, local_id INTEGER, svr_id INTEGER, "
                "create_time INTEGER, voice_data BLOB, data_index INTEGER)"
            )
            media.commit()
        finally:
            media.close()
        for relative in self.keys:
            os.chmod(self.source / relative, 0o600)

    @staticmethod
    def _table_name(conversation: str) -> str:
        return "Msg_" + hashlib.md5(conversation.encode()).hexdigest()

    def _add_contact_history_without_session(self, conversation: str) -> None:
        contact = self._connect_new("contact/contact.db")
        try:
            contact.execute(
                "INSERT INTO contact VALUES (?, ?, ?)",
                (conversation, "Archived Contact", ""),
            )
            contact.commit()
        finally:
            contact.close()
        message = self._connect_new("message/message_0.db")
        try:
            table = self._table_name(conversation)
            message.execute(
                f"""
                CREATE TABLE [{table}](
                    local_id INTEGER,
                    server_id INTEGER,
                    local_type INTEGER,
                    sort_seq INTEGER,
                    real_sender_id INTEGER,
                    create_time INTEGER,
                    status INTEGER,
                    message_content BLOB,
                    WCDB_CT_message_content INTEGER,
                    packed_info_data BLOB
                )
                """
            )
            message.execute(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (1, 901, 1, 1, 0, 1_724_000_001, 0, "archived history", 0, None),
            )
            message.commit()
        finally:
            message.close()

    @contextmanager
    def _count_shard_fetches(self) -> Any:
        """Count native payload and position rows fetched per statement.

        Wraps ``_connect`` so every ``execute`` records how many rows the query
        returned. The positions query (``source_rowid, create_time ... LIMIT``) and the
        payload query (``message_row.rowid AS source_rowid``) are the two eager shard
        fetches whose size the lazy merge bound depends on. It also records how many
        *statements* each fetch issued, so a caller can prove a bounded position batch
        replaced a full shard sort per selected row instead of only counting rows.
        """

        counts = {
            "positions": 0,
            "payload": 0,
            "position_queries": 0,
            "payload_queries": 0,
            "position_query_max": 0,
        }

        class _Cursor:
            def __init__(self, rows: list[Any]) -> None:
                self._rows = rows

            def fetchall(self) -> list[Any]:
                return self._rows

            def fetchone(self) -> Any:
                return self._rows[0] if self._rows else None

            def __iter__(self) -> Any:
                return iter(self._rows)

        class _Connection:
            def __init__(self, connection: Any) -> None:
                self._connection = connection

            def __getattr__(self, name: str) -> Any:
                return getattr(self._connection, name)

            def execute(self, sql: str, *args: Any, **kwargs: Any) -> _Cursor:
                rows = self._connection.execute(sql, *args, **kwargs).fetchall()
                normalized = " ".join(sql.split())
                if "source_rowid, create_time" in normalized and "LIMIT" in normalized:
                    counts["positions"] += len(rows)
                    counts["position_queries"] += 1
                    counts["position_query_max"] = max(counts["position_query_max"], len(rows))
                elif "message_row.rowid AS source_rowid" in normalized:
                    counts["payload"] += len(rows)
                    counts["payload_queries"] += 1
                return _Cursor(rows)

        real_connect = self.provider._connect

        @contextmanager
        def counting_connect(relative: str) -> Any:
            with real_connect(relative) as connection:
                yield _Connection(connection)

        with mock.patch.object(self.provider, "_connect", side_effect=counting_connect):
            yield counts

    def _add_message_shard(
        self,
        *,
        suffix: str,
        messages: list[tuple[int, int, int]],
    ) -> str:
        """Create an additional encrypted message shard holding the conversation table.

        ``messages`` are ``(local_id, create_time, sort_seq)`` tuples whose payload text
        is derived deterministically, so a multi-shard history stays unmistakably
        synthetic. The shard key is enrolled exactly like the primary fixture.
        """

        relative = f"message/message_{suffix}.db"
        key = hashlib.sha256(f"synthetic-shard-{suffix}".encode()).hexdigest()
        assert len(key) == 64
        self.keys[relative] = key
        self.provider._keys[relative] = {"enc_key": key}  # noqa: SLF001 - fixture enrollment
        connection = self._connect_new(relative)
        try:
            connection.execute("CREATE TABLE shard_probe(value INTEGER)")
            connection.execute("INSERT INTO shard_probe VALUES (1)")
            table = self._table_name(self.conversation)
            connection.execute(
                f"""
                CREATE TABLE [{table}](
                    local_id INTEGER,
                    server_id INTEGER,
                    local_type INTEGER,
                    sort_seq INTEGER,
                    real_sender_id INTEGER,
                    create_time INTEGER,
                    status INTEGER,
                    message_content BLOB,
                    WCDB_CT_message_content INTEGER,
                    packed_info_data BLOB
                )
                """
            )
            connection.executemany(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    (
                        local_id,
                        1000 + local_id,
                        1,
                        sort_seq,
                        0,
                        create_time,
                        0,
                        f"shard {suffix} message {local_id}",
                        0,
                        None,
                    )
                    for local_id, create_time, sort_seq in messages
                ),
            )
            connection.commit()
        finally:
            connection.close()
        os.chmod(self.source / relative, 0o600)
        return relative

    def _add_probe_message_shard(self, suffix: str) -> str:
        """Create an enrolled shard that cannot serve the fixture conversation."""

        relative = f"message/message_{suffix}.db"
        key = hashlib.sha256(f"synthetic-probe-{suffix}".encode()).hexdigest()
        self.keys[relative] = key
        self.provider._keys[relative] = {"enc_key": key}  # noqa: SLF001 - fixture enrollment
        connection = self._connect_new(relative)
        try:
            connection.execute("CREATE TABLE shard_probe(value INTEGER)")
            connection.execute("INSERT INTO shard_probe VALUES (1)")
            connection.commit()
        finally:
            connection.close()
        os.chmod(self.source / relative, 0o600)
        return relative

    def _add_conflicting_conversation_shard(
        self,
        *,
        suffix: str,
        conversation: str,
        server_id: int,
        create_time: int = 1_725_000_101,
    ) -> str:
        """Add a second shard whose row duplicates ``server_id`` with different content.

        The native reader fails the affected conversation closed with
        ``duplicate_message_identity_conflict`` instead of merging or dropping it, which
        is exactly the SG-039 degraded state the projection gate must tolerate.
        """

        relative = f"message/message_{suffix}.db"
        key = hashlib.sha256(f"synthetic-conflict-{suffix}".encode()).hexdigest()
        self.keys[relative] = key
        self.provider._keys[relative] = {"enc_key": key}  # noqa: SLF001 - fixture enrollment
        connection = self._connect_new(relative)
        try:
            connection.execute("CREATE TABLE shard_probe(value INTEGER)")
            connection.execute("INSERT INTO shard_probe VALUES (1)")
            table = self._table_name(conversation)
            connection.execute(
                f"""
                CREATE TABLE [{table}](
                    local_id INTEGER,
                    server_id INTEGER,
                    local_type INTEGER,
                    sort_seq INTEGER,
                    real_sender_id INTEGER,
                    create_time INTEGER,
                    status INTEGER,
                    message_content BLOB,
                    WCDB_CT_message_content INTEGER,
                    packed_info_data BLOB
                )
                """
            )
            connection.execute(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    900,
                    server_id,
                    1,
                    900,
                    0,
                    create_time,
                    0,
                    "conflicting shard copy",
                    0,
                    None,
                ),
            )
            connection.commit()
        finally:
            connection.close()
        os.chmod(self.source / relative, 0o600)
        return relative

    def _insert_message(
        self,
        *,
        relative: str = "message/message_0.db",
        local_id: int,
        server_id: int,
        sort_seq: int,
        create_time: int,
        content: str,
        status: int = 0,
        local_type: int = 1,
        packed_info_data: bytes | None = None,
    ) -> None:
        message = self._connect_new(relative)
        try:
            message.execute(
                f"INSERT INTO [{self._table_name(self.conversation)}] "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    local_id,
                    server_id,
                    local_type,
                    sort_seq,
                    0,
                    create_time,
                    status,
                    content,
                    0,
                    packed_info_data,
                ),
            )
            message.commit()
        finally:
            message.close()

    def _insert_voice_payload(
        self,
        *,
        local_id: int,
        server_id: int,
        create_time: int,
        payload: bytes,
    ) -> None:
        media = self._connect_new("message/media_0.db")
        try:
            media.execute(
                "INSERT OR IGNORE INTO Name2Id(user_name) VALUES (?)",
                (self.conversation,),
            )
            chat_name_id = int(
                media.execute(
                    "SELECT rowid FROM Name2Id WHERE user_name = ?",
                    (self.conversation,),
                ).fetchone()[0]
            )
            media.execute(
                "INSERT INTO VoiceInfo VALUES (?, ?, ?, ?, ?, ?)",
                (chat_name_id, local_id, server_id, create_time, payload, 0),
            )
            media.commit()
        finally:
            media.close()

    def _reader_tools(self, *source_conversation_ids: str) -> ReaderTools:
        external_account = opaque_id("wxacct", self.account_key)
        selected_source_ids = source_conversation_ids or (self.conversation,)
        external_conversations = frozenset(
            opaque_id("wxconv", external_account, source_id)
            for source_id in selected_source_ids
        )
        database = WindowDB(self.root / "state" / "window.db")
        enable_keep_residency(database)
        repository = WindowRepository(database)
        reader = ReaderContext(
            "demo_reader",
            "Demo Reader",
            ReaderPolicy(
                mode="allowlist",
                allowed_conversation_ids=external_conversations,
                identity_debug=True,
            ),
        )
        service = ReaderService(
            self.provider,
            repository,
            reader,
            SignedTokenCodec(b"native-fixture-reader-secret-32b"),
        )
        return ReaderTools(service)

    def _reader_tools_all_permitted(self) -> ReaderTools:
        """Reader tools whose policy permits every conversation.

        Used where a persisted synthetic conversation must be *policy-eligible* so a
        test can prove it is still excluded by the current-catalog gate rather than by
        the policy filter.
        """

        database = WindowDB(self.root / "state" / "window.db")
        enable_keep_residency(database)
        repository = WindowRepository(database)
        reader = ReaderContext(
            "demo_reader",
            "Demo Reader",
            ReaderPolicy(mode="all_except_denylist", identity_debug=True),
        )
        service = ReaderService(
            self.provider,
            repository,
            reader,
            SignedTokenCodec(b"native-fixture-reader-secret-32b"),
        )
        return ReaderTools(service)

    def _add_group_sender_fixture(self, suffix: str = "") -> tuple[str, str]:
        group = f"synthetic-group{suffix}@chatroom"
        member = f"synthetic-group-member{suffix}"
        source_self = f"synthetic-source-self{suffix}"
        contact = self._connect_new("contact/contact.db")
        try:
            contact.executemany(
                "INSERT INTO contact VALUES (?, ?, ?)",
                (
                    (group, "Fixture Group", ""),
                    (member, "Fixture Member", "Fixture Member Remark"),
                ),
            )
            contact.commit()
        finally:
            contact.close()
        session = self._connect_new("session/session.db")
        try:
            session.execute(
                "INSERT INTO SessionTable VALUES (?, ?, ?)",
                (group, 0, 1_725_000_103),
            )
            session.commit()
        finally:
            session.close()
        message = self._connect_new("message/message_0.db")
        try:
            message.execute(
                "CREATE TABLE IF NOT EXISTS Name2Id(user_name TEXT, is_session INTEGER)"
            )
            message.executemany(
                "INSERT INTO Name2Id(user_name, is_session) VALUES (?, 0)",
                ((member,), (source_self,)),
            )
            member_rowid = int(
                message.execute(
                    "SELECT rowid FROM Name2Id WHERE user_name = ?", (member,)
                ).fetchone()[0]
            )
            self_rowid = int(
                message.execute(
                    "SELECT rowid FROM Name2Id WHERE user_name = ?", (source_self,)
                ).fetchone()[0]
            )
            table = self._table_name(group)
            message.execute(
                f"""
                CREATE TABLE [{table}](
                    local_id INTEGER,
                    server_id INTEGER,
                    local_type INTEGER,
                    sort_seq INTEGER,
                    real_sender_id INTEGER,
                    create_time INTEGER,
                    status INTEGER,
                    message_content BLOB,
                    WCDB_CT_message_content INTEGER,
                    packed_info_data BLOB
                )
                """
            )
            message.executemany(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    (
                        101,
                        201,
                        1,
                        101,
                        member_rowid,
                        1_725_000_101,
                        3,
                        f"{member}:\nmember status three",
                        0,
                        None,
                    ),
                    (
                        102,
                        202,
                        1,
                        102,
                        self_rowid,
                        1_725_000_102,
                        3,
                        "self status three",
                        0,
                        None,
                    ),
                    (
                        103,
                        203,
                        1,
                        103,
                        member_rowid,
                        1_725_000_103,
                        4,
                        f"{member}:\nmember status four",
                        0,
                        None,
                    ),
                ),
            )
            message.commit()
        finally:
            message.close()
        return group, member

    def test_live_descriptor_health_and_latest_message_read(self) -> None:
        health = self.provider.health()
        self.assertTrue(health.complete)
        self.assertEqual(health.shard_counts["present"], 3)
        self.assertEqual(self.provider.descriptor.source_mode, "live")
        self.assertTrue(self.provider.descriptor.supports_resources)
        self.assertNotIn(str(self.source), json.dumps(health.as_dict()))

        with self.provider.snapshot() as snapshot:
            accounts = self.provider.list_accounts(snapshot)
            conversations = self.provider.list_conversations(
                accounts[0].source_account_key, snapshot
            )
            page = self.provider.read_recent(
                accounts[0].source_account_key,
                conversations[0].source_conversation_id,
                2,
                snapshot,
            )
            self.assertEqual(
                [item.raw_content for item in page.messages],
                [
                    "first message",
                    "latest message",
                ],
            )
            self.assertTrue(page.messages[-1].is_outgoing)
            self.assertIsNone(page.messages[0].sender_surface_label)
            self.assertEqual(page.messages[0].sender_labels[0].label, "Fixture Remark")
            self.assertEqual(page.messages[0].sender_labels[0].label_kind, "contact_remark")
            self.assertEqual(page.messages[0].sender_labels[0].temporal_confidence, "current_only")
            recovered = self.provider.get_message(
                accounts[0].source_account_key,
                page.messages[-1].source_message_id,
                snapshot,
            )
            self.assertEqual(recovered, page.messages[-1])

    def test_status_does_not_scan_the_native_conversation_catalog(self) -> None:
        tools = self._reader_tools()

        with mock.patch.object(
            self.provider,
            "list_conversations",
            side_effect=AssertionError("status must not scan the conversation catalog"),
        ):
            result = tools.wechat_status()

        self.assertEqual(result["schema"], "sightglass.status.v1")
        self.assertTrue(result["ready"])
        self.assertEqual(len(result["accounts"]), 1)
        with tools.service.repository.database.connection() as connection:
            conversation_count = int(
                connection.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]
            )
        self.assertEqual(conversation_count, 0)

    def test_known_native_message_read_does_not_rescan_the_full_catalog(self) -> None:
        tools = self._reader_tools()
        catalog = tools.wechat_find_conversations("Fixture")
        conversation_id = catalog["candidates"][0]["conversation_id"]
        before_source = tools.service.cached_status()["source"]

        with (
            mock.patch.object(
                self.provider,
                "snapshot",
                side_effect=AssertionError("known message read must use a scoped session"),
            ),
            mock.patch.object(
                self.provider,
                "session",
                wraps=self.provider.session,
            ) as scoped_session,
            mock.patch.object(
                self.provider,
                "list_conversations",
                side_effect=AssertionError("known message read must not rescan the catalog"),
            ),
            mock.patch.object(
                self.provider,
                "_conversations",
                side_effect=AssertionError("known message read must not rebuild the catalog"),
            ),
        ):
            page = tools.wechat_read_messages(
                mode="recent",
                conversation_id=conversation_id,
                projection="compact",
                limit=2,
            )

        self.assertEqual(page["schema"], "sightglass.message-batch.v1")
        self.assertEqual(len(page["messages"]), 2)
        self.assertEqual(scoped_session.call_count, 1)
        after_source = tools.service.cached_status()["source"]
        self.assertEqual(after_source["inventory_digest"], before_source["inventory_digest"])
        self.assertEqual(
            after_source["generation_set_digest"],
            before_source["generation_set_digest"],
        )

    def test_native_recent_admits_tail_and_reopens_without_source(self) -> None:
        tools = self._reader_tools()
        service = tools.service
        conversation_id = tools.wechat_find_conversations("Fixture")["candidates"][0][
            "conversation_id"
        ]
        arguments = {
            "mode": "recent",
            "conversation_id": conversation_id,
            "projection": "detail",
            "limit": 1,
        }
        first = service.read_messages(**arguments)
        target = first["messages"][0]
        state = service.repository.source_conversation_state(conversation_id)
        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state["source_inventory_epoch"], service._projection_inventory_epoch())
        self.assertEqual(state["backfill_state"], "partial")
        row = service.repository.message_position_row(target["message_id"])
        assert row is not None
        self.assertEqual(state["tail_source_message_id"], row["source_message_id"])
        with (
            mock.patch.object(
                self.provider, "snapshot", side_effect=AssertionError("tail reread opened catalog")
            ),
            mock.patch.object(
                self.provider, "session", side_effect=AssertionError("tail reread opened source")
            ),
        ):
            self.assertTrue(service.local_message_read_ready(arguments))
            repeated = service.read_messages(**arguments)
        self.assertEqual(repeated["messages"][0]["message_id"], target["message_id"])
        self.assertEqual(repeated["source_receipt"]["served_from"], "window_db")
        self.assertFalse(repeated["source_receipt"]["complete"])
        self.assertFalse(repeated["source_receipt"]["freshness"]["live_refresh_confirmed"])

    def test_historical_native_anchor_reopens_without_certifying_tail(self) -> None:
        tools = self._reader_tools()
        service = tools.service
        conversation_id = tools.wechat_find_conversations("Fixture")["candidates"][0][
            "conversation_id"
        ]
        recent = service.read_messages(
            mode="recent", conversation_id=conversation_id, projection="detail", limit=2
        )
        target = recent["messages"][0]
        with service.repository.database.transaction() as connection:
            connection.execute(
                "DELETE FROM source_conversation_state WHERE conversation_id = ?",
                (conversation_id,),
            )
            connection.execute(
                "UPDATE messages SET projection_epoch = 'old' WHERE conversation_id = ?",
                (conversation_id,),
            )
        arguments = {
            "mode": "context",
            "anchor": target["anchor"],
            "before": 0,
            "after": 0,
            "limit": 1,
            "projection": "detail",
        }
        with (
            mock.patch.object(
                self.provider, "snapshot", side_effect=AssertionError("anchor opened full snapshot")
            ),
            mock.patch.object(
                self.provider,
                "list_conversations",
                side_effect=AssertionError("anchor scanned catalog"),
            ),
            mock.patch.object(self.provider, "session", wraps=self.provider.session) as session,
        ):
            page = service.read_messages(**arguments)
        self.assertEqual(session.call_count, 1)
        self.assertEqual(
            session.call_args.args[0], SourceScope.conversation(self.account_key, self.conversation)
        )
        self.assertEqual(page["messages"][0]["message_id"], target["message_id"])
        self.assertIsNone(service.repository.source_conversation_state(conversation_id))
        context = service.repository.conversation_context(conversation_id)
        assert context is not None
        account_id = context["account_id"]
        self.assertNotIn(
            conversation_id,
            service._catalog_handled_ids(
                account_id, projection_epoch=service._projection_inventory_epoch()
            ),
        )
        self.assertFalse(
            service.local_message_read_ready({"mode": "recent", "conversation_id": conversation_id})
        )
        with (
            mock.patch.object(
                self.provider,
                "snapshot",
                side_effect=AssertionError("admitted history opened catalog"),
            ),
            mock.patch.object(
                self.provider,
                "session",
                side_effect=AssertionError("admitted history opened source"),
            ),
        ):
            self.assertTrue(service.local_message_read_ready(arguments))
            repeated = service.read_messages(**{**arguments, "before": 1, "after": 1, "limit": 3})
        self.assertEqual(
            [item["message_id"] for item in repeated["messages"]], [target["message_id"]]
        )
        self.assertFalse(repeated["page"]["has_more_after"])
        self.assertIn(
            "has_more_describes_admitted_messages", repeated["source_receipt"]["coverage"]["notes"]
        )

    def _native_anchor_needing_refresh(self) -> tuple[ReaderService, dict[str, Any], str]:
        tools = self._reader_tools()
        conversation_id = tools.wechat_find_conversations("Fixture")["candidates"][0][
            "conversation_id"
        ]
        page = tools.service.read_messages(
            mode="recent", conversation_id=conversation_id, projection="detail", limit=1
        )
        with tools.service.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE messages SET projection_epoch = 'old' WHERE conversation_id = ?",
                (conversation_id,),
            )
            connection.execute(
                "UPDATE source_conversation_state SET source_inventory_epoch = 'old' "
                "WHERE conversation_id = ?",
                (conversation_id,),
            )
        return tools.service, page["messages"][0], conversation_id

    def test_native_anchor_reconciles_sort_and_missing_target_in_selected_scope(
        self,
    ) -> None:
        service, target, conversation_id = self._native_anchor_needing_refresh()
        writer = self._connect_new("message/message_0.db")
        table = self._table_name(self.conversation)
        try:
            for epoch, change in (
                (None, f"UPDATE [{table}] SET sort_seq = sort_seq + 1 WHERE local_id = 2"),
                ("old", f"DELETE FROM [{table}] WHERE local_id = 2"),
            ):
                with self.subTest(change=change):
                    with service.repository.database.transaction() as connection:
                        connection.execute(
                            "UPDATE messages SET projection_epoch = ? WHERE message_id = ?",
                            (epoch, target["message_id"]),
                        )
                    writer.execute(change)
                    writer.commit()
                    with (
                        mock.patch.object(
                            self.provider,
                            "snapshot",
                            side_effect=AssertionError("anchor error opened full snapshot"),
                        ),
                        mock.patch.object(
                            self.provider,
                            "list_conversations",
                            side_effect=AssertionError("anchor error scanned catalog"),
                        ),
                        mock.patch.object(
                            self.provider, "session", wraps=self.provider.session
                        ) as session,
                    ):
                        with self.assertRaises(SightglassError) as caught:
                            service.read_messages(
                                mode="context", anchor=target["anchor"], before=0, after=0, limit=1
                            )
                    self.assertEqual(caught.exception.code, ErrorCode.CURSOR_STALE)
                    self.assertEqual(
                        session.call_args.args[0],
                        SourceScope.conversation(self.account_key, self.conversation),
                    )
                    row = service.repository.message_position_row(target["message_id"])
                    assert row is not None
                    self.assertEqual(row["projection_epoch"], epoch)
                    state = service.repository.source_conversation_state(conversation_id)
                    assert state is not None
                    self.assertEqual(state["source_inventory_epoch"], "old")
        finally:
            writer.close()

    def test_native_recent_tail_and_rows_roll_back_on_selected_dependency_change(self) -> None:
        service, target, conversation_id = self._native_anchor_needing_refresh()
        writer = self._connect_new("message/message_0.db")
        table = self._table_name(self.conversation)
        try:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            record_tail = service.repository.record_source_conversation_state

            def changing_tail(**kwargs: Any) -> None:
                record_tail(**kwargs)
                writer.execute(
                    f"UPDATE [{table}] SET message_content = ? WHERE local_id = 1",
                    ("synthetic changed during tail read",),
                )
                writer.commit()

            with (
                mock.patch.object(
                    self.provider,
                    "snapshot",
                    side_effect=AssertionError("recent opened catalog snapshot"),
                ),
                mock.patch.object(
                    service.repository,
                    "record_source_conversation_state",
                    side_effect=changing_tail,
                ),
            ):
                with self.assertRaises(SightglassError) as caught:
                    service.read_messages(
                        mode="recent", conversation_id=conversation_id, projection="detail", limit=2
                    )
            self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)
            state = service.repository.source_conversation_state(conversation_id)
            assert state is not None
            self.assertEqual(state["source_inventory_epoch"], "old")
            row = service.repository.message_position_row(target["message_id"])
            assert row is not None
            self.assertEqual(row["projection_epoch"], "old")
            with service.repository.database.connection() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM messages WHERE projection_epoch != 'old'"
                    ).fetchone()[0],
                    0,
                )
        finally:
            writer.close()

    def test_native_recent_records_unfiltered_tail_and_preserves_current_history_coverage(
        self,
    ) -> None:
        self._insert_message(
            local_id=3,
            server_id=103,
            sort_seq=1725000003,
            create_time=1725000003,
            content="synthetic system tail",
            local_type=10000,
        )
        tools = self._reader_tools()
        service = tools.service
        conversation_id = tools.wechat_find_conversations("Fixture")["candidates"][0][
            "conversation_id"
        ]
        page = service.read_messages(
            mode="recent",
            conversation_id=conversation_id,
            projection="detail",
            limit=2,
            system_policy="omit",
        )
        state = service.repository.source_conversation_state(conversation_id)
        assert state is not None
        with service.repository.database.connection() as connection:
            tail = connection.execute(
                "SELECT * FROM messages WHERE source_message_id = ?",
                (state["tail_source_message_id"],),
            ).fetchone()
        assert tail is not None
        self.assertEqual(tail["kind"], "system")
        self.assertNotIn(tail["message_id"], [item["message_id"] for item in page["messages"]])
        with service.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE source_conversation_state SET backfill_state = 'complete' "
                "WHERE conversation_id = ?",
                (conversation_id,),
            )
        state = service.repository.source_conversation_state(conversation_id)
        assert state is not None
        self.assertEqual(state["backfill_state"], "partial")
        self.assertFalse(state["history_complete"])
        service.read_messages(
            mode="recent",
            conversation_id=conversation_id,
            projection="detail",
            limit=1,
            refresh=True,
        )
        state = service.repository.source_conversation_state(conversation_id)
        assert state is not None
        self.assertEqual(state["backfill_state"], "partial")
        service.queue_backfill(conversation_id=conversation_id, max_messages=20)
        step = service.process_backfill_once(batch_limit=20)
        self.assertEqual(step["state"], "completed")
        self.assertEqual(step["message_count"], 1)
        state = service.repository.source_conversation_state(conversation_id)
        assert state is not None
        self.assertTrue(state["history_complete"])
        self.assertTrue(state["forward_complete"])
        self.assertEqual(state["backfill_state"], "complete")
        service.read_messages(
            mode="recent",
            conversation_id=conversation_id,
            projection="detail",
            limit=1,
            refresh=True,
        )
        state = service.repository.source_conversation_state(conversation_id)
        assert state is not None
        self.assertEqual(state["backfill_state"], "complete")
        with service.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE source_conversation_state SET source_inventory_epoch = 'old' "
                "WHERE conversation_id = ?",
                (conversation_id,),
            )
            connection.execute(
                "UPDATE messages SET projection_epoch = 'old' WHERE conversation_id = ?",
                (conversation_id,),
            )
        service.read_messages(
            mode="recent", conversation_id=conversation_id, projection="detail", limit=1
        )
        state = service.repository.source_conversation_state(conversation_id)
        assert state is not None
        self.assertEqual(state["backfill_state"], "partial")

    def test_native_empty_recent_does_not_manufacture_tail_readiness(self) -> None:
        writer = self._connect_new("message/message_0.db")
        try:
            writer.execute(f"DELETE FROM [{self._table_name(self.conversation)}]")
            writer.commit()
        finally:
            writer.close()
        tools = self._reader_tools()
        conversation_id = tools.wechat_find_conversations("Fixture")["candidates"][0][
            "conversation_id"
        ]
        page = tools.service.read_messages(
            mode="recent", conversation_id=conversation_id, projection="detail", limit=1
        )
        self.assertEqual(page["messages"], [])
        self.assertIsNone(tools.service.repository.source_conversation_state(conversation_id))

    def test_native_context_refresh_admits_missing_neighbors_in_selected_scope(self) -> None:
        for index in range(3, 32):
            self._insert_message(
                local_id=index,
                server_id=100 + index,
                sort_seq=1725000000 + index,
                create_time=1725000000 + index,
                content=f"synthetic neighbor {index}",
            )
        tools = self._reader_tools()
        service = tools.service
        conversation_id = tools.wechat_find_conversations("Fixture")["candidates"][0][
            "conversation_id"
        ]
        first = service.read_messages(
            mode="range",
            conversation_id=conversation_id,
            projection="detail",
            limit=1,
            time_after=datetime.fromtimestamp(1725000016, UTC).isoformat(),
            time_before=datetime.fromtimestamp(1725000017, UTC).isoformat(),
        )
        target = first["messages"][0]
        arguments = {
            "mode": "context",
            "anchor": target["anchor"],
            "before": 15,
            "after": 15,
            "limit": 31,
            "projection": "detail",
        }
        with mock.patch.object(
            self.provider, "session", side_effect=AssertionError("default opened source")
        ):
            partial = service.read_messages(**arguments)
        self.assertEqual(len(partial["messages"]), 1)
        self.assertFalse(partial["page"]["has_more_before"])
        self.assertTrue(service.local_message_read_ready(arguments))
        self.assertFalse(service.local_message_read_ready({**arguments, "refresh": True}))
        with (
            mock.patch.object(
                self.provider,
                "snapshot",
                side_effect=AssertionError("refresh opened global snapshot"),
            ),
            mock.patch.object(
                self.provider,
                "list_conversations",
                side_effect=AssertionError("refresh scanned catalog"),
            ),
            mock.patch.object(self.provider, "session", wraps=self.provider.session) as session,
        ):
            refreshed = service.read_messages(**arguments, refresh=True)
        self.assertEqual(session.call_count, 1)
        self.assertEqual(
            session.call_args.args[0], SourceScope.conversation(self.account_key, self.conversation)
        )
        self.assertEqual(len(refreshed["messages"]), 31)
        self.assertIsNone(service.repository.source_conversation_state(conversation_id))
        self.assertNotEqual(refreshed["source_receipt"].get("served_from"), "window_db")
        with mock.patch.object(
            self.provider, "session", side_effect=AssertionError("refreshed context opened source")
        ):
            repeated = service.read_messages(**arguments)
        self.assertEqual(
            [item["message_id"] for item in repeated["messages"]],
            [item["message_id"] for item in refreshed["messages"]],
        )
        self.assertFalse(repeated["source_receipt"]["freshness"]["live_refresh_confirmed"])

    def test_native_context_refresh_fails_strictly_and_preserves_admitted_page(self) -> None:
        tools = self._reader_tools()
        service = tools.service
        conversation_id = tools.wechat_find_conversations("Fixture")["candidates"][0][
            "conversation_id"
        ]
        target = service.read_messages(
            mode="recent", conversation_id=conversation_id, projection="detail", limit=1
        )["messages"][0]
        arguments = {
            "mode": "context",
            "anchor": target["anchor"],
            "before": 1,
            "after": 0,
            "limit": 2,
            "projection": "detail",
        }
        with mock.patch.object(
            self.provider, "session", side_effect=SightglassError(ErrorCode.SOURCE_INCOMPLETE)
        ):
            with self.assertRaises(SightglassError) as caught:
                service.read_messages(**arguments, refresh=True)
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)
        self.assertEqual(len(service.read_messages(**arguments)["messages"]), 1)
        writer = self._connect_new("message/message_0.db")
        table = self._table_name(self.conversation)
        try:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            ingest = service._ingest_prepared_messages

            def changing_admission(*args: Any, **kwargs: Any) -> Any:
                admitted = ingest(*args, **kwargs)
                writer.execute(
                    f"UPDATE [{table}] SET message_content = ? WHERE local_id = 1",
                    ("synthetic mutation after context admission",),
                )
                writer.commit()
                return admitted

            with mock.patch.object(
                service, "_ingest_prepared_messages", side_effect=changing_admission
            ):
                with self.assertRaises(SightglassError) as caught:
                    service.read_messages(**arguments, refresh=True)
            self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)
            self.assertEqual(len(service.read_messages(**arguments)["messages"]), 1)
            with service.repository.database.connection() as connection:
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0], 1
                )
            writer.execute(f"UPDATE [{table}] SET sort_seq = sort_seq + 1 WHERE local_id = 2")
            writer.commit()
            with self.assertRaises(SightglassError) as caught:
                service.read_messages(**arguments, refresh=True)
            self.assertEqual(caught.exception.code, ErrorCode.CURSOR_STALE)
        finally:
            writer.close()
        service.reader.policy = replace(service.reader.policy, allowed_conversation_ids=frozenset())
        with mock.patch.object(
            self.provider, "session", side_effect=AssertionError("denied refresh opened source")
        ):
            with self.assertRaises(SightglassError) as caught:
                service.read_messages(**arguments, refresh=True)
        self.assertEqual(caught.exception.code, ErrorCode.POLICY_DENIED)

    def test_native_sqlcipher_handles_are_reused_across_reader_threads(self) -> None:
        sqlite = _sqlcipher()
        failures: list[BaseException] = []

        def read_once() -> None:
            try:
                with self.provider.snapshot() as snapshot:
                    page = self.provider.read_recent(
                        self.account_key,
                        self.conversation,
                        2,
                        snapshot,
                    )
                self.assertEqual(len(page.messages), 2)
            except BaseException as exc:
                failures.append(exc)

        with mock.patch.object(sqlite, "connect", wraps=sqlite.connect) as connect:
            first = threading.Thread(target=read_once)
            first.start()
            first.join(timeout=5)
            self.assertFalse(first.is_alive())
            first_open_count = connect.call_count

            second = threading.Thread(target=read_once)
            second.start()
            second.join(timeout=5)
            self.assertFalse(second.is_alive())

        self.assertEqual(failures, [])
        self.assertGreater(first_open_count, 0)
        self.assertEqual(connect.call_count, first_open_count)

    def test_native_recent_query_fetches_only_the_requested_page_plus_probe(self) -> None:
        with mock.patch.object(
            self.provider,
            "_iter_shard_rows",
            wraps=self.provider._iter_shard_rows,
        ) as rows:
            with self.provider.snapshot() as snapshot:
                page = self.provider.read_recent(
                    self.account_key,
                    self.conversation,
                    3,
                    snapshot,
                )

        self.assertEqual(len(page.messages), 2)
        self.assertTrue(rows.call_args_list)
        self.assertTrue(all(call.kwargs["page_size"] == 4 for call in rows.call_args_list))

    def test_native_multishard_recent_lazily_bounds_payload_prefetch(self) -> None:
        # Six message shards (the primary fixture plus five appended), each holding
        # several rows, so an eager ``shards x page`` prefetch would materialize far more
        # payloads than the four requested rows need.
        base = 1_725_060_000
        for index in range(5):
            self._add_message_shard(
                suffix=str(index + 1),
                messages=[
                    (index * 10 + offset, base + index * 10 + offset, index * 10 + offset)
                    for offset in range(6)
                ],
            )

        with self.provider.snapshot() as snapshot:
            with self._count_shard_fetches() as counts:
                page = self.provider.read_recent(
                    self.account_key,
                    self.conversation,
                    4,
                    snapshot,
                )

        # Ordering, page size, and payload content remain exact.
        self.assertEqual(
            [message.raw_content for message in page.messages],
            [
                "shard 5 message 42",
                "shard 5 message 43",
                "shard 5 message 44",
                "shard 5 message 45",
            ],
        )

        shard_count = 6
        requested = 4
        # The eager ``shards x (requested + 1)`` bound would be 30 payload rows; the lazy
        # merge bound is O(shards + requested), i.e. each shard contributes roughly one
        # row per global winner instead of a full page up front.
        self.assertLessEqual(counts["payload"], shard_count + requested)
        self.assertLess(counts["payload"], shard_count * (requested + 1))
        # Positions are fetched as cheap bounded batches: at most one position statement
        # per shard, never one full shard sort per selected row. The old
        # ``position_limit == payload_page_size == 1`` behavior issued a fresh encrypted
        # position sort for every yielded row.
        self.assertLessEqual(counts["position_queries"], shard_count)
        self.assertLess(counts["position_queries"], shard_count + requested)
        self.assertLessEqual(counts["position_query_max"], 256)
        # Each payload page stays lazy: one point lookup per shard per global winner.
        self.assertLessEqual(counts["payload_queries"], shard_count + requested)

    def test_native_multishard_traversal_completes_past_one_position_page(self) -> None:
        # Three shards, 300 rows each: a page larger than the bounded 256-row
        # position batch must still walk every shard exactly once per batch and
        # yield the complete, exactly ordered window (proving the decoupled
        # position batch does not silently cap or drop rows past its page).
        base = 1_725_100_000
        for index in range(2):
            self._add_message_shard(
                suffix=str(index + 1),
                messages=[
                    (index * 1000 + offset, base + index * 1000 + offset, index * 1000 + offset)
                    for offset in range(300)
                ],
            )

        with self.provider.snapshot() as snapshot:
            with self._count_shard_fetches() as counts:
                page = self.provider.read_range(
                    self.account_key,
                    self.conversation,
                    after=None,
                    before=None,
                    direction="forward",
                    limit=600,
                    snapshot=snapshot,
                )

        keys = [message.sort_key.as_tuple() for message in page.messages]
        self.assertEqual(len(keys), 600)
        self.assertEqual(keys, sorted(keys))
        self.assertEqual(len(set(keys)), len(keys))
        # Each shard issues at most a bounded number of position statements (600
        # rows over a 256-row position page is three batches per shard), never one
        # statement per returned row.
        self.assertLessEqual(counts["position_queries"], 3 * 3)
        self.assertLess(counts["position_queries"], len(keys))
        self.assertLessEqual(counts["position_query_max"], 256)

    def test_native_indexed_keysets_seek_without_sorting_the_remaining_history(self) -> None:
        relative = "message/message_0.db"
        table = self._table_name(self.conversation)
        base = 1_725_150_000
        connection = self._connect_new(relative)
        try:
            connection.executemany(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    (
                        100 + index,
                        100_000 + index,
                        1,
                        None if index % 4 < 2 else 1,
                        0,
                        base + index // 4,
                        0,
                        f"synthetic indexed message {index}",
                        0,
                        None,
                    )
                    for index in range(20_000)
                ),
            )
            # Only this disposable fixture is written. Production source connections
            # stay read-only and reuse the native-compatible create_time index.
            connection.execute(
                f"CREATE INDEX synthetic_message_time ON [{table}](create_time, sort_seq)"
            )
            connection.commit()
        finally:
            connection.close()

        position_steps: list[int] = []
        real_connect = self.provider._connect

        class _PositionCursor:
            def __init__(self, cursor: Any, connection: Any, steps: list[int]) -> None:
                self._cursor = cursor
                self._connection = connection
                self._steps = steps

            def fetchall(self) -> list[Any]:
                try:
                    return self._cursor.fetchall()
                finally:
                    position_steps.append(self._steps[0])
                    self._connection.set_progress_handler(lambda: int(operation_expired()), 1_000)

        class _Connection:
            def __init__(self, connection: Any) -> None:
                self._connection = connection

            def __getattr__(self, name: str) -> Any:
                return getattr(self._connection, name)

            def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
                if "SELECT rowid AS source_rowid, create_time" not in " ".join(sql.split()):
                    return self._connection.execute(sql, *args, **kwargs)
                steps = [0]

                def progress() -> int:
                    steps[0] += 100
                    return int(operation_expired())

                self._connection.set_progress_handler(progress, 100)
                return _PositionCursor(
                    self._connection.execute(sql, *args, **kwargs), self._connection, steps
                )

        @contextmanager
        def counting_connect(relative: str) -> Any:
            with real_connect(relative) as connection:
                yield _Connection(connection)

        with self.provider.session(
            SourceScope.conversation(self.account_key, self.conversation)
        ) as snapshot:
            pivot_page = self.provider.read_range(
                self.account_key,
                self.conversation,
                after=None,
                before=None,
                direction="forward",
                limit=4,
                snapshot=snapshot,
                time_after_utc=datetime.fromtimestamp(base + 2_500, UTC).isoformat(),
            )
            # The pivot shares its timestamp and NULL-as-zero sort_seq with another
            # row; each subsequent timestamp also has sort ties. Rowid stays decisive.
            pivot = pivot_page.messages[1]
            for direction in ("forward", "backward"):
                with self.subTest(direction=direction):
                    clause, values = self.provider._keyset_clause(
                        self.provider._sql_boundary(pivot.sort_key),
                        direction=direction,
                        inclusive=False,
                    )
                    order = "ASC" if direction == "forward" else "DESC"
                    with real_connect(relative) as connection:
                        expected = connection.execute(
                            f"SELECT rowid, message_content FROM [{table}] WHERE {clause} "
                            f"ORDER BY create_time {order}, COALESCE(sort_seq, 0) {order}, "
                            f"rowid {order} LIMIT 600",
                            values,
                        ).fetchall()
                    if direction == "backward":
                        expected.reverse()
                    position_steps.clear()
                    with mock.patch.object(
                        self.provider, "_connect", side_effect=counting_connect
                    ):
                        page = self.provider.read_range(
                            self.account_key,
                            self.conversation,
                            after=pivot.sort_key if direction == "forward" else None,
                            before=pivot.sort_key if direction == "backward" else None,
                            direction=direction,
                            limit=600,
                            snapshot=snapshot,
                        )

                    self.assertEqual(
                        [(message.source_rowid, message.raw_content) for message in page.messages],
                        [(int(row[0]), str(row[1])) for row in expected],
                    )
                    self.assertEqual(len(page.messages), 600)
                    self.assertTrue(
                        page.has_more_after if direction == "forward" else page.has_more_before
                    )
                    # Three bounded position batches exercise the initial target and
                    # subsequent page boundaries. Count VM work, not fetchall rows:
                    # the old OR-only keyset sorted ~10k remaining rows per batch.
                    self.assertGreaterEqual(len(position_steps), 3)
                    self.assertLessEqual(max(position_steps), 40_000)

    def test_native_participant_roster_skips_resource_resolution(self) -> None:
        # The roster only needs exact sender keys and current labels. It must still
        # read the real message rows for sender evidence (no duplicated identity
        # parsing) but must not run the resource resolver for every recent row, and
        # it must use bounded position batches rather than one shard sort per row.
        base = 1_725_200_000
        for index in range(2):
            self._add_message_shard(
                suffix=str(index + 3),
                messages=[
                    (index * 1000 + offset, base + index * 1000 + offset, index * 1000 + offset)
                    for offset in range(300)
                ],
            )

        with self.provider.snapshot() as snapshot:
            with (
                self._count_shard_fetches() as counts,
                mock.patch.object(
                    self.provider._resource_resolver,
                    "resources_for_message",
                    wraps=self.provider._resource_resolver.resources_for_message,
                ) as resources,
            ):
                participants = self.provider.list_participants(
                    self.account_key,
                    self.conversation,
                    snapshot,
                )

        self.assertTrue(participants)
        resources.assert_not_called()
        self.assertLessEqual(counts["position_query_max"], 256)
        # 200 recent rows across three shards: each shard contributes at most one
        # bounded position batch, never one full sort per returned row.
        self.assertLessEqual(counts["position_queries"], 3)

    def test_native_multishard_range_keeps_order_and_duplicate_conflict_detection(
        self,
    ) -> None:
        base = 1_725_060_000
        for index in range(3):
            self._add_message_shard(
                suffix=str(index + 2),
                messages=[
                    (index * 10 + offset, base + index * 10 + offset, index * 10 + offset)
                    for offset in range(4)
                ],
            )

        with self.provider.snapshot() as snapshot:
            forward = self.provider.read_range(
                self.account_key,
                self.conversation,
                after=None,
                before=None,
                direction="forward",
                limit=5,
                snapshot=snapshot,
            )
            saw_ordering = [message.sort_key.as_tuple() for message in forward.messages]
            self.assertEqual(saw_ordering, sorted(saw_ordering))
            self.assertEqual(len(forward.messages), 5)

        # A duplicate identity that disagrees across shards still fails the read closed
        # rather than silently merging or dropping the row.
        relative = "message/message_9.db"
        key = hashlib.sha256(b"synthetic-shard-conflict").hexdigest()
        self.keys[relative] = key
        self.provider._keys[relative] = {"enc_key": key}  # noqa: SLF001 - fixture enrollment
        connection = self._connect_new(relative)
        try:
            table = self._table_name(self.conversation)
            connection.execute(
                f"""
                CREATE TABLE [{table}](
                    local_id INTEGER,
                    server_id INTEGER,
                    local_type INTEGER,
                    sort_seq INTEGER,
                    real_sender_id INTEGER,
                    create_time INTEGER,
                    status INTEGER,
                    message_content BLOB,
                    WCDB_CT_message_content INTEGER,
                    packed_info_data BLOB
                )
                """
            )
            connection.execute(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    98,
                    102,
                    1,
                    2,
                    0,
                    1_725_000_002,
                    0,
                    "conflicting shard copy",
                    0,
                    None,
                ),
            )
            connection.commit()
        finally:
            connection.close()
        os.chmod(self.source / relative, 0o600)

        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                self.provider.read_range(
                    self.account_key,
                    self.conversation,
                    after=None,
                    before=None,
                    direction="forward",
                    limit=20,
                    snapshot=snapshot,
                )
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)
        self.assertIn(
            "duplicate_message_identity_conflict",
            caught.exception.details["warning_codes"],
        )

    def test_native_single_shard_recent_keeps_requested_page_plus_probe(self) -> None:
        with self._count_shard_fetches() as counts:
            with self.provider.snapshot() as snapshot:
                page = self.provider.read_recent(
                    self.account_key,
                    self.conversation,
                    1,
                    snapshot,
                )

        self.assertEqual(len(page.messages), 1)
        # One shard keeps the existing limit + 1 page: the requested row plus its probe.
        self.assertEqual(counts["payload"], 2)
        self.assertEqual(counts["positions"], 2)

    def test_native_recent_keeps_time_order_when_sort_seq_is_not_monotonic(self) -> None:
        self._insert_message(
            local_id=99,
            server_id=199,
            sort_seq=0,
            create_time=1_725_000_099,
            content="late source row",
        )

        with self.provider.snapshot() as snapshot:
            page = self.provider.read_recent(
                self.account_key,
                self.conversation,
                1,
                snapshot,
            )

        self.assertEqual(len(page.messages), 1)
        self.assertEqual(page.messages[0].raw_content, "late source row")

    def test_native_catalog_reuses_only_unchanged_message_shard_metadata(self) -> None:
        def message_open_count(recorder: Any) -> int:
            return sum(
                1
                for call in recorder.call_args_list
                if call.args and str(call.args[0]).startswith("message/")
            )

        with mock.patch.object(self.provider, "_connect", wraps=self.provider._connect) as recorder:
            with self.provider.snapshot() as snapshot:
                self.provider.list_conversations(self.account_key, snapshot)
            first_message_opens = message_open_count(recorder)
            recorder.reset_mock()
            with self.provider.snapshot() as snapshot:
                self.provider.list_conversations(self.account_key, snapshot)
            unchanged_message_opens = message_open_count(recorder)

        self.assertGreater(first_message_opens, 0)
        self.assertEqual(unchanged_message_opens, 0)

        archived = "new-archived-contact"
        self._add_contact_history_without_session(archived)
        with mock.patch.object(self.provider, "_connect", wraps=self.provider._connect) as recorder:
            with self.provider.snapshot() as snapshot:
                conversations = self.provider.list_conversations(self.account_key, snapshot)

        self.assertGreater(message_open_count(recorder), 0)
        self.assertIn(
            archived,
            {item.source_conversation_id for item in conversations},
        )

    def test_native_sync_skips_a_fully_indexed_unchanged_generation(self) -> None:
        tools = self._reader_tools()
        first = tools.service.sync_source_once(
            initial_tail=20, batch_limit=20, conversation_limit=20
        )
        self.assertEqual(first["conversation_count"], 1)

        with mock.patch.object(
            self.provider,
            "list_conversations",
            side_effect=AssertionError("unchanged indexed source must not rescan"),
        ):
            second = tools.service.sync_source_once(
                initial_tail=20, batch_limit=20, conversation_limit=20
            )

        self.assertEqual(second["conversation_count"], 0)
        self.assertEqual(second["message_count"], 0)
        self.assertEqual(second["pending_conversation_count"], 0)

    def test_native_inbox_reads_the_admitted_index_without_a_new_source_scan(self) -> None:
        tools = self._reader_tools()
        with mock.patch.object(
            self.provider,
            "snapshot",
            side_effect=AssertionError("cold inbox must not start a live catalog scan"),
        ):
            cold = tools.wechat_read_inbox(limit=10)
        self.assertEqual(cold["code"], "SOURCE_INCOMPLETE")
        self.assertIn("source_catalog_not_ready", cold["details"]["warning_codes"])

        tools.service.sync_source_once(initial_tail=20, batch_limit=20, conversation_limit=20)
        self.assertTrue(
            tools.service.local_only_tool_call("wechat_read_inbox", {"limit": 10})
        )
        with (
            mock.patch.object(
                self.provider,
                "snapshot",
                side_effect=AssertionError("warm inbox must use admitted observations"),
            ),
            mock.patch.object(
                self.provider,
                "health",
                side_effect=AssertionError("warm inbox must not require live health"),
            ),
        ):
            warm = tools.wechat_read_inbox(limit=10, include_latest="text")

        self.assertEqual(warm["schema"], "sightglass.inbox-page.v1")
        self.assertEqual(len(warm["items"]), 1)
        self.assertEqual(warm["coverage"]["catalog"], "complete")
        self.assertIsInstance(warm["coverage"]["catalog_fresh_as_of"], str)
        self.assertEqual(warm["source_receipt"]["served_from"], "window_db")
        self.assertFalse(warm["source_receipt"]["freshness"]["live_refresh_confirmed"])

    def test_on_demand_native_catalog_readiness_needs_no_tail_and_inbox_uses_residents(self):
        from sightglass.residency.decisions import ResidencySettings
        from sightglass.residency.repository import ResidencyRepository

        tools = self._reader_tools()
        ResidencyRepository(tools.service.repository.database).set_settings(ResidencySettings())
        with mock.patch.object(
            self.provider, "read_recent", side_effect=AssertionError("idle body")
        ):
            tools.service.sync_source_once(conversation_limit=20)
        empty = tools.wechat_read_inbox(limit=10, include_latest="text")
        self.assertEqual(empty["schema"], "sightglass.inbox-page.v1")
        self.assertEqual(empty["items"], [])
        self.assertEqual(empty["coverage"]["message_scope"], "resident")
        group = tools.wechat_find_conversations("")["candidates"][0]["conversation_id"]
        read = tools.wechat_read_messages(mode="recent", conversation_id=group, limit=2)
        self.assertGreater(len(read["messages"]), 0)
        with mock.patch.object(
            self.provider, "snapshot", side_effect=AssertionError("warm source")
        ):
            warm = tools.wechat_read_inbox(limit=10, include_latest="text")
        self.assertEqual(len(warm["items"]), 1)
        store = ResidencyRepository(tools.service.repository.database)
        approved = store.stock_preview(group)
        store.release_stock(group, plan=approved["plan"])
        released = tools.wechat_read_inbox(limit=10, include_latest="text")
        self.assertEqual(released["items"], [])

    def test_compressed_message_uses_an_independent_decompressor(self) -> None:
        real_decompressor = zstandard.ZstdDecompressor
        with self.provider.snapshot() as snapshot:
            accounts = self.provider.list_accounts(snapshot)
            conversations = self.provider.list_conversations(
                accounts[0].source_account_key, snapshot
            )
            with mock.patch(
                "sightglass.source.macos_wechat.provider.zstandard.ZstdDecompressor",
                wraps=real_decompressor,
            ) as factory:
                page = self.provider.read_recent(
                    accounts[0].source_account_key,
                    conversations[0].source_conversation_id,
                    2,
                    snapshot,
                )

        self.assertEqual(page.messages[-1].raw_content, "latest message")
        self.assertEqual(factory.call_count, 1)

    def test_background_worker_observes_appends_and_recovers_exactly_after_restart(
        self,
    ) -> None:
        tools = self._reader_tools()
        service = tools.service

        def wait_for_latest(expected: str) -> None:
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                result = tools.wechat_read_inbox(limit=10, include_latest="text")
                if result.get("items") and result["items"][0]["latest"].get("text") == expected:
                    return
                time.sleep(0.01)
            self.fail(f"source worker did not index expected fixture append: {expected}")

        first_worker = SourceWorker(service, poll_interval_seconds=0.02)
        first_worker.start()
        try:
            wait_for_latest("latest message")
            self._insert_message(
                local_id=301,
                server_id=401,
                sort_seq=301,
                create_time=1_725_000_301,
                content="first background append",
            )
            wait_for_latest("first background append")
        finally:
            first_worker.stop()

        self._insert_message(
            local_id=302,
            server_id=402,
            sort_seq=302,
            create_time=1_725_000_302,
            content="append across worker restart",
        )
        second_worker = SourceWorker(service, poll_interval_seconds=0.02)
        second_worker.start()
        try:
            wait_for_latest("append across worker restart")
        finally:
            second_worker.stop()

        external_conversation = opaque_id(
            "wxconv", opaque_id("wxacct", self.account_key), self.conversation
        )
        with service.repository.database.connection() as connection:
            admitted = int(
                connection.execute(
                    "SELECT COUNT(*) FROM messages WHERE conversation_id = ?",
                    (external_conversation,),
                ).fetchone()[0]
            )
        self.assertEqual(admitted, 4)
        self.assertIsNone(second_worker.status()["last_error_code"])

    def test_invalid_message_token_is_not_accepted(self) -> None:
        with self.provider.snapshot() as snapshot:
            self.assertIsNone(self.provider.get_message(self.account_key, "not-a-token", snapshot))

    def test_snapshot_rejects_a_database_change_before_commit(self) -> None:
        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                self.provider.read_recent(self.account_key, self.conversation, 1, snapshot)
                message = self._connect_new("message/message_0.db")
                table = "Msg_" + hashlib.md5(self.conversation.encode()).hexdigest()
                try:
                    message.execute(
                        f"UPDATE [{table}] SET sort_seq = sort_seq + 10 WHERE local_id = 1"
                    )
                    message.commit()
                finally:
                    message.close()
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)

    def test_source_scope_rejects_invalid_target_shapes(self) -> None:
        with self.assertRaises(ValueError):
            SourceScope(kind="catalog", account_id=self.account_key)
        with self.assertRaises(ValueError):
            SourceScope(kind="conversation", account_id=self.account_key)
        with self.assertRaises(ValueError):
            SourceScope(kind="message", account_id=self.account_key)
        with self.assertRaises(ValueError):
            SourceScope(kind="resource", source_resource_key="")

    def test_message_scope_serves_only_its_exact_message(self) -> None:
        with self.provider.snapshot() as snapshot:
            messages = self.provider.read_recent(
                self.account_key,
                self.conversation,
                2,
                snapshot,
            ).messages
        target, other = messages
        scope = SourceScope.message(
            self.account_key,
            target.source_message_id,
            conversation_source_id=self.conversation,
        )
        with self.provider.session(scope) as snapshot:
            found = self.provider.get_message(
                self.account_key,
                target.source_message_id,
                snapshot,
            )
        self.assertIsNotNone(found)

        with self.assertRaises(SightglassError) as caught:
            with self.provider.session(scope) as snapshot:
                self.provider.get_message(
                    self.account_key,
                    other.source_message_id,
                    snapshot,
                )
        self.assertEqual(caught.exception.code, ErrorCode.MESSAGE_NOT_FOUND)

        with self.assertRaises(SightglassError) as paged:
            with self.provider.session(scope) as snapshot:
                self.provider.read_recent(
                    self.account_key,
                    self.conversation,
                    2,
                    snapshot,
                )
        self.assertEqual(paged.exception.code, ErrorCode.MESSAGE_NOT_FOUND)

    def test_scoped_session_ignores_unopened_shard_wal_append(self) -> None:
        unrelated = self._add_probe_message_shard("1")
        writer = self._connect_new(unrelated)
        try:
            self.assertEqual(str(writer.execute("PRAGMA journal_mode=WAL").fetchone()[0]), "wal")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            writer.execute("INSERT INTO shard_probe VALUES (2)")
            writer.commit()

            # Populate the generation-bound schema cache before opening the narrow
            # session. The body read therefore never opens this unrelated shard;
            # exit validation opens one fresh routing view only if its WAL changed.
            with self.provider.snapshot() as snapshot:
                self.provider.read_recent(
                    self.account_key,
                    self.conversation,
                    2,
                    snapshot,
                )

            opened: list[str] = []
            real_open = self.provider._open_scoped_connection  # noqa: SLF001

            def track_open(relative: str, *, auxiliary: bool) -> Any:
                opened.append(relative)
                return real_open(relative, auxiliary=auxiliary)

            with mock.patch.object(
                self.provider,
                "_open_scoped_connection",
                side_effect=track_open,
            ):
                with self.provider.session(
                    SourceScope.conversation(self.account_key, self.conversation)
                ) as snapshot:
                    page = self.provider.read_recent(
                        self.account_key,
                        self.conversation,
                        2,
                        snapshot,
                    )
                    self.assertNotIn(unrelated, opened)
                    writer.execute("INSERT INTO shard_probe VALUES (3)")
                    writer.commit()

            self.assertEqual(len(page.messages), 2)
            self.assertEqual(opened.count(unrelated), 1)
            self.assertIn("message/message_0.db", opened)
        finally:
            writer.close()

    def test_scoped_session_rejects_selected_database_wal_commit_and_recovers(self) -> None:
        relative = "message/message_0.db"
        table = self._table_name(self.conversation)
        writer = self._connect_new(relative)
        try:
            self.assertEqual(str(writer.execute("PRAGMA journal_mode=WAL").fetchone()[0]), "wal")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            with self.assertRaises(SightglassError) as caught:
                with self.provider.session(
                    SourceScope.conversation(self.account_key, self.conversation)
                ) as snapshot:
                    self.provider.read_recent(
                        self.account_key,
                        self.conversation,
                        2,
                        snapshot,
                    )
                    writer.execute(
                        f"UPDATE [{table}] SET message_content = ? WHERE local_id = 1",
                        ("selected WAL commit",),
                    )
                    writer.commit()
            self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)

            with self.provider.session(
                SourceScope.conversation(self.account_key, self.conversation)
            ) as snapshot:
                recovered = self.provider.read_recent(
                    self.account_key,
                    self.conversation,
                    2,
                    snapshot,
                )
            self.assertIn("selected WAL commit", [item.raw_content for item in recovered.messages])
        finally:
            writer.close()

    def test_scoped_session_rejects_wal_commit_while_pinning_read_view(self) -> None:
        relative = "message/message_0.db"
        table = self._table_name(self.conversation)
        sqlite = _sqlcipher()
        writer = self._connect_new(relative)
        committed = False
        connections: list[Any] = []
        real_connect = sqlite.connect

        class CommitAfterPinConnection(sqlite.Connection):
            scoped_read = False

            def execute(self, statement: str, *args: Any, **kwargs: Any) -> Any:
                nonlocal committed
                cursor = super().execute(statement, *args, **kwargs)
                if statement == "BEGIN":
                    self.scoped_read = True
                    connections.append(self)
                # SQLCipher has stepped the aggregate query and pinned its read
                # view. Commit before the provider can check the opened revision.
                if (
                    self.scoped_read
                    and statement == "SELECT COUNT(*) FROM sqlite_master"
                    and not committed
                ):
                    writer.execute(
                        f"UPDATE [{table}] SET message_content = ? WHERE local_id = 1",
                        ("synthetic commit after read-view pin",),
                    )
                    writer.commit()
                    committed = True
                return cursor

        def track_connect(*args: Any, **kwargs: Any) -> Any:
            if str(args[0]).endswith("/message_0.db?mode=ro"):
                kwargs["factory"] = CommitAfterPinConnection
            return real_connect(*args, **kwargs)

        try:
            self.assertEqual(str(writer.execute("PRAGMA journal_mode=WAL").fetchone()[0]), "wal")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            with (
                mock.patch.object(sqlite, "connect", side_effect=track_connect),
                self.assertRaises(SightglassError) as caught,
            ):
                with self.provider.session(
                    SourceScope.conversation(self.account_key, self.conversation)
                ) as snapshot:
                    self.provider.read_recent(self.account_key, self.conversation, 2, snapshot)
            self.assertTrue(committed)
            self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)
            self.assertTrue(connections)
            for connection in connections:
                with self.assertRaises(sqlite.ProgrammingError):
                    connection.execute("SELECT 1")

            with self.provider.session(
                SourceScope.conversation(self.account_key, self.conversation)
            ) as snapshot:
                recovered = self.provider.read_recent(
                    self.account_key, self.conversation, 2, snapshot
                )
            self.assertIn(
                "synthetic commit after read-view pin",
                [item.raw_content for item in recovered.messages],
            )
        finally:
            writer.close()

    def test_scoped_session_rejects_selected_database_replacement_and_recovers(self) -> None:
        relative = "message/message_0.db"
        source = self.source / relative
        replacement = self.root / "scoped-replacement.db"
        shutil.copy2(source, replacement)

        with self.assertRaises(SightglassError) as caught:
            with self.provider.session(
                SourceScope.conversation(self.account_key, self.conversation)
            ) as snapshot:
                self.provider.read_recent(
                    self.account_key,
                    self.conversation,
                    2,
                    snapshot,
                )
                os.replace(replacement, source)
                os.chmod(source, 0o600)
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)

        with self.provider.session(
            SourceScope.conversation(self.account_key, self.conversation)
        ) as snapshot:
            recovered = self.provider.read_recent(
                self.account_key,
                self.conversation,
                2,
                snapshot,
            )
        self.assertEqual(len(recovered.messages), 2)

    def test_resource_scope_rejects_a_different_locator(self) -> None:
        first_data = b"first scoped resource\n"
        second_data = b"second scoped resource\n"
        self._insert_file_message(
            name="scope-first.txt",
            data=first_data,
            local_id=30,
            server_id=130,
        )
        self._insert_file_message(
            name="scope-second.txt",
            data=second_data,
            local_id=31,
            server_id=131,
        )
        directory = self.root / "msg" / "file" / "2024-08"
        directory.mkdir(parents=True)
        (directory / "scope-first.txt").write_bytes(first_data)
        (directory / "scope-second.txt").write_bytes(second_data)
        with self.provider.snapshot() as snapshot:
            messages = self.provider.read_recent(
                self.account_key,
                self.conversation,
                20,
                snapshot,
            ).messages
        keys = [
            resource.source_resource_key
            for message in messages
            for resource in message.resources
            if resource.source_resource_key is not None
        ]
        self.assertGreaterEqual(len(keys), 2)

        with self.assertRaises(SightglassError) as caught:
            with self.provider.session(SourceScope.resource(keys[0])) as snapshot:
                self.provider.read_resource(keys[1], max_bytes=1024, snapshot=snapshot)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)

    def test_health_rejects_a_new_message_shard_without_a_key(self) -> None:
        tools = self._reader_tools()
        tools.service.sync_source_once(initial_tail=20, batch_limit=20)
        external_conversation = opaque_id(
            "wxconv", opaque_id("wxacct", self.account_key), self.conversation
        )
        before = tools.service.repository.source_conversation_state(external_conversation)
        self.assertIsNotNone(before)
        assert before is not None
        before_tail = (
            before["tail_sort_primary"],
            before["tail_sort_seq"],
            before["tail_sort_tie"],
            before["tail_source_message_id"],
        )
        sqlite = _sqlcipher()
        path = self.source / "message/message_1.db"
        connection = sqlite.connect(path)
        try:
            connection.execute(f'''PRAGMA key = "x'{"44" * 32}'"''')
            connection.execute("CREATE TABLE shard_probe(value INTEGER)")
            connection.execute("INSERT INTO shard_probe VALUES (1)")
            connection.commit()
        finally:
            connection.close()
        os.chmod(path, 0o600)

        health = self.provider.health()

        self.assertFalse(health.complete)
        with self.assertRaises(SightglassError) as caught:
            tools.service.sync_source_once(initial_tail=20, batch_limit=20)
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)
        after = tools.service.repository.source_conversation_state(external_conversation)
        self.assertIsNotNone(after)
        assert after is not None
        self.assertEqual(
            (
                after["tail_sort_primary"],
                after["tail_sort_seq"],
                after["tail_sort_tie"],
                after["tail_source_message_id"],
            ),
            before_tail,
        )
        self.assertIn("source_key_missing_or_invalid", health.warnings)
        self.assertEqual(health.shard_counts["missing"], 1)

    def test_forward_keyset_pagination_reaches_rows_beyond_the_first_sql_window(self) -> None:
        for offset in range(3, 10):
            self._insert_message(
                local_id=offset,
                server_id=100 + offset,
                sort_seq=offset,
                create_time=1_725_000_000 + offset,
                content=f"message {offset}",
            )

        with self.provider.snapshot() as snapshot:
            after = None
            seen: list[str] = []
            for _index in range(12):
                page = self.provider.read_range(
                    self.account_key,
                    self.conversation,
                    after=after,
                    before=None,
                    direction="forward",
                    limit=1,
                    snapshot=snapshot,
                )
                if not page.messages:
                    break
                message = page.messages[0]
                seen.append(message.raw_content)
                after = message.sort_key

        self.assertEqual(
            seen,
            ["first message", "latest message", *[f"message {value}" for value in range(3, 10)]],
        )

    def test_backward_keyset_pagination_reaches_the_oldest_rows(self) -> None:
        for offset in range(3, 10):
            self._insert_message(
                local_id=offset,
                server_id=100 + offset,
                sort_seq=offset,
                create_time=1_725_000_000 + offset,
                content=f"message {offset}",
            )

        with self.provider.snapshot() as snapshot:
            before = None
            seen: list[str] = []
            for _index in range(12):
                page = self.provider.read_range(
                    self.account_key,
                    self.conversation,
                    after=None,
                    before=before,
                    direction="backward",
                    limit=1,
                    snapshot=snapshot,
                )
                if not page.messages:
                    break
                message = page.messages[0]
                seen.append(message.raw_content)
                before = message.sort_key

        self.assertEqual(
            seen,
            [*[f"message {value}" for value in range(9, 2, -1)], "latest message", "first message"],
        )

    def test_sparse_source_message_filter_scans_past_nonmatching_rows(self) -> None:
        message = self._connect_new("message/message_0.db")
        try:
            table = self._table_name(self.conversation)
            message.executemany(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    (
                        offset,
                        100 + offset,
                        1,
                        offset,
                        0,
                        1_725_000_000 + offset,
                        0,
                        f"message {offset}",
                        0,
                        None,
                    )
                    for offset in range(3, 603)
                ),
            )
            message.commit()
        finally:
            message.close()

        with self.provider.snapshot() as snapshot:
            target = self.provider.read_recent(
                self.account_key,
                self.conversation,
                1,
                snapshot,
            ).messages[0]
            with (
                mock.patch.object(
                    self.provider,
                    "_keyset_clause",
                    wraps=self.provider._keyset_clause,
                ) as keyset,
                mock.patch.object(
                    self.provider._resource_resolver,
                    "resources_for_message",
                    wraps=self.provider._resource_resolver.resources_for_message,
                ) as resources,
            ):
                filtered = self.provider.read_range(
                    self.account_key,
                    self.conversation,
                    after=None,
                    before=None,
                    direction="forward",
                    limit=1,
                    snapshot=snapshot,
                    participant_source_ids=(
                        SourceParticipantFilter(source_message_id=target.source_message_id),
                    ),
                )

        self.assertEqual(filtered.messages, (target,))
        self.assertEqual(keyset.call_count, 0)
        self.assertEqual(resources.call_count, 1)

    def test_group_speaker_filter_resolves_resources_only_after_sender_match(self) -> None:
        group, member = self._add_group_sender_fixture()
        matching = SourceParticipantFilter(
            key_kind="internal_username",
            key_value=member,
            principal_eligible=True,
            scope_conversation_source_id=group,
        )
        missing = SourceParticipantFilter(
            key_kind="internal_username",
            key_value="synthetic-missing-member",
            principal_eligible=True,
            scope_conversation_source_id=group,
        )

        with self.provider.snapshot() as snapshot:
            with mock.patch.object(
                self.provider._resource_resolver,
                "resources_for_message",
                wraps=self.provider._resource_resolver.resources_for_message,
            ) as resources:
                filtered = self.provider.read_range(
                    self.account_key,
                    group,
                    after=None,
                    before=None,
                    direction="forward",
                    limit=10,
                    snapshot=snapshot,
                    participant_source_ids=(matching,),
                )
            self.assertEqual(
                [item.raw_content for item in filtered.messages],
                [f"{member}:\nmember status three", f"{member}:\nmember status four"],
            )
            self.assertEqual(resources.call_count, 2)

            with mock.patch.object(
                self.provider._resource_resolver,
                "resources_for_message",
                wraps=self.provider._resource_resolver.resources_for_message,
            ) as resources:
                absent = self.provider.read_range(
                    self.account_key,
                    group,
                    after=None,
                    before=None,
                    direction="forward",
                    limit=10,
                    snapshot=snapshot,
                    participant_source_ids=(missing,),
                )
            self.assertEqual(absent.messages, ())
            self.assertEqual(resources.call_count, 0)

    def test_same_second_keyset_uses_sort_sequence_before_rowid(self) -> None:
        for offset, sort_seq in enumerate((60, 10, 50, 20, 40, 30), start=10):
            self._insert_message(
                local_id=offset,
                server_id=100 + offset,
                sort_seq=sort_seq,
                create_time=1_725_000_100,
                content=f"same-second {sort_seq}",
            )

        with self.provider.snapshot() as snapshot:
            after = None
            seen: list[int] = []
            while True:
                page = self.provider.read_range(
                    self.account_key,
                    self.conversation,
                    after=after,
                    before=None,
                    direction="forward",
                    limit=1,
                    snapshot=snapshot,
                    time_after_utc="2024-08-30T06:41:40+00:00",
                )
                if not page.messages:
                    break
                message = page.messages[0]
                seen.append(message.sort_seq)
                after = message.sort_key

        self.assertEqual(seen, [10, 20, 30, 40, 50, 60])

    def test_range_respects_fractional_and_exact_second_time_bounds(self) -> None:
        cases = (
            ("2024-08-30T06:40:01.000001+00:00", None, {1_725_000_002}),
            (None, "2024-08-30T06:40:02.000001+00:00", {1_725_000_001, 1_725_000_002}),
            ("2024-08-30T06:40:02+00:00", None, {1_725_000_002}),
            (None, "2024-08-30T06:40:02+00:00", {1_725_000_001}),
        )
        for direction in ("forward", "backward"):
            for after, before, expected in cases:
                with self.subTest(direction=direction, after=after, before=before):
                    with self.provider.session(
                        SourceScope.conversation(self.account_key, self.conversation)
                    ) as snapshot:
                        page = self.provider.read_range(
                            self.account_key, self.conversation,
                            after=None, before=None, direction=direction, limit=2,
                            snapshot=snapshot, time_after_utc=after, time_before_utc=before,
                        )
                    self.assertEqual(
                        {int(message.source_time_raw) for message in page.messages}, expected
                    )

    def test_signed_timeline_cursor_survives_an_ordinary_append(self) -> None:
        tools = self._reader_tools()
        external_account = opaque_id("wxacct", self.account_key)
        external_conversation = opaque_id("wxconv", external_account, self.conversation)
        first = tools.wechat_read_messages(
            mode="recent",
            conversation_id=external_conversation,
            projection="detail",
            limit=1,
        )
        cursor = first["page"]["next_cursor"]
        self.assertIsInstance(cursor, str)

        self._insert_message(
            local_id=3,
            server_id=103,
            sort_seq=3,
            create_time=1_725_000_003,
            content="ordinary append",
        )

        older = tools.wechat_read_messages(
            mode="recent",
            conversation_id=external_conversation,
            projection="detail",
            limit=1,
            cursor=cursor,
        )
        self.assertEqual(older["schema"], "sightglass.message-page.v1")
        self.assertEqual([item["text"] for item in older["messages"]], ["first message"])

    def test_ordinary_append_preserves_logical_cursor_binding_digests(self) -> None:
        before = self.provider.health()
        self._insert_message(
            local_id=3,
            server_id=103,
            sort_seq=3,
            create_time=1_725_000_003,
            content="ordinary append",
        )
        after = self.provider.health()

        self.assertEqual(before.inventory_digest, after.inventory_digest)
        self.assertEqual(before.generation_set_digest, after.generation_set_digest)

    def test_signed_timeline_cursor_rejects_main_database_replacement(self) -> None:
        tools = self._reader_tools()
        external_account = opaque_id("wxacct", self.account_key)
        external_conversation = opaque_id("wxconv", external_account, self.conversation)
        first = tools.wechat_read_messages(
            mode="recent",
            conversation_id=external_conversation,
            projection="detail",
            limit=1,
        )
        cursor = first["page"]["next_cursor"]
        source = self.source / "message/message_0.db"
        replacement = self.root / "replacement.db"
        shutil.copy2(source, replacement)
        os.replace(replacement, source)
        os.chmod(source, 0o600)

        stale = tools.wechat_read_messages(
            mode="recent",
            conversation_id=external_conversation,
            projection="detail",
            limit=1,
            cursor=cursor,
        )

        self.assertEqual(stale["code"], "CURSOR_STALE")

    def test_stable_server_message_identity_survives_row_relocation(self) -> None:
        with self.provider.snapshot() as snapshot:
            first = self.provider.read_range(
                self.account_key,
                self.conversation,
                after=None,
                before=None,
                direction="forward",
                limit=1,
                snapshot=snapshot,
            ).messages[0]

        connection = self._connect_new("message/message_0.db")
        try:
            table = self._table_name(self.conversation)
            connection.execute(f"DELETE FROM [{table}] WHERE server_id = 101")
            connection.execute(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (1, 101, 1, 1, 0, 1_725_000_001, 0, "first message", 0, None),
            )
            connection.commit()
        finally:
            connection.close()

        with self.provider.snapshot() as snapshot:
            relocated = next(
                message
                for message in self.provider.read_range(
                    self.account_key,
                    self.conversation,
                    after=None,
                    before=None,
                    direction="forward",
                    limit=10,
                    snapshot=snapshot,
                ).messages
                if message.raw_content == "first message"
            )
            recovered = self.provider.get_message(
                self.account_key, first.source_message_id, snapshot
            )

        self.assertNotEqual(first.source_rowid, relocated.source_rowid)
        self.assertEqual(first.source_message_id, relocated.source_message_id)
        self.assertEqual(recovered, relocated)

    def test_stable_local_message_identity_survives_row_relocation(self) -> None:
        self._insert_message(
            local_id=50,
            server_id=0,
            sort_seq=50,
            create_time=1_725_000_050,
            content="local identity",
        )
        with self.provider.snapshot() as snapshot:
            first = next(
                message
                for message in self.provider.read_range(
                    self.account_key,
                    self.conversation,
                    after=None,
                    before=None,
                    direction="forward",
                    limit=10,
                    snapshot=snapshot,
                ).messages
                if message.raw_content == "local identity"
            )

        connection = self._connect_new("message/message_0.db")
        try:
            table = self._table_name(self.conversation)
            connection.execute(f"DELETE FROM [{table}] WHERE local_id = 50")
            connection.execute(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (51, 151, 1, 51, 0, 1_725_000_051, 0, "rowid spacer", 0, None),
            )
            connection.execute(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (50, 0, 1, 50, 0, 1_725_000_050, 0, "local identity", 0, None),
            )
            connection.commit()
        finally:
            connection.close()

        with self.provider.snapshot() as snapshot:
            relocated = next(
                message
                for message in self.provider.read_range(
                    self.account_key,
                    self.conversation,
                    after=None,
                    before=None,
                    direction="forward",
                    limit=10,
                    snapshot=snapshot,
                ).messages
                if message.raw_content == "local identity"
            )
            recovered = self.provider.get_message(
                self.account_key, first.source_message_id, snapshot
            )

        self.assertNotEqual(first.source_rowid, relocated.source_rowid)
        self.assertEqual(first.source_message_id, relocated.source_message_id)
        self.assertEqual(recovered, relocated)

    def test_fallback_identity_does_not_alias_reused_rowid_with_new_payload(self) -> None:
        self._insert_message(
            local_id=0,
            server_id=0,
            sort_seq=70,
            create_time=1_725_000_070,
            content="fallback first payload",
        )
        with self.provider.snapshot() as snapshot:
            first = next(
                message
                for message in self.provider.read_range(
                    self.account_key,
                    self.conversation,
                    after=None,
                    before=None,
                    direction="forward",
                    limit=10,
                    snapshot=snapshot,
                ).messages
                if message.raw_content == "fallback first payload"
            )

        connection = self._connect_new("message/message_0.db")
        try:
            table = self._table_name(self.conversation)
            connection.execute(f"DELETE FROM [{table}] WHERE rowid = ?", (first.source_rowid,))
            connection.execute(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (0, 0, 1, 70, 0, 1_725_000_070, 0, "fallback new payload", 0, None),
            )
            connection.commit()
        finally:
            connection.close()

        with self.provider.snapshot() as snapshot:
            replacement = next(
                message
                for message in self.provider.read_range(
                    self.account_key,
                    self.conversation,
                    after=None,
                    before=None,
                    direction="forward",
                    limit=10,
                    snapshot=snapshot,
                ).messages
                if message.raw_content == "fallback new payload"
            )

        self.assertEqual(first.source_rowid, replacement.source_rowid)
        self.assertNotEqual(first.source_message_id, replacement.source_message_id)

    def test_conflicting_duplicate_server_identity_fails_closed(self) -> None:
        relative = "message/message_1.db"
        key = "44" * 32
        self.keys[relative] = key
        self.provider._keys[relative] = {"enc_key": key}  # noqa: SLF001 - fixture enrollment
        connection = self._connect_new(relative)
        try:
            table = self._table_name(self.conversation)
            connection.execute(
                f"""
                CREATE TABLE [{table}](
                    local_id INTEGER,
                    server_id INTEGER,
                    local_type INTEGER,
                    sort_seq INTEGER,
                    real_sender_id INTEGER,
                    create_time INTEGER,
                    status INTEGER,
                    message_content BLOB,
                    WCDB_CT_message_content INTEGER,
                    packed_info_data BLOB
                )
                """
            )
            connection.execute(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (99, 101, 1, 1, 0, 1_725_000_001, 0, "conflicting copy", 0, None),
            )
            connection.commit()
        finally:
            connection.close()
        os.chmod(self.source / relative, 0o600)

        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                self.provider.read_range(
                    self.account_key,
                    self.conversation,
                    after=None,
                    before=None,
                    direction="forward",
                    limit=10,
                    snapshot=snapshot,
                )

        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)
        self.assertIn(
            "duplicate_message_identity_conflict",
            caught.exception.details["warning_codes"],
        )

    def test_conversation_table_discovery_is_refreshed_for_each_snapshot(self) -> None:
        relative = "message/message_1.db"
        key = "44" * 32
        self.keys[relative] = key
        self.provider._keys[relative] = {"enc_key": key}  # noqa: SLF001 - fixture enrollment
        connection = self._connect_new(relative)
        try:
            connection.execute("CREATE TABLE shard_probe(value INTEGER)")
            connection.execute("INSERT INTO shard_probe VALUES (1)")
            connection.commit()
        finally:
            connection.close()
        os.chmod(self.source / relative, 0o600)

        with self.provider.snapshot() as snapshot:
            initial = self.provider.read_recent(self.account_key, self.conversation, 10, snapshot)
            self.assertEqual(len(initial.messages), 2)

        connection = self._connect_new(relative)
        try:
            table = self._table_name(self.conversation)
            connection.execute(
                f"""
                CREATE TABLE [{table}](
                    local_id INTEGER,
                    server_id INTEGER,
                    local_type INTEGER,
                    sort_seq INTEGER,
                    real_sender_id INTEGER,
                    create_time INTEGER,
                    status INTEGER,
                    message_content BLOB,
                    WCDB_CT_message_content INTEGER,
                    packed_info_data BLOB
                )
                """
            )
            connection.execute(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (1, 201, 1, 3, 0, 1_725_000_003, 0, "later shard", 0, None),
            )
            connection.commit()
        finally:
            connection.close()

        with self.provider.snapshot() as snapshot:
            refreshed = self.provider.read_recent(self.account_key, self.conversation, 10, snapshot)

        self.assertEqual(
            [message.raw_content for message in refreshed.messages],
            ["first message", "latest message", "later shard"],
        )

    def test_snapshot_requires_the_exact_configured_profile(self) -> None:
        self.provider._discover_candidates = lambda: (  # noqa: SLF001 - fault injection
            replace(self.candidate, profile_id="another-profile"),
        )

        health = self.provider.health()
        self.assertFalse(health.complete)
        self.assertIn("source_build_binding_changed", health.warnings)
        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot():
                pass
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)

    def test_message_table_schema_mismatch_fails_closed(self) -> None:
        relative = "message/message_1.db"
        key = "44" * 32
        self.keys[relative] = key
        self.provider._keys[relative] = {"enc_key": key}  # noqa: SLF001 - fixture enrollment
        connection = self._connect_new(relative)
        try:
            table = self._table_name(self.conversation)
            connection.execute(f"CREATE TABLE [{table}](local_id INTEGER, server_id INTEGER)")
            connection.commit()
        finally:
            connection.close()
        os.chmod(self.source / relative, 0o600)

        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                self.provider.read_recent(self.account_key, self.conversation, 10, snapshot)

        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_INCOMPLETE)
        self.assertIn("source_schema_invalid", caught.exception.details["warning_codes"])

    def test_reader_service_projects_only_the_allowlisted_live_conversation(self) -> None:
        external_account = opaque_id("wxacct", self.account_key)
        external_conversation = opaque_id("wxconv", external_account, self.conversation)
        tools = self._reader_tools()

        status = tools.wechat_status("capabilities")
        self.assertTrue(status["ready"])
        self.assertFalse(status["capabilities"]["synthetic_only"])
        self.assertTrue(status["capabilities"]["live_refresh"])
        self.assertTrue(status["capabilities"]["resources"])
        discovered = tools.wechat_find_conversations("")
        self.assertEqual(len(discovered["candidates"]), 1)
        page = tools.wechat_read_messages(
            mode="recent",
            conversation_id=external_conversation,
            projection="detail", response_profile="diagnostic",
            limit=2,
        )
        self.assertEqual(
            [item["text"] for item in page["messages"]],
            ["first message", "latest message"],
        )
        self.assertIsNone(page["messages"][0]["sender"]["shown_as"])
        self.assertEqual(page["messages"][0]["sender"]["label"], "Fixture Remark")

    def _direct_conversation_id(self) -> str:
        return opaque_id("wxconv", opaque_id("wxacct", self.account_key), self.conversation)

    def test_participant_activity_uses_latest_observed_message_before_admission(self) -> None:
        self._insert_message(
            local_id=3, server_id=103, create_time=1_725_000_003,
            sort_seq=3, content="synthetic later incoming", status=0,
        )
        tools = self._reader_tools()
        page = tools.wechat_find_participants(
            self._direct_conversation_id(),
            "Fixture",
            active_after=datetime.fromtimestamp(1_725_000_002, UTC).isoformat(),
        )
        self.assertEqual(len(page["candidates"]), 1)
        self.assertEqual(
            datetime.fromisoformat(page["candidates"][0]["last_spoke_at"]),
            datetime.fromtimestamp(1_725_000_003, UTC),
        )
        with self.provider.snapshot() as snapshot:
            participants = self.provider.list_participants(
                self.account_key, self.conversation, snapshot
            )
        self_activity = next(item for item in participants if item.is_self).last_spoke_at_utc
        self.assertEqual(self_activity, datetime.fromtimestamp(1_725_000_002, UTC).isoformat(
            timespec="microseconds"
        ))

    def test_find_participants_uses_evidenced_fast_path_after_admission(self) -> None:
        # After the conversation is admitted, participant discovery on a live source
        # must not rebuild the whole account catalog: it resolves the already-admitted
        # target from its persisted row, exactly like an ordinary message read. The
        # source roster is still read, so labels stay current-source evidence.
        tools = self._reader_tools()
        conversation_id = self._direct_conversation_id()
        # First call admits the conversation (and legitimately scans the catalog).
        first = tools.wechat_find_participants(conversation_id, "Fixture")
        self.assertTrue(first["candidates"], first)

        with (
            mock.patch.object(
                self.provider,
                "list_conversations",
                wraps=self.provider.list_conversations,
            ) as list_conversations,
            mock.patch.object(
                self.provider,
                "list_participants",
                wraps=self.provider.list_participants,
            ) as list_participants,
        ):
            second = tools.wechat_find_participants(conversation_id, "Fixture")

        self.assertTrue(second["candidates"], second)
        self.assertEqual(first["total_matches"], second["total_matches"])
        # The live target does not trigger an account-wide catalog rebuild...
        list_conversations.assert_not_called()
        # ...but the roster is still read as current-source evidence.
        self.assertEqual(list_participants.call_count, 1)

    def test_direct_outgoing_row_with_advanced_status_stays_self(self) -> None:
        # The supported native build also stores account-sent rows with status 3.
        self._insert_message(
            local_id=41,
            server_id=141,
            sort_seq=41,
            create_time=1_725_000_041,
            content="read by the peer",
            status=3,
        )

        tools = self._reader_tools()
        page = tools.wechat_read_messages(
            mode="recent",
            conversation_id=self._direct_conversation_id(),
            projection="detail",
            limit=10,
        )
        message = next(item for item in page["messages"] if item["text"] == "read by the peer")

        self.assertEqual(message["sender"]["label"], "我")
        self.assertTrue(message["sender"]["is_self"])

    def test_direct_row_with_unrecognized_status_fails_closed(self) -> None:
        self._insert_message(
            local_id=42,
            server_id=142,
            sort_seq=42,
            create_time=1_725_000_042,
            content="ambiguous direction",
            status=4,
        )

        tools = self._reader_tools()
        page = tools.wechat_read_messages(
            mode="recent",
            conversation_id=self._direct_conversation_id(),
            projection="detail",
            limit=10,
        )
        message = next(item for item in page["messages"] if item["text"] == "ambiguous direction")

        self.assertFalse(message["sender"]["is_self"])
        self.assertEqual(message["sender"]["identity_state"], "alias_only")
        self.assertEqual(message["sender"]["label_source"], "opaque_fallback")
        self.assertNotEqual(message["sender"]["label"], "Fixture Remark")

    def test_direct_system_row_is_not_a_peer_speaker(self) -> None:
        system_text = "你已添加了Fixture Contact，现在可以开始聊天了"
        self._insert_message(
            local_id=43,
            server_id=143,
            sort_seq=43,
            create_time=1_725_000_043,
            content=system_text,
            status=3,
            local_type=10000,
        )

        tools = self._reader_tools()
        conversation_id = self._direct_conversation_id()
        peer = tools.wechat_find_participants(conversation_id, "Fixture")["candidates"][0]

        recent = tools.wechat_read_messages(
            mode="recent",
            conversation_id=conversation_id,
            projection="detail", response_profile="diagnostic",
            limit=10,
        )
        system = next(item for item in recent["messages"] if item["text"] == system_text)
        self.assertEqual(system["kind"], "system")
        self.assertIsNone(system["sender"]["participant_id"])
        self.assertFalse(system["sender"]["is_self"])

        speaker = tools.wechat_read_messages(
            mode="speaker",
            conversation_id=conversation_id,
            participant_ids=[peer["participant_id"]],
            speaker_view="only",
            projection="detail",
            limit=10,
        )
        self.assertEqual([item["text"] for item in speaker["messages"]], ["first message"])

    def test_verified_group_sender_is_consistent_across_reader_entries(self) -> None:
        group, _member = self._add_group_sender_fixture()
        tools = self._reader_tools(group)
        account_id = opaque_id("wxacct", self.account_key)
        conversation_id = opaque_id("wxconv", account_id, group)
        tools.service.sync_source_once(
            initial_tail=20,
            batch_limit=20,
            conversation_limit=20,
        )

        recent = tools.wechat_read_messages(
            mode="recent",
            conversation_id=conversation_id,
            projection="detail",
            limit=10,
        )
        member_messages = [
            item for item in recent["messages"] if item["text"].startswith("member status")
        ]
        self.assertEqual(
            [item["text"] for item in member_messages],
            ["member status three", "member status four"],
        )
        member_id = member_messages[0]["sender"]["participant_id"]
        self.assertIsInstance(member_id, str)
        self.assertTrue(
            all(item["sender"]["participant_id"] == member_id for item in member_messages)
        )
        self.assertTrue(all(not item["sender"]["is_self"] for item in member_messages))
        self.assertTrue(
            all(item["sender"]["label"] == "Fixture Member Remark" for item in member_messages)
        )
        own = next(item for item in recent["messages"] if item["text"] == "self status three")
        self.assertTrue(own["sender"]["is_self"])

        target = member_messages[0]
        exact = tools.wechat_read_messages(mode="message", message_id=target["message_id"])
        context = tools.wechat_read_messages(
            mode="context",
            message_id=target["message_id"],
            before=0,
            after=0,
            projection="detail",
            limit=1,
        )
        speaker = tools.wechat_read_messages(
            mode="speaker",
            conversation_id=conversation_id,
            participant_ids=[member_id],
            speaker_view="only",
            projection="detail",
            limit=10,
        )
        self.assertEqual(exact["messages"][0]["sender"]["participant_id"], member_id)
        self.assertEqual(context["messages"][0]["sender"]["participant_id"], member_id)
        self.assertEqual(
            [item["text"] for item in speaker["messages"]],
            ["member status three", "member status four"],
        )
        self.assertTrue(
            all(item["sender"]["participant_id"] == member_id for item in speaker["messages"])
        )

        search = tools.wechat_search_messages(
            query="member status",
            conversation_ids=[conversation_id],
            participant_ids=[member_id],
            limit=10,
        )
        self.assertEqual([row[4] for row in search["hits"]], [
            "member status three",
            "member status four",
        ])
        self.assertTrue(
            all(search["people"][row[2]]["id"] == member_id for row in search["hits"])
        )

        compact = tools.wechat_read_messages(
            mode="recent",
            conversation_id=conversation_id,
            projection="compact",
            limit=10,
        )
        self.assertIn(member_id, {person["id"] for person in compact["people"]})
        inbox = tools.wechat_read_inbox(include_latest="text", limit=10)
        item = next(row for row in inbox["items"] if row["conversation_id"] == conversation_id)
        self.assertEqual(item["latest"]["text"], "member status four")
        self.assertEqual(item["latest"]["sender"]["participant_id"], member_id)
        self.assertFalse(item["latest"]["sender"]["is_self"])

    def test_projection_epoch_refreshes_an_unchanged_native_tail(self) -> None:
        group, _member = self._add_group_sender_fixture()
        tools = self._reader_tools(group)
        account_id = opaque_id("wxacct", self.account_key)
        conversation_id = opaque_id("wxconv", account_id, group)
        first = tools.service.sync_source_once(
            initial_tail=20,
            batch_limit=20,
            conversation_limit=20,
        )
        self.assertEqual(first["message_count"], 3)
        self.assertEqual(
            tools.service.sync_source_once(
                initial_tail=20,
                batch_limit=20,
                conversation_limit=20,
            )["conversation_count"],
            0,
        )

        with tools.service.repository.database.transaction() as connection:
            self_sender = connection.execute(
                """
                SELECT sender_id, sender_membership_id, sender_label_snapshot_json
                FROM messages
                WHERE conversation_id = ? AND text = 'self status three'
                """,
                (conversation_id,),
            ).fetchone()
            self.assertIsNotNone(self_sender)
            assert self_sender is not None
            self_participant_id = str(self_sender["sender_id"])
            connection.execute(
                """
                UPDATE messages
                SET sender_id = ?, sender_membership_id = ?, sender_label_snapshot_json = ?
                WHERE conversation_id = ? AND text = 'member status four'
                """,
                (
                    self_sender["sender_id"],
                    self_sender["sender_membership_id"],
                    self_sender["sender_label_snapshot_json"],
                    conversation_id,
                ),
            )
        stale = tools.wechat_read_inbox(include_latest="text", limit=10)
        stale_item = next(
            row for row in stale["items"] if row["conversation_id"] == conversation_id
        )
        self.assertTrue(stale_item["latest"]["sender"]["is_self"])

        with tools.service.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE source_catalog_state SET source_inventory_epoch = 'old-semantics' "
                "WHERE account_id = ?",
                (account_id,),
            )
            connection.execute(
                "UPDATE source_conversation_state SET source_inventory_epoch = 'old-semantics' "
                "WHERE conversation_id = ?",
                (conversation_id,),
            )

        refreshed = tools.service.sync_source_once(
            initial_tail=20,
            batch_limit=20,
            conversation_limit=20,
        )
        self.assertEqual(refreshed["conversation_count"], 1)
        self.assertEqual(refreshed["message_count"], 3)
        repaired = tools.wechat_read_inbox(include_latest="text", limit=10)
        repaired_item = next(
            row for row in repaired["items"] if row["conversation_id"] == conversation_id
        )
        self.assertFalse(repaired_item["latest"]["sender"]["is_self"])
        self.assertIsInstance(
            repaired_item["latest"]["sender"]["participant_id"],
            str,
        )
        self.assertNotEqual(
            repaired_item["latest"]["sender"]["participant_id"],
            self_participant_id,
        )

    def test_projection_epoch_stales_timeline_and_search_cursors(self) -> None:
        group, _member = self._add_group_sender_fixture("-cursor")
        tools = self._reader_tools(group)
        account_id = opaque_id("wxacct", self.account_key)
        conversation_id = opaque_id("wxconv", account_id, group)
        tools.service.sync_source_once(
            initial_tail=20,
            batch_limit=20,
            conversation_limit=20,
        )

        recent = tools.wechat_read_messages(
            mode="recent",
            conversation_id=conversation_id,
            projection="detail",
            limit=1,
        )
        timeline_cursor = recent["page"]["next_cursor"]
        self.assertIsInstance(timeline_cursor, str)
        timeline_payload = tools.service.token_codec.decode(timeline_cursor)
        timeline_payload["projection"]["epoch"] = "old-semantics"
        stale_timeline = tools.wechat_read_messages(
            mode="recent",
            conversation_id=conversation_id,
            projection="detail",
            limit=1,
            cursor=tools.service.token_codec.encode(timeline_payload),
        )
        self.assertEqual(stale_timeline["code"], "CURSOR_STALE")

        search = tools.wechat_search_messages(
            query="member status",
            conversation_ids=[conversation_id],
            limit=1,
        )
        search_cursor = search["page"]["next_cursor"]
        self.assertIsInstance(search_cursor, str)
        search_payload = tools.service.token_codec.decode(search_cursor)
        search_payload["source"]["projection_epoch"] = "old-semantics"
        stale_search = tools.wechat_search_messages(
            query="member status",
            conversation_ids=[conversation_id],
            limit=1,
            cursor=tools.service.token_codec.encode(search_payload),
        )
        self.assertEqual(stale_search["code"], "CURSOR_STALE")

    def _sync_until_ready(self, tools: ReaderTools, *, conversation_limit: int = 20) -> None:
        """Drive bounded syncs until the native inbox reports a page (or gives up)."""

        for _index in range(6):
            tools.service.sync_source_once(
                initial_tail=20,
                batch_limit=20,
                conversation_limit=conversation_limit,
            )
        result = tools.wechat_read_inbox(limit=10)
        self.assertEqual(result["schema"], "sightglass.inbox-page.v1", result)

    def test_degraded_conflict_does_not_block_readiness_or_leak_into_inbox(self) -> None:
        healthy = self._add_group_sender_fixture("-healthy")[0]
        conflicted = self._add_group_sender_fixture("-conflict")[0]
        self._add_conflicting_conversation_shard(
            suffix="9", conversation=conflicted, server_id=201
        )
        tools = self._reader_tools(self.conversation, healthy, conflicted)
        account_id = opaque_id("wxacct", self.account_key)
        healthy_id = opaque_id("wxconv", account_id, healthy)
        conflicted_id = opaque_id("wxconv", account_id, conflicted)

        self._sync_until_ready(tools)

        inbox = tools.wechat_read_inbox(limit=10, include_latest="text")
        returned = {item["conversation_id"] for item in inbox["items"]}
        self.assertIn(healthy_id, returned)
        self.assertIn(opaque_id("wxconv", account_id, self.conversation), returned)
        # The explicitly degraded conflict is excluded rather than served with its stale
        # sender projection, and the aggregate count makes that exclusion visible.
        self.assertNotIn(conflicted_id, returned)
        self.assertEqual(inbox["coverage"]["degraded_conversations"], 1)
        self.assertEqual(inbox["coverage"]["indexed_conversations"], len(returned))

        conflicted_state = tools.service.repository.source_conversation_state(conflicted_id)
        self.assertIsNotNone(conflicted_state)
        assert conflicted_state is not None
        self.assertEqual(
            str(conflicted_state["last_error_code"]),
            "duplicate_message_identity_conflict",
        )
        # A degraded conversation is never stamped with the current projection epoch.
        self.assertNotEqual(
            str(conflicted_state["source_inventory_epoch"] or ""),
            tools.service._projection_inventory_epoch(),
        )

    def test_conflict_recovery_readmits_and_expires_a_prior_inbox_cursor(self) -> None:
        first_healthy = self._add_group_sender_fixture("-healthy-a")[0]
        conflicted = self._add_group_sender_fixture("-conflict")[0]
        conflict_shard = self._add_conflicting_conversation_shard(
            suffix="9", conversation=conflicted, server_id=201
        )
        tools = self._reader_tools(self.conversation, first_healthy, conflicted)
        account_id = opaque_id("wxacct", self.account_key)
        conflicted_id = opaque_id("wxconv", account_id, conflicted)
        healthy_id = opaque_id("wxconv", account_id, first_healthy)
        direct_id = opaque_id("wxconv", account_id, self.conversation)

        self._sync_until_ready(tools)
        degraded = tools.wechat_read_inbox(limit=1)
        self.assertEqual(degraded["coverage"]["degraded_conversations"], 1)
        self.assertNotIn(
            conflicted_id, {item["conversation_id"] for item in degraded["items"]}
        )
        cursor = degraded["page"]["next_cursor"]
        self.assertIsNotNone(cursor)

        # Clear the conflict with a real source-generation change (the conflicting shard
        # disappears), then re-sync so the conversation is admitted normally.
        os.remove(self.source / conflict_shard)
        self.keys.pop(conflict_shard, None)
        self.provider._keys.pop(conflict_shard, None)  # noqa: SLF001 - fixture enrollment
        self.provider._snapshots.clear()  # noqa: SLF001 - fixture snapshot cache
        self._sync_until_ready(tools)

        recovered = tools.wechat_read_inbox(limit=10)
        recovered_ids = {item["conversation_id"] for item in recovered["items"]}
        self.assertEqual(recovered["coverage"]["degraded_conversations"], 0)
        self.assertEqual(recovered_ids, {direct_id, healthy_id, conflicted_id})
        state = tools.service.repository.source_conversation_state(conflicted_id)
        assert state is not None
        self.assertIsNone(state["last_error_code"])
        self.assertEqual(
            str(state["source_inventory_epoch"]),
            tools.service._projection_inventory_epoch(),
        )

        stale = tools.wechat_read_inbox(limit=1, cursor=cursor)
        self.assertEqual(stale["code"], "CURSOR_STALE")

    def test_non_conflict_missing_epoch_still_keeps_inbox_closed(self) -> None:
        group, _member = self._add_group_sender_fixture("-pending")
        tools = self._reader_tools(self.conversation, group)
        account_id = opaque_id("wxacct", self.account_key)
        group_id = opaque_id("wxconv", account_id, group)
        tools.service.sync_source_once(
            initial_tail=20,
            batch_limit=20,
            conversation_limit=20,
        )
        self.assertEqual(tools.wechat_read_inbox(limit=10)["schema"], "sightglass.inbox-page.v1")

        # A missing epoch with no conflict degradation is not "handled": the inbox must
        # stay fail-closed until that conversation is actually reprojected.
        with tools.service.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE source_catalog_state SET source_inventory_epoch = 'old-semantics' "
                "WHERE account_id = ?",
                (account_id,),
            )
            connection.execute(
                "UPDATE source_conversation_state SET source_inventory_epoch = 'old-semantics' "
                "WHERE conversation_id = ?",
                (group_id,),
            )

        pending = tools.wechat_read_inbox(limit=10)
        self.assertEqual(pending["code"], "SOURCE_INCOMPLETE")
        self.assertIn(
            "source_projection_refresh_pending",
            pending["details"]["warning_codes"],
        )

        self._sync_until_ready(tools, conversation_limit=1)
        self.assertEqual(tools.wechat_read_inbox(limit=10)["schema"], "sightglass.inbox-page.v1")

    def test_projection_promotion_uses_the_current_read_membership(self) -> None:
        first = self._add_group_sender_fixture("-member-a")[0]
        second = self._add_group_sender_fixture("-member-b")[0]
        tools = self._reader_tools(self.conversation, first, second)
        account_id = opaque_id("wxacct", self.account_key)
        repository = tools.service.repository
        projection_epoch = tools.service._projection_inventory_epoch()

        tools.service.sync_source_once(
            initial_tail=20,
            batch_limit=20,
            conversation_limit=20,
        )
        catalog_state = repository.source_catalog_state(account_id)
        assert catalog_state is not None
        self.assertEqual(
            str(catalog_state["source_inventory_epoch"]),
            projection_epoch,
        )

        # Every permitted conversation is now stale, and a real source change advances
        # the snapshot observation time (: this is what makes the persisted membership
        # view resolve to the *previous* observation during promotion). The next bounded
        # sync repairs only one conversation, so two current-catalog conversations remain
        # unrepaired and the projection epoch must not be promoted.
        with repository.database.transaction() as connection:
            connection.execute(
                "UPDATE source_conversation_state SET source_inventory_epoch = 'old-semantics'"
            )
        self._insert_message(
            local_id=801,
            server_id=901,
            sort_seq=801,
            create_time=1_725_090_100,
            content="generation bump for promotion test",
        )
        self.provider._snapshots.clear()  # noqa: SLF001 - fixture snapshot cache

        synced = tools.service.sync_source_once(
            initial_tail=20,
            batch_limit=20,
            conversation_limit=1,
        )
        self.assertEqual(synced["conversation_count"], 1)

        handled = tools.service._catalog_handled_ids(
            account_id,
            projection_epoch=projection_epoch,
        )
        current_catalog = repository.current_catalog_conversation_ids(account_id)
        self.assertEqual(len(current_catalog), 3)
        # A current-catalog conversation is still unrepaired...
        self.assertLess(len(handled & current_catalog), len(current_catalog))
        # ...so promotion must not have marked the whole epoch complete.
        promoted_state = repository.source_catalog_state(account_id)
        assert promoted_state is not None
        self.assertNotEqual(
            str(promoted_state["source_inventory_epoch"]),
            projection_epoch,
        )

        # Once the rotation repairs the rest, the epoch is promoted normally.
        for _index in range(3):
            tools.service.sync_source_once(
                initial_tail=20,
                batch_limit=20,
                conversation_limit=20,
            )
        final_state = repository.source_catalog_state(account_id)
        assert final_state is not None
        self.assertEqual(
            str(final_state["source_inventory_epoch"]),
            projection_epoch,
        )

    def test_historical_rows_outside_the_current_catalog_do_not_block_or_appear(self) -> None:
        group, _member = self._add_group_sender_fixture("-current")
        # Permit everything, so the historical rows below are policy-eligible and their
        # exclusion can only come from the current-catalog gate under test.
        tools = self._reader_tools_all_permitted()
        account_id = opaque_id("wxacct", self.account_key)

        tools.service.sync_source_once(
            initial_tail=20,
            batch_limit=20,
            conversation_limit=20,
        )
        self._sync_until_ready(tools)
        ready = tools.wechat_read_inbox(limit=10)
        ready_ids = {item["conversation_id"] for item in ready["items"]}
        self.assertEqual(len(ready_ids), 2)

        # Persist two older active conversations the latest complete catalog observation
        # never saw (their catalog_observed_at does not match the account's last
        # observation): one stamped with the *current* semantic epoch, and one carrying
        # the conflict attention code. Neither must block readiness, appear in the
        # filtered inbox, or inflate the degraded coverage count.
        projection_epoch = tools.service._projection_inventory_epoch()
        stale_id = opaque_id("wxconv", account_id, "synthetic-historical-only")
        stale_conflict_id = opaque_id("wxconv", account_id, "synthetic-historical-conflict")
        with tools.service.repository.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO conversations(
                    conversation_id, account_id, source_conversation_id, kind,
                    current_title, first_seen_at, last_seen_at, last_message_at,
                    visibility_state, roster_complete, catalog_state, unread_count,
                    catalog_observed_at
                ) VALUES (?, ?, ?, 'direct', 'Historical Synthetic',
                          '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00',
                          '2026-01-01T00:00:00+00:00', 'active', 0, 'history_only', 0,
                          'stale-observation')
                """,
                (stale_id, account_id, "synthetic-historical-only"),
            )
            connection.execute(
                """
                INSERT INTO conversations(
                    conversation_id, account_id, source_conversation_id, kind,
                    current_title, first_seen_at, last_seen_at, last_message_at,
                    visibility_state, roster_complete, catalog_state, unread_count,
                    catalog_observed_at
                ) VALUES (?, ?, ?, 'direct', 'Historical Conflict Synthetic',
                          '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00',
                          '2026-01-01T00:00:00+00:00', 'active', 0, 'history_only', 0,
                          'stale-observation')
                """,
                (stale_conflict_id, account_id, "synthetic-historical-conflict"),
            )
            connection.execute(
                """
                INSERT INTO messages(
                    message_id, account_id, conversation_id, source_message_id,
                    source_time_raw, sent_at_utc, sort_primary, sort_seq, sort_tie,
                    sender_label_snapshot_json, kind, structured_json, first_seen_at,
                    last_seen_at, current_state, current_generation_id
                ) VALUES (?, ?, ?, 'source-historical', '0', '2026-01-01T00:00:00+00:00',
                          '2026-01-01T00:00:00+00:00', 1, 1, '{}', 'text', '{}',
                          '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00',
                          'present', 'gen-1')
                """,
                (f"msg-{stale_id}", account_id, stale_id),
            )
            connection.execute(
                """
                INSERT INTO messages(
                    message_id, account_id, conversation_id, source_message_id,
                    source_time_raw, sent_at_utc, sort_primary, sort_seq, sort_tie,
                    sender_label_snapshot_json, kind, structured_json, first_seen_at,
                    last_seen_at, current_state, current_generation_id
                ) VALUES (?, ?, ?, 'source-historical-conflict', '0',
                          '2026-01-01T00:00:01+00:00', '2026-01-01T00:00:01+00:00', 2, 2,
                          '{}', 'text', '{}', '2026-01-01T00:00:01+00:00',
                          '2026-01-01T00:00:01+00:00', 'present', 'gen-1')
                """,
                (f"msg-{stale_conflict_id}", account_id, stale_conflict_id),
            )
            connection.execute(
                """
                INSERT INTO message_observations(
                    observation_id, message_id, observed_at, source_generation_id,
                    state, payload_digest, parsed_json, parser_version
                ) VALUES (?, ?, '2026-01-01T00:00:00+00:00', 'gen-1', 'present',
                          'digest-historical', '{}', '1')
                """,
                (f"obs-{stale_id}", f"msg-{stale_id}"),
            )
            connection.execute(
                """
                INSERT INTO message_observations(
                    observation_id, message_id, observed_at, source_generation_id,
                    state, payload_digest, parsed_json, parser_version
                ) VALUES (?, ?, '2026-01-01T00:00:01+00:00', 'gen-1', 'present',
                          'digest-historical-conflict', '{}', '1')
                """,
                (f"obs-{stale_conflict_id}", f"msg-{stale_conflict_id}"),
            )
            connection.execute(
                """
                INSERT INTO source_conversation_state(
                    conversation_id, source_inventory_epoch, backfill_state, updated_at
                ) VALUES (?, ?, 'complete', '2026-01-01T00:00:00+00:00')
                """,
                (stale_id, projection_epoch),
            )
            connection.execute(
                """
                INSERT INTO source_conversation_state(
                    conversation_id, source_inventory_epoch, backfill_state,
                    last_error_code, updated_at
                ) VALUES (?, NULL, 'partial', 'duplicate_message_identity_conflict',
                          '2026-01-01T00:00:01+00:00')
                """,
                (stale_conflict_id,),
            )

        self._sync_until_ready(tools)
        after = tools.wechat_read_inbox(limit=10)
        self.assertEqual(after["schema"], "sightglass.inbox-page.v1")
        returned = {item["conversation_id"] for item in after["items"]}
        self.assertEqual(returned, ready_ids)
        self.assertNotIn(stale_id, returned)
        self.assertNotIn(stale_conflict_id, returned)
        self.assertEqual(after["coverage"]["degraded_conversations"], 0)

    def test_projection_refresh_keeps_inbox_closed_until_all_permitted_tails_repair(
        self,
    ) -> None:
        groups = tuple(self._add_group_sender_fixture(str(index))[0] for index in range(3))
        tools = self._reader_tools(*groups)
        account_id = opaque_id("wxacct", self.account_key)
        conversation_ids = tuple(opaque_id("wxconv", account_id, group) for group in groups)
        tools.service.sync_source_once(
            initial_tail=20,
            batch_limit=20,
            conversation_limit=20,
        )
        self.assertEqual(tools.wechat_read_inbox(limit=10)["schema"], "sightglass.inbox-page.v1")

        with tools.service.repository.database.transaction() as connection:
            connection.execute(
                "UPDATE source_catalog_state SET source_inventory_epoch = 'old-semantics' "
                "WHERE account_id = ?",
                (account_id,),
            )
            connection.executemany(
                "UPDATE source_conversation_state "
                "SET source_inventory_epoch = 'old-semantics' WHERE conversation_id = ?",
                ((conversation_id,) for conversation_id in conversation_ids),
            )

        tools.service.queue_backfill(
            conversation_id=conversation_ids[-1],
            max_messages=1,
        )
        tools.service.process_backfill_once(batch_limit=1)
        backfilled_state = tools.service.repository.source_conversation_state(
            conversation_ids[-1]
        )
        self.assertIsNotNone(backfilled_state)
        assert backfilled_state is not None
        self.assertEqual(
            str(backfilled_state["source_inventory_epoch"]),
            "old-semantics",
        )

        first = tools.service.sync_source_once(
            initial_tail=20,
            batch_limit=20,
            conversation_limit=1,
        )
        self.assertEqual(first["conversation_count"], 1)
        pending = tools.wechat_read_inbox(limit=10)
        self.assertEqual(pending["code"], "SOURCE_INCOMPLETE")
        self.assertIn(
            "source_projection_refresh_pending",
            pending["details"]["warning_codes"],
        )

        for _index in range(2):
            tools.service.sync_source_once(
                initial_tail=20,
                batch_limit=20,
                conversation_limit=1,
            )
        repaired = tools.wechat_read_inbox(limit=10)
        self.assertEqual(repaired["schema"], "sightglass.inbox-page.v1")

    def test_generation_change_does_not_skip_a_second_changed_conversation(self) -> None:
        group, member = self._add_group_sender_fixture("-rotation")
        tools = self._reader_tools(self.conversation, group)
        account_id = opaque_id("wxacct", self.account_key)
        direct_id = opaque_id("wxconv", account_id, self.conversation)
        group_id = opaque_id("wxconv", account_id, group)
        tools.service.sync_source_once(
            initial_tail=20,
            batch_limit=20,
            conversation_limit=20,
        )

        self._insert_message(
            local_id=701,
            server_id=801,
            sort_seq=701,
            create_time=1_725_000_701,
            content="direct changed generation",
        )
        message = self._connect_new("message/message_0.db")
        try:
            member_rowid = int(
                message.execute(
                    "SELECT rowid FROM Name2Id WHERE user_name = ?",
                    (member,),
                ).fetchone()[0]
            )
            message.execute(
                f"INSERT INTO [{self._table_name(group)}] "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    702,
                    802,
                    1,
                    702,
                    member_rowid,
                    1_725_000_702,
                    4,
                    f"{member}:\ngroup changed generation",
                    0,
                    None,
                ),
            )
            message.commit()
        finally:
            message.close()
        session = self._connect_new("session/session.db")
        try:
            session.execute(
                "UPDATE SessionTable SET last_timestamp = ? WHERE username = ?",
                (1_725_000_701, self.conversation),
            )
            session.execute(
                "UPDATE SessionTable SET last_timestamp = ? WHERE username = ?",
                (1_725_000_702, group),
            )
            session.commit()
        finally:
            session.close()

        first = tools.service.sync_source_once(
            initial_tail=20,
            batch_limit=20,
            conversation_limit=1,
        )
        second = tools.service.sync_source_once(
            initial_tail=20,
            batch_limit=20,
            conversation_limit=1,
        )
        self.assertEqual(first["conversation_count"], 1)
        self.assertEqual(second["conversation_count"], 1)
        with tools.service.repository.database.connection() as connection:
            admitted = {
                str(row["text"])
                for row in connection.execute(
                    "SELECT text FROM messages WHERE conversation_id IN (?, ?)",
                    (direct_id, group_id),
                )
            }
        self.assertIn("direct changed generation", admitted)
        self.assertIn("group changed generation", admitted)

    def test_catalog_includes_contact_history_missing_from_active_sessions(self) -> None:
        archived = "synthetic-archived-contact"
        self._add_contact_history_without_session(archived)

        with self.provider.snapshot() as snapshot:
            conversations = self.provider.list_conversations(self.account_key, snapshot)
            source_ids = {item.source_conversation_id for item in conversations}
            complete = self.provider.catalog_complete(snapshot)
            active_only = self.provider.active_conversations_only(snapshot)

        self.assertEqual(source_ids, {self.conversation, archived})
        self.assertTrue(complete)
        self.assertFalse(active_only)

    def test_catalog_excludes_session_containers_without_message_tables(self) -> None:
        named_containers = (
            ("synthetic-official-container", "Official Accounts"),
            ("synthetic-minimized-container", "Minimized Groups"),
        )
        contact = self._connect_new("contact/contact.db")
        try:
            contact.executemany(
                "INSERT INTO contact VALUES (?, ?, '')", named_containers
            )
            contact.commit()
        finally:
            contact.close()
        session = self._connect_new("session/session.db")
        try:
            session.executemany(
                "INSERT INTO SessionTable VALUES (?, 0, ?)",
                (
                    (named_containers[0][0], 1_725_000_010),
                    (named_containers[1][0], 1_725_000_011),
                    ("synthetic-unnamed-container", 1_725_000_012),
                ),
            )
            session.commit()
        finally:
            session.close()

        excluded = {
            named_containers[0][0],
            named_containers[1][0],
            "synthetic-unnamed-container",
        }
        with self.provider.snapshot() as snapshot:
            conversations = self.provider.list_conversations(self.account_key, snapshot)
            source_ids = {item.source_conversation_id for item in conversations}
            self.assertFalse(excluded & source_ids)
            with self.assertRaises(SightglassError) as caught:
                self.provider.read_recent(
                    self.account_key, named_containers[0][0], 10, snapshot
                )
        self.assertEqual(caught.exception.code, ErrorCode.CONVERSATION_NOT_FOUND)

    def test_catalog_uses_name2id_for_history_absent_from_contacts_and_sessions(self) -> None:
        history_only = "synthetic-name2id-history"
        message = self._connect_new("message/message_0.db")
        try:
            message.execute("CREATE TABLE Name2Id(user_name TEXT, is_session INTEGER)")
            message.execute("INSERT INTO Name2Id VALUES (?, 1)", (history_only,))
            table = self._table_name(history_only)
            message.execute(
                f"""
                CREATE TABLE [{table}](
                    local_id INTEGER,
                    server_id INTEGER,
                    local_type INTEGER,
                    sort_seq INTEGER,
                    real_sender_id INTEGER,
                    create_time INTEGER,
                    status INTEGER,
                    message_content BLOB,
                    WCDB_CT_message_content INTEGER,
                    packed_info_data BLOB
                )
                """
            )
            message.execute(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (1, 902, 1, 1, 0, 1_724_000_002, 0, "name2id history", 0, None),
            )
            message.commit()
        finally:
            message.close()

        with self.provider.snapshot() as snapshot:
            conversations = self.provider.list_conversations(self.account_key, snapshot)
            source_ids = {item.source_conversation_id for item in conversations}
            complete = self.provider.catalog_complete(snapshot)

        self.assertIn(history_only, source_ids)
        self.assertTrue(complete)

    def test_catalog_reports_partial_when_a_message_table_cannot_be_mapped(self) -> None:
        message = self._connect_new("message/message_0.db")
        try:
            message.execute("CREATE TABLE [Msg_00000000000000000000000000000000](value INTEGER)")
            message.commit()
        finally:
            message.close()

        with self.provider.snapshot() as snapshot:
            self.provider.list_conversations(self.account_key, snapshot)
            complete = self.provider.catalog_complete(snapshot)

        self.assertFalse(complete)

    @staticmethod
    def _file_message_xml(
        name: str,
        data: bytes,
        *,
        declared_hash: str | None = None,
    ) -> str:
        digest = declared_hash or hashlib.md5(data).hexdigest()
        return (
            "<msg><appmsg><title>"
            f"{name}"
            "</title><type>6</type><appattach>"
            f"<totallen>{len(data)}</totallen><md5>{digest}</md5>"
            "<attachid>fixture-attachment</attachid><fileext>txt</fileext>"
            "</appattach></appmsg></msg>"
        )

    def _insert_file_message(
        self,
        *,
        name: str,
        data: bytes,
        declared_hash: str | None = None,
        local_id: int = 30,
        server_id: int = 130,
    ) -> None:
        self._insert_message(
            local_id=local_id,
            server_id=server_id,
            sort_seq=local_id,
            create_time=1_725_000_030,
            content=self._file_message_xml(name, data, declared_hash=declared_hash),
            local_type=49,
        )

    def test_native_file_resource_round_trips_through_reader_tools(self) -> None:
        data = b"alpha\nbeta\n"
        self._insert_file_message(name="fixture-notes.txt", data=data)
        directory = self.root / "msg" / "file" / "2024-08"
        directory.mkdir(parents=True)
        (directory / "fixture-notes.txt").write_bytes(data)

        tools = self._reader_tools()
        page = tools.wechat_read_messages(
            mode="recent",
            conversation_id=opaque_id(
                "wxconv", opaque_id("wxacct", self.account_key), self.conversation
            ),
            projection="detail",
            limit=10,
        )
        message = next(item for item in page["messages"] if item["kind"] == "file")
        listed = tools.wechat_list_resources(message["message_id"])
        self.assertEqual(len(listed["resources"]), 1)
        descriptor = listed["resources"][0]
        self.assertEqual(descriptor["original_name"], "fixture-notes.txt")
        self.assertEqual(descriptor["declared_hash"], hashlib.md5(data).hexdigest())
        self.assertEqual(descriptor["availability"], "local_available")
        self.assertTrue(descriptor["original_available"])
        self.assertNotIn(str(self.root), json.dumps(listed))
        self.assertNotIn("fixture-attachment", json.dumps(listed))

        read = tools.wechat_read_resource(
            descriptor["resource_id"],
            mode="text",
            start_line=2,
            end_line=2,
        )
        self.assertFalse(read.isError)
        self.assertIsNotNone(read.structuredContent)
        assert read.structuredContent is not None
        self.assertEqual(read.structuredContent["line_range"], {"start": 2, "end": 2})
        text_blocks = [
            item.resource
            for item in read.content
            if isinstance(item, EmbeddedResource)
            and isinstance(item.resource, TextResourceContents)
        ]
        self.assertEqual(len(text_blocks), 1)
        self.assertEqual(text_blocks[0].text, "beta")

    def test_native_voice_original_round_trips_as_audio_content(self) -> None:
        payload = b"\x02#!SILK_V3\nfixture-silk-payload"
        self._insert_message(
            local_id=34,
            server_id=134,
            sort_seq=34,
            create_time=1_725_000_034,
            content="",
            local_type=34,
        )
        self._insert_voice_payload(
            local_id=34,
            server_id=134,
            create_time=1_725_000_034,
            payload=payload,
        )

        tools = self._reader_tools()
        page = tools.wechat_read_messages(
            mode="recent",
            conversation_id=opaque_id(
                "wxconv", opaque_id("wxacct", self.account_key), self.conversation
            ),
            projection="compact",
            limit=10,
        )
        message = next(item for item in page["messages"] if item[3] == "voice")
        listed = tools.wechat_list_resources(message[0])
        self.assertEqual(len(listed["resources"]), 1)
        descriptor = listed["resources"][0]
        self.assertEqual(descriptor["kind"], "voice")
        self.assertEqual(descriptor["availability"], "local_available")
        self.assertEqual(descriptor["mime_type"], "audio/silk")
        self.assertTrue(descriptor["original_available"])
        self.assertFalse(descriptor["preview_available"])

        metadata = tools.wechat_read_resource(descriptor["resource_id"], mode="metadata")
        self.assertFalse(metadata.isError)
        assert metadata.structuredContent is not None
        self.assertEqual(metadata.structuredContent["mode"], "metadata")
        self.assertEqual(metadata.structuredContent["sniffed_mime_type"], "audio/silk")
        self.assertEqual(
            metadata.structuredContent["audio"],
            {"format": "silk", "mime_type": "audio/silk"},
        )

        read = tools.wechat_read_resource(descriptor["resource_id"], mode="original")
        self.assertFalse(read.isError)
        assert read.structuredContent is not None
        self.assertEqual(read.structuredContent["media"]["content_block_type"], "audio")
        audio = [item for item in read.content if isinstance(item, AudioContent)]
        self.assertEqual(len(audio), 1)
        self.assertEqual(audio[0].mimeType, "audio/silk")

    def test_native_high_word_flags_do_not_hide_the_base_message_type(self) -> None:
        data = b"flagged appmsg bytes\n"
        self._insert_message(
            local_id=31,
            server_id=131,
            sort_seq=31,
            create_time=1_725_000_031,
            content=self._file_message_xml("flagged.txt", data),
            local_type=(6 << 32) | 49,
        )
        directory = self.root / "msg" / "file" / "2024-08"
        directory.mkdir(parents=True)
        (directory / "flagged.txt").write_bytes(data)

        tools = self._reader_tools()
        page = tools.wechat_read_messages(
            mode="recent",
            conversation_id=opaque_id(
                "wxconv", opaque_id("wxacct", self.account_key), self.conversation
            ),
            projection="detail",
            limit=10,
        )
        message = next(item for item in page["messages"] if item["text"] == "flagged.txt")

        self.assertEqual(message["kind"], "file")
        listed = tools.wechat_list_resources(message["message_id"])
        self.assertEqual(len(listed["resources"]), 1)
        self.assertEqual(listed["resources"][0]["availability"], "local_available")

    def test_native_flagged_finder_share_uses_the_cached_thumbnail_preview(self) -> None:
        self._insert_message(
            local_id=32,
            server_id=132,
            sort_seq=32,
            create_time=1_725_000_032,
            content=(
                "<msg><appmsg><title>Finder clip</title><des>Clip description</des>"
                "<type>51</type><url>https://example.invalid/finder</url>"
                "<finderFeed><nickname>Finder creator</nickname></finderFeed>"
                "</appmsg></msg>"
            ),
            local_type=(51 << 32) | 49,
        )
        (self._image_thumbnail_directory() / "32_1725000032_thumb.jpg").write_bytes(_png_bytes())

        tools = self._reader_tools()
        page = tools.wechat_read_messages(
            mode="recent",
            conversation_id=opaque_id(
                "wxconv", opaque_id("wxacct", self.account_key), self.conversation
            ),
            projection="detail",
            include_resources="metadata",
            limit=10,
        )
        message = next(item for item in page["messages"] if item["text"] == "Finder clip")

        self.assertEqual(message["kind"], "link")
        self.assertEqual(len(message["resources"]), 1)
        descriptor = message["resources"][0]
        self.assertEqual(descriptor["kind"], "image")
        self.assertEqual(descriptor["availability"], "preview_only")
        preview = tools.wechat_read_resource(descriptor["resource_id"], mode="preview")
        self.assertFalse(preview.isError)
        self.assertTrue(any(isinstance(item, ImageContent) for item in preview.content))

    def test_native_numbered_file_variants_require_equivalent_content(self) -> None:
        data = b"same bytes\n"
        self._insert_file_message(name="duplicate.txt", data=data)
        directory = self.root / "msg" / "file" / "2024-08"
        directory.mkdir(parents=True)
        (directory / "duplicate.txt").write_bytes(data)
        (directory / "duplicate (1).txt").write_bytes(data)

        with self.provider.snapshot() as snapshot:
            message = next(
                item
                for item in self.provider.read_recent(
                    self.account_key, self.conversation, 20, snapshot
                ).messages
                if item.wechat_type == 49
            )
            payload = self.provider.read_resource(
                message.resources[0].source_resource_key or "",
                max_bytes=1024,
                snapshot=snapshot,
            )
        self.assertEqual(payload.data, data)

        (directory / "duplicate (1).txt").write_bytes(b"different\n")
        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                message = next(
                    item
                    for item in self.provider.read_recent(
                        self.account_key, self.conversation, 20, snapshot
                    ).messages
                    if item.wechat_type == 49
                )
                self.provider.read_resource(
                    message.resources[0].source_resource_key or "",
                    max_bytes=1024,
                    snapshot=snapshot,
                )
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)
        self.assertEqual(caught.exception.details["reason"], "resource_candidates_ambiguous")

    def test_native_resource_rejects_symlink_hardlink_and_declared_hash_mismatch(self) -> None:
        data = b"protected bytes\n"
        directory = self.root / "msg" / "file" / "2024-08"
        directory.mkdir(parents=True)

        self._insert_file_message(name="unsafe.txt", data=data)
        outside = self.root / "outside.txt"
        outside.write_bytes(data)
        os.symlink(outside, directory / "unsafe.txt")
        with self.assertRaises(SightglassError) as symlinked:
            with self.provider.snapshot() as snapshot:
                message = next(
                    item
                    for item in self.provider.read_recent(
                        self.account_key, self.conversation, 20, snapshot
                    ).messages
                    if item.wechat_type == 49
                )
                self.provider.read_resource(
                    message.resources[0].source_resource_key or "",
                    max_bytes=1024,
                    snapshot=snapshot,
                )
        self.assertEqual(symlinked.exception.code, ErrorCode.RESOURCE_BLOCKED)

        (directory / "unsafe.txt").unlink()
        os.link(outside, directory / "unsafe.txt")
        with self.assertRaises(SightglassError) as hardlinked:
            with self.provider.snapshot() as snapshot:
                message = next(
                    item
                    for item in self.provider.read_recent(
                        self.account_key, self.conversation, 20, snapshot
                    ).messages
                    if item.wechat_type == 49
                )
                self.provider.read_resource(
                    message.resources[0].source_resource_key or "",
                    max_bytes=1024,
                    snapshot=snapshot,
                )
        self.assertEqual(hardlinked.exception.code, ErrorCode.RESOURCE_BLOCKED)

        (directory / "unsafe.txt").unlink()
        (directory / "unsafe.txt").write_bytes(data)
        connection = self._connect_new("message/message_0.db")
        try:
            connection.execute(
                f"UPDATE [{self._table_name(self.conversation)}] "
                "SET message_content = ? WHERE local_id = 30",
                (self._file_message_xml("unsafe.txt", data, declared_hash="0" * 32),),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(SightglassError) as mismatched:
            with self.provider.snapshot() as snapshot:
                message = next(
                    item
                    for item in self.provider.read_recent(
                        self.account_key, self.conversation, 20, snapshot
                    ).messages
                    if item.wechat_type == 49
                )
                self.provider.read_resource(
                    message.resources[0].source_resource_key or "",
                    max_bytes=1024,
                    snapshot=snapshot,
                )
        self.assertEqual(mismatched.exception.code, ErrorCode.RESOURCE_BLOCKED)
        self.assertEqual(mismatched.exception.details["reason"], "declared_hash_mismatch")

    def test_native_resource_detects_file_mutation_during_read(self) -> None:
        data = b"stable bytes\n"
        self._insert_file_message(name="mutable.txt", data=data)
        directory = self.root / "msg" / "file" / "2024-08"
        directory.mkdir(parents=True)
        target = directory / "mutable.txt"
        target.write_bytes(data)
        real_read = os.read
        mutated = False

        def mutating_read(descriptor: int, amount: int) -> bytes:
            nonlocal mutated
            chunk = real_read(descriptor, amount)
            if chunk and not mutated:
                mutated = True
                target.write_bytes(b"changed bytes\n")
            return chunk

        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                message = next(
                    item
                    for item in self.provider.read_recent(
                        self.account_key, self.conversation, 20, snapshot
                    ).messages
                    if item.wechat_type == 49
                )
                with mock.patch(
                    "sightglass.source.macos_wechat.resources.os.read",
                    side_effect=mutating_read,
                ):
                    self.provider.read_resource(
                        message.resources[0].source_resource_key or "",
                        max_bytes=1024,
                        snapshot=snapshot,
                    )
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)

    _PLAIN_IMAGE = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00"
    )
    _IMAGE_XML_DIGEST = "f" * 32
    # The stored file hash names the payload entry. It is deliberately unrelated to
    # both the payload bytes and the digest carried in the message XML.
    _IMAGE_STEM = "9f" * 16
    _IMAGE_RESOURCE_DATABASE = "message/message_resource.db"
    _IMAGE_HARDLINK_DATABASE = "hardlink/hardlink.db"
    _IMAGE_MAPPING_KEYS = {
        _IMAGE_RESOURCE_DATABASE: "55" * 32,
        _IMAGE_HARDLINK_DATABASE: "66" * 32,
    }

    def _image_directory(self) -> Path:
        directory = (
            self.root
            / "msg"
            / "attach"
            / hashlib.md5(self.conversation.encode()).hexdigest()
            / "2024-08"
            / "Img"
        )
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _image_thumbnail_directory(self) -> Path:
        directory = (
            self.root
            / "cache"
            / "2024-08"
            / "Message"
            / hashlib.md5(self.conversation.encode()).hexdigest()
            / "Thumb"
        )
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _insert_image_message(
        self,
        *,
        local_id: int = 33,
        server_id: int = 133,
        xml_digest: str | None = None,
    ) -> None:
        selected = self._IMAGE_XML_DIGEST if xml_digest is None else xml_digest
        self._insert_message(
            local_id=local_id,
            server_id=server_id,
            sort_seq=local_id,
            create_time=1_725_000_000 + local_id,
            content=f'<msg><img md5="{selected}" /></msg>',
            local_type=3,
        )

    def _create_image_mapping_databases(self) -> None:
        (self.source / "hardlink").mkdir(exist_ok=True)
        for relative, key in self._IMAGE_MAPPING_KEYS.items():
            self.keys[relative] = key
            connection = self._connect_new(relative)
            try:
                if relative == self._IMAGE_RESOURCE_DATABASE:
                    connection.execute("CREATE TABLE ChatName2Id(user_name TEXT)")
                    connection.execute(
                        "CREATE TABLE MessageResourceInfo("
                        "chat_id INTEGER, message_local_id INTEGER, packed_info BLOB)"
                    )
                else:
                    connection.execute(
                        "CREATE TABLE image_hardlink_info_v4("
                        "md5 TEXT, file_name TEXT, dir1 INTEGER, dir2 INTEGER)"
                    )
                    connection.execute("CREATE TABLE dir2id(username TEXT)")
                connection.commit()
            finally:
                connection.close()
            os.chmod(self.source / relative, 0o600)

    def _enroll_image_mapping_databases(self) -> None:
        """Enroll the optional mapping keys exactly as an operator import would."""
        self._create_image_mapping_databases()
        self.key_file.write_text(
            json.dumps({name: {"enc_key": key} for name, key in self.keys.items()}),
            encoding="utf-8",
        )
        imported = import_verified_key_file(self.source, self.key_file)
        self.assertEqual(set(imported), set(self.keys))
        self.imported_keys = imported
        self.provider = MacOSWeChatSourceProvider(
            self.settings_path,
            secret_loader=lambda _account: encode_key_map(imported),
            candidate_discovery=lambda: (self.candidate,),
        )

    @staticmethod
    def _packed_image_info(stem: str) -> bytes:
        # ``packed_info`` carries field 2 -> field 1 with the 32-byte ASCII file hash.
        return bytes([0x12, 0x22, 0x0A, 0x20]) + stem.encode()

    def _add_image_resource_row(self, *, local_id: int, stem: str) -> None:
        connection = self._connect_new(self._IMAGE_RESOURCE_DATABASE)
        try:
            row = connection.execute(
                "SELECT rowid FROM ChatName2Id WHERE user_name = ? LIMIT 1",
                (self.conversation,),
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO ChatName2Id(user_name) VALUES (?)", (self.conversation,)
                )
                row = connection.execute(
                    "SELECT rowid FROM ChatName2Id WHERE user_name = ? LIMIT 1",
                    (self.conversation,),
                ).fetchone()
            assert row is not None
            connection.execute(
                "INSERT INTO MessageResourceInfo(chat_id, message_local_id, packed_info)"
                " VALUES (?, ?, ?)",
                (int(row[0]), local_id, self._packed_image_info(stem)),
            )
            connection.commit()
        finally:
            connection.close()
        os.chmod(self.source / self._IMAGE_RESOURCE_DATABASE, 0o600)

    def _add_image_hardlink_row(
        self, *, xml_digest: str, file_name: str, chat_dir: str, date_dir: str
    ) -> None:
        connection = self._connect_new(self._IMAGE_HARDLINK_DATABASE)
        try:
            identifiers: list[int] = []
            for username in (chat_dir, date_dir):
                connection.execute("INSERT INTO dir2id(username) VALUES (?)", (username,))
                row = connection.execute(
                    "SELECT rowid FROM dir2id WHERE username = ? ORDER BY rowid DESC LIMIT 1",
                    (username,),
                ).fetchone()
                assert row is not None
                identifiers.append(int(row[0]))
            connection.execute(
                "INSERT INTO image_hardlink_info_v4(md5, file_name, dir1, dir2)"
                " VALUES (?, ?, ?, ?)",
                (xml_digest, file_name, identifiers[0], identifiers[1]),
            )
            connection.commit()
        finally:
            connection.close()
        os.chmod(self.source / self._IMAGE_HARDLINK_DATABASE, 0o600)

    def _image_resource(self) -> Any:
        with self.provider.snapshot() as snapshot:
            message = next(
                item
                for item in self.provider.read_recent(
                    self.account_key, self.conversation, 20, snapshot
                ).messages
                if item.wechat_type == 3
            )
        return message.resources[0]

    def _read_image(self, resource: Any) -> Any:
        with self.provider.snapshot() as snapshot:
            return self.provider.read_resource(
                resource.source_resource_key or "",
                max_bytes=4096,
                snapshot=snapshot,
            )

    def test_native_image_is_resolved_from_the_resource_database_mapping(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        self._enroll_image_mapping_databases()
        self._add_image_resource_row(local_id=31, stem=self._IMAGE_STEM)
        (self._image_directory() / f"{self._IMAGE_STEM}.dat").write_bytes(self._PLAIN_IMAGE)

        resource = self._image_resource()

        self.assertEqual(resource.kind, "image")
        self.assertEqual(resource.availability, "local_available")
        # The stored file hash is locator evidence, never a published payload digest.
        self.assertIsNone(resource.declared_hash)
        self.assertNotEqual(self._IMAGE_STEM, hashlib.md5(self._PLAIN_IMAGE).hexdigest())
        payload = self._read_image(resource)
        self.assertEqual(payload.variant, "original")
        self.assertEqual(payload.data, self._PLAIN_IMAGE)

    def test_native_image_falls_back_to_the_hardlink_mapping(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        self._enroll_image_mapping_databases()
        # No MessageResourceInfo row, so the fallback mapping has to resolve it.
        self._add_image_hardlink_row(
            xml_digest=self._IMAGE_XML_DIGEST,
            file_name=f"{self._IMAGE_STEM}.dat",
            chat_dir=hashlib.md5(self.conversation.encode()).hexdigest(),
            date_dir="2024-08",
        )
        (self._image_directory() / f"{self._IMAGE_STEM}.dat").write_bytes(self._PLAIN_IMAGE)

        resource = self._image_resource()

        self.assertEqual(resource.availability, "local_available")
        self.assertEqual(self._read_image(resource).data, self._PLAIN_IMAGE)

    def test_native_image_xml_digest_is_never_used_as_a_stored_file_name(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        (self._image_directory() / f"{self._IMAGE_XML_DIGEST}.dat").write_bytes(self._PLAIN_IMAGE)

        resource = self._image_resource()

        self.assertEqual(resource.availability, "metadata_only")
        self.assertIsNone(resource.declared_hash)
        with self.assertRaises(SightglassError) as caught:
            self._read_image(resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)
        self.assertEqual(caught.exception.details["reason"], "resource_missing")

    def test_native_image_mapping_databases_stay_optional_for_source_health(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)

        health = self.provider.health()

        self.assertTrue(health.complete)
        self.assertEqual(health.shard_counts["present"], 3)
        resource = self._image_resource()
        self.assertEqual(resource.availability, "metadata_only")
        with self.assertRaises(SightglassError) as caught:
            self._read_image(resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)

    def test_native_image_requires_a_page1_verified_auxiliary_key(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        self._create_image_mapping_databases()
        # A key that does not verify against the current page 1 is dropped instead of
        # being silently enrolled for the optional mapping database.
        self.keys[self._IMAGE_RESOURCE_DATABASE] = "77" * 32
        self.key_file.write_text(
            json.dumps({name: {"enc_key": key} for name, key in self.keys.items()}),
            encoding="utf-8",
        )
        imported = import_verified_key_file(self.source, self.key_file)

        self.assertNotIn(self._IMAGE_RESOURCE_DATABASE, imported)
        self.assertIn(self._IMAGE_HARDLINK_DATABASE, imported)

    def test_native_image_prefers_the_full_entry_over_mid_and_thumbnail(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        self._enroll_image_mapping_databases()
        self._add_image_resource_row(local_id=31, stem=self._IMAGE_STEM)
        directory = self._image_directory()
        (directory / f"{self._IMAGE_STEM}_h.dat").write_bytes(b"full resolution payload")
        (directory / f"{self._IMAGE_STEM}.dat").write_bytes(b"mid resolution payload")
        (directory / f"{self._IMAGE_STEM}_t.dat").write_bytes(b"\xff\xd8\xff thumbnail")
        (self._image_thumbnail_directory() / "31_1725000031_thumb.jpg").write_bytes(
            b"\xff\xd8\xff cached preview"
        )

        resource = self._image_resource()

        self.assertEqual(resource.availability, "local_available")
        payload = self._read_image(resource)
        self.assertEqual(payload.variant, "original")
        self.assertEqual(payload.data, b"full resolution payload")

    def test_native_image_serves_the_mid_entry_when_the_full_entry_is_absent(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        self._enroll_image_mapping_databases()
        self._add_image_resource_row(local_id=31, stem=self._IMAGE_STEM)
        (self._image_directory() / f"{self._IMAGE_STEM}.dat").write_bytes(b"mid resolution payload")

        resource = self._image_resource()

        self.assertEqual(resource.availability, "local_available")
        payload = self._read_image(resource)
        self.assertEqual(payload.variant, "original")
        self.assertEqual(payload.data, b"mid resolution payload")

    def test_native_image_thumbnail_entry_is_only_a_preview(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        self._enroll_image_mapping_databases()
        self._add_image_resource_row(local_id=31, stem=self._IMAGE_STEM)
        (self._image_directory() / f"{self._IMAGE_STEM}_t.dat").write_bytes(b"thumbnail")

        resource = self._image_resource()

        self.assertEqual(resource.availability, "preview_only")
        payload = self._read_image(resource)
        self.assertEqual(payload.variant, "thumbnail")
        self.assertEqual(payload.data, b"thumbnail")

    def test_native_image_cache_thumbnail_is_a_bounded_preview_source(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        (self._image_thumbnail_directory() / "31_1725000031_thumb.jpg").write_bytes(
            b"\xff\xd8\xff cached preview"
        )

        resource = self._image_resource()

        self.assertEqual(resource.availability, "preview_only")
        payload = self._read_image(resource)
        self.assertEqual(payload.variant, "thumbnail")
        self.assertEqual(payload.data, b"\xff\xd8\xff cached preview")

    def test_native_image_ambiguous_resource_rows_fail_closed(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        self._enroll_image_mapping_databases()
        self._add_image_resource_row(local_id=31, stem=self._IMAGE_STEM)
        self._add_image_resource_row(local_id=31, stem="b" * 32)

        resource = self._image_resource()

        self.assertEqual(resource.availability, "metadata_only")
        with self.assertRaises(SightglassError) as caught:
            self._read_image(resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)

    def test_native_image_mapping_mutation_invalidates_the_snapshot(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        self._enroll_image_mapping_databases()
        self._add_image_resource_row(local_id=31, stem=self._IMAGE_STEM)

        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                self.provider.read_recent(self.account_key, self.conversation, 20, snapshot)
                self._add_image_resource_row(local_id=32, stem="c" * 32)

        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)

    def test_native_image_rejects_symlink_and_hardlink_variants(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        self._enroll_image_mapping_databases()
        self._add_image_resource_row(local_id=31, stem=self._IMAGE_STEM)
        directory = self._image_directory()
        outside = self.root / "outside-image.dat"
        outside.write_bytes(self._PLAIN_IMAGE)

        os.symlink(outside, directory / f"{self._IMAGE_STEM}.dat")
        self.assertEqual(self._image_resource().availability, "blocked_by_policy")

        (directory / f"{self._IMAGE_STEM}.dat").unlink()
        os.link(outside, directory / f"{self._IMAGE_STEM}.dat")
        resource = self._image_resource()
        self.assertEqual(resource.availability, "blocked_by_policy")
        with self.assertRaises(SightglassError) as caught:
            self._read_image(resource)
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)

        (directory / f"{self._IMAGE_STEM}.dat").unlink()
        os.symlink(outside, directory / f"{self._IMAGE_STEM}_h.dat")
        self.assertEqual(self._image_resource().availability, "blocked_by_policy")

    def test_native_image_detects_entry_mutation_during_read(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        self._enroll_image_mapping_databases()
        self._add_image_resource_row(local_id=31, stem=self._IMAGE_STEM)
        target = self._image_directory() / f"{self._IMAGE_STEM}.dat"
        target.write_bytes(self._PLAIN_IMAGE)
        real_read = os.read
        mutated = False

        def mutating_read(descriptor: int, amount: int) -> bytes:
            nonlocal mutated
            chunk = real_read(descriptor, amount)
            if chunk and not mutated and amount > 64:
                mutated = True
                target.write_bytes(b"\x89PNG\r\n\x1a\nchanged bytes")
            return chunk

        resource = self._image_resource()
        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                with mock.patch(
                    "sightglass.source.macos_wechat.resources.os.read",
                    side_effect=mutating_read,
                ):
                    self.provider.read_resource(
                        resource.source_resource_key or "",
                        max_bytes=4096,
                        snapshot=snapshot,
                    )
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)

    def test_native_image_descriptor_is_path_free_through_reader_tools(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        self._enroll_image_mapping_databases()
        self._add_image_resource_row(local_id=31, stem=self._IMAGE_STEM)
        (self._image_directory() / f"{self._IMAGE_STEM}.dat").write_bytes(self._PLAIN_IMAGE)

        tools = self._reader_tools()
        page = tools.wechat_read_messages(
            mode="recent",
            conversation_id=opaque_id(
                "wxconv", opaque_id("wxacct", self.account_key), self.conversation
            ),
            projection="detail",
            limit=10,
        )
        message = next(item for item in page["messages"] if item["kind"] == "image")
        listed = tools.wechat_list_resources(message["message_id"], response_profile="diagnostic")
        self.assertEqual(len(listed["resources"]), 1)
        descriptor = listed["resources"][0]
        self.assertEqual(descriptor["kind"], "image")
        self.assertEqual(descriptor["availability"], "local_available")
        self.assertIsNone(descriptor["declared_hash"])
        serialized = json.dumps(listed)
        self.assertNotIn(str(self.root), serialized)
        self.assertNotIn(self._IMAGE_STEM, serialized)
        self.assertNotIn(self._IMAGE_XML_DIGEST, serialized)
        self.assertNotIn(".dat", serialized)
        self.assertNotIn("Thumb", serialized)

    def test_native_image_preview_reads_the_bounded_cache_thumbnail(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        (self._image_thumbnail_directory() / "31_1725000031_thumb.jpg").write_bytes(_png_bytes())

        tools = self._reader_tools()
        page = tools.wechat_read_messages(
            mode="recent",
            conversation_id=opaque_id(
                "wxconv", opaque_id("wxacct", self.account_key), self.conversation
            ),
            projection="detail",
            limit=10,
        )
        message = next(item for item in page["messages"] if item["kind"] == "image")
        descriptor = tools.wechat_list_resources(message["message_id"])["resources"][0]

        self.assertEqual(descriptor["availability"], "preview_only")
        self.assertFalse(descriptor["original_available"])
        self.assertTrue(descriptor["preview_available"])

        resource_id = str(descriptor["resource_id"])
        preview = tools.wechat_read_resource(resource_id=resource_id, mode="preview")
        self.assertFalse(preview.isError)
        original = tools.wechat_read_resource(resource_id=resource_id, mode="original")
        self.assertTrue(original.isError)
        self.assertIsNotNone(original.structuredContent)
        assert original.structuredContent is not None
        self.assertEqual(original.structuredContent["code"], "RESOURCE_UNAVAILABLE")

    def test_native_v2_image_without_decoder_key_is_explicitly_unavailable(self) -> None:
        self._insert_image_message(local_id=32, server_id=132)
        self._enroll_image_mapping_databases()
        self._add_image_resource_row(local_id=32, stem=self._IMAGE_STEM)
        (self._image_directory() / f"{self._IMAGE_STEM}_h.dat").write_bytes(
            b"\x07\x08V2\x08\x07" + b"encrypted"
        )

        resource = self._image_resource()
        with self.assertRaises(SightglassError) as caught:
            self._read_image(resource)

        self.assertEqual(resource.availability, "key_missing")
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)
        self.assertEqual(caught.exception.details["reason"], "image_decoder_key_missing")

    def test_native_locked_image_original_falls_back_to_the_cached_preview(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        self._enroll_image_mapping_databases()
        self._add_image_resource_row(local_id=31, stem=self._IMAGE_STEM)
        (self._image_directory() / f"{self._IMAGE_STEM}_h.dat").write_bytes(
            b"\x07\x08V2\x08\x07" + b"encrypted"
        )
        (self._image_thumbnail_directory() / "31_1725000031_thumb.jpg").write_bytes(_png_bytes())

        tools = self._reader_tools()
        page = tools.wechat_read_messages(
            mode="recent",
            conversation_id=opaque_id(
                "wxconv", opaque_id("wxacct", self.account_key), self.conversation
            ),
            projection="detail",
            limit=10,
        )
        message = next(item for item in page["messages"] if item["kind"] == "image")
        descriptor = tools.wechat_list_resources(message["message_id"])["resources"][0]

        # A decodable exact message-positioned preview is what this installation can
        # actually serve, so it owns the descriptor state instead of key_missing.
        self.assertEqual(descriptor["availability"], "preview_only")
        self.assertTrue(descriptor["preview_available"])
        self.assertFalse(descriptor["original_available"])

        resource_id = str(descriptor["resource_id"])
        preview = tools.wechat_read_resource(resource_id=resource_id, mode="preview")
        self.assertFalse(preview.isError)
        self.assertTrue(any(isinstance(item, ImageContent) for item in preview.content))

        original = tools.wechat_read_resource(resource_id=resource_id, mode="original")
        self.assertTrue(original.isError)
        self.assertIsNotNone(original.structuredContent)
        assert original.structuredContent is not None
        self.assertEqual(original.structuredContent["code"], "RESOURCE_UNAVAILABLE")
        self.assertNotIn(self._IMAGE_STEM, json.dumps(preview.structuredContent))

    def _enrolled_image_provider(self, key_loader: Any) -> MacOSWeChatSourceProvider:
        account = image_decoder_keychain_account(self.account_binding_id)
        replace(
            MacOSWeChatSettings.load(self.settings_path),
            image_keychain_account=account,
        ).save(self.settings_path)
        return MacOSWeChatSourceProvider(
            self.settings_path,
            secret_loader=lambda _account: encode_key_map(self.imported_keys),
            candidate_discovery=lambda: (self.candidate,),
            image_key_loader=key_loader,
        )

    def test_native_v2_image_decodes_with_an_enrolled_account_bound_key(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        self._enroll_image_mapping_databases()
        self._add_image_resource_row(local_id=31, stem=self._IMAGE_STEM)
        (self._image_directory() / f"{self._IMAGE_STEM}.dat").write_bytes(
            _v2_image_bytes(self._PLAIN_IMAGE)
        )
        requested: list[str] = []

        def load(account: str) -> str:
            requested.append(account)
            return SYNTHETIC_IMAGE_KEY.hex()

        provider = self._enrolled_image_provider(load)
        with provider.snapshot() as snapshot:
            message = next(
                item
                for item in provider.read_recent(
                    self.account_key, self.conversation, 20, snapshot
                ).messages
                if item.wechat_type == 3
            )
            resource = message.resources[0]
            payload = provider.read_resource(
                resource.source_resource_key or "", max_bytes=4096, snapshot=snapshot
            )

        self.assertEqual(requested, [image_decoder_keychain_account(self.account_binding_id)])
        self.assertEqual(resource.availability, "local_available")
        self.assertEqual(payload.variant, "original")
        self.assertEqual(payload.data, self._PLAIN_IMAGE)

    def test_native_malformed_enrolled_key_stays_explicit_fail_closed(self) -> None:
        self._insert_image_message(local_id=31, server_id=131)
        self._enroll_image_mapping_databases()
        self._add_image_resource_row(local_id=31, stem=self._IMAGE_STEM)
        (self._image_directory() / f"{self._IMAGE_STEM}.dat").write_bytes(
            _v2_image_bytes(self._PLAIN_IMAGE)
        )

        provider = self._enrolled_image_provider(lambda _account: "not-a-key")
        with provider.snapshot() as snapshot:
            message = next(
                item
                for item in provider.read_recent(
                    self.account_key, self.conversation, 20, snapshot
                ).messages
                if item.wechat_type == 3
            )
            resource = message.resources[0]
            with self.assertRaises(SightglassError) as caught:
                provider.read_resource(
                    resource.source_resource_key or "",
                    max_bytes=4096,
                    snapshot=snapshot,
                )

        self.assertEqual(resource.availability, "key_missing")
        self.assertEqual(caught.exception.details["reason"], "image_decoder_key_missing")

    def _insert_video_message(
        self,
        *,
        digests: tuple[str, ...] = (),
        local_id: int = 50,
        server_id: int = 150,
    ) -> None:
        packed = b""
        for index, digest in enumerate(digests):
            # Length-delimited field N|2, matching the resource metadata envelope shape.
            field = 0x0A if index == 0 else 0x0A + index * 8
            packed += bytes([field, len(digest)]) + digest.encode()
        self._insert_message(
            local_id=local_id,
            server_id=server_id,
            sort_seq=local_id,
            create_time=1_725_000_050,
            content="<msg><videomsg /></msg>",
            local_type=43,
            packed_info_data=packed or None,
        )

    def _video_month_directory(self) -> Path:
        directory = self.root / "msg" / "video" / "2024-08"
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _video_resource(self) -> Any:
        return self._video_resources()[0]

    def _video_resources(self) -> list[Any]:
        with self.provider.snapshot() as snapshot:
            messages = [
                item
                for item in self.provider.read_recent(
                    self.account_key, self.conversation, 20, snapshot
                ).messages
                if item.wechat_type == 43
            ]
        return [message.resources[0] for message in messages]

    def test_native_video_local_payload_is_probed_and_readable(self) -> None:
        digest = "b" * 32
        data = b"\x00\x00\x00\x18ftypmp42" + b"synthetic-video-payload"
        self._insert_video_message(digests=(digest,))
        (self._video_month_directory() / f"{digest}.mp4").write_bytes(data)

        resource = self._video_resource()
        self.assertEqual(resource.kind, "video")
        self.assertEqual(resource.availability, "local_available")
        self.assertIsNotNone(resource.source_resource_key)
        with self.provider.snapshot() as snapshot:
            payload = self.provider.read_resource(
                resource.source_resource_key or "",
                max_bytes=4096,
                snapshot=snapshot,
            )
        self.assertEqual(payload.data, data)
        serialized = json.dumps(resource.as_dict())
        self.assertNotIn(str(self.root), serialized)
        self.assertNotIn(digest, serialized)
        self.assertNotIn(".mp4", serialized)

    def test_native_video_descriptor_is_path_free_through_reader_tools(self) -> None:
        digest = "b" * 32
        self._insert_video_message(digests=(digest,))
        (self._video_month_directory() / f"{digest}.mp4").write_bytes(b"synthetic-video")

        tools = self._reader_tools()
        page = tools.wechat_read_messages(
            mode="recent",
            conversation_id=opaque_id(
                "wxconv", opaque_id("wxacct", self.account_key), self.conversation
            ),
            projection="detail",
            limit=10,
        )
        message = next(item for item in page["messages"] if item["kind"] == "video")
        listed = tools.wechat_list_resources(message["message_id"], response_profile="diagnostic")
        self.assertEqual(len(listed["resources"]), 1)
        descriptor = listed["resources"][0]
        self.assertEqual(descriptor["kind"], "video")
        self.assertEqual(descriptor["availability"], "local_available")
        self.assertTrue(descriptor["original_available"])
        self.assertIsNone(descriptor["declared_hash"])
        self.assertIsNone(descriptor["original_name"])
        serialized = json.dumps(listed)
        self.assertNotIn(str(self.root), serialized)
        self.assertNotIn(digest, serialized)
        self.assertNotIn(".mp4", serialized)

    def test_native_video_absent_payload_inside_observed_layout_is_missing(self) -> None:
        digest = "c" * 32
        self._insert_video_message(digests=(digest,))
        directory = self._video_month_directory()
        (directory / f"{'d' * 32}.mp4").write_bytes(b"another conversation video")

        resource = self._video_resource()
        self.assertEqual(resource.availability, "missing")
        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                self.provider.read_resource(
                    resource.source_resource_key or "",
                    max_bytes=4096,
                    snapshot=snapshot,
                )
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)
        self.assertEqual(caught.exception.details["reason"], "resource_missing")

    def test_native_video_unobserved_layout_is_not_reported_missing(self) -> None:
        digest = "e" * 32
        self._insert_video_message(digests=(digest,))

        resource = self._video_resource()
        self.assertEqual(resource.availability, "metadata_only")
        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                self.provider.read_resource(
                    resource.source_resource_key or "",
                    max_bytes=4096,
                    snapshot=snapshot,
                )
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)
        self.assertEqual(caught.exception.details["reason"], "resource_layout_unobserved")

    def test_native_video_unobserved_name_convention_is_not_reported_missing(self) -> None:
        digest = "9" * 32
        self._insert_video_message(digests=(digest,))
        (self._video_month_directory() / "notes.txt").write_bytes(b"not a digest-named payload")

        resource = self._video_resource()
        self.assertEqual(resource.availability, "metadata_only")
        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                self.provider.read_resource(
                    resource.source_resource_key or "",
                    max_bytes=4096,
                    snapshot=snapshot,
                )
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_UNAVAILABLE)
        self.assertEqual(caught.exception.details["reason"], "resource_layout_unobserved")

    def test_native_video_ambiguous_locator_evidence_stays_metadata_only(self) -> None:
        self._insert_video_message(digests=())
        self._video_month_directory()
        self.assertEqual(self._video_resource().availability, "metadata_only")

        self._insert_video_message(digests=("1" * 32, "2" * 32), local_id=51, server_id=151)
        self.assertEqual(
            [resource.availability for resource in self._video_resources()],
            ["metadata_only", "metadata_only"],
        )

    def test_native_video_ambiguous_variants_fail_closed(self) -> None:
        digest = "f" * 32
        self._insert_video_message(digests=(digest,))
        directory = self._video_month_directory()
        (directory / f"{digest}.mp4").write_bytes(b"compressed variant")
        (directory / f"{digest}_raw.mp4").write_bytes(b"original variant")

        resource = self._video_resource()
        self.assertEqual(resource.availability, "metadata_only")
        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                self.provider.read_resource(
                    resource.source_resource_key or "",
                    max_bytes=4096,
                    snapshot=snapshot,
                )
        self.assertEqual(caught.exception.code, ErrorCode.RESOURCE_BLOCKED)
        self.assertEqual(caught.exception.details["reason"], "resource_candidates_ambiguous")

    def test_native_video_rejects_symlink_hardlink_and_mutation(self) -> None:
        digest = "a" * 32
        data = b"synthetic video bytes\n"
        self._insert_video_message(digests=(digest,))
        directory = self._video_month_directory()
        outside = self.root / "outside-video.mp4"
        outside.write_bytes(data)

        os.symlink(outside, directory / f"{digest}.mp4")
        self.assertEqual(self._video_resource().availability, "blocked_by_policy")
        (directory / f"{digest}.mp4").unlink()

        os.link(outside, directory / f"{digest}.mp4")
        self.assertEqual(self._video_resource().availability, "blocked_by_policy")
        (directory / f"{digest}.mp4").unlink()

        target = directory / f"{digest}.mp4"
        target.write_bytes(data)
        real_read = os.read
        mutated = False

        def mutating_read(descriptor: int, amount: int) -> bytes:
            nonlocal mutated
            chunk = real_read(descriptor, amount)
            if chunk and not mutated:
                mutated = True
                target.write_bytes(b"changed video bytes\n")
            return chunk

        resource = self._video_resource()
        with self.assertRaises(SightglassError) as caught:
            with self.provider.snapshot() as snapshot:
                with mock.patch(
                    "sightglass.source.macos_wechat.resources.os.read",
                    side_effect=mutating_read,
                ):
                    self.provider.read_resource(
                        resource.source_resource_key or "",
                        max_bytes=4096,
                        snapshot=snapshot,
                    )
        self.assertEqual(caught.exception.code, ErrorCode.SOURCE_GENERATION_CHANGED)

    def test_native_missing_voice_and_unresolved_sticker_fail_closed(self) -> None:
        for offset, local_type in enumerate((34, 47), start=40):
            self._insert_message(
                local_id=offset,
                server_id=100 + offset,
                sort_seq=offset,
                create_time=1_725_000_000 + offset,
                content="transport envelope",
                local_type=local_type,
            )

        with self.provider.snapshot() as snapshot:
            messages = self.provider.read_recent(
                self.account_key, self.conversation, 20, snapshot
            ).messages

        resources = {
            message.wechat_type: message.resources[0]
            for message in messages
            if message.wechat_type in {34, 47}
        }
        self.assertEqual(
            {kind: resource.kind for kind, resource in resources.items()},
            {34: "voice", 47: "sticker"},
        )
        self.assertEqual(resources[34].availability, "missing")
        self.assertEqual(resources[47].availability, "metadata_only")
        self.assertTrue(
            all(resource.source_resource_key is None for resource in resources.values())
        )

"""Optional native media-database lookup for WeChat voice originals.

The message shards identify a voice message, while encrypted ``media_*.db``
shards keep its source bytes.  This module only correlates already verified,
read-only auxiliary handles; it never owns key discovery or reader policy.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sightglass.contracts.errors import ErrorCode, SightglassError

MEDIA_DATABASE = re.compile(r"^message/media_\d+\.db$")
_VOICE_COLUMNS = frozenset({"chat_name_id", "local_id", "svr_id", "create_time", "voice_data"})
_NAME_COLUMNS = frozenset({"user_name"})


@dataclass(frozen=True)
class NativeVoiceEntry:
    database: str
    conversation_source_id: str
    chat_name_id: int
    local_id: int
    server_id: int
    create_time: int


@dataclass(frozen=True)
class NativeVoiceResolution:
    entry: NativeVoiceEntry | None
    availability: str


class NativeVoiceIndex:
    """Resolve one voice row without exposing database or conversation identities."""

    def __init__(
        self,
        source_root: Path,
        *,
        databases: tuple[str, ...],
        open_database: Callable[[str], AbstractContextManager[Any]],
    ) -> None:
        self.source_root = source_root.resolve()
        self.databases = tuple(
            sorted(relative for relative in databases if MEDIA_DATABASE.fullmatch(relative))
        )
        self._open_database = open_database

    @staticmethod
    def _columns(connection: Any, table: str) -> frozenset[str]:
        escaped = table.replace('"', '""')
        return frozenset(
            str(row[1]) for row in connection.execute(f'PRAGMA table_info("{escaped}")')
        )

    @classmethod
    def _schema_supported(cls, connection: Any) -> bool:
        return _NAME_COLUMNS <= cls._columns(
            connection, "Name2Id"
        ) and _VOICE_COLUMNS <= cls._columns(connection, "VoiceInfo")

    def _media_layout_observed(self) -> bool:
        directory = self.source_root / "message"
        try:
            if directory.is_symlink() or not directory.is_dir():
                return False
            return any(
                MEDIA_DATABASE.fullmatch(f"message/{path.name}")
                and path.is_file()
                and not path.is_symlink()
                for path in directory.iterdir()
            )
        except OSError:
            return False

    def resolve(
        self,
        *,
        conversation_source_id: str,
        local_id: int,
        server_id: int,
        create_time: int,
    ) -> NativeVoiceResolution:
        if not self.databases:
            return NativeVoiceResolution(
                entry=None,
                availability=("key_missing" if self._media_layout_observed() else "metadata_only"),
            )

        matches: list[NativeVoiceEntry] = []
        supported = 0
        unavailable = 0
        ambiguous = False
        for relative in self.databases:
            try:
                with self._open_database(relative) as connection:
                    if not self._schema_supported(connection):
                        continue
                    supported += 1
                    chat_rows = connection.execute(
                        "SELECT rowid FROM Name2Id WHERE user_name = ? LIMIT 2",
                        (conversation_source_id,),
                    ).fetchall()
                    if len(chat_rows) != 1:
                        ambiguous = ambiguous or len(chat_rows) > 1
                        continue
                    chat_name_id = int(chat_rows[0][0])
                    voice_rows = connection.execute(
                        "SELECT 1 FROM VoiceInfo "
                        "WHERE chat_name_id = ? AND local_id = ? AND svr_id = ? "
                        "AND create_time = ? LIMIT 2",
                        (chat_name_id, local_id, server_id, create_time),
                    ).fetchall()
                    if len(voice_rows) == 1:
                        matches.append(
                            NativeVoiceEntry(
                                database=relative,
                                conversation_source_id=conversation_source_id,
                                chat_name_id=chat_name_id,
                                local_id=local_id,
                                server_id=server_id,
                                create_time=create_time,
                            )
                        )
                    elif len(voice_rows) > 1:
                        ambiguous = True
            except (SightglassError, OSError, TypeError, ValueError):
                unavailable += 1

        if ambiguous or unavailable or not supported:
            return NativeVoiceResolution(entry=None, availability="metadata_only")
        if len(matches) == 1:
            return NativeVoiceResolution(entry=matches[0], availability="local_available")
        if len(matches) > 1:
            return NativeVoiceResolution(entry=None, availability="metadata_only")
        return NativeVoiceResolution(entry=None, availability="missing")

    def read(self, entry: NativeVoiceEntry, *, max_bytes: int) -> bytes:
        if max_bytes < 1 or not MEDIA_DATABASE.fullmatch(entry.database):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        with self._open_database(entry.database) as connection:
            if not self._schema_supported(connection):
                raise SightglassError(
                    ErrorCode.RESOURCE_UNAVAILABLE,
                    details={"reason": "voice_schema_unavailable"},
                )
            chat_rows = connection.execute(
                "SELECT rowid FROM Name2Id WHERE user_name = ? LIMIT 2",
                (entry.conversation_source_id,),
            ).fetchall()
            if len(chat_rows) != 1 or int(chat_rows[0][0]) != entry.chat_name_id:
                raise SightglassError(
                    ErrorCode.RESOURCE_BLOCKED,
                    details={"reason": "voice_conversation_binding_changed"},
                )
            rows = connection.execute(
                "SELECT voice_data FROM VoiceInfo "
                "WHERE chat_name_id = ? AND local_id = ? AND svr_id = ? "
                "AND create_time = ? LIMIT 2",
                (
                    entry.chat_name_id,
                    entry.local_id,
                    entry.server_id,
                    entry.create_time,
                ),
            ).fetchall()
        if not rows:
            raise SightglassError(
                ErrorCode.RESOURCE_UNAVAILABLE,
                details={"reason": "resource_missing"},
            )
        if len(rows) != 1:
            raise SightglassError(
                ErrorCode.RESOURCE_BLOCKED,
                details={"reason": "resource_candidates_ambiguous"},
            )
        data = rows[0][0]
        if isinstance(data, memoryview):
            data = data.tobytes()
        if not isinstance(data, bytes) or not data:
            raise SightglassError(ErrorCode.RESOURCE_DECODE_FAILED)
        if len(data) > max_bytes:
            raise SightglassError(
                ErrorCode.RESOURCE_TOO_LARGE,
                details={"max_bytes": max_bytes},
            )
        return data

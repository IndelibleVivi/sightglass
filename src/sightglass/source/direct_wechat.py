from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import stat
from collections.abc import Callable, Iterator
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote

from sightglass.contracts.capture import CaptureRequest, ResourceCaptureBinding
from sightglass.contracts.common import SourceSortKey, to_utc_iso, utc_now, validate_timezone
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.identity import (
    ConversationCandidate,
    IdentityConfidence,
    LabelObservation,
    SourceAccount,
    SourceConversation,
    SourceIdentityKey,
    SourceParticipant,
    SourceParticipantFilter,
)
from sightglass.contracts.messages import (
    SourceDiscoveryPage,
    SourceMessage,
    SourceMessagePage,
)
from sightglass.contracts.resources import SourceResource, SourceResourcePayload
from sightglass.operations import check_operation_budget, operation_expired
from sightglass.resources.v2 import decode_wechat_v2_image
from sightglass.storage import temporary_workspace

from .base import (
    SourceHealth,
    SourcePreparationStep,
    SourceProviderDescriptor,
    SourceScope,
    SourceSnapshot,
    _check_preparation,
    _PreparationWindow,
)

SYNTHETIC_SOURCE_SCHEMA = "sightglass.synthetic-source.v1"

# One bounded physical discovery pass over the private fixture copies. The reader
# owns matching/validation; the provider only walks each shard's own rowid order
# (descending) and never claims chronology. The tag fails a stale cursor closed.
DISCOVERY_POSITION_SCHEMA = "sightglass.synthetic.discovery-position.v1"
DISCOVERY_SCAN_CAP = 256
DISCOVERY_DEFAULT_LIMIT = 100


@dataclass(frozen=True)
class _Inventory:
    manifest: dict[str, Any]
    health: SourceHealth
    generation_by_shard: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class _SnapshotState:
    manifest: dict[str, Any]
    catalog_path: Path
    shards: tuple[tuple[str, str, Path], ...]
    resources: tuple[tuple[str, Path, str, str | None, int | None, str | None, bytes | None], ...]
    scope: SourceScope | None = None


class SyntheticSourceProvider:
    """M0–M3 provider for explicit, deterministic synthetic sources only."""

    def __init__(self, source_root: str | os.PathLike[str]) -> None:
        self.source_root = Path(source_root).expanduser().resolve()
        self._snapshots: dict[str, _SnapshotState] = {}

    @property
    def descriptor(self) -> SourceProviderDescriptor:
        return SourceProviderDescriptor(
            kind="synthetic",
            implementation="sightglass.synthetic.v1",
            source_mode="synthetic",
            platform=(),
            supports_incremental=False,
            supports_resources=True,
            requires_running_app_for_key_refresh=False,
        )

    @property
    def manifest_path(self) -> Path:
        return self.source_root / "source.json"

    def _child(self, relative_name: str) -> Path:
        raw = str(relative_name or "")
        if not raw:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE, details={"warning_codes": ["source_path_invalid"]}
            )
        candidate = (self.source_root / raw).resolve()
        try:
            candidate.relative_to(self.source_root)
        except ValueError as exc:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                details={"warning_codes": ["source_path_escape"]},
            ) from exc
        return candidate

    def _resource_child(self, relative_name: str) -> Path:
        """Resolve a resource locator lexically without following source symlinks."""

        raw = str(relative_name or "")
        if not raw:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                details={"warning_codes": ["source_path_invalid"]},
            )
        candidate = Path(os.path.abspath(self.source_root / raw))
        try:
            candidate.relative_to(self.source_root)
        except ValueError as exc:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                details={"warning_codes": ["source_path_escape"]},
            ) from exc
        return candidate

    def _resource_path_state(self, path: Path) -> tuple[str, os.stat_result | None]:
        try:
            relative = path.relative_to(self.source_root)
        except ValueError:
            return "blocked_escape", None
        current = self.source_root
        for part in relative.parts[:-1]:
            current /= part
            try:
                metadata = current.lstat()
            except OSError:
                return "absent", None
            if stat.S_ISLNK(metadata.st_mode):
                return "blocked_symlink", None
            if not stat.S_ISDIR(metadata.st_mode):
                return "blocked_nonregular", None
        try:
            metadata = path.lstat()
        except OSError:
            return "absent", None
        if stat.S_ISLNK(metadata.st_mode):
            return "blocked_symlink", metadata
        if not stat.S_ISREG(metadata.st_mode):
            return "blocked_nonregular", metadata
        if metadata.st_nlink != 1:
            return "blocked_hardlink", metadata
        return "present", metadata

    def _open_resource_nofollow(self, path: Path) -> int:
        try:
            relative = path.relative_to(self.source_root)
        except ValueError as exc:
            raise SightglassError(ErrorCode.RESOURCE_BLOCKED) from exc
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        directory_flags = flags | getattr(os, "O_DIRECTORY", 0)
        descriptors: list[int] = []
        try:
            directory_fd = os.open(self.source_root, directory_flags)
            descriptors.append(directory_fd)
            for part in relative.parts[:-1]:
                directory_fd = os.open(part, directory_flags, dir_fd=directory_fd)
                descriptors.append(directory_fd)
            return os.open(relative.name, flags, dir_fd=directory_fd)
        except OSError as exc:
            raise SightglassError(ErrorCode.RESOURCE_BLOCKED) from exc
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)

    @staticmethod
    def _regular_file(path: Path) -> bool:
        try:
            return stat.S_ISREG(path.lstat().st_mode)
        except OSError:
            return False

    @classmethod
    def _file_fingerprint(cls, path: Path) -> dict[str, Any]:
        if not cls._regular_file(path):
            return {"state": "absent"}
        try:
            before = path.stat()
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            after = path.stat()
        except OSError:
            return {"state": "unreadable"}
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        )
        identity_after = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        )
        if identity_before != identity_after:
            return {"state": "changed_while_fingerprinting"}
        return {
            "state": "present",
            "device": after.st_dev,
            "inode": after.st_ino,
            "size": after.st_size,
            "mtime_ns": after.st_mtime_ns,
            "sha256": digest.hexdigest(),
        }

    @classmethod
    def _sqlite_fingerprint(cls, path: Path) -> dict[str, Any]:
        return {
            "main": cls._file_fingerprint(path),
            "wal": cls._file_fingerprint(Path(f"{path}-wal")),
        }

    def _load_manifest(self) -> dict[str, Any]:
        if not self._regular_file(self.manifest_path):
            raise SightglassError(ErrorCode.SOURCE_NOT_CONFIGURED)
        try:
            value = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                details={"warning_codes": ["source_manifest_invalid"]},
            ) from exc
        if not isinstance(value, dict) or value.get("schema") != SYNTHETIC_SOURCE_SCHEMA:
            raise SightglassError(
                ErrorCode.SOURCE_NOT_CONFIGURED,
                details={"warning_codes": ["synthetic_manifest_required"]},
            )
        account = value.get("account")
        catalog = value.get("catalog")
        shards = value.get("shards")
        resources = value.get("resources", [])
        if (
            not isinstance(account, dict)
            or not isinstance(catalog, dict)
            or not isinstance(shards, list)
            or not isinstance(resources, list)
        ):
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                details={"warning_codes": ["source_manifest_invalid"]},
            )
        required_account = (
            "source_account_key",
            "self_principal_key",
            "display_name",
            "reader_timezone",
        )
        if any(not str(account.get(key) or "") for key in required_account):
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                details={"warning_codes": ["source_account_identity_missing"]},
            )
        try:
            validate_timezone(str(account["reader_timezone"]))
        except ValueError as exc:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                details={"warning_codes": ["reader_timezone_invalid"]},
            ) from exc
        return value

    @staticmethod
    def _sqlite_readable(path: Path) -> bool:
        connection: sqlite3.Connection | None = None
        try:
            uri = f"file:{quote(str(path), safe='/')}?mode=ro"
            connection = sqlite3.connect(uri, uri=True)
            connection.execute("SELECT name FROM sqlite_master LIMIT 1").fetchone()
            return True
        except sqlite3.Error:
            return False
        finally:
            if connection is not None:
                connection.close()

    def _inventory(self) -> _Inventory:
        try:
            manifest = self._load_manifest()
        except SightglassError as exc:
            state = (
                "not_configured" if exc.code is ErrorCode.SOURCE_NOT_CONFIGURED else "incomplete"
            )
            health = SourceHealth(
                configured=False,
                available=False,
                account_count=0,
                source_state=state,
                fresh_as_of=utc_now().isoformat(),
                inventory_digest="",
                generation_set_digest="",
                shard_counts={
                    "present": 0,
                    "missing": 0,
                    "key_missing": 0,
                    "cache_only": 0,
                    "unreadable": 0,
                },
                warnings=tuple(exc.details.get("warning_codes", ())),
            )
            return _Inventory({}, health, ())

        warnings: list[str] = []
        counts = {"present": 0, "missing": 0, "key_missing": 0, "cache_only": 0, "unreadable": 0}
        generation_by_shard: list[tuple[str, str]] = []
        public_shards: list[dict[str, Any]] = []
        public_resources: list[dict[str, Any]] = []
        latest_ns = self.manifest_path.stat().st_mtime_ns

        catalog_path = self._child(str(manifest["catalog"].get("file") or ""))
        public_catalog: dict[str, Any]
        if not self._regular_file(catalog_path):
            warnings.append("source_catalog_missing")
            public_catalog = {
                "file": str(manifest["catalog"].get("file") or ""),
                "state": "missing",
            }
        elif not self._sqlite_readable(catalog_path):
            warnings.append("source_catalog_unreadable")
            public_catalog = {
                "file": str(manifest["catalog"].get("file") or ""),
                "state": "unreadable",
            }
        else:
            catalog_stat = catalog_path.stat()
            latest_ns = max(latest_ns, catalog_stat.st_mtime_ns)
            catalog_fingerprint = self._sqlite_fingerprint(catalog_path)
            public_catalog = {
                "file": str(manifest["catalog"].get("file") or ""),
                "state": "present",
                "fingerprint": catalog_fingerprint,
            }
            latest_ns = max(
                [latest_ns]
                + [
                    int(item["mtime_ns"])
                    for item in catalog_fingerprint.values()
                    if item["state"] == "present"
                ]
            )
            if any(
                item["state"] in {"unreadable", "changed_while_fingerprinting"}
                for item in public_catalog["fingerprint"].values()
            ):
                warnings.append("source_catalog_changed")

        seen_logical: set[str] = set()
        for item in manifest["shards"]:
            if not isinstance(item, dict):
                warnings.append("source_manifest_invalid")
                continue
            logical_key = str(item.get("logical_key") or "")
            generation_id = str(item.get("generation_id") or "")
            if not logical_key or not generation_id or logical_key in seen_logical:
                warnings.append("source_manifest_invalid")
                continue
            seen_logical.add(logical_key)
            generation_by_shard.append((logical_key, generation_id))
            shard_path = self._child(str(item.get("file") or ""))
            row: dict[str, Any] = {
                "file": str(item.get("file") or ""),
                "logical_key": logical_key,
                "generation_id": generation_id,
            }
            if not self._regular_file(shard_path):
                counts["missing"] += 1
                row["state"] = "missing"
                warnings.append("source_shard_missing")
            elif not self._sqlite_readable(shard_path):
                counts["unreadable"] += 1
                row["state"] = "unreadable"
                warnings.append("source_shard_unreadable")
            else:
                file_stat = shard_path.stat()
                counts["present"] += 1
                shard_fingerprint = self._sqlite_fingerprint(shard_path)
                row.update(
                    {
                        "state": "present",
                        "fingerprint": shard_fingerprint,
                    }
                )
                latest_ns = max(
                    [latest_ns]
                    + [
                        int(part["mtime_ns"])
                        for part in shard_fingerprint.values()
                        if part["state"] == "present"
                    ]
                )
                if any(
                    part["state"] in {"unreadable", "changed_while_fingerprinting"}
                    for part in row["fingerprint"].values()
                ):
                    warnings.append("source_shard_changed")
                latest_ns = max(latest_ns, file_stat.st_mtime_ns)
            public_shards.append(row)

        seen_resources: set[str] = set()
        for item in manifest.get("resources", []):
            if not isinstance(item, dict):
                warnings.append("source_manifest_invalid")
                continue
            resource_key = str(item.get("source_resource_key") or "")
            if not resource_key or resource_key in seen_resources:
                warnings.append("source_manifest_invalid")
                continue
            seen_resources.add(resource_key)
            row = {
                "source_resource_key": resource_key,
                "file": str(item.get("file") or ""),
            }
            try:
                resource_path = self._resource_child(row["file"])
            except SightglassError as exc:
                warnings.extend(str(value) for value in exc.details.get("warning_codes", ()))
                row["state"] = "invalid"
                public_resources.append(row)
                continue
            state_name, _metadata = self._resource_path_state(resource_path)
            row["state"] = state_name
            row["encoding"] = str(item.get("encoding") or "raw")
            if state_name == "present":
                fingerprint = self._file_fingerprint(resource_path)
                row["state"] = str(fingerprint["state"])
                row["fingerprint"] = fingerprint
                if fingerprint["state"] == "present":
                    latest_ns = max(latest_ns, int(fingerprint["mtime_ns"]))
            public_resources.append(row)

        if not generation_by_shard:
            warnings.append("source_shards_unavailable")
        generation_by_shard.sort()
        public_basis = {
            "schema": SYNTHETIC_SOURCE_SCHEMA,
            "manifest_sha256": hashlib.sha256(
                json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest(),
            "catalog": public_catalog,
            "shards": sorted(public_shards, key=lambda row: row["logical_key"]),
            "resources": sorted(public_resources, key=lambda row: row["source_resource_key"]),
        }
        inventory_digest = hashlib.sha256(
            json.dumps(public_basis, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        generation_set_digest = hashlib.sha256(
            json.dumps(generation_by_shard, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        complete = not warnings and counts["present"] == len(generation_by_shard)
        fresh_as_of = datetime.fromtimestamp(latest_ns / 1_000_000_000, UTC).isoformat()
        return _Inventory(
            manifest,
            SourceHealth(
                configured=True,
                available=complete,
                account_count=1,
                source_state="complete" if complete else "incomplete",
                fresh_as_of=fresh_as_of,
                inventory_digest=inventory_digest,
                generation_set_digest=generation_set_digest,
                shard_counts=counts,
                warnings=tuple(sorted(set(warnings))),
            ),
            tuple(generation_by_shard),
        )

    def health(self) -> SourceHealth:
        return self._inventory().health

    def _require_complete(self) -> _Inventory:
        inventory = self._inventory()
        if not inventory.health.complete:
            code = (
                ErrorCode.SOURCE_NOT_CONFIGURED
                if inventory.health.source_state == "not_configured"
                else ErrorCode.SOURCE_INCOMPLETE
            )
            raise SightglassError(
                code,
                retryable=True,
                details={"warning_codes": list(inventory.health.warnings)},
            )
        return inventory

    def _assert_snapshot(self, snapshot: SourceSnapshot) -> _SnapshotState:
        state = self._snapshots.get(snapshot.token)
        if state is None:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        inventory = self._require_complete()
        if (
            inventory.health.inventory_digest != snapshot.inventory_digest
            or inventory.health.generation_set_digest != snapshot.generation_set_digest
            or inventory.generation_by_shard != snapshot.generation_by_shard
        ):
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        return state

    def validate_snapshot(self, snapshot: SourceSnapshot) -> None:
        self._assert_snapshot(snapshot)

    def _backup_database(self, source: Path, destination: Path) -> None:
        try:
            with self._connect_readonly(source) as source_connection:
                with closing(sqlite3.connect(destination)) as destination_connection:
                    source_connection.backup(destination_connection)
                    destination_connection.commit()
            os.chmod(destination, 0o600)
        except (OSError, sqlite3.Error) as exc:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                retryable=True,
                details={"warning_codes": ["source_snapshot_failed"]},
            ) from exc

    def _copy_snapshot(
        self,
        inventory: _Inventory,
        directory: Path,
        *,
        resource_keys: frozenset[str] | None = None,
        scope: SourceScope | None = None,
    ) -> _SnapshotState:
        manifest = json.loads(json.dumps(inventory.manifest))
        catalog_path = directory / "catalog.db"
        self._backup_database(
            self._child(str(manifest["catalog"]["file"])),
            catalog_path,
        )
        shards: list[tuple[str, str, Path]] = []
        for index, item in enumerate(
            sorted(manifest["shards"], key=lambda value: str(value["logical_key"]))
        ):
            destination = directory / f"shard-{index:04d}.db"
            self._backup_database(self._child(str(item["file"])), destination)
            shards.append((str(item["logical_key"]), str(item["generation_id"]), destination))
        resources: list[
            tuple[str, Path, str, str | None, int | None, str | None, bytes | None]
        ] = []
        for item in sorted(
            manifest.get("resources", []),
            key=lambda value: str(value["source_resource_key"]),
        ):
            if resource_keys is not None and str(item["source_resource_key"]) not in resource_keys:
                continue
            path = self._resource_child(str(item["file"]))
            state_name, _metadata = self._resource_path_state(path)
            digest = None
            size = None
            if state_name == "present":
                fingerprint = self._file_fingerprint(path)
                state_name = str(fingerprint["state"])
                if state_name == "present":
                    digest = str(fingerprint["sha256"])
                    size = int(fingerprint["size"])
            encoding = str(item.get("encoding") or "raw")
            key: bytes | None = None
            if encoding == "wechat_v2":
                try:
                    key = bytes.fromhex(str(item.get("image_aes_key") or ""))
                except ValueError:
                    key = None
                if len(key or b"") != 16:
                    state_name = "blocked_key_missing"
                    key = None
            elif encoding != "raw":
                state_name = "blocked_encoding"
            resources.append(
                (str(item["source_resource_key"]), path, state_name, digest, size, encoding, key)
            )
        state = _SnapshotState(
            manifest,
            catalog_path,
            tuple(shards),
            tuple(resources),
            scope,
        )
        self._validate_snapshot_state(state)
        return state

    @staticmethod
    def _row_semantics(row: sqlite3.Row) -> dict[str, Any]:
        try:
            resources = json.loads(str(row["resources_json"] or "[]"))
        except json.JSONDecodeError as exc:
            raise SightglassError(ErrorCode.SOURCE_MESSAGE_DECODE_FAILED) from exc
        if not isinstance(resources, list):
            raise SightglassError(ErrorCode.SOURCE_MESSAGE_DECODE_FAILED)
        sent_at_utc = to_utc_iso(str(row["sent_at_utc"]))
        observed_at_utc = to_utc_iso(str(row["observed_at_utc"]))
        return {
            "source_conversation_id": row["source_conversation_id"],
            "source_time_raw": row["source_time_raw"],
            "sent_at_utc": sent_at_utc,
            "observed_at_utc": observed_at_utc,
            "sort_seq": int(row["sort_seq"]),
            "wechat_type": int(row["wechat_type"]),
            "raw_content": row["raw_content"],
            "is_outgoing": bool(row["is_outgoing"]),
            "sender_internal_id": row["sender_internal_id"],
            "sender_local_token": row["sender_local_token"],
            "sender_surface_label": row["sender_surface_label"],
            "resources": resources,
        }

    @classmethod
    def _row_semantic_digest(cls, row: sqlite3.Row) -> str:
        return hashlib.sha256(
            json.dumps(
                cls._row_semantics(row),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    def _validate_snapshot_state(self, state: _SnapshotState) -> None:
        try:
            with self._catalog(state) as connection:
                connection.execute(
                    """
                    SELECT source_conversation_id, kind, title, last_message_at_utc,
                           roster_complete FROM conversations LIMIT 0
                    """
                )
                connection.execute(
                    "SELECT source_conversation_id, alias FROM conversation_aliases LIMIT 0"
                )
                connection.execute(
                    """
                    SELECT internal_id, contact_remark, account_nickname,
                           public_handle, actor_kind FROM principals LIMIT 0
                    """
                )
                connection.execute(
                    """
                    SELECT source_conversation_id, internal_id, source_membership_id,
                           group_card, observed_at_utc FROM memberships LIMIT 0
                    """
                )
            seen: dict[str, str] = {}
            for row, _logical_key, _generation_id in self._read_rows(state):
                source_message_id = str(row["source_message_id"] or "")
                if not source_message_id:
                    raise SightglassError(ErrorCode.SOURCE_MESSAGE_DECODE_FAILED)
                digest = self._row_semantic_digest(row)
                previous = seen.setdefault(source_message_id, digest)
                if previous != digest:
                    raise SightglassError(
                        ErrorCode.SOURCE_INCOMPLETE,
                        details={"warning_codes": ["duplicate_message_identity_conflict"]},
                    )
        except SightglassError:
            raise
        except (IndexError, KeyError, TypeError, ValueError, sqlite3.Error) as exc:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                details={"warning_codes": ["source_schema_invalid"]},
            ) from exc

    @contextmanager
    def snapshot(self) -> Iterator[SourceSnapshot]:
        with self._open_snapshot(resource_keys=None, scope=None) as snapshot:
            yield snapshot

    @contextmanager
    def session(self, scope: SourceScope) -> Iterator[SourceSnapshot]:
        """Open a synthetic source view for one explicit dependency scope.

        The synthetic provider is deterministic and copy-based, so a scoped session
        narrows only the resource set it materializes; ``catalog`` keeps the
        historical account-wide snapshot behavior.
        """

        if scope.kind == "catalog":
            resource_keys: frozenset[str] | None = None
        elif scope.kind == "resource" and scope.source_resource_key is not None:
            resource_keys = frozenset({scope.source_resource_key})
        else:
            resource_keys = None
        with self._open_snapshot(resource_keys=resource_keys, scope=scope) as snapshot:
            yield snapshot

    @contextmanager
    def _open_snapshot(
        self, *, resource_keys: frozenset[str] | None, scope: SourceScope | None
    ) -> Iterator[SourceSnapshot]:
        inventory = self._require_complete()
        files = [inventory.manifest["catalog"], *inventory.manifest["shards"]]
        snapshot_bytes = 0
        for item in files:
            path = self._child(str(item["file"]))
            snapshot_bytes += path.stat().st_size
            wal = path.with_name(path.name + "-wal")
            if wal.exists():
                snapshot_bytes += wal.stat().st_size
        temporary = ExitStack()
        name = temporary.enter_context(temporary_workspace(
            "sightglass-source-snapshot-", 2 * snapshot_bytes + 65_536,
        ))
        token = secrets.token_urlsafe(24)
        try:
            directory = Path(name)
            os.chmod(directory, 0o700)
            state = self._copy_snapshot(
                inventory,
                directory,
                resource_keys=resource_keys,
                scope=scope,
            )
            after_copy = self._require_complete()
            if (
                after_copy.health.inventory_digest != inventory.health.inventory_digest
                or after_copy.health.generation_set_digest != inventory.health.generation_set_digest
                or after_copy.generation_by_shard != inventory.generation_by_shard
            ):
                raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
            self._snapshots[token] = state
            snapshot = SourceSnapshot(
                inventory_digest=inventory.health.inventory_digest,
                generation_set_digest=inventory.health.generation_set_digest,
                fresh_as_of=inventory.health.fresh_as_of,
                generation_by_shard=inventory.generation_by_shard,
                token=token,
                scope=scope,
            )
            try:
                yield snapshot
            finally:
                self._assert_snapshot(snapshot)
        finally:
            self._snapshots.pop(token, None)
            temporary.close()

    @staticmethod
    @contextmanager
    def _connect_readonly(path: Path) -> Iterator[sqlite3.Connection]:
        uri = f"file:{quote(str(path), safe='/')}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
        finally:
            connection.close()

    def _catalog(self, state: _SnapshotState):
        return self._connect_readonly(state.catalog_path)

    @staticmethod
    def _enforce_scope(
        state: _SnapshotState,
        *,
        account_id: str | None = None,
        conversation_source_id: str | None = None,
        source_message_id: str | None = None,
        source_resource_key: str | None = None,
    ) -> None:
        scope = state.scope
        if scope is None or scope.kind == "catalog":
            return
        if source_resource_key is not None:
            if (
                scope.kind != "resource"
                or scope.source_resource_key != source_resource_key
            ):
                raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
            return
        if scope.kind == "resource":
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
        if scope.account_id is not None and account_id is not None:
            if scope.account_id != account_id:
                raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)
        if scope.conversation_source_id is not None and conversation_source_id is not None:
            if scope.conversation_source_id != conversation_source_id:
                raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        if scope.kind == "conversations" and conversation_source_id is not None:
            if conversation_source_id not in scope.conversation_source_ids:
                raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        if scope.kind == "message":
            if source_message_id is None or scope.source_message_id != source_message_id:
                raise SightglassError(ErrorCode.MESSAGE_NOT_FOUND)

    def _account(self, manifest: dict[str, Any]) -> SourceAccount:
        account = manifest["account"]
        confidence = str(account.get("identity_confidence") or "exact")
        if confidence not in {"exact", "strong", "weak", "unknown"}:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                details={"warning_codes": ["source_account_identity_invalid"]},
            )
        return SourceAccount(
            source_namespace=str(manifest.get("source_namespace") or "synthetic"),
            source_account_key=str(account["source_account_key"]),
            self_principal_key=str(account["self_principal_key"]),
            display_name=str(account["display_name"]),
            reader_timezone=str(account["reader_timezone"]),
            identity_confidence=cast(IdentityConfidence, confidence),
        )

    def _require_account(self, account_key: str, manifest: dict[str, Any]) -> SourceAccount:
        account = self._account(manifest)
        if account.source_account_key != account_key:
            raise SightglassError(ErrorCode.ACCOUNT_NOT_FOUND)
        return account

    def list_accounts(self, snapshot: SourceSnapshot) -> list[SourceAccount]:
        state = self._assert_snapshot(snapshot)
        return [self._account(state.manifest)]

    def get_conversation(
        self,
        account_id: str,
        conversation_source_id: str,
        snapshot: SourceSnapshot,
    ) -> SourceConversation | None:
        state = self._assert_snapshot(snapshot)
        self._require_account(account_id, state.manifest)
        self._enforce_scope(
            state,
            account_id=account_id,
            conversation_source_id=conversation_source_id,
        )
        with self._catalog(state) as connection:
            row = connection.execute(
                "SELECT kind, title, last_message_at_utc, roster_complete "
                "FROM conversations WHERE source_conversation_id = ?",
                (conversation_source_id,),
            ).fetchone()
            if row is None:
                return None
            aliases = connection.execute(
                "SELECT alias FROM conversation_aliases WHERE source_conversation_id = ?",
                (conversation_source_id,),
            ).fetchall()
        return SourceConversation(
            source_conversation_id=conversation_source_id,
            kind=str(row["kind"]),
            title=str(row["title"]),
            aliases=tuple(str(alias["alias"]) for alias in aliases),
            last_message_at_utc=(
                to_utc_iso(str(row["last_message_at_utc"]))
                if row["last_message_at_utc"] else None
            ),
            roster_complete=bool(row["roster_complete"]),
        )

    def list_conversations(
        self, account_id: str, snapshot: SourceSnapshot
    ) -> list[SourceConversation]:
        state = self._assert_snapshot(snapshot)
        self._require_account(account_id, state.manifest)
        with self._catalog(state) as connection:
            rows = connection.execute(
                """
                SELECT source_conversation_id, kind, title, last_message_at_utc, roster_complete
                FROM conversations
                ORDER BY last_message_at_utc DESC, source_conversation_id ASC
                """
            ).fetchall()
            aliases = connection.execute(
                "SELECT source_conversation_id, alias FROM conversation_aliases"
            ).fetchall()
        by_conversation: dict[str, list[str]] = {}
        for row in aliases:
            by_conversation.setdefault(str(row["source_conversation_id"]), []).append(
                str(row["alias"])
            )
        return [
            SourceConversation(
                source_conversation_id=str(row["source_conversation_id"]),
                kind=str(row["kind"]),
                title=str(row["title"]),
                aliases=tuple(by_conversation.get(str(row["source_conversation_id"]), ())),
                last_message_at_utc=(
                    to_utc_iso(str(row["last_message_at_utc"]))
                    if row["last_message_at_utc"]
                    else None
                ),
                roster_complete=bool(row["roster_complete"]),
            )
            for row in rows
        ]

    def resolve_conversation(
        self, account_id: str, query: str, snapshot: SourceSnapshot
    ) -> list[ConversationCandidate]:
        state = self._assert_snapshot(snapshot)
        conversations = self.list_conversations(account_id, snapshot)
        query_folded = str(query or "").casefold()
        results: dict[str, ConversationCandidate] = {}
        for conversation in conversations:
            fields = [
                (conversation.title, "title"),
                *[(alias, "alias") for alias in conversation.aliases],
            ]
            if not query_folded:
                results[conversation.source_conversation_id] = ConversationCandidate(
                    conversation, conversation.title, "recent_activity"
                )
                continue
            for value, kind in fields:
                if query_folded in value.casefold():
                    results[conversation.source_conversation_id] = ConversationCandidate(
                        conversation, value, kind
                    )
                    break
        if query_folded:
            with self._catalog(state) as connection:
                rows = connection.execute(
                    """
                    SELECT m.source_conversation_id, p.contact_remark, p.account_nickname,
                           p.public_handle, m.group_card
                    FROM memberships m
                    LEFT JOIN principals p ON p.internal_id = m.internal_id
                    """
                ).fetchall()
            by_source = {item.source_conversation_id: item for item in conversations}
            for row in rows:
                source_id = str(row["source_conversation_id"])
                if source_id in results or source_id not in by_source:
                    continue
                for kind in ("contact_remark", "group_card", "account_nickname", "public_handle"):
                    value = row[kind]
                    if value and query_folded in str(value).casefold():
                        results[source_id] = ConversationCandidate(
                            by_source[source_id], str(value), kind
                        )
                        break
            for row, _logical_key, _generation_id in self._read_rows(state):
                source_id = str(row["source_conversation_id"])
                if source_id in results or source_id not in by_source:
                    continue
                value = row["sender_surface_label"]
                if value and query_folded in str(value).casefold():
                    results[source_id] = ConversationCandidate(
                        by_source[source_id], str(value), "message_surface"
                    )
        self._assert_snapshot(snapshot)
        return sorted(
            results.values(),
            key=lambda item: (
                item.conversation.last_message_at_utc or "",
                item.conversation.source_conversation_id,
            ),
            reverse=True,
        )

    def _principal_rows(self, state: _SnapshotState) -> dict[str, sqlite3.Row]:
        with self._catalog(state) as connection:
            rows = connection.execute("SELECT * FROM principals").fetchall()
        return {str(row["internal_id"]): row for row in rows}

    @staticmethod
    def _account_labels(row: sqlite3.Row | None, observed_at: str) -> list[LabelObservation]:
        if row is None:
            return []
        values = (
            ("contact_remark", row["contact_remark"]),
            ("account_nickname", row["account_nickname"]),
            ("public_handle", row["public_handle"]),
        )
        return [
            LabelObservation(
                label=str(value),
                label_kind=kind,
                scope="account",
                provenance="synthetic.catalog.principals",
                observed_at_utc=observed_at,
                temporal_confidence="current_only",
            )
            for kind, value in values
            if value
        ]

    def _read_rows(
        self,
        state: _SnapshotState,
        conversation_source_id: str | None = None,
    ) -> list[tuple[sqlite3.Row, str, str]]:
        rows: list[tuple[sqlite3.Row, str, str]] = []
        for logical_key, generation_id, path in state.shards:
            with self._connect_readonly(path) as connection:
                if conversation_source_id is None:
                    selected = connection.execute("SELECT * FROM messages").fetchall()
                else:
                    selected = connection.execute(
                        "SELECT * FROM messages WHERE source_conversation_id = ?",
                        (conversation_source_id,),
                    ).fetchall()
            rows.extend((row, logical_key, generation_id) for row in selected)
        return rows

    def list_participants(
        self,
        account_id: str,
        conversation_source_id: str,
        snapshot: SourceSnapshot,
    ) -> list[SourceParticipant]:
        state = self._assert_snapshot(snapshot)
        account = self._require_account(account_id, state.manifest)
        self._enforce_scope(state, account_id=account_id,
                            conversation_source_id=conversation_source_id)
        principals = self._principal_rows(state)
        with self._catalog(state) as connection:
            memberships = connection.execute(
                """
                SELECT source_conversation_id, internal_id, source_membership_id,
                       group_card, observed_at_utc
                FROM memberships WHERE source_conversation_id = ?
                """,
                (conversation_source_id,),
            ).fetchall()
        found: dict[str, dict[str, Any]] = {}

        def ensure(
            internal_id: str | None,
            local_token: str | None,
            source_message_id: str | None,
            source_membership_id: str | None = None,
        ) -> dict[str, Any]:
            if internal_id:
                identity = f"principal:{internal_id}"
                keys = (
                    SourceIdentityKey(
                        "internal_username",
                        internal_id,
                        "stable",
                        True,
                        "synthetic.message-envelope-or-catalog",
                    ),
                )
                state = "stable"
                confidence = "exact"
            elif local_token:
                identity = f"conversation:{local_token}"
                keys = (
                    SourceIdentityKey(
                        "conversation_sender_id",
                        local_token,
                        "conversation_local",
                        False,
                        "synthetic.message-envelope",
                        conversation_source_id,
                    ),
                )
                state = "conversation_local"
                confidence = "strong"
            elif source_membership_id:
                identity = f"membership:{source_membership_id}"
                keys = (
                    SourceIdentityKey(
                        "source_membership_id",
                        source_membership_id,
                        "conversation_local",
                        False,
                        "synthetic.catalog.memberships",
                        conversation_source_id,
                    ),
                )
                state = "conversation_local"
                confidence = "strong"
            else:
                identity = f"message:{source_message_id}"
                keys = ()
                state = "alias_only"
                confidence = "unknown"
            if identity not in found:
                principal_row = principals.get(internal_id or "")
                observed_at = snapshot.fresh_as_of
                found[identity] = {
                    "internal_id": internal_id,
                    "keys": keys,
                    "labels": self._account_labels(principal_row, observed_at),
                    "is_self": internal_id == account.self_principal_key,
                    "actor_kind": str(principal_row["actor_kind"]) if principal_row else "person",
                    "resolution_state": state,
                    "identity_confidence": confidence,
                    "source_membership_id": (
                        source_membership_id
                        or (
                            f"message:{source_message_id}"
                            if not internal_id and not local_token and source_message_id
                            else None
                        )
                    ),
                    "last_spoke_at": None,
                    "account_labels_complete": principal_row is not None,
                    "membership_labels_complete": False,
                }
            return found[identity]

        for membership in memberships:
            internal_id = str(membership["internal_id"]) if membership["internal_id"] else None
            source_membership_id = (
                str(membership["source_membership_id"])
                if membership["source_membership_id"]
                else None
            )
            if internal_id is None and source_membership_id is None:
                raise SightglassError(
                    ErrorCode.SOURCE_INCOMPLETE,
                    details={"warning_codes": ["anonymous_membership_identity_missing"]},
                )
            item = ensure(internal_id, None, None, source_membership_id)
            item["membership_labels_complete"] = True
            if membership["group_card"]:
                item["labels"].append(
                    LabelObservation(
                        label=str(membership["group_card"]),
                        label_kind="group_card",
                        scope="conversation",
                        provenance="synthetic.catalog.memberships",
                        observed_at_utc=to_utc_iso(str(membership["observed_at_utc"])),
                        temporal_confidence="current_only",
                    )
                )

        for row, _logical_key, _generation_id in self._read_rows(state, conversation_source_id):
            internal_id = (
                account.self_principal_key
                if bool(row["is_outgoing"])
                else (str(row["sender_internal_id"]) if row["sender_internal_id"] else None)
            )
            local_token = str(row["sender_local_token"]) if row["sender_local_token"] else None
            if internal_id is None and local_token is None and not row["sender_surface_label"]:
                continue
            item = ensure(internal_id, local_token, str(row["source_message_id"]))
            sent_at = to_utc_iso(str(row["sent_at_utc"]))
            if not item["last_spoke_at"] or sent_at > item["last_spoke_at"]:
                item["last_spoke_at"] = sent_at
            if row["sender_surface_label"]:
                item["labels"].append(
                    LabelObservation(
                        label=str(row["sender_surface_label"]),
                        label_kind="message_surface",
                        scope="message-surface",
                        provenance="synthetic.message.sender_surface_label",
                        observed_at_utc=to_utc_iso(str(row["observed_at_utc"])),
                        temporal_confidence="exact",
                        observed_source_message_id=str(row["source_message_id"]),
                    )
                )

        result: list[SourceParticipant] = []
        for identity in sorted(found):
            item = found[identity]
            labels = list(item["labels"])
            deduped = list(
                {
                    (
                        label.label,
                        label.label_kind,
                        label.scope,
                        label.observed_at_utc,
                        label.observed_source_message_id,
                    ): label
                    for label in labels
                }.values()
            )
            result.append(
                SourceParticipant(
                    source_conversation_id=conversation_source_id,
                    identity_keys=tuple(item["keys"]),
                    labels=tuple(deduped),
                    is_self=bool(item["is_self"]),
                    actor_kind=str(item["actor_kind"]),
                    resolution_state=item["resolution_state"],
                    identity_confidence=item["identity_confidence"],
                    source_membership_id=(
                        str(item["source_membership_id"]) if item["source_membership_id"] else None
                    ),
                    last_spoke_at_utc=item["last_spoke_at"],
                    account_labels_complete=bool(item["account_labels_complete"]),
                    membership_labels_complete=bool(item["membership_labels_complete"]),
                )
            )
        self._assert_snapshot(snapshot)
        return result

    def resolve_participant(
        self,
        account_id: str,
        conversation_source_id: str,
        query: str,
        snapshot: SourceSnapshot,
    ) -> list[SourceParticipant]:
        query_folded = str(query or "").casefold()
        participants = self.list_participants(account_id, conversation_source_id, snapshot)
        if not query_folded:
            return participants
        return [
            participant
            for participant in participants
            if any(query_folded in label.label.casefold() for label in participant.labels)
        ]

    def _conversation_kind(self, state: _SnapshotState, source_id: str) -> str:
        with self._catalog(state) as connection:
            row = connection.execute(
                "SELECT kind FROM conversations WHERE source_conversation_id = ?",
                (source_id,),
            ).fetchone()
        if row is None:
            raise SightglassError(ErrorCode.CONVERSATION_NOT_FOUND)
        return str(row["kind"])

    @staticmethod
    def _resource(value: dict[str, Any]) -> SourceResource:
        original_name = str(value["original_name"]) if value.get("original_name") else None
        if original_name is not None:
            original_name = original_name.replace("\\", "/").rsplit("/", 1)[-1]
            if original_name in {"", ".", ".."}:
                original_name = None
        return SourceResource(
            source_ordinal=int(value.get("source_ordinal", 0)),
            kind=str(value.get("kind") or "unknown"),
            source_resource_key=(
                str(value["source_resource_key"]) if value.get("source_resource_key") else None
            ),
            mime_type=(str(value["mime_type"]) if value.get("mime_type") else None),
            original_name=original_name,
            declared_size=(
                int(value["declared_size"]) if value.get("declared_size") is not None else None
            ),
            declared_hash=(str(value["declared_hash"]) if value.get("declared_hash") else None),
            availability=str(value.get("availability") or "metadata_only"),
        )

    def _row_to_message(
        self,
        row: sqlite3.Row,
        logical_key: str,
        generation_id: str,
        *,
        conversation_kind: str,
        account: SourceAccount,
    ) -> SourceMessage:
        if (
            bool(row["is_outgoing"])
            and row["sender_internal_id"]
            and str(row["sender_internal_id"]) != account.self_principal_key
        ):
            raise SightglassError(
                ErrorCode.SOURCE_MESSAGE_DECODE_FAILED,
                details={"warning_codes": ["outgoing_sender_identity_conflict"]},
            )
        internal_id = (
            account.self_principal_key
            if bool(row["is_outgoing"])
            else (str(row["sender_internal_id"]) if row["sender_internal_id"] else None)
        )
        if internal_id:
            sender_keys = (
                SourceIdentityKey(
                    "internal_username",
                    internal_id,
                    "stable",
                    True,
                    "synthetic.message-envelope",
                ),
            )
        elif row["sender_local_token"]:
            sender_keys = (
                SourceIdentityKey(
                    "conversation_sender_id",
                    str(row["sender_local_token"]),
                    "conversation_local",
                    False,
                    "synthetic.message-envelope",
                    str(row["source_conversation_id"]),
                ),
            )
        else:
            sender_keys = ()
        observed_at = to_utc_iso(str(row["observed_at_utc"]))
        labels = ()
        if row["sender_surface_label"]:
            labels = (
                LabelObservation(
                    label=str(row["sender_surface_label"]),
                    label_kind="message_surface",
                    scope="message-surface",
                    provenance="synthetic.message.sender_surface_label",
                    observed_at_utc=observed_at,
                    temporal_confidence="exact",
                    observed_source_message_id=str(row["source_message_id"]),
                ),
            )
        try:
            resource_values = json.loads(str(row["resources_json"] or "[]"))
        except json.JSONDecodeError as exc:
            raise SightglassError(ErrorCode.SOURCE_MESSAGE_DECODE_FAILED) from exc
        if not isinstance(resource_values, list):
            raise SightglassError(ErrorCode.SOURCE_MESSAGE_DECODE_FAILED)
        return SourceMessage(
            source_message_id=str(row["source_message_id"]),
            source_conversation_id=str(row["source_conversation_id"]),
            conversation_kind=conversation_kind,
            source_time_raw=str(row["source_time_raw"]),
            sent_at_utc=to_utc_iso(str(row["sent_at_utc"])),
            observed_at_utc=observed_at,
            sort_seq=int(row["sort_seq"]),
            source_rowid=int(row["source_rowid"]),
            wechat_type=int(row["wechat_type"]),
            raw_content=str(row["raw_content"] or ""),
            is_outgoing=bool(row["is_outgoing"]),
            source_generation_id=generation_id,
            logical_shard_key=logical_key,
            sender_keys=sender_keys,
            sender_labels=labels,
            sender_surface_label=(
                str(row["sender_surface_label"]) if row["sender_surface_label"] else None
            ),
            sender_local_token=(
                str(row["sender_local_token"]) if row["sender_local_token"] else None
            ),
            resources=tuple(self._resource(value) for value in resource_values),
        )

    def _messages(
        self,
        state: _SnapshotState,
        account: SourceAccount,
        conversation_source_id: str,
    ) -> list[SourceMessage]:
        kind = self._conversation_kind(state, conversation_source_id)
        selected_rows: dict[str, tuple[sqlite3.Row, str, str]] = {}
        digests: dict[str, str] = {}
        for row, logical_key, generation_id in self._read_rows(state, conversation_source_id):
            source_message_id = str(row["source_message_id"])
            digest = self._row_semantic_digest(row)
            if source_message_id in digests and digests[source_message_id] != digest:
                raise SightglassError(
                    ErrorCode.SOURCE_INCOMPLETE,
                    details={"warning_codes": ["duplicate_message_identity_conflict"]},
                )
            digests[source_message_id] = digest
            candidate = (row, logical_key, generation_id)
            current = selected_rows.get(source_message_id)
            candidate_key = (logical_key, int(row["source_rowid"]), generation_id)
            current_key = (
                (current[1], int(current[0]["source_rowid"]), current[2])
                if current is not None
                else None
            )
            if current_key is None or candidate_key < current_key:
                selected_rows[source_message_id] = candidate
        messages = [
            self._row_to_message(
                row,
                logical_key,
                generation_id,
                conversation_kind=kind,
                account=account,
            )
            for row, logical_key, generation_id in selected_rows.values()
        ]
        messages.sort(key=lambda item: item.sort_key.as_tuple())
        return messages

    def read_recent(
        self,
        account_id: str,
        conversation_source_id: str,
        limit: int,
        snapshot: SourceSnapshot,
    ) -> SourceMessagePage:
        state = self._assert_snapshot(snapshot)
        self._enforce_scope(
            state,
            account_id=account_id,
            conversation_source_id=conversation_source_id,
        )
        account = self._require_account(account_id, state.manifest)
        messages = self._messages(state, account, conversation_source_id)
        bounded = max(1, int(limit))
        selected = messages[-bounded:]
        self._assert_snapshot(snapshot)
        return SourceMessagePage(tuple(selected), has_more_before=len(messages) > len(selected))

    @staticmethod
    def _matches_participant(
        message: SourceMessage, filters: tuple[SourceParticipantFilter, ...]
    ) -> bool:
        if not filters:
            return True
        for participant_filter in filters:
            if participant_filter.source_message_id == message.source_message_id:
                return True
            for key in message.sender_keys:
                if (
                    participant_filter.key_kind == key.kind
                    and participant_filter.key_value == key.value
                    and participant_filter.principal_eligible == key.principal_eligible
                    and participant_filter.scope_conversation_source_id
                    == key.scope_conversation_source_id
                ):
                    return True
        return False

    def read_range(
        self,
        account_id: str,
        conversation_source_id: str,
        *,
        after: SourceSortKey | None,
        before: SourceSortKey | None,
        direction: str,
        limit: int,
        snapshot: SourceSnapshot,
        participant_source_ids: tuple[SourceParticipantFilter, ...] = (),
        time_after_utc: str | None = None,
        time_before_utc: str | None = None,
    ) -> SourceMessagePage:
        if direction not in {"forward", "backward"}:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        state = self._assert_snapshot(snapshot)
        self._enforce_scope(
            state,
            account_id=account_id,
            conversation_source_id=conversation_source_id,
        )
        account = self._require_account(account_id, state.manifest)
        messages = self._messages(state, account, conversation_source_id)
        if after is not None:
            messages = [item for item in messages if item.sort_key.as_tuple() > after.as_tuple()]
        if before is not None:
            messages = [item for item in messages if item.sort_key.as_tuple() < before.as_tuple()]
        if time_after_utc is not None:
            after_utc = to_utc_iso(time_after_utc)
            messages = [item for item in messages if item.sent_at_utc >= after_utc]
        if time_before_utc is not None:
            before_utc = to_utc_iso(time_before_utc)
            messages = [item for item in messages if item.sent_at_utc < before_utc]
        messages = [
            item for item in messages if self._matches_participant(item, participant_source_ids)
        ]
        bounded = max(1, int(limit))
        selected = messages[:bounded] if direction == "forward" else messages[-bounded:]
        self._assert_snapshot(snapshot)
        return SourceMessagePage(
            tuple(selected),
            has_more_before=direction == "backward" and len(messages) > len(selected),
            has_more_after=direction == "forward" and len(messages) > len(selected),
        )

    def search_generation_binding(
        self,
        account_id: str,
        conversation_source_id: str,
        *,
        snapshot: SourceSnapshot,
    ) -> tuple[tuple[str, str], ...]:
        """Return target shard generations under either a narrow or catalog view."""

        state = self._assert_snapshot(snapshot)
        self._require_account(account_id, state.manifest)
        self._enforce_scope(
            state, account_id=account_id, conversation_source_id=conversation_source_id,
        )
        self._conversation_kind(state, conversation_source_id)
        selected = []
        for logical, generation, path in state.shards:
            check_operation_budget()
            with self._connect_readonly(path) as connection:
                connection.set_progress_handler(lambda: int(operation_expired()), 1000)
                try:
                    present = connection.execute(
                        "SELECT 1 FROM messages WHERE source_conversation_id = ? LIMIT 1",
                        (conversation_source_id,),
                    ).fetchone()
                except sqlite3.Error:
                    check_operation_budget()
                    raise
            if present is not None:
                selected.append((logical, generation))
                snapshot.dependency_generation_by_shard[logical] = generation
        return tuple(selected)

    def prepare_search_page(
        self,
        account_id: str,
        conversation_source_id: str,
        *,
        direction: str,
        limit: int,
        snapshot: SourceSnapshot,
        after: SourceSortKey | None = None,
        before: SourceSortKey | None = None,
        time_after_utc: str | None = None,
        time_before_utc: str | None = None,
        batch_size: int = 256,
        check_authority: Callable[[], None] | None = None,
    ) -> Iterator[SourcePreparationStep]:
        """Scan private fixture positions in bounded batches, then parse one page.

        A copy-backed synthetic session retains its existing snapshot validation.
        Position reconciliation uses the same earliest-logical-shard rule as ordinary
        reads, including overlaps whose physical/source rowids differ. No iterator
        position is portable to another session or restart.
        """

        _check_preparation(check_authority)
        if direction not in {"forward", "backward"} or batch_size < 1:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        state = self._assert_snapshot(snapshot)
        if state.scope is None or state.scope.kind != "conversation":
            raise SightglassError(ErrorCode.QUERY_INVALID)
        self._enforce_scope(
            state, account_id=account_id, conversation_source_id=conversation_source_id,
        )
        account = self._require_account(account_id, state.manifest)
        kind = self._conversation_kind(state, conversation_source_id)
        batch = min(int(batch_size), 1024)
        bounded = max(1, int(limit))
        minimum_time = to_utc_iso(time_after_utc) if time_after_utc is not None else None
        maximum_time = to_utc_iso(time_before_utc) if time_before_utc is not None else None
        position_columns = "source_message_id, sent_at_utc, sort_seq, source_rowid"
        windows = []
        scanned = completed = 0
        yield SourcePreparationStep("positions", scanned, completed, len(state.shards))
        with ExitStack() as stack:
            connections = [
                stack.enter_context(self._connect_readonly(path))
                for _logical, _generation, path in state.shards
            ]
            for connection in connections:
                connection.set_progress_handler(lambda: int(operation_expired()), 1000)
            for index, (logical, generation, _path) in enumerate(state.shards):
                connection = connections[index]
                window = _PreparationWindow(bounded + 1, forward=direction == "forward")
                last_rowid: int | None = None
                while True:
                    _check_preparation(check_authority)
                    where = "WHERE rowid > ?" if last_rowid is not None else ""
                    params = (last_rowid, batch) if last_rowid is not None else (batch,)
                    try:
                        rows = connection.execute(
                            "SELECT rowid AS scan_rowid, source_conversation_id, "
                            f"{position_columns} "
                            f"FROM messages {where} ORDER BY rowid LIMIT ?", params,
                        ).fetchall()
                        target_rows = {
                            str(row["source_message_id"]): row for row in rows
                            if row["source_conversation_id"] == conversation_source_id
                        }
                        if target_rows:
                            snapshot.dependency_generation_by_shard[logical] = generation
                        # Canonicalize positions before truncation. Equal overlap may
                        # use a different source_rowid in the later shard; truncating
                        # first could otherwise lose an immediate unique neighbor.
                        canonical = {key: (row, index) for key, row in target_rows.items()}
                        ids = tuple(target_rows)
                        for earlier in range(index):
                            for offset in range(0, len(ids), 256):
                                check_operation_budget()
                                selected_ids = ids[offset:offset + 256]
                                placeholders = ",".join("?" for _ in selected_ids)
                                for row in connections[earlier].execute(
                                    f"SELECT {position_columns} FROM messages "
                                    f"WHERE source_message_id IN ({placeholders})", selected_ids,
                                ):
                                    key = str(row["source_message_id"])
                                    if canonical[key][1] > earlier:
                                        canonical[key] = (row, earlier)
                        for source_id, (row, canonical_index) in canonical.items():
                            check_operation_budget()
                            key = (
                                to_utc_iso(str(row["sent_at_utc"])), int(row["sort_seq"]),
                                int(row["source_rowid"]), source_id,
                            )
                            if minimum_time is not None and key[0] < minimum_time:
                                continue
                            if maximum_time is not None and key[0] >= maximum_time:
                                continue
                            if after is not None and key <= after.as_tuple():
                                continue
                            if before is not None and key >= before.as_tuple():
                                continue
                            window.offer(key, (key, canonical_index))
                    except sqlite3.Error:
                        check_operation_budget()
                        raise
                    scanned += len(rows)
                    exhausted = len(rows) < batch
                    if exhausted:
                        completed += 1
                    else:
                        last_rowid = int(rows[-1]["scan_rowid"])
                    _check_preparation(check_authority)
                    yield SourcePreparationStep("positions", scanned, completed, len(state.shards))
                    if exhausted:
                        break
                windows.extend(window.selected())

            selected = {}
            for key, index in sorted(windows, reverse=direction == "backward"):
                selected.setdefault(key[3], (key, index))
                if len(selected) == bounded + 1:
                    break
            more = len(selected) > bounded
            messages = []
            for source_id in tuple(selected)[:bounded]:
                _check_preparation(check_authority)
                matches = []
                try:
                    for index, connection in enumerate(connections):
                        row = connection.execute(
                            "SELECT * FROM messages WHERE source_message_id = ?", (source_id,),
                        ).fetchone()
                        if row is not None:
                            matches.append((row, index))
                except sqlite3.Error:
                    check_operation_budget()
                    raise
                if len({self._row_semantic_digest(row) for row, _index in matches}) != 1:
                    raise SightglassError(
                        ErrorCode.SOURCE_INCOMPLETE,
                        details={"warning_codes": ["duplicate_message_identity_conflict"]},
                    )
                row, index = min(matches, key=lambda item: (
                    state.shards[item[1]][0], int(item[0]["source_rowid"]),
                    state.shards[item[1]][1],
                ))
                logical, generation, _path = state.shards[index]
                messages.append(self._row_to_message(
                    row, logical, generation, conversation_kind=kind, account=account,
                ))
                _check_preparation(check_authority)
                yield SourcePreparationStep("payload", scanned, completed, len(state.shards))
        messages.sort(key=lambda item: item.sort_key.as_tuple())
        self._assert_snapshot(snapshot)
        _check_preparation(check_authority)
        yield SourcePreparationStep(
            "complete", scanned, completed, len(state.shards),
            SourceMessagePage(
                tuple(messages), has_more_before=direction == "backward" and more,
                has_more_after=direction == "forward" and more,
            ),
        )

    @staticmethod
    def _discovery_position(logical_key: str, rowid: int) -> dict[str, Any]:
        """Provider-owned physical continuation: resume the shard below ``rowid``."""

        return {"schema": DISCOVERY_POSITION_SCHEMA, "shard": logical_key, "rowid": int(rowid)}

    @staticmethod
    def _discovery_start(
        position: dict[str, Any] | None,
        logical_keys: tuple[str, ...],
    ) -> tuple[int, int | None]:
        """Resolve a position to ``(shard_index, exclusive_rowid)`` or fail closed."""

        if position is None:
            return (0, None)
        if not isinstance(position, dict):
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if position.get("schema") != DISCOVERY_POSITION_SCHEMA:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        shard = position.get("shard")
        rowid = position.get("rowid")
        if not isinstance(shard, str) or shard not in logical_keys:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        if isinstance(rowid, bool) or not isinstance(rowid, int) or rowid < 0:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        return (logical_keys.index(shard), rowid)

    @staticmethod
    def _discovery_scan_rows(
        path: Path,
        conversation_source_id: str,
        *,
        after_rowid: int | None,
        page_size: int,
    ) -> list[sqlite3.Row]:
        """One bounded descending-``rowid`` raw page from a single fixture shard."""

        bound = max(1, min(int(page_size), DISCOVERY_SCAN_CAP))
        clauses = ["source_conversation_id = ?"]
        params: list[Any] = [conversation_source_id]
        if after_rowid is not None:
            clauses.append("rowid < ?")
            params.append(int(after_rowid))
        params.append(bound)
        where = " AND ".join(clauses)
        # Bounded point range over the fixture's own physical rowid: no chronological
        # ORDER BY and no whole-history materialization.
        with SyntheticSourceProvider._connect_readonly(path) as connection:
            return connection.execute(
                f"""
                SELECT rowid AS scan_rowid, source_message_id, source_conversation_id,
                       source_time_raw, sent_at_utc, observed_at_utc, sort_seq,
                       source_rowid, wechat_type, raw_content, is_outgoing,
                       sender_internal_id, sender_local_token, sender_surface_label,
                       resources_json
                FROM messages
                WHERE {where}
                ORDER BY rowid DESC
                LIMIT ?
                """,
                params,
            ).fetchall()

    def scan_discovery_page(
        self,
        account_id: str,
        conversation_source_id: str,
        *,
        snapshot: SourceSnapshot,
        position: dict[str, Any] | None = None,
        limit: int = DISCOVERY_DEFAULT_LIMIT,
        time_after_utc: str | None = None,
        time_before_utc: str | None = None,
    ) -> SourceDiscoveryPage:
        """Bounded fixture physical scan; never re-materializes the full history.

        Mirrors the native provider contract: each call inspects at most
        ``min(limit, DISCOVERY_SCAN_CAP)`` raw rows in descending rowid order and
        applies the time window in Python. Returned rows are discovery candidates:
        the reader must revalidate each through ``get_message`` before admission and
        dedupe by canonical ``source_message_id``. ``has_more`` is conservative, so a
        page that fills exactly at the bound may require one extra terminating call.
        """

        check_operation_budget()
        bounded = max(1, min(int(limit), DISCOVERY_SCAN_CAP))
        minimum_time = to_utc_iso(time_after_utc) if time_after_utc is not None else None
        maximum_time = to_utc_iso(time_before_utc) if time_before_utc is not None else None
        state = self._assert_snapshot(snapshot)
        self._enforce_scope(
            state,
            account_id=account_id,
            conversation_source_id=conversation_source_id,
        )
        account = self._require_account(account_id, state.manifest)
        kind = self._conversation_kind(state, conversation_source_id)
        logical_keys = tuple(item[0] for item in state.shards)
        start_index, start_rowid = self._discovery_start(position, logical_keys)

        inspected = 0
        messages: list[SourceMessage] = []
        positions: list[dict[str, Any]] = []
        last_key: str | None = None
        last_rowid: int | None = None
        has_more = False

        for shard_index in range(start_index, len(state.shards)):
            logical_key, generation_id, path = state.shards[shard_index]
            after_rowid = start_rowid if shard_index == start_index else None
            while inspected < bounded:
                remaining = bounded - inspected
                rows = self._discovery_scan_rows(
                    path,
                    conversation_source_id,
                    after_rowid=after_rowid,
                    page_size=remaining,
                )
                if not rows:
                    break
                for row in rows:
                    check_operation_budget()
                    inspected += 1
                    last_key = logical_key
                    last_rowid = int(row["scan_rowid"])
                    sent_at_utc = to_utc_iso(str(row["sent_at_utc"]))
                    within_window = (
                        (minimum_time is None or sent_at_utc >= minimum_time)
                        and (maximum_time is None or sent_at_utc < maximum_time)
                    )
                    if not within_window:
                        continue
                    messages.append(
                        self._row_to_message(
                            row,
                            logical_key,
                            generation_id,
                            conversation_kind=kind,
                            account=account,
                        )
                    )
                    positions.append(self._discovery_position(logical_key, last_rowid))
                if inspected >= bounded:
                    has_more = True
                    break
                after_rowid = last_rowid
                if len(rows) < remaining:
                    break
            if inspected >= bounded:
                break

        next_position = (
            self._discovery_position(last_key, last_rowid)
            if has_more and last_key is not None and last_rowid is not None
            else None
        )
        self._assert_snapshot(snapshot)
        check_operation_budget()
        return SourceDiscoveryPage(
            messages=tuple(messages),
            positions=tuple(positions),
            next_position=next_position,
            has_more=has_more,
            scanned_rows=inspected,
        )

    def get_message(
        self,
        account_id: str,
        source_message_id: str,
        snapshot: SourceSnapshot,
    ) -> SourceMessage | None:
        state = self._assert_snapshot(snapshot)
        self._enforce_scope(
            state,
            account_id=account_id,
            source_message_id=source_message_id,
        )
        account = self._require_account(account_id, state.manifest)
        matches: list[tuple[sqlite3.Row, str, str]] = []
        allowed = (
            state.scope.conversation_source_ids if state.scope is not None
            and state.scope.kind == "conversations" else
            (state.scope.conversation_source_id,) if state.scope is not None
            and state.scope.conversation_source_id is not None else ()
        )
        for logical_key, generation_id, path in state.shards:
            check_operation_budget()
            with self._connect_readonly(path) as connection:
                if allowed:
                    placeholders = ",".join("?" for _ in allowed)
                    selected = connection.execute(
                        "SELECT * FROM messages WHERE source_message_id=? "
                        f"AND source_conversation_id IN ({placeholders})",
                        (source_message_id, *allowed),
                    ).fetchall()
                else:
                    selected = connection.execute(
                        "SELECT * FROM messages WHERE source_message_id=?", (source_message_id,),
                    ).fetchall()
            matches.extend((row, logical_key, generation_id) for row in selected)
        if not matches:
            self._assert_snapshot(snapshot)
            return None
        if len({self._row_semantic_digest(row) for row, _logical, _generation in matches}) > 1:
            raise SightglassError(
                ErrorCode.SOURCE_INCOMPLETE,
                details={"warning_codes": ["duplicate_message_identity_conflict"]},
            )
        row, logical_key, generation_id = min(
            matches,
            key=lambda item: (item[1], int(item[0]["source_rowid"]), item[2]),
        )
        self._enforce_scope(
            state, account_id=account_id, source_message_id=source_message_id,
            conversation_source_id=str(row["source_conversation_id"]),
        )
        kind = self._conversation_kind(state, str(row["source_conversation_id"]))
        found = self._row_to_message(
            row, logical_key, generation_id, conversation_kind=kind, account=account
        )
        self._assert_snapshot(snapshot)
        return found

    def capture_resource_binding(
        self, request: CaptureRequest, snapshot: SourceSnapshot,
    ) -> ResourceCaptureBinding:
        """Prove a fixture's exact resource metadata binding in this one session.

        This reads only ID/resource descriptor columns from the private copied
        fixture. It does not hydrate/parse the message or open another snapshot.
        """
        from sightglass.contracts.capture import CaptureProtocolError
        from sightglass.source.capture.resource import request_resource_binding

        state = self._assert_snapshot(snapshot)
        self._require_account(request.account_id, state.manifest)
        self._enforce_scope(state, source_resource_key=request.resource_key)
        binding = request_resource_binding(request)
        descriptors = []
        for _logical, _generation, path in state.shards:
            with self._connect_readonly(path) as connection:
                rows = connection.execute(
                    "SELECT source_conversation_id, resources_json FROM messages "
                    "WHERE source_message_id = ?",
                    (binding.source_message_id,),
                ).fetchall()
            for row in rows:
                if str(row["source_conversation_id"]) != binding.conversation_source_id:
                    raise CaptureProtocolError("resource_locator_scope_mismatch")
                try:
                    resources = json.loads(str(row["resources_json"] or "[]"))
                except json.JSONDecodeError as exc:
                    raise CaptureProtocolError("resource_locator_invalid") from exc
                descriptors.extend(
                    item for item in resources
                    if item.get("source_resource_key") == binding.source_resource_key
                )
        descriptor = request.resource_descriptor
        if not descriptors or descriptor is None or any(
            any(item.get(name) != getattr(descriptor, name) for name in (
                "kind", "mime_type", "original_name", "declared_size", "declared_hash",
            )) for item in descriptors
        ):
            raise CaptureProtocolError("resource_locator_scope_mismatch")
        return binding

    def read_resource(
        self,
        source_resource_key: str,
        *,
        max_bytes: int,
        snapshot: SourceSnapshot,
    ) -> SourceResourcePayload:
        if max_bytes < 1:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        state = self._assert_snapshot(snapshot)
        self._enforce_scope(state, source_resource_key=source_resource_key)
        resources = {
            key: (path, state_name, digest, size, encoding, decoder_key)
            for key, path, state_name, digest, size, encoding, decoder_key in state.resources
        }
        entry = resources.get(source_resource_key)
        if entry is None:
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
        path, state_name, expected_digest, expected_size, encoding, decoder_key = entry
        if state_name.startswith("blocked_"):
            raise SightglassError(
                ErrorCode.RESOURCE_BLOCKED,
                details={"reason": state_name.removeprefix("blocked_")},
            )
        if state_name != "present" or expected_digest is None or expected_size is None:
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE)
        if expected_size > max_bytes:
            raise SightglassError(
                ErrorCode.RESOURCE_TOO_LARGE,
                details={"max_bytes": max_bytes},
            )
        try:
            descriptor = self._open_resource_nofollow(path)
            with os.fdopen(descriptor, "rb", closefd=True) as handle:
                metadata = os.fstat(handle.fileno())
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_nlink != 1
                    or metadata.st_size != expected_size
                ):
                    raise SightglassError(ErrorCode.RESOURCE_BLOCKED)
                data = handle.read(max_bytes + 1)
        except SightglassError:
            raise
        except OSError as exc:
            raise SightglassError(ErrorCode.RESOURCE_UNAVAILABLE) from exc
        if len(data) > max_bytes:
            raise SightglassError(
                ErrorCode.RESOURCE_TOO_LARGE,
                details={"max_bytes": max_bytes},
            )
        if hashlib.sha256(data).hexdigest() != expected_digest:
            raise SightglassError(ErrorCode.SOURCE_GENERATION_CHANGED, retryable=True)
        if encoding == "wechat_v2":
            assert decoder_key is not None
            data = decode_wechat_v2_image(data, decoder_key, xor_key=0x88)
            if len(data) > max_bytes:
                raise SightglassError(
                    ErrorCode.RESOURCE_TOO_LARGE,
                    details={"max_bytes": max_bytes},
                )
        self._assert_snapshot(snapshot)
        return SourceResourcePayload(source_resource_key=source_resource_key, data=data)

    def catalog_complete(self, snapshot: SourceSnapshot) -> bool:
        state = self._assert_snapshot(snapshot)
        return bool(state.manifest["catalog"].get("complete"))

    def active_conversations_only(self, snapshot: SourceSnapshot) -> bool:
        state = self._assert_snapshot(snapshot)
        return bool(state.manifest["catalog"].get("active_only"))


# Compatibility import for the M0-M4 alpha line. New construction uses the
# accurate class name; remove this alias only at a declared public API break.
DirectWeChatSourceProvider = SyntheticSourceProvider

"""Offline compact candidate construction for ``window.db`` (SG-059).

This module is deliberately separate from normal startup.  Opening a legacy
``window.db`` normally must never trigger the large schema/backend conversion;
that conversion is a stopped-only, explicit, resumable operation performed by
an operator through the compact candidate path.

The candidate:

* re-encodes legacy ``TEXT`` observation payloads through the *canonical*
  :mod:`sightglass.model.observation_codec` from their original UTF-8 bytes
  (no JSON reserialization), leaving valid ``BLOB`` payloads untouched;
* rebuilds the lexical index as an FTS5 ``contentless_delete=1`` trigram table
  (``detail=none``; ``columnsize=0`` is refused) after an actual capability
  probe;
* preserves ``(rowid, stable identity)`` for rowid consumers and the historical
  ``sqlite_sequence`` high-water marks;
* is resumable and records a content-free manifest keyed to one frozen
  committed input so an exact recovery point can pair with it.

Real candidate construction, cleanup, migration and activation remain separate
operator authority; nothing here touches an installed runtime or account.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import stat
from collections.abc import Collection, Iterator
from contextlib import closing
from functools import lru_cache
from pathlib import Path
from typing import Any

from .current_body import CURRENT_BODY_VERSION, LegacyBodyConflict, normalized_columns
from .observation_codec import (
    decode_observation_bytes,
    encode_observation,
)

CANDIDATE_MANIFEST_SCHEMA = "sightglass.compact-candidate-manifest.v1"
CANDIDATE_PREVIEW_SCHEMA = "sightglass.compact-candidate-preview.v1"
CANDIDATE_PLAN_SCHEMA = "sightglass.compact-candidate-plan.v1"
ROWID_MANIFEST_SCHEMA = "sightglass.rowid-identity-manifest.v1"
LEXICAL_CANDIDATE_RECIPE = "sightglass.casefold-trigram.v1"

# Rowid consumers whose (rowid -> stable id) mapping must round-trip exactly.
ROWID_STABLE_KEY_TABLES: tuple[tuple[str, str], ...] = (
    ("messages", "message_id"),
    ("message_observations", "observation_id"),
    ("identity_corrections", "correction_id"),
    ("resources", "resource_id"),
    ("conversations", "conversation_id"),
    ("accounts", "account_id"),
)

_COPY_CHUNK = 500
_OFFLINE_CACHE_KIB = 8192


class CompactCandidateError(RuntimeError):
    """A candidate/preview step failed closed; the input was not modified."""


def capability_probe(connection: sqlite3.Connection) -> dict[str, Any]:
    """Probe the *actual* runtime for FTS5 contentless-delete trigram support.

    A version string is not evidence; this creates a real temporary table and
    exercises insert/match/delete.  ``columnsize=0`` is deliberately treated as
    incompatible rather than attempted.
    """

    report: dict[str, Any] = {
        "sqlite_version": sqlite3.sqlite_version,
        "fts5_trigram": False,
        "contentless_delete": False,
        "columnsize_zero_incompatible": True,
        "usable": False,
    }
    try:
        connection.execute(
            "CREATE VIRTUAL TABLE temp.__sg_fts_probe USING fts5("
            "text, tokenize='trigram case_sensitive 1', detail=none, "
            "content='', contentless_delete=1)"
        )
        connection.execute(
            "INSERT INTO temp.__sg_fts_probe(rowid, text) VALUES (1, 'synthetic probe')"
        )
        matched = connection.execute(
            "SELECT rowid FROM temp.__sg_fts_probe WHERE __sg_fts_probe MATCH '\"syn\"'"
        ).fetchall()
        connection.execute("DELETE FROM temp.__sg_fts_probe WHERE rowid = 1")
        connection.execute("DROP TABLE temp.__sg_fts_probe")
        report["fts5_trigram"] = True
        report["contentless_delete"] = True
        report["usable"] = [int(r[0]) for r in matched] == [1]
    except sqlite3.OperationalError as error:
        report["error"] = str(error)
    return report


def _open_read_only(path: Path) -> sqlite3.Connection:
    from .backups import _private_regular_file

    _private_regular_file(path)
    connection = sqlite3.connect(path.absolute().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute(f"PRAGMA cache_size=-{_OFFLINE_CACHE_KIB}")
    connection.execute("PRAGMA query_only=ON")
    return connection


def file_revision(path: Path) -> dict[str, Any]:
    from .backups import _private_regular_file

    result = {}
    for suffix in ("", "-wal", "-journal"):
        sibling = path.with_name(path.name + suffix)
        if sibling.exists():
            meta = _private_regular_file(sibling)
            result[suffix or "main"] = (
                None
                if suffix == "-wal" and meta.st_size == 0
                else [meta.st_dev, meta.st_ino, meta.st_size, meta.st_mtime_ns]
            )
        else:
            result[suffix or "main"] = None
    return result


def read_legacy_preview(database_path: Path) -> dict[str, Any]:
    """Page and schema metadata only; no full-history COUNT or layout scan."""
    path = database_path.absolute()
    revision = file_revision(path)
    with closing(_open_read_only(path)) as connection:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        pages = int(connection.execute("PRAGMA page_count").fetchone()[0])
        size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        free = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
        sequence = dict(connection.execute("SELECT name,seq FROM sqlite_sequence"))
    with closing(sqlite3.connect(":memory:")) as probe:
        capability = capability_probe(probe)
    if revision != file_revision(path):
        raise CompactCandidateError("preview input changed")
    return {
        "schema": CANDIDATE_PREVIEW_SCHEMA,
        "source_identity": revision,
        "legacy_schema_version": version,
        "database_bytes": pages * size,
        "freelist_pages": free,
        "sqlite_sequence": sequence,
        "message_count": None,
        "observation_count": None,
        "capability": capability,
        "read_only": True,
        "mutated": False,
    }


def preview_release(
    database_path: Path, conversation_id: str, *, after: str | None = None, limit: int = 200
) -> dict[str, Any]:
    from sightglass.residency.repository import ResidencyRepository

    identity = file_revision(database_path)
    with closing(_open_read_only(database_path)) as connection:
        connection.execute("BEGIN")
        result = ResidencyRepository.preview_connection(
            connection, conversation_id, after=after, limit=limit, source_identity=identity
        )
    if identity != file_revision(database_path):
        raise CompactCandidateError("release preview input changed")
    result.update(read_only=True, mutated=False)
    return result


def _free(path: Path) -> int:
    stats = os.statvfs(path)
    return int(stats.f_bavail) * int(stats.f_frsize or stats.f_bsize)


def _capacity(
    workspace: Path,
    *,
    budget: int,
    min_free: int,
    reserve: int = 0,
    additional_roots: tuple[Path, ...] = (),
) -> int:
    usage = 0
    seen: set[tuple[int, int]] = set()
    for root in (workspace, *additional_roots):
        for directory, dirs, files in os.walk(root, followlinks=False):
            base = Path(directory)
            dirs[:] = [name for name in dirs if not (base / name).is_symlink()]
            for name in files:
                item = base / name
                if item.is_file() and not item.is_symlink():
                    metadata = item.stat()
                    key = (metadata.st_dev, metadata.st_ino)
                    if key not in seen:
                        seen.add(key)
                        usage += max(metadata.st_size, metadata.st_blocks * 512)
    if usage + reserve > budget or _free(workspace) - reserve < min_free:
        raise CompactCandidateError("offline workspace budget/free floor exceeded")
    return usage


def require_encrypted_volume(path: Path) -> None:
    """Native recovery bytes require verified encryption on the destination volume."""
    import plistlib
    import subprocess

    existing = path.absolute()
    while not existing.exists():
        existing = existing.parent
    result = subprocess.run(["df", "-P", str(existing)], check=True, capture_output=True, text=True)
    device = result.stdout.splitlines()[-1].split()[0]
    info = subprocess.run(
        ["/usr/sbin/diskutil", "info", "-plist", device], check=True, capture_output=True
    )
    value = plistlib.loads(info.stdout)
    if value.get("Encryption") is not True or value.get("EncryptionThisVolumeProper") is not True:
        raise CompactCandidateError("native recovery destination encryption is unverified")


def freeze_input(
    source_path: Path,
    workspace: Path,
    *,
    workspace_budget_bytes: int,
    min_free_bytes: int,
    expected_identity: dict[str, Any] | None = None,
    fault: Any = None,
) -> dict[str, Any]:
    """SQLite backup pins one committed input; its exact compressed recovery is paired.

    Caller holds the stopped runtime process lock. Other writers changing the selected
    source revision invalidate the freeze instead of being guessed away.
    """
    from .backups import (
        _fsync_directory,
        _hash_file,
        _write_json_atomic,
        create_compressed_snapshot,
        verify_compressed_snapshot,
    )

    source = source_path.absolute()
    workspace = workspace.absolute()
    workspace.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stat.S_IMODE(workspace.stat().st_mode) & 0o077:
        raise CompactCandidateError("offline workspace must be owner-private")
    intent_path = workspace / "freeze.json"
    frozen = workspace / "frozen.db"
    if intent_path.exists():
        import json

        intent = json.loads(intent_path.read_text())
        digest, _ = _hash_file(frozen)
        if digest != intent["frozen_digest"]:
            raise CompactCandidateError("frozen input identity changed")
        verify_compressed_snapshot(
            frozen, Path(intent["recovery_artifact"]), expected_raw_sha256=digest
        )
        return intent
    import json
    import uuid

    journal_path = workspace / "freeze-progress.json"
    stage = workspace / "freeze-copy.db"
    identity = file_revision(source)
    if expected_identity is not None and identity != expected_identity:
        raise CompactCandidateError("freeze preview changed")
    journal = json.loads(journal_path.read_text()) if journal_path.exists() else None
    if journal is not None and journal["source_identity"] != identity:
        raise CompactCandidateError("interrupted freeze source changed")
    if journal is None or journal["phase"] == "copying":
        if frozen.exists():
            raise CompactCandidateError("unowned frozen input exists; retain it for recovery")
        if stage.exists():
            os.replace(stage, workspace / ("incomplete-freeze-" + uuid.uuid4().hex + ".db"))
            _fsync_directory(workspace)
        _capacity(
            workspace,
            budget=workspace_budget_bytes,
            min_free=min_free_bytes,
            reserve=source.stat().st_size * 2 + 1024**2,
        )
        _write_json_atomic(journal_path, {"phase": "copying", "source_identity": identity})
        with closing(_open_read_only(source)) as reader:
            reader.execute("BEGIN")
            with closing(sqlite3.connect(stage)) as writer:
                os.chmod(stage, 0o600)

                def progress(status, remaining, total):
                    _capacity(workspace, budget=workspace_budget_bytes, min_free=min_free_bytes)

                reader.backup(writer, pages=256, progress=progress)
        if identity != file_revision(source):
            raise CompactCandidateError("input changed while freezing")
        with stage.open("rb") as handle:
            os.fsync(handle.fileno())
        digest, frozen_bytes = _hash_file(stage)
        journal = {
            "phase": "copied",
            "source_identity": identity,
            "frozen_digest": digest,
            "frozen_bytes": frozen_bytes,
        }
        _write_json_atomic(journal_path, journal)
    digest, frozen_bytes = journal["frozen_digest"], journal["frozen_bytes"]
    if not frozen.exists():
        if _hash_file(stage) != (digest, frozen_bytes):
            raise CompactCandidateError("freeze copy changed")
        os.replace(stage, frozen)
        _fsync_directory(workspace)
    elif _hash_file(frozen) != (digest, frozen_bytes):
        raise CompactCandidateError("frozen input changed")
    if fault:
        fault("freeze_copied")
    _capacity(
        workspace,
        budget=workspace_budget_bytes,
        min_free=min_free_bytes,
        reserve=frozen_bytes + 1024**2,
    )
    recovery = create_compressed_snapshot(frozen)
    artifact = workspace / recovery["artifact"]
    verify_compressed_snapshot(frozen, artifact, expected_raw_sha256=digest)
    intent = {
        "schema": "sightglass.compact-freeze.v1",
        "source_identity": identity,
        "frozen_digest": digest,
        "frozen_bytes": frozen_bytes,
        "recovery_artifact": str(artifact),
        "workspace_budget_bytes": workspace_budget_bytes,
        "min_free_bytes": min_free_bytes,
    }
    _write_json_atomic(intent_path, intent)
    _fsync_directory(workspace)
    _capacity(workspace, budget=workspace_budget_bytes, min_free=min_free_bytes)
    return intent


def build_rowid_manifest(connection: sqlite3.Connection) -> dict[str, Any]:
    """Streaming identity mappings; no account-sized Python dictionary."""
    tables = {
        row[0] for row in connection.execute("SELECT name FROM sqlite_schema WHERE type='table'")
    }
    digests, counts = {}, {}
    for table, column in ROWID_STABLE_KEY_TABLES:
        if table not in tables:
            continue
        digest, count = hashlib.sha256(), 0
        for row in connection.execute(f'SELECT rowid,"{column}" FROM "{table}" ORDER BY rowid'):
            encoded = str(row[1]).encode("utf-8")
            digest.update(int(row[0]).to_bytes(8, "big", signed=True))
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
            count += 1
        digests[table], counts[table] = digest.hexdigest(), count
    return {
        "schema": ROWID_MANIFEST_SCHEMA,
        "digests": digests,
        "counts": counts,
        "sequence": dict(connection.execute("SELECT name,seq FROM sqlite_sequence")),
    }


class _StockRelease(Collection[str]):
    """Exact frozen-snapshot membership without a whole-history Python ID set."""

    def __init__(self, reader: sqlite3.Connection) -> None:
        self.reader = reader
        available = "body_available" in {
            row[1] for row in reader.execute("PRAGMA table_info(messages)")
        }
        self.predicate = "body_available=1" if available else "1"
        # The connection is pinned to the frozen read transaction. Most stopped
        # installations have no active dependencies: avoid millions of empty probes.
        self.has_pins = any(
            reader.execute(f"SELECT 1 FROM {table} WHERE {condition} LIMIT 1").fetchone()
            for table, condition in (
                ("resource_jobs", "state IN ('pending','leased','running')"),
                ("voice_jobs", "state IN ('pending','leased','running')"),
                ("reader_deliveries", "status='pending'"),
            )
        )
        self.all_legacy_messages = not available and not self.has_pins
        self._contains = lru_cache(maxsize=1024)(self._eligible)

    def _eligible(self, identity: str) -> bool:
        from sightglass.residency.repository import ResidencyRepository

        return bool(
            self.reader.execute(
                f"SELECT 1 FROM messages WHERE message_id=? AND {self.predicate}", (identity,)
            ).fetchone()
            and (not self.has_pins or not ResidencyRepository._pinned(self.reader, identity))
        )

    def __contains__(self, identity: object) -> bool:
        return isinstance(identity, str) and self._contains(identity)

    def __iter__(self) -> Iterator[str]:
        from sightglass.residency.repository import ResidencyRepository

        for row in self.reader.execute(
            f"SELECT message_id FROM messages WHERE {self.predicate} ORDER BY rowid"
        ):
            if not self.has_pins or not ResidencyRepository._pinned(self.reader, row[0]):
                yield str(row[0])

    def __len__(self) -> int:
        if not self.has_pins:
            return int(
                self.reader.execute(
                    f"SELECT COUNT(*) FROM messages WHERE {self.predicate}"
                ).fetchone()[0]
            )
        return sum(1 for _ in self)

    def __bool__(self) -> bool:
        return next(iter(self), None) is not None


def _released_message(selection: Collection[str], identity: Any) -> bool:
    """Membership for a message/reference row already read from the frozen input.

    Schema 9 has no unavailable bodies. An all-stock plan with no active pins
    therefore selects every source message; re-seeking its random ID index for
    every observation would add a full history of redundant disk reads.
    """
    if isinstance(selection, _StockRelease) and selection.all_legacy_messages:
        return identity is not None
    return identity in selection


def preview_stock_release(database_path: Path) -> dict[str, Any]:
    """Bind all existing eligible bodies to one exact committed file revision.

    This explicit deep operator preview streams the stock. The plan carries no
    message list; the stopped freeze must match its source identity exactly.
    """
    import json

    identity = file_revision(database_path)
    with closing(_open_read_only(database_path)) as reader:
        reader.execute("BEGIN")
        selected = _StockRelease(reader)
        total = int(
            reader.execute(f"SELECT COUNT(*) FROM messages WHERE {selected.predicate}").fetchone()[
                0
            ]
        )
        releasable = len(selected)
        plan = {
            "scope": "all_existing_bodies",
            "source_identity": identity,
            "examined_messages": total,
            "releasable_messages": releasable,
            "pinned_messages": total - releasable,
        }
    if identity != file_revision(database_path):
        raise CompactCandidateError("stock release preview input changed")
    plan["plan_digest"] = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
    return {
        "schema": "sightglass.stock-release-preview.v1",
        "plan": plan,
        "read_only": True,
        "mutated": False,
        "filesystem_reclaim_bytes": None,
    }


def _approved_release(
    reader: sqlite3.Connection, plans: list[dict[str, Any]], source_path: Path
) -> Collection[str]:
    import json

    from sightglass.residency.repository import ResidencyRepository

    stock = [plan for plan in plans if plan.get("scope") == "all_existing_bodies"]
    if stock:
        if len(plans) != 1:
            raise CompactCandidateError("whole-stock release cannot mix with page plans")
        content = dict(stock[0])
        digest = content.pop("plan_digest", None)
        if digest != hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest():
            raise CompactCandidateError("release plan changed")
        freeze = json.loads((source_path.parent / "freeze.json").read_text())
        from .backups import _hash_file

        if (
            content["source_identity"] != freeze["source_identity"]
            or _hash_file(source_path)[0] != freeze["frozen_digest"]
        ):
            raise CompactCandidateError("stock release frozen identity changed")
        return _StockRelease(reader)

    identities = set()
    for plan in plans:
        content = dict(plan)
        digest = content.pop("plan_digest", None)
        if digest != hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest():
            raise CompactCandidateError("release plan changed")
        tables = {
            row[0] for row in reader.execute("SELECT name FROM sqlite_schema WHERE type='table'")
        }
        revision = (
            int(reader.execute("SELECT revision FROM residency_state").fetchone()[0])
            if "residency_state" in tables
            else 0
        )
        if content["residency_revision"] != revision:
            raise CompactCandidateError("release revision changed")
        if len(content["entries"]) > 500:
            raise CompactCandidateError("release preview batch exceeds its bound")
        for entry in content["entries"]:
            row = reader.execute(
                "SELECT conversation_id,current_observation_seq FROM messages WHERE message_id=?",
                (entry["message_id"],),
            ).fetchone()
            if (
                row is None
                or row[0] != content["conversation_id"]
                or row[1] != entry["observation_seq"]
            ):
                raise CompactCandidateError("release episode changed")
            if ResidencyRepository._body_bytes(reader, entry["message_id"]) != entry["body_bytes"]:
                raise CompactCandidateError("release preview bytes changed")
            if not entry["pinned"] and not ResidencyRepository._pinned(reader, entry["message_id"]):
                identities.add(entry["message_id"])
    return identities


def build_candidate(
    source_path: Path,
    destination_path: Path,
    *,
    expected_identity: dict[str, Any] | None = None,
    release_plans: list[dict[str, Any]] | None = None,
    workspace_budget_bytes: int,
    min_free_bytes: int,
    fault: Any = None,
) -> dict[str, Any]:
    """Restart-safe batches copy rowids and encode/release bodies before insertion.

    Input is the immutable frozen.db from freeze_input, never the live window DB.
    Progress commits in the candidate with its rows; no partially committed batch
    is reported as complete. Candidate and recovery share the frozen digest.
    """
    import json

    from .backups import _fsync_directory, _hash_file, _write_json_atomic
    from .lexical import publish_lexical
    from .links import input_digest as canonical_link_digest
    from .observation_codec import (
        build_released_header,
        encode_released_observation,
        observation_payload_state,
    )
    from .schema import SCHEMA_SQL, SCHEMA_VERSION

    source, destination = source_path.absolute(), destination_path.absolute()
    if source == destination:
        raise CompactCandidateError("candidate must not overwrite its input")
    identity = file_revision(source)
    if expected_identity is not None and identity != expected_identity:
        raise CompactCandidateError("candidate source changed")
    digest, _ = _hash_file(source)
    freeze_path = source.parent / "freeze.json"
    from .backups import _private_regular_file, verify_compressed_snapshot

    _private_regular_file(freeze_path)
    freeze = json.loads(freeze_path.read_text())
    if source.name != "frozen.db" or digest != freeze["frozen_digest"]:
        raise CompactCandidateError("candidate requires its exact verified frozen input")
    verify_compressed_snapshot(
        source, Path(freeze["recovery_artifact"]), expected_raw_sha256=digest
    )
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stat.S_IMODE(destination.parent.stat().st_mode) & 0o077:
        raise CompactCandidateError("candidate directory must be owner-private")
    manifest_path = destination.with_suffix(destination.suffix + ".json")
    scope_digest = hashlib.sha256(
        json.dumps(release_plans or [], sort_keys=True).encode()
    ).hexdigest()
    if manifest_path.exists():
        receipt = json.loads(manifest_path.read_text())
        if receipt.get("current_body_version") != CURRENT_BODY_VERSION:
            raise CompactCandidateError("completed candidate transformation changed")
        if receipt["frozen_digest"] != digest or receipt["release_scope_digest"] != scope_digest:
            raise CompactCandidateError("completed candidate input/scope changed")
        verify_candidate(source, destination, release_plans=release_plans)
        _retire_progress(destination)
        return receipt
    with closing(sqlite3.connect(":memory:")) as probe:
        capability = capability_probe(probe)
    if not capability["usable"]:
        raise CompactCandidateError("contentless-delete backend unavailable")
    _capacity(
        destination.parent, budget=workspace_budget_bytes, min_free=min_free_bytes, reserve=1024**2
    )
    new = not destination.exists()
    with (
        closing(_open_read_only(source)) as reader,
        closing(sqlite3.connect(destination)) as writer,
    ):
        reader.execute("BEGIN")
        version = int(reader.execute("PRAGMA user_version").fetchone()[0])
        if version not in {9, SCHEMA_VERSION}:
            raise CompactCandidateError("compact input must be schema 9 or the current schema")
        writer.row_factory = sqlite3.Row
        writer.execute(f"PRAGMA cache_size=-{_OFFLINE_CACHE_KIB}")
        writer.execute("PRAGMA foreign_keys=OFF")
        writer.execute("PRAGMA journal_mode=WAL")
        if new:
            writer.executescript(SCHEMA_SQL)
            # Source ledgers are copied exactly; do not run accounting triggers during copy.
            for row in writer.execute(
                "SELECT name FROM sqlite_schema WHERE type='trigger'"
            ).fetchall():
                writer.execute(f'DROP TRIGGER "{row[0]}"')
            writer.execute("DELETE FROM residency_state")
            writer.execute(
                "CREATE TABLE compact_progress(table_name TEXT PRIMARY KEY, "
                "after_rowid INTEGER,copied INTEGER,encoded INTEGER)"
            )
            writer.execute(
                "CREATE TABLE compact_identity(digest TEXT,scope TEXT,body_version TEXT)"
            )
            writer.execute("CREATE TABLE compact_peak(workspace INTEGER,wal INTEGER)")
            writer.execute("INSERT INTO compact_peak VALUES (0,0)")
            writer.execute(
                "INSERT INTO compact_identity VALUES (?,?,?)",
                (digest, scope_digest, CURRENT_BODY_VERSION),
            )
            writer.commit()
            os.chmod(destination, 0o600)
        elif tuple(writer.execute("SELECT * FROM compact_identity").fetchone()) != (
            digest,
            scope_digest,
            CURRENT_BODY_VERSION,
        ):
            raise CompactCandidateError("interrupted candidate input/scope/transformation changed")
        for plan in release_plans or []:
            if "source_identity" in plan and plan["source_identity"] != freeze["source_identity"]:
                raise CompactCandidateError("release preview frozen identity changed")
        release_ids = _approved_release(reader, release_plans or [], source)
        tables = [
            row[0]
            for row in reader.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'message_lexical%' AND "
                "name NOT LIKE 'compact_%' ORDER BY name"
            )
        ]
        peak, peak_wal = tuple(writer.execute("SELECT workspace,wal FROM compact_peak").fetchone())
        for table in tables:
            columns = [row[1] for row in reader.execute(f'PRAGMA table_info("{table}")')]
            projection = ",".join(f'"{name}"' for name in columns)
            index = {name: i + 1 for i, name in enumerate(columns)}
            progress = writer.execute(
                "SELECT * FROM compact_progress WHERE table_name=?", (table,)
            ).fetchone()
            after, count, encoded = (
                (int(progress[1]), int(progress[2]), int(progress[3])) if progress else (0, 0, 0)
            )
            while True:
                rows = reader.execute(
                    f'SELECT rowid,{projection} FROM "{table}" WHERE rowid>? '
                    "ORDER BY rowid LIMIT ?",
                    (after, _COPY_CHUNK),
                ).fetchall()
                if not rows:
                    break
                _capacity(
                    destination.parent,
                    budget=workspace_budget_bytes,
                    min_free=min_free_bytes,
                    reserve=max(1024**2, sum(len(repr(tuple(row))) * 4 for row in rows)),
                )
                for row in rows:
                    values = list(row)
                    message_id = values[index["message_id"]] if "message_id" in index else None
                    if table in {"message_links", "message_link_projection", "body_release_jobs"}:
                        if _released_message(release_ids, message_id):
                            continue
                    if table in {"read_lease", "read_lease_message"}:
                        lease = values[index["lease_id"]]
                        if any(
                            _released_message(release_ids, r[0])
                            for r in reader.execute(
                                "SELECT message_id FROM read_lease_message WHERE lease_id=?",
                                (lease,),
                            )
                        ):
                            continue
                    if table == "message_observations":
                        payload = values[index["parsed_json"]]
                        if (
                            _released_message(release_ids, values[index["message_id"]])
                            and observation_payload_state(payload) == "full"
                        ):
                            raw = decode_observation_bytes(payload)
                            values[index["parsed_json"]] = encode_released_observation(
                                build_released_header(payload), original_bytes=len(raw)
                            )
                        elif isinstance(payload, str):
                            values[index["parsed_json"]] = encode_observation(
                                payload.encode("utf-8")
                            )
                            encoded += 1
                    elif table == "messages" and _released_message(
                        release_ids, values[index["message_id"]]
                    ):
                        for name, replacement in {
                            "text": None,
                            "search_text": None,
                            "structured_json": "{}",
                            "projection_epoch": None,
                            "body_available": 0,
                        }.items():
                            if name in index:
                                values[index[name]] = replacement
                        if "body_available" not in index:
                            # Schema9 input gets the new field explicitly for approved release.
                            writer.execute(
                                f'INSERT INTO "{table}"(rowid,{projection},body_available) '
                                f"VALUES ({','.join('?' for _ in values)},0)",
                                values,
                            )
                            continue
                    elif table == "messages":
                        # Explicit stopped-only normalization of the redundant
                        # current-body representations. The body column is the
                        # single source; an exact-duplicate ``search_text`` and an
                        # exact-duplicate ``structured_json.text`` are dropped. A
                        # conflicting legacy ``structured_json.text`` fails closed
                        # rather than being silently lost.
                        stored = {
                            name: values[index[name]]
                            for name in ("text", "search_text", "structured_json")
                            if name in index
                        }
                        try:
                            normalized = normalized_columns(stored)
                        except LegacyBodyConflict as error:
                            raise CompactCandidateError(
                                "legacy current-body representation conflict"
                            ) from error
                        for name, normalized_value in normalized.items():
                            if name in index:
                                values[index[name]] = normalized_value
                    writer.execute(
                        f'INSERT INTO "{table}"(rowid,{projection}) '
                        f"VALUES ({','.join('?' for _ in values)})",
                        values,
                    )
                after, count = int(rows[-1][0]), count + len(rows)
                writer.execute(
                    "INSERT OR REPLACE INTO compact_progress VALUES (?,?,?,?)",
                    (table, after, count, encoded),
                )
                writer.commit()
                wal = destination.with_name(destination.name + "-wal")
                peak_wal = max(peak_wal, wal.stat().st_size if wal.exists() else 0)
                peak = max(
                    peak,
                    _capacity(
                        destination.parent, budget=workspace_budget_bytes, min_free=min_free_bytes
                    ),
                )
                writer.execute("UPDATE compact_peak SET workspace=?,wal=?", (peak, peak_wal))
                writer.commit()
                writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                if fault:
                    fault("copy_batch")
        # Preserve deleted AUTOINCREMENT high water, not merely max surviving rowid.
        writer.execute("DELETE FROM sqlite_sequence")
        writer.executemany(
            "INSERT INTO sqlite_sequence(name,seq) VALUES (?,?)",
            list(reader.execute("SELECT name,seq FROM sqlite_sequence")),
        )
        from sightglass.residency.repository import ResidencyRepository

        from .schema import RESIDENCY_SCHEMA_STATEMENTS

        ownership = writer.execute(
            "SELECT after_rowid FROM compact_progress WHERE table_name='__ownership'"
        ).fetchone()
        if ownership is None:
            writer.execute("DELETE FROM message_body_residency")
            writer.execute("DELETE FROM residency_totals")
            for statement in RESIDENCY_SCHEMA_STATEMENTS:
                if statement.startswith("CREATE TRIGGER"):
                    writer.execute(statement)
            writer.execute("INSERT INTO compact_progress VALUES ('__ownership',0,0,0)")
            writer.execute("INSERT OR IGNORE INTO residency_state VALUES (1,0)")
            if release_ids:
                writer.execute("UPDATE residency_state SET revision=revision+1")
                writer.execute(
                    "UPDATE derived_index_state SET generation=generation+1 WHERE "
                    "index_kind='links'"
                )
            writer.commit()
        after = int(ownership[0]) if ownership else 0
        tracked = "message_body_residency" in tables
        while True:
            rows = writer.execute(
                "SELECT rowid,* FROM messages WHERE rowid>? ORDER BY rowid LIMIT ?",
                (after, _COPY_CHUNK),
            ).fetchall()
            if not rows:
                break
            for row in rows:
                old = (
                    reader.execute(
                        "SELECT * FROM message_body_residency WHERE message_id=?",
                        (row["message_id"],),
                    ).fetchone()
                    if tracked
                    else None
                )
                available = bool(row["body_available"])
                size = (
                    ResidencyRepository._body_bytes(writer, row["message_id"]) if available else 0
                )
                owner = (
                    old["owner"]
                    if old is not None and available
                    else ("protected" if available else "on_demand")
                )
                writer.execute(
                    "INSERT INTO message_body_residency VALUES (?,?,?,?,?,?)",
                    (
                        row["message_id"],
                        row["conversation_id"],
                        owner,
                        size,
                        old["admitted_at"] if old else "1970-01-01T00:00:00+00:00",
                        old["expires_at"] if old is not None and available else None,
                    ),
                )
            after = int(rows[-1][0])
            writer.execute(
                "UPDATE compact_progress SET after_rowid=? WHERE table_name='__ownership'", (after,)
            )
            writer.commit()
            peak_wal = max(
                peak_wal, destination.with_name(destination.name + "-wal").stat().st_size
            )
            peak = max(
                peak,
                _capacity(
                    destination.parent, budget=workspace_budget_bytes, min_free=min_free_bytes
                ),
            )
            writer.execute("UPDATE compact_peak SET workspace=?,wal=?", (peak, peak_wal))
            writer.commit()
            writer.execute("UPDATE compact_peak SET workspace=?,wal=?", (peak, peak_wal))
            writer.commit()
            writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            if fault:
                fault("ownership_batch")
        # Resume index publication by rowid, with canonical casefold/digest/recipe.
        progress = writer.execute(
            "SELECT * FROM compact_progress WHERE table_name='__lexical'"
        ).fetchone()
        after = int(progress[1]) if progress else 0
        while True:
            rows = writer.execute(
                "SELECT rowid AS __rowid,* FROM messages WHERE rowid>? AND "
                "body_available=1 AND current_observation_seq IS NOT NULL "
                "ORDER BY rowid LIMIT ?",
                (after, _COPY_CHUNK),
            ).fetchall()
            if not rows:
                break
            _capacity(
                destination.parent,
                budget=workspace_budget_bytes,
                min_free=min_free_bytes,
                reserve=4 * 1024**2,
            )
            for row in rows:
                publish_lexical(writer, row, historical=False)
                # Representation-only normalization changes the canonical
                # ``input_digest`` for a legacy row whose structured envelope
                # embedded the duplicate body text. Refresh the existing link
                # receipt's digest to the canonical value so normalization cannot
                # leave a stale receipt behind; the link rows themselves are
                # byte-identical and are never rewritten (their deterministic IDs
                # and timestamps stay part of exact recovery).
                writer.execute(
                    "UPDATE message_link_projection SET input_digest=? "
                    "WHERE message_id=? AND input_digest IS NOT ?",
                    (canonical_link_digest(row), row["message_id"], canonical_link_digest(row)),
                )
            after = int(rows[-1]["__rowid"])
            writer.execute(
                "INSERT OR REPLACE INTO compact_progress VALUES ('__lexical',?,0,0)", (after,)
            )
            writer.commit()
            peak_wal = max(
                peak_wal, destination.with_name(destination.name + "-wal").stat().st_size
            )
            peak = max(
                peak,
                _capacity(
                    destination.parent, budget=workspace_budget_bytes, min_free=min_free_bytes
                ),
            )
            writer.execute("UPDATE compact_peak SET workspace=?,wal=?", (peak, peak_wal))
            writer.commit()
            writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            if fault:
                fault("lexical_batch")
        writer.execute(
            "UPDATE derived_index_state SET recipe=?,generation=generation+1,state='ready' "
            "WHERE index_kind='lexical'",
            (LEXICAL_CANDIDATE_RECIPE,),
        )
        writer.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        writer.commit()
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        if identity != file_revision(source):
            raise CompactCandidateError("frozen input changed during construction")
        verification = verify_candidate(source, destination, release_plans=release_plans)
        # Install the canonical triggers only after copying the exact accounting ledger.
        from .schema import RESIDENCY_SCHEMA_STATEMENTS

        for statement in RESIDENCY_SCHEMA_STATEMENTS:
            if statement.startswith("CREATE TRIGGER"):
                writer.execute(statement)
        copied = dict(
            writer.execute(
                "SELECT table_name,copied FROM compact_progress WHERE substr(table_name,1,2)!='__'"
            )
        )
        encoded = int(
            writer.execute("SELECT COALESCE(SUM(encoded),0) FROM compact_progress").fetchone()[0]
        )
        writer.commit()
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    with destination.open("rb") as handle:
        os.fsync(handle.fileno())
    receipt = {
        "schema": CANDIDATE_MANIFEST_SCHEMA,
        "frozen_digest": digest,
        "source_identity": identity,
        "release_scope_digest": scope_digest,
        "current_body_version": CURRENT_BODY_VERSION,
        "release_plans": release_plans or [],
        "rebuilt_schema_version": SCHEMA_VERSION,
        "copied_rows": copied,
        "encoded": {"encoded_text_observations": encoded},
        "lexical": {"recipe": LEXICAL_CANDIDATE_RECIPE},
        "verified": verification,
        "peak_workspace_bytes": peak,
        "peak_candidate_wal_bytes": peak_wal,
        "capability": capability,
    }
    _write_json_atomic(manifest_path, receipt)
    _retire_progress(destination)
    _fsync_directory(destination.parent)
    return receipt


def _retire_progress(path: Path) -> None:
    with closing(sqlite3.connect(path)) as connection:
        for table in ("compact_progress", "compact_identity", "compact_peak"):
            connection.execute(f"DROP TABLE IF EXISTS {table}")
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _verify_durable_rows(
    reader: sqlite3.Connection,
    candidate: sqlite3.Connection,
    released: Collection[str],
    state_relocation: tuple[Path, Path] | None = None,
) -> None:
    """Compare every durable source column, allowing only the declared cache transformations."""
    from itertools import zip_longest

    from .links import input_digest as canonical_link_digest

    source_message_columns = {
        row[1] for row in reader.execute("PRAGMA table_info(messages)")
    }

    excluded = {
        "message_body_residency",
        "residency_totals",
        "residency_state",
        "derived_index_state",
        "message_lexical_projection",
    }
    for (table,) in reader.execute(
        "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name"
    ):
        if table.startswith(("sqlite_", "message_lexical", "compact_")) or table in excluded:
            continue
        columns = [row[1] for row in reader.execute(f'PRAGMA table_info("{table}")')]
        if table == "message_observations":
            columns.remove("parsed_json")  # checked losslessly by the canonical codec below
        projection = ",".join(f'"{name}"' for name in columns)

        # Cursors stay streaming: one row from each DB at a time.
        def expected_rows():
            for row in reader.execute(f'SELECT rowid,{projection} FROM "{table}" ORDER BY rowid'):
                values = list(row)
                if state_relocation is not None and table in {
                    "reader_deliveries",
                    "resource_objects",
                }:
                    from sightglass.runtime.paired_state import relocated_path

                    field = "payload_ref" if table == "reader_deliveries" else "local_path_internal"
                    index = columns.index(field) + 1
                    values[index] = relocated_path(values[index], *state_relocation)
                identity = (
                    values[columns.index("message_id") + 1] if "message_id" in columns else None
                )
                is_released = _released_message(released, identity)
                if is_released:
                    if table in {
                        "message_links",
                        "message_link_projection",
                        "body_release_jobs",
                        "read_lease_message",
                    }:
                        continue
                    if table == "messages":
                        for name, replacement in {
                            "text": None,
                            "search_text": None,
                            "structured_json": "{}",
                            "projection_epoch": None,
                            "body_available": 0,
                        }.items():
                            if name in columns:
                                values[columns.index(name) + 1] = replacement
                elif table == "messages":
                    # The deliberate candidate projection normalizes the redundant
                    # current-body representations for a non-released row; the
                    # verifier applies the identical deterministic projection so
                    # extraction is exact.
                    stored = {
                        name: values[columns.index(name) + 1]
                        for name in ("text", "search_text", "structured_json")
                        if name in columns
                    }
                    for name, normalized_value in normalized_columns(stored).items():
                        if name in columns:
                            values[columns.index(name) + 1] = normalized_value
                elif table == "message_link_projection" and "input_digest" in columns:
                    # The candidate refreshes a link receipt to the canonical
                    # (representation-invariant) digest only for a resident row it
                    # published (``__lexical`` pass); mirror that exact condition.
                    available = "body_available" in source_message_columns
                    extra = ",body_available,current_observation_seq" if available else ""
                    message = reader.execute(
                        f"SELECT text,structured_json,current_state{extra} "
                        "FROM messages WHERE message_id=?",
                        (identity,),
                    ).fetchone()
                    if (
                        message is not None
                        and (not available or message["body_available"] == 1)
                        and (
                            not available
                            or message["current_observation_seq"] is not None
                        )
                    ):
                        values[columns.index("input_digest") + 1] = canonical_link_digest(message)
                if table in {"read_lease", "read_lease_message"}:
                    lease = values[columns.index("lease_id") + 1]
                    if any(
                        _released_message(released, r[0])
                        for r in reader.execute(
                            "SELECT message_id FROM read_lease_message WHERE lease_id=?", (lease,)
                        )
                    ):
                        continue
                yield tuple(values)

        actual = candidate.execute(f'SELECT rowid,{projection} FROM "{table}" ORDER BY rowid')
        for before, after in zip_longest(expected_rows(), actual):
            if before is None or after is None or before != tuple(after):
                raise CompactCandidateError(f"durable rows changed in {table}")


def verify_candidate(
    source_path: Path,
    candidate_path: Path,
    *,
    expected_identity: dict[str, Any] | None = None,
    release_plans: list[dict[str, Any]] | None = None,
    state_relocation: tuple[Path, Path] | None = None,
) -> dict[str, Any]:
    from .observation_codec import (
        build_released_header,
        decode_released_header,
        observation_payload_state,
    )
    from .schema import SCHEMA_VERSION

    identity = file_revision(source_path)
    if expected_identity is not None and identity != expected_identity:
        raise CompactCandidateError("verification input changed")
    with (
        closing(_open_read_only(source_path)) as reader,
        closing(_open_read_only(candidate_path)) as candidate,
    ):
        reader.execute("BEGIN")
        candidate.execute("BEGIN")
        before, after = build_rowid_manifest(reader), build_rowid_manifest(candidate)
        if before != after:
            raise CompactCandidateError("rowid-to-identity/sequence mapping changed")
        if int(candidate.execute("PRAGMA user_version").fetchone()[0]) != SCHEMA_VERSION:
            raise CompactCandidateError("candidate/runtime schema mismatch")
        if candidate.execute("PRAGMA foreign_key_check").fetchone():
            raise CompactCandidateError("candidate foreign key violation")
        if candidate.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise CompactCandidateError("candidate SQLite consistency failure")
        release_ids = _approved_release(reader, release_plans or [], source_path)
        _verify_durable_rows(reader, candidate, release_ids, state_relocation)
        for row in reader.execute(
            "SELECT observation_seq,message_id,payload_digest,parsed_json "
            "FROM message_observations ORDER BY observation_seq"
        ):
            other = candidate.execute(
                "SELECT payload_digest,parsed_json FROM message_observations "
                "WHERE observation_seq=?",
                (row[0],),
            ).fetchone()
            if other is None or other[0] != row[2]:
                raise CompactCandidateError("observation identity/digest changed")
            old_state = observation_payload_state(row[3])
            if _released_message(release_ids, row[1]) and old_state == "full":
                header = decode_released_header(other[1])
                if (
                    header is None
                    or header["retained"] != build_released_header(row[3])
                    or header["original_bytes"] != len(decode_observation_bytes(row[3]))
                ):
                    raise CompactCandidateError("released identity evidence changed")
            elif old_state == "released":
                if bytes(row[3]) != bytes(other[1]):
                    raise CompactCandidateError("retained identity header changed")
            elif decode_observation_bytes(row[3]) != decode_observation_bytes(other[1]):
                raise CompactCandidateError("original observation bytes changed")
    return {
        "schema": "sightglass.compact-candidate-verify.v1",
        "verified": True,
        "rowid_manifest": after,
        "source_identity": identity,
    }

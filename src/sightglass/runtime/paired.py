"""Stopped-only selection of an immutable runtime/config/database pair.

A fixed bootstrap runs this stdlib-only module. One atomic selector chooses both
runtime and database; it never selects them in two independent namespace moves.
Old pairs and their databases remain usable for explicit rollback.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import uuid
from contextlib import closing
from pathlib import Path
from typing import Any

PAIR_SCHEMA = "sightglass.runtime-db-pair.v2"
_PAIR_ID = re.compile(r"[a-f0-9]{32}\Z")


def _private_root(root: Path) -> Path:
    root = root.absolute()
    if not root.exists():
        root.mkdir(mode=0o700, parents=True)
    if root.is_symlink() or root.stat().st_mode & 0o077:
        raise RuntimeError("pair root must be an owner-private directory")
    # Config and private payload stores use canonical paths (macOS /var is an
    # alias of /private/var). Store the same spelling in relocated references.
    return root.resolve()


def _runtime_probe(python: Path) -> dict[str, Any]:
    probe = """import json,importlib.metadata as m,hashlib,sys,sqlite3
from sightglass.model.schema import SCHEMA_VERSION
package=m.distribution("sightglass")
digest=hashlib.sha256()
for item in sorted(package.files or [],key=str):
 if str(item).endswith(".py"):
  digest.update(str(item).encode());digest.update(package.locate_file(item).read_bytes())
capable=True
if SCHEMA_VERSION>=10:
 with sqlite3.connect(":memory:") as c:
  try:
   c.execute("CREATE VIRTUAL TABLE probe USING fts5(text,tokenize='trigram',"
             "detail=none,content='',contentless_delete=1)")
  except sqlite3.OperationalError:capable=False
print(json.dumps({"schema":SCHEMA_VERSION,"version":package.version,"source_digest":digest.hexdigest(),
"python":sys.version,"sqlite":sqlite3.sqlite_version,"backend_capable":capable,
"dependencies":sorted((d.metadata["Name"],d.version) for d in m.distributions()),
"direct_url":json.loads(package.read_text("direct_url.json") or "{}")}))"""
    result = subprocess.run(
        [str(python), "-c", probe], check=True, capture_output=True, text=True, timeout=30
    )
    return json.loads(result.stdout)


def _database_schema(path: Path) -> int:
    with closing(sqlite3.connect(path.absolute().as_uri() + "?mode=ro", uri=True)) as connection:
        connection.execute("PRAGMA query_only=ON")
        return int(connection.execute("PRAGMA user_version").fetchone()[0])


def selected_pair(root: Path) -> dict[str, Any] | None:
    """Read-only: even a cut after selector publication yields one complete pair."""
    from sightglass.model.backups import _private_regular_file

    selector = root / "current.json"
    if not selector.exists():
        return None
    _private_regular_file(selector)
    selection = json.loads(selector.read_text())
    pair_id = selection["pair_id"]
    if not isinstance(pair_id, str) or not _PAIR_ID.fullmatch(pair_id):
        raise RuntimeError("invalid pair selection")
    manifest_path = root / pair_id / "pair.json"
    _private_regular_file(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    if manifest["schema"] != PAIR_SCHEMA or manifest["pair_id"] != pair_id:
        raise RuntimeError("pair manifest identity changed")
    _private_regular_file(Path(manifest["config_path"]))
    _private_regular_file(Path(manifest["database_path"]))
    config = json.loads(Path(manifest["config_path"]).read_text())
    if config["paths"]["window_db"] != manifest["database_path"]:
        raise RuntimeError("pair configuration/database mismatch")
    if (manifest["candidate"] or manifest.get("copied_current", False)) and (
        config["paths"]["data_dir"] != str(manifest_path.parent.resolve())
    ):
        raise RuntimeError("pair private namespace changed")
    if _database_schema(Path(manifest["database_path"])) != manifest["schema_version"]:
        raise RuntimeError("pair database schema changed")
    if _runtime_probe(Path(manifest["runtime_python"])) != manifest["runtime_probe"]:
        raise RuntimeError("selected runtime provenance changed")
    return manifest


def prepare_pair(
    root: Path,
    *,
    config_path: Path,
    runtime_python: Path,
    candidate_path: Path | None = None,
    frozen_path: Path | None = None,
    copy_current: bool = False,
    workspace_budget_bytes: int,
    min_free_bytes: int,
    fault: Any = None,
) -> dict[str, Any]:
    """Register an existing pair, or stage a candidate on the selected target volume.

    Registration preserves the existing DB. Candidate/current preparation streams a copy
    into a same-filesystem namespace, verifies/fsyncs it, then publishes its
    immutable pair manifest. No current selection changes here.
    """
    from sightglass.model.backups import (
        _fsync_directory,
        _hash_file,
        _private_regular_file,
        _write_json_atomic,
    )
    from sightglass.model.compact_candidate import (
        _capacity,
        file_revision,
        require_encrypted_volume,
        verify_candidate,
    )
    from sightglass.runtime.config import SightglassConfig

    if copy_current and (candidate_path is not None or frozen_path is not None):
        raise RuntimeError("current-pair relocation cannot also select a candidate")
    staged = bool(candidate_path) or copy_current
    root = _private_root(root)
    _private_regular_file(config_path)
    value = json.loads(config_path.read_text())
    config = SightglassConfig.from_dict(value)
    runtime = runtime_python.absolute()
    probe = _runtime_probe(runtime)
    if not probe["backend_capable"]:
        raise RuntimeError("runtime lacks the selected database backend")
    if config.source_kind != "synthetic":
        if probe["direct_url"].get("dir_info", {}).get("editable"):
            raise RuntimeError("production pair requires a non-editable wheel runtime")
        require_encrypted_volume(root)
        if frozen_path:
            require_encrypted_volume(frozen_path.parent)
    target = candidate_path or config.window_db_path
    source_identity = file_revision(config.window_db_path) if staged else None
    schema = _database_schema(target)
    if schema != probe["schema"]:
        raise RuntimeError("candidate/runtime schema mismatch")
    if candidate_path:
        if frozen_path is None:
            raise RuntimeError("candidate preparation requires its exact frozen input")
        receipt_path = candidate_path.with_suffix(candidate_path.suffix + ".json")
        _private_regular_file(receipt_path)
        receipt = json.loads(receipt_path.read_text())
        verify_candidate(frozen_path, candidate_path, release_plans=receipt.get("release_plans"))
        freeze = json.loads((frozen_path.parent / "freeze.json").read_text())
        if file_revision(config.window_db_path) != freeze["source_identity"]:
            raise RuntimeError("active database changed since the frozen input")
        from sightglass.model.backups import verify_compressed_snapshot

        verify_compressed_snapshot(
            frozen_path,
            Path(freeze["recovery_artifact"]),
            expected_raw_sha256=receipt["frozen_digest"],
        )
        if _hash_file(frozen_path)[0] != receipt["frozen_digest"]:
            raise RuntimeError("candidate frozen identity mismatch")
    if staged:
        _private_regular_file(target)
        wal = target.with_name(target.name + "-wal")
        if wal.exists() and wal.stat().st_size:
            raise RuntimeError("staging input has an uncheckpointed WAL")
    _capacity(
        root,
        budget=workspace_budget_bytes,
        min_free=min_free_bytes,
        reserve=target.stat().st_size if staged else 1024**2,
        additional_roots=(frozen_path.parent,) if frozen_path else (),
    )
    pair_id = uuid.uuid4().hex
    directory = root / pair_id
    directory.mkdir(mode=0o700)
    state_files: list[dict[str, Any]] = []
    if staged:
        destination = directory / "window.db"
        with target.open("rb") as source, destination.open("xb") as writer:
            os.fchmod(writer.fileno(), 0o600)
            shutil.copyfileobj(source, writer, 1024**2)
            writer.flush()
            os.fsync(writer.fileno())
        if fault:
            fault("stage_copied")
        if _hash_file(destination) != _hash_file(target):
            raise RuntimeError("staged candidate differs from verified input")
        from sightglass.runtime.paired_state import clone_state

        def capacity(reserve: int) -> int:
            return _capacity(
                root,
                budget=workspace_budget_bytes,
                min_free=min_free_bytes,
                reserve=reserve,
                additional_roots=(frozen_path.parent,) if frozen_path else (),
            )

        state_files = clone_state(
            destination,
            config.window_db_path.parent,
            config.data_dir,
            directory,
            capacity=capacity,
            default_voice_helper=not config.voice_helper_path.strip(),
        )
        if candidate_path:
            assert frozen_path is not None
            verify_candidate(
                frozen_path,
                destination,
                release_plans=receipt.get("release_plans"),
                state_relocation=(config.window_db_path.parent, directory),
            )
        # A current-schema relocation began as an exact byte copy. clone_state
        # changes only verified private path references; no conversion is needed.
        if copy_current:
            with closing(sqlite3.connect(destination.as_uri() + "?mode=ro", uri=True)) as check:
                check.execute("PRAGMA query_only=ON")
                if check.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise RuntimeError("relocated database consistency failure")
                if check.execute("PRAGMA foreign_key_check").fetchone():
                    raise RuntimeError("relocated database foreign key violation")
        if file_revision(config.window_db_path) != source_identity:
            raise RuntimeError("active database changed while staging")
        with destination.open("rb") as handle:
            os.fsync(handle.fileno())
        value["paths"]["data_dir"] = str(directory)
        if fault:
            fault("state_copied")
        database = destination
    else:
        database = config.window_db_path
    value["paths"]["window_db"] = str(database.absolute())
    value["paused"] = True
    paired_config = directory / "config.json"
    _write_json_atomic(paired_config, value)
    manifest = {
        "schema": PAIR_SCHEMA,
        "pair_id": pair_id,
        "runtime_python": str(runtime),
        "runtime_probe": probe,
        "database_path": str(database.absolute()),
        "schema_version": schema,
        "config_path": str(paired_config),
        "candidate": bool(candidate_path),
        "copied_current": copy_current,
        "recovery_preserved": bool(frozen_path),
        "frozen_path": str(frozen_path) if frozen_path else None,
        "frozen_digest": receipt["frozen_digest"] if candidate_path else None,
        "initial_database_digest": _hash_file(database)[0] if staged else None,
        "state_files": state_files,
        "state_namespace_independent": staged,
        "source_path": str(config.window_db_path) if staged else None,
        "source_identity": source_identity,
    }
    _write_json_atomic(directory / "pair.json", manifest)
    _fsync_directory(directory)
    _fsync_directory(root)
    if fault:
        fault("pair_prepared")
    return manifest


def activate_pair(
    root: Path, pair_id: str, *, expected_current: str | None, fault: Any = None
) -> dict[str, Any]:
    """Caller holds the stopped process lock; one atomic selection is the commit."""
    from sightglass.model.backups import _write_json_atomic

    root = _private_root(root)
    if not _PAIR_ID.fullmatch(pair_id):
        raise RuntimeError("invalid pair identity")
    previous = selected_pair(root)
    actual = previous["pair_id"] if previous else None
    if actual != expected_current:
        if actual == pair_id:
            return {"selected": pair_id, "already_selected": True}
        raise RuntimeError("pair selection changed since preview")
    path = root / pair_id / "pair.json"
    from sightglass.model.backups import _private_regular_file

    _private_regular_file(path)
    target = json.loads(path.read_text())
    if (
        target["pair_id"] != pair_id
        or _database_schema(Path(target["database_path"])) != target["schema_version"]
    ):
        raise RuntimeError("prepared pair changed")
    if _runtime_probe(Path(target["runtime_python"])) != target["runtime_probe"]:
        raise RuntimeError("prepared runtime changed")
    _private_regular_file(Path(target["config_path"]))
    config = json.loads(Path(target["config_path"]).read_text())
    if config["paths"]["window_db"] != target["database_path"]:
        raise RuntimeError("prepared config/database changed")
    staged = target["candidate"] or target.get("copied_current", False)
    if staged and config["paths"]["data_dir"] != str(path.parent):
        raise RuntimeError("prepared private namespace changed")
    if staged and not (root / pair_id / "activated.json").exists():
        from sightglass.model.backups import _hash_file, verify_compressed_snapshot
        from sightglass.model.compact_candidate import file_revision
        from sightglass.runtime.paired_state import verify_initial_state

        if (any(item.get("kind") == "default_voice_helper" for item in target["state_files"])
                and str(config.get("voice", {}).get("helper_path", "") or "").strip()):
            raise RuntimeError("prepared voice helper configuration changed")
        verify_initial_state(target["state_files"])
        if file_revision(Path(target["source_path"])) != target["source_identity"]:
            raise RuntimeError("active database changed after pair preparation")
        if _hash_file(Path(target["database_path"]))[0] != target["initial_database_digest"]:
            raise RuntimeError("staged candidate changed before activation")
    if target["candidate"] and not (root / pair_id / "activated.json").exists():
        frozen = Path(target["frozen_path"])
        freeze = json.loads((frozen.parent / "freeze.json").read_text())
        if _hash_file(frozen)[0] != target["frozen_digest"]:
            raise RuntimeError("prepared recovery input changed")
        verify_compressed_snapshot(
            frozen, Path(freeze["recovery_artifact"]), expected_raw_sha256=target["frozen_digest"]
        )
    _write_json_atomic(
        root / "selection-journal.json",
        {"schema": PAIR_SCHEMA, "previous": actual, "next": pair_id, "committed": False},
    )
    if fault:
        fault("selection_prepared")
    _write_json_atomic(root / "current.json", {"schema": PAIR_SCHEMA, "pair_id": pair_id})
    if fault:
        fault("selection_published")
    _write_json_atomic(
        root / "selection-journal.json",
        {"schema": PAIR_SCHEMA, "previous": actual, "next": pair_id, "committed": True},
    )
    _write_json_atomic(root / pair_id / "activated.json", {"pair_id": pair_id})
    return {
        "schema": PAIR_SCHEMA,
        "selected": pair_id,
        "previous": actual,
        "runtime_db_paired": True,
        "old_pair_preserved": True,
    }


def recover_selection(root: Path) -> dict[str, Any]:
    """A cut selects either complete old or complete new pair; reconcile the journal."""
    from sightglass.model.backups import _private_regular_file, _write_json_atomic

    path = root / "selection-journal.json"
    pair = selected_pair(root)
    if not path.exists():
        return {"selected": pair["pair_id"] if pair else None, "recovered": False}
    _private_regular_file(path)
    intent = json.loads(path.read_text())
    selected = pair["pair_id"] if pair else None
    if selected not in {intent["previous"], intent["next"]}:
        raise RuntimeError("pair selection journal mismatch")
    intent["committed"] = selected == intent["next"]
    if selected is not None:
        _write_json_atomic(root / selected / "activated.json", {"pair_id": selected})
    _write_json_atomic(path, intent)
    return {"selected": selected, "recovered": True, "committed": intent["committed"]}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="sightglass-pair")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("kind", choices=("ctl", "daemon", "mcp"))
    args, remainder = parser.parse_known_args(argv)
    pair = selected_pair(args.root)
    if pair is None:
        raise RuntimeError("no runtime/database pair is selected")
    module = {
        "ctl": "sightglass.cli",
        "daemon": "sightglass.runtime.daemon",
        "mcp": "sightglass.mcp.server",
    }[args.kind]
    if any(arg == "--config" or arg.startswith("--config=") for arg in remainder):
        raise RuntimeError("paired launcher does not accept an independent config override")
    environment = dict(os.environ, SIGHTGLASS_CONFIG=pair["config_path"])
    os.execve(
        pair["runtime_python"], [pair["runtime_python"], "-m", module, *remainder], environment
    )


if __name__ == "__main__":
    main()

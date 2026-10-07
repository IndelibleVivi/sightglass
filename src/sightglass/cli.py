from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import sys
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sightglass.contracts.common import validate_timezone
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.model.db import WindowDB
from sightglass.runtime.config import ConfigStore, SightglassConfig
from sightglass.runtime.control import process_is_running
from sightglass.runtime.ipc import IPCClient
from sightglass.runtime.secrets import (
    OPERATOR_SECRET_ACCOUNT,
    READER_SECRET_ACCOUNT,
    KeychainSecretStore,
    new_token,
    semantic_secret_account,
    token_hash,
)
from sightglass.runtime.source_worker import SOURCE_WORKER_STOP_TIMEOUT_SECONDS
from sightglass.semantic.settings import SemanticSettings
from sightglass.source.direct_wechat import SyntheticSourceProvider
from sightglass.source.identity import opaque_id
from sightglass.source.registry import provider_descriptors
from sightglass.source.synthetic import create_synthetic_source

_LITERAL_KEY_ARGUMENT = re.compile(r"--key(?:[=:].*)?\Z")

# Startup may reconcile private state before binding its socket. Schema/backend
# conversion is separately explicit and stopped-only; this remains a bounded wait.
DAEMON_START_TIMEOUT_SECONDS = 120.0
DAEMON_START_TIMEOUT_ENV = "SIGHTGLASS_DAEMON_START_TIMEOUT_SECONDS"


def _daemon_start_timeout() -> float:
    raw = os.environ.get(DAEMON_START_TIMEOUT_ENV, "").strip()
    if raw:
        try:
            configured = float(raw)
        except ValueError:
            configured = 0.0
        if configured > 0:
            return configured
    return DAEMON_START_TIMEOUT_SECONDS


class _ExactArgumentParser(argparse.ArgumentParser):
    """Argument parser that never accepts an abbreviated long option.

    Option abbreviation would let ``--key`` silently bind to ``--key-file``, which
    must never become an alternate way to hand key material in on the command line.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("allow_abbrev", False)
        super().__init__(*args, **kwargs)


def _subcommands(parser: argparse.ArgumentParser, **kwargs: Any) -> Any:
    """Subcommands that never accept an abbreviated long option."""
    return parser.add_subparsers(parser_class=_ExactArgumentParser, **kwargs)


def _parser() -> argparse.ArgumentParser:
    parser = _ExactArgumentParser(prog="sightglassctl")
    parser.add_argument("--config", help="private Sightglass config path")
    commands = _subcommands(parser, dest="command", required=True)

    create = commands.add_parser("synthetic-create")
    create.add_argument("root")

    initialize = commands.add_parser("init")
    initialize.add_argument("--source-root", required=True)
    initialize.add_argument("--data-dir")

    source = commands.add_parser("source")
    source_commands = _subcommands(source, dest="source_command", required=True)
    source_commands.add_parser("providers")
    discover = source_commands.add_parser("discover")
    discover.add_argument("--kind", choices=("macos-wechat",), default="macos-wechat")
    native = source_commands.add_parser("init-macos-wechat")
    native_credentials = native.add_mutually_exclusive_group(required=True)
    native_credentials.add_argument("--key-file")
    native_credentials.add_argument("--reuse-installed-keys", action="store_true")
    native.add_argument("--candidate")

    image_key = source_commands.add_parser("image-key")
    image_key_commands = _subcommands(image_key, dest="image_key_command", required=True)
    image_key_status = image_key_commands.add_parser("status")
    image_key_status.add_argument("--settings")
    image_key_import = image_key_commands.add_parser("import")
    image_key_import.add_argument("--settings")
    image_key_import.add_argument("--xor-key-file")
    image_key_source = image_key_import.add_mutually_exclusive_group(required=True)
    image_key_source.add_argument("--key-file")
    image_key_source.add_argument("--stdin", action="store_true")
    image_key_remove = image_key_commands.add_parser("remove")
    image_key_remove.add_argument("--settings")

    commands.add_parser("status")
    commands.add_parser("doctor")
    commands.add_parser("pause")
    commands.add_parser("resume")

    retrieval = commands.add_parser("retrieval")
    retrieval_commands = _subcommands(retrieval, dest="retrieval_command", required=True)
    retrieval_commands.add_parser("status")
    retrieval_commands.add_parser("explain")
    rebuild = retrieval_commands.add_parser("rebuild")
    rebuild.add_argument("--kind", choices=("links", "lexical", "semantic"), required=True)
    semantic_config = retrieval_commands.add_parser("configure-semantic")
    semantic_config.add_argument("--settings-file", required=True)
    semantic_config.add_argument("--authorize-external-data", action="store_true")
    retrieval_commands.add_parser("disable-semantic")
    semantic_token = retrieval_commands.add_parser("import-semantic-token")
    token_input = semantic_token.add_mutually_exclusive_group(required=True)
    token_input.add_argument("--token-file")
    token_input.add_argument("--stdin", action="store_true")

    storage = commands.add_parser("storage")
    storage_commands = _subcommands(storage, dest="storage_command", required=True)
    storage_commands.add_parser("status")
    storage_explain = storage_commands.add_parser("explain")
    storage_explain.add_argument("--offset", type=int, default=0)
    storage_explain.add_argument("--limit", type=int, default=500)
    storage_explain.add_argument("--sample-size", type=int, default=128)
    storage_explain.add_argument("--deep", action="store_true")
    storage_explain.add_argument(
        "--phase", choices=("all", "tables", "layout", "observations", "sample"), default="all"
    )
    storage_explain.add_argument("--after-object")
    storage_explain.add_argument("--deadline-seconds", type=float, default=10.0)
    storage_configure = storage_commands.add_parser("configure")
    for option in (
        "soft-limit-bytes",
        "hard-limit-bytes",
        "min-free-bytes",
        "maintenance-reserve-bytes",
    ):
        storage_configure.add_argument(f"--{option}", type=int)
    compact = storage_commands.add_parser("compact")
    compact_commands = _subcommands(compact, dest="compact_command", required=True)
    compact_preview = compact_commands.add_parser("preview")
    compact_scope = compact_preview.add_mutually_exclusive_group()
    compact_scope.add_argument("--conversation-id")
    compact_scope.add_argument("--all-stock", action="store_true")
    compact_preview.add_argument("--cursor")
    compact_preview.add_argument("--limit", type=int, default=200)
    compact_build = compact_commands.add_parser("build")
    compact_build.add_argument("--workspace", type=Path, required=True)
    compact_build.add_argument("--workspace-budget-bytes", type=int, required=True)
    compact_build.add_argument("--release-plans-file", type=Path)
    compact_verify = compact_commands.add_parser("verify")
    compact_verify.add_argument("--workspace", type=Path, required=True)
    pair_prepare = compact_commands.add_parser("prepare-pair")
    pair_prepare.add_argument("--pair-root", type=Path, required=True)
    pair_prepare.add_argument("--runtime-python", type=Path, required=True)
    pair_input = pair_prepare.add_mutually_exclusive_group()
    pair_input.add_argument("--workspace", type=Path)
    pair_input.add_argument("--copy-current", action="store_true")
    pair_prepare.add_argument("--workspace-budget-bytes", type=int, required=True)
    pair_activate = compact_commands.add_parser("activate-pair")
    pair_activate.add_argument("--pair-root", type=Path, required=True)
    pair_activate.add_argument("--pair-id", required=True)
    pair_activate.add_argument("--expected-current")
    pair_recover = compact_commands.add_parser("recover-pair")
    pair_recover.add_argument("--pair-root", type=Path, required=True)
    storage_backup = storage_commands.add_parser("backup")
    storage_backup_commands = _subcommands(storage_backup, dest="backup_command", required=True)
    storage_backup_commands.add_parser("plan")
    storage_backup_commands.add_parser("create")
    storage_backup_retire = storage_backup_commands.add_parser("retire")
    storage_backup_retire.add_argument("--ack", required=True)
    storage_backup_retire.add_argument("--artifact")
    storage_backup_restore = storage_backup_commands.add_parser("restore")
    storage_backup_restore.add_argument("--artifact", required=True)
    storage_backup_restore.add_argument("--ack", required=True)

    maintenance = commands.add_parser("maintenance")
    maintenance_commands = _subcommands(maintenance, dest="maintenance_command", required=True)
    observations = maintenance_commands.add_parser("observations")
    observation_commands = _subcommands(observations, dest="observation_command", required=True)
    inspect = observation_commands.add_parser("inspect")
    inspect.add_argument("--after-message-id")
    inspect.add_argument("--limit", type=int, default=100)
    repair = observation_commands.add_parser("repair")
    repair.add_argument("--limit", type=int, default=100)

    timezone = commands.add_parser("timezone")
    timezone_commands = _subcommands(timezone, dest="timezone_command", required=True)
    timezone_commands.add_parser("status")
    timezone_set = timezone_commands.add_parser("set")
    timezone_set.add_argument("iana_timezone")

    daemon = commands.add_parser("daemon")
    daemon_commands = _subcommands(daemon, dest="daemon_command", required=True)
    daemon_commands.add_parser("start")
    daemon_commands.add_parser("stop")
    daemon_commands.add_parser("restart")

    conversation = commands.add_parser("conversation")
    conversation_commands = _subcommands(conversation, dest="conversation_command", required=True)
    for name in ("allow", "deny"):
        subcommand = conversation_commands.add_parser(name)
        subcommand.add_argument("conversation_id")

    scope = commands.add_parser("scope")
    scope_commands = _subcommands(scope, dest="scope_command", required=True)
    scope_commands.add_parser("status")
    scope_set = scope_commands.add_parser("set")
    scope_set.add_argument("mode", choices=("selected", "account"))
    scope_set.add_argument("--confirm-account-scope", action="store_true")
    for name in ("allow", "deny", "clear-deny"):
        subcommand = scope_commands.add_parser(name)
        subcommand.add_argument("conversation_id")
    scope_catalog = scope_commands.add_parser("catalog")
    scope_catalog.add_argument("--cursor")
    scope_catalog.add_argument("--limit", type=int, default=100)

    backfill = commands.add_parser("backfill")
    backfill_commands = _subcommands(backfill, dest="backfill_command", required=True)
    backfill_commands.add_parser("status")
    backfill_conversation = backfill_commands.add_parser("conversation")
    backfill_conversation.add_argument("conversation_id")
    backfill_conversation.add_argument("--after")
    backfill_conversation.add_argument("--before")
    backfill_conversation.add_argument("--max-messages", type=int, default=10_000)
    backfill_account = backfill_commands.add_parser("account")
    backfill_account.add_argument("--from", dest="after")
    backfill_account.add_argument("--before")
    backfill_account.add_argument("--max-messages", type=int, default=10_000)
    backfill_commands.add_parser("pause")
    backfill_commands.add_parser("resume")

    cache = commands.add_parser("cache")
    cache_commands = _subcommands(cache, dest="cache_command", required=True)
    cache_commands.add_parser("status")
    cache_commands.add_parser("preview")
    cleanup = cache_commands.add_parser("cleanup")
    cleanup.add_argument("--apply", action="store_true")

    residency = commands.add_parser("residency")
    residency_commands = _subcommands(residency, dest="residency_command", required=True)
    residency_commands.add_parser("status")
    residency_list = residency_commands.add_parser("list")
    residency_list.add_argument("--mode", choices=("keep", "recent", "on_demand"))
    residency_list.add_argument("--sort", choices=("bytes", "conversation"), default="bytes")
    residency_list.add_argument("--limit", type=int, default=200)
    residency_list.add_argument("--cursor")
    residency_set = residency_commands.add_parser("set")
    residency_set.add_argument("mode", choices=("keep", "recent", "on_demand"))
    residency_set.add_argument("conversation_ids", nargs="+")
    residency_set.add_argument("--keep-backfill", action="store_true")
    residency_set.add_argument("--recent-window-days", type=int)
    residency_set.add_argument("--recent-max-bytes", type=int)
    residency_set.add_argument("--reason")
    residency_configure = residency_commands.add_parser("configure")
    residency_configure.add_argument("--default-mode", choices=("keep", "recent", "on_demand"))
    residency_configure.add_argument("--recent-window-days", type=int)
    residency_configure.add_argument("--recent-max-bytes", type=int)
    residency_configure.add_argument("--global-max-bytes", type=int)
    residency_configure.add_argument("--lease-ttl-seconds", type=int)
    residency_configure.add_argument("--lease-max-bytes", type=int)
    residency_preview = residency_commands.add_parser("preview")
    residency_preview.add_argument("conversation_id")
    residency_preview.add_argument("--cursor")
    residency_preview.add_argument("--limit", type=int, default=200)
    residency_release = residency_commands.add_parser("release")
    residency_release.add_argument("conversation_id")
    release_apply = residency_release.add_mutually_exclusive_group()
    release_apply.add_argument("--apply", dest="apply", action="store_true")
    residency_release.add_argument("--plan-file", type=Path)
    residency_release.set_defaults(apply=False)
    residency_rebaseline = residency_commands.add_parser("rebaseline")
    residency_rebaseline.add_argument("conversation_id")
    residency_rebaseline.add_argument("--reason")

    voice = commands.add_parser("voice")
    voice_commands = _subcommands(voice, dest="voice_command", required=True)
    voice_commands.add_parser("retry-blocked")

    alias = commands.add_parser("alias")
    alias_commands = _subcommands(alias, dest="alias_command", required=True)
    alias_set = alias_commands.add_parser("set")
    alias_set.add_argument("participant_id")
    alias_set.add_argument("alias")
    alias_set.add_argument("--conversation-id")
    alias_set.add_argument("--reason")
    alias_unset = alias_commands.add_parser("unset")
    alias_unset.add_argument("participant_id")
    alias_unset.add_argument("--conversation-id")
    alias_unset.add_argument("--reason")

    correction = commands.add_parser("correction")
    correction_commands = _subcommands(correction, dest="correction_command", required=True)
    merge = correction_commands.add_parser("merge")
    merge.add_argument("source_participant_id")
    merge.add_argument("target_participant_id")
    merge.add_argument("--reason")
    split = correction_commands.add_parser("split")
    split.add_argument("merge_correction_id")
    split.add_argument("--reason")
    rebind = correction_commands.add_parser("rebind")
    rebind.add_argument("source_key_id")
    rebind.add_argument("target_participant_id")
    rebind.add_argument("--reason")
    rollback = correction_commands.add_parser("rollback")
    rollback.add_argument("correction_id")
    rollback.add_argument("--reason")
    list_corrections = correction_commands.add_parser("list")
    list_corrections.add_argument("--limit", type=int, default=50)
    return parser


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _refuse_literal_key_argument(argv: list[str]) -> None:
    """Refuse ``--key`` before parsing so typed key material is never echoed or logged.

    No Sightglass command accepts literal key material on the command line; options
    such as ``--key-file`` and ``--stdin`` are the only import paths.
    """
    for token in argv:
        if _LITERAL_KEY_ARGUMENT.fullmatch(token):
            print(
                json.dumps(
                    {
                        "schema": "sightglass.operator-error.v1",
                        "ok": False,
                        "message": (
                            "key material must be imported from --key-file or "
                            "--stdin, never from argv"
                        ),
                    },
                    ensure_ascii=False,
                ),
                file=sys.stderr,
            )
            raise SystemExit(2)


def _config_store(path: str | None) -> ConfigStore:
    return ConfigStore(path) if path else ConfigStore()


def _initialize(args: argparse.Namespace, store: ConfigStore) -> dict[str, Any]:
    if store.path.exists():
        raise RuntimeError("Sightglass is already initialized at this config path")
    source_root = Path(args.source_root).expanduser().resolve()
    health = SyntheticSourceProvider(source_root).health()
    if not health.complete:
        raise RuntimeError("the selected synthetic source is incomplete")
    data_dir = (
        Path(args.data_dir).expanduser().resolve() if args.data_dir else store.path.parent.resolve()
    )
    reader_token = new_token()
    operator_token = new_token()
    secret_store = KeychainSecretStore()
    secret_store.set(READER_SECRET_ACCOUNT, reader_token)
    secret_store.set(OPERATOR_SECRET_ACCOUNT, operator_token)
    config = replace(
        SightglassConfig.create(data_dir, source_root),
        reader_token_hash=token_hash(reader_token),
        operator_token_hash=token_hash(operator_token),
    )
    store.save(config)
    WindowDB(config.window_db_path)
    return {
        "schema": "sightglass.init.v1",
        "initialized": True,
        "synthetic_only": True,
        "config": str(store.path),
    }


def _native_modules() -> dict[str, Any]:
    try:
        from sightglass.source.macos_wechat import MacOSWeChatSourceProvider
        from sightglass.source.macos_wechat.config import MacOSWeChatSettings
        from sightglass.source.macos_wechat.discovery import discover_candidates
        from sightglass.source.macos_wechat.keys import (
            decode_key_map,
            delete_keychain_secret,
            encode_key_map,
            image_decoder_keychain_account,
            import_verified_key_file,
            new_source_account_binding_id,
            parse_image_decoder_key,
            read_image_decoder_key_file,
            read_image_decoder_key_stream,
            read_image_xor_key_file,
            read_keychain_secret,
            source_account_key,
            verify_key_map,
            write_keychain_secret,
        )
    except ImportError as exc:
        raise RuntimeError(
            "macos-wechat dependencies are missing; install sightglass[macos-wechat]"
        ) from exc
    return {
        "provider": MacOSWeChatSourceProvider,
        "settings": MacOSWeChatSettings,
        "discover": discover_candidates,
        "decode_keys": decode_key_map,
        "encode_keys": encode_key_map,
        "import_keys": import_verified_key_file,
        "new_account_binding": new_source_account_binding_id,
        "read_secret": read_keychain_secret,
        "account_key": source_account_key,
        "verify_keys": verify_key_map,
        "write_secret": write_keychain_secret,
        "delete_secret": delete_keychain_secret,
        "parse_image_key": parse_image_decoder_key,
        "read_image_key_file": read_image_decoder_key_file,
        "read_image_key_stream": read_image_decoder_key_stream,
        "read_image_xor_key_file": read_image_xor_key_file,
        "image_key_account": image_decoder_keychain_account,
    }


def _discover_native() -> tuple[Any, ...]:
    modules = _native_modules()
    return tuple(modules["discover"]())


def _source_command(args: argparse.Namespace, store: ConfigStore) -> dict[str, Any]:
    if args.source_command == "providers":
        return {
            "schema": "sightglass.source-providers.v1",
            "providers": list(provider_descriptors()),
        }
    if args.source_command == "discover":
        return {
            "schema": "sightglass.source-candidates.v1",
            "kind": args.kind,
            "candidates": [item.as_public_dict() for item in _discover_native()],
        }
    if args.source_command == "image-key":
        return _source_image_key(args, store)
    return _initialize_macos_wechat(args, store)


def _daemon_must_be_stopped(config: SightglassConfig, store: ConfigStore) -> None:
    if not config.socket_path.exists() and not config.socket_path.is_symlink():
        return
    try:
        _operator(store).call("daemon.status")
    except Exception as exc:
        raise RuntimeError(
            "Sightglass IPC path still exists; stop or recover sightglassd before changing config"
        ) from exc
    raise RuntimeError("stop sightglassd before changing the configured runtime")


def _timezone_command(args: argparse.Namespace, store: ConfigStore) -> dict[str, Any]:
    config = store.load()
    if args.timezone_command == "set":
        timezone = validate_timezone(args.iana_timezone)
        _daemon_must_be_stopped(config, store)
        config = replace(config, reader_timezone=timezone)
        store.save(config)
    return {
        "schema": "sightglass.reader-timezone.v1",
        "timezone": config.reader_timezone,
    }


def _compact_command(args: argparse.Namespace, store: ConfigStore) -> dict[str, Any]:
    from sightglass.model.backups import _private_regular_file
    from sightglass.model.compact_candidate import (
        build_candidate,
        freeze_input,
        preview_release,
        preview_stock_release,
        read_legacy_preview,
        require_encrypted_volume,
        verify_candidate,
    )
    from sightglass.runtime.paired import activate_pair, prepare_pair, recover_selection
    from sightglass.runtime.process_lock import acquire_runtime_lock, release_runtime_lock

    _private_regular_file(store.path)
    config = SightglassConfig.from_dict(json.loads(store.path.read_text()))
    if args.compact_command == "preview":
        if args.all_stock:
            return preview_stock_release(config.window_db_path)
        if args.conversation_id:
            return preview_release(
                config.window_db_path, args.conversation_id, after=args.cursor, limit=args.limit
            )
        return read_legacy_preview(config.window_db_path)
    _daemon_must_be_stopped(config, store)
    lock = acquire_runtime_lock(config.socket_path.parent / "sightglassd.lock")
    try:
        if args.compact_command == "build":
            if config.source_kind != "synthetic":
                require_encrypted_volume(args.workspace)
            if args.release_plans_file:
                _private_regular_file(args.release_plans_file)
            plans = (
                json.loads(args.release_plans_file.read_text()) if args.release_plans_file else []
            )
            freeze_input(
                config.window_db_path,
                args.workspace,
                workspace_budget_bytes=args.workspace_budget_bytes,
                min_free_bytes=config.storage.min_free_bytes,
            )
            return build_candidate(
                args.workspace / "frozen.db",
                args.workspace / "candidate.db",
                release_plans=plans,
                workspace_budget_bytes=args.workspace_budget_bytes,
                min_free_bytes=config.storage.min_free_bytes,
            )
        if args.compact_command == "verify":
            manifest = json.loads((args.workspace / "candidate.db.json").read_text())
            return verify_candidate(
                args.workspace / "frozen.db",
                args.workspace / "candidate.db",
                release_plans=manifest.get("release_plans"),
            )
        if args.compact_command == "prepare-pair":
            return prepare_pair(
                args.pair_root,
                config_path=store.path,
                runtime_python=args.runtime_python,
                candidate_path=args.workspace / "candidate.db" if args.workspace else None,
                frozen_path=args.workspace / "frozen.db" if args.workspace else None,
                copy_current=args.copy_current,
                workspace_budget_bytes=args.workspace_budget_bytes,
                min_free_bytes=config.storage.min_free_bytes,
            )
        if args.compact_command == "activate-pair":
            return activate_pair(
                args.pair_root, args.pair_id, expected_current=args.expected_current
            )
        if args.compact_command == "recover-pair":
            return recover_selection(args.pair_root)
        raise RuntimeError("unknown compact operation")
    finally:
        release_runtime_lock(lock)


def _storage_command(args: argparse.Namespace, store: ConfigStore) -> dict[str, Any]:
    from sightglass.storage import StorageBudget, StorageSettings

    if args.storage_command == "compact":
        return _compact_command(args, store)
    config = store.load()
    if args.storage_command == "backup":
        from sightglass.model.backups import (
            backup_plan,
            create_compressed_snapshot,
            recover_interrupted_restore,
            restore_compressed_snapshot,
            retire_compressed_snapshot,
            retire_legacy_backups,
        )
        from sightglass.runtime.process_lock import (
            acquire_runtime_lock,
            release_runtime_lock,
        )

        _daemon_must_be_stopped(config, store)
        runtime_lock = acquire_runtime_lock(config.socket_path.parent / "sightglassd.lock")
        try:
            recover_interrupted_restore(config.window_db_path)
            if args.backup_command == "plan":
                return backup_plan(config.window_db_path)
            if args.backup_command == "create":
                return create_compressed_snapshot(config.window_db_path)
            if args.backup_command == "retire":
                if args.artifact:
                    return retire_compressed_snapshot(
                        config.window_db_path, args.artifact, args.ack
                    )
                return retire_legacy_backups(config.window_db_path, args.ack)
            if args.backup_command == "restore":
                return restore_compressed_snapshot(config.window_db_path, args.artifact, args.ack)
            raise RuntimeError("unsupported storage backup command")
        finally:
            release_runtime_lock(runtime_lock)
    if args.storage_command == "explain":
        params = {
            "offset": args.offset,
            "limit": args.limit,
            "sample_size": args.sample_size,
            "deep": getattr(args, "deep", False),
            "phase": getattr(args, "phase", "all"),
            "after_object": getattr(args, "after_object", None),
            "deadline_seconds": getattr(args, "deadline_seconds", 10.0),
        }
        if config.socket_path.exists() or config.socket_path.is_symlink():
            return _operator(store, timeout=30.0).call("operator.storage.explain", params)
        from sightglass.runtime.storage_history import StorageHistory

        budget = StorageBudget(config.data_dir, config.window_db_path, config.storage)
        return {
            "schema": "sightglass.storage-explain.v1",
            "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "status": budget.status(reconcile=True),
            "files": budget.explain(offset=args.offset, limit=args.limit),
            "database": {
                "available": False,
                "reason": "daemon_not_running",
            },
            "history": StorageHistory(config.data_dir, budget).history(
                limits=config.storage.as_dict()
            ),
            "mutated": False,
        }
    if args.storage_command == "configure":
        _daemon_must_be_stopped(config, store)
        values = config.storage.as_dict()
        changes = {key: getattr(args, key) for key in values if getattr(args, key) is not None}
        if not changes:
            raise RuntimeError("storage configure requires at least one limit")
        values.update(changes)
        config = replace(config, storage=StorageSettings.from_dict(values))
        store.save(config)
    elif config.socket_path.exists() or config.socket_path.is_symlink():
        status = _operator(store).call("daemon.status")
        if "storage" in status:
            return status["storage"]
        # The new CLI may inspect an installed pre-budget daemon. Its response
        # lacks storage accounting, so fall back to file bytes without opening DB.
    budget = StorageBudget(config.data_dir, config.window_db_path, config.storage)
    return budget.status()


def _select_native_canary(provider: Any) -> tuple[str, str, str]:
    for attempt in range(3):
        try:
            with provider.snapshot() as snapshot:
                accounts = provider.list_accounts(snapshot)
                if len(accounts) != 1:
                    raise RuntimeError("native source must expose exactly one account")
                account = accounts[0]
                conversations = provider.list_conversations(account.source_account_key, snapshot)
                for conversation in conversations:
                    page = provider.read_recent(
                        account.source_account_key,
                        conversation.source_conversation_id,
                        1,
                        snapshot,
                    )
                    if page.messages:
                        return (
                            account.source_account_key,
                            conversation.source_conversation_id,
                            page.messages[-1].sent_at_utc,
                        )
        except SightglassError as exc:
            if exc.code == ErrorCode.SOURCE_GENERATION_CHANGED and attempt < 2:
                continue
            raise
        break
    raise RuntimeError("no readable recent WeChat conversation was discovered")


def _initialize_macos_wechat(args: argparse.Namespace, store: ConfigStore) -> dict[str, Any]:
    if not store.path.exists():
        raise RuntimeError("initialize Sightglass before selecting a native source")
    config = store.load()
    _daemon_must_be_stopped(config, store)
    modules = _native_modules()
    candidates = [
        item for item in modules["discover"]() if item.running and item.profile_id is not None
    ]
    if args.candidate:
        candidates = [item for item in candidates if item.candidate_id == args.candidate]
    if len(candidates) != 1:
        raise RuntimeError("select exactly one running, supported WeChat source candidate")
    candidate = candidates[0]
    settings_path = config.data_dir / "sources" / candidate.candidate_id / "source.json"
    existing = None
    if settings_path.exists():
        existing = modules["settings"].load(settings_path)
        if (
            existing.instance_id != candidate.candidate_id
            or existing.source_root != candidate.source_root
            or existing.bundle_id != candidate.bundle_id
            or existing.version != candidate.version
            or existing.build != candidate.build
            or existing.architecture != candidate.architecture
            or (existing.profile_id and existing.profile_id != candidate.profile_id)
        ):
            raise RuntimeError("existing native settings bind a different source candidate")
    if args.reuse_installed_keys:
        if existing is None:
            raise RuntimeError("no installed native source keys are available to reuse")
        installed_keys = modules["decode_keys"](modules["read_secret"](existing.keychain_account))
        keys = modules["verify_keys"](candidate.source_root, installed_keys)
        credential_source = "installed_keychain"
    else:
        key_file = Path(os.path.abspath(Path(args.key_file).expanduser()))
        keys = modules["import_keys"](candidate.source_root, key_file)
        credential_source = "verified_file"
    encoded_keys = modules["encode_keys"](keys)
    binding_id = (
        existing.source_account_binding_id
        if existing is not None
        else modules["new_account_binding"]()
    )
    account_key = (
        existing.source_account_key if existing is not None else modules["account_key"](binding_id)
    )
    keychain_account = (
        existing.keychain_account if existing is not None else f"source.{binding_id}.database-keys"
    )
    settings = modules["settings"](
        instance_id=candidate.candidate_id,
        source_root=candidate.source_root,
        keychain_account=keychain_account,
        source_account_binding_id=binding_id,
        source_account_key=account_key,
        bundle_id=candidate.bundle_id,
        version=candidate.version,
        build=candidate.build,
        architecture=candidate.architecture,
        profile_id=str(candidate.profile_id),
        image_keychain_account=(existing.image_keychain_account if existing is not None else None),
    )
    settings.save(settings_path)
    provider = modules["provider"](
        settings_path,
        secret_loader=lambda _account: encoded_keys,
    )
    source_account, source_conversation, latest_at = _select_native_canary(provider)
    external_account = opaque_id("wxacct", source_account)
    external_conversation = opaque_id("wxconv", external_account, source_conversation)
    if credential_source == "verified_file":
        modules["write_secret"](keychain_account, encoded_keys)
    window_path = config.data_dir / f"window-{candidate.candidate_id}-source-id-v2.db"
    WindowDB(window_path)
    activated = config.with_source(
        kind="macos-wechat",
        instance_id=candidate.candidate_id,
        settings_path=settings_path,
        window_db_path=window_path,
        paused=False,
        policy_mode="allowlist",
        allowed=(external_conversation,),
    )
    store.save(activated)
    return {
        "schema": "sightglass.source-initialization.v1",
        "initialized": True,
        "source_kind": "macos-wechat",
        "source_mode": "live",
        "candidate": candidate.as_public_dict(),
        "credential_source": credential_source,
        "verified_database_count": len(keys),
        "allowlisted_conversation_id": external_conversation,
        "latest_message_at": latest_at,
    }


def _operator(store: ConfigStore, *, timeout: float = 30.0) -> IPCClient:
    return IPCClient(config_store=store, role="operator", timeout=timeout)


def _image_key_settings(args: argparse.Namespace, store: ConfigStore) -> tuple[Any, Any, Path]:
    """Resolve the native settings that own the account-bound image key identity."""
    modules = _native_modules()
    override = getattr(args, "settings", None)
    if override:
        settings_path = Path(override).expanduser().resolve()
        return modules, None, settings_path
    if not store.path.exists():
        raise RuntimeError("initialize and select a native source before managing the image key")
    config = store.load()
    if config.source_kind != "macos-wechat" or config.source_settings_path is None:
        raise RuntimeError("the configured source does not keep a native image key")
    return modules, config, config.source_settings_path


def _source_image_key(args: argparse.Namespace, store: ConfigStore) -> dict[str, Any]:
    modules, config, settings_path = _image_key_settings(args, store)
    settings = modules["settings"].load(settings_path)
    account = modules["image_key_account"](settings.source_account_binding_id)
    try:
        modules["parse_image_key"](modules["read_secret"](account))
        enrolled = True
    except Exception:
        enrolled = False
    if args.image_key_command != "status":
        if config is not None:
            _daemon_must_be_stopped(config, store)
    report: dict[str, Any] = {
        "schema": "sightglass.source-image-key.v1",
        "action": args.image_key_command,
        "account_bound": True,
    }
    if args.image_key_command == "status":
        return {**report, "enrolled": enrolled}
    if args.image_key_command == "import":
        if args.key_file:
            key_file = Path(os.path.abspath(Path(args.key_file).expanduser()))
            key = modules["read_image_key_file"](key_file)
        else:
            key = modules["read_image_key_stream"](sys.stdin.buffer)
        try:
            xor_path = getattr(args, "xor_key_file", None)
            if xor_path:
                xor = modules["read_image_xor_key_file"](
                    Path(os.path.abspath(Path(xor_path).expanduser()))
                )
                try:
                    material = json.dumps(
                        {"aes_key": key.hex(), "xor_key": xor.hex()}, separators=(",", ":")
                    )
                    modules["write_secret"](account, material)
                finally:
                    del xor
                    del material
            else:
                modules["write_secret"](account, key.hex())
        finally:
            del key
        if settings.image_keychain_account != account:
            replace(settings, image_keychain_account=account).save(settings_path)
        return {
            **report,
            "enrolled": True,
            "replaced": enrolled,
            "accepted_format": "32 ascii hexadecimal characters",
        }
    modules["delete_secret"](account)
    if settings.image_keychain_account is not None:
        replace(settings, image_keychain_account=None).save(settings_path)
    return {**report, "enrolled": False, "removed": enrolled}


def _daemon_start(store: ConfigStore) -> dict[str, Any]:
    client = _operator(store)
    try:
        status = client.call("daemon.status")
        return {**status, "already_running": True}
    except Exception:
        pass
    config = store.load()
    log_path = config.data_dir / "sightglassd.log"
    descriptor = os.open(
        log_path,
        os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise RuntimeError("Sightglass daemon log must be a regular file")
        if stat.S_IMODE(metadata.st_mode) & 0o077:
            raise RuntimeError("Sightglass daemon log must use mode 0600")
        process = subprocess.Popen(
            [sys.executable, "-m", "sightglass.runtime.daemon", "--config", str(store.path)],
            stdin=subprocess.DEVNULL,
            stdout=descriptor,
            stderr=descriptor,
            start_new_session=True,
            close_fds=True,
        )
    finally:
        os.close(descriptor)
    deadline = time.monotonic() + _daemon_start_timeout()
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                "sightglassd exited before becoming ready; its output is in the private "
                f"daemon log at {log_path}"
            )
        try:
            return client.call("daemon.status")
        except Exception:
            time.sleep(0.05)
    process.terminate()
    raise RuntimeError(
        "sightglassd did not become ready within "
        f"{_daemon_start_timeout():.0f}s; check the private sightglassd.log and run "
        "`sightglassctl doctor`. Older schemas require stopped-only `storage compact`; "
        f"{DAEMON_START_TIMEOUT_ENV} controls this startup readiness wait."
    )


def _daemon_stop(store: ConfigStore) -> dict[str, Any]:
    client = _operator(store)
    status = client.call("daemon.status")
    pid = int(status["pid"])
    result = client.call("daemon.shutdown")
    # The daemon may be waiting for one bounded live-source poll to leave the
    # SourceWorker join window. Keep the CLI grace period longer than that owned
    # timeout so a clean stop is not misreported as failure at eight seconds.
    deadline = time.monotonic() + SOURCE_WORKER_STOP_TIMEOUT_SECONDS + 2
    while time.monotonic() < deadline and process_is_running(pid):
        time.sleep(0.05)
    if process_is_running(pid):
        raise RuntimeError("sightglassd did not stop cleanly")
    return result


def _control_call(args: argparse.Namespace, store: ConfigStore) -> Any:
    client = _operator(store)
    if args.command == "status":
        return client.call("daemon.status")
    if args.command == "doctor":
        return client.call("operator.doctor")
    if args.command in {"pause", "resume"}:
        return client.call(f"operator.{args.command}")
    if args.command == "conversation":
        return client.call(
            f"operator.policy.{args.conversation_command}",
            {"conversation_id": args.conversation_id},
        )
    if args.command == "scope":
        if args.scope_command == "status":
            return client.call("operator.policy.status")
        if args.scope_command == "set":
            if args.mode == "account" and not args.confirm_account_scope:
                raise RuntimeError("account scope requires --confirm-account-scope")
            return client.call(
                "operator.policy.set",
                {"mode": args.mode},
            )
        if args.scope_command == "catalog":
            return client.call(
                "operator.policy.catalog",
                {"cursor": args.cursor, "limit": args.limit},
            )
        method = args.scope_command.replace("-", "_")
        return client.call(
            f"operator.policy.{method}",
            {"conversation_id": args.conversation_id},
        )
    if args.command == "backfill":
        if args.backfill_command == "status":
            return client.call("operator.backfill.status")
        if args.backfill_command in {"pause", "resume"}:
            return client.call(f"operator.backfill.{args.backfill_command}")
        return client.call(
            "operator.backfill.queue",
            {
                "conversation_id": getattr(args, "conversation_id", None),
                "after": args.after,
                "before": args.before,
                "max_messages": args.max_messages,
            },
        )
    if args.command == "maintenance":
        params = {"limit": args.limit}
        if args.observation_command == "inspect":
            params["after_message_id"] = args.after_message_id
        return client.call(f"operator.maintenance.observations.{args.observation_command}", params)
    if args.command == "cache":
        if args.cache_command == "status":
            return client.call("operator.cache.status")
        if args.cache_command == "preview":
            return client.call("operator.cache.preview")
        return client.call("operator.cache.cleanup", {"apply": args.apply})
    if args.command == "residency":
        if args.residency_command == "status":
            return client.call("operator.residency.status")
        if args.residency_command == "list":
            return client.call(
                "operator.residency.list",
                {
                    "mode": args.mode,
                    "sort": args.sort,
                    "limit": args.limit,
                    "cursor": args.cursor,
                },
            )
        if args.residency_command == "set":
            return client.call(
                "operator.residency.set",
                {
                    "conversation_ids": list(args.conversation_ids),
                    "mode": args.mode,
                    "keep_backfill": args.keep_backfill,
                    "recent_window_days": args.recent_window_days,
                    "recent_max_bytes": args.recent_max_bytes,
                    "reason": args.reason,
                },
            )
        if args.residency_command == "configure":
            settings = {}
            if args.default_mode is not None:
                settings["default_mode"] = args.default_mode
            if args.recent_window_days is not None:
                settings["recent_window_days"] = args.recent_window_days
            if args.recent_max_bytes is not None:
                settings["recent_max_bytes"] = args.recent_max_bytes
            if args.global_max_bytes is not None:
                settings["global_max_bytes"] = args.global_max_bytes
            if args.lease_ttl_seconds is not None:
                settings["lease_ttl_seconds"] = args.lease_ttl_seconds
            if args.lease_max_bytes is not None:
                settings["lease_max_bytes"] = args.lease_max_bytes
            return client.call("operator.residency.configure", {"settings": settings or None})
        if args.residency_command == "preview":
            return client.call(
                "operator.residency.release",
                {
                    "conversation_id": args.conversation_id,
                    "apply": False,
                    "cursor": args.cursor,
                    "limit": args.limit,
                },
            )
        if args.residency_command == "release":
            if args.apply and args.plan_file is None:
                raise ValueError("release --apply requires --plan-file from residency preview")
            plan = json.loads(args.plan_file.read_text()) if args.plan_file else None
            if plan is not None and "plan" in plan:
                plan = plan["plan"]
            return client.call(
                "operator.residency.release",
                {"conversation_id": args.conversation_id, "apply": args.apply, "plan": plan},
            )
        if args.residency_command == "rebaseline":
            return client.call(
                "operator.residency.rebaseline",
                {"conversation_id": args.conversation_id, "reason": args.reason},
            )
        raise RuntimeError("unsupported Sightglass residency command")
    if args.command == "voice":
        if args.voice_command == "retry-blocked":
            return client.call("operator.voice.retry_blocked")
        raise RuntimeError("unsupported Sightglass voice command")
    if args.command == "alias":
        params = {
            "participant_id": args.participant_id,
            "conversation_id": args.conversation_id,
            "reason": args.reason,
        }
        if args.alias_command == "set":
            params["alias"] = args.alias
        return client.call(f"operator.alias.{args.alias_command}", params)
    if args.command == "correction":
        params = {"reason": getattr(args, "reason", None)}
        if args.correction_command == "merge":
            params.update(
                source_participant_id=args.source_participant_id,
                target_participant_id=args.target_participant_id,
            )
        elif args.correction_command == "split":
            params["merge_correction_id"] = args.merge_correction_id
        elif args.correction_command == "rebind":
            params.update(
                source_key_id=args.source_key_id,
                target_participant_id=args.target_participant_id,
            )
        elif args.correction_command == "rollback":
            params["correction_id"] = args.correction_id
        else:
            params = {"limit": args.limit}
        return client.call(f"operator.correction.{args.correction_command}", params)
    raise RuntimeError("unsupported Sightglass control command")


def main(argv: list[str] | None = None) -> None:
    tokens = list(sys.argv[1:] if argv is None else argv)
    _refuse_literal_key_argument(tokens)
    args = _parser().parse_args(tokens)
    store = _config_store(args.config)
    try:
        if args.command == "synthetic-create":
            _print({"created": str(create_synthetic_source(args.root)), "synthetic_only": True})
            return
        if args.command == "init":
            _print(_initialize(args, store))
            return
        if args.command == "source":
            _print(_source_command(args, store))
            return
        if args.command == "timezone":
            _print(_timezone_command(args, store))
            return
        if args.command == "retrieval":
            if args.retrieval_command in {
                "configure-semantic",
                "disable-semantic",
                "import-semantic-token",
            }:
                _print(_semantic_command(args, store))
                return
            params = {"kind": args.kind} if args.retrieval_command == "rebuild" else {}
            _print(_operator(store).call(f"operator.retrieval.{args.retrieval_command}", params))
            return
        if args.command == "storage":
            _print(_storage_command(args, store))
            return
        if args.command == "daemon":
            if args.daemon_command == "start":
                _print(_daemon_start(store))
            elif args.daemon_command == "stop":
                _print(_daemon_stop(store))
            else:
                _daemon_stop(store)
                _print(_daemon_start(store))
            return
        _print(_control_call(args, store))
    except Exception as exc:
        print(
            json.dumps(
                {"schema": "sightglass.operator-error.v1", "ok": False, "message": str(exc)},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        raise SystemExit(2) from None


def _read_semantic_file(filename: str) -> bytes:
    path = Path(os.path.abspath(Path(filename).expanduser()))
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o077
            or info.st_size > 8192
        ):
            raise RuntimeError("semantic input must be an owner-private regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            data = stream.read(8193)
        if len(data) > 8192:
            raise RuntimeError("semantic input exceeds 8192 bytes")
        return data
    finally:
        os.close(descriptor)


def _semantic_command(args: argparse.Namespace, store: ConfigStore) -> dict[str, Any]:
    config = store.load()
    _daemon_must_be_stopped(config, store)
    action = args.retrieval_command
    if action == "disable-semantic":
        store.save(replace(config, semantic=replace(config.semantic, enabled=False)))
        return {"semantic_enabled": False, "activated": False}
    if action == "configure-semantic":
        settings = SemanticSettings.from_dict(json.loads(_read_semantic_file(args.settings_file)))
        if settings.enabled and not args.authorize_external_data:
            raise RuntimeError("semantic activation requires --authorize-external-data")
        store.save(replace(config, semantic=settings))
        return {"semantic_enabled": settings.enabled, "activated": False}
    if not config.semantic.cf_account_id:
        raise RuntimeError("configure the exact Cloudflare account before importing its token")
    data = _read_semantic_file(args.token_file) if args.token_file else sys.stdin.buffer.read(8193)
    try:
        token = data.decode("utf-8").strip()
        if not token or len(data) > 8192 or any(char.isspace() for char in token):
            raise ValueError("invalid token")
    except (UnicodeError, ValueError):
        raise RuntimeError("semantic token must be a bounded UTF-8 token") from None
    # Reuse Security.framework storage; the token never enters subprocess argv.
    from sightglass.source.macos_wechat.keys import write_keychain_secret

    write_keychain_secret(semantic_secret_account(config.semantic.cf_account_id), token)
    return {"semantic_token_enrolled": True, "activated": False}


if __name__ == "__main__":
    main()

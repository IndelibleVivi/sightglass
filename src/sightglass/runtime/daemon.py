from __future__ import annotations

import argparse
import hashlib
import json
import os
import select
import signal
import socket
import stat
import sys
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

from pydantic import BaseModel

from sightglass import __version__
from sightglass.contracts.errors import ErrorCode, SightglassError, map_unexpected_error
from sightglass.model.db import WindowDB
from sightglass.operations import (
    check_operation_budget,
    local_read_only_scope,
    operation_budget,
    operation_expired,
    operation_remaining_seconds,
    wait_for_event,
)
from sightglass.resources.jobs import ResourceJobService
from sightglass.resources.processors import processor_status

from .config import ConfigStore, SightglassConfig
from .control import (
    cache_preview,
    cache_status,
    cleanup_cache,
    doctor,
    pending_delivery_count,
    residency_configure,
    residency_list,
    residency_rebaseline,
    residency_release,
    residency_set,
    residency_status,
)
from .corrections import CorrectionService
from .derived_worker import DerivedIndexWorker
from .ipc import (
    IPC_VERSION,
    authenticated_role,
    peer_effective_ids,
    receive_frame,
    send_frame,
)
from .lanes import RuntimeLanes, WorkClass
from .observability import ToolMetrics
from .process_lock import acquire_runtime_lock, release_runtime_lock
from .resource_worker import ResourceWorker
from .search_preparation import SearchPreparation
from .secrets import (
    OPERATOR_SECRET_ACCOUNT,
    READER_SECRET_ACCOUNT,
    SecretStore,
    default_secret_store,
    token_hash,
)
from .semantic_worker import SemanticWorker
from .service import build_daemon_tools
from .source_worker import SourceWorker
from .storage_history import (
    STORAGE_HISTORY_STATE_SCHEMA,
    StorageHistory,
    StorageHistoryError,
    capture_daily_snapshot,
    next_utc_midnight_delay,
)
from .voice_setup import VoiceSetup, build_voice_setup, injected_readiness
from .voice_wait import VOICE_WAIT_CEILING_MS, VoiceWaiters, bound_wait_ms
from .voice_worker import Transcriber, VoiceWorker

READER_METHODS = {"tools.call", "daemon.status"}
OPERATOR_METHODS = {
    "daemon.status",
    "daemon.shutdown",
    "operator.pause",
    "operator.resume",
    "operator.policy.allow",
    "operator.policy.deny",
    "operator.policy.clear_deny",
    "operator.policy.status",
    "operator.policy.set",
    "operator.policy.catalog",
    "operator.backfill.status",
    "operator.backfill.queue",
    "operator.backfill.pause",
    "operator.backfill.resume",
    "operator.storage.explain",
    "operator.maintenance.observations.inspect",
    "operator.maintenance.observations.repair",
    "operator.retrieval.status",
    "operator.retrieval.explain",
    "operator.retrieval.rebuild",
    "operator.cache.status",
    "operator.cache.cleanup",
    "operator.cache.preview",
    "operator.residency.status",
    "operator.residency.list",
    "operator.residency.set",
    "operator.residency.configure",
    "operator.residency.release",
    "operator.residency.rebaseline",
    "operator.voice.retry_blocked",
    "operator.alias.set",
    "operator.alias.unset",
    "operator.correction.merge",
    "operator.correction.split",
    "operator.correction.rebind",
    "operator.correction.rollback",
    "operator.correction.list",
    "operator.doctor",
}
TOOL_NAMES = {
    "wechat_status",
    "wechat_find_conversations",
    "wechat_read_inbox",
    "wechat_find_participants",
    "wechat_read_messages",
    "wechat_read_transcripts",
    "wechat_search_messages",
    "wechat_find_links",
    "wechat_retrieve",
    "wechat_find_resources",
    "wechat_list_resources",
    "wechat_read_resource",
    "wechat_search_resource_text",
}
MAX_CONCURRENT_IPC_CONNECTIONS = 16
IPC_HANDLER_SHUTDOWN_TIMEOUT_SECONDS = 10.0
TOOL_OPERATION_TIMEOUT_SECONDS = 25.0
STORAGE_HISTORY_STOP_TIMEOUT_SECONDS = 5.0
TRANSCRIPT_WAIT_KEYS = {"reading_token", "cursor", "wait_ms", "response_profile"}
TRANSCRIPT_WAIT_BUDGET_MARGIN_SECONDS = 1.0
TRANSCRIPT_WAIT_RETRY_AFTER_MS = 250
ACTIVE_STACK_FRAME_LIMIT = 8
ACTIVE_STACK_WALK_LIMIT = 64


class _StateGate:
    """Allow concurrent readers while keeping operator mutations exclusive."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._readers = 0
        self._writer = False
        self._writers_waiting = 0

    @contextmanager
    def read(self) -> Iterator[None]:
        with self._condition:
            while self._writer or self._writers_waiting:
                self._condition.wait(timeout=0.05)
                check_operation_budget()
            self._readers += 1
        try:
            yield
        finally:
            with self._condition:
                self._readers -= 1
                if self._readers == 0:
                    self._condition.notify_all()

    @contextmanager
    def write(self) -> Iterator[None]:
        with self._condition:
            self._writers_waiting += 1
            try:
                while self._writer or self._readers:
                    self._condition.wait()
                self._writer = True
            finally:
                self._writers_waiting -= 1
        try:
            yield
        finally:
            with self._condition:
                self._writer = False
                self._condition.notify_all()


def _json_value(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return {"__pydantic__": value.__class__.__name__, "value": value.model_dump(mode="json")}
    return value


@dataclass
class _InflightOperation:
    kind: str
    started_at: float
    completed: threading.Event
    owner_thread_ident: int
    result: Any = None


@dataclass
class _StorageHistoryRecorder:
    """One immutable daily-history recorder generation.

    The stop event belongs to this generation, so a timed-out predecessor can never be
    revived by a later start clearing a shared event. The recorder keeps the exact
    history/database pair it was created for, so a leaked thread cannot write into a
    replacement configuration.
    """

    history: StorageHistory
    database: WindowDB
    stop: threading.Event
    thread: threading.Thread | None = None


class SightglassDaemon:
    def __init__(
        self,
        *,
        config_store: ConfigStore | None = None,
        secret_store: SecretStore | None = None,
        voice_transcriber: Transcriber | None = None,
        runtime_lock: TextIO | None = None,
    ) -> None:
        self.config_store = config_store or ConfigStore()
        self.secret_store = secret_store or default_secret_store()
        self.voice_transcriber = voice_transcriber
        self.voice_setup: VoiceSetup | None = None
        self.voice_readiness: dict[str, Any] = {}
        self.config = self.config_store.load()
        from .activation import require_core_activation

        require_core_activation(self.config)
        reader_token = self.secret_store.get(READER_SECRET_ACCOUNT)
        operator_token = self.secret_store.get(OPERATOR_SECRET_ACCOUNT)
        if reader_token == operator_token:
            raise RuntimeError("reader and operator credentials must differ")
        if token_hash(reader_token) != self.config.reader_token_hash:
            raise RuntimeError("reader credential does not match the private config")
        if token_hash(operator_token) != self.config.operator_token_hash:
            raise RuntimeError("operator credential does not match the private config")
        self.lanes = RuntimeLanes()
        self.tools = build_daemon_tools(self.config, secret_store=self.secret_store)
        from .capture_core import CoreCapture

        self.capture_core = (
            CoreCapture(self.tools.service, self.config)
            if self.config.source_kind == "remote-capture" else None
        )
        self.source_worker = SourceWorker(
            self.tools.service, notify_work=self._wake_derived_work,
            sync=self.capture_core.sync_once if self.capture_core is not None else None,
            backfill=self.capture_core.backfill_once if self.capture_core is not None else None,
        )
        self.derived_worker = DerivedIndexWorker(
            self.tools.service.retrieval.links, self.tools.service.reader
        )
        self.semantic_worker = SemanticWorker(self.tools.service.semantic)
        self.resource_jobs = ResourceJobService(self.tools.service.repository)
        self.resource_worker = ResourceWorker(
            self.tools.service.resource_service,
            self.resource_jobs,
        )
        self._configure_resource_runtime()
        self.corrections = CorrectionService(self.tools.service.repository.database)
        self.instance_id = f"sightglassd_{uuid.uuid4().hex}"
        self.started_at = time.time()
        self.last_error_code: str | None = None
        self.total_bridge_calls = 0
        self.active_bridge_calls = 0
        self._state_gate = _StateGate()
        self._metrics_lock = threading.Lock()
        self._connection_slots = threading.BoundedSemaphore(MAX_CONCURRENT_IPC_CONNECTIONS)
        self._operations_lock = threading.RLock()
        self._operations: dict[str, _InflightOperation] = {}
        self._joined_tool_calls = 0
        self._timed_out_tool_calls = 0
        self.tool_metrics = ToolMetrics()
        self._transcript_waiters = VoiceWaiters()
        self._connection_threads: set[threading.Thread] = set()
        self._connection_threads_lock = threading.Lock()
        self._stopping = threading.Event()
        self._server: socket.socket | None = None
        self._lock_handle: TextIO | None = runtime_lock
        self.voice_worker = self._open_voice_worker(self.tools)
        self._storage_history_recorder: _StorageHistoryRecorder | None = None
        self.storage_history: StorageHistory | None = None
        self.storage_history_state: dict[str, Any] = {
            "schema": STORAGE_HISTORY_STATE_SCHEMA,
            "available": False,
            "reason": "not_captured",
            "day_count": 0,
            "latest_day": None,
            "latest_captured_at": None,
        }
        self._configure_storage_history()
        self._configure_search_preparation()

    @property
    def database(self):
        return self.tools.service.repository.database

    def _open_voice_worker(self, tools) -> VoiceWorker:
        voice_service = tools.voice_service
        if voice_service is None:
            raise RuntimeError("Sightglass daemon requires the voice domain service")
        transcriber = self._resolve_voice_transcriber(tools)
        worker = VoiceWorker(
            voice_service,
            transcriber,
            notify_events=self._transcript_waiters.notify,
        )
        voice_service.set_worker_wake(worker.wake)
        return worker

    def _configure_resource_runtime(self) -> None:
        self.tools.service.resource_service.configure_runtime(
            lanes=self.lanes,
            jobs=self.resource_jobs,
            wake_resource_worker=self.resource_worker.wake,
            source_foreground_enter=self.source_worker.foreground_enter,
            source_foreground_exit=self.source_worker.foreground_exit,
        )

    def _configure_search_preparation(self) -> None:
        binding = hashlib.sha256(json.dumps({
            "source_kind": self.config.source_kind,
            "source_instance_id": self.config.source_instance_id,
            "source_settings_path": str(self.config.source_settings_path),
            "source_root": str(self.config.source_root),
            "epoch": self.tools.service._projection_inventory_epoch(),
            "store_file": [self.config.window_db_path.stat().st_dev,
                           self.config.window_db_path.stat().st_ino],
        }, sort_keys=True).encode()).hexdigest()
        self.search_preparation = SearchPreparation(
            self.tools.service, self.config.window_db_path.with_name("search-preparation.json"),
            binding=binding, lanes=self.lanes, source_worker=self.source_worker,
        )

    def _claim_resource_leases(self) -> None:
        self.resource_jobs.recover_outstanding_leases()

    def _close_resource_worker(self) -> None:
        self.resource_worker.stop()
        self._claim_resource_leases()

    def _resolve_voice_transcriber(self, tools) -> Transcriber | None:
        """Use an injected recognizer, else the probe result for this configuration."""

        if self.voice_transcriber is not None:
            self.voice_setup = None
            self.voice_readiness = injected_readiness(self.voice_transcriber)
            return self.voice_transcriber
        setup = build_voice_setup(self.config, tools.service)
        self.voice_setup = setup
        self.voice_readiness = setup.readiness
        return setup.transcriber

    def _claim_voice_leases(self) -> None:
        """Return leases left by a crashed or replaced context to the queue.

        Whichever process holds the runtime lock owns voice work, so a predecessor's
        live lease must not block the queue or stay able to commit a stale result.
        """

        voice_service = self.tools.voice_service
        if voice_service is not None:
            voice_service.recover_outstanding_leases()

    def _close_voice_worker(self) -> None:
        """Stop this context's worker, wake its waiters, and fence out its leases."""

        self.voice_worker.stop()
        self._transcript_waiters.notify()
        self._claim_voice_leases()

    def _configure_storage_history(self) -> None:
        """Bind the daily history sidecar to the current data directory/budget."""

        self.storage_history = StorageHistory(self.config.data_dir, self.database.storage)

    @staticmethod
    def _storage_history_failure(reason: str) -> dict[str, Any]:
        return {
            "schema": STORAGE_HISTORY_STATE_SCHEMA,
            "available": False,
            "reason": reason,
            "day_count": 0,
            "latest_day": None,
            "latest_captured_at": None,
        }

    def _capture_storage_history(
        self, history: StorageHistory, database: WindowDB, *, now=None
    ) -> dict[str, Any]:
        """Record one content-free snapshot, returning content-free state on failure."""

        try:
            snapshot = capture_daily_snapshot(database, now=now)
            history.record(snapshot)
        except StorageHistoryError as exc:
            return self._storage_history_failure(exc.reason)
        except Exception:
            return self._storage_history_failure("capture_failed")
        return history.status()

    def _capture_storage_history_day(self, *, now=None) -> None:
        """Record with the current generation and publish its content-free state."""

        recorder = self._storage_history_recorder
        if recorder is not None:
            state = self._capture_storage_history(recorder.history, recorder.database, now=now)
            if self._storage_history_recorder is recorder:
                self.storage_history_state = state
            return
        history = self.storage_history
        if history is None:
            return
        self.storage_history_state = self._capture_storage_history(history, self.database, now=now)

    def _start_storage_history(self) -> None:
        """Capture once at daemon start, then wait for each new UTC day.

        Refuses to start while a previous generation is still alive; the caller (reload)
        must abort rather than run two recorders.
        """

        if not self._stop_storage_history():
            raise RuntimeError("storage history recorder could not be stopped")
        history = self.storage_history
        if history is None:
            self._configure_storage_history()
            history = self.storage_history
            assert history is not None
        recorder = _StorageHistoryRecorder(
            history=history, database=self.database, stop=threading.Event()
        )
        self._storage_history_recorder = recorder
        self.storage_history_state = self._capture_storage_history(
            recorder.history, recorder.database
        )
        thread = threading.Thread(
            target=self._storage_history_loop,
            args=(recorder,),
            name="sightglass-storage-history",
            daemon=True,
        )
        recorder.thread = thread
        thread.start()

    def _stop_storage_history(self) -> bool:
        """Stop and join the current recorder; False if its thread could not be reaped.

        A generation is only cleared once its thread is confirmed stopped, so a leaked
        thread keeps its own stop event (never revived by a later start clearing it).
        """

        recorder = self._storage_history_recorder
        if recorder is None:
            return True
        recorder.stop.set()
        thread = recorder.thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=STORAGE_HISTORY_STOP_TIMEOUT_SECONDS)
        if thread is not None and thread.is_alive():
            return False
        self._storage_history_recorder = None
        return True

    def _storage_history_loop(self, recorder: _StorageHistoryRecorder) -> None:
        while not recorder.stop.is_set():
            if recorder.stop.wait(next_utc_midnight_delay()):
                return
            state = self._capture_storage_history(recorder.history, recorder.database)
            if self._storage_history_recorder is recorder:
                self.storage_history_state = state

    def _storage_history_explain(self) -> dict[str, Any]:
        """Return the already-persisted history block; never write during explain.

        Sidecar mutation is owned exclusively by daemon start/restart and the UTC-day
        recorder, so the read-only operator explain keeps `mutated=false` truthful.
        """

        recorder = self._storage_history_recorder
        history = recorder.history if recorder is not None else self.storage_history
        if history is None:
            return {
                "schema": STORAGE_HISTORY_STATE_SCHEMA,
                "available": False,
                "reason": "history_unavailable",
            }
        return history.history(limits=self.config.storage.as_dict())

    @property
    def lock_path(self) -> Path:
        return self.config.socket_path.parent / "sightglassd.lock"

    def _acquire_process_lock(self) -> None:
        handle = self._lock_handle
        if handle is None:
            handle = acquire_runtime_lock(self.lock_path)
        handle.seek(0)
        handle.truncate()
        json.dump(
            {
                "schema": "sightglass.process-identity.v1",
                "pid": os.getpid(),
                "instance_id": self.instance_id,
                "started_at_epoch": self.started_at,
            },
            handle,
            sort_keys=True,
        )
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        self._lock_handle = handle

    def _open_socket(self) -> socket.socket:
        path = self.config.socket_path
        if path.exists() or path.is_symlink():
            metadata = path.lstat()
            if not stat.S_ISSOCK(metadata.st_mode):
                raise RuntimeError("refusing to replace a non-socket IPC path")
            path.unlink()
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            server.bind(str(path))
            os.chmod(path, 0o600)
            server.listen(16)
            server.settimeout(0.25)
        except Exception:
            server.close()
            path.unlink(missing_ok=True)
            raise
        return server

    def _install_runtime(self, config: SightglassConfig, tools: Any) -> None:
        self.config = config
        self.tools = tools
        from .capture_core import CoreCapture

        self.capture_core = (
            CoreCapture(tools.service, config) if config.source_kind == "remote-capture" else None
        )
        self.source_worker = SourceWorker(
            tools.service, notify_work=self._wake_derived_work,
            sync=self.capture_core.sync_once if self.capture_core else None,
            backfill=self.capture_core.backfill_once if self.capture_core else None,
        )
        self.derived_worker = DerivedIndexWorker(
            tools.service.retrieval.links, tools.service.reader
        )
        self.semantic_worker = SemanticWorker(tools.service.semantic)
        self.resource_jobs = ResourceJobService(tools.service.repository)
        self.resource_worker = ResourceWorker(tools.service.resource_service, self.resource_jobs)
        self._configure_resource_runtime()
        self.voice_worker = self._open_voice_worker(tools)
        self._configure_storage_history()
        self.corrections = CorrectionService(tools.service.repository.database)
        self._configure_search_preparation()

    def _wake_derived_work(self) -> None:
        self.derived_worker.wake()
        self.semantic_worker.wake()

    def _start_runtime_workers(self) -> None:
        if self.capture_core is not None:
            self.capture_core.start()
        self.search_preparation.start()
        self._claim_resource_leases()
        self.source_worker.start()
        self.derived_worker.start()
        self.semantic_worker.start()
        self.resource_worker.start()
        self.voice_worker.start()
        self._start_storage_history()

    def _stop_runtime_workers(self) -> None:
        if not self.search_preparation.stop():
            raise RuntimeError("search preparation did not stop; refusing runtime reload")
        if not self.semantic_worker.stop():
            raise RuntimeError("semantic worker did not stop; refusing runtime reload")
        if not self.derived_worker.stop():
            raise RuntimeError("derived index worker did not stop; refusing runtime reload")
        if not self._stop_storage_history():
            raise RuntimeError("storage history recorder did not stop; refusing runtime reload")
        self._close_resource_worker()
        self._close_voice_worker()
        self.source_worker.stop()
        if self.source_worker.status().get("running"):
            raise RuntimeError("source worker did not stop; refusing runtime reload")
        if self.capture_core is not None:
            self.capture_core.close()

    def _reload(self, config: SightglassConfig) -> None:
        # The operator holds the state-gate writer: current reader calls have drained.
        # Prepare/probe without closing the usable generation. Retain it until config
        # publication and every replacement worker's start have succeeded.
        candidate = build_daemon_tools(config, secret_store=self.secret_store)
        previous_config, previous_tools = self.config, self.tools
        config_published = False
        swapped = False
        try:
            if candidate.service.repository.database.schema_version != self.database.schema_version:
                raise RuntimeError(
                    "replacement runtime schema does not match the active generation"
                )
            self._stop_runtime_workers()
            swapped = True
            self._install_runtime(config, candidate)
            config_published = True
            self.config_store.save(config)
            self._start_runtime_workers()
        except BaseException:
            if swapped:
                self._stop_runtime_workers()
                if config_published:
                    self.config_store.save(previous_config)
                self._install_runtime(previous_config, previous_tools)
                self._start_runtime_workers()
            candidate.close()
            raise
        previous_tools.close()

    def status(self) -> dict[str, Any]:
        cache = cache_status(self.database)
        descriptor = self.tools.service.provider.descriptor
        with self._metrics_lock:
            active_bridge_calls = self.active_bridge_calls
            total_bridge_calls = self.total_bridge_calls
        return {
            "schema": "sightglass.daemon-status.v1",
            "ready": not self._stopping.is_set(),
            "pid": os.getpid(),
            "instance_id": self.instance_id,
            "uptime_seconds": max(0.0, time.time() - self.started_at),
            "paused": self.config.paused,
            "source_kind": descriptor.kind,
            "source_mode": descriptor.source_mode,
            "source_implementation": descriptor.implementation,
            "synthetic_only": descriptor.source_mode == "synthetic",
            "build": self._build_status(),
            "config_revision": self._config_revision(),
            "window_db_schema_version": self.database.schema_version,
            "pending_deliveries": pending_delivery_count(self.database),
            "resource_cache": cache,
            "storage": self.database.storage_status(detailed=True),
            "mcp_bridge": {
                "active_calls": active_bridge_calls,
                "total_calls": total_bridge_calls,
            },
            "operations": self._operation_status(),
            "work_lanes": self._lane_status(),
            "resource_runtime": self.tools.service.resource_service.runtime_status(),
            "resource_worker": self.resource_worker.status().as_dict(),
            "derived_worker": self.derived_worker.status(),
            "semantic_worker": self.semantic_worker.status(),
            "retrieval": self.tools.service.retrieval.status(include_counts=False),
            "tool_metrics": self.tool_metrics.status(),
            "receipt_writer": self.tools.receipt_writer_status(),
            "window_writer": self.database.writer_status(),
            "source_worker": self.source_worker.status(),
            "source_connections": self._source_connection_status(),
            "capture": self.capture_core.status() if self.capture_core is not None else None,
            "storage_history": dict(self.storage_history_state),
            "voice_worker": self.voice_worker.status().as_dict(),
            "transcript_waiters": self._transcript_waiters.status(),
            "voice_read": {
                "enabled": bool(self.config.voice_enabled),
                "default_policy": self.config.voice_policy,
                "language": self.config.voice_language,
                "open_item_limit": self.config.voice_open_item_limit,
                "open_duration_ms": self.config.voice_open_duration_ms,
                "helper_timeout_seconds": self.config.voice_helper_timeout_seconds,
                "readiness": dict(self.voice_readiness),
            },
            "last_error_code": self.last_error_code,
        }

    def _source_connection_status(self) -> dict[str, int] | None:
        cache_status = getattr(self.tools.service.provider, "connection_cache_status", None)
        return cache_status() if cache_status is not None else None

    @staticmethod
    def _build_status() -> dict[str, Any]:
        return {
            "schema": "sightglass.build-status.v1",
            "version": __version__,
        }

    def _config_revision(self) -> str:
        encoded = json.dumps(self.config.as_dict(), sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _operation_key(name: str, arguments: dict[str, Any]) -> str:
        encoded = json.dumps(
            {"name": name, "arguments": arguments},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _active_operation_stack(owner_thread_ident: int) -> list[dict[str, Any]]:
        """Sample public code locations only; never retain frames or inspect locals."""

        frames = sys._current_frames()
        frame = frames.get(owner_thread_ident)
        locations: list[dict[str, Any]] = []
        examined = 0
        try:
            while (
                frame is not None
                and examined < ACTIVE_STACK_WALK_LIMIT
                and len(locations) < ACTIVE_STACK_FRAME_LIMIT
            ):
                module = frame.f_globals.get("__name__")
                if isinstance(module, str) and module.startswith("sightglass."):
                    locations.append(
                        {
                            "module": module,
                            "function": frame.f_code.co_name,
                            "line": frame.f_lineno,
                        }
                    )
                frame = frame.f_back
                examined += 1
        finally:
            del frame, frames
        return locations

    def _operation_status(self, *, include_stack: bool = False) -> dict[str, Any]:
        now = time.monotonic()
        with self._operations_lock:
            active = tuple(self._operations.values())
            joined = self._joined_tool_calls
            timed_out = self._timed_out_tool_calls
        oldest = min(active, key=lambda item: item.started_at) if active else None
        result = {
            "schema": "sightglass.operation-status.v1",
            "active_count": len(active),
            "active_operation": oldest.kind if oldest is not None else None,
            "active_operation_age_ms": (
                max(0, round((now - oldest.started_at) * 1_000)) if oldest is not None else None
            ),
            "max_concurrent": sum(self.lanes.limits.as_dict().values()),
            "joined_call_count": joined,
            "timed_out_call_count": timed_out,
        }
        if include_stack:
            locations = (
                self._active_operation_stack(oldest.owner_thread_ident)
                if oldest is not None else []
            )
            result["active_stack"] = locations
            result["active_phase"] = locations[0] if locations else None
        return result

    def _lane_status(self) -> dict[str, Any]:
        writer = self.database.writer_status()
        waiters = self._transcript_waiters.status()
        return self.lanes.status(
            {
                WorkClass.WINDOW_WRITE: {
                    "active": int(bool(writer["active"])),
                    "waiting": int(writer["waiting_count"] or 0),
                },
                WorkClass.WAIT_POLL: {
                    "capacity": int(waiters["max_waiters"]),
                    "active": int(waiters["active_waiters"]),
                    "waiting": 0,
                },
            }
        )

    def _busy_result(self, lane: WorkClass | None = None) -> dict[str, Any]:
        status = self._operation_status()
        return SightglassError(
            ErrorCode.SERVICE_BUSY,
            retryable=True,
            details={
                "active_operation": status["active_operation"],
                "active_operation_age_ms": status["active_operation_age_ms"],
                "active_count": status["active_count"],
                "work_class": lane.value if lane is not None else None,
                "retry_after_ms": 250,
            },
        ).as_dict()

    def _fast_status(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if set(arguments) - {"detail", "response_profile"}:
            return SightglassError(ErrorCode.QUERY_INVALID).as_dict()
        profile = arguments.get("response_profile", "brief")
        if not isinstance(profile, str) or (profile != "brief" and profile != "diagnostic"):
            return SightglassError(ErrorCode.QUERY_INVALID).as_dict()
        detail = str(arguments.get("detail", "summary"))
        if detail == "summary":
            result = self.tools.wechat_status("summary", response_profile=profile)
        elif detail == "sources":
            result = self.tools.wechat_status("sources", response_profile=profile)
        elif detail == "capabilities":
            result = self.tools.wechat_status("capabilities", response_profile=profile)
        else:
            return SightglassError(ErrorCode.QUERY_INVALID).as_dict()
        if result.get("schema") == "sightglass.status.v1":
            result["runtime"] = {
                "operations": self._operation_status(),
                "work_lanes": self._lane_status(),
                "resource_runtime": self.tools.service.resource_service.runtime_status(),
                "resource_worker": self.resource_worker.status().as_dict(),
                "derived_worker": self.derived_worker.status(),
                "semantic_worker": self.semantic_worker.status(),
                "retrieval": self.tools.service.retrieval.status(include_counts=False),
                "tool_metrics": self.tool_metrics.status(),
                "source_worker": self.source_worker.status(),
                "voice_worker": self.voice_worker.status().as_dict(),
                "transcript_waiters": self._transcript_waiters.status(),
                "receipt_writer": self.tools.receipt_writer_status(),
                "window_writer": self.database.writer_status(),
            }
        from sightglass.mcp.projection import project_result
        return project_result("wechat_status", result, arguments, str(profile))

    def _transcript_page(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Read one committed voice page inside a short reader gate."""

        with self.lanes.held(WorkClass.LOCAL_READ, wait=False) as lease:
            if lease is None:
                return self._busy_result(WorkClass.LOCAL_READ)
            with self._state_gate.read():
                return self.tools.wechat_read_transcripts(**arguments)

    def _wait_ceiling_ms(self) -> int:
        remaining = operation_remaining_seconds()
        if remaining is None:
            return VOICE_WAIT_CEILING_MS
        available = max(0.0, remaining - TRANSCRIPT_WAIT_BUDGET_MARGIN_SECONDS)
        return max(0, min(VOICE_WAIT_CEILING_MS, round(available * 1_000)))

    def _wait_block(
        self,
        state: str,
        *,
        requested_ms: int,
        elapsed_ms: int,
    ) -> dict[str, Any]:
        waiters = self._transcript_waiters.status()
        block: dict[str, Any] = {
            "schema": "sightglass.voice-wait.v1",
            "state": state,
            "requested_ms": requested_ms,
            "elapsed_ms": elapsed_ms,
            "active_waiters": waiters["active_waiters"],
            "max_waiters": waiters["max_waiters"],
            "waiter_available": waiters["available"],
            "voice_worker_enabled": self.voice_worker.status().enabled,
        }
        if state == "capacity_exhausted":
            block["retry_after_ms"] = TRANSCRIPT_WAIT_RETRY_AFTER_MS
        return block

    def _transcript_wait(
        self,
        page: dict[str, Any],
        *,
        requested_ms: int,
        connection: socket.socket | None = None,
    ) -> dict[str, Any] | None:
        """Park one bounded waiter outside the gate while transcription advances."""

        if page.get("schema") != "sightglass.voice-page.v2":
            return None
        elapsed_ms = 0
        if page.get("items"):
            state = "delivered"
        elif page.get("processing_complete"):
            state = "complete"
        elif requested_ms == 0:
            state = "disabled"
        elif not self.voice_worker.status().enabled:
            state = "unavailable"
        elif not self._transcript_waiters.acquire():
            state = "capacity_exhausted"
        else:
            try:
                started = time.monotonic()
                state = self._transcript_waiters.wait(
                    requested_ms / 1_000,
                    probe=(None if connection is None else lambda: self._peer_gone(connection)),
                )
                elapsed_ms = max(0, round((time.monotonic() - started) * 1_000))
            finally:
                self._transcript_waiters.release()
        return self._wait_block(state, requested_ms=requested_ms, elapsed_ms=elapsed_ms)

    @staticmethod
    def _peer_gone(connection: socket.socket) -> bool:
        """Report whether a parked caller has closed its IPC connection."""

        try:
            readable, _writable, _errors = select.select([connection], [], [], 0)
        except (OSError, ValueError):
            return True
        if not readable:
            return False
        try:
            return connection.recv(1, socket.MSG_PEEK) == b""
        except BlockingIOError:
            return False
        except OSError:
            return True

    def _dispatch_transcripts(
        self,
        params: dict[str, Any],
        *,
        connection: socket.socket | None = None,
    ) -> dict[str, Any]:
        """Run the three-phase transcript read: short gate, waiter, short gate."""

        name = params.get("name")
        arguments = params.get("arguments", {})
        if name != "wechat_read_transcripts" or not isinstance(arguments, dict):
            raise RuntimeError("invalid MCP tool call")
        started = time.monotonic()
        result: dict[str, Any] | None = None
        with self._metrics_lock:
            self.total_bridge_calls += 1
            self.active_bridge_calls += 1
        try:
            with operation_budget(TOOL_OPERATION_TIMEOUT_SECONDS):
                requested_ms = bound_wait_ms(
                    arguments.get("wait_ms"), ceiling_ms=self._wait_ceiling_ms()
                )
                if requested_ms is None or set(arguments) - TRANSCRIPT_WAIT_KEYS:
                    result = SightglassError(ErrorCode.QUERY_INVALID).as_dict()
                    return result
                page = self._transcript_page(arguments)
                wait = self._transcript_wait(page, requested_ms=requested_ms, connection=connection)
                if wait is not None:
                    if wait["state"] == "woken":
                        page = self._transcript_page(
                            {**arguments, "cursor": page.get("next_cursor")}
                        )
                    if page.get("schema") == "sightglass.voice-page.v2":
                        page["wait"] = wait
                result = page
                return result
        finally:
            if result is not None:
                self.tool_metrics.record(
                    str(name),
                    elapsed_ms=round((time.monotonic() - started) * 1_000),
                    result=result,
                )
            with self._metrics_lock:
                self.active_bridge_calls -= 1

    def _dispatch_tool(
        self,
        params: dict[str, Any],
        *,
        lane: WorkClass | None = None,
        source_foreground: bool = False,
        local_only: bool = False,
    ) -> Any:
        name = params.get("name")
        arguments = params.get("arguments", {})
        if name not in TOOL_NAMES or not isinstance(arguments, dict):
            raise RuntimeError("invalid MCP tool call")
        started = time.monotonic()
        with self._metrics_lock:
            self.total_bridge_calls += 1
        if name == "wechat_status":
            result = self._fast_status(arguments)
            self.tool_metrics.record(
                str(name),
                elapsed_ms=round((time.monotonic() - started) * 1_000),
                result=result,
            )
            return result

        operation_key = self._operation_key(str(name), arguments)
        with self._operations_lock:
            operation = self._operations.get(operation_key)
            owner = operation is None
            if operation is not None:
                self._joined_tool_calls += 1
            else:
                operation = _InflightOperation(
                    kind=str(name),
                    started_at=time.monotonic(),
                    completed=threading.Event(),
                    owner_thread_ident=threading.get_ident(),
                )
                self._operations[operation_key] = operation
        assert operation is not None
        if not owner:
            try:
                wait_for_event(operation.completed)
            except SightglassError as exc:
                result = exc.as_dict()
            else:
                result = operation.result
            self.tool_metrics.record(
                str(name),
                elapsed_ms=round((time.monotonic() - started) * 1_000),
                result=result,
            )
            return result

        with self._metrics_lock:
            self.active_bridge_calls += 1
        try:
            try:
                with ExitStack() as stack:
                    if lane is not None:
                        lease = stack.enter_context(self.lanes.held(lane, wait=False))
                        if lease is None:
                            operation.result = self._busy_result(lane)
                            return operation.result
                    if local_only:
                        stack.enter_context(local_read_only_scope())
                    if source_foreground:
                        self.source_worker.foreground_enter()
                        stack.callback(self.source_worker.foreground_exit)
                    def invoke() -> Any:
                        return getattr(self.tools, str(name))(**arguments)
                    operation.result = _json_value(
                        self.capture_core.call(str(name), arguments, invoke)
                        if self.capture_core is not None else invoke()
                    )
                    if (
                        source_foreground
                        and isinstance(operation.result, dict)
                        and operation.result.get("ok") is not False
                    ):
                        self._wake_derived_work()
                if operation_expired():
                    operation.result = SightglassError(
                        ErrorCode.SERVICE_TIMEOUT,
                        retryable=True,
                        details={"reason": "operation_deadline"},
                    ).as_dict()
            except TypeError:
                operation.result = map_unexpected_error(ValueError("invalid tool arguments"))
            except SightglassError as exc:
                operation.result = exc.as_dict()
            except Exception as exc:
                operation.result = map_unexpected_error(exc)
            if (
                isinstance(operation.result, dict)
                and operation.result.get("code") == ErrorCode.SERVICE_TIMEOUT.value
            ):
                details = operation.result.setdefault("details", {})
                if isinstance(details, dict):
                    details.update(
                        {
                            "operation": operation.kind,
                            "elapsed_ms": max(
                                0,
                                round((time.monotonic() - operation.started_at) * 1_000),
                            ),
                        }
                    )
                with self._operations_lock:
                    self._timed_out_tool_calls += 1
            return operation.result
        finally:
            if operation.result is not None:
                self.tool_metrics.record(
                    str(name),
                    elapsed_ms=round((time.monotonic() - started) * 1_000),
                    result=operation.result,
                )
            operation.completed.set()
            with self._operations_lock:
                self._operations.pop(operation_key, None)
            with self._metrics_lock:
                self.active_bridge_calls -= 1

    def _dispatch_operator(self, method: str, params: dict[str, Any]) -> Any:
        if method == "daemon.status":
            return self.status()
        if method == "daemon.shutdown":
            self._stopping.set()
            return {"stopping": True, "instance_id": self.instance_id}
        if method in {"operator.pause", "operator.resume"}:
            paused = method == "operator.pause"
            self._reload(self.config.with_pause(paused))
            return {"paused": paused}
        if method in {"operator.policy.allow", "operator.policy.deny"}:
            conversation_id = str(params.get("conversation_id", ""))
            if not conversation_id:
                raise RuntimeError("conversation_id is required")
            allowed = set(self.config.allowed_conversation_ids)
            denied = set(self.config.denied_conversation_ids)
            if method.endswith("allow"):
                allowed.add(conversation_id)
                denied.discard(conversation_id)
            else:
                denied.add(conversation_id)
                allowed.discard(conversation_id)
            self._reload(
                self.config.with_policy(
                    allowed=tuple(sorted(allowed)), denied=tuple(sorted(denied))
                )
            )
            return {
                "conversation_id": conversation_id,
                "decision": "allow" if method.endswith("allow") else "deny",
            }
        if method == "operator.policy.clear_deny":
            conversation_id = str(params.get("conversation_id", ""))
            if not conversation_id:
                raise RuntimeError("conversation_id is required")
            denied = set(self.config.denied_conversation_ids)
            denied.discard(conversation_id)
            self._reload(self.config.with_policy(denied=tuple(sorted(denied))))
            return {"conversation_id": conversation_id, "decision": "clear_deny"}
        if method == "operator.policy.status":
            return self.tools.service.scope_summary()
        if method == "operator.policy.set":
            selected = str(params.get("mode", ""))
            if selected == "account":
                mode = "all_except_denylist"
            elif selected == "selected":
                if not self.config.allowed_conversation_ids:
                    raise RuntimeError("selected scope requires at least one allowed conversation")
                mode = "allowlist"
            else:
                raise RuntimeError("scope mode must be selected or account")
            self._reload(self.config.with_policy(mode=mode))
            return self.tools.service.scope_summary()
        if method == "operator.policy.catalog":
            return self.tools.wechat_find_conversations(
                "",
                cursor=params.get("cursor"),
                limit=int(params.get("limit", 100)),
            )
        if method == "operator.backfill.status":
            return self.tools.service.backfill_status()
        if method == "operator.backfill.queue":
            def queue() -> dict[str, Any]:
                return self.tools.service.queue_backfill(
                    conversation_id=params.get("conversation_id"),
                    after=params.get("after"),
                    before=params.get("before"),
                    max_messages=int(params.get("max_messages", 10_000)),
                )

            result = self.capture_core.catalog_call(queue) if self.capture_core else queue()
            self.source_worker.wake()
            return result
        if method in {"operator.backfill.pause", "operator.backfill.resume"}:
            result = self.tools.service.set_backfill_paused(method.endswith("pause"))
            self.source_worker.wake()
            return result
        if method in {"operator.retrieval.status", "operator.retrieval.explain"}:
            result = self.tools.service.retrieval.status(detailed=method.endswith("explain"))
            result["worker"] = self.derived_worker.status()
            result["semantic_worker"] = self.semantic_worker.status()
            return result
        if method == "operator.retrieval.rebuild":
            if params.get("kind") == "semantic" and self.tools.service.semantic is not None:
                self.tools.service.semantic.request_rebuild()
                self.semantic_worker.wake()
                return self.tools.service.retrieval.status()
            if params.get("kind") not in {"links", "lexical"}:
                raise SightglassError(
                    ErrorCode.QUERY_INVALID, details={"reason": "index_not_enabled"}
                )
            self.tools.service.retrieval.links.request_rebuild(str(params["kind"]))
            self.derived_worker.wake()
            return self.tools.service.retrieval.status()
        if method == "operator.storage.explain":
            result = self.database.storage_explain(
                offset=params.get("offset", 0),
                limit=params.get("limit", 500),
                sample_size=params.get("sample_size", 128),
                deep=params.get("deep", False),
                phase=params.get("phase", "all"),
                after_object=params.get("after_object"),
                deadline_seconds=params.get("deadline_seconds", 10.0),
            )
            result["history"] = self._storage_history_explain()
            result["semantic"] = self.tools.service.retrieval.semantic_status()
            result["growth"] = {
                "operator_paused": self.config.paused,
                "foreground_admission_allowed": result["status"].get("admission_allowed", True),
                "background_growth_allowed": not self.config.paused
                and result["status"].get("background_growth_allowed", True),
            }
            return result
        if method == "operator.maintenance.observations.inspect":
            from sightglass.model.observation_maintenance import inspect_observation_consistency

            return inspect_observation_consistency(
                self.database, after_message_id=params.get("after_message_id"),
                limit=params.get("limit", 100),
            )
        if method == "operator.maintenance.observations.repair":
            from sightglass.model.observation_maintenance import repair_observation_consistency

            result = repair_observation_consistency(self.database, limit=params.get("limit", 100))
            self.derived_worker.wake()
            return result
        if method == "operator.cache.status":
            return cache_status(self.database)
        if method == "operator.cache.cleanup":
            return cleanup_cache(self.database, apply=bool(params.get("apply", False)))
        if method == "operator.cache.preview":
            return cache_preview(self.database)
        if method == "operator.residency.status":
            return residency_status(self.database)
        if method == "operator.residency.list":
            return residency_list(
                self.database,
                mode=params.get("mode"),
                sort=params.get("sort", "bytes"),
                limit=params.get("limit", 200),
                cursor=params.get("cursor"),
            )
        if method == "operator.residency.set":
            result = residency_set(
                self.database,
                conversation_ids=params.get("conversation_ids") or [],
                mode=str(params.get("mode", "on_demand")),
                keep_backfill=params.get("keep_backfill", False),
                recent_window_days=params.get("recent_window_days"),
                recent_max_bytes=params.get("recent_max_bytes"),
                reason=params.get("reason"),
            )
            self.source_worker.wake()
            return result
        if method == "operator.residency.configure":
            result = residency_configure(self.database, settings=params.get("settings"))
            self.source_worker.wake()
            return result
        if method == "operator.residency.release":
            return residency_release(
                self.database,
                conversation_id=str(params.get("conversation_id", "")),
                apply=params.get("apply", False),
                plan=params.get("plan"),
                cursor=params.get("cursor"),
                limit=params.get("limit", 200),
            )
        if method == "operator.residency.rebaseline":
            result = residency_rebaseline(
                self.database,
                conversation_id=str(params.get("conversation_id", "")),
                reason=params.get("reason"),
            )
            self.source_worker.wake()
            return result
        if method == "operator.voice.retry_blocked":
            voice_service = self.tools.voice_service
            if voice_service is None:
                raise RuntimeError("Sightglass daemon requires the voice domain service")
            requeued = voice_service.retry_blocked_jobs()
            if requeued:
                self._transcript_waiters.notify()
                self.voice_worker.wake()
            return {"requeued": requeued}
        if method == "operator.alias.set":
            return self.corrections.alias_set(
                str(params.get("participant_id", "")),
                str(params.get("alias", "")),
                conversation_id=params.get("conversation_id"),
                reason=params.get("reason"),
            )
        if method == "operator.alias.unset":
            return self.corrections.alias_unset(
                str(params.get("participant_id", "")),
                conversation_id=params.get("conversation_id"),
                reason=params.get("reason"),
            )
        if method == "operator.correction.merge":
            return self.corrections.merge(
                str(params.get("source_participant_id", "")),
                str(params.get("target_participant_id", "")),
                reason=params.get("reason"),
            )
        if method == "operator.correction.split":
            return self.corrections.split(
                str(params.get("merge_correction_id", "")), reason=params.get("reason")
            )
        if method == "operator.correction.rebind":
            return self.corrections.rebind(
                str(params.get("source_key_id", "")),
                str(params.get("target_participant_id", "")),
                reason=params.get("reason"),
            )
        if method == "operator.correction.rollback":
            return self.corrections.rollback(
                str(params.get("correction_id", "")), reason=params.get("reason")
            )
        if method == "operator.correction.list":
            return self.corrections.list(limit=int(params.get("limit", 50)))
        if method == "operator.doctor":
            result = doctor(self.config, keychain_ready=True)
            result["runtime"] = {
                "build": self._build_status(),
                "config_revision": self._config_revision(),
                "window_db_schema_version": self.database.schema_version,
                "resource_processors": processor_status(),
                "work_lanes": self._lane_status(),
                "resource_runtime": self.tools.service.resource_service.runtime_status(),
                "resource_worker": self.resource_worker.status().as_dict(),
                "derived_worker": self.derived_worker.status(),
                "semantic_worker": self.semantic_worker.status(),
                "retrieval": self.tools.service.retrieval.status(include_counts=False),
                "tool_metrics": self.tool_metrics.status(),
                "source_worker": self.source_worker.status(),
                "window_writer": self.database.writer_status(),
            }
            return result
        raise RuntimeError("operator method is unavailable")

    def _dispatch(
        self,
        role: str,
        method: str,
        params: dict[str, Any],
        *,
        connection: socket.socket | None = None,
    ) -> Any:
        allowed = READER_METHODS if role == "reader" else OPERATOR_METHODS
        if method not in allowed:
            raise RuntimeError("IPC role is not authorized for this method")
        if method == "daemon.status":
            if set(params) - {"operations_only", "include_stack"}:
                raise RuntimeError("invalid daemon status arguments")
            operations_only = params.get("operations_only", False)
            include_stack = params.get("include_stack", False)
            if not isinstance(operations_only, bool) or not isinstance(include_stack, bool):
                raise RuntimeError("daemon status flags must be booleans")
            if include_stack and not operations_only:
                raise RuntimeError("include_stack requires operations_only")
            if operations_only:
                if role != "operator":
                    raise RuntimeError("operation diagnostics require the operator role")
                return self._operation_status(include_stack=include_stack)
            return self.status()
        if method == "operator.storage.explain":
            with self._storage_diagnostic_cancellation(
                connection, deadline_seconds=params.get("deadline_seconds", 10.0)
            ):
                with self._state_gate.read():
                    return self._dispatch_operator(method, params)
        if method == "operator.maintenance.observations.inspect":
            with operation_budget(TOOL_OPERATION_TIMEOUT_SECONDS):
                with self._state_gate.read():
                    return self._dispatch_operator(method, params)
        if method == "tools.call" and params.get("name") == "wechat_status":
            return self._dispatch_tool(params)
        if method == "tools.call" and params.get("name") == "wechat_read_transcripts":
            return self._dispatch_transcripts(params, connection=connection)
        if method == "tools.call":
            with operation_budget(TOOL_OPERATION_TIMEOUT_SECONDS):
                with self._state_gate.read():
                    if params.get("name") == "wechat_read_resource":
                        # The resource service changes lanes at the acquisition and
                        # derivation boundaries; keeping a top-level slot here would
                        # recreate the global bottleneck SG-049 removes.
                        return self._dispatch_tool(params)
                    if self._local_only_tool_call(params):
                        # Classification and execution share the same state-gate read
                        # lease. Nested services are also forbidden from falling back
                        # to the live source if their local premise disappears.
                        return self._dispatch_tool(
                            params,
                            lane=WorkClass.SEMANTIC_READ
                            if params.get("name") == "wechat_retrieve"
                            and self.tools.service.semantic is not None
                            else WorkClass.LOCAL_READ,
                            local_only=True,
                        )
                    return self._dispatch_tool(
                        params,
                        lane=WorkClass.SOURCE_READ,
                        source_foreground=True,
                    )
        with self._state_gate.write():
            return self._dispatch_operator(method, params)

    @contextmanager
    def _storage_diagnostic_cancellation(
        self, connection: socket.socket | None, *, deadline_seconds: float,
    ) -> Iterator[None]:
        from sightglass.model.storage_diagnostics import diagnostic_deadline

        timeout = diagnostic_deadline(deadline_seconds)
        cancelled = threading.Event()
        finished = threading.Event()

        def watch() -> None:
            while not finished.wait(0.05):
                if self._stopping.is_set() or (
                    connection is not None and self._peer_gone(connection)
                ):
                    cancelled.set()
                    return

        watcher = threading.Thread(target=watch, name="sightglass-storage-cancel", daemon=True)
        watcher.start()
        try:
            with operation_budget(timeout, cancelled=cancelled):
                yield
        finally:
            finished.set()
            watcher.join(timeout=0.2)

    def _local_only_tool_call(self, params: dict[str, Any]) -> bool:
        """Ask the service whether this exact call is served without source access."""

        name = params.get("name")
        arguments = params.get("arguments", {})
        if not isinstance(name, str) or not isinstance(arguments, dict):
            return False
        try:
            return bool(self.tools.service.local_only_tool_call(name, arguments))
        except Exception:
            return False

    def _handle(self, connection: socket.socket) -> None:
        request_id: Any = None
        role: str | None = None
        try:
            peer_uid, _peer_gid = peer_effective_ids(connection)
            if peer_uid != os.geteuid():
                raise RuntimeError("IPC peer is not the daemon owner")
            request = receive_frame(connection)
            request_id = request.get("id")
            if (
                request.get("version") != IPC_VERSION
                or not isinstance(request_id, str)
                or not isinstance(request.get("token"), str)
                or not isinstance(request.get("method"), str)
                or not isinstance(request.get("params", {}), dict)
            ):
                raise RuntimeError("invalid IPC request envelope")
            role = authenticated_role(
                request["token"],
                reader_token_hash=self.config.reader_token_hash,
                operator_token_hash=self.config.operator_token_hash,
            )
            if role is None:
                raise RuntimeError("IPC authentication failed")
            result = self._dispatch(
                role,
                request["method"],
                request.get("params", {}),
                connection=connection,
            )
            response = {
                "version": IPC_VERSION,
                "id": request_id,
                "ok": True,
                "result": result,
            }
        except Exception as exc:
            self.last_error_code = exc.__class__.__name__
            operator_message = str(exc) if role == "operator" else None
            response = {
                "version": IPC_VERSION,
                "id": request_id,
                "ok": False,
                "error": {
                    "code": "IPC_REQUEST_REJECTED",
                    "message": operator_message or "Sightglass local request was rejected",
                },
            }
        try:
            send_frame(connection, response)
        except Exception:
            pass

    def _serve_connection(self, connection: socket.socket) -> None:
        try:
            with connection:
                connection.settimeout(60)
                self._handle(connection)
        finally:
            current = threading.current_thread()
            with self._connection_threads_lock:
                self._connection_threads.discard(current)
            self._connection_slots.release()

    def shutdown(self) -> None:
        self._stopping.set()
        preparation_stopped = self.search_preparation.stop()
        semantic_stopped = self.semantic_worker.stop()
        self.derived_worker.stop()
        self._stop_storage_history()
        self._close_resource_worker()
        self._close_voice_worker()
        self.source_worker.stop()
        if semantic_stopped and preparation_stopped:
            self.tools.close()
        if self._server is not None:
            self._server.close()

    def serve_forever(self, *, install_signal_handlers: bool = True) -> None:
        self._acquire_process_lock()
        self._server = self._open_socket()
        if self.capture_core is not None:
            self.capture_core.start()
        self.search_preparation.start()
        self._claim_resource_leases()
        self._claim_voice_leases()
        self.source_worker.start()
        self.derived_worker.start()
        self.semantic_worker.start()
        self.resource_worker.start()
        self.voice_worker.start()
        self._start_storage_history()
        if install_signal_handlers:
            signal.signal(signal.SIGTERM, lambda _signum, _frame: self.shutdown())
            signal.signal(signal.SIGINT, lambda _signum, _frame: self.shutdown())
        try:
            while not self._stopping.is_set():
                try:
                    connection, _address = self._server.accept()
                except TimeoutError:
                    continue
                except OSError:
                    if self._stopping.is_set():
                        break
                    raise
                while not self._connection_slots.acquire(timeout=0.25):
                    if self._stopping.is_set():
                        connection.close()
                        break
                else:
                    thread = threading.Thread(
                        target=self._serve_connection,
                        args=(connection,),
                        name="sightglass-ipc",
                        daemon=True,
                    )
                    with self._connection_threads_lock:
                        self._connection_threads.add(thread)
                    thread.start()
                    continue
                break
        finally:
            if self.capture_core is not None:
                self.capture_core.close()
            preparation_stopped = self.search_preparation.stop()
            semantic_stopped = self.semantic_worker.stop()
            self.derived_worker.stop()
            self._stop_storage_history()
            self._close_resource_worker()
            self._close_voice_worker()
            self.source_worker.stop()
            if semantic_stopped and preparation_stopped:
                self.tools.close()
            if self._server is not None:
                self._server.close()
            deadline = time.monotonic() + IPC_HANDLER_SHUTDOWN_TIMEOUT_SECONDS
            with self._connection_threads_lock:
                connection_threads = tuple(self._connection_threads)
            for thread in connection_threads:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                thread.join(timeout=remaining)
            self.config.socket_path.unlink(missing_ok=True)
            if self._lock_handle is not None:
                release_runtime_lock(self._lock_handle)
                self._lock_handle = None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sightglassd")
    parser.add_argument("--config")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    store = ConfigStore(args.config) if args.config else ConfigStore()
    config = store.load()
    runtime_lock = acquire_runtime_lock(config.socket_path.parent / "sightglassd.lock")
    try:
        SightglassDaemon(config_store=store, runtime_lock=runtime_lock).serve_forever()
    finally:
        if not runtime_lock.closed:
            release_runtime_lock(runtime_lock)


if __name__ == "__main__":
    main()

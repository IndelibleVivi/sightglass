from __future__ import annotations

import socket
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any

from sightglass.contracts.common import parse_aware_datetime, utc_now
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.contracts.voice import VoiceSelectionItem
from sightglass.mcp.bridge import DaemonReaderTools
from sightglass.model.db import WindowDB
from sightglass.operations import check_operation_budget
from sightglass.runtime.config import ConfigStore, SightglassConfig
from sightglass.runtime.daemon import SightglassDaemon
from sightglass.runtime.ipc import IPC_VERSION, IPCClient, send_frame
from sightglass.runtime.secrets import (
    OPERATOR_SECRET_ACCOUNT,
    READER_SECRET_ACCOUNT,
    MemorySecretStore,
    token_hash,
)
from sightglass.source.identity import (
    SignedTokenCodec,
    load_or_create_token_secret,
    opaque_id,
)
from sightglass.source.synthetic import create_synthetic_source
from sightglass.voice.repository import VoiceRepository
from sightglass.voice.service import VoiceService

POLL_TIMEOUT_SECONDS = 5.0
TRANSCRIPT_WAIT_MS = 8_000


class SyntheticTranscriber:
    """Programmable test recognizer; production daemon builds always pass ``None``."""

    def __init__(self) -> None:
        self.text = "Synthetic transcript"
        self.behavior = "gate"
        self.release = threading.Event()
        self.calls: list[str] = []
        self._lock = threading.Lock()

    def call_count(self) -> int:
        with self._lock:
            return len(self.calls)

    def text_for(self, call_index: int) -> str:
        return f"{self.text} #{call_index}"

    def transcribe(
        self, job: dict[str, Any], *, duration_ms: int, deadline: float | None,
    ) -> str:
        with self._lock:
            self.calls.append(str(job["job_id"]))
            call_index = len(self.calls)
            behavior = self.behavior
        if behavior == "empty":
            return ""
        if behavior == "blocked":
            raise SightglassError(ErrorCode.RESOURCE_BLOCKED)
        if behavior == "transient":
            raise SightglassError(ErrorCode.SERVICE_UNAVAILABLE, retryable=True)
        while not self.release.wait(timeout=0.05):
            check_operation_budget()
        return self.text_for(call_index)


class VoiceDaemonTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source_root = self.root / "source"
        create_synthetic_source(self.source_root)
        self.reader_token = "synthetic-reader-token"
        self.operator_token = "synthetic-operator-token"
        self.config = replace(
            SightglassConfig.create(self.root / "state", self.source_root),
            reader_token_hash=token_hash(self.reader_token),
            operator_token_hash=token_hash(self.operator_token),
        )
        self.config_store = ConfigStore(self.config.data_dir / "config.json")
        self.config_store.save(self.config)
        self.secrets = MemorySecretStore(
            {
                READER_SECRET_ACCOUNT: self.reader_token,
                OPERATOR_SECRET_ACCOUNT: self.operator_token,
            }
        )
        self.transcriber: SyntheticTranscriber | None = SyntheticTranscriber()
        self.daemon: SightglassDaemon | None = None
        self.thread: threading.Thread | None = None
        self.start_daemon()

    def tearDown(self) -> None:
        self.stop_daemon()
        self.temp.cleanup()

    # -- daemon lifecycle --------------------------------------------------

    def start_daemon(self) -> None:
        self.daemon = SightglassDaemon(
            config_store=self.config_store,
            secret_store=self.secrets,
            voice_transcriber=self.transcriber,
        )
        self.thread = threading.Thread(
            target=self.daemon.serve_forever,
            kwargs={"install_signal_handlers": False},
            daemon=True,
        )
        self.thread.start()
        deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
        while time.monotonic() < deadline and not self.config.socket_path.exists():
            time.sleep(0.01)
        self.assertTrue(self.config.socket_path.exists())
        # Socket creation precedes worker/history startup. An IPC round-trip is the
        # synchronization point that proves serve_forever reached its accept loop.
        self.assertTrue(self.operator.call("daemon.status")["ready"])

    def stop_daemon(self) -> None:
        if self.thread is None or not self.thread.is_alive():
            return
        self.operator.call("daemon.shutdown")
        self.thread.join(timeout=POLL_TIMEOUT_SECONDS)
        self.assertFalse(self.thread.is_alive())

    def restart_daemon(self) -> None:
        self.stop_daemon()
        if self.transcriber is not None:
            self.transcriber.release.clear()
        self.start_daemon()

    @property
    def reader(self) -> IPCClient:
        return IPCClient(config_store=self.config_store, secret_store=self.secrets, role="reader")

    @property
    def operator(self) -> IPCClient:
        return IPCClient(config_store=self.config_store, secret_store=self.secrets, role="operator")

    @property
    def bridge(self) -> DaemonReaderTools:
        return DaemonReaderTools(self.reader)

    def recognizer(self) -> SyntheticTranscriber:
        assert self.transcriber is not None
        return self.transcriber

    def voice_service(self):
        assert self.daemon is not None
        assert self.daemon.tools.voice_service is not None
        return self.daemon.tools.voice_service

    def standalone_voice_service(self) -> VoiceService:
        """A second domain handle for leases left behind while the daemon is down."""

        return VoiceService(
            VoiceRepository(WindowDB(self.config.window_db_path)),
            SignedTokenCodec(load_or_create_token_secret(self.config.window_db_path)),
        )

    # -- fixtures ----------------------------------------------------------

    def wait_until(self, predicate, *, timeout: float = POLL_TIMEOUT_SECONDS) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.02)
        self.fail("condition was not reached before the deadline")

    def _group(self) -> str:
        return self.bridge.wechat_find_conversations("Synthetic Group")["candidates"][0][
            "conversation_id"
        ]

    def _wait_for_reader_profile(self) -> None:
        def recorded() -> bool:
            assert self.daemon is not None
            with self.daemon.database.connection() as connection:
                return (
                    connection.execute(
                        "SELECT 1 FROM reader_profiles WHERE reader_id = ?",
                        (self.config.reader_id,),
                    ).fetchone()
                    is not None
                )

        self.wait_until(recorded)

    def _bind_voice_resource(self, message_id: str, ordinal: int) -> str:
        """Bind one synthetic voice resource; repeated calls reuse the binding."""

        assert self.daemon is not None
        resource_id = opaque_id("wxres", "synthetic-voice", message_id)
        now = utc_now().isoformat(timespec="microseconds")
        with self.daemon.database.transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO resources(resource_id, message_id, source_ordinal, kind,
                    availability, resolver_json, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, 'voice', 'available', '{}', ?, ?)
                """,
                (resource_id, message_id, 90 + ordinal, now, now),
            )
        return resource_id

    def prepare_batch(
        self, *, count: int = 1, recipe: str = "synthetic-voice-recipe",
    ) -> tuple[str, list[dict[str, Any]]]:
        """Create one voice batch over real synthetic messages bound to voice resources."""

        conversation_id = self._group()
        page = self.bridge.wechat_read_messages(
            mode="recent", conversation_id=conversation_id, projection="detail", limit=50
        )
        self._wait_for_reader_profile()
        selection = [
            VoiceSelectionItem(
                str(message["message_id"]),
                self._bind_voice_resource(str(message["message_id"]), index),
                "synthetic-revision",
                10_000,
            )
            for index, message in enumerate(page["messages"][:count])
        ]
        receipt = self.voice_service().create_batch(
            reader_id=self.config.reader_id,
            account_id=self._account_id(selection[0].message_id),
            account_binding_id=None,
            selection=selection,
            recipe_digest=recipe,
            recipe_json='{"engine":"synthetic"}',
        )
        self.assertTrue(receipt.created)
        assert receipt.reading_token is not None
        return receipt.reading_token, self.batch_items(receipt.reading_token)

    def _account_id(self, message_id: str) -> str:
        assert self.daemon is not None
        with self.daemon.database.connection() as connection:
            row = connection.execute(
                "SELECT account_id FROM messages WHERE message_id = ?", (message_id,)
            ).fetchone()
        assert row is not None
        return str(row["account_id"])

    def batch_items(self, token: str) -> list[dict[str, Any]]:
        return self.voice_service().repository.items(token)

    def job(self, job_id: str) -> dict[str, Any]:
        row = self.voice_service().repository.job(job_id)
        assert row is not None
        return row

    def release_recognizer(self) -> None:
        self.recognizer().release.set()

    def read_transcripts(
        self, token: str, *, cursor: str | None = None, wait_ms: int | None = 0
    ) -> dict[str, Any]:
        return self.bridge.wechat_read_transcripts(token, cursor=cursor, wait_ms=wait_ms)

    def follow(
        self,
        token: str,
        *,
        done,
        wait_ms: int = 0,
        timeout: float = POLL_TIMEOUT_SECONDS,
    ) -> tuple[list[list[Any]], dict[str, Any]]:
        """Follow the transcript cursor until the drained page satisfies ``done``."""

        rows: list[list[Any]] = []
        cursor: str | None = None
        deadline = time.monotonic() + timeout
        while True:
            page = self.read_transcripts(token, cursor=cursor, wait_ms=wait_ms)
            rows.extend(page.get("items", []))
            cursor = page.get("next_cursor")
            if not page.get("items") and done(page):
                return rows, page
            if time.monotonic() >= deadline:
                self.fail(f"transcript stream did not settle: {page.get('coverage')}")
            time.sleep(0.02)

    def ready(self, token: str, *, count: int) -> tuple[list[list[Any]], dict[str, Any]]:
        return self.follow(token, done=lambda page: page["coverage"]["ready"] >= count)

    def drained_cursor(self, token: str) -> str | None:
        _rows, page = self.follow(token, done=lambda _page: True)
        return page.get("next_cursor")

    def park_in_background(
        self, token: str, *, cursor: str | None, wait_ms: int = TRANSCRIPT_WAIT_MS
    ) -> tuple[threading.Thread, dict[str, Any]]:
        result: dict[str, Any] = {}

        def run() -> None:
            result["page"] = self.read_transcripts(token, cursor=cursor, wait_ms=wait_ms)

        thread = threading.Thread(target=run)
        thread.start()
        return thread, result

    def waiter_status(self) -> dict[str, Any]:
        return self.bridge.wechat_status("summary", response_profile="diagnostic")["runtime"][
            "transcript_waiters"
        ]

    def voice_worker_status(self) -> dict[str, Any]:
        return self.bridge.wechat_status("summary", response_profile="diagnostic")["runtime"][
            "voice_worker"
        ]

    @staticmethod
    def ready_rows(rows: list[list[Any]]) -> list[list[Any]]:
        return [row for row in rows if row[3] == "ready"]

    # -- delivery ----------------------------------------------------------

    def test_transcript_pages_replay_out_of_order_results_over_ipc(self) -> None:
        recognizer = self.recognizer()
        token, items = self.prepare_batch(count=2)
        self.wait_until(lambda: recognizer.call_count() >= 1)
        self.wait_until(
            lambda: any(self.job(str(row["job_id"]))["state"] == "leased" for row in items)
        )
        held = next(
            row for row in items if self.job(str(row["job_id"]))["state"] == "leased"
        )
        second = next(row for row in items if row["ordinal"] != held["ordinal"])
        service = self.voice_service()
        fence = service.lease(str(second["job_id"]), owner_id="synthetic-second-owner")
        service.complete(
            str(second["job_id"]),
            owner_id="synthetic-second-owner",
            fencing_token=fence,
            text="Synthetic transcript second",
        )
        self.release_recognizer()
        rows, page = self.ready(token, count=2)
        self.assertEqual(
            [row[4] for row in self.ready_rows(rows)],
            ["Synthetic transcript second", recognizer.text_for(1)],
        )
        self.assertEqual(
            [row[2] for row in self.ready_rows(rows)], [second["ordinal"], held["ordinal"]]
        )
        self.assertEqual(page["schema"], "sightglass.voice-page.v2")
        self.assertEqual(page["coverage"]["ready"], 2)
        self.assertTrue(page["processing_complete"])
        self.assertTrue(page["text_coverage_complete"])
        self.assertEqual(
            page["fields"],
            ["message_id", "resource_id", "ordinal", "state", "text", "error_code"],
        )
        self.assertEqual(page["derivation"], {"kind": "derived_transcript"})

    def test_transcript_reads_do_not_wait_when_results_exist(self) -> None:
        recognizer = self.recognizer()
        token, _items = self.prepare_batch()
        self.release_recognizer()
        rows, tail = self.ready(token, count=1)
        self.assertEqual(self.ready_rows(rows)[0][4], recognizer.text_for(1))
        self.assertEqual(tail["wait"]["state"], "complete")
        self.assertEqual(tail["wait"]["elapsed_ms"], 0)
        self.assertEqual(tail["wait"]["requested_ms"], 0)

        started = time.monotonic()
        delivered = self.read_transcripts(token, cursor=None, wait_ms=TRANSCRIPT_WAIT_MS)
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertEqual(delivered["wait"]["state"], "delivered")
        self.assertEqual(delivered["wait"]["requested_ms"], TRANSCRIPT_WAIT_MS)
        self.assertEqual(delivered["wait"]["elapsed_ms"], 0)
        self.assertEqual(len(delivered["items"]), 1)
        self.assertEqual(delivered["items"][0][3], "pending")
        self.assertIsNone(delivered["items"][0][4])
        self.assertEqual(self.waiter_status()["active_waiters"], 0)

    def test_transcript_wait_parks_then_wakes_on_a_new_event(self) -> None:
        recognizer = self.recognizer()
        token, _items = self.prepare_batch()
        self.wait_until(lambda: recognizer.call_count() >= 1)
        cursor = self.drained_cursor(token)
        thread, result = self.park_in_background(token, cursor=cursor)
        try:
            self.wait_until(lambda: self.waiter_status()["active_waiters"] == 1)
            started = time.monotonic()
            self.release_recognizer()
            thread.join(timeout=POLL_TIMEOUT_SECONDS)
            self.assertFalse(thread.is_alive())
            elapsed = time.monotonic() - started
        finally:
            self.release_recognizer()
        page = result["page"]
        self.assertEqual(page["wait"]["state"], "woken")
        self.assertLess(elapsed, 4.0)
        self.assertEqual(
            [row[4] for row in self.ready_rows(page["items"])], [recognizer.text_for(1)]
        )
        self.assertEqual(self.waiter_status()["active_waiters"], 0)
        self.assertEqual(self.waiter_status()["woken_count"], 1)

    def test_transcript_wait_capacity_is_bounded_and_reported(self) -> None:
        recognizer = self.recognizer()
        token, _items = self.prepare_batch()
        self.wait_until(lambda: recognizer.call_count() >= 1)
        cursor = self.drained_cursor(token)
        parked = [self.park_in_background(token, cursor=cursor) for _ in range(2)]
        try:
            self.wait_until(lambda: self.waiter_status()["active_waiters"] == 2)
            started = time.monotonic()
            page = self.read_transcripts(token, cursor=cursor, wait_ms=TRANSCRIPT_WAIT_MS)
            elapsed = time.monotonic() - started
            self.assertEqual(page["wait"]["state"], "capacity_exhausted")
            self.assertFalse(page["wait"]["waiter_available"])
            self.assertEqual(page["wait"]["max_waiters"], 2)
            self.assertEqual(page["wait"]["active_waiters"], 2)
            self.assertEqual(page["wait"]["retry_after_ms"], 250)
            self.assertLess(elapsed, 1.0)
            self.assertEqual(page["schema"], "sightglass.voice-page.v2")
            self.assertEqual(page["coverage"]["pending"], 1)
            self.assertEqual(page["items"], [])
        finally:
            self.release_recognizer()
            for thread, _result in parked:
                thread.join(timeout=POLL_TIMEOUT_SECONDS)
        self.assertTrue(all(not thread.is_alive() for thread, _result in parked))
        self.assertEqual(self.waiter_status()["active_waiters"], 0)
        self.assertEqual(self.waiter_status()["rejected_count"], 1)

    def test_status_and_reader_tools_stay_responsive_while_parked(self) -> None:
        recognizer = self.recognizer()
        token, _items = self.prepare_batch()
        self.wait_until(lambda: recognizer.call_count() >= 1)
        cursor = self.drained_cursor(token)
        thread, _result = self.park_in_background(token, cursor=cursor)
        try:
            self.wait_until(lambda: self.waiter_status()["active_waiters"] == 1)
            started = time.monotonic()
            status = self.bridge.wechat_status("summary", response_profile="diagnostic")
            elapsed = time.monotonic() - started
            self.assertLess(elapsed, 1.0)
            self.assertEqual(status["schema"], "sightglass.status.v1")
            self.assertEqual(status["runtime"]["transcript_waiters"]["active_waiters"], 1)
            self.assertEqual(status["runtime"]["operations"]["active_count"], 0)
            self.assertEqual(status["runtime"]["voice_worker"]["enabled"], True)
            conversations = self.bridge.wechat_find_conversations("Synthetic Group")
            self.assertTrue(conversations["candidates"])
            search = self.bridge.wechat_search_messages(query="消息", limit=1)
            self.assertEqual(search["state"], "preparing")
            token = search["reading_token"]
            self.wait_until(lambda: self.bridge.wechat_search_messages(
                query="消息", limit=1, cursor=token,
            ).get("schema") == "sightglass.search-results.v2")
        finally:
            self.release_recognizer()
            thread.join(timeout=POLL_TIMEOUT_SECONDS)
        self.assertFalse(thread.is_alive())

    # -- policy ------------------------------------------------------------

    def test_pause_and_deny_wake_the_waiter_and_block_the_token(self) -> None:
        recognizer = self.recognizer()
        token, _items = self.prepare_batch()
        self.wait_until(lambda: recognizer.call_count() >= 1)
        cursor = self.drained_cursor(token)
        thread, result = self.park_in_background(token, cursor=cursor)
        try:
            self.wait_until(lambda: self.waiter_status()["active_waiters"] == 1)
            started = time.monotonic()
            self.operator.call("operator.pause")
            thread.join(timeout=POLL_TIMEOUT_SECONDS)
            self.assertFalse(thread.is_alive())
            self.assertLess(time.monotonic() - started, 4.0)
            self.assertEqual(result["page"]["schema"], "sightglass.error.v1")
            self.assertEqual(result["page"]["code"], "SERVICE_PAUSED")

            paused = self.read_transcripts(token)
            self.assertEqual(paused["code"], "SERVICE_PAUSED")
            self.operator.call("operator.resume")
            conversation_id = self._group()
            self.operator.call("operator.policy.deny", {"conversation_id": conversation_id})
            denied = self.read_transcripts(token)
            self.assertEqual(denied["schema"], "sightglass.error.v1")
            self.assertEqual(denied["code"], "POLICY_DENIED")
            self.operator.call(
                "operator.policy.clear_deny", {"conversation_id": conversation_id}
            )
            allowed = self.read_transcripts(token)
            self.assertEqual(allowed["schema"], "sightglass.voice-page.v2")
            self.assertEqual(allowed["wait"]["state"], "delivered")
        finally:
            self.release_recognizer()

    # -- lifecycle ---------------------------------------------------------

    def test_reload_fences_out_a_late_result_from_the_previous_context(self) -> None:
        recognizer = self.recognizer()
        token, items = self.prepare_batch()
        self.wait_until(lambda: recognizer.call_count() >= 1)
        assert self.daemon is not None
        previous_worker = self.daemon.voice_worker
        job_id = str(items[0]["job_id"])
        previous_fence = int(self.job(job_id)["fencing_token"] or 0)
        self.operator.call("operator.resume")
        self.wait_until(lambda: recognizer.call_count() >= 2)
        self.assertFalse(previous_worker.status().running)
        self.assertGreater(int(self.job(job_id)["fencing_token"]), previous_fence)
        self.release_recognizer()
        rows, page = self.ready(token, count=1)
        self.assertEqual([row[4] for row in self.ready_rows(rows)], [recognizer.text_for(2)])
        self.assertEqual(page["coverage"]["ready"], 1)
        self.assertEqual(previous_worker.status().completed_count, 0)
        self.assertEqual(self.job(job_id)["state"], "ready")
        with self.assertRaises(SightglassError) as caught:
            self.voice_service().complete(
                job_id,
                owner_id=previous_worker.owner_id,
                fencing_token=previous_fence,
                text="Synthetic late transcript",
            )
        self.assertEqual(caught.exception.code, ErrorCode.CURSOR_STALE)
        self.assertEqual(self.job(job_id)["state"], "ready")

    def test_client_disconnect_releases_only_the_waiter(self) -> None:
        recognizer = self.recognizer()
        token, _items = self.prepare_batch()
        self.wait_until(lambda: recognizer.call_count() >= 1)
        cursor = self.drained_cursor(token)
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(POLL_TIMEOUT_SECONDS)
        connection.connect(str(self.config.socket_path))
        started = 0.0
        try:
            send_frame(
                connection,
                {
                    "version": IPC_VERSION,
                    "id": "synthetic-disconnect",
                    "token": self.reader_token,
                    "method": "tools.call",
                    "params": {
                        "name": "wechat_read_transcripts",
                        "arguments": {
                            "reading_token": token,
                            "cursor": cursor,
                            "wait_ms": TRANSCRIPT_WAIT_MS,
                        },
                    },
                },
            )
            self.wait_until(lambda: self.waiter_status()["active_waiters"] == 1)
            started = time.monotonic()
            connection.close()
            self.wait_until(lambda: self.waiter_status()["active_waiters"] == 0, timeout=4.0)
        finally:
            connection.close()
            self.release_recognizer()
        self.assertLess(time.monotonic() - started, 4.0)
        self.assertEqual(self.waiter_status()["aborted_count"], 1)
        self.assertEqual(self.bridge.wechat_status()["schema"], "sightglass.status.v1")
        self.assertTrue(self.bridge.wechat_find_conversations("Synthetic Group")["candidates"])

    def test_restart_recovers_an_abandoned_lease_and_redelivers(self) -> None:
        recognizer = self.recognizer()
        token, items = self.prepare_batch()
        self.wait_until(lambda: recognizer.call_count() >= 1)
        job_id = str(items[0]["job_id"])
        self.assertEqual(self.job(job_id)["state"], "leased")
        self.restart_daemon()
        self.wait_until(lambda: recognizer.call_count() >= 2)
        self.release_recognizer()
        rows, page = self.ready(token, count=1)
        self.assertEqual([row[4] for row in self.ready_rows(rows)], [recognizer.text_for(2)])
        self.assertEqual(page["coverage"]["ready"], 1)
        self.assertEqual(self.job(job_id)["attempt"], 2)
        self.assertEqual(self.job(job_id)["state"], "ready")

    def test_startup_takes_over_a_live_lease_from_a_crashed_context(self) -> None:
        """A dead context's unexpired lease must not outlive the next startup."""

        self.stop_daemon()
        self.transcriber = None
        self.start_daemon()
        token, items = self.prepare_batch()
        job_id = str(items[0]["job_id"])
        self.stop_daemon()
        abandoned_fence = self.standalone_voice_service().lease(
            job_id, owner_id="synthetic-crashed-worker", lease_seconds=600
        )
        self.assertEqual(self.job(job_id)["state"], "leased")
        self.assertGreater(
            parse_aware_datetime(str(self.job(job_id)["lease_expires_at"])), utc_now()
        )
        self.transcriber = SyntheticTranscriber()
        self.start_daemon()
        self.wait_until(lambda: self.recognizer().call_count() >= 1)
        self.assertEqual(self.job(job_id)["state"], "leased")
        self.assertGreater(int(self.job(job_id)["fencing_token"]), abandoned_fence)
        self.release_recognizer()
        rows, page = self.ready(token, count=1)
        self.assertEqual(self.ready_rows(rows)[0][4], self.recognizer().text_for(1))
        self.assertEqual(page["coverage"]["ready"], 1)
        self.assertEqual(self.job(job_id)["attempt"], 2)
        with self.assertRaises(SightglassError) as caught:
            self.standalone_voice_service().complete(
                job_id,
                owner_id="synthetic-crashed-worker",
                fencing_token=abandoned_fence,
                text="Synthetic late transcript",
            )
        self.assertEqual(caught.exception.code, ErrorCode.CURSOR_STALE)

    def test_disabled_worker_reports_unavailable_without_parking(self) -> None:
        self.stop_daemon()
        self.transcriber = None
        self.start_daemon()
        token, items = self.prepare_batch()
        self.assertEqual(self.job(str(items[0]["job_id"]))["state"], "pending")
        status = self.voice_worker_status()
        self.assertFalse(status["enabled"])
        self.assertFalse(status["running"])
        cursor = self.drained_cursor(token)
        started = time.monotonic()
        page = self.read_transcripts(token, cursor=cursor, wait_ms=TRANSCRIPT_WAIT_MS)
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertEqual(page["schema"], "sightglass.voice-page.v2")
        self.assertEqual(page["wait"]["state"], "unavailable")
        self.assertFalse(page["wait"]["voice_worker_enabled"])
        self.assertEqual(page["items"], [])
        self.assertEqual(page["coverage"]["pending"], 1)
        self.assertEqual(self.waiter_status()["active_waiters"], 0)

    # -- recognizer outcomes ----------------------------------------------

    def test_transient_recognizer_failure_backs_off_before_succeeding(self) -> None:
        recognizer = self.recognizer()
        recognizer.behavior = "transient"
        token, items = self.prepare_batch()
        self.wait_until(lambda: recognizer.call_count() >= 1)
        recognizer.behavior = "gate"
        self.release_recognizer()
        rows, page = self.ready(token, count=1)
        self.assertEqual(self.ready_rows(rows)[0][4], recognizer.text_for(2))
        assert self.daemon is not None
        worker = self.daemon.voice_worker.status()
        self.assertEqual(worker.retry_count, 1)
        self.assertEqual(worker.completed_count, 1)
        self.assertEqual(worker.failed_count, 0)
        self.assertEqual(self.job(str(items[0]["job_id"]))["attempt"], 2)
        self.assertEqual(page["coverage"]["ready"], 1)

    def test_retryable_recognizer_failure_stops_at_the_attempt_limit(self) -> None:
        recognizer = self.recognizer()
        recognizer.behavior = "transient"
        token, _items = self.prepare_batch()
        started = time.monotonic()
        _rows, page = self.follow(
            token, done=lambda candidate: candidate["coverage"]["failed"] == 1
        )
        elapsed = time.monotonic() - started
        self.assertGreater(elapsed, 1.0)
        self.assertEqual(page["coverage"]["failed"], 1)
        failed_events = [row for row in _rows if row[3] == "failed"]
        self.assertEqual(failed_events[-1][5], "SERVICE_UNAVAILABLE")
        self.assertEqual(page["coverage"]["pending"], 0)
        self.assertTrue(page["processing_complete"])
        assert self.daemon is not None
        worker = self.daemon.voice_worker.status()
        self.assertEqual(worker.retry_count, 2)
        self.assertEqual(worker.failed_count, 1)
        self.assertEqual(worker.historical_error_code, "SERVICE_UNAVAILABLE")
        self.assertEqual(recognizer.call_count(), 3)
        time.sleep(0.6)
        self.assertEqual(recognizer.call_count(), 3)
        self.assertEqual(self.voice_service().repository.pending_jobs(8), [])

    def test_blocked_recognizer_result_is_never_retried(self) -> None:
        recognizer = self.recognizer()
        recognizer.behavior = "blocked"
        token, items = self.prepare_batch()
        _rows, page = self.follow(
            token, done=lambda candidate: candidate["coverage"]["blocked"] == 1
        )
        self.assertEqual(page["coverage"]["blocked"], 1)
        self.assertTrue(page["processing_complete"])
        job_id = str(items[0]["job_id"])
        self.assertEqual(self.job(job_id)["state"], "blocked")
        self.assertEqual(self.job(job_id)["error_code"], "RESOURCE_BLOCKED")
        assert self.daemon is not None
        self.assertEqual(self.daemon.voice_worker.status().blocked_count, 1)

        time.sleep(0.5)
        self.assertEqual(recognizer.call_count(), 1)
        replayed = self.read_transcripts(token, cursor=None, wait_ms=0)
        self.assertEqual(replayed["schema"], "sightglass.voice-page.v2")
        self.assertEqual(replayed["wait"]["state"], "delivered")
        replayed_rows, tail = self.follow(token, done=lambda _page: True)
        blocked_events = [row for row in replayed_rows if row[3] == "blocked"]
        self.assertEqual(blocked_events[-1][5], "RESOURCE_BLOCKED")
        self.assertEqual(tail["coverage"]["blocked"], 1)
        self.assertEqual(recognizer.call_count(), 1)
        self.assertEqual(self.job(job_id)["state"], "blocked")
        self.assertEqual(self.voice_service().repository.pending_jobs(8), [])
        self.assertEqual(self.voice_service().repository.outstanding_leases(), [])

    def test_empty_transcript_is_reported_as_coverage_empty(self) -> None:
        recognizer = self.recognizer()
        recognizer.behavior = "empty"
        token, _items = self.prepare_batch()
        rows, page = self.follow(
            token, done=lambda candidate: candidate["coverage"]["empty"] == 1
        )
        self.assertEqual(page["coverage"]["empty"], 1)
        self.assertEqual(page["coverage"]["ready"], 0)
        self.assertTrue(page["processing_complete"])
        self.assertFalse(page["text_coverage_complete"])
        empty_rows = [row for row in rows if row[3] == "empty"]
        self.assertEqual(len(empty_rows), 1)
        self.assertEqual(empty_rows[0][4], "")
        self.assertEqual(recognizer.call_count(), 1)


if __name__ == "__main__":
    unittest.main()

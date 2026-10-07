from __future__ import annotations

import hashlib
import io
import json
import os
import selectors
import signal
import sqlite3
import struct
import subprocess
import sys
import tempfile
import threading
import unittest
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest import mock

from sightglass.contracts.capture import (
    MAX_CAPTURE_METADATA_BYTES,
    CaptureAck,
    CaptureCeiling,
    CaptureCoverage,
    CaptureDocument,
    CaptureExpectation,
    CaptureOperation,
    CaptureProtocolError,
    CaptureRequest,
    CaptureStreamPosition,
)
from sightglass.contracts.common import utc_now
from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.runtime.edge import EDGE_RECOVERY_STATE_SCHEMA, EdgeSpool
from sightglass.runtime.edge_relay import FramedStream, RelayDisconnected, SSHRelayConnector
from sightglass.source.base import SourceScope
from sightglass.source.capture.codec import (
    SealedCapture,
    canonical_json,
    typed_value,
    validate_capture,
)
from sightglass.source.capture.executor import CaptureExecutor
from sightglass.source.capture.frozen import FrozenCaptureProvider
from sightglass.source.capture.receiver import CaptureReceiver
from sightglass.source.capture.resource import resource_descriptor_digest
from sightglass.source.direct_wechat import SyntheticSourceProvider
from sightglass.source.identity import opaque_id
from sightglass.source.synthetic import create_synthetic_source

_CRASH_CHILD_CODE = r"""
import os
import sys
import threading
from pathlib import Path

import sightglass.runtime.edge as edge
from sightglass.contracts.capture import CaptureAck
from sightglass.source.capture.codec import SealedCapture, strict_json, typed_value

phase, directory, envelope_file, ack_file, instance, account, origin = sys.argv[1:]
directory = Path(directory)
spool = edge.EdgeSpool(
    directory, source_instance_id=instance, account_id=account, origin_epoch=origin
)
envelope = SealedCapture.from_bytes(Path(envelope_file).read_bytes())

def checkpoint():
    print("checkpoint:" + phase, flush=True)
    threading.Event().wait()

if phase == "staging_write":
    real_fdopen = os.fdopen
    class PartialWrite:
        def __init__(self, stream):
            self.stream = stream
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return self.stream.__exit__(*args)
        def write(self, data):
            self.stream.write(data[:max(1, len(data) // 2)])
            self.stream.flush()
            checkpoint()
        def __getattr__(self, name):
            return getattr(self.stream, name)
    os.fdopen = lambda descriptor, mode: PartialWrite(real_fdopen(descriptor, mode))
elif phase == "file_fsync":
    real_fsync = os.fsync
    def fsync(descriptor):
        real_fsync(descriptor)
        staging = directory / "pending.staging"
        if staging.exists():
            current, target = os.fstat(descriptor), staging.stat()
            if (current.st_dev, current.st_ino) == (target.st_dev, target.st_ino):
                checkpoint()
    os.fsync = fsync
elif phase == "rename":
    real_replace = os.replace
    def replace(source, destination):
        real_replace(source, destination)
        if Path(destination) == directory / "pending.capture":
            checkpoint()
    os.replace = replace
elif phase == "dir_fsync":
    real_sync_directory = edge._sync_directory
    def sync_directory(path):
        real_sync_directory(path)
        checkpoint()
    edge._sync_directory = sync_directory
elif phase in {"store_insert", "store_commit", "ack_delete", "ack_commit"}:
    class Connection:
        def __init__(self, connection):
            self.connection = connection
            self.store, self.ack = False, False
        def execute(self, sql, *args):
            result = self.connection.execute(sql, *args)
            if sql.startswith("INSERT INTO pending"):
                self.store = True
                if phase == "store_insert":
                    checkpoint()
            if sql.startswith("DELETE FROM pending"):
                self.ack = True
                if phase == "ack_delete":
                    checkpoint()
            return result
        def __enter__(self):
            self.connection.__enter__()
            return self
        def __exit__(self, *args):
            result = self.connection.__exit__(*args)
            if args[0] is None and (
                (phase == "store_commit" and self.store)
                or (phase == "ack_commit" and self.ack)
            ):
                checkpoint()
            return result
        def __getattr__(self, name):
            return getattr(self.connection, name)
    spool._connection = Connection(spool._connection)
elif phase == "ack_unlink":
    real_unlink = Path.unlink
    def unlink(path, *args, **kwargs):
        result = real_unlink(path, *args, **kwargs)
        if path == directory / "pending.capture":
            checkpoint()
        return result
    Path.unlink = unlink
else:
    raise RuntimeError("unsupported fixture crash phase")

if phase.startswith("ack_"):
    spool.acknowledge(typed_value(CaptureAck, strict_json(Path(ack_file).read_bytes())))
else:
    spool.store(envelope)
raise RuntimeError("fixture crash checkpoint was not reached")
"""


class FixtureReceiveJournal:
    """One generated DB transaction proves the integration hook's atomic boundary."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._local = threading.local()
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.executescript("""
                CREATE TABLE stream(instance TEXT, account TEXT, epoch TEXT, next_sequence INTEGER,
                    PRIMARY KEY(instance,account));
                CREATE TABLE receipts(batch_id TEXT PRIMARY KEY, ack TEXT NOT NULL);
                CREATE TABLE episodes(sequence INTEGER PRIMARY KEY, body TEXT NOT NULL);
                CREATE TABLE reader_state(singleton INTEGER PRIMARY KEY, position INTEGER);
                INSERT INTO reader_state VALUES(1,0);
            """)

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        current = getattr(self._local, "connection", None)
        if current is not None:
            yield current
        else:
            connection = sqlite3.connect(self.path)
            try:
                yield connection
            finally:
                connection.close()

    @contextmanager
    def transaction(self) -> Iterator[object]:
        with self.connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._local.connection = connection
            try:
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            finally:
                self._local.connection = None

    def stream_position(
        self, source_instance_id: str, account_id: str
    ) -> CaptureStreamPosition | None:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT epoch,next_sequence FROM stream WHERE instance=? AND account=?",
                (source_instance_id, account_id),
            ).fetchone()
        return CaptureStreamPosition(str(row[0]), int(row[1])) if row else None

    def batch_receipt(self, batch_id: str) -> CaptureAck | None:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT ack FROM receipts WHERE batch_id=?", (batch_id,)
            ).fetchone()
        return typed_value(CaptureAck, json.loads(row[0])) if row else None

    def record_terminal(self, document: CaptureDocument, ack: CaptureAck) -> None:
        connection = getattr(self._local, "connection", None)
        if connection is None:
            raise AssertionError("terminal receive must share admission transaction")
        connection.execute(
            "INSERT INTO receipts VALUES(?,?)", (ack.batch_id, canonical_json(ack).decode())
        )
        connection.execute(
            "INSERT INTO stream VALUES(?,?,?,?) ON CONFLICT(instance,account) DO UPDATE SET "
            "epoch=excluded.epoch,next_sequence=excluded.next_sequence",
            (
                document.origin.source_instance_id,
                document.request.account_id,
                ack.stream_epoch,
                ack.sequence + 1,
            ),
        )

    def append(self, provider: FrozenCaptureProvider, document: CaptureDocument) -> None:
        connection = self._local.connection
        with provider.snapshot() as snapshot:
            message = provider.get_message(
                document.request.account_id,
                document.evidence.messages[0].source_message_id,
                snapshot,
            )
        assert message is not None
        connection.execute(
            "INSERT INTO episodes VALUES(?,?)", (document.origin.sequence, message.raw_content)
        )
        connection.execute(
            "UPDATE reader_state SET position=? WHERE singleton=1", (document.origin.sequence,)
        )


class CaptureBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = create_synthetic_source(self.root / "source")
        self.provider = SyntheticSourceProvider(self.source)
        self.ceiling = CaptureCeiling(
            "synthetic-account-demo", frozenset({"conv_group"}), "fixture-egress-1"
        )
        self.executor = CaptureExecutor(
            self.provider, self.ceiling, source_instance_id="fixture-capture-instance"
        )
        self.expected = CaptureExpectation(
            self.ceiling.account_id,
            self.executor.source_instance_id,
            self.executor.origin_epoch,
            "fixture-policy-1",
            self.ceiling.revision,
            self.ceiling.conversations,
        )

    def request(self, operation: CaptureOperation = "recent", **kwargs: Any) -> CaptureRequest:
        return CaptureRequest(
            "fixture-request",
            operation,
            self.ceiling.account_id,
            "fixture-policy-1",
            conversation_source_id=None if operation == "catalog" else "conv_group",
            **kwargs,
        )

    def capture(self, request: CaptureRequest | None = None, sequence: int = 1) -> SealedCapture:
        return self.executor.capture(
            request or self.request(),
            stream_epoch="fixture-stream",
            sequence=sequence,
            batch_id=f"fixture-batch-{sequence}",
        )

    def spool(self, **kwargs: Any) -> EdgeSpool:
        spool = EdgeSpool.initialize(
            self.root / "spool",
            source_instance_id=self.executor.source_instance_id,
            account_id=self.ceiling.account_id,
            origin_epoch=self.executor.origin_epoch,
            stream_epoch="fixture-stream",
            **kwargs,
        )
        self.addCleanup(spool.close)
        return spool

    def test_catalog_is_filtered_before_serialization(self) -> None:
        envelope = self.capture(self.request("catalog"))
        document = validate_capture(envelope, self.expected)
        self.assertEqual(
            [item.source_conversation_id for item in document.evidence.conversations],
            ["conv_group"],
        )
        self.assertNotIn(b"conv_direct", envelope.metadata)
        self.assertNotIn(b"Demo Direct", envelope.metadata)
        self.assertTrue(document.receipt.coverage.catalog_complete)

    def test_full_operation_is_sealed_after_local_session_exit_and_replays_locally(self) -> None:
        exited = False
        original = self.provider.session

        @contextmanager
        def session(scope: SourceScope):
            nonlocal exited
            with original(scope) as snapshot:
                yield snapshot
            exited = True

        with mock.patch.object(self.provider, "session", side_effect=session):
            envelope = self.capture()
        self.assertTrue(exited)
        frozen = FrozenCaptureProvider(envelope)
        self.assertEqual(frozen.descriptor.implementation, self.provider.descriptor.implementation)
        self.assertEqual(frozen.origin_epoch, self.executor.origin_epoch)
        with mock.patch.object(
            self.provider, "session", side_effect=AssertionError("no source after seal")
        ):
            with frozen.session(
                SourceScope.conversation(self.ceiling.account_id, "conv_group")
            ) as snapshot:
                page = frozen.read_recent(self.ceiling.account_id, "conv_group", 100, snapshot)
                self.assertEqual(page.messages, envelope.document().evidence.messages)
                self.assertFalse(frozen.catalog_complete(snapshot))
                with self.assertRaises(CaptureProtocolError):
                    frozen.read_recent(self.ceiling.account_id, "conv_group", 201, snapshot)
                with self.assertRaises(CaptureProtocolError):
                    frozen.get_message(self.ceiling.account_id, "not-captured", snapshot)

    def test_range_context_verification_and_bounded_discovery(self) -> None:
        recent = self.capture(self.request(limit=3)).document()
        anchor = recent.evidence.messages[1]
        range_capture = self.capture(self.request("range", limit=2, before=anchor.sort_key))
        frozen = FrozenCaptureProvider(range_capture)
        with frozen.snapshot() as snapshot:
            page = frozen.read_range(
                self.ceiling.account_id,
                "conv_group",
                limit=2,
                direction="backward",
                before=anchor.sort_key,
                after=None,
                snapshot=snapshot,
            )
            self.assertTrue(all(item.sort_key < anchor.sort_key for item in page.messages))
        context = self.capture(
            self.request(
                "context",
                focus_source_message_id=anchor.source_message_id,
                context_before=1,
                context_after=1,
            )
        ).document()
        self.assertIn(
            anchor.source_message_id, [item.source_message_id for item in context.evidence.messages]
        )
        verification = self.capture(
            self.request("verify", message_ids=(anchor.source_message_id, "not-found"))
        )
        self.assertEqual(verification.document().evidence.missing_message_ids, ("not-found",))
        discovery = self.capture(self.request("discovery", limit=2)).document()
        self.assertEqual(discovery.receipt.coverage.kind, "discovery")
        self.assertLessEqual(len(discovery.evidence.messages), 2)
        self.assertGreater(discovery.receipt.coverage.scanned_rows, 0)
        self.assertTrue(discovery.receipt.coverage.discovery_has_more)

    def test_multi_conversation_verify_and_empty_scope_are_one_batch(self) -> None:
        ceiling = replace(self.ceiling, conversations=frozenset({"conv_group", "conv_direct"}))
        executor = CaptureExecutor(
            self.provider, ceiling, source_instance_id=self.executor.source_instance_id
        )
        request = CaptureRequest(
            "fixture-multi-request",
            "verify",
            self.ceiling.account_id,
            "fixture-policy-1",
            conversation_source_ids=("conv_group", "conv_direct"),
            message_ids=("source-msg-001", "source-msg-002"),
        )
        with mock.patch.object(self.provider, "session", wraps=self.provider.session) as session:
            envelope = executor.capture(request, stream_epoch="fixture-stream", sequence=1)
        self.assertEqual(session.call_count, 1)
        document = validate_capture(
            envelope, replace(self.expected, conversations=ceiling.conversations)
        )
        self.assertEqual(
            {item.source_conversation_id for item in document.evidence.messages},
            {"conv_group", "conv_direct"},
        )
        self.assertEqual(len(document.evidence.conversations), 2)
        frozen = FrozenCaptureProvider(envelope)
        with frozen.session(
            SourceScope.conversation(self.ceiling.account_id, "conv_group")
        ) as snapshot:
            self.assertIsNotNone(
                frozen.get_message(self.ceiling.account_id, "source-msg-001", snapshot)
            )
            with self.assertRaises(SightglassError) as caught:
                frozen.get_message(self.ceiling.account_id, "source-msg-002", snapshot)
            self.assertEqual(caught.exception.code, ErrorCode.CONVERSATION_NOT_FOUND)
        empty = executor.capture(
            replace(request, message_ids=()), stream_epoch="fixture-stream", sequence=1
        )
        self.assertEqual(empty.document().receipt.terminal, "complete")
        self.assertEqual(empty.document().evidence.messages, ())
        self.assertEqual(len(empty.document().evidence.conversations), 2)
        with self.assertRaises(CaptureProtocolError):
            replace(request, conversation_source_ids=())

    def test_range_context_keeps_base_pagination_separate_from_neighbors(self) -> None:
        request = self.request("range", limit=2, context_before=1, context_after=1)
        envelope = self.capture(request)
        document = envelope.document()
        self.assertEqual(document.receipt.terminal, "complete", document.receipt.reason)
        self.assertEqual(len(document.evidence.range_page_message_ids), 2)
        frozen = FrozenCaptureProvider(envelope)
        with frozen.snapshot() as snapshot:
            page = frozen.read_range(
                self.ceiling.account_id,
                "conv_group",
                after=None,
                before=None,
                direction="backward",
                limit=2,
                snapshot=snapshot,
            )
            self.assertEqual(
                tuple(item.source_message_id for item in page.messages),
                document.evidence.range_page_message_ids,
            )
            for focus in page.messages:
                neighbors = frozen.read_range(
                    self.ceiling.account_id,
                    "conv_group",
                    after=None,
                    before=focus.sort_key,
                    direction="backward",
                    limit=1,
                    snapshot=snapshot,
                )
                self.assertTrue(all(item.sort_key < focus.sort_key for item in neighbors.messages))
                with self.assertRaises(CaptureProtocolError):
                    frozen.read_range(
                        self.ceiling.account_id,
                        "conv_group",
                        after=None,
                        before=focus.sort_key,
                        direction="backward",
                        limit=2,
                        snapshot=snapshot,
                    )
        with self.assertRaises(CaptureProtocolError):
            self.request("range", limit=100, context_before=1, context_after=1)

    def test_range_cursor_boundary_is_verified_without_entering_the_base_page(self) -> None:
        with self.provider.snapshot() as snapshot:
            boundary = self.provider.get_message(
                self.ceiling.account_id, "source-msg-001", snapshot
            )
        assert boundary is not None
        request = self.request(
            "range",
            after=boundary.sort_key,
            direction="forward",
            limit=2,
            message_ids=(boundary.source_message_id, "synthetic-missing-boundary"),
        )
        envelope = self.capture(request)
        document = envelope.document()
        self.assertEqual(document.receipt.terminal, "complete", document.receipt.reason)
        self.assertNotIn(boundary.source_message_id, document.evidence.range_page_message_ids)
        self.assertEqual(document.evidence.missing_message_ids, ("synthetic-missing-boundary",))
        frozen = FrozenCaptureProvider(envelope)
        with frozen.snapshot() as snapshot:
            self.assertEqual(
                frozen.get_message(self.ceiling.account_id, boundary.source_message_id, snapshot),
                boundary,
            )
            self.assertIsNone(
                frozen.get_message(self.ceiling.account_id, "synthetic-missing-boundary", snapshot)
            )
            page = frozen.read_range(
                self.ceiling.account_id,
                "conv_group",
                after=request.after,
                before=None,
                direction="forward",
                limit=2,
                snapshot=snapshot,
            )
            self.assertEqual(len(page.messages), 2)
            self.assertEqual(
                tuple(item.source_message_id for item in page.messages),
                document.evidence.range_page_message_ids,
            )
        with self.assertRaises(CaptureProtocolError):
            self.request("range", limit=200, message_ids=(boundary.source_message_id,))

    def test_corrupt_unsealed_mismatch_expired_and_duplicate_fields_fail_closed(self) -> None:
        envelope = self.capture()
        with self.assertRaises(CaptureProtocolError):
            SealedCapture(envelope.metadata.replace(b"Synthetic Owner", b"Altered___Owner"))
        with self.assertRaises(CaptureProtocolError):
            SealedCapture(canonical_json({"schema": "sightglass.capture.v1"}))
        with self.assertRaises(CaptureProtocolError):
            validate_capture(envelope, replace(self.expected, policy_revision="other-policy"))
        with self.assertRaises(CaptureProtocolError):
            validate_capture(
                envelope, self.expected, request=replace(self.request(), request_id="other-request")
            )
        with self.assertRaisesRegex(CaptureProtocolError, "fresh_receipt_expired"):
            validate_capture(
                envelope, self.expected, now=(utc_now() + timedelta(hours=1)).isoformat()
            )
        with self.assertRaises(CaptureProtocolError):
            SealedCapture(b'{"schema":"x","schema":"y"}')
        with self.assertRaises(CaptureProtocolError):
            SealedCapture.from_bytes(envelope.to_bytes()[:-1])

    def test_denied_scope_and_cancelled_operation_carry_no_coverage(self) -> None:
        denied = self.capture(
            replace(self.request(), conversation_source_id="conv_direct")
        ).document()
        self.assertEqual(denied.receipt.terminal, "rejected")
        self.assertEqual(denied.receipt.coverage, CaptureCoverage("none"))
        self.assertEqual(denied.evidence.messages, ())
        cancelled = threading.Event()
        cancelled.set()
        document = self.executor.capture(
            self.request(), stream_epoch="fixture-stream", sequence=1, cancelled=cancelled
        ).document()
        self.assertEqual(document.receipt.terminal, "cancelled")
        self.assertEqual(document.receipt.coverage, CaptureCoverage("none"))

    def test_terminal_recovery_preserves_cancelled_receipt_and_releases_no_coverage(self) -> None:
        cancelled = threading.Event()
        cancelled.set()
        envelope = self.executor.capture(
            self.request(), stream_epoch="fixture-stream", sequence=1, cancelled=cancelled
        )
        spool = self.spool()
        spool.store(envelope)
        journal = FixtureReceiveJournal(self.root / "receive.sqlite")
        receiver = CaptureReceiver(journal, self.expected, stream_epoch="fixture-stream")
        ack = receiver.admit(
            receiver.prepare(envelope),
            None,
            receipt_id="fixture-cancelled-terminal",
            reject=True,
            now=(utc_now() + timedelta(hours=1)).isoformat(),
        )
        self.assertEqual(ack.terminal, "cancelled")
        spool.acknowledge(ack)
        self.assertIsNone(spool.pending())
        with journal.connection() as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM episodes").fetchone()[0], 0)

    def test_source_change_during_session_cannot_seal_success(self) -> None:
        original = self.provider.read_recent

        def mutate(*args: Any, **kwargs: Any):
            page = original(*args, **kwargs)
            with closing(sqlite3.connect(self.source / "messages-1.db")) as connection, connection:
                connection.execute(
                    "UPDATE messages SET raw_content='fixture correction' "
                    "WHERE source_message_id='source-msg-001'"
                )
            return page

        with mock.patch.object(self.provider, "read_recent", side_effect=mutate):
            document = self.capture().document()
        self.assertEqual(document.receipt.terminal, "rejected")
        self.assertEqual(document.receipt.reason, "SOURCE_GENERATION_CHANGED")
        self.assertEqual(document.evidence.messages, ())

    def test_oversized_body_is_terminal_rejection_without_truncation(self) -> None:
        with closing(sqlite3.connect(self.source / "messages-1.db")) as connection, connection:
            connection.execute(
                "UPDATE messages SET raw_content=? WHERE source_message_id='source-msg-001'",
                ("x" * MAX_CAPTURE_METADATA_BYTES,),
            )
        document = self.capture().document()
        self.assertEqual(document.receipt.terminal, "rejected")
        self.assertEqual(document.receipt.reason, "metadata_size_invalid")
        self.assertEqual(document.evidence.messages, ())

    def resource_request(self) -> CaptureRequest:
        with self.provider.snapshot() as snapshot:
            message = self.provider.get_message(self.ceiling.account_id, "source-msg-004", snapshot)
        assert message is not None
        descriptor = message.resources[0]
        message_id = opaque_id(
            "wxmsg", opaque_id("wxacct", self.ceiling.account_id), message.source_message_id
        )
        revision = {
            "resource_id": opaque_id("wxres", message_id, descriptor.source_resource_key),
            "message_id": message_id,
            "source_resource_key": descriptor.source_resource_key,
            "availability": descriptor.availability,
            "resolver_json": '{"active":true}',
            "kind": descriptor.kind,
            "mime_type": descriptor.mime_type,
            "declared_size": descriptor.declared_size,
            "declared_hash": descriptor.declared_hash,
        }
        revision_json = json.dumps(revision, sort_keys=True, separators=(",", ":"))
        return self.request(
            "resource",
            focus_source_message_id=message.source_message_id,
            resource_key=descriptor.source_resource_key,
            resource_descriptor=descriptor,
            resource_descriptor_digest=resource_descriptor_digest(descriptor),
            resource_revision_json=revision_json,
            expected_resource_revision=hashlib.sha256(revision_json.encode()).hexdigest(),
        )

    def test_exact_resource_one_session_full_digest_and_edge_image_decode(self) -> None:
        request = self.resource_request()
        with (
            mock.patch.object(
                self.provider, "get_message", side_effect=AssertionError("no owning hydration")
            ),
            mock.patch.object(self.provider, "session", wraps=self.provider.session) as session,
        ):
            envelope = self.capture(request)
        document = envelope.document()
        self.assertEqual(document.receipt.terminal, "complete", document.receipt.reason)
        self.assertEqual(session.call_count, 1)
        self.assertTrue(envelope.resource.startswith(b"\x89PNG"))
        self.assertNotIn(b"image_aes_key", envelope.metadata)
        self.assertNotIn(str(self.root).encode(), envelope.metadata)
        with self.assertRaises(CaptureProtocolError):
            SealedCapture(envelope.metadata, envelope.resource + b"corruption")
        frozen = FrozenCaptureProvider(envelope)
        with frozen.session(SourceScope.resource(request.resource_key or "")) as snapshot:
            self.assertEqual(
                frozen.read_resource(
                    request.resource_key or "", max_bytes=32 * 1024 * 1024, snapshot=snapshot
                ).data,
                envelope.resource,
            )
        denied = self.capture(replace(request, conversation_source_id="conv_direct")).document()
        self.assertEqual(denied.receipt.terminal, "rejected")

    def test_spool_restart_replays_exact_bytes_and_only_matching_durable_ack_releases(self) -> None:
        spool = self.spool()
        envelope = self.capture()
        spool.store(envelope)
        self.assertEqual(spool.next_sequence, 2)
        spool.close()
        restarted = EdgeSpool(
            self.root / "spool",
            source_instance_id=self.executor.source_instance_id,
            account_id=self.ceiling.account_id,
            origin_epoch=self.executor.origin_epoch,
        )
        self.addCleanup(restarted.close)
        pending = restarted.pending()
        assert pending is not None
        self.assertEqual(pending.to_bytes(), envelope.to_bytes())
        journal = FixtureReceiveJournal(self.root / "receive.sqlite")
        receiver = CaptureReceiver(journal, self.expected, stream_epoch="fixture-stream")
        ack = receiver.admit(
            receiver.prepare(envelope), journal.append, receipt_id="fixture-receipt"
        )
        with self.assertRaises(CaptureProtocolError):
            restarted.acknowledge(replace(ack, envelope_digest="0" * 64))
        self.assertIsNotNone(restarted.pending())
        restarted.acknowledge(ack)
        restarted.acknowledge(ack)
        self.assertIsNone(restarted.pending())
        self.assertEqual(restarted.next_sequence, 2)

    def test_sigkill_publication_and_ack_crash_windows_recover_without_sequence_gaps(self) -> None:
        envelope = self.capture()
        envelope_file = self.root / "fixture-envelope.capture"
        envelope_file.write_bytes(envelope.to_bytes())
        envelope_file.chmod(0o600)
        before_publication = {
            "staging_write", "file_fsync", "rename", "dir_fsync", "store_insert"
        }
        phases = (
            "staging_write", "file_fsync", "rename", "dir_fsync", "store_insert",
            "store_commit", "ack_delete", "ack_commit", "ack_unlink",
        )
        for phase in phases:
            with self.subTest(phase=phase):
                directory = self.root / f"crash-{phase}"
                spool = EdgeSpool.initialize(
                    directory,
                    source_instance_id=self.executor.source_instance_id,
                    account_id=self.ceiling.account_id,
                    origin_epoch=self.executor.origin_epoch,
                    stream_epoch="fixture-stream",
                )
                self.addCleanup(spool.close)
                ack_file = self.root / f"fixture-{phase}.ack"
                ack: CaptureAck | None = None
                if phase.startswith("ack_"):
                    spool.store(envelope)
                    journal = FixtureReceiveJournal(self.root / f"receive-{phase}.sqlite")
                    receiver = CaptureReceiver(
                        journal, self.expected, stream_epoch="fixture-stream"
                    )
                    ack = receiver.admit(
                        receiver.prepare(envelope), journal.append, receipt_id=f"fixture-{phase}"
                    )
                    ack_file.write_bytes(canonical_json(ack))
                    ack_file.chmod(0o600)
                spool.close()
                process = subprocess.Popen(
                    [
                        sys.executable, "-c", _CRASH_CHILD_CODE, phase, str(directory),
                        str(envelope_file), str(ack_file), self.executor.source_instance_id,
                        self.ceiling.account_id, self.executor.origin_epoch,
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                assert process.stdout is not None and process.stderr is not None
                try:
                    with selectors.DefaultSelector() as selector:
                        selector.register(process.stdout, selectors.EVENT_READ)
                        self.assertTrue(selector.select(timeout=10), "fixture checkpoint timed out")
                    self.assertEqual(
                        process.stdout.readline(), f"checkpoint:{phase}\n".encode()
                    )
                    process.kill()  # SIGKILL: child finally/close handlers cannot repair state.
                    remaining_output, errors = process.communicate(timeout=5)
                    self.assertEqual(process.returncode, -signal.SIGKILL)
                    self.assertEqual(remaining_output, b"")
                    self.assertEqual(errors, b"")
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.wait(timeout=5)
                    process.stdout.close()
                    process.stderr.close()
                with EdgeSpool(
                    directory,
                    source_instance_id=self.executor.source_instance_id,
                    account_id=self.ceiling.account_id,
                    origin_epoch=self.executor.origin_epoch,
                ) as recovered:
                    self.assertEqual(recovered.stream_epoch, "fixture-stream")
                    self.assertFalse(recovered.epoch_lost)
                    self.assertEqual(
                        recovered.next_sequence, 1 if phase in before_publication else 2
                    )
                    pending = recovered.pending()
                    if phase in {"store_commit", "ack_delete"}:
                        assert pending is not None
                        self.assertEqual(pending.to_bytes(), envelope.to_bytes())
                        recovered.store(envelope)  # Exact replay cannot allocate another sequence.
                        self.assertEqual(recovered.next_sequence, 2)
                    else:
                        self.assertIsNone(pending)
                    if phase.startswith("ack_"):
                        assert ack is not None
                        self.assertEqual(receiver.lookup_terminal(envelope), ack)
                        recovered.acknowledge(ack)
                        self.assertIsNone(recovered.pending())
                        self.assertEqual(recovered.next_sequence, 2)
                        with journal.connection() as connection:
                            self.assertEqual(
                                connection.execute("SELECT COUNT(*) FROM episodes").fetchone()[0], 1
                            )
                    self.assertFalse((directory / "pending.staging").exists())
                    self.assertLessEqual(recovered.used_bytes(), recovered.max_bytes)

    def test_spool_pressure_and_loss_do_not_reset_sequence(self) -> None:
        spool = self.spool(max_bytes=256 * 1024)
        with closing(sqlite3.connect(self.source / "messages-1.db")) as connection, connection:
            connection.execute(
                "UPDATE messages SET raw_content=? WHERE source_message_id='source-msg-001'",
                ("x" * (256 * 1024),),
            )
        with self.assertRaisesRegex(CaptureProtocolError, "edge_spool_pressure"):
            spool.store(self.capture())
        self.assertEqual(spool.next_sequence, 1)
        self.assertIsNone(spool.pending())
        with self.assertRaisesRegex(CaptureProtocolError, "epoch_loss"):
            EdgeSpool(
                self.root / "missing",
                source_instance_id=self.executor.source_instance_id,
                account_id=self.ceiling.account_id,
                origin_epoch=self.executor.origin_epoch,
            )

    def test_terminal_ack_growth_obeys_the_same_finite_spool_ceiling(self) -> None:
        spool = self.spool(max_bytes=256 * 1024)
        envelope = self.capture()
        spool.store(envelope)
        journal = FixtureReceiveJournal(self.root / "receive.sqlite")
        receiver = CaptureReceiver(journal, self.expected, stream_epoch="fixture-stream")
        ack = receiver.admit(
            receiver.prepare(envelope), journal.append, receipt_id="fixture-terminal"
        )
        with self.assertRaisesRegex(CaptureProtocolError, "spool_pressure"):
            spool.acknowledge(replace(ack, receipt_id="x" * (128 * 1024)))
        self.assertIsNotNone(spool.pending())
        self.assertEqual(spool.next_sequence, 2)
        self.assertLessEqual(spool.used_bytes(), spool.max_bytes)
        spool.acknowledge(ack)
        self.assertIsNone(spool.pending())

    def test_spool_requires_explicit_recovery_floor_and_never_overflows_sequence(self) -> None:
        recovery = self.root / "recovery-spool"
        with self.assertRaisesRegex(CaptureProtocolError, "transition_receipt"):
            EdgeSpool.initialize(
                recovery,
                source_instance_id=self.executor.source_instance_id,
                account_id=self.ceiling.account_id,
                origin_epoch=self.executor.origin_epoch,
                next_sequence=2,
            )
        self.assertFalse(recovery.exists())
        spool = EdgeSpool.initialize(
            recovery,
            stream_epoch="fixture-recovery-stream",
            next_sequence=(1 << 63) - 1,
            previous_epoch="fixture-lost-stream",
            transition_receipt_id="fixture-agreed-loss-transition",
            source_instance_id=self.executor.source_instance_id,
            account_id=self.ceiling.account_id,
            origin_epoch=self.executor.origin_epoch,
        )
        self.addCleanup(spool.close)
        document = self.capture().document()
        envelope = SealedCapture.seal(
            replace(
                document,
                origin=replace(
                    document.origin,
                    stream_epoch=spool.stream_epoch,
                    sequence=spool.next_sequence,
                ),
            )
        )
        with self.assertRaisesRegex(CaptureProtocolError, "sequence_exhausted"):
            spool.store(envelope)
        self.assertEqual(spool.next_sequence, (1 << 63) - 1)
        self.assertIsNone(spool.pending())

    def test_corrupt_pending_requires_explicit_terminal_epoch_loss_transition(self) -> None:
        spool = self.spool()
        envelope = self.capture()
        spool.store(envelope)
        spool.close()
        payload = self.root / "spool" / "pending.capture"
        payload.write_bytes(envelope.to_bytes()[:-1])
        reopened = EdgeSpool(
            self.root / "spool",
            source_instance_id=self.executor.source_instance_id,
            account_id=self.ceiling.account_id,
            origin_epoch=self.executor.origin_epoch,
        )
        self.addCleanup(reopened.close)
        self.assertTrue(reopened.epoch_lost)
        with self.assertRaises(CaptureProtocolError):
            reopened.store(self.capture(sequence=2))
        ack = CaptureAck(
            "fixture-stream",
            1,
            "fixture-batch-1",
            envelope.digest,
            "fixture-request",
            "epoch_loss",
            "fixture-loss-receipt",
        )
        reopened.acknowledge_epoch_loss(ack)
        with self.assertRaises(CaptureProtocolError):
            reopened.transition_epoch(
                new_epoch="fixture-stream-2", next_sequence=1, receipt_id="fixture-transition"
            )
        reopened.transition_epoch(
            new_epoch="fixture-stream-2", next_sequence=2, receipt_id="fixture-transition"
        )
        self.assertEqual(reopened.next_sequence, 2)
        self.assertFalse(reopened.epoch_lost)

    def test_existing_accepted_lost_ack_resolves_corrupt_bytes_without_rewriting_receipt(
        self,
    ) -> None:
        spool = self.spool()
        envelope = self.capture()
        spool.store(envelope)
        journal = FixtureReceiveJournal(self.root / "receive.sqlite")
        receiver = CaptureReceiver(journal, self.expected, stream_epoch="fixture-stream")
        accepted = receiver.admit(
            receiver.prepare(envelope), journal.append, receipt_id="fixture-accepted-before-loss"
        )
        self.assertEqual(accepted.terminal, "accepted")
        # The core committed, but the wire ACK never reached the edge. Its bytes
        # are then damaged before restart while cached terminal identity survives.
        spool.close()
        payload = self.root / "spool" / "pending.capture"
        payload.write_bytes(envelope.to_bytes()[:-1])
        reopened = EdgeSpool(
            self.root / "spool",
            source_instance_id=self.executor.source_instance_id,
            account_id=self.ceiling.account_id,
            origin_epoch=self.executor.origin_epoch,
        )
        self.addCleanup(reopened.close)
        self.assertTrue(reopened.epoch_lost)
        with self.assertRaises(CaptureProtocolError):
            reopened.acknowledge(accepted)  # The ordinary path needs all sealed bytes.
        wrong_acks = (
            replace(accepted, stream_epoch="fixture-wrong-stream"),
            replace(accepted, sequence=2),
            replace(accepted, sequence=True),
            replace(accepted, batch_id="fixture-wrong-batch"),
            replace(accepted, envelope_digest="0" * 64),
            replace(accepted, request_id="fixture-wrong-request"),
            replace(accepted, receipt_id=""),
            replace(accepted, receipt_id=123),
            replace(accepted, terminal="unknown"),
        )
        for wrong in wrong_acks:
            with self.subTest(wrong=wrong):
                with self.assertRaisesRegex(CaptureProtocolError, "epoch_loss_ack_mismatch"):
                    reopened.acknowledge_epoch_loss(wrong)
                self.assertTrue(payload.exists())
                self.assertTrue(reopened.epoch_lost)
                self.assertEqual(reopened.next_sequence, 2)
                self.assertEqual(receiver.lookup_terminal(envelope), accepted)
        reopened.acknowledge_epoch_loss(accepted)
        self.assertIsNone(reopened.pending())
        self.assertFalse(payload.exists())
        self.assertTrue(reopened.epoch_lost)
        self.assertEqual(reopened.next_sequence, 2)
        self.assertEqual(receiver.lookup_terminal(envelope), accepted)
        with journal.connection() as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM episodes").fetchone()[0], 1)
        reopened.transition_epoch(
            new_epoch="fixture-recovered-stream",
            next_sequence=2,
            receipt_id="fixture-explicit-transition",
        )
        self.assertFalse(reopened.epoch_lost)
        self.assertEqual(reopened.stream_epoch, "fixture-recovered-stream")
        self.assertEqual(reopened.next_sequence, 2)
        next_envelope = self.executor.capture(
            replace(self.request(), request_id="fixture-after-recovery"),
            stream_epoch=reopened.stream_epoch,
            sequence=reopened.next_sequence,
            batch_id="fixture-next-batch",
        )
        reopened.store(next_envelope)
        self.assertEqual(reopened.next_sequence, 3)
        pending = reopened.pending()
        assert pending is not None
        self.assertEqual(pending.to_bytes(), next_envelope.to_bytes())
        self.assertEqual(receiver.lookup_terminal(envelope), accepted)

    def test_corrupt_pending_resolution_preserves_rejected_and_cancelled_acks(self) -> None:
        for terminal in ("rejected", "cancelled"):
            with self.subTest(terminal=terminal):
                directory = self.root / f"spool-{terminal}"
                spool = EdgeSpool.initialize(
                    directory,
                    source_instance_id=self.executor.source_instance_id,
                    account_id=self.ceiling.account_id,
                    origin_epoch=self.executor.origin_epoch,
                    stream_epoch="fixture-stream",
                )
                self.addCleanup(spool.close)
                cancelled = threading.Event()
                if terminal == "cancelled":
                    cancelled.set()
                envelope = self.executor.capture(
                    self.request(), stream_epoch="fixture-stream", sequence=1, cancelled=cancelled
                )
                spool.store(envelope)
                journal = FixtureReceiveJournal(self.root / f"receive-{terminal}.sqlite")
                receiver = CaptureReceiver(journal, self.expected, stream_epoch="fixture-stream")
                ack = receiver.admit(
                    receiver.prepare(envelope),
                    None,
                    receipt_id=f"fixture-existing-{terminal}",
                    reject=True,
                )
                self.assertEqual(ack.terminal, terminal)
                spool.close()
                (directory / "pending.capture").write_bytes(envelope.to_bytes()[:-1])
                reopened = EdgeSpool(
                    directory,
                    source_instance_id=self.executor.source_instance_id,
                    account_id=self.ceiling.account_id,
                    origin_epoch=self.executor.origin_epoch,
                )
                self.addCleanup(reopened.close)
                self.assertTrue(reopened.epoch_lost)
                reopened.acknowledge_epoch_loss(ack)
                self.assertIsNone(reopened.pending())
                self.assertTrue(reopened.epoch_lost)
                self.assertEqual(reopened.next_sequence, 2)
                self.assertEqual(receiver.lookup_terminal(envelope), ack)
                with journal.connection() as connection:
                    self.assertEqual(
                        connection.execute("SELECT COUNT(*) FROM episodes").fetchone()[0], 0
                    )

    def test_recovery_state_reads_only_bound_cached_identity_under_corruption(self) -> None:
        spool = self.spool()
        envelope = self.capture()
        spool.store(envelope)
        spool.close()
        payload = self.root / "spool" / "pending.capture"
        payload.write_bytes(envelope.to_bytes()[:-1])
        reopened = EdgeSpool(
            self.root / "spool",
            source_instance_id=self.executor.source_instance_id,
            account_id=self.ceiling.account_id,
            origin_epoch=self.executor.origin_epoch,
        )
        self.addCleanup(reopened.close)
        with mock.patch.object(
            reopened, "pending", side_effect=AssertionError("recovery cannot read corrupt bytes")
        ):
            state = reopened.recovery_state()
        self.assertEqual(
            state,
            {
                "schema": EDGE_RECOVERY_STATE_SCHEMA,
                "source_instance_id": self.executor.source_instance_id,
                "account_id": self.ceiling.account_id,
                "origin_epoch": self.executor.origin_epoch,
                "stream_epoch": "fixture-stream",
                "next_sequence": 2,
                "epoch_lost": True,
                "pending": {
                    "sequence": 1,
                    "batch_id": "fixture-batch-1",
                    "request_id": "fixture-request",
                    "digest": envelope.digest,
                },
            },
        )
        self.assertNotIn(str(self.root), json.dumps(state))
        self.assertNotIn("Synthetic Owner", json.dumps(state))
        self.assertTrue(payload.exists())
        with reopened._connection:
            reopened._connection.execute(
                "UPDATE stream SET source_instance_id='fixture-wrong-binding' WHERE singleton=1"
            )
        with self.assertRaisesRegex(CaptureProtocolError, "edge_spool_binding_changed"):
            reopened.recovery_state()
        with reopened._connection:
            reopened._connection.execute(
                "UPDATE stream SET source_instance_id=? WHERE singleton=1",
                (self.executor.source_instance_id,),
            )
            reopened._connection.execute(
                "UPDATE pending SET sequence=2 WHERE singleton=1"
            )
        with self.assertRaisesRegex(CaptureProtocolError, "edge_recovery_state_corrupt"):
            reopened.recovery_state()

    def test_receiver_atomic_rollback_dedup_order_conflict_and_A_B_A(self) -> None:
        journal = FixtureReceiveJournal(self.root / "receive.sqlite")
        receiver = CaptureReceiver(journal, self.expected, stream_epoch="fixture-stream")
        first = self.capture(self.request("verify", message_ids=("source-msg-001",)))

        def fail_after_admission(provider, document):
            journal.append(provider, document)
            raise RuntimeError("fixture failure before outer commit")

        with self.assertRaises(RuntimeError):
            receiver.admit(
                receiver.prepare(first), fail_after_admission, receipt_id="fixture-receipt-1"
            )
        self.assertIsNone(journal.batch_receipt("fixture-batch-1"))
        with journal.connection() as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM episodes").fetchone()[0], 0)
            self.assertEqual(
                connection.execute("SELECT position FROM reader_state").fetchone()[0], 0
            )
        with self.assertRaisesRegex(CaptureProtocolError, "out_of_order"):
            receiver.admit(
                receiver.prepare(self.capture(sequence=2)),
                journal.append,
                receipt_id="out-of-order",
            )
        ack = receiver.admit(
            receiver.prepare(first), journal.append, receipt_id="fixture-receipt-1"
        )
        replay = receiver.admit(
            receiver.prepare(first),
            journal.append,
            receipt_id="not-the-new-receipt",
            now=(utc_now() + timedelta(hours=1)).isoformat(),
        )
        self.assertEqual(replay, ack)
        conflict = SealedCapture.seal(
            replace(
                first.document(), request=replace(first.document().request, request_id="conflict")
            )
        )
        with self.assertRaisesRegex(CaptureProtocolError, "batch_identity_conflict"):
            receiver.admit(receiver.prepare(conflict), journal.append, receipt_id="conflict")
        original = first.document().evidence.messages[0].raw_content
        for sequence, body in ((2, "fixture body B"), (3, original)):
            with closing(sqlite3.connect(self.source / "messages-1.db")) as connection, connection:
                connection.execute(
                    "UPDATE messages SET raw_content=? WHERE source_message_id='source-msg-001'",
                    (body,),
                )
            envelope = self.capture(
                replace(first.document().request, request_id=f"fixture-request-{sequence}"),
                sequence,
            )
            receiver.admit(
                receiver.prepare(envelope), journal.append, receipt_id=f"fixture-receipt-{sequence}"
            )
        with journal.connection() as connection:
            self.assertEqual(
                [
                    row[0]
                    for row in connection.execute("SELECT body FROM episodes ORDER BY sequence")
                ],
                [original, "fixture body B", original],
            )

    def test_frame_disconnect_size_and_fixed_ssh_command(self) -> None:
        wire = io.BytesIO()
        FramedStream(io.BytesIO(), wire).send(self.capture())
        frame = FramedStream(io.BytesIO(wire.getvalue()), io.BytesIO()).receive()
        self.assertIsInstance(frame, SealedCapture)
        with self.assertRaises(RelayDisconnected):
            FramedStream(io.BytesIO(wire.getvalue()[:-5]), io.BytesIO()).receive()
        with self.assertRaises(CaptureProtocolError):
            FramedStream(io.BytesIO(struct.pack("!I", 1 << 30)), io.BytesIO()).receive()
        connector = SSHRelayConnector("fixture-host", Path("/synthetic/operator-key"))
        self.assertEqual(connector.argv()[-1], "sightglassctl edge-session")
        self.assertIn("ClearAllForwardings=yes", connector.argv())
        self.assertIn("IdentitiesOnly=yes", connector.argv())
        with self.assertRaises(CaptureProtocolError):
            SSHRelayConnector("fixture;sh", Path("/synthetic/operator-key")).argv()
        self.assertEqual(os.stat(self.root).st_uid, os.geteuid())

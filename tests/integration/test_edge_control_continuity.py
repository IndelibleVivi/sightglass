"""Real duplex control exchange and content-free Edge connection diagnostics."""
from __future__ import annotations

import json
import socket
import threading
import unittest
from pathlib import Path
from typing import Any, cast
from unittest import mock

from sightglass.contracts.capture import CAPTURE_VERSION, CaptureCeiling
from sightglass.operations import check_operation_budget
from sightglass.runtime.edge import EdgeSpool
from sightglass.runtime.edge_relay import EdgeSession, FramedStream, RelayDisconnected
from sightglass.runtime.edge_runtime import EdgeConnectionStatus
from sightglass.source.capture.executor import CaptureExecutor
from tests.unit import test_capture_boundary as fixtures


class EdgeControlContinuityTests(unittest.TestCase):
    root: Path
    executor: CaptureExecutor
    ceiling: CaptureCeiling
    setUp = fixtures.CaptureBoundaryTests.setUp
    request = fixtures.CaptureBoundaryTests.request

    def exchange(self, behavior):
        request = self.request()
        spool = EdgeSpool.initialize(
            self.root / 'spool', source_instance_id=self.executor.source_instance_id,
            account_id=self.ceiling.account_id, origin_epoch=self.executor.origin_epoch,
            stream_epoch='fixture-stream',
        )
        self.addCleanup(spool.close)
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        streams = [s.makefile(mode, buffering=0) for s in (a, b) for mode in ('rb', 'wb')]
        for stream in streams:
            self.addCleanup(stream.close)
        channel = FramedStream(cast(Any, streams[0]), cast(Any, streams[1]))
        peer = FramedStream(cast(Any, streams[2]), cast(Any, streams[3]))
        working, source_done, release = (threading.Event() for _ in range(3))
        peer_errors = []

        def serve():
            try:
                while not release.is_set():
                    try:
                        frame = peer.receive(timeout=.1)
                    except RelayDisconnected as exc:
                        if str(exc) == 'edge_relay_read_timeout':
                            continue
                        raise
                    assert isinstance(frame, dict)
                    self.assertEqual(frame['kind'], 'working')
                    working.set()
                    if behavior in ('withhold', 'partial'):
                        if behavior == 'partial':
                            b.sendall(b'\x00\x00')
                        release.wait(2)
                        return
                    if behavior == 'finish_before_reply':
                        self.assertTrue(source_done.wait(2))
                    peer.send({'schema': CAPTURE_VERSION,
                               'kind': 'cancel' if behavior == 'cancel' else 'continue',
                               'request_id': request.request_id, 'core_generation': ''})
            except BaseException as exc:
                peer_errors.append(exc)

        thread = threading.Thread(target=serve)
        thread.start()
        original = self.executor._execute
        capture = self.executor.capture

        def execute(*args, **kwargs):
            self.assertTrue(working.wait(2))
            if behavior in ('cancel', 'withhold', 'partial'):
                while True:
                    check_operation_budget()
                    release.wait(.002)
            return original(*args, **kwargs)

        def capture_returned(*args, **kwargs):
            envelope = capture(*args, **kwargs)
            source_done.set()
            return envelope

        status = EdgeConnectionStatus(spool.directory)
        session = EdgeSession(self.executor, spool, token='synthetic-secret',
                              connection_observer=status)
        failure = None
        try:
            with mock.patch.object(self.executor, '_execute', side_effect=execute), \
                 mock.patch.object(self.executor, 'capture', side_effect=capture_returned), \
                 mock.patch('sightglass.runtime.edge_relay.RELAY_CONTROL_TIMEOUT_SECONDS', .2):
                try:
                    session._capture_controlled(channel, request, 2., threading.Event())
                except RelayDisconnected as exc:
                    failure = exc
                    session._observe_connection('disconnected', exc)
        finally:
            release.set()
            thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(peer_errors, [])
        pending = spool.pending()
        assert pending is not None
        self.assertEqual(spool.next_sequence, 2)
        replay = spool.pending()
        assert replay is not None
        self.assertEqual(replay.to_bytes(), pending.to_bytes())
        return pending, failure, session, status

    def test_timely_continue_keeps_one_complete_spool(self):
        pending, failure, _session, _status = self.exchange('continue')
        self.assertIsNone(failure)
        self.assertEqual(pending.document().receipt.terminal, 'complete')

    def test_timely_cancel_keeps_one_cancelled_spool(self):
        pending, failure, _session, _status = self.exchange('cancel')
        self.assertIsNone(failure)
        self.assertEqual(pending.document().receipt.terminal, 'cancelled')

    def test_source_completion_drains_timely_inflight_reply(self):
        pending, failure, _session, _status = self.exchange('finish_before_reply')
        self.assertIsNone(failure)
        self.assertEqual(pending.document().receipt.terminal, 'complete')

    def assert_timeout_diagnostic(self, behavior):
        pending, failure, session, status = self.exchange(behavior)
        assert isinstance(failure, RelayDisconnected)
        self.assertEqual(str(failure.__cause__), 'edge_relay_read_timeout')
        self.assertEqual(pending.document().receipt.terminal, 'cancelled')
        value = json.loads(status.path.read_text())
        self.assertEqual(value['control']['phase'], 'reply_receive')
        self.assertEqual(value['control']['reason'], 'edge_relay_read_timeout')
        self.assertFalse(value['control']['source_returned'])
        self.assertTrue(value['control']['spooled'])
        self.assertFalse(value['control']['owner_alive'])
        self.assertEqual(value['last_error_location']['function'], '_read_exact')
        self.assertEqual(status.path.stat().st_mode & 0o777, 0o600)
        encoded = status.path.read_text()
        for sensitive in ('synthetic-secret', str(self.root), self.request().request_id,
                          self.ceiling.account_id):
            self.assertNotIn(sensitive, encoded)
        self.assertLess(len(encoded), 4096)
        session._observe_connection('connected')
        cleared = json.loads(status.path.read_text())
        self.assertIsNone(cleared['last_error_code'])
        self.assertEqual(cleared['historical_error_count'], 1)
        self.assertEqual(cleared['control'], value['control'])

    def test_withheld_reply_retains_exact_spool_and_first_cause(self):
        self.assert_timeout_diagnostic('withhold')

    def test_partial_reply_retains_exact_spool_and_first_cause(self):
        self.assert_timeout_diagnostic('partial')

    def test_observer_failure_does_not_change_transport_or_publication(self):
        observer = mock.Mock(side_effect=OSError('synthetic-secret /private/source'))
        session = EdgeSession(self.executor, mock.Mock(), token='synthetic-secret',
                              connection_observer=observer)
        session._observe_connection('connected')
        session._observe_connection('disconnected', RelayDisconnected('unknown-private-detail'))
        self.assertEqual(observer.call_count, 2)
        state = observer.call_args.args[0]
        self.assertEqual(state['last_error_code'], 'relay_disconnected')
        self.assertNotIn('unknown-private-detail', json.dumps(state))
        self.assertNotIn('synthetic-secret', json.dumps(state))

    def test_status_cannot_follow_symlink_or_replace_foreign_schema(self):
        target = self.root / 'foreign'
        target.write_text('{"schema":"foreign"}')
        target.chmod(0o600)
        spool = self.root / 'status-spool'
        status_path = spool.with_name(spool.name + '.connection.json')
        status_path.symlink_to(target)
        status = EdgeConnectionStatus(spool)
        status({'schema': 'sightglass.edge-connection.v1', 'phase': 'connected'})
        self.assertEqual(target.read_text(), '{"schema":"foreign"}')
        status_path.unlink()
        for contents in ('{"schema":"foreign"}', '{"schema":'):
            status_path.write_text(contents)
            status_path.chmod(0o600)
            EdgeConnectionStatus(spool)({'schema': 'sightglass.edge-connection.v1',
                                         'phase': 'connected'})
            self.assertEqual(status_path.read_text(), contents)

    def test_truncated_diagnostic_does_not_block_capture_or_exact_pending(self):
        path = self.root / 'spool.connection.json'
        path.write_text('{"schema":')
        path.chmod(0o600)
        pending, failure, session, _status = self.exchange('continue')
        self.assertIsNone(failure)
        session._observe_connection('connected')
        self.assertEqual(path.read_text(), '{"schema":')
        self.assertEqual(pending.document().receipt.terminal, 'complete')

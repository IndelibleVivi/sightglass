from __future__ import annotations

import hashlib
import io
import json
import os
import socket
import struct
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from sightglass.runtime.migration import (
    CHUNK_BYTES,
    FrozenTransfer,
    freeze_files,
    receive_frozen,
    send_frozen,
)


class FrozenTransferTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.target = self.root / "target"
        self.source.mkdir(mode=0o700)
        (self.source / "deliveries").mkdir(mode=0o700)
        self.body = b"unmistakably synthetic state\n" * 90_000
        (self.source / "window.db").write_bytes(self.body)
        (self.source / "window.db").chmod(0o600)
        (self.source / "deliveries" / "synthetic.json").write_bytes(b'{"synthetic":true}')
        (self.source / "deliveries" / "synthetic.json").chmod(0o600)
        self.plan = freeze_files(self.source, ("window.db", "deliveries/synthetic.json"))

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def transfer(self) -> None:
        left, right = socket.socketpair()
        failures: list[BaseException] = []

        def receiver() -> None:
            try:
                with right, right.makefile("rb") as incoming, right.makefile("wb") as outgoing:
                    receive_frozen(self.target, incoming, outgoing, min_free_bytes=0)
            except BaseException as error:
                failures.append(error)

        thread = threading.Thread(target=receiver)
        thread.start()
        try:
            with left, left.makefile("rb") as incoming, left.makefile("wb") as outgoing:
                send_frozen(self.source, self.plan, incoming, outgoing)
        finally:
            thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])

    def test_exact_transfer_and_lost_completion_response_replay(self) -> None:
        self.transfer()
        self.transfer()
        self.assertEqual((self.target / "window.db").read_bytes(), self.body)
        self.assertEqual((self.source / "window.db").read_bytes(), self.body)
        self.assertEqual((self.target / "window.db").stat().st_mode & 0o777, 0o600)
        self.assertTrue((self.target / "transfer-complete.json").exists())
        self.assertFalse(any(self.source.glob("*.sgpartial")))

    def test_interrupted_transfer_resumes_verified_whole_chunks(self) -> None:
        # Place the large member first so the interrupted pipe has one verified chunk.
        self.plan = FrozenTransfer(tuple(reversed(self.plan.files)))
        manifest = json.dumps(self.plan.as_dict()).encode()
        interrupted = io.BytesIO(struct.pack("!I", len(manifest)) + manifest
                                 + self.body[:CHUNK_BYTES + 15])
        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            receive_frozen(self.target, interrupted, io.BytesIO(), min_free_bytes=0)
        self.assertEqual((self.target / "window.db.sgpartial").stat().st_size, CHUNK_BYTES)
        self.assertFalse((self.target / "transfer-complete.json").exists())
        self.transfer()
        self.assertEqual(hashlib.sha256((self.target / "window.db").read_bytes()).hexdigest(),
                         self.plan.files[0].digest)

    def test_source_mutation_refuses_transfer_before_any_destination_bytes(self) -> None:
        with (self.source / "window.db").open("ab") as handle:
            handle.write(b"changed")
        outgoing = io.BytesIO()
        with self.assertRaisesRegex(RuntimeError, "source changed"):
            send_frozen(self.source, self.plan, io.BytesIO(), outgoing)
        self.assertEqual(outgoing.getvalue(), b"")

    def test_wrong_completed_destination_and_other_manifest_fail_closed(self) -> None:
        self.transfer()
        (self.target / "window.db").write_bytes(b"tampered")
        value = json.dumps(self.plan.as_dict()).encode()
        with self.assertRaisesRegex(RuntimeError, "does not match"):
            receive_frozen(self.target, io.BytesIO(struct.pack("!I", len(value)) + value),
                           io.BytesIO(), min_free_bytes=0)

    def test_physical_free_floor_refuses_before_body_and_unpublished_state_remains(self) -> None:
        value = json.dumps(self.plan.as_dict()).encode()
        capacity = mock.Mock(f_bavail=1, f_frsize=4096)
        with mock.patch("sightglass.runtime.migration.os.statvfs", return_value=capacity):
            with self.assertRaisesRegex(RuntimeError, "physical free floor"):
                receive_frozen(self.target, io.BytesIO(struct.pack("!I", len(value)) + value),
                               io.BytesIO())
        self.assertFalse((self.target / "transfer-complete.json").exists())
        self.assertFalse((self.target / "window.db").exists())

    def test_paths_links_and_duplicate_members_are_rejected(self) -> None:
        for value in ("../escape", "/absolute", "a/../b", "x.sgpartial", "a\\b"):
            plan = self.plan.as_dict()
            plan["files"][0]["relative"] = value
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                FrozenTransfer.parse(plan)
        (self.source / "linked").symlink_to(self.source / "window.db")
        with self.assertRaises(RuntimeError):
            freeze_files(self.source, ("linked",))
        os.link(self.source / "window.db", self.source / "hardlink")
        with self.assertRaises(RuntimeError):
            freeze_files(self.source, ("hardlink",))


if __name__ == "__main__":
    unittest.main()

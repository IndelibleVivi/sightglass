"""File publication ordering, including a directory-sync failure."""
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sightglass.reader.deliveries import DeliveryPayloadStore


class DeliveryDurabilityTests(unittest.TestCase):
    def test_file_and_parent_sync_precede_reference(self):
        with tempfile.TemporaryDirectory() as root:
            store = DeliveryPayloadStore(Path(root) / "window.db")
            synced = []
            original = os.fsync

            def sync(fd):
                synced.append(stat.S_ISDIR(os.fstat(fd).st_mode))
                original(fd)

            with patch("sightglass.reader.deliveries.os.fsync", side_effect=sync):
                ref, digest = store.write("synthetic-delivery", {"value": "synthetic"})
            self.assertEqual(synced, [False, True])
            self.assertEqual(store.read(ref, digest), {"value": "synthetic"})

    def test_directory_sync_failure_returns_no_reference(self):
        with tempfile.TemporaryDirectory() as root:
            store = DeliveryPayloadStore(Path(root) / "window.db")
            with patch.object(store, "_sync_directory", side_effect=OSError("synthetic fsync")):
                with self.assertRaises(OSError):
                    store.write("synthetic-failed", {})

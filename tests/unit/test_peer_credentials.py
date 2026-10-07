from __future__ import annotations

import os
import socket
import struct
import unittest
from unittest.mock import Mock, patch

from sightglass.runtime.ipc import peer_effective_ids


class PeerCredentialTests(unittest.TestCase):
    def test_connected_unix_socket_reports_actual_peer_owner(self):
        first, second = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            self.assertEqual(peer_effective_ids(first), (os.geteuid(), os.getegid()))
            self.assertEqual(peer_effective_ids(second), (os.geteuid(), os.getegid()))
        finally:
            first.close()
            second.close()

    def test_linux_uses_kernel_peer_credentials_instead_of_process_defaults(self):
        connection = Mock()
        connection.getsockopt.return_value = struct.pack("iII", 731, 1001, 1002)
        with (
            patch("sightglass.runtime.ipc.sys.platform", "linux"),
            patch("sightglass.runtime.ipc.socket.SO_PEERCRED", 17, create=True),
        ):
            self.assertEqual(peer_effective_ids(connection), (1001, 1002))
        connection.getsockopt.assert_called_once_with(
            socket.SOL_SOCKET, 17, struct.calcsize("iII")
        )

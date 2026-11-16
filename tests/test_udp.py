"""
Tests for UDP asynchronous communication (Server, Client, PING-PONG).
Validates Week 1 Module 1 (UDP Server) and Module 2 (UDP Client).
"""

from __future__ import annotations

import asyncio
import socket
import unittest

from udp_client import AsyncUDPClient
from udp_server import create_udp_socket, run_server


class TestUDPServerUnit(unittest.TestCase):
    """Unit tests for UDP server socket initialization and binding."""

    def test_create_udp_socket_success(self):
        """Verifies that create_udp_socket creates a non-blocking UDP socket."""
        sock = create_udp_socket("127.0.0.1", 0)
        try:
            self.assertIsNotNone(sock)
            self.assertFalse(sock.getblocking())
            addr = sock.getsockname()
            self.assertEqual(addr[0], "127.0.0.1")
            self.assertGreater(addr[1], 0)
        finally:
            sock.close()

    def test_port_collision_raises_oserror(self):
        """Verifies that attempting to bind to an occupied port raises OSError."""
        sock1 = create_udp_socket("127.0.0.1", 0)
        port = sock1.getsockname()[1]
        try:
            with self.assertRaises(OSError):
                create_udp_socket("127.0.0.1", port)
        finally:
            sock1.close()


class TestUDPClientUnit(unittest.IsolatedAsyncioTestCase):
    """Unit tests for UDP client connection lifecycle, timeouts, and validation."""

    def test_client_socket_lifecycle(self):
        """Verifies client start and close manage socket state correctly."""
        client = AsyncUDPClient()
        self.assertIsNone(client.sock)
        client.start()
        self.assertIsNotNone(client.sock)
        self.assertFalse(client.sock.getblocking())
        client.close()
        self.assertIsNone(client.sock)

    async def test_client_not_started_raises_runtime_error(self):
        """Verifies sending or receiving before start() raises RuntimeError."""
        client = AsyncUDPClient()
        with self.assertRaises(RuntimeError):
            await client.send_message("TEST")
        with self.assertRaises(RuntimeError):
            await client.receive_response()

    async def test_timeout_when_server_unavailable(self):
        """Verifies timeout handling when sending to an unreachable server."""
        temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        temp_sock.bind(("127.0.0.1", 0))
        unused_port = temp_sock.getsockname()[1]
        temp_sock.close()

        async with AsyncUDPClient(
            server_host="127.0.0.1", server_port=unused_port, timeout=0.2
        ) as client:
            await client.send_message("PING")
            with self.assertRaises((TimeoutError, ConnectionResetError)):
                await client.receive_response()

    async def test_unexpected_server_response_validation(self):
        """Verifies that non-PONG responses cause ping() to return False."""
        loop = asyncio.get_running_loop()
        mock_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        mock_server.setblocking(False)
        mock_server.bind(("127.0.0.1", 0))
        port = mock_server.getsockname()[1]

        async def respond_unexpected():
            data, addr = await loop.sock_recvfrom(mock_server, 1024)
            await loop.sock_sendto(mock_server, b"UNEXPECTED_DATA", addr)

        server_task = asyncio.create_task(respond_unexpected())
        try:
            async with AsyncUDPClient(
                server_host="127.0.0.1", server_port=port, timeout=1.0
            ) as client:
                is_pong = await client.ping()
                self.assertFalse(is_pong)
        finally:
            server_task.cancel()
            mock_server.close()


class TestUDPPingPongIntegration(unittest.IsolatedAsyncioTestCase):
    """End-to-end integration tests between live UDP Server and UDP Client."""

    async def asyncSetUp(self):
        """Spawns a real UDP server task on an ephemeral port."""
        temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        temp_sock.bind(("127.0.0.1", 0))
        self.port = temp_sock.getsockname()[1]
        temp_sock.close()

        self.server_task = asyncio.create_task(
            run_server("127.0.0.1", self.port)
        )
        await asyncio.sleep(0.05)

    async def asyncTearDown(self):
        """Gracefully cancels server task after test completion."""
        self.server_task.cancel()
        try:
            await self.server_task
        except asyncio.CancelledError:
            pass

    async def test_ping_pong_success(self):
        """Verifies successful PING send and PONG validation."""
        async with AsyncUDPClient(
            server_host="127.0.0.1", server_port=self.port, timeout=2.0
        ) as client:
            success = await client.ping()
            self.assertTrue(success)

    async def test_unexpected_message_handling(self):
        """Verifies server responds with error message when client sends unexpected text."""
        async with AsyncUDPClient(
            server_host="127.0.0.1", server_port=self.port, timeout=2.0
        ) as client:
            await client.send_message("HELLO_PEER")
            response, addr = await client.receive_response()
            self.assertEqual(addr[0], "127.0.0.1")
            self.assertEqual(addr[1], self.port)
            self.assertIn("Unexpected message 'HELLO_PEER'", response)


if __name__ == "__main__":
    unittest.main()

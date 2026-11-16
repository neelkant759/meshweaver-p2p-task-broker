"""
MeshWeaver - Asynchronous UDP Client
Module 2 of Week 1: Core Networking Layer

This module implements an asynchronous UDP client using Python's built-in
`socket` and `asyncio` modules. It provides capabilities to dispatch datagrams,
await server responses with a configurable timeout, validate PING-PONG round trips,
handle network anomalies and timeouts gracefully, and ensure proper resource cleanup.
"""

from __future__ import annotations

import argparse
import asyncio
import socket
import sys
from typing import Tuple, Optional

# Default Client Configuration
DEFAULT_SERVER_HOST: str = "127.0.0.1"
DEFAULT_SERVER_PORT: int = 8000
DEFAULT_TIMEOUT: float = 3.0
BUFFER_SIZE: int = 4096


class AsyncUDPClient:
    """
    An asynchronous UDP client managing datagram transmission and reception.

    Attributes:
        server_host (str): IP address of the target UDP server.
        server_port (int): Port of the target UDP server.
        timeout (float): Timeout in seconds when waiting for server responses.
        sock (socket.socket | None): Non-blocking UDP socket instance.
    """

    def __init__(
        self,
        server_host: str = DEFAULT_SERVER_HOST,
        server_port: int = DEFAULT_SERVER_PORT,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self.server_host: str = server_host
        self.server_port: int = server_port
        self.timeout: float = timeout
        self.sock: Optional[socket.socket] = None
        self._is_open: bool = False

    def start(self) -> None:
        """
        Initializes and configures the non-blocking UDP socket.
        """
        if self._is_open and self.sock:
            return

        try:
            # Create an IPv4 (AF_INET) datagram (SOCK_DGRAM) socket
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # Set socket to non-blocking mode for asyncio event loop integration
            self.sock.setblocking(False)
            self._is_open = True
        except OSError as err:
            print(f"[Client Error] Failed to create UDP socket: {err}")
            raise

    def close(self) -> None:
        """
        Closes the UDP socket and safely releases system resources.
        """
        if self.sock:
            try:
                self.sock.close()
            except Exception as err:
                print(f"[Client Error] Error closing socket: {err}")
            finally:
                self.sock = None
                self._is_open = False

    async def __aenter__(self) -> AsyncUDPClient:
        """Asynchronous context manager entry."""
        self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Asynchronous context manager exit."""
        self.close()

    async def send_message(self, message: str) -> None:
        """
        Asynchronously sends a UTF-8 encoded string message to the configured server.

        Args:
            message (str): Text payload to send.

        Raises:
            RuntimeError: If the socket is not initialized.
            OSError: If transmission fails.
        """
        if not self._is_open or not self.sock:
            raise RuntimeError("Client socket is not open. Call start() before sending.")

        loop = asyncio.get_running_loop()
        data = message.encode("utf-8")
        target_addr = (self.server_host, self.server_port)

        print(f"[Client] Sending to {target_addr}: {message}")
        try:
            await loop.sock_sendto(self.sock, data, target_addr)
        except OSError as err:
            print(f"[Client Error] Failed to send message to {target_addr}: {err}")
            raise

    async def receive_response(self) -> Tuple[str, Tuple[str, int]]:
        """
        Asynchronously awaits a datagram response within the configured timeout period.

        Returns:
            Tuple[str, Tuple[str, int]]: Decoded string response and the sender's (ip, port).

        Raises:
            TimeoutError: If no response is received before timeout expires.
            ConnectionResetError: If the target port is unreachable (ICMP port unreachable).
            RuntimeError: If client socket is not open.
            OSError: On general socket communication failures.
        """
        if not self._is_open or not self.sock:
            raise RuntimeError("Client socket is not open. Call start() before receiving.")

        loop = asyncio.get_running_loop()

        try:
            # Enforce response timeout via asyncio.wait_for
            data, addr = await asyncio.wait_for(
                loop.sock_recvfrom(self.sock, BUFFER_SIZE),
                timeout=self.timeout,
            )
            response = data.decode("utf-8")
            print(f"[Client] Received from {addr}: {response}")
            return response, addr
        except asyncio.TimeoutError:
            print(
                f"[Client Error] Request timed out after {self.timeout}s waiting for response from {self.server_host}:{self.server_port}"
            )
            raise TimeoutError(f"No response received from {self.server_host}:{self.server_port} within {self.timeout}s.")
        except ConnectionResetError as reset_err:
            print(f"[Client Error] Connection reset: Server at {self.server_host}:{self.server_port} is unavailable.")
            raise reset_err
        except OSError as err:
            print(f"[Client Error] Socket error during receive: {err}")
            raise

    async def ping(self) -> bool:
        """
        Sends a 'PING' message to the server, awaits a response, and verifies if it is 'PONG'.

        Returns:
            bool: True if 'PONG' was received, False if an unexpected response was received.

        Raises:
            TimeoutError: If server does not reply within configured timeout.
            ConnectionResetError: If server is unavailable.
            OSError: On socket errors.
        """
        await self.send_message("PING")
        response, _ = await self.receive_response()
        cleaned_response = response.strip()

        if cleaned_response == "PONG":
            print("[Client] Validation successful: received valid 'PONG' response.")
            return True
        else:
            print(f"[Client Warning] Validation failed: expected 'PONG', received '{cleaned_response}'.")
            return False


async def run_client(
    host: str = DEFAULT_SERVER_HOST,
    port: int = DEFAULT_SERVER_PORT,
    timeout: float = DEFAULT_TIMEOUT,
    message: str = "PING",
) -> None:
    """
    Executes a client communication lifecycle and performs graceful shutdown.

    Args:
        host: Target server IP.
        port: Target server Port.
        timeout: Wait timeout in seconds.
        message: Payload message to transmit.
    """
    client = AsyncUDPClient(server_host=host, server_port=port, timeout=timeout)
    client.start()

    print("\nMeshWeaver UDP Client Initialized")
    print(f"Target Server: {host}:{port}")
    print(f"Configured Timeout: {timeout}s\n")

    try:
        if message == "PING":
            success = await client.ping()
            if success:
                print("\n[Result] PING-PONG communication verified successfully.")
            else:
                print("\n[Result] Unexpected response received from server.")
        else:
            await client.send_message(message)
            response, addr = await client.receive_response()
            print(f"\n[Result] Response from {addr}: {response}")
    except TimeoutError:
        print("\n[Result] Failed: Server did not respond (timed out).")
    except ConnectionResetError:
        print("\n[Result] Failed: Server is unreachable (connection reset).")
    except OSError as err:
        print(f"\n[Result] Communication error: {err}")
    finally:
        client.close()
        print("[Client] Client socket closed cleanly.")


def main() -> None:
    """Entry point for command-line execution."""
    parser = argparse.ArgumentParser(description="MeshWeaver Asynchronous UDP Client")
    parser.add_argument("--host", default=DEFAULT_SERVER_HOST, help="Server IP address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=DEFAULT_SERVER_PORT, help="Server Port (default: 8000)")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="Response timeout in seconds (default: 3.0)")
    parser.add_argument("--message", default="PING", help="Message to transmit (default: PING)")
    args = parser.parse_args()

    try:
        asyncio.run(
            run_client(
                host=args.host,
                port=args.port,
                timeout=args.timeout,
                message=args.message,
            )
        )
    except KeyboardInterrupt:
        print("\n[Client] Interrupted by user. Exiting.")


if __name__ == "__main__":
    main()

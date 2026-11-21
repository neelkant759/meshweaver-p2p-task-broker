"""
MeshWeaver - Asynchronous UDP Server
Module 1 of Week 1: Core Networking Layer

This module implements an asynchronous UDP server using Python's built-in
`socket` and `asyncio` modules. It binds to a configured IP and port,
continuously listens for incoming datagrams, handles PING-PONG health checks,
and gracefully recovers from communication anomalies and shutdown signals.

Week 1 Module 3/4 extension:
  The server now also recognises MeshWeaver task payloads (identified by the
  TASK_MAGIC_PREFIX from serializer.py). When a task packet is received it is
  forwarded to executor.execute_task() and the result bytes are sent back to
  the requesting peer. Plain PING-PONG health checks are fully preserved.
"""

from __future__ import annotations

import asyncio
import errno
import socket
import sys
from typing import Tuple

from executor import execute_task
from serializer import is_task_payload
from dht import DHTNode, is_dht_payload
from gossip import GossipEngine, is_gossip_payload

# Default Server Configuration
DEFAULT_HOST: str = "127.0.0.1"
DEFAULT_PORT: int = 8000
BUFFER_SIZE: int = 4096


def create_udp_socket(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> socket.socket:
    """
    Creates, configures, and binds a non-blocking IPv4 UDP socket.

    Args:
        host: IP address to bind (defaults to 127.0.0.1).
        port: Port number to listen on (defaults to 8000).

    Returns:
        socket.socket: Bound non-blocking UDP socket.

    Raises:
        OSError: If port is occupied or socket creation fails.
    """
    # Create an IPv4 (AF_INET) datagram (SOCK_DGRAM) socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Enable non-blocking mode for asynchronous event loop integration
        sock.setblocking(False)
        # Bind the socket to the target address and port
        sock.bind((host, port))
        return sock
    except OSError as err:
        sock.close()
        # Check for address already in use (EADDRINUSE / WSAEADDRINUSE)
        if err.errno == errno.EADDRINUSE or getattr(err, "winerror", None) == 10048:
            print(f"\n[Error] Port {port} is already in use by another application.")
            print("Please terminate the occupying process or select a different port.")
        else:
            print(f"\n[Error] Failed to create and bind socket: {err}")
        raise


async def handle_message(
    loop: asyncio.AbstractEventLoop,
    sock: socket.socket,
    data: bytes,
    addr: Tuple[str, int],
    dht_node: DHTNode | None = None,
    gossip_engine: GossipEngine | None = None,
) -> None:
    """
    Processes a received datagram and dispatches the appropriate response.

    Handles five classes of incoming message:
      1. MeshWeaver DHT discovery message (starts with MWDHT:):
         Processes Kademlia peer discovery and returns DHT responses.
      2. MeshWeaver Gossip message (starts with MWGOSSIP:):
         Processes periodic telemetry (CPU/RAM) and gossip neighbor discovery.
      3. MeshWeaver task payload (binary, identified by TASK_MAGIC_PREFIX):
         Deserializes and executes the function; sends back a result payload.
      4. Plain-text "PING" -> responds "PONG" (health-check).
      5. Any other text -> responds with an error message.

    Args:
        loop: The active asyncio event loop.
        sock: The active non-blocking UDP socket.
        data: Raw received payload bytes.
        addr: Tuple of (client_ip, client_port).
        dht_node: Optional active DHTNode controller instance for peer discovery.
        gossip_engine: Optional active GossipEngine instance for telemetry synchronization.
    """
    # --- Route 1: Kademlia DHT Discovery Message ---
    if is_dht_payload(data):
        if dht_node:
            resp_bytes = dht_node.handle_dht_message(data, addr)
            if resp_bytes:
                try:
                    await loop.sock_sendto(sock, resp_bytes, addr)
                except OSError as send_err:
                    print(f"[Server] Failed to send DHT response to {addr}: {send_err}")
        return

    # --- Route 2: Gossip Telemetry Heartbeat ---
    if is_gossip_payload(data):
        if gossip_engine:
            gossip_engine.handle_gossip(data, addr)
        return

    # --- Route 3: MeshWeaver task packet (binary, starts with TASK_MAGIC_PREFIX) ---
    if is_task_payload(data):
        print(f"[Server] Received task payload from {addr} ({len(data)} bytes)")

        async def _execute_and_send(task_data: bytes, target_addr: Tuple[str, int]) -> None:
            try:
                result_bytes = await asyncio.to_thread(execute_task, task_data)
                await loop.sock_sendto(sock, result_bytes, target_addr)
                print(f"[Server] Sent task result to {target_addr} ({len(result_bytes)} bytes)")
            except OSError as send_err:
                print(f"[Server] Failed to send task result to {target_addr}: {send_err}")

        loop.create_task(_execute_and_send(data, addr))
        return

    # --- Route 2 & 3: Plain-text messages ---
    try:
        # Decode received bytes to string
        message = data.decode("utf-8").strip()
    except UnicodeDecodeError:
        print(f"Received malformed non-UTF8 payload from {addr}")
        error_response = b"ERROR: Invalid payload encoding (expected UTF-8)"
        try:
            await loop.sock_sendto(sock, error_response, addr)
        except OSError as send_err:
            print(f"Failed to send error response to {addr}: {send_err}")
        return

    print(f"Received from {addr}: {message}")

    # Process message logic
    if message == "PING":
        response = b"PONG"
        try:
            await loop.sock_sendto(sock, response, addr)
            print("Sent response: PONG")
        except OSError as send_err:
            print(f"Failed to send PONG to {addr}: {send_err}")
    else:
        # Handle unexpected message
        response = f"ERROR: Unexpected message '{message}'. Expected 'PING'.".encode("utf-8")
        try:
            await loop.sock_sendto(sock, response, addr)
            print(f"Sent response: ERROR (Unexpected message)")
        except OSError as send_err:
            print(f"Failed to send response to {addr}: {send_err}")


async def run_server(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    dht_node: DHTNode | None = None,
    gossip_engine: GossipEngine | None = None,
) -> None:
    """
    Runs the asynchronous UDP server event loop.

    Continuously listens for incoming datagrams using `loop.sock_recvfrom`
    and dispatches them for processing without blocking the event loop.

    Args:
        host: IP address to bind.
        port: Port number to bind.
        dht_node: Optional active DHTNode controller instance.
        gossip_engine: Optional active GossipEngine instance.
    """
    sock: socket.socket | None = None
    try:
        sock = create_udp_socket(host, port)
    except OSError:
        sys.exit(1)

    loop = asyncio.get_running_loop()

    print("\nMeshWeaver UDP Server Started")
    print(f"Server IP: {host}")
    print(f"Server Port: {port}")
    if dht_node:
        print(f"DHT Node ID: {dht_node.node_id.to_hex()}")
    if gossip_engine:
        print(f"Gossip Engine active (Interval: {gossip_engine.interval}s)")
        gossip_engine.start(loop=loop, sock=sock)
    print("Waiting for incoming messages...\n", flush=True)

    try:
        while True:
            try:
                # Asynchronously wait for incoming datagram
                data, addr = await loop.sock_recvfrom(sock, BUFFER_SIZE)
                if not data or not addr:
                    continue
                await handle_message(
                    loop, sock, data, addr, dht_node=dht_node, gossip_engine=gossip_engine
                )
            except asyncio.CancelledError:
                # Raised when the server task is cancelled during shutdown
                break
            except OSError as sock_err:
                # Handle unexpected socket error during reception
                print(f"[Warning] Socket communication error: {sock_err}")
                await asyncio.sleep(0.05)
    finally:
        if gossip_engine:
            gossip_engine.stop()
        if sock:
            sock.close()
            print("\nSocket successfully closed.")


def main() -> None:
    """Main application entry point."""
    try:
        asyncio.run(run_server(DEFAULT_HOST, DEFAULT_PORT))
    except KeyboardInterrupt:
        print("\nMeshWeaver UDP Server stopped cleanly.")


if __name__ == "__main__":
    main()

"""
MeshWeaver - Asynchronous UDP Client
Module 2 of Week 1: Core Networking Layer

This module implements an asynchronous UDP client using Python's built-in
`socket` and `asyncio` modules. It provides capabilities to dispatch datagrams,
await server responses with a configurable timeout, validate PING-PONG round trips,
handle network anomalies and timeouts gracefully, and ensure proper resource cleanup.

Week 1 Module 3/4 extension:
  Added send_task() and receive_task_result() methods to allow the client to
  submit serialized function payloads and receive their execution results from
  the server. All existing PING-PONG functionality is fully preserved.
"""

from __future__ import annotations

import argparse
import asyncio
import socket
import sys
from typing import Any, Callable, Dict, Tuple, Optional
import uuid

from serializer import (
    deserialize_result,
    is_result_payload,
    serialize_task,
)

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

        # Task correlation & demultiplexing state
        self._pending_tasks: Dict[str, asyncio.Future[Tuple[str, Any]]] = {}
        self._listener_task: Optional[asyncio.Task] = None
        self._text_response_queue: asyncio.Queue[Tuple[str, Tuple[str, int]]] = asyncio.Queue()
        self._last_task_id: Optional[str] = None

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
            self.sock.bind(("127.0.0.1", 0))
            self._is_open = True
            self._pending_tasks.clear()
            self._text_response_queue = asyncio.Queue()
        except OSError as err:
            print(f"[Client Error] Failed to create UDP socket: {err}")
            raise

    def close(self) -> None:
        """
        Closes the UDP socket and safely releases system resources and pending tasks.
        Cancels the background listener and sets ConnectionResetError on any unresolved tasks.
        """
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
        self._listener_task = None

        # Clean up all pending task futures
        for tid, fut in list(self._pending_tasks.items()):
            if not fut.done():
                fut.set_exception(ConnectionResetError(f"Client closed while awaiting task {tid}"))
        self._pending_tasks.clear()

        # Drain text response queue
        while not self._text_response_queue.empty():
            try:
                self._text_response_queue.get_nowait()
            except Exception:
                break

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

    def _ensure_listener(self) -> None:
        """
        Ensures that exactly one background asyncio listener task is active
        per AsyncUDPClient instance.
        """
        if self._listener_task is None or self._listener_task.done():
            try:
                loop = asyncio.get_running_loop()
                self._listener_task = loop.create_task(self._listen_loop())
            except RuntimeError:
                pass

    async def _listen_loop(self) -> None:
        """
        Single background demultiplexer loop per AsyncUDPClient instance.

        Receives incoming datagrams and routes them:
        - Task result envelopes -> matched by task_id to the registered Future in _pending_tasks.
        - Legacy task results without task_id -> assigned only if exactly 1 task is pending.
        - Plain-text messages -> pushed to _text_response_queue for receive_response().
        - Malformed, unknown, duplicate, or late responses -> dropped safely without crashing.
        """
        loop = asyncio.get_running_loop()
        while self._is_open and self.sock:
            try:
                raw, addr = await loop.sock_recvfrom(self.sock, BUFFER_SIZE)
                if not raw:
                    continue

                # Route 1: MeshWeaver Task Result Payload
                if is_result_payload(raw):
                    try:
                        status, value, task_id = deserialize_result(raw, with_task_id=True)
                    except Exception as parse_err:
                        # Safeguard 6: Malformed response must not crash listener
                        print(f"[Client Warning] Received malformed result payload from {addr}: {parse_err}")
                        continue

                    if task_id and task_id in self._pending_tasks:
                        fut = self._pending_tasks[task_id]
                        if not fut.done():
                            fut.set_result((status, value))
                        else:
                            # Safeguard 6: Duplicate response safely ignored
                            print(f"[Client Warning] Duplicate response received for task_id={task_id!r}. Dropping.")
                    elif task_id is None:
                        # Safeguard 2: Legacy response permitted only when exactly 1 task is pending
                        if len(self._pending_tasks) == 1:
                            fut = next(iter(self._pending_tasks.values()))
                            if not fut.done():
                                fut.set_result((status, value))
                        else:
                            print(
                                f"[Client Warning] Unidentified legacy response dropped because "
                                f"{len(self._pending_tasks)} tasks are pending."
                            )
                    else:
                        # Safeguard 6: Unknown, late, or timed-out task response safely dropped
                        print(f"[Client Warning] Received response for unknown or already completed task_id={task_id!r}. Dropping.")
                    continue

                # Route 2: Plain-text string responses (PING/PONG, error messages)
                try:
                    text = raw.decode("utf-8")
                    self._text_response_queue.put_nowait((text, addr))
                except UnicodeDecodeError:
                    print(f"[Client Warning] Unrecognized non-UTF8 datagram from {addr} ({len(raw)} bytes). Dropping.")

            except asyncio.CancelledError:
                break
            except ConnectionResetError as reset_err:
                # ICMP port unreachable notification on loopback
                for tid, fut in list(self._pending_tasks.items()):
                    if not fut.done():
                        fut.set_exception(reset_err)
                self._pending_tasks.clear()
                await asyncio.sleep(0.02)
            except OSError as err:
                if not self._is_open:
                    break
                print(f"[Client Warning] Socket error in listener loop: {err}")
                await asyncio.sleep(0.02)
            except Exception as unhandled_err:
                print(f"[Client Warning] Unexpected error in listener loop: {unhandled_err}")
                await asyncio.sleep(0.02)

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

        self._ensure_listener()
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
            ConnectionResetError: If the target port is unreachable.
            RuntimeError: If client socket is not open.
            OSError: On general socket communication failures.
        """
        if not self._is_open or not self.sock:
            raise RuntimeError("Client socket is not open. Call start() before receiving.")

        self._ensure_listener()
        try:
            data, addr = await asyncio.wait_for(
                self._text_response_queue.get(),
                timeout=self.timeout,
            )
            print(f"[Client] Received from {addr}: {data}")
            return data, addr
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

    async def send_task(
        self,
        func: Callable,
        args: Tuple = (),
        kwargs: Dict[str, Any] | None = None,
        task_id: Optional[str] = None,
    ) -> str:
        """
        Serialize a Python function and its arguments, register the task in the
        pending tasks correlation table, and transmit the payload to the server via UDP.

        Args:
            func:    Any Python callable (function, lambda, closure).
            args:    Positional arguments.
            kwargs:  Keyword arguments.
            task_id: Optional unique identifier; auto-generated if omitted.

        Returns:
            str: The unique task_id assigned to this submission.

        Raises:
            RuntimeError: If the socket is not initialized.
            ValueError:   If the payload exceeds the UDP size limit.
            OSError:      If transmission fails.
        """
        if not self._is_open or not self.sock:
            raise RuntimeError("Client socket is not open. Call start() before sending.")

        if kwargs is None:
            kwargs = {}

        if task_id is None:
            task_id = f"task_{uuid.uuid4().hex[:12]}"

        self._last_task_id = task_id

        # Register pending future before sending to prevent race conditions
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Tuple[str, Any]] = loop.create_future()
        self._pending_tasks[task_id] = fut
        self._ensure_listener()

        func_name = getattr(func, "__name__", repr(func))
        print(f"[Client] Serializing task [{task_id}]: {func_name}(args={args}, kwargs={kwargs})")

        try:
            payload = serialize_task(func, args=args, kwargs=kwargs, task_id=task_id)
        except (TypeError, ValueError, RuntimeError) as exc:
            # Safeguard 3: Clean up if serialization fails
            self._pending_tasks.pop(task_id, None)
            print(f"[Client Error] Task serialization failed: {exc}")
            raise

        print(f"[Client] Sending task [{task_id}] to {self.server_host}:{self.server_port} ({len(payload)} bytes)")
        try:
            await loop.sock_sendto(self.sock, payload, (self.server_host, self.server_port))
        except Exception as err:
            # Safeguard 3: Clean up if transmission fails
            self._pending_tasks.pop(task_id, None)
            print(f"[Client Error] Failed to send task payload: {err}")
            raise

        return task_id

    async def receive_task_result(
        self,
        task_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Tuple[str, Any]:
        """
        Awaits and returns the execution result corresponding to the specified task_id.

        Uses the background demultiplexer to match incoming responses by task_id.
        Supports out-of-order delivery, concurrent pipelining, and cleans up records
        on success, remote failure, timeout, or cancellation.

        Args:
            task_id: Optional task identifier to await. Defaults to the last sent task
                     or the single pending task if only one is outstanding.
            timeout: Optional per-task timeout in seconds. Defaults to self.timeout.

        Returns:
            Tuple[str, Any]: (status, value) where status is 'ok' or 'error'
                             and value is the function's return value or an
                             error description string.

        Raises:
            RuntimeError: If socket is not open or task target cannot be resolved.
            KeyError: If task_id is not found in pending tasks registry.
            TimeoutError: If no response arrives within the configured timeout.
        """
        if not self._is_open or not self.sock:
            raise RuntimeError("Client socket is not open. Call start() before receiving.")

        target_task_id = task_id
        if target_task_id is None:
            if self._last_task_id and self._last_task_id in self._pending_tasks:
                target_task_id = self._last_task_id
            elif len(self._pending_tasks) == 1:
                target_task_id = next(iter(self._pending_tasks.keys()))
            else:
                raise RuntimeError("No target task_id specified and multiple tasks are pending.")

        if target_task_id not in self._pending_tasks:
            raise KeyError(f"Task ID {target_task_id!r} is not in pending tasks registry.")

        fut = self._pending_tasks[target_task_id]
        effective_timeout = timeout if timeout is not None else self.timeout

        self._ensure_listener()
        try:
            status, value = await asyncio.wait_for(fut, timeout=effective_timeout)
            print(f"[Client] Task [{target_task_id}] result received: status={status!r}, value={value!r}")
            return status, value
        except asyncio.TimeoutError:
            print(f"[Client Error] Task [{target_task_id}] result timed out after {effective_timeout}s")
            raise TimeoutError(
                f"No result received for task {target_task_id} from {self.server_host}:{self.server_port} within {effective_timeout}s."
            )
        except ConnectionResetError as exc:
            print(f"[Client Error] Connection reset waiting for task [{target_task_id}] result.")
            raise exc
        finally:
            # Safeguard 3: Clean up pending task record on completion, error, timeout, or cancellation
            self._pending_tasks.pop(target_task_id, None)

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

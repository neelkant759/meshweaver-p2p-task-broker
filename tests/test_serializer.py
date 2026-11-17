"""
Tests for MeshWeaver task serialization and remote execution.
Covers Module 3 (serializer.py) and Module 4 (executor.py).

Test classes:
  TestSerializer          – unit tests for serializer.py
  TestExecutorLocal       – unit tests for executor.py (no network)
  TestTaskSocketIntegration – end-to-end: client serializes → server executes
                             → client receives result
"""

from __future__ import annotations

import asyncio
import socket
import unittest

from executor import execute_task, run_task_locally
from serializer import (
    TASK_MAGIC_PREFIX,
    deserialize_result,
    deserialize_task,
    is_result_payload,
    is_task_payload,
    serialize_result,
    serialize_task,
)
from udp_client import AsyncUDPClient
from udp_server import run_server


# ---------------------------------------------------------------------------
# Helper functions used by multiple tests
# ---------------------------------------------------------------------------

def add(x, y):
    """Simple addition – used to test serialization of named functions."""
    return x + y


def multiply(x, y):
    return x * y


def greet(name: str, greeting: str = "Hello") -> str:
    return f"{greeting}, {name}!"


def raise_value_error():
    raise ValueError("Intentional test error")


# ---------------------------------------------------------------------------
# Module 3: Serializer unit tests
# ---------------------------------------------------------------------------

class TestSerializer(unittest.TestCase):
    """Unit tests for serializer.py – no network required."""

    # --- serialize_task ---

    def test_serialize_simple_function(self):
        """Serializes a simple named function; result must be bytes with magic prefix."""
        payload = serialize_task(add, args=(10, 20))
        self.assertIsInstance(payload, bytes)
        self.assertTrue(payload.startswith(TASK_MAGIC_PREFIX))

    def test_serialize_lambda(self):
        """Serializes a lambda (not supported by standard pickle, requires cloudpickle)."""
        fn = lambda x: x * x
        payload = serialize_task(fn, args=(5,))
        self.assertIsInstance(payload, bytes)
        self.assertTrue(payload.startswith(TASK_MAGIC_PREFIX))

    def test_serialize_positional_args_preserved(self):
        """Positional arguments survive a serialize → deserialize round-trip."""
        payload = serialize_task(add, args=(7, 13))
        func, args, kwargs = deserialize_task(payload)
        self.assertEqual(args, (7, 13))
        self.assertEqual(func(*args, **kwargs), 20)

    def test_serialize_keyword_args_preserved(self):
        """Keyword arguments survive a serialize → deserialize round-trip."""
        payload = serialize_task(greet, args=("Alice",), kwargs={"greeting": "Hi"})
        func, args, kwargs = deserialize_task(payload)
        self.assertEqual(kwargs, {"greeting": "Hi"})
        self.assertEqual(func(*args, **kwargs), "Hi, Alice!")

    def test_serialize_no_args(self):
        """Function with no arguments serializes and executes correctly."""
        fn = lambda: 42
        payload = serialize_task(fn)
        func, args, kwargs = deserialize_task(payload)
        self.assertEqual(func(*args, **kwargs), 42)

    def test_serialize_non_callable_raises_type_error(self):
        """Passing a non-callable to serialize_task raises TypeError."""
        with self.assertRaises(TypeError):
            serialize_task(42)

    # --- deserialize_task ---

    def test_deserialize_returns_callable(self):
        """Deserialized function object must be callable."""
        payload = serialize_task(add, args=(1, 2))
        func, args, kwargs = deserialize_task(payload)
        self.assertTrue(callable(func))

    def test_deserialize_missing_magic_prefix_raises_value_error(self):
        """Payload without TASK_MAGIC_PREFIX raises ValueError."""
        with self.assertRaises(ValueError):
            deserialize_task(b"garbage bytes without prefix")

    def test_deserialize_corrupted_payload_raises_runtime_error(self):
        """Payload with correct prefix but corrupted body raises RuntimeError."""
        bad_payload = TASK_MAGIC_PREFIX + b"\x00\x01\x02\x03corrupted"
        with self.assertRaises(RuntimeError):
            deserialize_task(bad_payload)

    def test_deserialize_wrong_type_raises_value_error(self):
        """Passing a string instead of bytes to deserialize_task raises ValueError."""
        with self.assertRaises(ValueError):
            deserialize_task("not bytes")

    # --- serialize_result / deserialize_result ---

    def test_serialize_result_integer(self):
        """Serializes an integer result; can be deserialized back."""
        payload = serialize_result(30, status="ok")
        status, value = deserialize_result(payload)
        self.assertEqual(status, "ok")
        self.assertEqual(value, 30)

    def test_serialize_result_string(self):
        """Serializes a string result correctly."""
        payload = serialize_result("hello world", status="ok")
        status, value = deserialize_result(payload)
        self.assertEqual(status, "ok")
        self.assertEqual(value, "hello world")

    def test_serialize_result_error_status(self):
        """Serializes an error result with status 'error'."""
        payload = serialize_result("Something went wrong", status="error")
        status, value = deserialize_result(payload)
        self.assertEqual(status, "error")
        self.assertEqual(value, "Something went wrong")

    def test_serialize_result_invalid_status_raises_value_error(self):
        """Passing an invalid status string raises ValueError."""
        with self.assertRaises(ValueError):
            serialize_result(42, status="unknown")

    def test_deserialize_result_missing_prefix_raises_value_error(self):
        """Result payload without magic prefix raises ValueError."""
        with self.assertRaises(ValueError):
            deserialize_result(b"no prefix here")

    # --- is_task_payload / is_result_payload helpers ---

    def test_is_task_payload_true(self):
        payload = serialize_task(add, args=(1, 2))
        self.assertTrue(is_task_payload(payload))

    def test_is_task_payload_false_for_plain_text(self):
        self.assertFalse(is_task_payload(b"PING"))

    def test_is_result_payload_true(self):
        payload = serialize_result(99, status="ok")
        self.assertTrue(is_result_payload(payload))

    def test_is_result_payload_false_for_plain_text(self):
        self.assertFalse(is_result_payload(b"PONG"))


# ---------------------------------------------------------------------------
# Module 4: Executor unit tests (no network)
# ---------------------------------------------------------------------------

class TestExecutorLocal(unittest.TestCase):
    """Unit tests for executor.py using run_task_locally() – no UDP involved."""

    def test_addition(self):
        """Locally executes addition(10, 20) and expects 30."""
        status, result = run_task_locally(add, args=(10, 20))
        self.assertEqual(status, "ok")
        self.assertEqual(result, 30)

    def test_multiplication(self):
        """Locally executes multiplication(6, 7) and expects 42."""
        status, result = run_task_locally(multiply, args=(6, 7))
        self.assertEqual(status, "ok")
        self.assertEqual(result, 42)

    def test_multiple_arguments(self):
        """Locally executes a function with both positional and keyword args."""
        status, result = run_task_locally(greet, args=("Bob",), kwargs={"greeting": "Hey"})
        self.assertEqual(status, "ok")
        self.assertEqual(result, "Hey, Bob!")

    def test_string_returning_function(self):
        """Locally executes a function that returns a string."""
        status, result = run_task_locally(greet, args=("Alice",))
        self.assertEqual(status, "ok")
        self.assertEqual(result, "Hello, Alice!")

    def test_lambda_execution(self):
        """Locally executes a lambda expression."""
        fn = lambda a, b: a ** b
        status, result = run_task_locally(fn, args=(2, 10))
        self.assertEqual(status, "ok")
        self.assertEqual(result, 1024)

    def test_execution_error_returns_error_status(self):
        """When a function raises, run_task_locally returns status='error'."""
        status, result = run_task_locally(raise_value_error)
        self.assertEqual(status, "error")
        self.assertIn("ValueError", result)
        self.assertIn("Intentional test error", result)

    def test_execute_task_via_payload(self):
        """Full serialize → execute_task() pipeline returns a valid result payload."""
        payload = serialize_task(add, args=(100, 200))
        result_bytes = execute_task(payload)
        self.assertTrue(is_result_payload(result_bytes))
        status, value = deserialize_result(result_bytes)
        self.assertEqual(status, "ok")
        self.assertEqual(value, 300)

    def test_execute_task_bad_payload_returns_error(self):
        """execute_task() with corrupt payload returns an error result payload."""
        result_bytes = execute_task(b"corrupted payload bytes")
        self.assertTrue(is_result_payload(result_bytes))
        status, value = deserialize_result(result_bytes)
        self.assertEqual(status, "error")

    def test_execute_task_function_raises_returns_error(self):
        """execute_task() where the function raises returns an error result payload."""
        payload = serialize_task(raise_value_error)
        result_bytes = execute_task(payload)
        status, value = deserialize_result(result_bytes)
        self.assertEqual(status, "error")
        self.assertIn("ValueError", value)


# ---------------------------------------------------------------------------
# Integration test: client → server → result (over local UDP socket)
# ---------------------------------------------------------------------------

class TestTaskSocketIntegration(unittest.IsolatedAsyncioTestCase):
    """
    End-to-end integration: the client serializes a function, transmits it
    to the live UDP server task, the server executes it via execute_task(),
    and the client receives and deserializes the result.
    """

    async def asyncSetUp(self):
        """Spin up a live server on an ephemeral port for each test."""
        # Find a free port
        tmp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        tmp.bind(("127.0.0.1", 0))
        self.port = tmp.getsockname()[1]
        tmp.close()

        self.server_task = asyncio.create_task(run_server("127.0.0.1", self.port))
        await asyncio.sleep(0.05)

    async def asyncTearDown(self):
        self.server_task.cancel()
        try:
            await self.server_task
        except asyncio.CancelledError:
            pass

    async def test_remote_addition_via_socket(self):
        """
        Real UDP round-trip: client sends add(10, 20) → server executes → result = 30.
        This is an actual socket-based integration test, not a mock.
        """
        async with AsyncUDPClient(
            server_host="127.0.0.1", server_port=self.port, timeout=2.0
        ) as client:
            await client.send_task(add, args=(10, 20))
            status, value = await client.receive_task_result()

        self.assertEqual(status, "ok")
        self.assertEqual(value, 30)

    async def test_remote_multiplication_via_socket(self):
        """Real UDP round-trip: client sends multiply(6, 7) → server → result = 42."""
        async with AsyncUDPClient(
            server_host="127.0.0.1", server_port=self.port, timeout=2.0
        ) as client:
            await client.send_task(multiply, args=(6, 7))
            status, value = await client.receive_task_result()

        self.assertEqual(status, "ok")
        self.assertEqual(value, 42)

    async def test_remote_lambda_via_socket(self):
        """Real UDP round-trip: client sends a lambda → server → correct result."""
        fn = lambda x: x * x
        async with AsyncUDPClient(
            server_host="127.0.0.1", server_port=self.port, timeout=2.0
        ) as client:
            await client.send_task(fn, args=(9,))
            status, value = await client.receive_task_result()

        self.assertEqual(status, "ok")
        self.assertEqual(value, 81)

    async def test_remote_function_error_propagated(self):
        """Real UDP round-trip: function that raises; server returns error status."""
        async with AsyncUDPClient(
            server_host="127.0.0.1", server_port=self.port, timeout=2.0
        ) as client:
            await client.send_task(raise_value_error)
            status, value = await client.receive_task_result()

        self.assertEqual(status, "error")
        self.assertIn("ValueError", value)

    async def test_ping_pong_still_works_alongside_tasks(self):
        """PING-PONG health check is preserved after task routing was added to server."""
        async with AsyncUDPClient(
            server_host="127.0.0.1", server_port=self.port, timeout=2.0
        ) as client:
            success = await client.ping()

        self.assertTrue(success)


if __name__ == "__main__":
    unittest.main()

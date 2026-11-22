"""
MeshWeaver - Task Execution Registry & Duplicate Task Protection Test Suite
Week 3: Task Deduplication, Execution State Tracking & Result Caching Tests

Covers:
1. Unit tests for peek_task_id fast metadata inspection.
2. Unit tests for TaskExecutionRegistry lifecycle states (PENDING, RUNNING, COMPLETED, FAILED).
3. Concurrent duplicate request resolution via attached waiter futures.
4. Cached result retrieval and idempotent replay on repeated submissions.
5. Safe failure recording without blocking subsequent legitimate tasks.
6. Configurable TTL-based expiration and cleanup of completed records.
7. End-to-end UDP socket integration testing of worker duplicate suppression.
"""

from __future__ import annotations

import asyncio
import socket
import time
import unittest
from typing import Any, List, Tuple

from serializer import (
    deserialize_result,
    peek_task_id,
    serialize_result,
    serialize_task,
)
from task_registry import TaskExecutionRegistry, TaskRecord, TaskState
from udp_client import AsyncUDPClient
from udp_server import run_server


# Test callable helpers
_invocation_counter: int = 0


def reset_counter() -> None:
    global _invocation_counter
    _invocation_counter = 0


def tracked_computation(x: int, y: int) -> int:
    """Computation that increments an execution counter to detect duplicate calls."""
    global _invocation_counter
    _invocation_counter += 1
    return x * y + 10


def slow_computation(duration: float, result_value: Any) -> Any:
    """Simulates a long-running CPU task."""
    global _invocation_counter
    _invocation_counter += 1
    time.sleep(duration)
    return result_value


def failing_computation(message: str) -> None:
    """Deliberately raises an exception to verify failure caching."""
    global _invocation_counter
    _invocation_counter += 1
    raise RuntimeError(message)


class TestPeekTaskID(unittest.TestCase):
    """Unit tests for peek_task_id without unpickling callable function bytecode."""

    def test_peek_task_id_from_task_payload(self):
        payload = serialize_task(tracked_computation, args=(5, 6), task_id="task_peek_01")
        tid = peek_task_id(payload)
        self.assertEqual(tid, "task_peek_01")

    def test_peek_task_id_none_when_omitted(self):
        payload = serialize_task(tracked_computation, args=(1, 2), task_id=None)
        tid = peek_task_id(payload)
        self.assertIsNone(tid)

    def test_peek_task_id_from_result_payload(self):
        result_bytes = serialize_result(42, status="ok", task_id="task_result_peek_02")
        tid = peek_task_id(result_bytes)
        self.assertEqual(tid, "task_result_peek_02")

    def test_peek_task_id_invalid_payload(self):
        self.assertIsNone(peek_task_id(b"not_a_valid_payload"))
        self.assertIsNone(peek_task_id(b""))
        self.assertIsNone(peek_task_id(12345))  # type: ignore


class TestTaskExecutionRegistryUnit(unittest.IsolatedAsyncioTestCase):
    """Unit tests for TaskExecutionRegistry lifecycle states and concurrent synchronization."""

    async def asyncSetUp(self):
        self.registry = TaskExecutionRegistry(default_ttl=10.0)

    async def test_initial_registration_returns_running(self):
        """New task registration marks state as RUNNING with no waiters."""
        state, cached, waiter = self.registry.register_or_wait("task_unit_01")
        self.assertEqual(state, TaskState.RUNNING)
        self.assertIsNone(cached)
        self.assertIsNone(waiter)

        rec = self.registry.get_record("task_unit_01")
        self.assertIsNotNone(rec)
        self.assertEqual(rec.state, TaskState.RUNNING)

    async def test_duplicate_while_running_attaches_waiter(self):
        """A duplicate request for a RUNNING task attaches a waiter future."""
        state1, cached1, waiter1 = self.registry.register_or_wait("task_unit_02")
        self.assertEqual(state1, TaskState.RUNNING)
        self.assertIsNone(waiter1)

        # Duplicate arrives while original is RUNNING
        state2, cached2, waiter2 = self.registry.register_or_wait("task_unit_02")
        self.assertEqual(state2, TaskState.RUNNING)
        self.assertIsNone(cached2)
        self.assertIsNotNone(waiter2)
        self.assertFalse(waiter2.done())

        # Complete original task
        dummy_result = serialize_result("RESULT_OK", status="ok", task_id="task_unit_02")
        self.registry.complete_task("task_unit_02", dummy_result)

        # The waiter future must be resolved with the computed result
        resolved_bytes = await waiter2
        self.assertEqual(resolved_bytes, dummy_result)

    async def test_cached_result_retrieval_after_completion(self):
        """Submitting a completed task returns the cached result immediately."""
        tid = "task_unit_03"
        self.registry.register_or_wait(tid)
        result_payload = serialize_result({"key": "value"}, status="ok", task_id=tid)
        self.registry.complete_task(tid, result_payload)

        # Later duplicate request
        state, cached, waiter = self.registry.register_or_wait(tid)
        self.assertEqual(state, TaskState.COMPLETED)
        self.assertEqual(cached, result_payload)
        self.assertIsNone(waiter)

    async def test_failed_task_caching_and_subsequent_tasks(self):
        """Failed tasks cache the error payload and do not block subsequent new tasks."""
        tid_fail = "task_fail_01"
        self.registry.register_or_wait(tid_fail)
        error_payload = serialize_result("Internal computation error", status="error", task_id=tid_fail)
        self.registry.fail_task(tid_fail, error_payload)

        # Duplicate query returns cached error
        state, cached, waiter = self.registry.register_or_wait(tid_fail)
        self.assertEqual(state, TaskState.FAILED)
        self.assertEqual(cached, error_payload)
        self.assertIsNone(waiter)

        # Subsequent different task executes normally
        tid_new = "task_success_after_fail"
        state_new, cached_new, waiter_new = self.registry.register_or_wait(tid_new)
        self.assertEqual(state_new, TaskState.RUNNING)
        self.assertIsNone(cached_new)
        self.assertIsNone(waiter_new)

    async def test_ttl_cleanup_expired_records(self):
        """Completed records older than TTL are pruned; active ones remain."""
        t0 = 1000.0
        self.registry.register_or_wait("task_expire_old")
        self.registry.complete_task("task_expire_old", b"res_old")
        # Force completed_at to t0
        rec_old = self.registry.get_record("task_expire_old")
        rec_old.completed_at = t0

        self.registry.register_or_wait("task_fresh")
        self.registry.complete_task("task_fresh", b"res_fresh")
        rec_fresh = self.registry.get_record("task_fresh")
        rec_fresh.completed_at = t0 + 8.0  # 8s later

        # Still running task (never pruned)
        self.registry.register_or_wait("task_in_progress")

        # Cleanup at t0 + 10.0 (TTL = 5.0s)
        pruned = self.registry.cleanup_expired(ttl_seconds=5.0, now=t0 + 10.0)
        self.assertEqual(pruned, 1)

        self.assertIsNone(self.registry.get_record("task_expire_old"))
        self.assertIsNotNone(self.registry.get_record("task_fresh"))
        self.assertIsNotNone(self.registry.get_record("task_in_progress"))


class TestTaskRegistrySocketIntegration(unittest.IsolatedAsyncioTestCase):
    """
    Live UDP socket integration tests verifying server-side duplicate suppression,
    concurrent request deduplication, and cached result replay.
    """

    async def asyncSetUp(self):
        reset_counter()
        # Find free loopback port
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(("127.0.0.1", 0))
        self.port = s.getsockname()[1]
        s.close()

        self.registry = TaskExecutionRegistry(default_ttl=60.0)
        self.server_task = asyncio.create_task(
            run_server("127.0.0.1", self.port, task_registry=self.registry)
        )
        await asyncio.sleep(0.08)

    async def asyncTearDown(self):
        self.server_task.cancel()
        try:
            await self.server_task
        except asyncio.CancelledError:
            pass

    async def test_duplicate_submission_after_completion_returns_cached_result(self):
        """
        Verify that submitting the same task_id after completion returns the cached
        result without re-executing the underlying function.
        """
        tid = "task_dup_e2e_01"
        async with AsyncUDPClient("127.0.0.1", self.port, timeout=3.0) as client:
            # 1. Initial submission
            await client.send_task(tracked_computation, args=(7, 8), task_id=tid)
            status1, val1 = await client.receive_task_result(task_id=tid)
            self.assertEqual(status1, "ok")
            self.assertEqual(val1, 66)  # 7 * 8 + 10
            self.assertEqual(_invocation_counter, 1)

            # 2. Resubmit identical task_id
            await client.send_task(tracked_computation, args=(7, 8), task_id=tid)
            status2, val2 = await client.receive_task_result(task_id=tid)
            self.assertEqual(status2, "ok")
            self.assertEqual(val2, 66)
            # Must NOT have re-executed the function!
            self.assertEqual(_invocation_counter, 1)

    async def test_same_task_submitted_concurrently_executes_once(self):
        """
        Safeguard 4 & 5: When the same task_id is submitted concurrently from multiple
        requests while in-flight, the server executes the callable exactly once and
        distributes the result to all callers.
        """
        tid = "task_concurrent_dup_02"

        async def send_and_wait():
            async with AsyncUDPClient("127.0.0.1", self.port, timeout=4.0) as client:
                await client.send_task(slow_computation, args=(0.15, "CONCURRENT_OK"), task_id=tid)
                return await client.receive_task_result(task_id=tid)

        # Fire 3 concurrent submissions of the SAME task_id
        results = await asyncio.gather(send_and_wait(), send_and_wait(), send_and_wait())

        for status, val in results:
            self.assertEqual(status, "ok")
            self.assertEqual(val, "CONCURRENT_OK")

        # The slow function must have been invoked exactly ONCE!
        self.assertEqual(_invocation_counter, 1)

    async def test_failed_task_cached_and_replayed(self):
        """
        When a task deliberately raises an exception, the error result is cached
        and replayed for subsequent duplicate submissions without re-executing.
        """
        tid = "task_error_dup_03"
        async with AsyncUDPClient("127.0.0.1", self.port, timeout=3.0) as client:
            await client.send_task(failing_computation, args=("Intentional Failure",), task_id=tid)
            status1, err1 = await client.receive_task_result(task_id=tid)
            self.assertEqual(status1, "error")
            self.assertIn("Intentional Failure", err1)
            self.assertEqual(_invocation_counter, 1)

            # Resubmit the same failing task_id
            await client.send_task(failing_computation, args=("Intentional Failure",), task_id=tid)
            status2, err2 = await client.receive_task_result(task_id=tid)
            self.assertEqual(status2, "error")
            self.assertEqual(err1, err2)
            # Must NOT have re-executed the failing function!
            self.assertEqual(_invocation_counter, 1)

    async def test_distinct_task_ids_execute_independently(self):
        """
        Multiple concurrent requests with different task_ids execute in parallel
        and do not interfere with each other's deduplication state.
        """
        task_count = 5

        async def run_distinct(i: int):
            tid = f"distinct_task_{i}"
            async with AsyncUDPClient("127.0.0.1", self.port, timeout=3.0) as client:
                await client.send_task(tracked_computation, args=(i, 10), task_id=tid)
                status, val = await client.receive_task_result(task_id=tid)
                self.assertEqual(status, "ok")
                self.assertEqual(val, i * 10 + 10)
                return val

        await asyncio.gather(*(run_distinct(i) for i in range(task_count)))
        # All 5 distinct tasks must have executed
        self.assertEqual(_invocation_counter, task_count)
        self.assertEqual(self.registry.total_records(), task_count)

    async def test_legacy_tasks_without_task_id_supported(self):
        """
        Tasks submitted without a task_id execute directly without registry tracking
        for full backward compatibility.
        """
        raw_payload = serialize_task(tracked_computation, args=(3, 4), task_id=None)
        loop = asyncio.get_running_loop()
        cli_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cli_sock.setblocking(False)
        cli_sock.bind(("127.0.0.1", 0))
        try:
            await loop.sock_sendto(cli_sock, raw_payload, ("127.0.0.1", self.port))
            resp_raw, _ = await asyncio.wait_for(loop.sock_recvfrom(cli_sock, 4096), timeout=3.0)
            status, val = deserialize_result(resp_raw)
            self.assertEqual(status, "ok")
            self.assertEqual(val, 22)  # 3 * 4 + 10
        finally:
            cli_sock.close()


if __name__ == "__main__":
    unittest.main()

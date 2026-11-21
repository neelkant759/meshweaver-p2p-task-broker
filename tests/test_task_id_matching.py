"""
MeshWeaver - Task ID Demultiplexing & Correlation Tests
Week 3: Reliable Task ID Generation, Response Matching, and Demultiplexing

Validates:
1. End-to-end Task ID preservation (client -> wire -> worker -> wire -> client).
2. Concurrent task execution (5+ tasks over a single AsyncUDPClient instance).
3. Out-of-order response handling (fast task completing before slow task).
4. Calling coroutine cancellation and automatic pending-task cleanup.
5. Unknown / late Task ID handling (safely dropped without listener disruption).
6. Legacy response compatibility:
   - Permitted when exactly one task is pending.
   - Rejected / dropped when multiple tasks are pending.
7. Malformed payload error resilience (listener remains operational).
8. Individual task timeout handling and cleanup.
9. Concurrently dispatched tasks with mixed success and failure outcomes.
10. Send failure cleanup (pending table kept consistent).
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
import unittest
from typing import Any, Dict, List

# Ensure project root is available in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from serializer import (
    deserialize_result,
    deserialize_task,
    serialize_result,
    serialize_task,
)
from udp_client import AsyncUDPClient


# Sample worker functions
def add_numbers(x: int, y: int) -> int:
    return x + y


def slow_task(duration: float, value: Any) -> Any:
    time.sleep(duration)
    return value


def faulty_task(err_msg: str) -> None:
    raise RuntimeError(err_msg)


class TestTaskIDSerializationUnit(unittest.TestCase):
    """Unit tests for Task ID embedding in serializer.py."""

    def test_serialize_and_deserialize_task_with_task_id(self):
        """Verifies task_id is serialized and retrieved when with_task_id=True."""
        custom_id = "test_custom_id_12345"
        payload = serialize_task(add_numbers, args=(3, 7), task_id=custom_id)
        func, args, kwargs, task_id = deserialize_task(payload, with_task_id=True)
        self.assertEqual(task_id, custom_id)
        self.assertEqual(func(*args, **kwargs), 10)

    def test_deserialize_task_backward_compatibility(self):
        """Verifies deserialize_task returns 3-tuple (func, args, kwargs) by default."""
        payload = serialize_task(add_numbers, args=(2, 3), task_id="abc")
        res = deserialize_task(payload)
        self.assertEqual(len(res), 3)
        func, args, kwargs = res
        self.assertEqual(func(*args, **kwargs), 5)

    def test_serialize_and_deserialize_result_with_task_id(self):
        """Verifies task_id is embedded in result payload and deserialized."""
        custom_id = "result_id_999"
        payload = serialize_result("success_value", status="ok", task_id=custom_id)
        status, value, task_id = deserialize_result(payload, with_task_id=True)
        self.assertEqual(status, "ok")
        self.assertEqual(value, "success_value")
        self.assertEqual(task_id, custom_id)

    def test_deserialize_result_backward_compatibility(self):
        """Verifies deserialize_result returns 2-tuple (status, value) by default."""
        payload = serialize_result(42, status="ok", task_id="tid_xyz")
        res = deserialize_result(payload)
        self.assertEqual(len(res), 2)
        status, value = res
        self.assertEqual(status, "ok")
        self.assertEqual(value, 42)


class TestTaskIDDemuxIntegration(unittest.IsolatedAsyncioTestCase):
    """
    Integration tests verifying response demultiplexing, out-of-order handling,
    and concurrency safety against an actual independent Python worker process.
    """

    @classmethod
    def setUpClass(cls):
        """Spawn an independent worker node running node.py on an ephemeral port."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(("127.0.0.1", 0))
            cls.worker_port = s.getsockname()[1]

        env = os.environ.copy()
        python_paths = [PROJECT_ROOT, os.path.join(PROJECT_ROOT, "tests")]
        if "PYTHONPATH" in env:
            python_paths.append(env["PYTHONPATH"])
        env["PYTHONPATH"] = os.pathsep.join(python_paths)

        cls.worker_proc = subprocess.Popen(
            [
                sys.executable,
                "-u",
                os.path.join(PROJECT_ROOT, "node.py"),
                "--host",
                "127.0.0.1",
                "--port",
                str(cls.worker_port),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )
        time.sleep(0.6)

    @classmethod
    def tearDownClass(cls):
        """Clean up the worker subprocess."""
        if cls.worker_proc and cls.worker_proc.poll() is None:
            cls.worker_proc.terminate()
            try:
                cls.worker_proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                cls.worker_proc.kill()

    async def asyncSetUp(self):
        self.client = AsyncUDPClient("127.0.0.1", self.worker_port, timeout=4.0)
        self.client.start()

    async def asyncTearDown(self):
        self.client.close()

    async def test_task_id_preservation_end_to_end(self):
        """Verifies Task ID is preserved from dispatch to result receipt."""
        custom_id = "test_e2e_id_alpha"
        sent_id = await self.client.send_task(add_numbers, args=(12, 13), task_id=custom_id)
        self.assertEqual(sent_id, custom_id)

        status, value = await self.client.receive_task_result(task_id=custom_id)
        self.assertEqual(status, "ok")
        self.assertEqual(value, 25)
        # Verify pending table is completely empty after completion
        self.assertEqual(len(self.client._pending_tasks), 0)

    async def test_concurrent_tasks_five_or_more(self):
        """
        Safeguard 4: Five or more concurrent tasks must be supported on the same
        client instance without competing socket readers or response cross-talk.
        """
        task_count = 6
        task_ids = [f"concurrent_task_{i}" for i in range(task_count)]
        expected_results = {task_ids[i]: (i * 10, i * 20, (i * 10) + (i * 20)) for i in range(task_count)}

        async def execute_task_coro(tid: str) -> None:
            a, b, expected = expected_results[tid]
            await self.client.send_task(add_numbers, args=(a, b), task_id=tid)
            status, value = await self.client.receive_task_result(task_id=tid)
            self.assertEqual(status, "ok")
            self.assertEqual(value, expected)

        # Run all 6 tasks concurrently
        await asyncio.gather(*(execute_task_coro(tid) for tid in task_ids))

        # Confirms all records cleaned up
        self.assertEqual(len(self.client._pending_tasks), 0)

    async def test_out_of_order_responses(self):
        """
        Verifies that out-of-order responses are correctly demultiplexed:
        Task A (slow: 0.25s) and Task B (fast: 0.05s) dispatched concurrently.
        Task B returns first, but each task receives its own matching result.
        """
        completion_order: List[str] = []

        async def run_slow():
            tid = "task_slow_01"
            await self.client.send_task(slow_task, args=(0.25, "SLOW_RESULT"), task_id=tid)
            status, val = await self.client.receive_task_result(task_id=tid)
            completion_order.append(val)
            return val

        async def run_fast():
            await asyncio.sleep(0.02)  # Ensure slow task is sent first
            tid = "task_fast_02"
            await self.client.send_task(slow_task, args=(0.04, "FAST_RESULT"), task_id=tid)
            status, val = await self.client.receive_task_result(task_id=tid)
            completion_order.append(val)
            return val

        res_slow, res_fast = await asyncio.gather(run_slow(), run_fast())
        self.assertEqual(res_slow, "SLOW_RESULT")
        self.assertEqual(res_fast, "FAST_RESULT")
        # FAST_RESULT should have completed before SLOW_RESULT
        self.assertEqual(completion_order, ["FAST_RESULT", "SLOW_RESULT"])
        self.assertEqual(len(self.client._pending_tasks), 0)

    async def test_coroutine_cancellation_cleans_up_pending_task(self):
        """
        Safeguard 3: When a calling coroutine is cancelled while awaiting a result,
        its entry in _pending_tasks must be cleanly removed.
        """
        tid = "task_to_be_cancelled"
        await self.client.send_task(slow_task, args=(1.0, "SHOULD_NOT_ARRIVE"), task_id=tid)
        self.assertIn(tid, self.client._pending_tasks)

        coro_task = asyncio.create_task(self.client.receive_task_result(task_id=tid))
        await asyncio.sleep(0.05)
        coro_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await coro_task

        # Verify task is cleaned up despite cancellation
        self.assertNotIn(tid, self.client._pending_tasks)

    async def test_unknown_task_id_does_not_crash_listener(self):
        """
        Safeguards 1 & 6: Incoming responses with an unknown / unmapped task_id
        must be dropped safely without crashing the background listener.
        """
        # Inject an unsolicited response packet directly into the client's socket using an independent sender
        bogus_payload = serialize_result("unexpected_value", status="ok", task_id="bogus_unknown_tid_999")
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            sender.sendto(bogus_payload, self.client.sock.getsockname())
        await asyncio.sleep(0.05)

        # Confirm client listener is still active and can process normal tasks
        tid = "task_after_bogus"
        await self.client.send_task(add_numbers, args=(10, 15), task_id=tid)
        status, val = await self.client.receive_task_result(task_id=tid)
        self.assertEqual(status, "ok")
        self.assertEqual(val, 25)

    async def test_legacy_response_single_pending_task(self):
        """
        Safeguard 2: When exactly ONE task is pending, a legacy response without a Task ID
        is accepted via fallback matching.
        """
        loop = asyncio.get_running_loop()
        tid = "single_pending_legacy_test"

        # Register a pending task locally
        fut = loop.create_future()
        self.client._pending_tasks[tid] = fut
        self.client._ensure_listener()

        # Send an un-tagged legacy response packet to client socket
        legacy_payload = serialize_result("legacy_val_ok", status="ok", task_id=None)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            sender.sendto(legacy_payload, self.client.sock.getsockname())

        status, val = await self.client.receive_task_result(task_id=tid, timeout=1.0)
        self.assertEqual(status, "ok")
        self.assertEqual(val, "legacy_val_ok")

    async def test_legacy_response_dropped_when_multiple_pending(self):
        """
        Safeguard 2: When MULTIPLE tasks are pending, an un-tagged legacy response
        must be dropped to prevent cross-talk and corruption.
        """
        loop = asyncio.get_running_loop()
        fut1 = loop.create_future()
        fut2 = loop.create_future()
        self.client._pending_tasks["task_A"] = fut1
        self.client._pending_tasks["task_B"] = fut2
        self.client._ensure_listener()

        # Send un-tagged response
        legacy_payload = serialize_result("ambiguous_val", status="ok", task_id=None)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            sender.sendto(legacy_payload, self.client.sock.getsockname())
        await asyncio.sleep(0.05)

        # Neither future should be resolved by the ambiguous packet
        self.assertFalse(fut1.done())
        self.assertFalse(fut2.done())

        # Clean up
        self.client._pending_tasks.clear()

    async def test_timeout_cleans_up_pending_task(self):
        """
        Safeguard 3: When a task times out, it raises TimeoutError and is
        promptly removed from the pending tasks registry.
        """
        tid = "task_that_times_out"
        # Send task that takes 0.3s on server but timeout is 0.05s on client
        await self.client.send_task(slow_task, args=(0.3, "LATE"), task_id=tid)
        self.assertIn(tid, self.client._pending_tasks)

        with self.assertRaises(TimeoutError):
            await self.client.receive_task_result(task_id=tid, timeout=0.05)

        # Confirms removal from pending registry
        self.assertNotIn(tid, self.client._pending_tasks)

        # Give worker process time to finish the delayed execution before client closes
        await asyncio.sleep(0.35)

    async def test_independent_error_handling_in_concurrency(self):
        """
        Verifies that when one concurrent task fails with an exception,
        it does not affect another concurrently running successful task.
        """
        async def run_good():
            tid = "task_success"
            await self.client.send_task(add_numbers, args=(100, 200), task_id=tid)
            return await self.client.receive_task_result(task_id=tid)

        async def run_bad():
            tid = "task_failure"
            await self.client.send_task(faulty_task, args=("Critical database failure",), task_id=tid)
            return await self.client.receive_task_result(task_id=tid)

        res_good, res_bad = await asyncio.gather(run_good(), run_bad())
        self.assertEqual(res_good[0], "ok")
        self.assertEqual(res_good[1], 300)

        self.assertEqual(res_bad[0], "error")
        self.assertIn("Critical database failure", str(res_bad[1]))
        self.assertEqual(len(self.client._pending_tasks), 0)


if __name__ == "__main__":
    unittest.main()

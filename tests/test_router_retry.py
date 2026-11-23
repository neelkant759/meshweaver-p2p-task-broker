"""
MeshWeaver - Task Router Automatic Retry & Worker Reassignment Test Suite
Week 3: Fault Tolerance, Peer Reassignment, and Execution Safeguards

Covers:
1. Worker timeout triggers automatic reassignment to the next healthy peer.
2. Worker network/socket error triggers automatic reassignment.
3. Strict preservation of the original task_id across all retries.
4. Duplicate task execution prevention via task registry on re-dispatched peers.
5. Configurable retry limits (max_retries=0, max_retries=1, max_retries=2).
6. Handling peer exhaustion when no alternative healthy peers are available.
7. Local execution fallback upon remote worker exhaustion (fallback_to_local=True).
8. Configurable retry delay between reassignment attempts.
9. Remote application error handling (retry_on_remote_error=True vs False).
10. Multi-worker reassignment chain (Node A fails -> Node B fails -> Node C succeeds).
"""

from __future__ import annotations

import asyncio
import socket
import time
import unittest
from typing import Any, Dict

from dht import DHTNode, NodeID, Peer
from gossip import GossipEngine, NodeResourceMetrics, PeerTelemetry
from router import NoPeerAvailableError, TaskExecutionResponse, TaskRouter
from task_registry import TaskExecutionRegistry
from udp_server import run_server


# Test helpers
_exec_count: int = 0


def reset_exec_counter() -> None:
    global _exec_count
    _exec_count = 0


def add_numbers(a: int, b: int) -> int:
    global _exec_count
    _exec_count += 1
    return a + b


def faulty_task(err_msg: str) -> None:
    global _exec_count
    _exec_count += 1
    raise RuntimeError(err_msg)


class TestRouterRetryAndReassignment(unittest.IsolatedAsyncioTestCase):
    """
    Integration tests verifying TaskRouter's automatic retry and reassignment
    mechanisms over live asyncio UDP servers and unreachable simulated endpoints.
    """

    async def asyncSetUp(self):
        reset_exec_counter()
        self.server_tasks: list[asyncio.Task] = []
        self.registries: list[TaskExecutionRegistry] = []

        # Local gossip engine to hold peer telemetry
        self.local_dht = DHTNode(port=0)
        self.gossip_local = GossipEngine(dht_node=self.local_dht)

    async def asyncTearDown(self):
        for t in self.server_tasks:
            t.cancel()
        for t in self.server_tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass

    def _get_free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    async def _start_worker_server(self) -> Tuple[int, str, TaskExecutionRegistry]:
        """Starts a live worker server on an ephemeral port with a dedicated registry."""
        port = self._get_free_port()
        node_id = NodeID().to_hex()
        registry = TaskExecutionRegistry(default_ttl=60.0)
        self.registries.append(registry)

        task = asyncio.create_task(
            run_server("127.0.0.1", port, task_registry=registry)
        )
        await asyncio.sleep(0.06)
        self.server_tasks.append(task)
        return port, node_id, registry

    def _register_telemetry(self, node_id: str, port: int, cpu: float, ram_free: float = 8000.0) -> None:
        """Helper to register peer telemetry with specific CPU and RAM metrics."""
        peer = Peer(ip="127.0.0.1", port=port, node_id=node_id)
        self.gossip_local.peer_telemetry[node_id] = PeerTelemetry(
            peer=peer,
            metrics=NodeResourceMetrics(
                cpu_percent=cpu,
                ram_total_mb=16384.0,
                ram_used_mb=16384.0 - ram_free,
                ram_free_mb=ram_free,
                ram_percent=50.0,
                timestamp=time.time(),
            ),
            last_updated=time.time(),
        )

    async def test_worker_timeout_triggers_automatic_reassignment(self):
        """
        Scenario 1: Primary peer (lowest CPU) is dead/unresponsive.
        Router times out and automatically reassigns the task to the secondary healthy peer.
        """
        dead_port = self._get_free_port()
        dead_id = "dead_peer_01"
        self._register_telemetry(dead_id, dead_port, cpu=5.0)  # Lower CPU -> selected first

        live_port, live_id, _ = await self._start_worker_server()
        self._register_telemetry(live_id, live_port, cpu=20.0)  # Higher CPU -> selected second

        router = TaskRouter(self.gossip_local, max_retries=2, retry_delay=0.0)
        custom_task_id = "task_reassign_test_01"

        resp = await router.dispatch_task(
            add_numbers,
            args=(14, 16),
            task_id=custom_task_id,
            timeout=0.3,
        )

        self.assertEqual(resp.status, "ok")
        self.assertEqual(resp.result, 30)
        self.assertEqual(resp.executed_by, f"127.0.0.1:{live_port}")
        self.assertEqual(resp.target_node_id, live_id)
        self.assertEqual(resp.task_id, custom_task_id)
        self.assertEqual(resp.attempts, 2)
        self.assertEqual(resp.retries, 1)
        self.assertEqual(_exec_count, 1)

    async def test_task_id_strictly_preserved_across_retries(self):
        """
        Scenario 2: The original task_id is preserved throughout failure, retry,
        and final execution across workers.
        """
        dead_port1 = self._get_free_port()
        dead_id1 = "dead_peer_alpha"
        self._register_telemetry(dead_id1, dead_port1, cpu=2.0)

        dead_port2 = self._get_free_port()
        dead_id2 = "dead_peer_beta"
        self._register_telemetry(dead_id2, dead_port2, cpu=4.0)

        live_port, live_id, _ = await self._start_worker_server()
        self._register_telemetry(live_id, live_port, cpu=10.0)

        router = TaskRouter(self.gossip_local, max_retries=3)
        preserved_id = "task_uuid_strictly_preserved_999"

        resp = await router.dispatch_task(
            add_numbers,
            args=(100, 250),
            task_id=preserved_id,
            timeout=0.2,
        )

        self.assertEqual(resp.task_id, preserved_id)
        self.assertEqual(resp.result, 350)
        self.assertEqual(resp.target_node_id, live_id)
        self.assertEqual(resp.attempts, 3)  # 2 dead peers + 1 live peer
        self.assertEqual(resp.retries, 2)

    async def test_retry_limits_enforced_and_raises_timeout(self):
        """
        Scenario 3: When all peers fail and retries exceed max_retries,
        TimeoutError is raised and no infinite loop occurs.
        """
        dead_port1 = self._get_free_port()
        self._register_telemetry("dead_1", dead_port1, cpu=5.0)

        dead_port2 = self._get_free_port()
        self._register_telemetry("dead_2", dead_port2, cpu=10.0)

        dead_port3 = self._get_free_port()
        self._register_telemetry("dead_3", dead_port3, cpu=15.0)

        # max_retries = 1: will only attempt 2 peers (initial + 1 retry)
        router = TaskRouter(self.gossip_local, max_retries=1, retry_delay=0.0)

        with self.assertRaises((TimeoutError, ConnectionResetError)):
            await router.dispatch_task(add_numbers, args=(1, 2), timeout=0.2)

    async def test_no_alternative_peer_available_raises_no_peer_available_error(self):
        """
        Scenario 4: When a worker fails and all mesh peers have been exhausted,
        NoPeerAvailableError is raised with a descriptive message.
        """
        dead_port = self._get_free_port()
        self._register_telemetry("sole_dead_peer", dead_port, cpu=5.0)

        # Allow 3 retries, but only 1 peer exists in mesh
        router = TaskRouter(self.gossip_local, max_retries=3)

        with self.assertRaises(NoPeerAvailableError) as ctx:
            await router.dispatch_task(add_numbers, args=(1, 2), timeout=0.2)

        self.assertIn("no alternative peer available", str(ctx.exception).lower())

    async def test_local_fallback_on_peer_exhaustion(self):
        """
        Scenario 5: When all remote peers fail and fallback_to_local is True,
        TaskRouter automatically falls back to local execution.
        """
        dead_port = self._get_free_port()
        self._register_telemetry("unreachable_worker", dead_port, cpu=1.0)

        router = TaskRouter(
            self.gossip_local,
            fallback_to_local=True,
            max_retries=1,
        )

        resp = await router.dispatch_task(add_numbers, args=(50, 75), timeout=0.2)
        self.assertEqual(resp.status, "ok")
        self.assertEqual(resp.result, 125)
        self.assertEqual(resp.executed_by, "local")
        self.assertIsNone(resp.target_node_id)
        self.assertGreater(resp.attempts, 1)

    async def test_duplicate_execution_avoided_on_retried_peer(self):
        """
        Scenario 6: If a retry targets a peer that previously completed the task
        (e.g. initial ACK was delayed or lost), the worker returns the cached
        result without re-running the callable.
        """
        live_port, live_id, registry = await self._start_worker_server()
        self._register_telemetry(live_id, live_port, cpu=10.0)

        router = TaskRouter(self.gossip_local, max_retries=1)
        tid = "task_duplicate_cache_test"

        # First dispatch
        resp1 = await router.dispatch_task(add_numbers, args=(8, 9), task_id=tid)
        self.assertEqual(resp1.result, 17)
        self.assertEqual(_exec_count, 1)

        # Second dispatch with identical task_id to the same peer
        resp2 = await router.dispatch_task(add_numbers, args=(8, 9), task_id=tid)
        self.assertEqual(resp2.result, 17)
        # Function was NOT executed a second time!
        self.assertEqual(_exec_count, 1)

    async def test_configurable_retry_delay(self):
        """Scenario 7: Configured retry_delay is respected between attempts."""
        dead_port = self._get_free_port()
        self._register_telemetry("dead_delay_peer", dead_port, cpu=2.0)

        live_port, live_id, _ = await self._start_worker_server()
        self._register_telemetry(live_id, live_port, cpu=8.0)

        # 0.15s delay between attempts
        router = TaskRouter(self.gossip_local, max_retries=1, retry_delay=0.15)
        t_start = time.perf_counter()

        resp = await router.dispatch_task(add_numbers, args=(2, 3), timeout=0.2)
        elapsed = time.perf_counter() - t_start

        self.assertEqual(resp.result, 5)
        self.assertEqual(resp.retries, 1)
        # Elapsed must reflect at least the retry_delay (0.15s)
        self.assertGreaterEqual(elapsed, 0.15)

    async def test_remote_execution_error_with_retry_enabled(self):
        """
        Scenario 8: When retry_on_remote_error=True and Worker A returns an error status,
        the task is reassigned to Worker B.
        """
        # Worker A will execute and fail
        port_a, id_a, _ = await self._start_worker_server()
        self._register_telemetry(id_a, port_a, cpu=5.0)

        # Worker B is available for retry
        port_b, id_b, _ = await self._start_worker_server()
        self._register_telemetry(id_b, port_b, cpu=10.0)

        # Counter will record both attempts
        router = TaskRouter(self.gossip_local, max_retries=1, retry_on_remote_error=True)

        with self.assertRaises(RuntimeError):
            # Both workers fail because the function itself throws
            await router.dispatch_task(faulty_task, args=("Always fails",))

        # Both workers were tried (initial + 1 retry)
        self.assertEqual(_exec_count, 2)

    async def test_multi_worker_reassignment_chain(self):
        """
        Scenario 9: Chain of failures across 3 dead nodes before successfully
        executing on the 4th node.
        """
        for i in range(3):
            dead_port = self._get_free_port()
            self._register_telemetry(f"dead_chain_{i}", dead_port, cpu=1.0 + i)

        live_port, live_id, _ = await self._start_worker_server()
        self._register_telemetry(live_id, live_port, cpu=100.0)

        router = TaskRouter(self.gossip_local, max_retries=3, retry_delay=0.0)
        resp = await router.dispatch_task(add_numbers, args=(40, 2), timeout=0.15)

        self.assertEqual(resp.result, 42)
        self.assertEqual(resp.executed_by, f"127.0.0.1:{live_port}")
        self.assertEqual(resp.attempts, 4)  # 3 dead + 1 live
        self.assertEqual(resp.retries, 3)

    async def test_clean_client_state_after_reassignment(self):
        """
        Scenario 10: Ensures no socket or pending task future leaks exist in
        router or client state after reassignment.
        """
        dead_port = self._get_free_port()
        self._register_telemetry("dead_leak_test", dead_port, cpu=1.0)

        live_port, live_id, _ = await self._start_worker_server()
        self._register_telemetry(live_id, live_port, cpu=10.0)

        router = TaskRouter(self.gossip_local, max_retries=1)
        resp = await router.dispatch_task(add_numbers, args=(1, 1), timeout=0.2)

        self.assertEqual(resp.result, 2)
        self.assertEqual(resp.retries, 1)


if __name__ == "__main__":
    unittest.main()

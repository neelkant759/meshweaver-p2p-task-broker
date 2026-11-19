"""
MeshWeaver - Task Router & Intelligent Peer Selection Test Suite
Week 3: Decentralized Task Scheduling Layer (Part 1: Peer Selection)

Covers:
1. Lowest CPU peer selection among multiple active candidates.
2. RAM-based tie-breaking when CPU metrics are identical.
3. Deterministic NodeID tie-breaking when CPU and RAM metrics are identical.
4. Stale peer rejection using configurable staleness thresholds.
5. Peer exclusion filtering (case-insensitive).
6. Error handling when no eligible peers exist (NoPeerAvailableError).
7. Local execution fallback when fallback_to_local is True.
8. Empty telemetry handling.
9. Asynchronous method invocations.
"""

from __future__ import annotations

import asyncio
import time
import unittest
from typing import Dict

from dht import DHTNode, NodeID, Peer
from gossip import GossipEngine, NodeResourceMetrics, PeerTelemetry
from router import DEFAULT_STALE_THRESHOLD, NoPeerAvailableError, TaskRouter


def sample_add(x: int, y: int) -> int:
    """Sample task function for testing local fallback."""
    return x + y


def sample_failing_task() -> None:
    """Sample task that raises an exception for testing error fallback."""
    raise ValueError("Intentional task failure")


class TestTaskRouterPeerSelection(unittest.TestCase):
    """Unit tests for TaskRouter peer selection and tie-breaking algorithms."""

    def setUp(self) -> None:
        self.local_node = DHTNode(
            node_id=NodeID.from_string("local_coordinator"),
            host="127.0.0.1",
            port=9000,
        )
        self.gossip_engine = GossipEngine(dht_node=self.local_node, interval=5.0)
        self.router = TaskRouter(
            gossip_engine=self.gossip_engine,
            stale_threshold=30.0,
            fallback_to_local=False,
        )

    def _inject_peer(
        self,
        node_id_hex: str,
        port: int,
        cpu_percent: float,
        ram_free_mb: float,
        last_updated: float | None = None,
    ) -> None:
        """Helper to inject a mock peer telemetry record into GossipEngine."""
        norm_id = node_id_hex.strip().lower()
        peer = Peer(node_id=norm_id, ip="127.0.0.1", port=port)
        metrics = NodeResourceMetrics(
            cpu_percent=cpu_percent,
            ram_total_mb=16384.0,
            ram_used_mb=16384.0 - ram_free_mb,
            ram_free_mb=ram_free_mb,
            ram_percent=round(100.0 * (16384.0 - ram_free_mb) / 16384.0, 2),
            timestamp=last_updated if last_updated is not None else time.time(),
        )
        self.gossip_engine.peer_telemetry[norm_id] = PeerTelemetry(
            peer=peer,
            metrics=metrics,
            last_updated=last_updated if last_updated is not None else time.time(),
        )

    def test_lowest_cpu_peer_selection(self) -> None:
        """Verifies that the peer with the lowest CPU usage is selected."""
        self._inject_peer("1" * 40, port=9001, cpu_percent=45.0, ram_free_mb=8000.0)
        self._inject_peer("2" * 40, port=9002, cpu_percent=12.5, ram_free_mb=4000.0)
        self._inject_peer("3" * 40, port=9003, cpu_percent=78.0, ram_free_mb=12000.0)

        peer, metrics = self.router.select_target_peer()
        self.assertEqual(peer.node_id, "2" * 40)
        self.assertEqual(metrics.cpu_percent, 12.5)

    def test_ram_based_tie_breaking(self) -> None:
        """
        When two peers report identical CPU loads, the peer with the
        highest available free RAM must be selected.
        """
        # Both peers have 20.0% CPU, but peer B has more free RAM
        self._inject_peer("a" * 40, port=9001, cpu_percent=20.0, ram_free_mb=4096.0)
        self._inject_peer("b" * 40, port=9002, cpu_percent=20.0, ram_free_mb=8192.0)

        peer, metrics = self.router.select_target_peer()
        self.assertEqual(peer.node_id, "b" * 40)
        self.assertEqual(metrics.ram_free_mb, 8192.0)

    def test_deterministic_node_id_tie_breaking(self) -> None:
        """
        When both CPU and free RAM are identical, selection falls back
        to deterministic alphabetical NodeID ordering.
        """
        id_high = "f" * 40
        id_low = "1" * 40

        self._inject_peer(id_high, port=9001, cpu_percent=10.0, ram_free_mb=8000.0)
        self._inject_peer(id_low, port=9002, cpu_percent=10.0, ram_free_mb=8000.0)

        peer, metrics = self.router.select_target_peer()
        self.assertEqual(peer.node_id, id_low)

    def test_stale_peer_rejection(self) -> None:
        """Verifies that peers older than stale_threshold are rejected."""
        now = time.time()
        # Fresh peer with higher CPU
        self._inject_peer("fresh" * 8, port=9001, cpu_percent=50.0, ram_free_mb=8000.0, last_updated=now - 5.0)
        # Stale peer with lower CPU (updated 45s ago, threshold is 30s)
        self._inject_peer("stale" * 8, port=9002, cpu_percent=5.0, ram_free_mb=12000.0, last_updated=now - 45.0)

        peer, metrics = self.router.select_target_peer(now=now)
        # Should pick the fresh peer despite its higher CPU
        self.assertEqual(peer.node_id, "fresh" * 8)
        self.assertEqual(metrics.cpu_percent, 50.0)

    def test_all_peers_stale_raises_no_peer_available_error(self) -> None:
        """If all known peers have stale telemetry, NoPeerAvailableError is raised."""
        now = time.time()
        self._inject_peer("stale1" * 6 + "0000", port=9001, cpu_percent=10.0, ram_free_mb=8000.0, last_updated=now - 60.0)
        self._inject_peer("stale2" * 6 + "0000", port=9002, cpu_percent=15.0, ram_free_mb=8000.0, last_updated=now - 40.0)

        with self.assertRaises(NoPeerAvailableError):
            self.router.select_target_peer(now=now)

    def test_excluded_peer_rejection(self) -> None:
        """Verifies that peers in the exclude_nodes set are bypassed."""
        id_best = "best" * 10
        id_second = "secn" * 10

        self._inject_peer(id_best, port=9001, cpu_percent=5.0, ram_free_mb=8000.0)
        self._inject_peer(id_second, port=9002, cpu_percent=25.0, ram_free_mb=8000.0)

        # Exclude the best peer (e.g. following a timeout)
        peer, metrics = self.router.select_target_peer(exclude_nodes={id_best})
        self.assertEqual(peer.node_id, id_second)
        self.assertEqual(metrics.cpu_percent, 25.0)

    def test_excluded_peer_case_insensitivity(self) -> None:
        """Verifies that exclude_nodes handles uppercase hex representations."""
        id_best = "a1b2c3d4" * 5
        self._inject_peer(id_best, port=9001, cpu_percent=5.0, ram_free_mb=8000.0)
        self._inject_peer("other" * 8, port=9002, cpu_percent=30.0, ram_free_mb=8000.0)

        # Pass uppercase variant to exclude_nodes
        peer, _ = self.router.select_target_peer(exclude_nodes={id_best.upper()})
        self.assertEqual(peer.node_id, "other" * 8)

    def test_empty_telemetry_raises_no_peer_available_error(self) -> None:
        """Verifies that an empty telemetry table raises NoPeerAvailableError."""
        self.gossip_engine.peer_telemetry.clear()
        with self.assertRaises(NoPeerAvailableError):
            self.router.select_target_peer()

    def test_local_fallback_disabled_raises_error(self) -> None:
        """When fallback_to_local=False, schedule_task raises NoPeerAvailableError if no peers exist."""
        self.router.fallback_to_local = False
        with self.assertRaises(NoPeerAvailableError):
            self.router.schedule_task(sample_add, args=(10, 20))

    def test_local_fallback_enabled_executes_locally(self) -> None:
        """When fallback_to_local=True, schedule_task executes locally when no peers exist."""
        self.router.fallback_to_local = True
        status, val, executed_by = self.router.schedule_task(sample_add, args=(15, 27))
        self.assertEqual(status, "ok")
        self.assertEqual(val, 42)
        self.assertEqual(executed_by, "local")

    def test_local_fallback_handles_task_exception(self) -> None:
        """When fallback_to_local=True and the task raises an exception, returns status='error'."""
        self.router.fallback_to_local = True
        status, val, executed_by = self.router.schedule_task(sample_failing_task)
        self.assertEqual(status, "error")
        self.assertIn("ValueError", str(val))
        self.assertEqual(executed_by, "local")

    def test_schedule_task_returns_peer_when_available(self) -> None:
        """When an eligible peer exists, schedule_task returns 'peer_selected' and peer contact info."""
        self._inject_peer("candidate" * 4 + "0000", port=9005, cpu_percent=18.0, ram_free_mb=6000.0)
        status, (peer, metrics), executed_by = self.router.schedule_task(sample_add, args=(1, 2))
        self.assertEqual(status, "peer_selected")
        self.assertEqual(peer.node_id, "candidate" * 4 + "0000")
        self.assertEqual(executed_by, "candidate" * 4 + "0000")


class TestTaskRouterAsync(unittest.IsolatedAsyncioTestCase):
    """Asynchronous test cases for TaskRouter."""

    async def test_async_select_target_peer(self) -> None:
        local_node = DHTNode(node_id=NodeID.from_string("async_test_node"))
        gossip = GossipEngine(dht_node=local_node)
        router = TaskRouter(gossip_engine=gossip)

        # Inject peer
        peer = Peer(node_id="f" * 40, ip="127.0.0.1", port=9090)
        metrics = NodeResourceMetrics(
            cpu_percent=11.0,
            ram_total_mb=8192.0,
            ram_used_mb=2048.0,
            ram_free_mb=6144.0,
            ram_percent=25.0,
            timestamp=time.time(),
        )
        gossip.peer_telemetry["f" * 40] = PeerTelemetry(peer=peer, metrics=metrics, last_updated=time.time())

        selected_peer, selected_metrics = await router.async_select_target_peer()
        self.assertEqual(selected_peer.node_id, "f" * 40)
        self.assertEqual(selected_metrics.cpu_percent, 11.0)

    async def test_async_schedule_task_fallback(self) -> None:
        local_node = DHTNode(node_id=NodeID.from_string("async_fallback_node"))
        gossip = GossipEngine(dht_node=local_node)
        router = TaskRouter(gossip_engine=gossip, fallback_to_local=True)

        status, val, executed_by = await router.async_schedule_task(sample_add, args=(100, 250))
        self.assertEqual(status, "ok")
        self.assertEqual(val, 350)
        self.assertEqual(executed_by, "local")


if __name__ == "__main__":
    unittest.main()

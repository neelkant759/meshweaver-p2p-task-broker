"""
MeshWeaver - Gossip Protocol & Resource Monitoring Test Suite
Week 2: Telemetry Dissemination & Decentralized Peer Synchronization

Covers:
1. CPU & RAM resource monitoring across native and fallback paths.
2. Structured Gossip message framing, serialization, and validation.
3. GossipEngine peer telemetry caching and epidemic neighbor ingestion.
4. Least-loaded peer selection based on CPU and RAM telemetry.
5. End-to-end multi-node gossip synchronization over live UDP sockets.
"""

from __future__ import annotations

import asyncio
import socket
import time
import unittest
from typing import List

from dht import DHTNode, NodeID, Peer
from gossip import (
    DEFAULT_GOSSIP_INTERVAL,
    GOSSIP_MAGIC_PREFIX,
    GossipEngine,
    NodeResourceMetrics,
    ResourceMonitor,
    is_gossip_payload,
    pack_gossip_message,
    unpack_gossip_message,
)
from udp_server import run_server


class TestResourceMonitor(unittest.TestCase):
    """Unit tests for CPU and RAM telemetry gathering."""

    def setUp(self) -> None:
        self.monitor = ResourceMonitor()

    def test_cpu_percent_range(self) -> None:
        """Verifies that CPU percent returns a valid float within [0.0, 100.0]."""
        cpu = self.monitor.get_cpu_percent()
        self.assertIsInstance(cpu, float)
        self.assertGreaterEqual(cpu, 0.0)
        self.assertLessEqual(cpu, 100.0)

    def test_ram_info_keys_and_values(self) -> None:
        """Verifies that RAM information contains expected fields with sensible values."""
        ram = self.monitor.get_ram_info()
        self.assertIn("total_mb", ram)
        self.assertIn("used_mb", ram)
        self.assertIn("free_mb", ram)
        self.assertIn("percent", ram)

        self.assertGreater(ram["total_mb"], 0.0)
        self.assertGreaterEqual(ram["used_mb"], 0.0)
        self.assertGreaterEqual(ram["free_mb"], 0.0)
        self.assertGreaterEqual(ram["percent"], 0.0)
        self.assertLessEqual(ram["percent"], 100.0)

    def test_get_metrics_snapshot(self) -> None:
        """Verifies full metrics snapshot capture."""
        metrics = self.monitor.get_metrics()
        self.assertIsInstance(metrics, NodeResourceMetrics)
        self.assertGreater(metrics.ram_total_mb, 0.0)
        self.assertGreater(metrics.timestamp, 0.0)

    def test_metrics_serialization_roundtrip(self) -> None:
        """Verifies serialization and deserialization of NodeResourceMetrics."""
        original = NodeResourceMetrics(
            cpu_percent=24.5,
            ram_total_mb=16384.0,
            ram_used_mb=8192.0,
            ram_free_mb=8192.0,
            ram_percent=50.0,
            timestamp=1731940000.0,
        )
        data = original.to_dict()
        reconstructed = NodeResourceMetrics.from_dict(data)
        self.assertEqual(original, reconstructed)


class TestGossipFraming(unittest.TestCase):
    """Unit tests for Gossip message framing and parsing."""

    def test_is_gossip_payload(self) -> None:
        valid_packet = GOSSIP_MAGIC_PREFIX + b'{"type": "GOSSIP_HEARTBEAT"}'
        self.assertTrue(is_gossip_payload(valid_packet))
        self.assertFalse(is_gossip_payload(b"MWDHT:something"))
        self.assertFalse(is_gossip_payload(b"PING"))
        self.assertFalse(is_gossip_payload(b""))

    def test_pack_and_unpack_gossip(self) -> None:
        metrics = NodeResourceMetrics(
            cpu_percent=15.0,
            ram_total_mb=8192.0,
            ram_used_mb=2048.0,
            ram_free_mb=6144.0,
            ram_percent=25.0,
            timestamp=time.time(),
        )
        sender_id = "a" * 40
        packed = pack_gossip_message(
            sender_id=sender_id,
            listen_ip="127.0.0.1",
            listen_port=9000,
            metrics=metrics,
            neighbors=[{"node_id": "b" * 40, "ip": "127.0.0.1", "port": 9001}],
        )
        self.assertTrue(packed.startswith(GOSSIP_MAGIC_PREFIX))

        unpacked = unpack_gossip_message(packed)
        self.assertEqual(unpacked["type"], "GOSSIP_HEARTBEAT")
        self.assertEqual(unpacked["sender_id"], sender_id)
        self.assertEqual(unpacked["listen_port"], 9000)
        self.assertEqual(len(unpacked["neighbors"]), 1)
        self.assertEqual(unpacked["resources"]["cpu_percent"], 15.0)

    def test_unpack_invalid_payload_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            unpack_gossip_message(b"INVALID_HEADER:{}")


class TestGossipEngineUnit(unittest.TestCase):
    """Unit tests for GossipEngine logic without network sockets."""

    def setUp(self) -> None:
        self.node = DHTNode(node_id=NodeID.from_string("local_node"), host="127.0.0.1", port=8001)
        self.engine = GossipEngine(dht_node=self.node, interval=1.0)

    def test_handle_gossip_updates_peer_and_metrics(self) -> None:
        sender_id = ("b" * 40)
        payload = pack_gossip_message(
            sender_id=sender_id,
            listen_ip="127.0.0.1",
            listen_port=8002,
            metrics=NodeResourceMetrics(
                cpu_percent=12.0,
                ram_total_mb=16384.0,
                ram_used_mb=4096.0,
                ram_free_mb=12288.0,
                ram_percent=25.0,
                timestamp=time.time(),
            ),
        )

        res = self.engine.handle_gossip(payload, ("127.0.0.1", 8002))
        self.assertIsNotNone(res)

        # Verify peer was added to DHT routing table
        self.assertEqual(self.node.routing_table.total_peers(), 1)
        peer = self.node.routing_table.all_peers()[0]
        self.assertEqual(peer.node_id, sender_id)
        self.assertEqual(peer.port, 8002)

        # Verify telemetry was recorded
        metrics = self.engine.get_peer_metrics(sender_id)
        self.assertIsNotNone(metrics)
        self.assertEqual(metrics.cpu_percent, 12.0)

    def test_handle_gossip_self_message_ignored(self) -> None:
        payload = pack_gossip_message(
            sender_id=self.node.node_id.to_hex(),
            listen_ip=self.node.host,
            listen_port=self.node.port,
            metrics=self.engine.monitor.get_metrics(),
        )
        res = self.engine.handle_gossip(payload, ("127.0.0.1", self.node.port))
        self.assertIsNone(res)
        self.assertEqual(self.node.routing_table.total_peers(), 0)

    def test_handle_gossip_epidemic_neighbor_discovery(self) -> None:
        sender_id = "b" * 40
        neighbor_id = "c" * 40
        payload = pack_gossip_message(
            sender_id=sender_id,
            listen_ip="127.0.0.1",
            listen_port=8002,
            metrics=self.engine.monitor.get_metrics(),
            neighbors=[{"node_id": neighbor_id, "ip": "127.0.0.1", "port": 8003}],
        )

        self.engine.handle_gossip(payload, ("127.0.0.1", 8002))

        # Both the sender and the advertised neighbor should now be in the routing table
        self.assertEqual(self.node.routing_table.total_peers(), 2)
        node_ids = {p.node_id for p in self.node.routing_table.all_peers()}
        self.assertIn(sender_id, node_ids)
        self.assertIn(neighbor_id, node_ids)

    def test_find_least_loaded_peer(self) -> None:
        # Add two peers with different CPU utilization
        high_load_id = "1" * 40
        low_load_id = "2" * 40

        self.engine.handle_gossip(
            pack_gossip_message(
                sender_id=high_load_id,
                listen_ip="127.0.0.1",
                listen_port=8002,
                metrics=NodeResourceMetrics(
                    cpu_percent=85.0,
                    ram_total_mb=16384.0,
                    ram_used_mb=12000.0,
                    ram_free_mb=4384.0,
                    ram_percent=73.0,
                    timestamp=time.time(),
                ),
            ),
            ("127.0.0.1", 8002),
        )

        self.engine.handle_gossip(
            pack_gossip_message(
                sender_id=low_load_id,
                listen_ip="127.0.0.1",
                listen_port=8003,
                metrics=NodeResourceMetrics(
                    cpu_percent=15.0,
                    ram_total_mb=16384.0,
                    ram_used_mb=4000.0,
                    ram_free_mb=12384.0,
                    ram_percent=24.0,
                    timestamp=time.time(),
                ),
            ),
            ("127.0.0.1", 8003),
        )

        best = self.engine.find_least_loaded_peer()
        self.assertIsNotNone(best)
        best_id, best_peer, best_metrics = best
        self.assertEqual(best_id, low_load_id)
        self.assertEqual(best_metrics.cpu_percent, 15.0)


class TestGossipIntegrationLive(unittest.IsolatedAsyncioTestCase):
    """Integration tests running live UDP nodes with periodic Gossip dissemination."""

    def _get_free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    async def test_live_two_node_gossip_telemetry_exchange(self) -> None:
        """
        Spins up 2 live UDP server nodes with GossipEngine active (interval = 0.2s).
        Node B pings Node A to register in DHT.
        After a brief interval, both nodes should exchange CPU/RAM telemetry via Gossip.
        """
        port_a = self._get_free_port()
        port_b = self._get_free_port()

        dht_a = DHTNode(node_id=NodeID.from_string("node_a"), host="127.0.0.1", port=port_a)
        dht_b = DHTNode(node_id=NodeID.from_string("node_b"), host="127.0.0.1", port=port_b)

        gossip_a = GossipEngine(dht_node=dht_a, interval=0.2)
        gossip_b = GossipEngine(dht_node=dht_b, interval=0.2)

        server_a_task = asyncio.create_task(
            run_server("127.0.0.1", port_a, dht_node=dht_a, gossip_engine=gossip_a)
        )
        server_b_task = asyncio.create_task(
            run_server("127.0.0.1", port_b, dht_node=dht_b, gossip_engine=gossip_b)
        )

        await asyncio.sleep(0.1)

        try:
            # Node B bootstraps / pings Node A to establish DHT contact
            ping_success = await dht_b.ping_peer("127.0.0.1", port_a, timeout=2.0)
            self.assertTrue(ping_success)

            # Node A now knows Node B, and Node B knows Node A
            # Wait 0.6s to allow periodic gossip ticks to fire across both nodes
            await asyncio.sleep(0.6)

            metrics_of_b_at_a = gossip_a.get_peer_metrics(dht_b.node_id.to_hex())
            metrics_of_a_at_b = gossip_b.get_peer_metrics(dht_a.node_id.to_hex())

            self.assertIsNotNone(metrics_of_b_at_a, "Node A should have received Node B's telemetry")
            self.assertIsNotNone(metrics_of_a_at_b, "Node B should have received Node A's telemetry")

            self.assertGreater(metrics_of_b_at_a.ram_total_mb, 0.0)
            self.assertGreater(metrics_of_a_at_b.ram_total_mb, 0.0)

        finally:
            server_a_task.cancel()
            server_b_task.cancel()
            await asyncio.gather(server_a_task, server_b_task, return_exceptions=True)

    async def test_live_three_node_epidemic_discovery(self) -> None:
        """
        Three nodes: Node A, Node B, Node C.
        Node B knows Node A and Node C. Node A only knows Node B initially.
        Through Gossip neighbor dissemination, Node A discovers Node C without direct bootstrap.
        """
        port_a = self._get_free_port()
        port_b = self._get_free_port()
        port_c = self._get_free_port()

        dht_a = DHTNode(node_id=NodeID.from_string("cluster_a"), host="127.0.0.1", port=port_a)
        dht_b = DHTNode(node_id=NodeID.from_string("cluster_b"), host="127.0.0.1", port=port_b)
        dht_c = DHTNode(node_id=NodeID.from_string("cluster_c"), host="127.0.0.1", port=port_c)

        gossip_a = GossipEngine(dht_node=dht_a, interval=0.2)
        gossip_b = GossipEngine(dht_node=dht_b, interval=0.2)
        gossip_c = GossipEngine(dht_node=dht_c, interval=0.2)

        task_a = asyncio.create_task(run_server("127.0.0.1", port_a, dht_node=dht_a, gossip_engine=gossip_a))
        task_b = asyncio.create_task(run_server("127.0.0.1", port_b, dht_node=dht_b, gossip_engine=gossip_b))
        task_c = asyncio.create_task(run_server("127.0.0.1", port_c, dht_node=dht_c, gossip_engine=gossip_c))

        await asyncio.sleep(0.1)

        try:
            # B connects to A and C
            self.assertTrue(await dht_b.ping_peer("127.0.0.1", port_a))
            self.assertTrue(await dht_b.ping_peer("127.0.0.1", port_c))

            # A currently only knows B
            self.assertEqual(dht_a.routing_table.total_peers(), 1)

            # Allow gossip rounds to propagate B's neighbors to A
            await asyncio.sleep(0.7)

            # Node A should now know Node C through epidemic gossip
            known_ids_at_a = {p.node_id for p in dht_a.routing_table.all_peers()}
            self.assertIn(dht_c.node_id.to_hex(), known_ids_at_a, "Node A should discover Node C via gossip")

        finally:
            task_a.cancel()
            task_b.cancel()
            task_c.cancel()
            await asyncio.gather(task_a, task_b, task_c, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()

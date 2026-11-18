"""
MeshWeaver - Kademlia DHT & Peer Discovery Test Suite
Week 2: Decentralized Networking Tests

Covers:
1. NodeID generation, hex/bytes representation, and XOR distance metric.
2. Peer metadata serialization and deserialization.
3. KBucket capacity and least-recently-seen eviction semantics.
4. RoutingTable distance bucket placement and closest-peers query.
5. DHT message framing and protocol serialization.
6. End-to-end multi-node decentralized peer discovery and dynamic bootstrap
   over live asyncio UDP sockets.
"""

from __future__ import annotations

import asyncio
import socket
import unittest

from dht import (
    DHT_ID_BITS,
    K_BUCKET_SIZE,
    DHTNode,
    KBucket,
    NodeID,
    Peer,
    RoutingTable,
    is_dht_payload,
    pack_dht_message,
    unpack_dht_message,
)
from udp_server import run_server


class TestNodeID(unittest.TestCase):
    """Unit tests for 160-bit Kademlia NodeID and XOR metric."""

    def test_random_generation(self):
        id1 = NodeID()
        id2 = NodeID()
        self.assertNotEqual(id1, id2)
        self.assertEqual(len(id1.to_hex()), 40)
        self.assertEqual(len(id1.to_bytes()), 20)

    def test_deterministic_from_string(self):
        id1 = NodeID.from_string("node_alpha")
        id2 = NodeID.from_string("node_alpha")
        self.assertEqual(id1, id2)
        self.assertEqual(id1.to_hex(), id2.to_hex())

    def test_xor_distance_properties(self):
        id_a = NodeID.from_string("node_a")
        id_b = NodeID.from_string("node_b")

        # Identity: d(a, a) == 0
        self.assertEqual(id_a.distance(id_a), 0)

        # Symmetry: d(a, b) == d(b, a)
        self.assertEqual(id_a.distance(id_b), id_b.distance(id_a))

        # Positivity: d(a, b) > 0 for a != b
        self.assertGreater(id_a.distance(id_b), 0)

    def test_invalid_inputs_and_type_guards(self):
        id_a = NodeID.from_string("node_a")
        # Distance with non-NodeID must raise TypeError
        with self.assertRaises(TypeError):
            id_a.distance("not_a_node_id")
        with self.assertRaises(TypeError):
            id_a.distance(12345)

        # Invalid hex string must raise ValueError
        with self.assertRaises(ValueError):
            NodeID("not_a_valid_hex_string_zzzz")

        # Negative int must raise ValueError
        with self.assertRaises(ValueError):
            NodeID(-100)


class TestPeer(unittest.TestCase):
    """Unit tests for Peer dataclass."""

    def test_peer_serialization_roundtrip(self):
        peer = Peer(node_id="a" * 40, ip="127.0.0.1", port=8000, last_seen=12345.67)
        data = peer.to_dict()
        reconstructed = Peer.from_dict(data)
        self.assertEqual(peer.node_id, reconstructed.node_id)
        self.assertEqual(peer.ip, reconstructed.ip)
        self.assertEqual(peer.port, reconstructed.port)
        self.assertAlmostEqual(peer.last_seen, reconstructed.last_seen)

    def test_invalid_port_raises_value_error(self):
        """Verifies port boundary validation (1 <= port <= 65535)."""
        with self.assertRaises(ValueError):
            Peer(node_id="a" * 40, ip="127.0.0.1", port=0)
        with self.assertRaises(ValueError):
            Peer(node_id="a" * 40, ip="127.0.0.1", port=70000)
        with self.assertRaises(ValueError):
            Peer(node_id="a" * 40, ip="127.0.0.1", port=-80)

    def test_node_id_normalized_to_lowercase(self):
        """Verifies node IDs are normalized to lowercase on creation."""
        upper_id = "ABCD" * 10
        peer = Peer(node_id=upper_id, ip="127.0.0.1", port=8000)
        self.assertEqual(peer.node_id, upper_id.lower())


class TestKBucket(unittest.TestCase):
    """Unit tests for KBucket capacity and least-recently-seen semantics."""

    def test_capacity_limit(self):
        bucket = KBucket(k=3)
        for i in range(3):
            p = Peer(node_id=f"{i:040x}", ip="127.0.0.1", port=8000 + i)
            self.assertTrue(bucket.add_or_update(p))

        self.assertEqual(len(bucket), 3)

        # Adding 4th peer should fail because bucket is full
        p4 = Peer(node_id=f"{4:040x}", ip="127.0.0.1", port=8004)
        self.assertFalse(bucket.add_or_update(p4))
        self.assertEqual(len(bucket), 3)

    def test_update_existing_moves_to_tail(self):
        bucket = KBucket(k=3)
        p0 = Peer(node_id=f"{0:040x}", ip="127.0.0.1", port=8000)
        p1 = Peer(node_id=f"{1:040x}", ip="127.0.0.1", port=8001)
        bucket.add_or_update(p0)
        bucket.add_or_update(p1)

        self.assertEqual(bucket.peers[0].node_id, p0.node_id)

        # Re-adding p0 moves it to the most-recently-seen position (tail)
        bucket.add_or_update(p0)
        self.assertEqual(bucket.peers[-1].node_id, p0.node_id)


class TestRoutingTable(unittest.TestCase):
    """Unit tests for Kademlia RoutingTable."""

    def test_self_excluded(self):
        local_id = NodeID.from_string("local_node")
        table = RoutingTable(local_id, k=5)
        self_peer = Peer(node_id=local_id.to_hex(), ip="127.0.0.1", port=8000)
        # Adding self should return False and not store peer
        self.assertFalse(table.add_peer(self_peer))
        self.assertEqual(table.total_peers(), 0)

    def test_self_exclusion_case_insensitive(self):
        """Verifies that local node is excluded even if peer ID is uppercase."""
        local_id = NodeID.from_string("local_node")
        table = RoutingTable(local_id, k=5)
        upper_self_peer = Peer(node_id=local_id.to_hex().upper(), ip="127.0.0.1", port=8000)
        self.assertFalse(table.add_peer(upper_self_peer))
        self.assertEqual(table.total_peers(), 0)

    def test_duplicate_peer_handling(self):
        """Verifies that re-adding existing peers does not duplicate entries."""
        local_id = NodeID(0)
        table = RoutingTable(local_id, k=5)
        peer = Peer(node_id=f"{1:040x}", ip="127.0.0.1", port=8001)

        # First add
        self.assertTrue(table.add_peer(peer))
        self.assertEqual(table.total_peers(), 1)

        # Re-add same peer (e.g. updated port / refresh)
        peer_updated = Peer(node_id=f"{1:040x}", ip="127.0.0.1", port=9001)
        self.assertTrue(table.add_peer(peer_updated))
        self.assertEqual(table.total_peers(), 1)
        self.assertEqual(table.all_peers()[0].port, 9001)

    def test_get_closest_peers_ordering(self):
        local_id = NodeID(0)
        table = RoutingTable(local_id, k=10)

        # Insert peers with known integer IDs: 10, 50, 100, 200
        for val in [100, 10, 200, 50]:
            p = Peer(node_id=f"{val:040x}", ip="127.0.0.1", port=8000 + val)
            table.add_peer(p)

        # Target = 12. Closest should be 10 (dist 6), then 50, etc.
        target = NodeID(12)
        closest = table.get_closest_peers(target, count=3)
        self.assertEqual(len(closest), 3)
        self.assertEqual(closest[0].node_id, f"{10:040x}")


class TestDHTProtocol(unittest.TestCase):
    """Unit tests for DHT message framing."""

    def test_pack_and_unpack(self):
        raw = pack_dht_message("DHT_PING", "abcd" * 10, {"status": "ok"})
        self.assertTrue(is_dht_payload(raw))
        unpacked = unpack_dht_message(raw)
        self.assertEqual(unpacked["type"], "DHT_PING")
        self.assertEqual(unpacked["sender_id"], "abcd" * 10)
        self.assertEqual(unpacked["payload"]["status"], "ok")


class TestDHTIntegration(unittest.IsolatedAsyncioTestCase):
    """
    Integration tests verifying decentralized peer discovery over live
    asyncio UDP sockets.
    """

    async def asyncSetUp(self):
        self.servers = []
        self.tasks = []

    async def asyncTearDown(self):
        for t in self.tasks:
            t.cancel()
        for t in self.tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass

    def _get_free_port(self) -> int:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        return port

    async def _start_node(self) -> Tuple[DHTNode, int]:
        port = self._get_free_port()
        node = DHTNode(port=port)
        task = asyncio.create_task(run_server("127.0.0.1", port, dht_node=node))
        await asyncio.sleep(0.05)
        self.tasks.append(task)
        return node, port

    async def test_dht_ping_pong_between_nodes(self):
        """Verifies two live DHT nodes exchange DHT_PING and DHT_PONG."""
        node_a, port_a = await self._start_node()
        node_b, port_b = await self._start_node()

        success = await node_b.ping_peer("127.0.0.1", port_a)
        self.assertTrue(success)

        # Node B should have recorded Node A in its routing table
        self.assertEqual(node_b.routing_table.total_peers(), 1)
        self.assertEqual(
            node_b.routing_table.all_peers()[0].node_id,
            node_a.node_id.to_hex(),
        )

    async def test_find_node_rpc(self):
        """Verifies FIND_NODE returns closest known peers."""
        node_a, port_a = await self._start_node()
        node_b, port_b = await self._start_node()
        node_c, port_c = await self._start_node()

        # Let Node A know about Node B
        await node_a.ping_peer("127.0.0.1", port_b)
        self.assertEqual(node_a.routing_table.total_peers(), 1)

        # Node C queries Node A for Node C's closest peers
        discovered = await node_c.find_node_rpc("127.0.0.1", port_a, node_c.node_id)

        # Node C should have discovered Node B through Node A
        self.assertGreaterEqual(len(discovered), 1)
        discovered_ids = [p.node_id for p in discovered]
        self.assertIn(node_b.node_id.to_hex(), discovered_ids)

    async def test_dynamic_multi_node_bootstrap(self):
        """
        Decentralized 3-node bootstrap test:
        1. Node A is active.
        2. Node B joins and bootstraps to Node A.
        3. Node C joins and bootstraps ONLY to Node B.
        4. Node C discovers Node A through Node B without any central server!
        """
        node_a, port_a = await self._start_node()
        node_b, port_b = await self._start_node()
        node_c, port_c = await self._start_node()

        # Step 1: Node B bootstraps to Node A
        b_success = await node_b.bootstrap("127.0.0.1", port_a)
        self.assertTrue(b_success)
        self.assertIn(node_a.node_id.to_hex(), [p.node_id for p in node_b.routing_table.all_peers()])

        # Step 2: Node C bootstraps ONLY to Node B
        c_success = await node_c.bootstrap("127.0.0.1", port_b)
        self.assertTrue(c_success)

        # Node C must have discovered Node A via Node B!
        c_known_ids = [p.node_id for p in node_c.routing_table.all_peers()]
        self.assertIn(node_b.node_id.to_hex(), c_known_ids)
        self.assertIn(node_a.node_id.to_hex(), c_known_ids)

    async def test_bootstrap_failure_on_unavailable_node(self):
        """Verifies bootstrap returns False when the target node is offline."""
        unused_port = self._get_free_port()
        local_node = DHTNode(port=self._get_free_port())
        success = await local_node.bootstrap("127.0.0.1", unused_port, timeout=0.2)
        self.assertFalse(success)
        self.assertEqual(local_node.routing_table.total_peers(), 0)

    async def test_network_communication_error_handling(self):
        """Verifies ping_peer and find_node_rpc handle timeouts and errors cleanly."""
        unused_port = self._get_free_port()
        local_node = DHTNode(port=self._get_free_port())

        # Ping to offline port must return False
        ping_ok = await local_node.ping_peer("127.0.0.1", unused_port, timeout=0.2)
        self.assertFalse(ping_ok)

        # find_node_rpc to offline port must return empty list
        peers = await local_node.find_node_rpc("127.0.0.1", unused_port, local_node.node_id, timeout=0.2)
        self.assertEqual(peers, [])

    async def test_four_node_mesh_topology_discovery(self):
        """
        Decentralized 4-node discovery chain:
        A <- B <- C <- D
        Node D bootstraps ONLY to C, but through iterative discovery
        learns about C, B, and A.
        """
        node_a, port_a = await self._start_node()
        node_b, port_b = await self._start_node()
        node_c, port_c = await self._start_node()
        node_d, port_d = await self._start_node()

        # Chain bootstrapping
        await node_b.bootstrap("127.0.0.1", port_a)
        await node_c.bootstrap("127.0.0.1", port_b)
        await node_d.bootstrap("127.0.0.1", port_c)

        d_peers = [p.node_id for p in node_d.routing_table.all_peers()]
        # Node D should have discovered Node C directly, and Node B + A through C
        self.assertIn(node_c.node_id.to_hex(), d_peers)
        self.assertIn(node_b.node_id.to_hex(), d_peers)
        self.assertIn(node_a.node_id.to_hex(), d_peers)


if __name__ == "__main__":
    unittest.main()

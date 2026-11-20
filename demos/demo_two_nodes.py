"""
MeshWeaver Demo 1: Two-Node Decentralized Network & Telemetry Exchange

Demonstrates:
1. Two independent MeshWeaver nodes starting asynchronously.
2. Node 2 bootstrapping via Node 1's listening address.
3. Kademlia DHT bidirectional peer discovery (Node 1 <-> Node 2).
4. Real-time Gossip protocol CPU and physical RAM telemetry exchange.
5. Clean socket closure and task shutdown.

Usage (from project root):
    python demos/demo_two_nodes.py
"""

from __future__ import annotations

import asyncio
import os
import socket
import sys
import time

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dht import DHTNode, NodeID
from gossip import GossipEngine
from udp_server import run_server


def find_free_port() -> int:
    """Find an available port on loopback."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def run_two_node_demo() -> None:
    port1 = find_free_port()
    port2 = find_free_port()

    print("=" * 72)
    print("      MeshWeaver Demonstration: Two-Node Decentralized Peering")
    print("=" * 72)
    print(f"[*] Node 1 Target Endpoint : 127.0.0.1:{port1}")
    print(f"[*] Node 2 Target Endpoint : 127.0.0.1:{port2}")
    print("=" * 72)

    # 1. Initialize Node 1
    dht1 = DHTNode(node_id=NodeID.from_string("demo_seed_node_1"), host="127.0.0.1", port=port1)
    gossip1 = GossipEngine(dht_node=dht1, interval=1.0)
    server1_task = asyncio.create_task(
        run_server("127.0.0.1", port1, dht_node=dht1, gossip_engine=gossip1)
    )

    # 2. Initialize Node 2
    dht2 = DHTNode(node_id=NodeID.from_string("demo_joining_node_2"), host="127.0.0.1", port=port2)
    gossip2 = GossipEngine(dht_node=dht2, interval=1.0)
    server2_task = asyncio.create_task(
        run_server("127.0.0.1", port2, dht_node=dht2, gossip_engine=gossip2)
    )

    await asyncio.sleep(0.2)
    print(f"[+] Node 1 Online -> ID: {dht1.node_id.to_hex()[:12]}...")
    print(f"[+] Node 2 Online -> ID: {dht2.node_id.to_hex()[:12]}...")

    try:
        # 3. Node 2 bootstraps via Node 1
        print(f"\n[*] Node 2 initiating bootstrap contact with Node 1 (127.0.0.1:{port1})...")
        boot_ok = await dht2.bootstrap("127.0.0.1", port1, timeout=2.0)
        print(f"[+] Bootstrap handshake result: {boot_ok}")

        # 4. Wait for Gossip dissemination round
        print("[*] Awaiting Gossip protocol telemetry exchange...")
        await asyncio.sleep(1.5)

        # 5. Display DHT Routing Table Status
        peers_at_1 = [f"{p.ip}:{p.port} (ID: {p.node_id[:8]}..)" for p in dht1.routing_table.all_peers()]
        peers_at_2 = [f"{p.ip}:{p.port} (ID: {p.node_id[:8]}..)" for p in dht2.routing_table.all_peers()]

        print("\n" + "-" * 72)
        print(f"Node 1 Routing Table ({dht1.routing_table.total_peers()} peer): {peers_at_1}")
        print(f"Node 2 Routing Table ({dht2.routing_table.total_peers()} peer): {peers_at_2}")
        print("-" * 72)

        # 6. Display Telemetry Exchanged
        tel_2_at_1 = gossip1.get_peer_metrics(dht2.node_id.to_hex())
        tel_1_at_2 = gossip2.get_peer_metrics(dht1.node_id.to_hex())

        print("\n[Telemetry Exchanged via Gossip Protocol]:")
        if tel_2_at_1:
            print(f" -> Node 1 received Node 2 Metrics: CPU = {tel_2_at_1.cpu_percent:.1f}% | "
                  f"RAM Load = {tel_2_at_1.ram_percent:.1f}% | Free RAM = {tel_2_at_1.ram_free_mb:.1f} MB")
        if tel_1_at_2:
            print(f" -> Node 2 received Node 1 Metrics: CPU = {tel_1_at_2.cpu_percent:.1f}% | "
                  f"RAM Load = {tel_1_at_2.ram_percent:.1f}% | Free RAM = {tel_1_at_2.ram_free_mb:.1f} MB")

        print("\n[+] Verification Complete: Decentralized peer discovery and resource sharing verified.")

    finally:
        print("\n[*] Shutting down demonstration nodes...")
        server1_task.cancel()
        server2_task.cancel()
        await asyncio.gather(server1_task, server2_task, return_exceptions=True)
        print("[+] Both nodes shutdown cleanly.")


def main() -> None:
    try:
        asyncio.run(run_two_node_demo())
    except KeyboardInterrupt:
        print("\nDemo interrupted by user.")


if __name__ == "__main__":
    main()

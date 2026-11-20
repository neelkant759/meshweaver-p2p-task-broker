"""
MeshWeaver Demo 2: 10-Node Dynamic Mesh Discovery Demonstration

Demonstrates:
1. Spawning 10 independent MeshWeaver nodes on unique UDP ports.
2. Single-entry bootstrapping: Node 0 acts as the seed node; Nodes 1..9 join sequentially.
3. Recursive Kademlia DHT peer discovery without any hardcoded topology.
4. Autonomous convergence: All nodes discover their neighborhood peers.
5. Clean coordinated shutdown.

Usage (from project root):
    python demos/demo_10_nodes.py
"""

from __future__ import annotations

import asyncio
import os
import socket
import sys
import time
from typing import List

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dht import DHTNode, NodeID
from udp_server import run_server


def find_free_ports(count: int) -> List[int]:
    """Allocate distinct available ports on localhost."""
    ports: List[int] = []
    sockets: List[socket.socket] = []
    for _ in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(("127.0.0.1", 0))
        ports.append(s.getsockname()[1])
        sockets.append(s)
    for s in sockets:
        s.close()
    return ports


async def run_10_nodes_demo() -> None:
    node_count = 10
    ports = find_free_ports(node_count)

    print("=" * 75)
    print(f"      MeshWeaver Demonstration: {node_count}-Node Dynamic Mesh Topology")
    print("=" * 75)

    nodes: List[DHTNode] = []
    servers: List[asyncio.Task] = []

    # 1. Initialize and start 10 nodes
    for i in range(node_count):
        dht = DHTNode(
            node_id=NodeID.from_string(f"mesh_node_cluster_{i}"),
            host="127.0.0.1",
            port=ports[i],
        )
        nodes.append(dht)
        task = asyncio.create_task(run_server("127.0.0.1", ports[i], dht_node=dht))
        servers.append(task)

    await asyncio.sleep(0.2)
    print(f"[+] All {node_count} nodes initialized on ports: {ports[0]} to {ports[-1]}")
    print(f"[*] Node 0 ({ports[0]}) configured as the decentralized bootstrap seed.")

    try:
        # 2. Sequential dynamic joining via Node 0
        print("\n[*] Nodes 1 through 9 joining dynamic mesh via Node 0...")
        for i in range(1, node_count):
            boot_ok = await nodes[i].bootstrap("127.0.0.1", ports[0], timeout=2.0)
            status_symbol = "[+]" if boot_ok else "[-]"
            print(f"    {status_symbol} Node {i} ({ports[i]}) joined -> Discovered {nodes[i].routing_table.total_peers()} peer(s)")

        # Allow iterative lookups to propagate
        await asyncio.sleep(0.5)

        # 3. Display Convergence & Discovery Statistics
        print("\n" + "=" * 75)
        print("                  Mesh Discovery Topology Summary")
        print("=" * 75)
        peer_counts = [n.routing_table.total_peers() for n in nodes]

        for i in range(node_count):
            peer_endpoints = [f":{p.port}" for p in nodes[i].routing_table.all_peers()]
            peers_sample = ", ".join(peer_endpoints[:6])
            if len(peer_endpoints) > 6:
                peers_sample += f", ... (+{len(peer_endpoints) - 6} more)"
            print(f" Node {i} (Port {ports[i]} | ID {nodes[i].node_id.to_hex()[:8]}..): "
                  f"Known Peers = {peer_counts[i]}/{node_count - 1} [{peers_sample}]")

        print("-" * 75)
        min_peers, max_peers = min(peer_counts), max(peer_counts)
        print(f"[+] Network Convergence Stats: Min Peers = {min_peers}, Max Peers = {max_peers}")
        print(f"[+] Every node discovered {min_peers} to {max_peers} peers without any central server or hardcoded list!")

    finally:
        print("\n[*] Terminating all 10 demonstration nodes...")
        for s in servers:
            s.cancel()
        await asyncio.gather(*servers, return_exceptions=True)
        print("[+] All 10 node sockets and background tasks closed cleanly.")


def main() -> None:
    try:
        asyncio.run(run_10_nodes_demo())
    except KeyboardInterrupt:
        print("\nDemo interrupted by user.")


if __name__ == "__main__":
    main()

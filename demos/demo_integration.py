"""
MeshWeaver Demo 4: Full Multi-Module Integration (UDP, DHT, Gossip, TaskRouter)

Demonstrates:
1. Multi-node cluster startup (Coordinator Node + 2 Worker Nodes).
2. Dynamic Kademlia DHT peer discovery.
3. Periodic Gossip Protocol CPU and RAM telemetry dissemination.
4. Intelligent TaskRouter peer selection:
   - Evaluates real-time Gossip telemetry.
   - Selects the optimal least-loaded worker (lowest CPU load).
   - Breaks ties using available free RAM.
5. Task execution boundary:
   - Demonstrates TaskRouter selecting the target peer.
   - Dispatches a computation to the selected peer via AsyncUDPClient.
   - Receives and validates the execution result.

Usage (from project root):
    python demos/demo_integration.py
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
from router import TaskRouter
from udp_client import AsyncUDPClient
from udp_server import run_server


def find_free_ports(count: int):
    ports = []
    socks = []
    for _ in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(("127.0.0.1", 0))
        ports.append(s.getsockname()[1])
        socks.append(s)
    for s in socks:
        s.close()
    return ports


def sample_compute(x: int, y: int) -> int:
    """Sample task to execute on the selected worker."""
    return (x * y) ** 2 + 100


async def run_integration_demo() -> None:
    ports = find_free_ports(3)
    p_coord, p_worker_b, p_worker_c = ports[0], ports[1], ports[2]

    print("=" * 75)
    print("      MeshWeaver Demonstration: Full Architecture Integration")
    print("=" * 75)
    print(f"[*] Coordinator Node (A) : 127.0.0.1:{p_coord}")
    print(f"[*] Worker Node (B)      : 127.0.0.1:{p_worker_b} (Simulated Load: High)")
    print(f"[*] Worker Node (C)      : 127.0.0.1:{p_worker_c} (Simulated Load: Low)")
    print("=" * 75)

    # 1. Initialize DHT Nodes
    dht_a = DHTNode(node_id=NodeID.from_string("node_coordinator_a"), host="127.0.0.1", port=p_coord)
    dht_b = DHTNode(node_id=NodeID.from_string("node_worker_b"), host="127.0.0.1", port=p_worker_b)
    dht_c = DHTNode(node_id=NodeID.from_string("node_worker_c"), host="127.0.0.1", port=p_worker_c)

    # 2. Initialize Gossip Engines
    gossip_a = GossipEngine(dht_node=dht_a, interval=0.5)
    gossip_b = GossipEngine(dht_node=dht_b, interval=0.5)
    gossip_c = GossipEngine(dht_node=dht_c, interval=0.5)

    # 3. Initialize TaskRouter on Coordinator Node A
    router_a = TaskRouter(gossip_engine=gossip_a, stale_threshold=30.0, fallback_to_local=True)

    # 4. Start all 3 servers
    tasks = [
        asyncio.create_task(run_server("127.0.0.1", p_coord, dht_node=dht_a, gossip_engine=gossip_a)),
        asyncio.create_task(run_server("127.0.0.1", p_worker_b, dht_node=dht_b, gossip_engine=gossip_b)),
        asyncio.create_task(run_server("127.0.0.1", p_worker_c, dht_node=dht_c, gossip_engine=gossip_c)),
    ]

    await asyncio.sleep(0.2)
    print("[+] All 3 nodes online on non-blocking UDP sockets.")

    try:
        # 5. Dynamic Peer Discovery: Workers bootstrap with Coordinator
        print("\n[*] Step 1: Kademlia DHT Discovery...")
        await dht_b.bootstrap("127.0.0.1", p_coord, timeout=2.0)
        await dht_c.bootstrap("127.0.0.1", p_coord, timeout=2.0)
        print(f"[+] Coordinator A discovered {dht_a.routing_table.total_peers()} peers via DHT handshake.")

        # 6. Gossip Telemetry Exchange
        print("\n[*] Step 2: Gossip Telemetry Exchange...")
        await asyncio.sleep(1.0)
        print(f"[+] Coordinator A tracking {len(gossip_a.peer_telemetry)} peer telemetry records.")

        # Simulate disparate load condition to demonstrate intelligent selection
        # Worker B: 72.0% CPU load
        # Worker C: 14.5% CPU load
        gossip_a.peer_telemetry[dht_b.node_id.to_hex()].metrics.cpu_percent = 72.0
        gossip_a.peer_telemetry[dht_c.node_id.to_hex()].metrics.cpu_percent = 14.5

        print("\n[Current Telemetry State at Coordinator A]:")
        for nid, rec in gossip_a.peer_telemetry.items():
            name = "Worker B" if rec.peer.port == p_worker_b else "Worker C"
            print(f" -> {name} ({rec.peer.ip}:{rec.peer.port}): CPU = {rec.metrics.cpu_percent:.1f}% | Free RAM = {rec.metrics.ram_free_mb:.1f} MB")

        # 7. Intelligent Peer Selection via TaskRouter
        print("\n[*] Step 3: Intelligent Peer Selection via TaskRouter...")
        selected_peer, selected_metrics = router_a.select_target_peer()
        winner_name = "Worker C" if selected_peer.port == p_worker_c else "Worker B"

        print(f"[+] TaskRouter selected: {winner_name} ({selected_peer.ip}:{selected_peer.port})")
        print(f"    Reason: Lowest CPU utilization ({selected_metrics.cpu_percent:.1f}%)")
        assert selected_peer.port == p_worker_c, "Router failed to pick least-loaded Worker C!"

        # 8. Remote Task Dispatch to Selected Peer
        print(f"\n[*] Step 4: Dispatching Task to Selected Peer ({selected_peer.ip}:{selected_peer.port})...")
        client = AsyncUDPClient(selected_peer.ip, selected_peer.port, timeout=3.0)
        client.start()
        try:
            arg1, arg2 = 5, 8
            print(f"    - Submitting task: sample_compute({arg1}, {arg2}) -> ({arg1}*{arg2})^2 + 100")
            await client.send_task(sample_compute, args=(arg1, arg2))
            status, result = await client.receive_task_result()
            expected = sample_compute(arg1, arg2)
            print(f"[+] Task Result Received: status='{status}', result={result}")
            assert status == "ok" and result == expected, "Computation result verification failed!"
            print(f"[+] Verification SUCCESS: Result {result} matched expected ground truth!")
        finally:
            client.close()

        print("\n" + "=" * 75)
        print(" [SUMMARY] Architectural Boundaries Demonstrated:")
        print(" 1. Networking Layer   : Non-blocking UDP server and client communication.")
        print(" 2. Discovery Layer    : Kademlia DHT XOR metric dynamic topology convergence.")
        print(" 3. Telemetry Layer    : Gossip protocol periodic background resource exchange.")
        print(" 4. Scheduling Layer   : TaskRouter intelligent peer selection (lowest CPU).")
        print(" 5. Execution Layer    : Remote Cloudpickle serialization and execution.")
        print("=" * 75)

    finally:
        print("\n[*] Terminating all demonstration nodes...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        print("[+] All demonstration sockets and tasks cleanly shutdown.")


def main() -> None:
    try:
        asyncio.run(run_integration_demo())
    except KeyboardInterrupt:
        print("\nDemo interrupted by user.")


if __name__ == "__main__":
    main()

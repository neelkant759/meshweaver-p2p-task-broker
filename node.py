"""
MeshWeaver - Decentralized P2P Node Launcher
CLI Entrypoint for running a MeshWeaver node with integrated UDP Networking,
Kademlia DHT Peer Discovery, and Gossip Protocol Resource Telemetry.

Usage:
  # First (seed / bootstrap) node:
  python node.py --host 127.0.0.1 --port 8000

  # Joining node:
  python node.py --host 127.0.0.1 --port 8001 --bootstrap 127.0.0.1:8000
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Optional

from dht import DHTNode
from gossip import GossipEngine
from task_registry import TaskExecutionRegistry
from udp_server import run_server


async def run_node(
    host: str,
    port: int,
    bootstrap: Optional[str] = None,
    secret_key: Optional[str] = None,
    enforce_signatures: bool = False,
) -> None:
    """
    Initializes and orchestrates the MeshWeaver P2P Node runtime:
    1. Instantiates the Kademlia DHTNode.
    2. Instantiates the GossipEngine for 5-second periodic resource dissemination.
    3. Spawns the UDP Server background task on (host, port).
    4. If bootstrap target is supplied, contacts it and discovers mesh peers.
    5. Displays live peer updates and shuts down cleanly on cancellation.
    """
    # 1. Initialize Kademlia DHT controller
    dht_node = DHTNode(host=host, port=port)

    # 2. Initialize Gossip Engine with default 5.0s interval
    gossip_engine = GossipEngine(dht_node=dht_node, interval=5.0)

    # 3. Initialize Task Execution Registry for duplicate protection & caching
    task_registry = TaskExecutionRegistry()

    # 4. Start asynchronous UDP Server loop
    server_task = asyncio.create_task(
        run_server(
            host=host,
            port=port,
            dht_node=dht_node,
            gossip_engine=gossip_engine,
            task_registry=task_registry,
            secret_key=secret_key,
            enforce_signatures=enforce_signatures,
        )
    )

    # Allow non-blocking UDP socket to bind cleanly
    await asyncio.sleep(0.15)

    print("=" * 68, flush=True)
    print("                 MeshWeaver P2P Node Online", flush=True)
    print(f" Node ID   : {dht_node.node_id.to_hex()}", flush=True)
    print(f" Endpoint  : {host}:{port}", flush=True)
    print("=" * 68, flush=True)

    # 4. Bootstrap into existing network if requested
    if bootstrap:
        try:
            boot_ip, boot_port_str = bootstrap.strip().split(":")
            boot_port = int(boot_port_str)
            print(f"[*] Bootstrapping into mesh via {boot_ip}:{boot_port}...", flush=True)
            success = await dht_node.bootstrap(boot_ip, boot_port, timeout=3.0)
            if success:
                print(f"[+] Bootstrap successful! Discovered {dht_node.routing_table.total_peers()} initial peer(s).", flush=True)
            else:
                print(f"[-] Bootstrap node at {boot_ip}:{boot_port} did not respond. Running standalone.", flush=True)
        except ValueError:
            print(f"[!] Invalid bootstrap format '{bootstrap}'. Expected IP:PORT (e.g. 127.0.0.1:8000).", flush=True)
        except Exception as exc:
            print(f"[!] Bootstrap connection error: {exc}", flush=True)

    # 5. Live status display loop
    try:
        last_peer_count = -1
        while True:
            await asyncio.sleep(5.0)
            current_peer_count = dht_node.routing_table.total_peers()
            telemetry_count = len(gossip_engine.peer_telemetry)

            # Prune expired task execution records
            pruned_tasks = task_registry.cleanup_expired()
            if pruned_tasks > 0:
                print(f"[TaskRegistry] Pruned {pruned_tasks} expired task records.", flush=True)

            # Print status log
            peer_list = [f"{p.ip}:{p.port}" for p in dht_node.routing_table.all_peers()]
            peers_display = ", ".join(peer_list) if peer_list else "None (Standalone)"
            print(f"[Status] DHT Peers ({current_peer_count}): [{peers_display}] | Telemetry Cached: {telemetry_count} | Tasks Tracked: {task_registry.total_records()}", flush=True)

            if telemetry_count > 0:
                for nid, rec in gossip_engine.peer_telemetry.items():
                    print(
                        f"         -> Peer {nid[:8]}.. ({rec.peer.ip}:{rec.peer.port}) "
                        f"| CPU: {rec.metrics.cpu_percent:.1f}% | Free RAM: {rec.metrics.ram_free_mb:.1f} MB",
                        flush=True
                    )
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        print("\n[*] Stopping MeshWeaver node...", flush=True)
        server_task.cancel()
        await asyncio.gather(server_task, return_exceptions=True)
        print("[+] Sockets closed and background tasks terminated cleanly.", flush=True)


def main() -> None:
    """CLI Argument Parser and Entry Point."""
    # Ensure Windows and POSIX signals cleanly raise KeyboardInterrupt
    def _handle_exit(sig, frame):
        raise KeyboardInterrupt

    try:
        import signal
        signal.signal(signal.SIGINT, _handle_exit)
        if hasattr(signal, "SIGBREAK"):
            signal.signal(signal.SIGBREAK, _handle_exit)
    except Exception:
        pass

    parser = argparse.ArgumentParser(
        description="MeshWeaver: Zero-Dependency P2P Async Task Broker Node",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default="127.0.0.1", help="Listening IP address")
    parser.add_argument("--port", type=int, default=8000, help="Listening UDP port number")
    parser.add_argument("--bootstrap", help="Bootstrap node address in IP:PORT format")
    parser.add_argument("--secret-key", help="Shared secret key for HMAC-SHA256 task signatures")
    parser.add_argument(
        "--enforce-signatures",
        action="store_true",
        help="Enforce cryptographic signatures, rejecting unsigned task requests",
    )
    args = parser.parse_args()

    try:
        asyncio.run(
            run_node(
                host=args.host,
                port=args.port,
                bootstrap=args.bootstrap,
                secret_key=args.secret_key,
                enforce_signatures=args.enforce_signatures,
            )
        )
    except KeyboardInterrupt:
        print("\n[+] MeshWeaver Node stopped cleanly by user.")
        sys.exit(0)


if __name__ == "__main__":
    main()

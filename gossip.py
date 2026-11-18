"""
MeshWeaver - Gossip Protocol & Resource Monitoring
Week 2: Decentralized Telemetry & Periodic State Synchronization

This module implements:
1. Native zero-dependency CPU and RAM monitoring with standard library fallbacks
   (Windows ctypes GlobalMemoryStatusEx & GetSystemTimes, Linux /proc, and psutil if present).
2. Structured Gossip Protocol message framing (MWGOSSIP: prefix).
3. Asynchronous periodic background dissemination (default every 5.0s) sharing
   telemetry and peer states across neighboring nodes.
4. Seamless integration with Kademlia DHT routing table for peer targeting and
   epidemic peer discovery.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import socket
import sys
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

from dht import DHTNode, NodeID, Peer

# Protocol Constants
GOSSIP_MAGIC_PREFIX: bytes = b"MWGOSSIP:"
DEFAULT_GOSSIP_INTERVAL: float = 5.0
DEFAULT_GOSSIP_FANOUT: int = 3


# ============================================================================
# 1. Telemetry Data Structure
# ============================================================================

@dataclass
class NodeResourceMetrics:
    """Telemetry snapshot of a node's CPU and RAM utilization."""
    cpu_percent: float
    ram_total_mb: float
    ram_used_mb: float
    ram_free_mb: float
    ram_percent: float
    timestamp: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> NodeResourceMetrics:
        return cls(
            cpu_percent=float(data.get("cpu_percent", 0.0)),
            ram_total_mb=float(data.get("ram_total_mb", 0.0)),
            ram_used_mb=float(data.get("ram_used_mb", 0.0)),
            ram_free_mb=float(data.get("ram_free_mb", 0.0)),
            ram_percent=float(data.get("ram_percent", 0.0)),
            timestamp=float(data.get("timestamp", time.time())),
        )


# ============================================================================
# 2. Resource Monitor (CPU & RAM)
# ============================================================================

class ResourceMonitor:
    """
    Monitors CPU and RAM utilization.
    Adheres to the MeshWeaver zero-external-dependency rule:
    - Uses psutil if installed in the environment.
    - Otherwise, falls back to native OS APIs (Windows ctypes kernel32, Linux /proc, etc.).
    """

    def __init__(self) -> None:
        self._psutil_available: bool = False
        try:
            import psutil  # type: ignore
            self._psutil = psutil
            self._psutil_available = True
            # Initial prime call for psutil cpu_percent
            self._psutil.cpu_percent(interval=None)
        except ImportError:
            self._psutil = None

        # State tracking for Windows GetSystemTimes
        self._prev_idle: int = 0
        self._prev_kernel: int = 0
        self._prev_user: int = 0
        self._prev_cpu_time: float = time.time()

        if not self._psutil_available and sys.platform == "win32":
            self._init_windows_cpu()

    def _init_windows_cpu(self) -> None:
        """Prime Windows system times for delta calculations."""
        try:
            import ctypes
            class _FILETIME(ctypes.Structure):
                _fields_ = [("dwLowDateTime", ctypes.c_ulong), ("dwHighDateTime", ctypes.c_ulong)]

            idle, kernel, user = _FILETIME(), _FILETIME(), _FILETIME()
            if ctypes.windll.kernel32.GetSystemTimes(ctypes.byref(idle), ctypes.byref(kernel), ctypes.byref(user)):
                self._prev_idle = (idle.dwHighDateTime << 32) + idle.dwLowDateTime
                self._prev_kernel = (kernel.dwHighDateTime << 32) + kernel.dwLowDateTime
                self._prev_user = (user.dwHighDateTime << 32) + user.dwLowDateTime
        except Exception:
            pass

    def get_cpu_percent(self) -> float:
        """Return overall CPU usage percentage (0.0 to 100.0)."""
        if self._psutil_available and self._psutil:
            try:
                return float(self._psutil.cpu_percent(interval=None))
            except Exception:
                pass

        if sys.platform == "win32":
            try:
                import ctypes
                class _FILETIME(ctypes.Structure):
                    _fields_ = [("dwLowDateTime", ctypes.c_ulong), ("dwHighDateTime", ctypes.c_ulong)]

                idle, kernel, user = _FILETIME(), _FILETIME(), _FILETIME()
                if ctypes.windll.kernel32.GetSystemTimes(ctypes.byref(idle), ctypes.byref(kernel), ctypes.byref(user)):
                    curr_idle = (idle.dwHighDateTime << 32) + idle.dwLowDateTime
                    curr_kernel = (kernel.dwHighDateTime << 32) + kernel.dwLowDateTime
                    curr_user = (user.dwHighDateTime << 32) + user.dwLowDateTime

                    delta_idle = curr_idle - self._prev_idle
                    delta_kernel = curr_kernel - self._prev_kernel
                    delta_user = curr_user - self._prev_user
                    delta_total = delta_kernel + delta_user

                    self._prev_idle, self._prev_kernel, self._prev_user = curr_idle, curr_kernel, curr_user

                    if delta_total > 0:
                        pct = 100.0 * (1.0 - (delta_idle / delta_total))
                        return max(0.0, min(100.0, round(pct, 2)))
            except Exception:
                pass

        # Linux /proc/stat fallback
        if sys.platform.startswith("linux"):
            try:
                with open("/proc/stat", "r", encoding="utf-8") as f:
                    fields = [float(x) for x in f.readline().strip().split()[1:5]]
                    idle_time = fields[3]
                    total_time = sum(fields)
                    pct = 100.0 * (1.0 - (idle_time / max(1.0, total_time)))
                    return max(0.0, min(100.0, round(pct, 2)))
            except Exception:
                pass

        # Fallback approximation using process time
        now = time.time()
        elapsed = max(0.001, now - self._prev_cpu_time)
        self._prev_cpu_time = now
        proc_delta = time.process_time()
        pct = (proc_delta / elapsed) * 10.0
        return max(0.0, min(100.0, round(pct, 2)))

    def get_ram_info(self) -> Dict[str, float]:
        """
        Return memory metrics:
        total_mb, used_mb, free_mb, and percent.
        """
        if self._psutil_available and self._psutil:
            try:
                v = self._psutil.virtual_memory()
                return {
                    "total_mb": round(v.total / (1024 * 1024), 2),
                    "used_mb": round(v.used / (1024 * 1024), 2),
                    "free_mb": round(v.available / (1024 * 1024), 2),
                    "percent": round(v.percent, 2),
                }
            except Exception:
                pass

        # Windows ctypes native GlobalMemoryStatusEx
        if sys.platform == "win32":
            try:
                import ctypes
                class _MEMORYSTATUSEX(ctypes.Structure):
                    _fields_ = [
                        ("dwLength", ctypes.c_ulong),
                        ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", ctypes.c_ulonglong),
                        ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong),
                        ("ullAvailPageFile", ctypes.c_ulonglong),
                        ("ullTotalVirtual", ctypes.c_ulonglong),
                        ("ullAvailVirtual", ctypes.c_ulonglong),
                        ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                    ]

                stat = _MEMORYSTATUSEX()
                stat.dwLength = ctypes.sizeof(_MEMORYSTATUSEX)
                if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
                    total_mb = stat.ullTotalPhys / (1024 * 1024)
                    free_mb = stat.ullAvailPhys / (1024 * 1024)
                    used_mb = total_mb - free_mb
                    pct = float(stat.dwMemoryLoad)
                    return {
                        "total_mb": round(total_mb, 2),
                        "used_mb": round(used_mb, 2),
                        "free_mb": round(free_mb, 2),
                        "percent": round(pct, 2),
                    }
            except Exception:
                pass

        # Linux /proc/meminfo fallback
        if sys.platform.startswith("linux"):
            try:
                meminfo: Dict[str, float] = {}
                with open("/proc/meminfo", "r", encoding="utf-8") as f:
                    for line in f:
                        parts = line.split(":")
                        if len(parts) == 2:
                            key = parts[0].strip()
                            val = float(parts[1].strip().split()[0])  # in kB
                            meminfo[key] = val
                total_kb = meminfo.get("MemTotal", 1.0)
                free_kb = meminfo.get("MemAvailable", meminfo.get("MemFree", 0.0))
                used_kb = total_kb - free_kb
                return {
                    "total_mb": round(total_kb / 1024.0, 2),
                    "used_mb": round(used_kb / 1024.0, 2),
                    "free_mb": round(free_kb / 1024.0, 2),
                    "percent": round(100.0 * (used_kb / total_kb), 2),
                }
            except Exception:
                pass

        # Safe default stub if OS probing is completely unavailable
        return {
            "total_mb": 16384.0,
            "used_mb": 4096.0,
            "free_mb": 12288.0,
            "percent": 25.0,
        }

    def get_metrics(self) -> NodeResourceMetrics:
        """Capture a consolidated snapshot of CPU and RAM usage."""
        cpu = self.get_cpu_percent()
        ram = self.get_ram_info()
        return NodeResourceMetrics(
            cpu_percent=cpu,
            ram_total_mb=ram["total_mb"],
            ram_used_mb=ram["used_mb"],
            ram_free_mb=ram["free_mb"],
            ram_percent=ram["percent"],
            timestamp=time.time(),
        )


# ============================================================================
# 3. Gossip Protocol Framing
# ============================================================================

def is_gossip_payload(data: bytes) -> bool:
    """Check if the datagram begins with the MeshWeaver Gossip prefix."""
    return isinstance(data, (bytes, bytearray)) and data.startswith(GOSSIP_MAGIC_PREFIX)


def pack_gossip_message(
    sender_id: str,
    listen_ip: str,
    listen_port: int,
    metrics: NodeResourceMetrics | Dict[str, Any],
    neighbors: List[Dict[str, Any]] | None = None,
) -> bytes:
    """Serialize a gossip heartbeat packet into magic-prefixed JSON bytes."""
    metrics_dict = metrics.to_dict() if isinstance(metrics, NodeResourceMetrics) else metrics
    envelope = {
        "type": "GOSSIP_HEARTBEAT",
        "sender_id": sender_id,
        "listen_ip": listen_ip,
        "listen_port": listen_port,
        "timestamp": time.time(),
        "resources": metrics_dict,
        "neighbors": neighbors or [],
    }
    return GOSSIP_MAGIC_PREFIX + json.dumps(envelope).encode("utf-8")


def unpack_gossip_message(raw: bytes) -> Dict[str, Any]:
    """Deserialize magic-prefixed gossip JSON bytes into a dictionary."""
    if not is_gossip_payload(raw):
        raise ValueError("Payload missing GOSSIP_MAGIC_PREFIX")
    body = raw[len(GOSSIP_MAGIC_PREFIX):].decode("utf-8")
    return json.loads(body)


# ============================================================================
# 4. Peer Telemetry Record
# ============================================================================

@dataclass
class PeerTelemetry:
    """Tracks a peer's contact details and latest telemetry state."""
    peer: Peer
    metrics: NodeResourceMetrics
    last_updated: float


# ============================================================================
# 5. Gossip Engine
# ============================================================================

class GossipEngine:
    """
    Decentralized Gossip Engine.
    Periodically synchronizes CPU and RAM telemetry and peer topology:
    1. Collects local hardware resource metrics.
    2. Transmits structured gossip heartbeats every `interval` seconds to neighboring peers.
    3. Ingests received heartbeats, updating local peer state and routing table.
    4. Enables epidemic peer discovery by learning neighbor contacts reported by peers.
    """

    def __init__(
        self,
        dht_node: DHTNode,
        interval: float = DEFAULT_GOSSIP_INTERVAL,
        fanout: int = DEFAULT_GOSSIP_FANOUT,
        monitor: ResourceMonitor | None = None,
    ) -> None:
        self.dht_node: DHTNode = dht_node
        self.interval: float = interval
        self.fanout: int = fanout
        self.monitor: ResourceMonitor = monitor or ResourceMonitor()
        self.peer_telemetry: Dict[str, PeerTelemetry] = {}
        self._task: Optional[asyncio.Task] = None
        self._is_running: bool = False
        self._sock: Optional[socket.socket] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def start(
        self,
        loop: asyncio.AbstractEventLoop | None = None,
        sock: socket.socket | None = None,
    ) -> asyncio.Task:
        """Start the periodic background dissemination loop."""
        if self._is_running and self._task and not self._task.done():
            return self._task

        self._loop = loop or asyncio.get_running_loop()
        self._sock = sock
        self._is_running = True
        self._task = self._loop.create_task(self._run_gossip_loop())
        return self._task

    def stop(self) -> None:
        """Stop the periodic background dissemination loop."""
        self._is_running = False
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = None

    async def _run_gossip_loop(self) -> None:
        """Periodic background task that disseminates gossip heartbeats."""
        while self._is_running:
            try:
                await self.send_gossip_tick()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                print(f"[Gossip Warning] Error during gossip tick: {exc}")

            try:
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                break

    async def send_gossip_tick(self) -> int:
        """
        Execute a single gossip dissemination round:
        1. Capture local CPU/RAM metrics.
        2. Pick up to `fanout` peers from DHT routing table.
        3. Send gossip datagram to each chosen peer.
        Returns the number of peers contacted.
        """
        all_peers = self.dht_node.routing_table.all_peers()
        if not all_peers:
            return 0

        # Select up to fanout peers (random selection or full set if <= fanout)
        target_peers = random.sample(all_peers, min(self.fanout, len(all_peers)))
        metrics = self.monitor.get_metrics()

        # Build list of known neighbors to share (up to 5 peers)
        neighbor_sample = [
            {"node_id": p.node_id, "ip": p.ip, "port": p.port}
            for p in all_peers[:5]
        ]

        payload = pack_gossip_message(
            sender_id=self.dht_node.node_id.to_hex(),
            listen_ip=self.dht_node.host,
            listen_port=self.dht_node.port,
            metrics=metrics,
            neighbors=neighbor_sample,
        )

        loop = self._loop or asyncio.get_running_loop()
        sock = self._sock
        created_temp_sock = False

        if sock is None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setblocking(False)
            created_temp_sock = True

        sent_count = 0
        try:
            for peer in target_peers:
                try:
                    await loop.sock_sendto(sock, payload, (peer.ip, peer.port))
                    sent_count += 1
                except OSError as send_err:
                    print(f"[Gossip Warning] Failed to send gossip to {peer.ip}:{peer.port}: {send_err}")
        finally:
            if created_temp_sock:
                sock.close()

        return sent_count

    def handle_gossip(self, raw_data: bytes, sender_addr: Tuple[str, int]) -> Dict[str, Any] | None:
        """
        Process an incoming gossip payload:
        1. Validates and unpacks the envelope.
        2. Updates or creates Peer in DHT routing table.
        3. Updates peer telemetry table with CPU and RAM utilization.
        4. Discovers and ingests new neighbors into the DHT routing table.
        """
        try:
            msg = unpack_gossip_message(raw_data)
        except Exception as exc:
            print(f"[Gossip Error] Invalid gossip packet from {sender_addr}: {exc}")
            return None

        sender_id = msg.get("sender_id", "").strip().lower()
        if not sender_id or sender_id == self.dht_node.node_id.to_hex():
            return None  # Ignore self-messages or malformed sender IDs

        listen_ip = msg.get("listen_ip", sender_addr[0])
        listen_port = int(msg.get("listen_port", sender_addr[1]))
        resources_dict = msg.get("resources", {})
        metrics = NodeResourceMetrics.from_dict(resources_dict)

        # 1. Update DHT routing table with sender
        peer = Peer(
            node_id=sender_id,
            ip=listen_ip,
            port=listen_port,
            last_seen=time.time(),
        )
        self.dht_node.routing_table.add_peer(peer)

        # 2. Update peer telemetry cache
        self.peer_telemetry[sender_id] = PeerTelemetry(
            peer=peer,
            metrics=metrics,
            last_updated=time.time(),
        )

        # 3. Epidemic discovery of advertised neighbors
        for nbr in msg.get("neighbors", []):
            try:
                nbr_id = str(nbr.get("node_id", "")).strip().lower()
                nbr_ip = str(nbr.get("ip", ""))
                nbr_port = int(nbr.get("port", 0))
                if (
                    nbr_id
                    and nbr_id != self.dht_node.node_id.to_hex()
                    and (1 <= nbr_port <= 65535)
                    and nbr_ip
                ):
                    nbr_peer = Peer(node_id=nbr_id, ip=nbr_ip, port=nbr_port, last_seen=time.time())
                    self.dht_node.routing_table.add_peer(nbr_peer)
            except Exception:
                continue

        return msg

    def get_peer_metrics(self, node_id: str) -> Optional[NodeResourceMetrics]:
        """Retrieve the latest telemetry for a given peer Node ID."""
        record = self.peer_telemetry.get(node_id.strip().lower())
        return record.metrics if record else None

    def get_all_peer_telemetry(self) -> Dict[str, PeerTelemetry]:
        """Return all tracked peer telemetry records."""
        return dict(self.peer_telemetry)

    def find_least_loaded_peer(self) -> Optional[Tuple[str, Peer, NodeResourceMetrics]]:
        """
        Identify the peer with the lowest CPU utilization.
        Breaks ties by highest available free RAM.
        Useful for dynamic task distribution in decentralized broker.
        """
        if not self.peer_telemetry:
            return None

        # Filter out peers not heard from in the last 30 seconds
        cutoff = time.time() - 30.0
        active_records = [
            (nid, rec.peer, rec.metrics)
            for nid, rec in self.peer_telemetry.items()
            if rec.last_updated >= cutoff
        ]
        if not active_records:
            return None

        # Sort by cpu_percent ascending, then free RAM descending
        active_records.sort(key=lambda item: (item[2].cpu_percent, -item[2].ram_free_mb))
        return active_records[0]

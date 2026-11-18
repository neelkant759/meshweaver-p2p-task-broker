"""
MeshWeaver - Kademlia Distributed Hash Table (DHT) & Peer Discovery
Week 2: Decentralized P2P Networking Layer

This module implements the decentralized peer discovery engine based on the
Kademlia DHT protocol:
1. 160-bit NodeID with XOR metric distance calculation.
2. Peer metadata management (Node ID, IP address, Port, Last Seen).
3. K-Bucket routing table with least-recently-seen replacement semantics.
4. RPC framing for DHT messages (DHT_PING, DHT_PONG, FIND_NODE, NODES_FOUND).
5. Asynchronous decentralized node bootstrapping and peer discovery without
   relying on any central server or database.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import socket
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

# Kademlia Protocol Constants
DHT_ID_BITS: int = 160
K_BUCKET_SIZE: int = 8  # Default bucket capacity (k)
ALPHA: int = 3          # Concurrency parameter for lookups
DHT_MAGIC_PREFIX: bytes = b"MWDHT:"  # Identifies DHT datagrams


# ============================================================================
# 1. Node ID & Distance Metric
# ============================================================================

class NodeID:
    """
    Represents a 160-bit Kademlia Node Identifier.
    Supports distance calculation using bitwise XOR metric.
    """

    def __init__(self, value: int | bytes | str | None = None) -> None:
        if value is None:
            # Generate 160 random bits (20 bytes)
            raw = os.urandom(DHT_ID_BITS // 8)
            self.int_val: int = int.from_bytes(raw, byteorder="big")
        elif isinstance(value, int):
            self.int_val = value & ((1 << DHT_ID_BITS) - 1)
        elif isinstance(value, bytes):
            self.int_val = int.from_bytes(value[: DHT_ID_BITS // 8], byteorder="big")
        elif isinstance(value, str):
            # Parse hex string
            self.int_val = int(value, 16) & ((1 << DHT_ID_BITS) - 1)
        else:
            raise TypeError(f"Unsupported type for NodeID: {type(value)}")

    @classmethod
    def from_string(cls, text: str) -> NodeID:
        """Derive a deterministic 160-bit NodeID using SHA-1 hash."""
        digest = hashlib.sha1(text.encode("utf-8")).digest()
        return cls(digest)

    def distance(self, other: NodeID) -> int:
        """Compute the Kademlia XOR metric distance: d(x, y) = x ^ y."""
        return self.int_val ^ other.int_val

    def to_hex(self) -> str:
        """Return 40-character hexadecimal representation."""
        return f"{self.int_val:040x}"

    def to_bytes(self) -> bytes:
        """Return 20-byte binary representation."""
        return self.int_val.to_bytes(DHT_ID_BITS // 8, byteorder="big")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, NodeID):
            return self.int_val == other.int_val
        return False

    def __hash__(self) -> int:
        return hash(self.int_val)

    def __repr__(self) -> str:
        return f"NodeID({self.to_hex()[:8]}...)"


# ============================================================================
# 2. Peer Representation
# ============================================================================

@dataclass
class Peer:
    """Represents a remote MeshWeaver peer in the Kademlia DHT."""
    node_id: str   # 40-character hex string
    ip: str
    port: int
    last_seen: float = 0.0

    def get_node_id(self) -> NodeID:
        return NodeID(self.node_id)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "ip": self.ip,
            "port": self.port,
            "last_seen": self.last_seen,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Peer:
        return cls(
            node_id=str(data["node_id"]),
            ip=str(data["ip"]),
            port=int(data["port"]),
            last_seen=float(data.get("last_seen", time.time())),
        )


# ============================================================================
# 3. K-Bucket & Routing Table
# ============================================================================

class KBucket:
    """
    A bucket storing up to k peers whose distance from the local node falls
    within a specific range [2^i, 2^(i+1)).
    """

    def __init__(self, k: int = K_BUCKET_SIZE) -> None:
        self.k: int = k
        self.peers: List[Peer] = []

    def add_or_update(self, peer: Peer) -> bool:
        """
        Add a peer or update its position using least-recently-seen semantics:
        - If peer already exists: move to end (most recently seen).
        - If bucket not full: append peer to end.
        - If bucket full: return False (in basic Kademlia, candidates are kept or pinged).
        """
        for idx, existing in enumerate(self.peers):
            if existing.node_id == peer.node_id:
                # Update address/time and move to tail
                existing.ip = peer.ip
                existing.port = peer.port
                existing.last_seen = time.time()
                self.peers.append(self.peers.pop(idx))
                return True

        if len(self.peers) < self.k:
            peer.last_seen = time.time()
            self.peers.append(peer)
            return True

        return False  # Bucket is full

    def remove(self, node_id_hex: str) -> None:
        self.peers = [p for p in self.peers if p.node_id != node_id_hex]

    def __len__(self) -> int:
        return len(self.peers)


class RoutingTable:
    """
    Kademlia routing table containing 160 KBuckets organized by distance.
    """

    def __init__(self, local_node_id: NodeID, k: int = K_BUCKET_SIZE) -> None:
        self.local_node_id: NodeID = local_node_id
        self.k: int = k
        self.buckets: List[KBucket] = [KBucket(k=self.k) for _ in range(DHT_ID_BITS)]

    def _get_bucket_index(self, node_id: NodeID) -> int:
        """Calculate bucket index based on leading zeros of distance."""
        dist = self.local_node_id.distance(node_id)
        if dist == 0:
            return 0
        # Index corresponds to highest bit set: (bit_length - 1)
        return min(dist.bit_length() - 1, DHT_ID_BITS - 1)

    def add_peer(self, peer: Peer) -> bool:
        """Add or refresh a peer in the appropriate bucket."""
        if peer.node_id == self.local_node_id.to_hex():
            return False  # Do not add self to routing table
        idx = self._get_bucket_index(peer.get_node_id())
        return self.buckets[idx].add_or_update(peer)

    def remove_peer(self, node_id_hex: str) -> None:
        idx = self._get_bucket_index(NodeID(node_id_hex))
        self.buckets[idx].remove(node_id_hex)

    def get_closest_peers(self, target_id: NodeID, count: int = K_BUCKET_SIZE) -> List[Peer]:
        """Return up to `count` peers closest to `target_id` sorted by XOR distance."""
        all_peers: List[Peer] = []
        for bucket in self.buckets:
            all_peers.extend(bucket.peers)

        # Sort peers by XOR distance to target_id
        all_peers.sort(key=lambda p: target_id.distance(p.get_node_id()))
        return all_peers[:count]

    def all_peers(self) -> List[Peer]:
        """Return a flat list of all peers currently in the routing table."""
        res: List[Peer] = []
        for b in self.buckets:
            res.extend(b.peers)
        return res

    def total_peers(self) -> int:
        return sum(len(b) for b in self.buckets)


# ============================================================================
# 4. DHT Protocol & Message Framing
# ============================================================================

def is_dht_payload(data: bytes) -> bool:
    """Check if the datagram starts with the MeshWeaver DHT magic prefix."""
    return isinstance(data, (bytes, bytearray)) and data.startswith(DHT_MAGIC_PREFIX)


def pack_dht_message(msg_type: str, sender_id: str, payload: Dict[str, Any] | None = None) -> bytes:
    """Encode a DHT RPC dictionary into magic-prefixed JSON bytes."""
    envelope = {
        "type": msg_type,
        "sender_id": sender_id,
        "payload": payload or {},
    }
    return DHT_MAGIC_PREFIX + json.dumps(envelope).encode("utf-8")


def unpack_dht_message(raw: bytes) -> Dict[str, Any]:
    """Decode magic-prefixed DHT JSON bytes into a dictionary."""
    if not is_dht_payload(raw):
        raise ValueError("Payload is missing DHT_MAGIC_PREFIX")
    body = raw[len(DHT_MAGIC_PREFIX):].decode("utf-8")
    return json.loads(body)


# ============================================================================
# 5. DHT Node (Decentralized Peer Discovery Controller)
# ============================================================================

class DHTNode:
    """
    Decentralized Kademlia DHT Node.
    Manages local routing table, handles DHT RPC queries, and performs
    dynamic node bootstrap discovery over UDP.
    """

    def __init__(
        self,
        node_id: NodeID | None = None,
        host: str = "127.0.0.1",
        port: int = 8000,
        k: int = K_BUCKET_SIZE,
    ) -> None:
        self.node_id: NodeID = node_id or NodeID()
        self.host: str = host
        self.port: int = port
        self.routing_table: RoutingTable = RoutingTable(self.node_id, k=k)
        self.sock: Optional[socket.socket] = None
        self._is_running: bool = False

    def handle_dht_message(self, raw_data: bytes, sender_addr: Tuple[str, int]) -> bytes | None:
        """
        Process an incoming DHT message and return response bytes (if any).
        Updates local routing table with the sender's contact information.
        """
        try:
            msg = unpack_dht_message(raw_data)
        except Exception as exc:
            print(f"[DHT Error] Failed to decode DHT message from {sender_addr}: {exc}")
            return None

        msg_type = msg.get("type")
        sender_id = msg.get("sender_id", "")
        payload = msg.get("payload", {})

        # Automatically record or refresh sender peer in routing table
        if sender_id and sender_id != self.node_id.to_hex():
            sender_peer = Peer(
                node_id=sender_id,
                ip=sender_addr[0],
                port=sender_addr[1],
                last_seen=time.time(),
            )
            self.routing_table.add_peer(sender_peer)

        # Route RPC command
        if msg_type == "DHT_PING":
            return pack_dht_message("DHT_PONG", self.node_id.to_hex(), {"status": "ok"})

        elif msg_type == "FIND_NODE":
            target_hex = payload.get("target_id", sender_id)
            target_id = NodeID(target_hex)
            closest = self.routing_table.get_closest_peers(target_id, count=self.routing_table.k)
            # Exclude the sender itself from the returned list
            peers_data = [p.to_dict() for p in closest if p.node_id != sender_id]
            return pack_dht_message(
                "NODES_FOUND",
                self.node_id.to_hex(),
                {"target_id": target_hex, "peers": peers_data},
            )

        elif msg_type == "NODES_FOUND":
            # Ingest discovered peers into routing table
            discovered = payload.get("peers", [])
            for p_dict in discovered:
                try:
                    p = Peer.from_dict(p_dict)
                    if p.node_id != self.node_id.to_hex():
                        self.routing_table.add_peer(p)
                except Exception:
                    pass
            return None

        elif msg_type == "DHT_PONG":
            return None

        return None

    async def ping_peer(
        self,
        target_ip: str,
        target_port: int,
        timeout: float = 2.0,
    ) -> bool:
        """Send a DHT_PING to a remote peer and await DHT_PONG."""
        loop = asyncio.get_running_loop()
        query = pack_dht_message("DHT_PING", self.node_id.to_hex())

        client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_sock.setblocking(False)
        try:
            await loop.sock_sendto(client_sock, query, (target_ip, target_port))
            resp_data, _ = await asyncio.wait_for(
                loop.sock_recvfrom(client_sock, 4096),
                timeout=timeout,
            )
            if is_dht_payload(resp_data):
                resp = unpack_dht_message(resp_data)
                if resp.get("type") == "DHT_PONG":
                    sender_id = resp.get("sender_id")
                    if sender_id:
                        self.routing_table.add_peer(
                            Peer(node_id=sender_id, ip=target_ip, port=target_port, last_seen=time.time())
                        )
                    return True
            return False
        except (asyncio.TimeoutError, OSError):
            return False
        finally:
            client_sock.close()

    async def find_node_rpc(
        self,
        target_ip: str,
        target_port: int,
        target_id: NodeID,
        timeout: float = 2.0,
    ) -> List[Peer]:
        """
        Send a FIND_NODE RPC to a peer requesting peers closest to `target_id`.
        Returns the list of peers discovered and adds them to routing table.
        """
        loop = asyncio.get_running_loop()
        query = pack_dht_message(
            "FIND_NODE",
            self.node_id.to_hex(),
            {"target_id": target_id.to_hex()},
        )

        client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_sock.setblocking(False)
        discovered_peers: List[Peer] = []

        try:
            await loop.sock_sendto(client_sock, query, (target_ip, target_port))
            resp_data, _ = await asyncio.wait_for(
                loop.sock_recvfrom(client_sock, 8192),
                timeout=timeout,
            )
            if is_dht_payload(resp_data):
                resp = unpack_dht_message(resp_data)
                if resp.get("type") == "NODES_FOUND":
                    # Record bootstrap peer itself
                    boot_id = resp.get("sender_id")
                    if boot_id:
                        self.routing_table.add_peer(
                            Peer(node_id=boot_id, ip=target_ip, port=target_port, last_seen=time.time())
                        )
                    # Record discovered neighbors
                    peers_list = resp.get("payload", {}).get("peers", [])
                    for p_data in peers_list:
                        p = Peer.from_dict(p_data)
                        if p.node_id != self.node_id.to_hex():
                            self.routing_table.add_peer(p)
                            discovered_peers.append(p)
        except (asyncio.TimeoutError, OSError):
            pass
        finally:
            client_sock.close()

        return discovered_peers

    async def bootstrap(
        self,
        bootstrap_ip: str,
        bootstrap_port: int,
        timeout: float = 2.0,
    ) -> bool:
        """
        Bootstrap into the decentralized network by contacting an initial known peer:
        1. Ping the bootstrap peer to verify availability.
        2. Send FIND_NODE for local NodeID to discover closest neighbors.
        3. Query discovered neighbors iteratively.
        """
        # Step 1: Query bootstrap node for nodes closest to ourselves
        discovered = await self.find_node_rpc(
            bootstrap_ip, bootstrap_port, self.node_id, timeout=timeout
        )

        # Step 2: Query each newly discovered node
        for peer in discovered:
            await self.find_node_rpc(peer.ip, peer.port, self.node_id, timeout=timeout)

        return self.routing_table.total_peers() > 0

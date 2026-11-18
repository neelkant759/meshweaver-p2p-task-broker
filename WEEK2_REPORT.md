# Week 2 Progress Report: MeshWeaver

## Project Details
- **Project**: MeshWeaver – Zero-Dependency P2P Async Task Broker
- **Phase**: Week 2 – Decentralized Peer Discovery (Kademlia DHT)
- **Current Status**: Kademlia DHT & Decentralized Peer Discovery Implemented and Verified

---

## Executive Progress Summary

| Component / Requirement | Status | Notes |
| :--- | :--- | :--- |
| **Node ID Generation & Metric** | **COMPLETED** | 160-bit SHA-1 / random NodeID with bitwise XOR metric in `dht.py` |
| **Peer Information Management** | **COMPLETED** | `Peer` dataclass tracking Node ID, IP, Port, and last seen timestamp |
| **K-Bucket & Routing Table** | **COMPLETED** | 160 distance buckets with $k=8$ capacity and least-recently-seen updates |
| **Kademlia RPC Protocol** | **COMPLETED** | Magic-prefixed JSON messaging (`DHT_PING`, `DHT_PONG`, `FIND_NODE`, `NODES_FOUND`) |
| **Dynamic Node Bootstrap** | **COMPLETED** | `DHTNode.bootstrap()` queries bootstrap peers and dynamically expands network |
| **Backward Compatibility** | **COMPLETED** | Preserved Week 1 text PING-PONG and binary task execution (`MWTK`/`MWRS`) |
| **Automated Testing** | **COMPLETED** | 12 new DHT tests (53 tests total across full suite, 100% pass rate) |

---

## Technical Implementation Details

### 1. 160-Bit NodeID & XOR Distance Metric (`dht.py`)
- Nodes have a 160-bit identifier (`NodeID`) randomly generated or derived via `hashlib.sha1`.
- Implements standard Kademlia XOR metric: $d(x, y) = x \oplus y$.
- Provides hex string conversions (`to_hex()`) and byte representations (`to_bytes()`).

### 2. Peer & Routing Table Management (`Peer`, `KBucket`, `RoutingTable`)
- `Peer`: Tracks contact address, port, node identifier, and freshness timestamp.
- `KBucket`: Fixed capacity $k$ (default 8). When new contact arrives:
  - If peer exists: refresh timestamp and move to tail (most recently seen).
  - If bucket not full: append peer.
  - If full: retain existing active peers.
- `RoutingTable`: 160 buckets covering distances $[2^i, 2^{i+1})$. Local node is excluded. `get_closest_peers(target_id, count=k)` sorts active peers by XOR distance to target.

### 3. Asynchronous DHT RPC & Dynamic Bootstrap (`DHTNode`)
- Server routing in `udp_server.py` dispatches datagrams prefixed with `MWDHT:` to `DHTNode.handle_dht_message()`.
- RPC Messages:
  - `DHT_PING` / `DHT_PONG`: Verifies peer availability and records peer contact.
  - `FIND_NODE`: Requests the $k$ closest peers to a specified target NodeID.
  - `NODES_FOUND`: Returns closest neighbor contacts.
- Dynamic Bootstrap:
  - A joining node contacts any single bootstrap peer `(ip, port)`.
  - Sends `FIND_NODE` for its own `NodeID`.
  - Discovers neighbors from the response and iteratively queries them, populating its routing table without any central server or database.

---

## Automated Test Results

```
Command: python -m unittest discover -s tests -v

Ran 53 tests in 1.115s

OK (53/53 passed, 0 failures, 0 errors, 0 skipped)
```

- **`tests/test_dht.py` (12 tests)**:
  - `test_random_generation`: Random 160-bit ID generation.
  - `test_deterministic_from_string`: SHA-1 deterministic ID derivation.
  - `test_xor_distance_properties`: Identity, symmetry, positivity.
  - `test_peer_serialization_roundtrip`: Peer dict conversion.
  - `test_capacity_limit`: KBucket enforces capacity $k$.
  - `test_update_existing_moves_to_tail`: Least-recently-seen semantics.
  - `test_self_excluded`: Routing table excludes local node ID.
  - `test_get_closest_peers_ordering`: XOR distance sorting.
  - `test_pack_and_unpack`: DHT magic prefix envelope encoding.
  - `test_dht_ping_pong_between_nodes`: Live UDP socket exchange.
  - `test_find_node_rpc`: Live FIND_NODE neighbor retrieval.
  - `test_dynamic_multi_node_bootstrap`: Decentralized 3-node bootstrap (Node C discovers Node A through Node B).
- **`tests/test_udp.py` (8 tests)**: All Week 1 networking tests pass.
- **`tests/test_serializer.py` (33 tests)**: All Week 1 serialization & execution tests pass.

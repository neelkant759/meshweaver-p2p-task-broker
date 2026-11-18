# Week 2 Progress Report: MeshWeaver

## Project Details
- **Project**: MeshWeaver – Zero-Dependency P2P Async Task Broker
- **Phase**: Week 2 – Decentralized Peer Discovery (Kademlia DHT) & Gossip Protocol
- **Current Status**: Kademlia DHT & Gossip Protocol Implemented, Hardened, and Verified

---

## Executive Progress Summary

| Component / Requirement | Status | Notes |
| :--- | :--- | :--- |
| **Node ID Generation & Metric** | **COMPLETED** | 160-bit SHA-1 / random NodeID with XOR metric and strict type guards |
| **Peer Information Management** | **COMPLETED** | `Peer` dataclass with port boundary validation (1–65535) and lowercase normalization |
| **K-Bucket & Routing Table** | **COMPLETED** | 160 distance buckets, $k=8$, least-recently-seen updates, case-insensitive self-exclusion |
| **Kademlia RPC Protocol** | **COMPLETED** | Magic-prefixed JSON messaging (`DHT_PING`, `DHT_PONG`, `FIND_NODE`, `NODES_FOUND`) |
| **Dynamic Node Bootstrap** | **COMPLETED** | `DHTNode.bootstrap()` with multi-hop iterative neighbor discovery |
| **CPU & RAM Telemetry Monitoring** | **COMPLETED** | Native zero-dependency monitoring via Windows ctypes, Linux `/proc`, and optional psutil |
| **Gossip Protocol Framing** | **COMPLETED** | Magic-prefixed JSON envelopes (`MWGOSSIP:`) with sender listening address and resource stats |
| **Asynchronous Background Dissemination**| **COMPLETED** | Periodic async heartbeat loop (5.0s default, configurable) spreading state across peers |
| **Epidemic Discovery & Task Selection** | **COMPLETED** | Transitive neighbor discovery and `find_least_loaded_peer()` for decentralized scheduling |
| **Backward Compatibility** | **COMPLETED** | Week 1 text PING-PONG, binary task execution (`MWTK`/`MWRS`), and DHT preserved |
| **Automated Testing** | **COMPLETED** | 74 tests total across full suite, 100% pass rate in 3.0s |

---

## Technical Hardening & Quality Improvements

### 1. Robust Type & Range Validation (`dht.py`)
- **Distance Metric Type Guard**: Added explicit check in `NodeID.distance(other)` raising a descriptive `TypeError` if `other` is not a `NodeID`.
- **Hex String Normalization**: Lowercase normalization applied across `NodeID` and `Peer` instantiation.
- **Port Boundary Validation**: `Peer.__post_init__` enforces $1 \le \text{port} \le 65535$, preventing routing table poisoning with corrupt or negative ports.
- **Case-Insensitive Self-Exclusion**: `RoutingTable.add_peer` verifies `peer.get_node_id() == self.local_node_id`, ensuring nodes never store themselves regardless of hex formatting.
- **Listen Port Advertising**: `DHT_PING` and `FIND_NODE` payloads explicitly advertise the node's listening server port, eliminating ephemeral client port recording.

### 2. Zero-Dependency Resource Telemetry & Gossip Synchronization (`gossip.py`)
- **Native OS Probing**: Utilizes `ctypes.windll.kernel32.GlobalMemoryStatusEx` and `GetSystemTimes` on Windows, and `/proc/meminfo` and `/proc/stat` on Linux, falling back gracefully without requiring external wheels.
- **Periodic Telemetry Loop**: `GossipEngine` runs an asynchronous background loop sending heartbeat packets to up to $F=3$ randomly sampled peers.
- **Epidemic Neighbor Learning**: Gossip payloads include a sample of known neighbor addresses, propagating topology updates across disconnected partitions.
- **Workload Profiling**: Tracks active peer telemetry records with 30-second stale pruning and offers `find_least_loaded_peer()` for task broker dispatching.

---

## Automated Test Results

```
Command: python -m unittest discover -s tests -v

Ran 74 tests in 2.999s

OK (74/74 passed, 0 failures, 0 errors, 0 skipped)
```

- **`tests/test_gossip.py` (13 tests)**:
  - `test_cpu_percent_range`: CPU utilization calculation returns valid percent in [0.0, 100.0].
  - `test_ram_info_keys_and_values`: RAM metrics (total, used, free, percent) validation.
  - `test_get_metrics_snapshot`: `NodeResourceMetrics` snapshot capture.
  - `test_metrics_serialization_roundtrip`: JSON-safe telemetry dict conversions.
  - `test_is_gossip_payload`: Prefix detection for `MWGOSSIP:`.
  - `test_pack_and_unpack_gossip`: Envelope packing and unpacking validation.
  - `test_unpack_invalid_payload_raises_value_error`: Corrupted / missing header rejection.
  - `test_handle_gossip_updates_peer_and_metrics`: Ingestion of gossip updates routing table and telemetry.
  - `test_handle_gossip_self_message_ignored`: Local node self-gossip drop.
  - `test_handle_gossip_epidemic_neighbor_discovery`: Discovery of transitive peers advertised in gossip payload.
  - `test_find_least_loaded_peer`: Optimal peer selection based on CPU load and free RAM.
  - `test_live_two_node_gossip_telemetry_exchange`: End-to-end periodic UDP gossip telemetry exchange between 2 live nodes.
  - `test_live_three_node_epidemic_discovery`: Decentralized 3-node epidemic propagation without direct bootstrap.
- **`tests/test_dht.py` (20 tests)**: All 20 Kademlia DHT tests pass.
- **`tests/test_udp.py` (8 tests)**: All Week 1 networking tests pass.
- **`tests/test_serializer.py` (33 tests)**: All Week 1 serialization & execution tests pass.

---

## Current Status & Next Steps
- **Current State**: Week 2 Decentralized Peer Discovery (Kademlia DHT) and Gossip Protocol & Resource Monitoring are fully implemented and verified with 74 passing tests.
- **Next Phase (Week 3)**: Decentralized Task Scheduling & Broker (routing compute tasks to the least-loaded nodes, failover, and result collection).

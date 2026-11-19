# MeshWeaver – Zero-Dependency P2P Async Task Broker

## Project Overview & Objective
MeshWeaver is a peer-to-peer asynchronous task broker designed in Python. It enables distributed task offloading, execution, and coordination across peer nodes without relying on heavy external broker infrastructure (such as RabbitMQ, Redis, or Celery).

The project is structured into four weekly implementation phases:
- **Week 1**: Core Networking (Async UDP Server & Client), Function Serialization (Cloudpickle), Remote Task Execution.
- **Week 2**: Decentralized Peer Discovery (Kademlia DHT, 160-bit NodeIDs, XOR routing table, dynamic node joining).
- **Week 3**: Distributed Task Scheduling, Load Balancing, Fault Tolerance.
- **Week 4**: Verification, Benchmarking, Performance Profiling, Final Documentation.

---

## Project Structure
```
meshweaver-p2p-task-broker/
├── README.md               # Project documentation and roadmap
├── WEEK1_REPORT.md         # Week 1 status report and implementation records
├── WEEK2_REPORT.md         # Week 2 status report (Kademlia DHT & Peer Discovery)
├── requirements.txt        # cloudpickle>=3.0.0
├── dht.py                  # Kademlia DHT: NodeID, Peer, KBucket, RoutingTable, DHTNode
├── udp_server.py           # Async UDP server: PING-PONG, DHT discovery, task routing
├── udp_client.py           # Async UDP client: PING-PONG, task submission
├── serializer.py           # Task & function serializer using cloudpickle
├── executor.py             # Remote function executor
└── tests/
    ├── __init__.py
    ├── test_dht.py         # 20 Kademlia DHT & peer discovery tests
    ├── test_udp.py         # 8 UDP networking unit & integration tests
    └── test_serializer.py  # 33 serializer, executor & socket integration tests
```

---

## Kademlia DHT Architecture (Week 2)

MeshWeaver implements a decentralized peer discovery engine based on the Kademlia Distributed Hash Table (DHT) protocol, operating 100% over asynchronous UDP sockets without central servers or registries.

### 1. 160-bit Node ID Generation
Every node in the MeshWeaver network has a unique 160-bit identifier (`NodeID`):
- **Random generation**: Produced using cryptographically secure entropy (`os.urandom(20)`).
- **Deterministic generation**: Derived from node names or network identifiers using `hashlib.sha1`.
- Expressed internally as an integer and represented externally as a 40-character hexadecimal string.

### 2. XOR Metric Distance Calculation
Kademlia calculates distance between any two nodes $x$ and $y$ using bitwise exclusive OR:
$$\Delta(x, y) = x \oplus y$$
Key mathematical properties:
- **Identity**: $\Delta(x, x) = 0$
- **Symmetry**: $\Delta(x, y) = \Delta(y, x)$
- **Triangle Inequality**: $\Delta(x, z) \le \Delta(x, y) \oplus \Delta(y, z)$

### 3. K-Bucket Routing Table
- The `RoutingTable` manages 160 `KBucket` instances, where bucket $i$ holds peers at distance $[2^i, 2^{i+1})$ from the local node.
- **Bucket capacity**: Fixed at $k=8$ peers.
- **Least-Recently-Seen Eviction Policy**: When an existing peer communicates, it is refreshed and moved to the tail of the bucket (most-recently-seen position). When the bucket is full, existing stable peers are preserved.
- **Self-Exclusion**: The local node is excluded from its own routing table regardless of string casing.

### 4. Decentralized Peer Discovery & Bootstrap Workflow
When a new node joins the network dynamically:
```
  [Joining Node C]                  [Bootstrap Node B]                  [Neighbor Node A]
         │                                   │                                  │
         │─── FIND_NODE(target=Node C) ─────▶│                                  │
         │    (Magic prefix MWDHT:)          │                                  │
         │                                   │── (Locates closest peers)        │
         │◀── NODES_FOUND ([Node A info]) ───│                                  │
         │                                                                      │
         │─── FIND_NODE(target=Node C) ────────────────────────────────────────▶│
         │                                                                      │
         │◀── NODES_FOUND ([...]) ──────────────────────────────────────────────│
         ▼                                                                      ▼
  (Node C discovers Node B & Node A without any central database or coordinator)
```
1. **Initial Contact**: The joining node sends a `FIND_NODE` query for its own `NodeID` to any known peer address (`bootstrap_ip`, `bootstrap_port`).
2. **Neighbor Discovery**: The bootstrap peer records the new node and returns the $k$ closest peers it knows.
3. **Iterative Expansion**: The joining node queries the returned neighbors, recursively populating its routing table until all nearby peers are discovered.

---

## Gossip Protocol & Resource Monitoring Architecture

The Gossip Protocol (`gossip.py`) acts as the dynamic telemetry and decentralized synchronization layer on top of Kademlia DHT:

### 1. Zero-Dependency Hardware Telemetry (`ResourceMonitor`)
- **CPU Usage Tracking**: Non-blocking calculation of system CPU utilization percentage.
  - Windows: Native `ctypes.windll.kernel32.GetSystemTimes` delta calculations.
  - Linux: Real-time `/proc/stat` parsing.
  - Fallback / Optional: `psutil` if installed.
- **RAM Usage Tracking**: Exact measurement of total, used, free MB and memory load %.
  - Windows: Native `GlobalMemoryStatusEx` via `ctypes`.
  - Linux: `/proc/meminfo` parsing.
  - Fallback / Optional: `psutil` virtual memory if available.

### 2. Message Framing & Envelope (`MWGOSSIP:`)
Structured heartbeat packets identified by the `MWGOSSIP:` binary magic prefix:
```json
{
  "type": "GOSSIP_HEARTBEAT",
  "sender_id": "bdc5145c603e50ec0bb50a7bf1000db8e7d25cc1",
  "listen_ip": "127.0.0.1",
  "listen_port": 8000,
  "timestamp": 1731940000.0,
  "resources": {
    "cpu_percent": 12.5,
    "ram_total_mb": 16384.0,
    "ram_used_mb": 8192.0,
    "ram_free_mb": 8192.0,
    "ram_percent": 50.0
  },
  "neighbors": [
    {"node_id": "763a443d433249871c7c507bf81564856d290ef2", "ip": "127.0.0.1", "port": 8001}
  ]
}
```

### 3. Asynchronous Background Dissemination (`GossipEngine`)
- **Periodic Loop**: An async background task running every 5.0 seconds (configurable for tests).
- **Fanout Selection**: In each tick, randomly selects up to $F=3$ neighboring peers from the Kademlia routing table.
- **Epidemic Discovery**: Transmits advertised neighbors alongside telemetry, allowing nodes to organically discover transitive peers without manual bootstrapping.
- **Dynamic Task Scheduling Readiness**: `find_least_loaded_peer()` identifies peers with lowest CPU load and greatest free memory, enabling intelligent decentralized workload placement.

---

## Decentralized Task Scheduling & Intelligent Peer Selection (`router.py`)

Week 3 introduces the decentralized task scheduling layer (`TaskRouter`) that uses real-time gossip telemetry to select the optimal peer across the P2P network:

### 1. Multi-Metric Evaluation Hierarchy
1. **Primary Metric (Lowest CPU)**: Peers are ranked by `cpu_percent` in ascending order, prioritizing available computational capacity.
2. **Secondary Metric (Highest Free RAM)**: If multiple peers report identical CPU loads (e.g. idle nodes at 0.0%), ties are broken by `ram_free_mb` in descending order.
3. **Tertiary Metric (Deterministic NodeID)**: If CPU and RAM metrics are both identical, deterministic alphabetical ordering of `node_id` guarantees consistent, jitter-free tie-breaking without central coordination.

### 2. Stale Telemetry Pruning & Dynamic Exclusion
- **Stale Record Invalidation**: Telemetry entries older than `stale_threshold` (default: 30.0s) are automatically ignored.
- **Node Exclusion**: Supports `exclude_nodes` (case-insensitive) to omit specific peers (e.g., self or unresponsive nodes during failover).
- **Error Handling**: Raises `NoPeerAvailableError` when no eligible, non-stale peers exist.

### 3. Configurable Local Fallback
When `fallback_to_local=True`, `schedule_task()` falls back to running the task locally on the host via `executor.run_task_locally()`, ensuring uninterrupted service when the node is isolated or the mesh is idle.

---

## Dependencies
```bash
# Install dependencies
pip install -r requirements.txt
```
- **cloudpickle 3.1.2**: Function & lambda serialization.
- **Core Engine (Networking, DHT, Gossip, Router)**: 100% Zero-Dependency (built entirely with Python standard library: `ctypes`, `hashlib`, `json`, `asyncio`, `socket`, `time`, `dataclasses`).

---

## Running Tests
```bash
# Full test suite across all modules (88 tests)
python -m unittest discover -s tests -v

# Run Task Router & Peer Selection tests only (14 tests)
python -m unittest tests/test_router.py -v

# Run Gossip Protocol & Resource Monitoring tests only (13 tests)
python -m unittest tests/test_gossip.py -v

# Run Kademlia DHT peer discovery tests only (20 tests)
python -m unittest tests/test_dht.py -v

# Run UDP networking tests only (8 tests)
python -m unittest tests/test_udp.py -v

# Run Serializer and execution tests only (33 tests)
python -m unittest tests/test_serializer.py -v
```

---

## Test Results
```
Ran 88 tests in 2.957s   OK   (0 failures, 0 errors, 0 skipped)
```

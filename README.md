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
    ├── test_dht.py         # 12 Kademlia DHT & peer discovery tests
    ├── test_udp.py         # 8 UDP networking unit & integration tests
    └── test_serializer.py  # 33 serializer, executor & socket integration tests
```

---

## Dependencies
```bash
# Install dependencies
pip install -r requirements.txt
```
- **cloudpickle 3.1.2**: Function & lambda serialization.
- **Kademlia DHT**: 100% Zero-Dependency (built on Python standard library: `hashlib`, `json`, `asyncio`, `socket`).

---

## Running Tests
```bash
# Full test suite across all modules (53 tests)
python -m unittest discover -s tests -v

# Run Kademlia DHT peer discovery tests only (12 tests)
python -m unittest tests/test_dht.py -v

# Run UDP networking tests only (8 tests)
python -m unittest tests/test_udp.py -v

# Run Serializer and execution tests only (33 tests)
python -m unittest tests/test_serializer.py -v
```

---

## Test Results
```
Ran 53 tests in 1.115s   OK   (0 failures, 0 errors, 0 skipped)
```

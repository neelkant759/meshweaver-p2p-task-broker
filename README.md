# MeshWeaver – Zero-Dependency P2P Async Task Broker

## Project Overview & Objective
MeshWeaver is a peer-to-peer asynchronous task broker designed in Python. The objective of the project is to enable distributed task offloading, execution, and coordination across peer nodes without relying on heavy external broker infrastructure (such as RabbitMQ, Redis, or Celery).

The project is structured into four weekly implementation phases.

---

## Week 1: Core Networking, Serialization & Foundation

### Scope & Status
1. **Project Structure Creation**: **Completed** (Initial files, directories, and manifests scaffolded).
2. **Asynchronous UDP Server**: **Completed** (Implemented in `udp_server.py` using Python `asyncio` and `socket`).
3. **Asynchronous UDP Client**: **Completed** (Implemented in `udp_client.py` using Python `asyncio` and `socket`).
4. **PING-PONG Communication**: **Completed** (Verified across separate processes and in automated test suites).
5. **Function Serialization**: **Proposed (Pending Mentor Approval)** (Evaluation of `cloudpickle`; implementation pending).
6. **Remote Function Execution & Result Handling**: **Pending** (Execution engine and result pipeline to be implemented).
7. **Automated Testing**: **In Progress** (Unit & integration test suites for UDP server and client active in `tests/test_udp.py`).
8. **Documentation & Progress Reporting**: **In Progress** (Modules 1 & 2 documentation and reports completed; subsequent modules pending).

---

## Project Structure (Week 1)
```
meshweaver-p2p-task-broker/
├── README.md               # Project documentation and roadmap
├── WEEK1_REPORT.md         # Week 1 status report and implementation checklist
├── requirements.txt        # Proposed dependencies (cloudpickle pending mentor approval)
├── udp_server.py           # Asynchronous UDP server module (implemented)
├── udp_client.py           # Asynchronous UDP client module (implemented)
├── serializer.py           # Task & function serializer module (scaffolded, implementation pending)
├── executor.py             # Remote execution engine (scaffolded, implementation pending)
└── tests/                  # Automated unit test suite
    ├── __init__.py         # Test package initialization
    ├── test_udp.py         # UDP networking unit and integration tests (implemented)
    └── test_serializer.py  # Serialization tests (scaffolded, tests pending)
```

---

## Getting Started
### Prerequisites
- Python 3.10+ recommended

### Running the UDP Server
```bash
python udp_server.py
```

### Running the UDP Client
```bash
# Send default PING
python udp_client.py

# Send custom message with specific timeout
python udp_client.py --message "PING" --timeout 2.0
```

### Running Tests
```bash
# Run all unit tests
python -m unittest discover tests

# Run UDP test suite specifically
python -m unittest tests/test_udp.py
```

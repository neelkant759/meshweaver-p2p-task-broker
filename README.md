# MeshWeaver – Zero-Dependency P2P Async Task Broker

## Project Overview & Objective
MeshWeaver is a peer-to-peer asynchronous task broker designed in Python. The objective of the project is to enable distributed task offloading, execution, and coordination across peer nodes without relying on heavy external broker infrastructure (such as RabbitMQ, Redis, or Celery).

The project is structured into four weekly implementation phases.

---

## Week 1: Core Networking, Serialization & Foundation

### Scope & Status
1. **Project Structure Creation**: **Completed** (Initial files, directories, and manifests scaffolded).
2. **Asynchronous UDP Server**: **Pending** (To be implemented using Python `asyncio` and sockets).
3. **Asynchronous UDP Client**: **Pending** (To be implemented for asynchronous datagram dispatch).
4. **PING-PONG Communication**: **Pending** (Targeting round-trip verification between two Python runtime instances).
5. **Function Serialization**: **Proposed (Pending Mentor Approval)** (Evaluation of `cloudpickle`; implementation pending).
6. **Remote Function Execution & Result Handling**: **Pending** (Execution engine and result pipeline to be implemented).
7. **Automated Testing**: **Pending** (Unit test suites in `tests/` to be implemented using Python `unittest`).
8. **Documentation & Progress Reporting**: **In Progress** (Scaffolding documentation completed; implementation details pending).

---

## Project Structure (Week 1)
```
meshweaver-p2p-task-broker/
├── README.md               # Project documentation and roadmap
├── WEEK1_REPORT.md         # Week 1 status report and implementation checklist
├── requirements.txt        # Proposed dependencies (cloudpickle pending mentor approval)
├── udp_server.py           # Asynchronous UDP server module (scaffolded, implementation pending)
├── udp_client.py           # Asynchronous UDP client module (scaffolded, implementation pending)
├── serializer.py           # Task & function serializer module (scaffolded, implementation pending)
├── executor.py             # Remote execution engine (scaffolded, implementation pending)
└── tests/                  # Automated unit test suite
    ├── __init__.py         # Test package initialization
    ├── test_udp.py         # UDP networking tests (scaffolded, tests pending)
    └── test_serializer.py  # Serialization tests (scaffolded, tests pending)
```

---

## Getting Started
### Prerequisites
- Python 3.10+ recommended

### Environment Setup
```bash
python -m venv venv
# On Windows:
.\venv\Scripts\activate
# On Linux/macOS:
source venv/bin/activate

# Install proposed dependencies once approved:
pip install -r requirements.txt
```

### Running Tests
```bash
python -m unittest discover tests
```

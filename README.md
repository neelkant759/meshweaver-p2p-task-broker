# MeshWeaver – Zero-Dependency P2P Async Task Broker

## Project Overview & Objective
MeshWeaver is a peer-to-peer asynchronous task broker designed in Python. It enables distributed task offloading, execution, and coordination across peer nodes without relying on heavy external broker infrastructure (such as RabbitMQ, Redis, or Celery).

The project is structured into four weekly implementation phases.

---

## Week 1: Core Networking, Serialization & Foundation

### Scope & Status

| # | Requirement | Status |
|---|-------------|--------|
| 1 | Project Structure Creation | **Completed** |
| 2 | Asynchronous UDP Server (`udp_server.py`) | **Completed** |
| 3 | Asynchronous UDP Client (`udp_client.py`) | **Completed** |
| 4 | PING-PONG Communication | **Completed** |
| 5 | Function Serialization with Cloudpickle (`serializer.py`) | **Completed** |
| 6 | Remote Function Execution (`executor.py`) | **Completed** |
| 7 | Automated Testing (`tests/test_udp.py`, `tests/test_serializer.py`) | **Completed** |
| 8 | Documentation & Progress Reporting | **In Progress** |

---

## Project Structure (Week 1)
```
meshweaver-p2p-task-broker/
├── README.md               # Project documentation and roadmap
├── WEEK1_REPORT.md         # Week 1 status report and implementation checklist
├── requirements.txt        # cloudpickle>=3.0.0 (approved, installed)
├── udp_server.py           # Async UDP server – PING-PONG + task execution routing
├── udp_client.py           # Async UDP client – PING-PONG + task submission
├── serializer.py           # Task & function serializer using cloudpickle
├── executor.py             # Remote function executor
└── tests/
    ├── __init__.py
    ├── test_udp.py          # 8 UDP networking unit & integration tests
    └── test_serializer.py   # 33 serializer, executor & socket integration tests
```

---

## Dependencies
```bash
# Install dependencies
pip install -r requirements.txt
```
- **cloudpickle 3.1.2**: Serializes Python functions, lambdas, and closures into bytes for UDP transmission. Standard `pickle` cannot handle these cases.

---

## How It Works

### 1. Function Serialization (Module 3 – `serializer.py`)
```python
from serializer import serialize_task, deserialize_task

def add(x, y): return x + y

# Serialize function + args into UDP-ready bytes
payload = serialize_task(add, args=(10, 20))

# Reconstruct on the other side
func, args, kwargs = deserialize_task(payload)
result = func(*args, **kwargs)   # → 30
```

### 2. Remote Execution (Module 4 – `executor.py`)
```python
from executor import execute_task
from serializer import serialize_task, deserialize_result

payload = serialize_task(add, args=(10, 20))
result_bytes = execute_task(payload)      # server side
status, value = deserialize_result(result_bytes)
# status = 'ok', value = 30
```

### 3. Full UDP Round-Trip
```
Client                              Server
  │── serialize_task(add, 10, 20) ──▶│
  │                                  │  execute_task(payload)
  │◀── serialize_result(30, 'ok') ───│
  │  deserialize_result → (ok, 30)   │
```

---

## Running the Server
```bash
python udp_server.py
```

## Running the Client
```bash
# PING health check
python udp_client.py

# Custom message
python udp_client.py --message "PING" --timeout 2.0
```

## Running Tests
```bash
# Full suite (41 tests)
python -m unittest discover -s tests -v

# UDP networking tests only (8 tests)
python -m unittest tests/test_udp.py -v

# Serializer + executor + integration tests (33 tests)
python -m unittest tests/test_serializer.py -v
```

---

## Actual Test Results (Week 1 Final)
```
Ran 41 tests in 0.555s   OK   (0 failures, 0 errors)
```

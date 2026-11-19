# Week 3 Progress Report: MeshWeaver

## Project Details
- **Project**: MeshWeaver – Zero-Dependency P2P Async Task Broker
- **Phase**: Week 3 – Decentralized Task Scheduling & Intelligent Task Routing (Part 1: Peer Selection)
- **Current Status**: Intelligent Peer Selection & Task Router Implemented, Verified, and Tested

---

## Executive Progress Summary

| Component / Requirement | Status | Notes |
| :--- | :---: | :--- |
| **`TaskRouter` Core Architecture** | **COMPLETED** | Created `router.py` managing decentralized peer evaluation and task scheduling |
| **Telemetry Ingestion** | **COMPLETED** | Directly queries `GossipEngine.peer_telemetry` for real-time CPU % and RAM MB metrics |
| **Lowest CPU Selection** | **COMPLETED** | Evaluates active peers and prioritizes nodes with minimum `cpu_percent` |
| **RAM Tie-Breaking** | **COMPLETED** | When CPU usage is identical, selects the peer with maximum available `ram_free_mb` |
| **Deterministic NodeID Tie-Breaking** | **COMPLETED** | When CPU and RAM metrics match, deterministic alphabetical `node_id` ordering breaks ties |
| **Stale Telemetry Pruning** | **COMPLETED** | Filters out peer records older than `stale_threshold` (default: 30.0s, configurable) |
| **Peer Exclusion Filtering** | **COMPLETED** | Case-insensitive exclusion of specific peer IDs (e.g. self or previously failed nodes) |
| **No Eligible Peer Handling** | **COMPLETED** | Raises `NoPeerAvailableError` with descriptive telemetry count when no peers qualify |
| **Configurable Local Fallback** | **COMPLETED** | `fallback_to_local=True` invokes `executor.run_task_locally()`, avoiding duplicate code |
| **Asynchronous Integration** | **COMPLETED** | Provides `async_select_target_peer()` and `async_schedule_task()` for non-blocking workflows |
| **Automated Testing** | **COMPLETED** | 14 dedicated router unit & async tests in `tests/test_router.py`; **88 tests passing total** |

---

## Technical Implementation Details (`router.py`)

### 1. Multi-Metric Selection Hierarchy (`select_target_peer`)
Peers are evaluated through a three-tier deterministic comparator:
```python
candidates.sort(
    key=lambda item: (
        round(item[2].cpu_percent, 4),      # Tier 1: Lowest CPU percentage
        -round(item[2].ram_free_mb, 4),     # Tier 2: Highest available free RAM
        item[0],                            # Tier 3: Deterministic NodeID ordering
    )
)
```

### 2. Telemetry Staleness Pruning
Peer records are validated against a reference timestamp:
```python
cutoff_time = current_time - self.stale_threshold
if rec.last_updated < cutoff_time:
    continue  # Exclude stale record
```

### 3. Local Fallback Without Code Duplication
When `fallback_to_local=True`, `schedule_task` reuses the established `executor.run_task_locally()` helper from Week 1:
```python
except NoPeerAvailableError:
    if self.fallback_to_local:
        status, val = run_task_locally(func, args=args, kwargs=kwargs)
        return status, val, "local"
    raise
```

---

## Automated Test Results

```
Command: python -m unittest discover -s tests -v

Ran 88 tests in 2.957s

OK (88/88 passed, 0 failures, 0 errors, 0 skipped)
```

- **`tests/test_router.py` (14 tests)**:
  - `test_lowest_cpu_peer_selection`: Lowest CPU peer is selected among candidates.
  - `test_ram_based_tie_breaking`: Peer with highest free RAM is chosen on identical CPU load.
  - `test_deterministic_node_id_tie_breaking`: Deterministic alphabetical NodeID ordering breaks ties.
  - `test_stale_peer_rejection`: Stale peers (> 30s) are rejected even if reporting lower CPU load.
  - `test_all_peers_stale_raises_no_peer_available_error`: Rejection when all peers are stale.
  - `test_excluded_peer_rejection`: Specific node IDs in `exclude_nodes` are bypassed.
  - `test_excluded_peer_case_insensitivity`: Case-insensitive matching for uppercase hex IDs.
  - `test_empty_telemetry_raises_no_peer_available_error`: Clean exception when telemetry table is empty.
  - `test_local_fallback_disabled_raises_error`: Strict mode raises `NoPeerAvailableError`.
  - `test_local_fallback_enabled_executes_locally`: Fallback executes callable locally and marks `executed_by='local'`.
  - `test_local_fallback_handles_task_exception`: Captures exceptions during fallback and returns `status='error'`.
  - `test_schedule_task_returns_peer_when_available`: Returns `('peer_selected', (peer, metrics), node_id)` when peer is chosen.
  - `test_async_select_target_peer`: Asynchronous selection wrapper.
  - `test_async_schedule_task_fallback`: Asynchronous task execution with local fallback.
- **`tests/test_gossip.py` (13 tests)**: All Week 2 Gossip & Resource Monitoring tests pass.
- **`tests/test_dht.py` (20 tests)**: All Week 2 Kademlia DHT tests pass.
- **`tests/test_serializer.py` (33 tests)**: All Week 1 serialization & execution tests pass.
- **`tests/test_udp.py` (8 tests)**: All Week 1 networking tests pass.

---

## Next Steps
- **Stage 2 of Week 3**:
  - Implement unique `task_id` tracking across requests and responses.
  - Implement duplicate task execution protection on receiving worker nodes.
  - Implement remote task dispatch, timeout detection, and automatic task reassignment to alternative peers.

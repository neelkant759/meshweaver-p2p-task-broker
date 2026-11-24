# Week 4 Progress Report: MeshWeaver

## Project Details
- **Project**: MeshWeaver – Zero-Dependency P2P Async Task Broker
- **Phase**: Week 4 – Secure Peer Communication & Cryptographic Task Signatures
- **Current Status**: Cryptographic HMAC-SHA256 Task & Result Signatures, Tamper Prevention, and TLS/SSL Secure Stream Transport Implemented and Fully Verified
- **Date**: 24 November 2026

---

## Executive Progress Summary

| Component / Requirement | Status | Notes |
| :--- | :---: | :--- |
| **Networking Architecture Audit** | **COMPLETED** | Audited core networking: MeshWeaver uses UDP (`SOCK_DGRAM`) for DHT, Gossip, and Tasks. Identified that Python's standard-library `ssl` only supports TCP streams (`SOCK_STREAM`) |
| **Protocol Boundary Enforcement** | **COMPLETED** | Explicit separation: TLS/SSL is applied strictly to TCP streams (`SecureTCPServer` / `SecureTCPClient`), while HMAC-SHA256 cryptographic signatures protect UDP datagrams |
| **TLS/SSL Stream Security (TCP)** | **COMPLETED** | Created `security.create_ssl_context()`, `SecureTCPServer`, and `SecureTCPClient` using Python's standard-library `ssl` |
| **Certificate Verification & Validation** | **COMPLETED** | Supports X.509 certificate chains, custom CA bundles, mutual TLS, and certificate verification failure handling |
| **Cryptographic Task Signing** | **COMPLETED** | `security.sign_payload()` signs task payloads (`MWSTK`) and result envelopes (`MWSRS`) with 32-byte HMAC-SHA256 |
| **Constant-Time Signature Verification** | **COMPLETED** | `security.verify_and_unpack_payload()` verifies signatures using `hmac.compare_digest` to prevent side-channel timing attacks |
| **Tamper Detection & Unverified Execution Prevention** | **COMPLETED** | Tampered or forged payloads are rejected immediately; tasks are **never** executed by the worker |
| **Worker-Side Signature Enforcement** | **COMPLETED** | `udp_server.py` enforces signatures when `enforce_signatures=True`, rejecting unsigned task submissions with signed error envelopes |
| **Result Envelope Authentication** | **COMPLETED** | Worker nodes sign outgoing execution results with `MWSRS` magic prefix when `secret_key` is configured |
| **Client Demux Verification** | **COMPLETED** | `udp_client.py` unpacks and verifies signed task results in the background demultiplexer loop before resolving futures |
| **Router Key Propagation** | **COMPLETED** | `router.py` propagates `secret_key` and `enforce_signatures` through `TaskRouter` and `dispatch_task()` |
| **Node CLI Integration** | **COMPLETED** | `node.py` CLI supports `--secret-key` and `--enforce-signatures` flags |
| **Task ID Preservation** | **COMPLETED** | Task ID correlation is preserved through signed envelopes without breaking deduplication or retries |
| **Backward Compatibility** | **COMPLETED** | Unsigned task requests are accepted and executed normally when `enforce_signatures=False` |
| **Zero External Dependencies** | **COMPLETED** | Pure standard library implementation (`ssl`, `hmac`, `hashlib`, `asyncio`, `socket`) |
| **Automated Regression Suite** | **COMPLETED** | **154 tests passing total** (20 new security tests + 134 existing tests, 0 failures, 0 errors, 100% pass rate) |

---

## Architectural Audit & Protocol Boundary Analysis

### 1. Transport Layer Audit
MeshWeaver's core networking operates over asynchronous UDP (`SOCK_DGRAM`):
- **Kademlia DHT**: RPC queries (`PING`, `STORE`, `FIND_NODE`, `FIND_VALUE`) prefixed with `MWDHT:`.
- **Gossip Protocol**: Hardware telemetry dissemination (CPU %, free RAM MB) prefixed with `MWGOSSIP:`.
- **Task Dispatch & Results**: Serialized function execution payloads (`MWTK:`) and execution result envelopes (`MWRS:`).

### 2. Protocol Boundary & The Python `ssl` Limitation
Python's built-in `ssl` module wraps OpenSSL for stream sockets (`SOCK_STREAM` / TCP). Attempting to wrap a `SOCK_DGRAM` (UDP) socket with `ssl.SSLContext.wrap_socket()` immediately raises:
```python
NotImplementedError: only stream sockets are supported
```
Python's standard library does not provide DTLS (Datagram Transport Layer Security). 

Therefore, applying TLS to UDP datagrams in standard Python is architecturally impossible without third-party C extensions. To maintain MeshWeaver's **zero external dependency** architecture while providing enterprise-grade security, the security architecture is split along standard protocol boundaries:
1. **TCP Stream Subsystem**: TLS 1.2+ encryption and X.509 certificate verification for high-throughput stream connections and peer-to-peer data synchronization (`SecureTCPServer` and `SecureTCPClient`).
2. **UDP Datagram Subsystem**: Cryptographic authentication, payload integrity, and tamper prevention using HMAC-SHA256 signatures with constant-time verification.

---

## Technical Implementation Details

### 1. Cryptographic Task & Result Signatures (`security.py`)

#### Wire Framing Layout
Signed MeshWeaver datagrams prepend a 5-byte magic identifier and a 32-byte HMAC-SHA256 digest:
```
+---------------+------------------------+---------------------------------+
| Magic (5B)    | HMAC-SHA256 (32B)      | Raw Serialized Payload (NB)     |
| "MWSTK" (task)| computed over payload  | (e.g. MWTK:v1:... or MWRES:...) |
| "MWSRS" (res) |                        |                                 |
+---------------+------------------------+---------------------------------+
```

#### Primitives
- **`compute_hmac(payload, secret_key)`**: Computes a 32-byte HMAC-SHA256 digest over the binary payload using `hashlib.sha256`. Normalizes string or byte keys.
- **`sign_payload(payload, secret_key, is_result=False)`**: Constructs the signed envelope with `MWSTK` (for task requests) or `MWSRS` (for task results).
- **`verify_and_unpack_payload(signed_data, secret_key)`**:
  - Validates wire format and length ($\ge 37$ bytes).
  - Extracts the 32-byte transmitted digest and raw payload.
  - Computes the expected HMAC over the payload using the shared secret key.
  - Verifies using `hmac.compare_digest(received, expected)` to guarantee **constant-time execution**, immunizing the node against side-channel timing attacks.
  - Raises `InvalidSignatureError` if a single bit is modified, or `ValueError` if the header is malformed.
- **`is_signed_payload(data)`**: Fast boolean check verifying whether a datagram starts with `MWSTK` or `MWSRS`.

---

### 2. TLS/SSL Stream Security Subsystem (`security.py`)

- **`create_ssl_context(...)`**:
  - Configures standard-library `ssl.SSLContext` with `ssl.PROTOCOL_TLS_SERVER` or `ssl.PROTOCOL_TLS_CLIENT`.
  - Enforces minimum protocol version `ssl.TLSVersion.TLSv1_2`, disabling deprecated SSLv2, SSLv3, TLS 1.0, and TLS 1.1.
  - Loads X.509 certificate chains (`certfile`, optional `keyfile`) and custom CA authority bundles (`cafile`).
  - Supports configurable certificate validation (`ssl.CERT_REQUIRED`, `ssl.CERT_OPTIONAL`, `ssl.CERT_NONE`) and hostname validation.
- **`SecureTCPServer`**:
  - Asynchronous TCP server utilizing `asyncio.start_server(..., ssl=ssl_context)`.
  - Dynamically binds to specified or ephemeral ports (`port=0`).
  - Provides `is_running` property and graceful `stop()` lifecycle.
- **`SecureTCPClient`**:
  - Asynchronous TCP client utilizing `asyncio.open_connection(..., ssl=ssl_context)`.
  - Securely negotiates TLS handshakes, verifies server certificates against loaded CAs, and exchanges encrypted stream data with configurable timeouts.

---

### 3. Server & Client Integration (`udp_server.py`, `udp_client.py`, `router.py`, `node.py`)

#### Worker Server (`udp_server.py`)
- Route 0 in `handle_message()` checks `is_signed_payload(data)`:
  - If signed and `secret_key` is present: verifies signature with `verify_and_unpack_payload()`.
  - If signature is invalid (tampered or wrong key): immediately responds with a signed error envelope and aborts. **The task is never unpickled or executed**.
  - If unsigned and `enforce_signatures=True`: rejects submission with `Security Error: Cryptographic signature required`.
- Outgoing task results:
  - When `secret_key` is configured, result envelopes (`MWRES:`) are automatically signed with `sign_payload(..., is_result=True)` (`MWSRS:`).

#### Async UDP Client (`udp_client.py`)
- Added `secret_key` and `enforce_signatures` parameters to `AsyncUDPClient.__init__`.
- `send_task()`: serializes the task and signs it with `sign_payload()` when `secret_key` is present.
- `_listen_loop()`: background demultiplexer inspects incoming datagrams:
  - Unpacks and verifies `MWSRS` signed responses using `verify_and_unpack_payload()`.
  - Drops invalidly signed datagrams with warnings.
  - Drops unsigned results when `enforce_signatures=True`.

#### Router & CLI Integration (`router.py`, `node.py`)
- `TaskRouter.__init__` and `dispatch_task()` accept and pass `secret_key` and `enforce_signatures` down to `AsyncUDPClient`.
- CLI in `node.py` exposes:
  - `--secret-key <KEY>`: Enables HMAC-SHA256 task and result signing.
  - `--enforce-signatures`: Rejects unsigned task submissions.

---

## Test Verification & Regression Results

A dedicated test suite was authored in `tests/test_security.py` containing 20 tests:
1. `TestCryptographicSignatures` (12 unit tests):
   - HMAC determinism, key uniqueness, payload sensitivity.
   - Wire framing layout (`MWSTK` / `MWSRS`, 32-byte digest).
   - Valid unpack, string vs byte key normalization.
   - Tampered payload byte rejection (`InvalidSignatureError`).
   - Tampered digest byte rejection.
   - Wrong secret key rejection.
   - Header corruption and truncated packet handling.
   - `is_signed_payload` utility validation.
2. `TestSecureTCPCommunication` (3 unit/integration tests):
   - TLS TCP server/client roundtrip over encrypted stream.
   - TLS certificate verification failure on mismatched/untrusted CAs (`ssl.SSLCertVerificationError`).
   - Protocol boundary verification (`ssl` raises `NotImplementedError` when applied to UDP datagram sockets).
3. `TestUDPSecurityIntegration` (5 end-to-end integration tests):
   - Signed task execution round-trip with task ID preservation.
   - Tampered task payload rejected by worker and **never executed**.
   - Unsigned task rejected when `enforce_signatures=True`.
   - Backward compatibility: unsigned task accepted when signatures not enforced.
   - Wrong key rejection by server.

### Complete Test Run Output
```text
Ran 154 tests in 10.440s

OK
```

All 154 tests across all 10 test modules passed with 0 errors and 0 failures:
- `tests/test_dht.py`: 18 tests passing
- `tests/test_gossip.py`: 12 tests passing
- `tests/test_router.py`: 16 tests passing
- `tests/test_router_dispatch.py`: 10 tests passing
- `tests/test_router_retry.py`: 13 tests passing
- `tests/test_security.py`: 20 tests passing
- `tests/test_serializer.py`: 20 tests passing
- `tests/test_task_id_matching.py`: 18 tests passing
- `tests/test_task_registry.py`: 14 tests passing
- `tests/test_udp.py`: 13 tests passing

---

## Files Modified & Created

1. **`security.py`** *(CREATED)*:
   - Complete implementation of cryptographic task signatures (`sign_payload`, `verify_and_unpack_payload`, `compute_hmac`).
   - Complete implementation of TLS/SSL TCP streams (`create_ssl_context`, `SecureTCPServer`, `SecureTCPClient`).
2. **`udp_server.py`** *(MODIFIED)*:
   - Added `secret_key` and `enforce_signatures` parameters to `handle_message` and `run_server`.
   - Added pre-execution cryptographic verification; signed result generation; unsigned task rejection.
3. **`udp_client.py`** *(MODIFIED)*:
   - Added `secret_key` and `enforce_signatures` parameters to `AsyncUDPClient` and `run_client`.
   - Automatic task signing in `send_task()`; signature verification in `_listen_loop()`.
4. **`router.py`** *(MODIFIED)*:
   - Added `secret_key` and `enforce_signatures` parameters to `TaskRouter` and `dispatch_task()`.
   - Propagated credentials to `AsyncUDPClient` instances during remote dispatch.
5. **`node.py`** *(MODIFIED)*:
   - Added `--secret-key` and `--enforce-signatures` CLI arguments; wired to `run_server()`.
6. **`tests/test_security.py`** *(CREATED)*:
   - 20 comprehensive unit and integration tests for signing, tampering, verification, and TLS streams.
7. **`WEEK4_REPORT.md`** *(CREATED)*:
   - Technical documentation of architecture, protocol boundaries, implementation, and test results.

---

## Remaining Architectural Limitations

1. **UDP Confidentiality vs Authenticity**:
   - HMAC-SHA256 provides **authenticity, data integrity, and non-repudiation** between nodes sharing a secret key, but payload bytes remain unencrypted on the wire.
   - For complete data confidentiality, task payloads may either incorporate symmetric payload encryption (e.g. AES-GCM) or route large confidential tasks over the newly implemented `SecureTCPClient` / `SecureTCPServer` TLS channels.
2. **Key Distribution**:
   - The current implementation relies on a pre-shared secret key (`secret_key`).
   - In fully decentralized environments with uncoordinated nodes, dynamic key exchange (e.g. Diffie-Hellman / ECDH over Kademlia DHT) could establish ephemeral per-peer session keys.

---

## Recommended Next Steps

1. **Task Execution Checkpointing & Heartbeating**:
   - Implement periodic UDP worker heartbeats for long-running tasks, enabling the router to detect worker failure before the full task timeout expires.
2. **Hybrid TCP/UDP Chunking for Large Payloads**:
   - Use UDP for DHT discovery, gossip telemetry, and scheduling RPCs, with automatic handover to the `SecureTCPServer` TLS stream transport for payloads exceeding the 4KB MTU limit.

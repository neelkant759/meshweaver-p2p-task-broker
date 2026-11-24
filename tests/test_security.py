"""
MeshWeaver - Security & Cryptographic Task Signatures Tests
Week 4: Cryptographic Task Signatures & TLS/SSL Transport Security

Tests:
1. HMAC-SHA256 cryptographic signing, constant-time verification, and digest generation.
2. Tamper detection and payload corruption rejection.
3. Wrong secret key and corrupted signature rejection.
4. Header validation and truncation handling.
5. TLS/SSL secure TCP stream communication with peer certificate verification.
6. Certificate verification failure handling on untrusted/mismatched certificates.
7. Protocol boundary enforcement (TLS on TCP vs HMAC-SHA256 on UDP).
8. Full UDP client/server signed task execution round-trip with task_id preservation.
9. Prevention of unverified remote task execution when payload is tampered or unsigned.
10. Backward compatibility for unsigned task execution when signatures are not enforced.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import socket
import ssl
import sys
import unittest
from typing import Any, Tuple

from security import (
    MAC_SIZE_BYTES,
    SIGNED_RESULT_MAGIC,
    SIGNED_TASK_MAGIC,
    InvalidSignatureError,
    MissingSignatureError,
    SecureTCPClient,
    SecureTCPServer,
    compute_hmac,
    create_ssl_context,
    is_signed_payload,
    sign_payload,
    verify_and_unpack_payload,
)
from serializer import (
    deserialize_result,
    is_result_payload,
    is_task_payload,
    serialize_task,
)
from task_registry import TaskExecutionRegistry
from udp_client import AsyncUDPClient
from udp_server import handle_message, run_server


# Locate standard test certificates shipped with Python
def _get_test_cert_dir() -> pathlib.Path | None:
    try:
        import test
        p = pathlib.Path(test.__file__).parent / "certdata"
        if (p / "keycert.pem").exists():
            return p
    except Exception:
        pass
    return None


# Helper dummy tasks
def sample_multiply(x: int, y: int) -> int:
    return x * y


def sample_with_side_effect(flag_container: list) -> str:
    flag_container.append("EXECUTED")
    return "done"


class TestCryptographicSignatures(unittest.TestCase):
    """Unit tests for cryptographic HMAC-SHA256 signing and verification."""

    def setUp(self) -> None:
        self.secret_key = b"meshweaver-super-secret-key-32b"
        self.raw_task = serialize_task(sample_multiply, args=(6, 7), task_id="task_crypto_001")

    def test_compute_hmac_deterministic(self) -> None:
        """HMAC-SHA256 must produce identical 32-byte digests for identical inputs."""
        d1 = compute_hmac(self.raw_task, self.secret_key)
        d2 = compute_hmac(self.raw_task, self.secret_key)
        self.assertEqual(len(d1), MAC_SIZE_BYTES)
        self.assertEqual(d1, d2)

    def test_compute_hmac_different_keys_produce_different_digests(self) -> None:
        """Different secret keys must produce completely different digests."""
        d1 = compute_hmac(self.raw_task, b"key-alpha")
        d2 = compute_hmac(self.raw_task, b"key-beta")
        self.assertNotEqual(d1, d2)

    def test_compute_hmac_different_payloads_produce_different_digests(self) -> None:
        """Different payloads must produce completely different digests."""
        d1 = compute_hmac(self.raw_task, self.secret_key)
        d2 = compute_hmac(self.raw_task + b"\x00", self.secret_key)
        self.assertNotEqual(d1, d2)

    def test_sign_payload_structure(self) -> None:
        """Signed payload layout must be [Magic: 5B][Digest: 32B][Raw Payload: NB]."""
        signed_task = sign_payload(self.raw_task, self.secret_key, is_result=False)
        self.assertTrue(signed_task.startswith(SIGNED_TASK_MAGIC))
        self.assertEqual(len(signed_task), 5 + 32 + len(self.raw_task))
        self.assertTrue(is_signed_payload(signed_task))

        signed_result = sign_payload(b"MWRES:result", self.secret_key, is_result=True)
        self.assertTrue(signed_result.startswith(SIGNED_RESULT_MAGIC))
        self.assertEqual(len(signed_result), 5 + 32 + len(b"MWRES:result"))
        self.assertTrue(is_signed_payload(signed_result))

    def test_verify_and_unpack_payload_success(self) -> None:
        """A validly signed payload unpacks exactly to the original raw payload."""
        signed = sign_payload(self.raw_task, self.secret_key)
        unpacked = verify_and_unpack_payload(signed, self.secret_key)
        self.assertEqual(unpacked, self.raw_task)
        self.assertTrue(is_task_payload(unpacked))

    def test_verify_and_unpack_string_key_supported(self) -> None:
        """String secret keys must be automatically normalized to bytes."""
        str_key = "meshweaver-secret-string"
        signed = sign_payload(self.raw_task, str_key)
        unpacked = verify_and_unpack_payload(signed, str_key)
        self.assertEqual(unpacked, self.raw_task)

    def test_tampered_payload_rejected(self) -> None:
        """Modifying even a single byte in the payload body must fail signature verification."""
        signed = bytearray(sign_payload(self.raw_task, self.secret_key))
        # Tamper with the last byte of the payload
        signed[-1] ^= 0x01
        with self.assertRaises(InvalidSignatureError) as ctx:
            verify_and_unpack_payload(bytes(signed), self.secret_key)
        self.assertIn("failed", str(ctx.exception).lower())

    def test_tampered_digest_rejected(self) -> None:
        """Modifying a byte in the MAC digest field must fail signature verification."""
        signed = bytearray(sign_payload(self.raw_task, self.secret_key))
        # Tamper with a byte inside the 32-byte digest (index 5 to 36)
        signed[10] ^= 0x55
        with self.assertRaises(InvalidSignatureError):
            verify_and_unpack_payload(bytes(signed), self.secret_key)

    def test_wrong_secret_key_rejected(self) -> None:
        """Verification with an unauthorized or mismatched secret key must fail."""
        signed = sign_payload(self.raw_task, self.secret_key)
        with self.assertRaises(InvalidSignatureError):
            verify_and_unpack_payload(signed, b"wrong-secret-key-attacker")

    def test_missing_magic_header_rejected(self) -> None:
        """Payloads lacking the MeshWeaver signature magic header must raise ValueError."""
        with self.assertRaises(ValueError):
            verify_and_unpack_payload(b"NOT_A_SIGNED_PAYLOAD", self.secret_key)

    def test_truncated_payload_rejected(self) -> None:
        """Payloads shorter than header + MAC length must raise ValueError."""
        truncated = SIGNED_TASK_MAGIC + b"too_short"
        with self.assertRaises(ValueError):
            verify_and_unpack_payload(truncated, self.secret_key)

    def test_is_signed_payload_utility(self) -> None:
        """is_signed_payload must correctly distinguish signed datagrams from regular ones."""
        signed_tk = SIGNED_TASK_MAGIC + b"..."
        signed_rs = SIGNED_RESULT_MAGIC + b"..."
        unsigned = b"MWTK:v1:payload"
        plain = b"PING"

        self.assertTrue(is_signed_payload(signed_tk))
        self.assertTrue(is_signed_payload(signed_rs))
        self.assertFalse(is_signed_payload(unsigned))
        self.assertFalse(is_signed_payload(plain))
        self.assertFalse(is_signed_payload(b""))


class TestSecureTCPCommunication(unittest.IsolatedAsyncioTestCase):
    """Tests for TLS/SSL stream communication over TCP."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.cert_dir = _get_test_cert_dir()
        if cls.cert_dir:
            cls.keycert = str(cls.cert_dir / "keycert.pem")
            cls.pycacert = str(cls.cert_dir / "pycacert.pem")
            cls.badcert = str(cls.cert_dir / "badcert.pem")
        else:
            cls.keycert = None
            cls.pycacert = None
            cls.badcert = None

    async def test_tls_tcp_server_client_roundtrip(self) -> None:
        """Verify full TLS handshake and bidirectional secure communication over TCP."""
        if not self.keycert:
            self.skipTest("Standard test certificates not available in Python environment.")

        # Configure server SSL context with cert + key
        server_ctx = create_ssl_context(
            certfile=self.keycert,
            server_side=True,
        )

        # Configure client SSL context (trusting the test cert)
        client_ctx = create_ssl_context(
            cafile=self.keycert,
            server_side=False,
            verify_mode=ssl.CERT_REQUIRED,
            check_hostname=False,
        )

        server = SecureTCPServer(host="127.0.0.1", port=0, ssl_context=server_ctx)
        actual_port = await server.start()
        self.assertTrue(server.is_running)

        try:
            client = SecureTCPClient(host="127.0.0.1", port=actual_port, ssl_context=client_ctx)

            # Send secure message and await echo
            test_msg = b"MeshWeaver TLS Protected Payload 12345"
            reply = await client.send_and_receive(test_msg)
            self.assertEqual(reply, b"ECHO:" + test_msg)
        finally:
            await server.stop()
            self.assertFalse(server.is_running)

    async def test_tls_certificate_verification_failure(self) -> None:
        """A client requiring valid CA verification must reject an untrusted or mismatched CA."""
        if not self.keycert or not self.pycacert:
            self.skipTest("Standard test certificates not available in Python environment.")

        server_ctx = create_ssl_context(
            certfile=self.keycert,
            server_side=True,
        )

        # Client loads mismatched CA bundle that did not sign keycert
        client_ctx = create_ssl_context(
            cafile=self.pycacert,
            server_side=False,
            verify_mode=ssl.CERT_REQUIRED,
            check_hostname=False,
        )

        server = SecureTCPServer(host="127.0.0.1", port=0, ssl_context=server_ctx)
        actual_port = await server.start()

        try:
            client = SecureTCPClient(host="127.0.0.1", port=actual_port, ssl_context=client_ctx)
            # The TLS handshake must fail due to certificate verification failure
            with self.assertRaises((ssl.SSLCertVerificationError, ssl.SSLError, ConnectionResetError)):
                await client.send_and_receive(b"ping")
        finally:
            await server.stop()

    def test_protocol_boundary_tls_does_not_apply_to_udp(self) -> None:
        """
        Verify the architectural principle: Python's standard-library ssl.SSLContext
        cannot wrap a SOCK_DGRAM (UDP) socket. Attempting to do so must raise an error.
        This verifies that UDP task security MUST rely on cryptographic signatures.
        """
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ctx = ssl.create_default_context()
        try:
            # Python standard library ssl.wrap_socket / SSLContext.wrap_bio only supports streams
            with self.assertRaises((NotImplementedError, ValueError, OSError, AttributeError)):
                ctx.wrap_socket(udp_sock)
        finally:
            udp_sock.close()


class TestUDPSecurityIntegration(unittest.IsolatedAsyncioTestCase):
    """End-to-end integration tests for cryptographic task signatures over UDP."""

    async def asyncSetUp(self) -> None:
        self.secret_key = b"meshweaver-production-secret-99"
        self.registry = TaskExecutionRegistry()

    async def test_signed_task_execution_round_trip(self) -> None:
        """
        Client signs task with secret_key; server verifies signature before execution;
        server returns signed result; client verifies result signature.
        """
        # Find an open UDP port for server
        temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        temp_sock.bind(("127.0.0.1", 0))
        server_port = temp_sock.getsockname()[1]
        temp_sock.close()

        server_task = asyncio.create_task(
            run_server(
                host="127.0.0.1",
                port=server_port,
                task_registry=self.registry,
                secret_key=self.secret_key,
                enforce_signatures=True,
            )
        )
        await asyncio.sleep(0.15)

        try:
            async with AsyncUDPClient(
                server_host="127.0.0.1",
                server_port=server_port,
                timeout=3.0,
                secret_key=self.secret_key,
                enforce_signatures=True,
            ) as client:
                tid = await client.send_task(sample_multiply, args=(8, 9), task_id="task_sec_001")
                self.assertEqual(tid, "task_sec_001")

                status, result = await client.receive_task_result(task_id=tid)
                self.assertEqual(status, "ok")
                self.assertEqual(result, 72)
        finally:
            server_task.cancel()
            await asyncio.gather(server_task, return_exceptions=True)

    async def test_tampered_task_rejected_and_never_executed(self) -> None:
        """
        Tampered task payload must fail signature verification and NEVER be executed
        by the server's executor.
        """
        side_effects: list = []

        temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        temp_sock.bind(("127.0.0.1", 0))
        server_port = temp_sock.getsockname()[1]
        temp_sock.close()

        server_task = asyncio.create_task(
            run_server(
                host="127.0.0.1",
                port=server_port,
                task_registry=self.registry,
                secret_key=self.secret_key,
                enforce_signatures=True,
            )
        )
        await asyncio.sleep(0.15)

        try:
            # 1. Prepare and sign a task payload
            raw_payload = serialize_task(
                sample_with_side_effect,
                args=(side_effects,),
                task_id="task_tamper_99",
            )
            signed_payload = bytearray(sign_payload(raw_payload, self.secret_key))

            # 2. Maliciously tamper with the payload bytes
            signed_payload[-1] ^= 0xFF

            # 3. Transmit the tampered payload directly using non-blocking UDP
            loop = asyncio.get_running_loop()
            client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client_sock.setblocking(False)
            await loop.sock_sendto(client_sock, bytes(signed_payload), ("127.0.0.1", server_port))

            # Receive rejection response asynchronously
            data, _ = await asyncio.wait_for(loop.sock_recvfrom(client_sock, 4096), timeout=3.0)
            client_sock.close()

            # Verify response is signed error result
            self.assertTrue(is_signed_payload(data))
            verified_data = verify_and_unpack_payload(data, self.secret_key)
            status, val = deserialize_result(verified_data)
            self.assertEqual(status, "error")
            self.assertIn("Security Error", str(val))

            # Crucial assertion: Function side effect was NEVER triggered
            self.assertEqual(side_effects, [], "Unverified tampered task must NEVER be executed!")
        finally:
            server_task.cancel()
            await asyncio.gather(server_task, return_exceptions=True)

    async def test_unsigned_task_rejected_when_signatures_enforced(self) -> None:
        """
        When server enforces signatures, an unsigned task payload must be rejected
        without execution.
        """
        side_effects: list = []

        temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        temp_sock.bind(("127.0.0.1", 0))
        server_port = temp_sock.getsockname()[1]
        temp_sock.close()

        server_task = asyncio.create_task(
            run_server(
                host="127.0.0.1",
                port=server_port,
                task_registry=self.registry,
                secret_key=self.secret_key,
                enforce_signatures=True,
            )
        )
        await asyncio.sleep(0.15)

        try:
            # Client has NO secret_key configured
            async with AsyncUDPClient(
                server_host="127.0.0.1",
                server_port=server_port,
                timeout=2.0,
                secret_key=None,
            ) as unsigned_client:
                # Transmit raw unsigned task
                raw_payload = serialize_task(
                    sample_with_side_effect,
                    args=(side_effects,),
                    task_id="task_unsigned_01",
                )
                loop = asyncio.get_running_loop()
                await loop.sock_sendto(unsigned_client.sock, raw_payload, ("127.0.0.1", server_port))

                # Wait for rejection response
                resp, _ = await loop.sock_recvfrom(unsigned_client.sock, 4096)
                self.assertTrue(is_signed_payload(resp))
                unpacked = verify_and_unpack_payload(resp, self.secret_key)
                status, val = deserialize_result(unpacked)
                self.assertEqual(status, "error")
                self.assertIn("Cryptographic signature required", str(val))

                # Function was never executed
                self.assertEqual(side_effects, [])
        finally:
            server_task.cancel()
            await asyncio.gather(server_task, return_exceptions=True)

    async def test_unsigned_task_accepted_when_signatures_not_enforced(self) -> None:
        """
        Backward compatibility: When signatures are NOT enforced, unsigned tasks
        are accepted and executed normally.
        """
        temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        temp_sock.bind(("127.0.0.1", 0))
        server_port = temp_sock.getsockname()[1]
        temp_sock.close()

        # Server without signature enforcement
        server_task = asyncio.create_task(
            run_server(
                host="127.0.0.1",
                port=server_port,
                task_registry=self.registry,
                secret_key=None,
                enforce_signatures=False,
            )
        )
        await asyncio.sleep(0.15)

        try:
            async with AsyncUDPClient(
                server_host="127.0.0.1",
                server_port=server_port,
                timeout=3.0,
            ) as client:
                tid = await client.send_task(sample_multiply, args=(5, 5))
                status, result = await client.receive_task_result(task_id=tid)
                self.assertEqual(status, "ok")
                self.assertEqual(result, 25)
        finally:
            server_task.cancel()
            await asyncio.gather(server_task, return_exceptions=True)

    async def test_wrong_key_rejected_by_server(self) -> None:
        """A task signed with a different key than the server expects must be rejected."""
        temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        temp_sock.bind(("127.0.0.1", 0))
        server_port = temp_sock.getsockname()[1]
        temp_sock.close()

        server_task = asyncio.create_task(
            run_server(
                host="127.0.0.1",
                port=server_port,
                task_registry=self.registry,
                secret_key=b"server-secret-key-alpha",
                enforce_signatures=True,
            )
        )
        await asyncio.sleep(0.15)

        try:
            # Client signs with different key
            client_key = b"client-different-key-beta"
            raw_payload = serialize_task(sample_multiply, args=(3, 3), task_id="task_wrong_key")
            signed_payload = sign_payload(raw_payload, client_key)

            loop = asyncio.get_running_loop()
            client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client_sock.setblocking(False)
            await loop.sock_sendto(client_sock, signed_payload, ("127.0.0.1", server_port))

            data, _ = await asyncio.wait_for(loop.sock_recvfrom(client_sock, 4096), timeout=3.0)
            client_sock.close()

            # Server rejects with signed error (signed by server's key)
            self.assertTrue(is_signed_payload(data))
            unpacked = verify_and_unpack_payload(data, b"server-secret-key-alpha")
            status, val = deserialize_result(unpacked)
            self.assertEqual(status, "error")
            self.assertIn("Security Error", str(val))
        finally:
            server_task.cancel()
            await asyncio.gather(server_task, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()

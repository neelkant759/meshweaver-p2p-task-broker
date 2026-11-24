"""
MeshWeaver - Cryptographic Security & Secure Peer Communication
Week 4: Cryptographic Task Signatures & TLS/SSL Transport Security

Provides:
1. Cryptographic task signing and verification using Python standard-library HMAC-SHA256
   with constant-time verification to prevent payload tampering and unauthorized execution.
2. TLS/SSL secure stream transport (TCP) using Python's standard-library ssl module,
   supporting certificate configuration, peer certificate verification, and mutual TLS.
3. Explicit protocol boundary enforcement: TLS applies strictly to stream (TCP) connections,
   while HMAC-SHA256 signatures protect datagram (UDP) task payloads.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import ssl
from typing import Any, Callable, Optional, Tuple


# Magic headers for signed MeshWeaver datagrams
SIGNED_TASK_MAGIC: bytes = b"\x4d\x57\x53\x54\x4b"    # "MWSTK" in ASCII (5 bytes)
SIGNED_RESULT_MAGIC: bytes = b"\x4d\x57\x53\x52\x53"  # "MWSRS" in ASCII (5 bytes)
MAC_SIZE_BYTES: int = 32  # SHA-256 produces 32-byte digests


class SecurityError(Exception):
    """Base class for MeshWeaver security and verification exceptions."""
    pass


class InvalidSignatureError(SecurityError):
    """Raised when a cryptographic task or result signature fails verification."""
    pass


class MissingSignatureError(SecurityError):
    """Raised when an unsigned payload is received by a node enforcing task signatures."""
    pass


# ---------------------------------------------------------------------------
# Task 2: Cryptographic Task & Result Signatures (UDP Datagram Protection)
# ---------------------------------------------------------------------------

def _to_key_bytes(secret_key: bytes | str) -> bytes:
    """Normalize a secret key string or bytes to raw UTF-8 bytes."""
    if isinstance(secret_key, str):
        return secret_key.encode("utf-8")
    if isinstance(secret_key, (bytes, bytearray)):
        return bytes(secret_key)
    raise TypeError(f"secret_key must be str or bytes, got {type(secret_key).__name__!r}")


def compute_hmac(payload: bytes, secret_key: bytes | str) -> bytes:
    """
    Computes a cryptographic HMAC-SHA256 digest over the given payload.

    Args:
        payload: The raw bytes to authenticate.
        secret_key: The shared secret key (string or bytes).

    Returns:
        bytes: 32-byte binary HMAC-SHA256 digest.
    """
    key_bytes = _to_key_bytes(secret_key)
    return hmac.new(key_bytes, payload, hashlib.sha256).digest()


def sign_payload(
    payload: bytes,
    secret_key: bytes | str,
    is_result: bool = False,
) -> bytes:
    """
    Digitally signs a task or result payload with HMAC-SHA256.

    Wire layout:
    [Magic: 5 bytes] [Digest: 32 bytes] [Raw Payload: N bytes]

    Args:
        payload: Serialized task or result payload (e.g. starting with MWTK: or MWRS:).
        secret_key: Shared secret key for authentication.
        is_result: True if signing an execution result (MWSRS:), False for task (MWSTK:).

    Returns:
        bytes: Cryptographically signed payload ready for UDP transmission.
    """
    prefix = SIGNED_RESULT_MAGIC if is_result else SIGNED_TASK_MAGIC
    digest = compute_hmac(payload, secret_key)
    return prefix + digest + payload


def verify_and_unpack_payload(
    signed_data: bytes,
    secret_key: bytes | str,
) -> bytes:
    """
    Verifies the HMAC-SHA256 signature of a signed payload using constant-time
    comparison, returning the underlying authenticated payload bytes.

    Args:
        signed_data: The incoming signed datagram bytes.
        secret_key: The expected shared secret key.

    Returns:
        bytes: The verified, authentic payload bytes (without the signature header).

    Raises:
        InvalidSignatureError: If the signature is corrupted, tampered with, or invalid.
        ValueError: If the payload header is missing or improperly framed.
    """
    if not isinstance(signed_data, (bytes, bytearray)):
        raise ValueError(f"Expected bytes, got {type(signed_data).__name__!r}")

    is_task_sig = signed_data.startswith(SIGNED_TASK_MAGIC)
    is_result_sig = signed_data.startswith(SIGNED_RESULT_MAGIC)

    if not (is_task_sig or is_result_sig):
        raise ValueError("Payload is missing a valid MeshWeaver signature header (MWSTK or MWSRS).")

    header_len = len(SIGNED_TASK_MAGIC)
    min_len = header_len + MAC_SIZE_BYTES
    if len(signed_data) < min_len:
        raise ValueError("Signed payload is too short to contain a valid HMAC-SHA256 signature.")

    received_digest = signed_data[header_len : header_len + MAC_SIZE_BYTES]
    original_payload = signed_data[header_len + MAC_SIZE_BYTES :]

    expected_digest = compute_hmac(original_payload, secret_key)

    # Use hmac.compare_digest for constant-time comparison to prevent timing attacks
    if not hmac.compare_digest(received_digest, expected_digest):
        raise InvalidSignatureError(
            "Cryptographic signature verification failed! The payload was tampered with, "
            "corrupted, or signed with an unauthorized key."
        )

    return original_payload


def is_signed_payload(data: bytes) -> bool:
    """Returns True if the byte sequence begins with a MeshWeaver signed payload magic header."""
    return isinstance(data, (bytes, bytearray)) and (
        data.startswith(SIGNED_TASK_MAGIC) or data.startswith(SIGNED_RESULT_MAGIC)
    )


# ---------------------------------------------------------------------------
# Task 1: TLS/SSL Stream Security (TCP Transport Subsystem)
# ---------------------------------------------------------------------------

def create_ssl_context(
    certfile: Optional[str] = None,
    keyfile: Optional[str] = None,
    cafile: Optional[str] = None,
    server_side: bool = False,
    verify_mode: ssl.VerifyMode = ssl.CERT_REQUIRED,
    check_hostname: bool = False,
) -> ssl.SSLContext:
    """
    Creates and configures an SSLContext for secure TCP stream communication.

    Args:
        certfile: Path to the X.509 certificate file (PEM format).
        keyfile: Path to the private key file (PEM format).
        cafile: Path to the trusted Certificate Authority (CA) bundle.
        server_side: True for server contexts, False for client contexts.
        verify_mode: ssl.CERT_NONE, ssl.CERT_OPTIONAL, or ssl.CERT_REQUIRED.
        check_hostname: Whether client verifies peer certificate matches target hostname.

    Returns:
        ssl.SSLContext: Configured standard-library SSL context.
    """
    if server_side:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        if certfile:
            context.load_cert_chain(certfile=certfile, keyfile=keyfile)
        if cafile:
            context.load_verify_locations(cafile=cafile)
            context.verify_mode = verify_mode
    else:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = check_hostname
        context.verify_mode = verify_mode
        if cafile:
            context.load_verify_locations(cafile=cafile)
        if certfile:
            context.load_cert_chain(certfile=certfile, keyfile=keyfile)

    # Disable outdated, insecure protocols (SSLv2, SSLv3, TLSv1, TLSv1.1)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    return context


class SecureTCPServer:
    """
    Asynchronous TCP server with TLS/SSL encryption and peer certificate validation.
    Provides secure stream-based peer communication.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        ssl_context: Optional[ssl.SSLContext] = None,
        handler: Optional[Callable[[asyncio.StreamReader, asyncio.StreamWriter], Any]] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.handler = handler or self._default_handler
        self._server: Optional[asyncio.Server] = None
        self.actual_port: int = port

    @property
    def is_running(self) -> bool:
        """Returns True if the TCP server is actively listening for connections."""
        return self._server is not None and self._server.is_serving()

    async def _default_handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Default secure echo handler."""
        try:
            data = await reader.read(4096)
            if data:
                writer.write(b"ECHO:" + data)
                await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def start(self) -> int:
        """Starts the TLS TCP server and returns the bound listening port."""
        self._server = await asyncio.start_server(
            self.handler,
            host=self.host,
            port=self.port,
            ssl=self.ssl_context,
        )
        sockets = self._server.sockets
        if sockets:
            self.actual_port = sockets[0].getsockname()[1]
        return self.actual_port

    async def stop(self) -> None:
        """Closes the server and releases all socket resources."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None


class SecureTCPClient:
    """
    Asynchronous TCP client connecting to a peer over TLS/SSL with certificate verification.
    """

    def __init__(
        self,
        host: str,
        port: int,
        ssl_context: Optional[ssl.SSLContext] = None,
        timeout: float = 5.0,
    ) -> None:
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.timeout = timeout

    async def send_and_receive(self, data: bytes) -> bytes:
        """
        Connects securely via TLS, sends the data bytes, and reads the reply.

        Args:
            data: Raw payload bytes to transmit over the encrypted stream.

        Returns:
            bytes: Reply bytes received from the secure server.

        Raises:
            ssl.SSLCertVerificationError: If peer certificate verification fails.
            ssl.SSLError: On TLS handshake or encryption failures.
            TimeoutError: If the connection or response times out.
            OSError: On general socket connection failures.
        """
        async with asyncio.timeout(self.timeout):
            reader, writer = await asyncio.open_connection(
                host=self.host,
                port=self.port,
                ssl=self.ssl_context,
            )
            try:
                writer.write(data)
                await writer.drain()
                response = await reader.read(65536)
                return response
            finally:
                writer.close()
                await writer.wait_closed()

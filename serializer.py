"""
MeshWeaver - Task & Function Serializer
Module 3 of Week 1: Serialization Layer

This module handles serializing and deserializing Python functions and their
arguments using cloudpickle, so they can be transmitted as UDP byte payloads
between peers. It also serializes execution results and error responses.

Why cloudpickle?
  Standard pickle cannot serialize lambdas, closures, or functions defined
  interactively. cloudpickle extends pickle to handle these cases, making it
  ideal for a P2P task broker that ships arbitrary callables across the network.

Payload structure (Python dict, pickled to bytes):
  {
    "type": "task",
    "func": <cloudpickle-serialized function bytes>,
    "args": <cloudpickle-serialized positional args bytes>,
    "kwargs": <cloudpickle-serialized keyword args bytes>,
  }

Result structure:
  {
    "type": "result",
    "status": "ok" | "error",
    "value": <cloudpickle-serialized return value or exception string>,
  }
"""

from __future__ import annotations

import pickle
from typing import Any, Callable, Dict, Tuple

import cloudpickle

# Magic prefix added to every task payload so the server can distinguish
# a serialized task packet from a plain text PING or error message.
TASK_MAGIC_PREFIX: bytes = b"\x4d\x57\x54\x4b"   # "MWTK" in ASCII
RESULT_MAGIC_PREFIX: bytes = b"\x4d\x57\x52\x53"  # "MWRS"

# Safety cap: UDP practical safe payload size (stays well below 65507 byte hard limit).
MAX_PAYLOAD_BYTES: int = 60_000


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def serialize_task(
    func: Callable,
    args: Tuple = (),
    kwargs: Dict[str, Any] | None = None,
    task_id: Optional[str] = None,
) -> bytes:
    """
    Serialize a callable and its arguments into a transmittable byte payload.

    The function is serialized with cloudpickle (supports lambdas, closures,
    locally-defined functions). The args/kwargs are also pickled separately
    so they can be unpacked independently on the receiver side.
    An optional task_id is embedded in the envelope for response correlation.

    Args:
        func:    Any Python callable (function, lambda, closure).
        args:    Positional arguments to pass when executing the function.
        kwargs:  Keyword arguments to pass when executing the function.
        task_id: Optional unique identifier for request/response correlation.

    Returns:
        bytes: Magic-prefixed serialized task payload.

    Raises:
        TypeError:   If `func` is not callable.
        ValueError:  If the resulting payload exceeds MAX_PAYLOAD_BYTES.
        RuntimeError: On any cloudpickle or pickle serialization failure.

    Example:
        >>> def add(x, y): return x + y
        >>> payload = serialize_task(add, args=(10, 20), task_id="task_123")
    """
    if not callable(func):
        raise TypeError(f"Expected a callable, got {type(func).__name__!r}.")

    if kwargs is None:
        kwargs = {}

    try:
        task_dict: Dict[str, Any] = {
            "type": "task",
            # cloudpickle handles lambdas, closures, nested functions
            "func": cloudpickle.dumps(func),
            # Standard pickle is fine for ordinary args/kwargs
            "args": pickle.dumps(args),
            "kwargs": pickle.dumps(kwargs),
            "task_id": task_id,
        }
        raw: bytes = pickle.dumps(task_dict)
    except Exception as exc:
        raise RuntimeError(f"Failed to serialize task: {exc}") from exc

    payload: bytes = TASK_MAGIC_PREFIX + raw

    if len(payload) > MAX_PAYLOAD_BYTES:
        raise ValueError(
            f"Serialized task payload is {len(payload):,} bytes, which exceeds "
            f"the {MAX_PAYLOAD_BYTES:,}-byte UDP transmission limit. "
            "Consider reducing the function or argument size."
        )

    return payload


def deserialize_task(
    payload: bytes,
    with_task_id: bool = False,
) -> Tuple:
    """
    Reconstruct a callable and its arguments from a received byte payload.

    Args:
        payload:      Raw bytes received from the UDP socket (may include magic prefix).
        with_task_id: When True, returns a 4-tuple including task_id.
                      When False (default), returns a 3-tuple (func, args, kwargs)
                      to preserve complete backward compatibility.

    Returns:
        Tuple of (callable, positional_args tuple, keyword_args dict) if with_task_id is False,
        or Tuple of (callable, positional_args tuple, keyword_args dict, task_id) if with_task_id is True.

    Raises:
        ValueError:   If the payload is missing the magic prefix or is malformed.
        RuntimeError: On deserialization failure (corrupted bytes, incompatible Python version).

    Example:
        >>> func, args, kwargs = deserialize_task(payload)
        >>> func, args, kwargs, task_id = deserialize_task(payload, with_task_id=True)
    """
    if not isinstance(payload, (bytes, bytearray)):
        raise ValueError(f"Expected bytes payload, got {type(payload).__name__!r}.")

    # Strip magic prefix if present
    if payload.startswith(TASK_MAGIC_PREFIX):
        raw = payload[len(TASK_MAGIC_PREFIX):]
    else:
        raise ValueError(
            "Payload is missing the MeshWeaver task magic prefix. "
            "Ensure the payload was created by serialize_task()."
        )

    try:
        task_dict = pickle.loads(raw)
    except Exception as exc:
        raise RuntimeError(f"Failed to unpickle task envelope: {exc}") from exc

    # Validate structure
    if not isinstance(task_dict, dict) or task_dict.get("type") != "task":
        raise ValueError("Deserialized payload is not a valid MeshWeaver task envelope.")

    for key in ("func", "args", "kwargs"):
        if key not in task_dict:
            raise ValueError(f"Task envelope is missing required field: {key!r}.")

    try:
        func: Callable = cloudpickle.loads(task_dict["func"])
        args: Tuple = pickle.loads(task_dict["args"])
        kwargs: Dict[str, Any] = pickle.loads(task_dict["kwargs"])
    except Exception as exc:
        raise RuntimeError(
            f"Failed to deserialize task components: {exc}"
        ) from exc

    if not callable(func):
        raise ValueError("Deserialized function object is not callable.")

    task_id: Optional[str] = task_dict.get("task_id")
    if with_task_id:
        return func, args, kwargs, task_id
    return func, args, kwargs


def serialize_result(
    value: Any,
    *,
    status: str = "ok",
    task_id: Optional[str] = None,
) -> bytes:
    """
    Serialize an execution result or error string for transmission back to the caller.

    Args:
        value:   Return value from the executed function (any picklable object),
                 or an error description string when status == 'error'.
        status:  'ok' for a successful result, 'error' for a failure description.
        task_id: Unique task identifier to correlate this response with the caller's request.

    Returns:
        bytes: Magic-prefixed serialized result payload.

    Raises:
        RuntimeError: If serialization fails.

    Example:
        >>> payload = serialize_result(30, status="ok", task_id="task_123")
    """
    if status not in ("ok", "error"):
        raise ValueError(f"status must be 'ok' or 'error', got {status!r}.")

    try:
        result_dict: Dict[str, Any] = {
            "type": "result",
            "status": status,
            "value": pickle.dumps(value),
            "task_id": task_id,
        }
        raw: bytes = pickle.dumps(result_dict)
    except Exception as exc:
        raise RuntimeError(f"Failed to serialize result: {exc}") from exc

    return RESULT_MAGIC_PREFIX + raw


def deserialize_result(
    payload: bytes,
    with_task_id: bool = False,
) -> Tuple:
    """
    Reconstruct an execution result from a received byte payload.

    Args:
        payload:      Raw bytes received from the UDP socket.
        with_task_id: When True, returns a 3-tuple (status, value, task_id).
                      When False (default), returns a 2-tuple (status, value)
                      for complete backward compatibility.

    Returns:
        Tuple of (status: str, value: Any) if with_task_id is False,
        or Tuple of (status: str, value: Any, task_id: Optional[str]) if with_task_id is True.

    Raises:
        ValueError:   If the payload is not a valid MeshWeaver result envelope.
        RuntimeError: On deserialization failure.

    Example:
        >>> status, result = deserialize_result(payload)
        >>> status, result, task_id = deserialize_result(payload, with_task_id=True)
    """
    if not isinstance(payload, (bytes, bytearray)):
        raise ValueError(f"Expected bytes payload, got {type(payload).__name__!r}.")

    if payload.startswith(RESULT_MAGIC_PREFIX):
        raw = payload[len(RESULT_MAGIC_PREFIX):]
    else:
        raise ValueError(
            "Payload is missing the MeshWeaver result magic prefix. "
            "Ensure the payload was created by serialize_result()."
        )

    try:
        result_dict = pickle.loads(raw)
    except Exception as exc:
        raise RuntimeError(f"Failed to unpickle result envelope: {exc}") from exc

    if not isinstance(result_dict, dict) or result_dict.get("type") != "result":
        raise ValueError("Deserialized payload is not a valid MeshWeaver result envelope.")

    for key in ("status", "value"):
        if key not in result_dict:
            raise ValueError(f"Result envelope is missing required field: {key!r}.")

    try:
        status: str = result_dict["status"]
        value: Any = pickle.loads(result_dict["value"])
    except Exception as exc:
        raise RuntimeError(f"Failed to deserialize result value: {exc}") from exc

    task_id: Optional[str] = result_dict.get("task_id")
    if with_task_id:
        return status, value, task_id
    return status, value


def is_task_payload(data: bytes) -> bool:
    """
    Returns True if `data` starts with the MeshWeaver task magic prefix.

    Use this in the server's message handler to distinguish task packets
    from plain-text PING health checks.

    Args:
        data: Raw bytes received from the UDP socket.

    Returns:
        bool: True if this looks like a serialized task payload.
    """
    return isinstance(data, (bytes, bytearray)) and data.startswith(TASK_MAGIC_PREFIX)


def is_result_payload(data: bytes) -> bool:
    """
    Returns True if `data` starts with the MeshWeaver result magic prefix.

    Args:
        data: Raw bytes received from the UDP socket.

    Returns:
        bool: True if this looks like a serialized result payload.
    """
    return isinstance(data, (bytes, bytearray)) and data.startswith(RESULT_MAGIC_PREFIX)

"""
MeshWeaver - Remote Function Executor
Module 4 of Week 1: Execution Layer

This module provides a safe, structured pipeline for executing deserialized
Python functions received from trusted local peers over UDP.

Workflow (server side):
  1. Receive raw bytes from UDP socket.
  2. Detect TASK magic prefix with serializer.is_task_payload().
  3. Call executor.execute_task(payload) which:
       a. Deserializes the function and arguments via serializer.deserialize_task().
       b. Invokes the function inside a try/except.
       c. Serializes and returns the result or error via serializer.serialize_result().
  4. Transmit the returned result bytes back to the sender.

Security note:
  This executor is intended for development and local peer testing only.
  It executes arbitrary deserialized callables without sandboxing.
  Production deployments must add an allow-list / signature verification layer.
"""

from __future__ import annotations

import traceback
from typing import Any, Callable, Dict, Tuple

from serializer import (
    deserialize_task,
    is_task_payload,
    serialize_result,
)


def execute_task(payload: bytes) -> bytes:
    """
    Deserialize, execute, and return the result of a MeshWeaver task payload.

    This is the main entry point called by the server when it detects an
    incoming task packet (identified by the TASK_MAGIC_PREFIX).
    Preserves task_id throughout execution and embeds it in the result payload.

    Args:
        payload: Raw bytes from the UDP socket containing a serialized task.

    Returns:
        bytes: A serialized result payload (success or error), ready to be
               transmitted back to the client via UDP.

    Behaviour:
        - On success:  returns serialize_result(return_value, status="ok", task_id=task_id).
        - On deserialization error: returns serialize_result(error_msg, status="error").
        - On execution exception:   returns serialize_result(error_msg, status="error", task_id=task_id).

    Example (server handler):
        >>> if is_task_payload(data):
        ...     result_bytes = execute_task(data)
        ...     await loop.sock_sendto(sock, result_bytes, addr)
    """
    # Step 1: Deserialize task payload including task_id
    try:
        func, args, kwargs, task_id = deserialize_task(payload, with_task_id=True)
    except (ValueError, RuntimeError) as exc:
        error_msg = f"[Executor] Task deserialization failed: {exc}"
        print(error_msg)
        return serialize_result(error_msg, status="error")

    # Step 2: Log what we are about to execute
    func_name = getattr(func, "__name__", repr(func))
    task_label = f" [{task_id}]" if task_id else ""
    print(f"[Executor] Running task{task_label}: {func_name}(args={args}, kwargs={kwargs})")

    # Step 3: Execute the function, capturing any exception
    try:
        result: Any = func(*args, **kwargs)
        print(f"[Executor] Task completed. Result: {result!r}")
        return serialize_result(result, status="ok", task_id=task_id)
    except Exception as exc:
        error_msg = (
            f"[Executor] Task execution raised an exception in '{func_name}': "
            f"{type(exc).__name__}: {exc}\n"
            f"{traceback.format_exc()}"
        )
        print(error_msg)
        return serialize_result(error_msg, status="error", task_id=task_id)


def run_task_locally(
    func: Callable,
    args: Tuple = (),
    kwargs: Dict[str, Any] | None = None,
) -> Tuple[str, Any]:
    """
    Convenience helper: execute a function locally without network transmission.

    Useful for unit testing the executor without spinning up a UDP server.

    Args:
        func:   Any Python callable.
        args:   Positional arguments.
        kwargs: Keyword arguments.

    Returns:
        Tuple of (status: str, value: Any).
        status is 'ok' on success, 'error' on exception.
        value is the return value or an error description string.

    Example:
        >>> status, result = run_task_locally(lambda x, y: x + y, args=(10, 20))
        >>> assert status == "ok"
        >>> assert result == 30
    """
    if kwargs is None:
        kwargs = {}

    func_name = getattr(func, "__name__", repr(func))
    try:
        result: Any = func(*args, **kwargs)
        return "ok", result
    except Exception as exc:
        error_msg = (
            f"[Executor] Exception in '{func_name}': "
            f"{type(exc).__name__}: {exc}"
        )
        return "error", error_msg

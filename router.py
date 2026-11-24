"""
MeshWeaver - Task Router & Intelligent Peer Selection
Week 3: Decentralized Task Scheduling Layer (Part 1: Peer Selection)

This module implements the intelligent peer selection logic for decentralized task
distribution across the MeshWeaver P2P network:
1. Multi-metric evaluation (Lowest CPU -> Highest Free RAM -> Deterministic NodeID).
2. Stale peer pruning with configurable staleness threshold.
3. Excluded peer filtering.
4. Error handling with NoPeerAvailableError.
5. Configurable local execution fallback reusing existing executor.py functionality.
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import uuid

from dht import Peer
from executor import run_task_locally
from gossip import GossipEngine, NodeResourceMetrics, PeerTelemetry
from udp_client import AsyncUDPClient


class NoPeerAvailableError(Exception):
    """Raised when no active, non-stale peers are eligible for task scheduling."""
    pass


@dataclass
class TaskExecutionResponse:
    """
    Structured response object returned upon task execution.

    Attributes:
        task_id: Unique identifier for the submitted task.
        status: Execution outcome status ('ok' or 'error').
        result: The function return value on success, or error string / traceback.
        executed_by: Endpoint of the node that executed the task ("ip:port" or "local").
        target_node_id: 160-bit hex identifier of the executing peer (or None if local).
        round_trip_ms: Total latency from task dispatch to result receipt in milliseconds.
        attempts: Total number of execution attempts made (default: 1).
        retries: Number of retries performed after the initial attempt (default: 0).
    """
    task_id: str
    status: str
    result: Any
    executed_by: str
    target_node_id: Optional[str] = None
    round_trip_ms: float = 0.0
    attempts: int = 1
    retries: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert the response to a standard Python dictionary."""
        return asdict(self)


DEFAULT_STALE_THRESHOLD: float = 30.0


class TaskRouter:
    """
    Decentralized Task Router.
    Evaluates peer hardware telemetry collected by GossipEngine and selects the
    optimal node for task execution.
    """

    def __init__(
        self,
        gossip_engine: GossipEngine,
        stale_threshold: float = DEFAULT_STALE_THRESHOLD,
        fallback_to_local: bool = False,
        max_retries: int = 0,
        retry_delay: float = 0.0,
        retry_on_remote_error: bool = False,
        secret_key: Optional[bytes | str] = None,
        enforce_signatures: bool = False,
    ) -> None:
        """
        Initialize the TaskRouter.

        Args:
            gossip_engine: Active GossipEngine holding peer telemetry.
            stale_threshold: Max age in seconds before peer telemetry is considered stale.
            fallback_to_local: Whether to fallback to local execution when no remote peer is available.
            max_retries: Maximum number of reassignment retries on worker failure (default: 0).
            retry_delay: Delay in seconds before re-attempting task dispatch (default: 0.0).
            retry_on_remote_error: Whether to reassign task if worker returns an execution error (default: False).
            secret_key: Optional HMAC-SHA256 secret key for signing dispatched tasks.
            enforce_signatures: Whether to enforce cryptographic signatures on responses.
        """
        self.gossip_engine: GossipEngine = gossip_engine
        self.stale_threshold: float = stale_threshold
        self.fallback_to_local: bool = fallback_to_local
        self.max_retries: int = max_retries
        self.retry_delay: float = retry_delay
        self.retry_on_remote_error: bool = retry_on_remote_error
        self.secret_key: Optional[bytes | str] = secret_key
        self.enforce_signatures: bool = enforce_signatures

    def select_target_peer(
        self,
        exclude_nodes: Optional[Set[str] | List[str]] = None,
        now: Optional[float] = None,
    ) -> Tuple[Peer, NodeResourceMetrics]:
        """
        Select the best peer for task execution based on multi-criteria ranking:
        1. Lowest CPU utilization (cpu_percent ascending).
        2. Highest available RAM (ram_free_mb descending).
        3. Deterministic NodeID ordering (alphabetical ascending) to guarantee consistent tie-breaks.

        Stale telemetry records (age > stale_threshold) and excluded nodes are filtered out.

        Args:
            exclude_nodes: Set or list of node_ids to exclude from selection.
            now: Optional reference timestamp (defaults to time.time()).

        Returns:
            Tuple of (Peer, NodeResourceMetrics) for the selected peer.

        Raises:
            NoPeerAvailableError: If no eligible non-stale peers are available.
        """
        current_time = now if now is not None else time.time()
        cutoff_time = current_time - self.stale_threshold

        excluded_set: Set[str] = {
            str(n).strip().lower() for n in (exclude_nodes or set())
        }

        # Filter active and non-stale peers
        candidates: List[Tuple[str, Peer, NodeResourceMetrics]] = []
        for node_id, rec in self.gossip_engine.peer_telemetry.items():
            normalized_id = str(node_id).strip().lower()
            if normalized_id in excluded_set:
                continue
            if rec.last_updated < cutoff_time:
                continue
            candidates.append((normalized_id, rec.peer, rec.metrics))

        if not candidates:
            raise NoPeerAvailableError(
                f"No eligible peer available. Telemetry contains {len(self.gossip_engine.peer_telemetry)} total records, "
                f"all excluded or older than {self.stale_threshold}s threshold."
            )

        # Multi-criteria sorting:
        # 1. cpu_percent: ascending (lower is better)
        # 2. ram_free_mb: descending (higher free RAM is better, so -ram_free_mb)
        # 3. node_id: ascending (deterministic alphabetical tie-break)
        candidates.sort(
            key=lambda item: (
                round(item[2].cpu_percent, 4),
                -round(item[2].ram_free_mb, 4),
                item[0],
            )
        )

        best_id, best_peer, best_metrics = candidates[0]
        return best_peer, best_metrics

    async def async_select_target_peer(
        self,
        exclude_nodes: Optional[Set[str] | List[str]] = None,
        now: Optional[float] = None,
    ) -> Tuple[Peer, NodeResourceMetrics]:
        """Asynchronous wrapper for select_target_peer."""
        return self.select_target_peer(exclude_nodes=exclude_nodes, now=now)

    def schedule_task(
        self,
        func: Callable,
        args: Tuple = (),
        kwargs: Dict[str, Any] | None = None,
        exclude_nodes: Optional[Set[str] | List[str]] = None,
        now: Optional[float] = None,
    ) -> Tuple[str, Any, str]:
        """
        Resolve the target peer for task execution, applying local fallback if configured.

        Args:
            func: The callable to schedule.
            args: Positional arguments for the callable.
            kwargs: Keyword arguments for the callable.
            exclude_nodes: Peer IDs to exclude.
            now: Optional reference timestamp.

        Returns:
            Tuple of (status: str, value: Any, executed_by: str).
            - When a peer is selected: ('peer_selected', (peer, metrics), peer.node_id)
            - When local fallback executes: ('ok' | 'error', result_value, 'local')

        Raises:
            NoPeerAvailableError: If no peer is available and fallback_to_local is False.
        """
        try:
            peer, metrics = self.select_target_peer(exclude_nodes=exclude_nodes, now=now)
            return "peer_selected", (peer, metrics), peer.node_id
        except NoPeerAvailableError:
            if self.fallback_to_local:
                # Reuse existing executor functionality without duplication
                status, val = run_task_locally(func, args=args, kwargs=kwargs)
                return status, val, "local"
            raise

    async def async_schedule_task(
        self,
        func: Callable,
        args: Tuple = (),
        kwargs: Dict[str, Any] | None = None,
        exclude_nodes: Optional[Set[str] | List[str]] = None,
        now: Optional[float] = None,
    ) -> Tuple[str, Any, str]:
        """Asynchronous wrapper for schedule_task."""
        return self.schedule_task(
            func, args=args, kwargs=kwargs, exclude_nodes=exclude_nodes, now=now
        )

    async def dispatch_task(
        self,
        func: Callable,
        args: Tuple = (),
        kwargs: Dict[str, Any] | None = None,
        task_id: Optional[str] = None,
        timeout: float = 5.0,
        exclude_nodes: Optional[Set[str] | List[str]] = None,
        now: Optional[float] = None,
        max_retries: Optional[int] = None,
        retry_delay: Optional[float] = None,
        retry_on_remote_error: Optional[bool] = None,
        secret_key: Optional[bytes | str] = None,
        enforce_signatures: Optional[bool] = None,
    ) -> TaskExecutionResponse:
        """
        Select an optimal peer via Gossip telemetry and dispatch the task remotely over UDP,
        with automatic reassignment and retry when a worker peer fails or times out.

        Workflow:
        1. Generates a unique task_id if one is not provided, preserving it across retries.
        2. Measures round_trip_ms using high-precision monotonic timing.
        3. Selects the target peer using multi-criteria ranking (lowest CPU, highest RAM).
           If no peer is available and fallback_to_local is True, executes locally.
        4. Serializes and transmits the task payload over UDP via AsyncUDPClient.
        5. Receives, deserializes, and formats the execution outcome into a TaskExecutionResponse.
        6. On worker failure or timeout, automatically excludes the failed peer and reassigns
           the task to the next eligible healthy peer up to max_retries.

        Args:
            func: The Python callable to execute remotely.
            args: Positional arguments for the callable.
            kwargs: Keyword arguments for the callable.
            task_id: Optional unique identifier; auto-generated if omitted (preserved across retries).
            timeout: Maximum wait time in seconds for the remote node to return a result.
            exclude_nodes: Node IDs to exclude from peer selection.
            now: Optional reference timestamp for staleness filtering.
            max_retries: Maximum number of retries upon worker failure (defaults to self.max_retries).
            retry_delay: Optional delay in seconds before retrying (defaults to self.retry_delay).
            retry_on_remote_error: Whether to retry when worker returns an execution error (defaults to self.retry_on_remote_error).

        Returns:
            TaskExecutionResponse: Complete execution record including task_id, status,
                                   result, executed_by, target_node_id, round_trip_ms,
                                   attempts, and retries.

        Raises:
            NoPeerAvailableError: If no eligible remote peers exist and fallback_to_local is False.
            TimeoutError: If all selected remote peers fail to return a result within the timeout.
            RuntimeError / OSError: On network or communication failures.
        """
        if task_id is None:
            task_id = f"task_{uuid.uuid4().hex[:12]}"

        if kwargs is None:
            kwargs = {}

        start_time = time.perf_counter()
        effective_max_retries = max_retries if max_retries is not None else self.max_retries
        effective_retry_delay = retry_delay if retry_delay is not None else self.retry_delay
        effective_retry_on_error = (
            retry_on_remote_error if retry_on_remote_error is not None else self.retry_on_remote_error
        )
        effective_secret_key = secret_key if secret_key is not None else self.secret_key
        effective_enforce_sig = (
            enforce_signatures if enforce_signatures is not None else self.enforce_signatures
        )

        excluded: Set[str] = {
            str(n).strip().lower() for n in (exclude_nodes or set())
        }

        attempts = 0
        last_exception: Optional[Exception] = None

        while True:
            # Step 1: Select peer excluding previously failed or user-excluded nodes
            try:
                peer, metrics = self.select_target_peer(exclude_nodes=excluded, now=now)
            except NoPeerAvailableError as no_peer_err:
                if self.fallback_to_local:
                    status, val = run_task_locally(func, args=args, kwargs=kwargs)
                    duration_ms = (time.perf_counter() - start_time) * 1000.0
                    return TaskExecutionResponse(
                        task_id=task_id,
                        status=status,
                        result=val,
                        executed_by="local",
                        target_node_id=None,
                        round_trip_ms=round(duration_ms, 2),
                        attempts=attempts + 1,
                        retries=attempts,
                    )
                if last_exception is not None:
                    raise NoPeerAvailableError(
                        f"Task [{task_id}] failed after {attempts} attempt(s); no alternative peer available. "
                        f"Last error: {last_exception}"
                    ) from last_exception
                raise no_peer_err

            # Step 2: Remote dispatch via AsyncUDPClient with task_id preservation
            try:
                async with AsyncUDPClient(
                    server_host=peer.ip,
                    server_port=peer.port,
                    timeout=timeout,
                    secret_key=effective_secret_key,
                    enforce_signatures=effective_enforce_sig,
                ) as client:
                    actual_task_id = await client.send_task(
                        func, args=args, kwargs=kwargs, task_id=task_id
                    )
                    status, val = await client.receive_task_result(
                        task_id=actual_task_id, timeout=timeout
                    )

                if status == "error" and effective_retry_on_error:
                    raise RuntimeError(f"Remote execution returned error status: {val}")

                duration_ms = (time.perf_counter() - start_time) * 1000.0
                return TaskExecutionResponse(
                    task_id=task_id,
                    status=status,
                    result=val,
                    executed_by=f"{peer.ip}:{peer.port}",
                    target_node_id=peer.node_id,
                    round_trip_ms=round(duration_ms, 2),
                    attempts=attempts + 1,
                    retries=attempts,
                )

            except (TimeoutError, ConnectionResetError, OSError, RuntimeError) as exc:
                last_exception = exc
                attempts += 1
                # Mark failed peer as excluded to reassign to a DIFFERENT healthy peer
                excluded.add(peer.node_id.strip().lower())

                # Check if retry limit reached
                if attempts > effective_max_retries:
                    if self.fallback_to_local:
                        status, val = run_task_locally(func, args=args, kwargs=kwargs)
                        duration_ms = (time.perf_counter() - start_time) * 1000.0
                        return TaskExecutionResponse(
                            task_id=task_id,
                            status=status,
                            result=val,
                            executed_by="local",
                            target_node_id=None,
                            round_trip_ms=round(duration_ms, 2),
                            attempts=attempts,
                            retries=attempts - 1,
                        )
                    raise exc

                if effective_retry_delay > 0:
                    await asyncio.sleep(effective_retry_delay)

    def dispatch_task_sync(
        self,
        func: Callable,
        args: Tuple = (),
        kwargs: Dict[str, Any] | None = None,
        task_id: Optional[str] = None,
        timeout: float = 5.0,
        exclude_nodes: Optional[Set[str] | List[str]] = None,
        now: Optional[float] = None,
        max_retries: Optional[int] = None,
        retry_delay: Optional[float] = None,
        retry_on_remote_error: Optional[bool] = None,
        secret_key: Optional[bytes | str] = None,
        enforce_signatures: Optional[bool] = None,
    ) -> TaskExecutionResponse:
        """Synchronous wrapper for dispatch_task."""
        return asyncio.run(
            self.dispatch_task(
                func,
                args=args,
                kwargs=kwargs,
                task_id=task_id,
                timeout=timeout,
                exclude_nodes=exclude_nodes,
                now=now,
                max_retries=max_retries,
                retry_delay=retry_delay,
                retry_on_remote_error=retry_on_remote_error,
            )
        )

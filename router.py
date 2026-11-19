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

import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from dht import Peer
from executor import run_task_locally
from gossip import GossipEngine, NodeResourceMetrics, PeerTelemetry


class NoPeerAvailableError(Exception):
    """Raised when no active, non-stale peers are eligible for task scheduling."""
    pass


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
    ) -> None:
        """
        Initialize the TaskRouter.

        Args:
            gossip_engine: Active GossipEngine holding peer telemetry.
            stale_threshold: Max age in seconds before peer telemetry is considered stale.
            fallback_to_local: Whether to fallback to local execution when no remote peer is available.
        """
        self.gossip_engine: GossipEngine = gossip_engine
        self.stale_threshold: float = stale_threshold
        self.fallback_to_local: bool = fallback_to_local

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

"""
MeshWeaver - Task Execution Registry & Duplicate Protection
Week 3: Execution State Tracking, Duplicate Task Detection & Result Caching

Maintains server-side execution lifecycle states (PENDING, RUNNING, COMPLETED, FAILED),
detects duplicate task submissions, prevents concurrent duplicate execution, caches
completed/failed task results, and provides TTL-based expiration and cleanup.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
import threading
import time
from typing import Dict, List, Optional, Tuple


class TaskState(str, Enum):
    """Execution lifecycle states of a task on a worker node."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass
class TaskRecord:
    """
    Represents the execution state, outcome, and pending waiters for a task.

    Attributes:
        task_id: Unique identifier for the task.
        state: Current lifecycle state (PENDING, RUNNING, COMPLETED, FAILED).
        result_bytes: Cached serialized result payload ready for re-transmission.
        created_at: Monotonic timestamp when the task was first registered.
        completed_at: Monotonic timestamp when execution finished.
        waiters: List of asyncio futures for concurrent requests awaiting completion.
    """
    task_id: str
    state: TaskState
    result_bytes: Optional[bytes] = None
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    waiters: List[asyncio.Future[bytes]] = field(default_factory=list)


DEFAULT_TASK_TTL: float = 300.0  # 5 minutes default retention for cached results


class TaskExecutionRegistry:
    """
    Thread-safe and asyncio-compatible task execution registry.

    Key capabilities:
    1. Tracks task states: PENDING, RUNNING, COMPLETED, FAILED.
    2. Detects duplicate Task IDs and prevents double-execution.
    3. Concurrency multiplexing: if a duplicate request arrives while the original
       is still RUNNING, the duplicate waits on an attached future and receives
       the computed result upon completion without invoking the function again.
    4. Caches COMPLETED and FAILED results and immediately replays them for late duplicates.
    5. Configurable TTL cleanup to prune expired task records safely.
    """

    def __init__(self, default_ttl: float = DEFAULT_TASK_TTL) -> None:
        self._lock = threading.Lock()
        self._records: Dict[str, TaskRecord] = {}
        self.default_ttl: float = default_ttl

    def register_or_wait(
        self,
        task_id: str,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> Tuple[TaskState, Optional[bytes], Optional[asyncio.Future[bytes]]]:
        """
        Registers an incoming task or correlates it with an existing execution record.

        Args:
            task_id: Unique task identifier.
            loop: Optional asyncio event loop used to create waiter futures.

        Returns:
            Tuple[TaskState, Optional[bytes], Optional[asyncio.Future[bytes]]]:
            - (TaskState.RUNNING, None, None): Task is newly registered; caller owns execution.
            - (TaskState.RUNNING, None, waiter_fut): Task is already RUNNING; caller should await waiter_fut.
            - (TaskState.COMPLETED, cached_bytes, None): Task already COMPLETED; cached result returned.
            - (TaskState.FAILED, cached_bytes, None): Task already FAILED; cached error returned.
        """
        with self._lock:
            record = self._records.get(task_id)

            if record is None:
                # Brand new task registration: mark RUNNING and designate caller as executor
                new_record = TaskRecord(
                    task_id=task_id,
                    state=TaskState.RUNNING,
                    created_at=time.time(),
                )
                self._records[task_id] = new_record
                return TaskState.RUNNING, None, None

            if record.state in (TaskState.COMPLETED, TaskState.FAILED):
                # Return cached result / error bytes immediately
                return record.state, record.result_bytes, None

            # Task is already PENDING or RUNNING: attach a waiter future
            if loop is None:
                loop = asyncio.get_running_loop()

            waiter_fut: asyncio.Future[bytes] = loop.create_future()
            record.waiters.append(waiter_fut)
            return TaskState.RUNNING, None, waiter_fut

    def complete_task(self, task_id: str, result_bytes: bytes) -> None:
        """
        Marks a task as COMPLETED, stores its serialized result, and resolves
        any concurrent duplicate requests awaiting its completion.

        Args:
            task_id: Unique task identifier.
            result_bytes: Serialized result payload (MWRS:).
        """
        waiters_to_notify: List[asyncio.Future[bytes]] = []
        with self._lock:
            record = self._records.get(task_id)
            if record is None:
                record = TaskRecord(task_id=task_id, state=TaskState.COMPLETED)
                self._records[task_id] = record

            record.state = TaskState.COMPLETED
            record.result_bytes = result_bytes
            record.completed_at = time.time()
            waiters_to_notify = list(record.waiters)
            record.waiters.clear()

        # Resolve all attached waiters outside the lock
        for fut in waiters_to_notify:
            if not fut.done():
                fut.set_result(result_bytes)

    def fail_task(self, task_id: str, error_bytes: bytes) -> None:
        """
        Marks a task as FAILED, stores its error payload, and resolves any
        concurrent duplicate requests awaiting its completion with the error payload.

        Args:
            task_id: Unique task identifier.
            error_bytes: Serialized error payload (MWRS:).
        """
        waiters_to_notify: List[asyncio.Future[bytes]] = []
        with self._lock:
            record = self._records.get(task_id)
            if record is None:
                record = TaskRecord(task_id=task_id, state=TaskState.FAILED)
                self._records[task_id] = record

            record.state = TaskState.FAILED
            record.result_bytes = error_bytes
            record.completed_at = time.time()
            waiters_to_notify = list(record.waiters)
            record.waiters.clear()

        # Resolve all attached waiters outside the lock
        for fut in waiters_to_notify:
            if not fut.done():
                fut.set_result(error_bytes)

    def abort_task(self, task_id: str, exc: Exception) -> None:
        """
        Aborts an in-flight task upon unexpected server exception or worker cancellation,
        notifying waiters with an exception and clearing the record to allow future retries.

        Args:
            task_id: Unique task identifier.
            exc: Exception to propagate to waiters.
        """
        waiters_to_notify: List[asyncio.Future[bytes]] = []
        with self._lock:
            record = self._records.pop(task_id, None)
            if record is not None:
                waiters_to_notify = list(record.waiters)

        for fut in waiters_to_notify:
            if not fut.done():
                fut.set_exception(exc)

    def get_record(self, task_id: str) -> Optional[TaskRecord]:
        """Retrieve the current record for task_id, if present."""
        with self._lock:
            return self._records.get(task_id)

    def total_records(self) -> int:
        """Return the total number of records currently tracked."""
        with self._lock:
            return len(self._records)

    def cleanup_expired(
        self,
        ttl_seconds: Optional[float] = None,
        now: Optional[float] = None,
    ) -> int:
        """
        Prunes completed or failed task records older than ttl_seconds.
        Running tasks are never pruned.

        Args:
            ttl_seconds: Max retention time in seconds. Defaults to self.default_ttl.
            now: Optional reference timestamp. Defaults to time.time().

        Returns:
            int: Number of expired records pruned.
        """
        effective_ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl
        if now is None:
            now = time.time()

        pruned_count = 0
        with self._lock:
            expired_keys = [
                tid
                for tid, rec in self._records.items()
                if rec.state in (TaskState.COMPLETED, TaskState.FAILED)
                and rec.completed_at is not None
                and (now - rec.completed_at) >= effective_ttl
            ]
            for tid in expired_keys:
                del self._records[tid]
                pruned_count += 1

        return pruned_count

    def clear(self) -> None:
        """Clears all records and pending waiters safely."""
        with self._lock:
            for rec in self._records.values():
                for fut in rec.waiters:
                    if not fut.done():
                        fut.cancel()
            self._records.clear()

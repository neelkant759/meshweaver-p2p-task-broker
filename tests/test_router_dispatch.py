"""
MeshWeaver - Task Router Remote Dispatch Integration Tests
Week 3 Part 2: Remote Task Dispatch Layer

Validates end-to-end remote task dispatching from TaskRouter to an actual
independent remote worker process via UDP networking:
1. Successful remote task dispatch.
2. Remote addition function execution.
3. Function execution with multiple *args and **kwargs.
4. Complex function execution (closure capturing state, recursion, statistical processing).
5. Execution in an independent remote Python process (worker PID != client PID).
6. Structured TaskExecutionResponse fields validation.
7. Remote execution error handling and exception traceback propagation.
8. Communication timeout handling.
9. Local fallback execution when no peer is available.
"""

from __future__ import annotations

import asyncio
import math
import os
import socket
import subprocess
import sys
import time
import unittest
from typing import Any, Dict

# Ensure project root is available in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dht import DHTNode, NodeID, Peer
from gossip import GossipEngine, NodeResourceMetrics, PeerTelemetry
from router import NoPeerAvailableError, TaskExecutionResponse, TaskRouter


# Sample functions for remote execution testing
def remote_add(a: int, b: int) -> int:
    return a + b


def remote_compute_volume(length: float, width: float, height: float, unit: str = "cm^3") -> str:
    vol = length * width * height
    return f"{vol:.1f} {unit}"


def remote_get_execution_info() -> Dict[str, Any]:
    return {
        "pid": os.getpid(),
        "platform": sys.platform,
        "cwd": os.getcwd(),
    }


def remote_failing_function(message: str) -> None:
    raise ValueError(f"Intentional remote failure: {message}")


def build_complex_closure(multiplier: float):
    memo = {}

    def worker(data_list, depth=10):
        def fib(n):
            if n in memo:
                return memo[n]
            if n <= 1:
                return n
            res = fib(n - 1) + fib(n - 2)
            memo[n] = res
            return res

        f_val = fib(depth)
        evens = [x * multiplier for x in data_list if x % 2 == 0]
        return {
            "worker_pid": os.getpid(),
            "fib_factor": f_val,
            "count": len(evens),
            "sum": sum(evens),
            "mean": sum(evens) / max(1, len(evens)),
        }

    return worker


class TestRouterRemoteDispatchIntegration(unittest.IsolatedAsyncioTestCase):
    """
    Integration tests verifying TaskRouter remote dispatch against an
    actual independent Python worker process running node.py.
    """

    @classmethod
    def setUpClass(cls):
        """Allocate an ephemeral port and spawn an independent worker process."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(("127.0.0.1", 0))
            cls.worker_port = s.getsockname()[1]

        cls.worker_node_id = "worker_node_alpha_12345"

        # Set up PYTHONPATH for the worker process so it can deserialize any test-defined callables
        env = os.environ.copy()
        python_paths = [PROJECT_ROOT, os.path.join(PROJECT_ROOT, "tests")]
        if "PYTHONPATH" in env:
            python_paths.append(env["PYTHONPATH"])
        env["PYTHONPATH"] = os.pathsep.join(python_paths)

        # Spawn node.py as an actual independent Python process
        cls.worker_proc = subprocess.Popen(
            [
                sys.executable,
                "-u",
                os.path.join(PROJECT_ROOT, "node.py"),
                "--host",
                "127.0.0.1",
                "--port",
                str(cls.worker_port),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        # Give the independent process time to bind its UDP socket
        time.sleep(0.6)

    @classmethod
    def tearDownClass(cls):
        """Clean up the independent worker process."""
        if cls.worker_proc and cls.worker_proc.poll() is None:
            cls.worker_proc.terminate()
            try:
                cls.worker_proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                cls.worker_proc.kill()

    def setUp(self):
        """Set up a TaskRouter populated with the worker's telemetry."""
        self.dht_local = DHTNode(
            node_id=NodeID.from_string("router_coordinator"),
            host="127.0.0.1",
            port=0,
        )
        self.gossip_local = GossipEngine(dht_node=self.dht_local)

        # Inject worker node telemetry into gossip engine
        self.worker_peer = Peer(
            ip="127.0.0.1",
            port=self.worker_port,
            node_id=self.worker_node_id,
        )
        self.worker_metrics = NodeResourceMetrics(
            cpu_percent=15.0,
            ram_total_mb=8192.0,
            ram_used_mb=4096.0,
            ram_free_mb=4096.0,
            ram_percent=50.0,
            timestamp=time.time(),
        )
        self.gossip_local.peer_telemetry[self.worker_node_id] = PeerTelemetry(
            peer=self.worker_peer,
            metrics=self.worker_metrics,
            last_updated=time.time(),
        )

        self.router = TaskRouter(
            gossip_engine=self.gossip_local,
            stale_threshold=30.0,
            fallback_to_local=False,
        )

    async def test_remote_dispatch_addition(self):
        """Verifies remote dispatch of a standard addition function."""
        resp = await self.router.dispatch_task(remote_add, args=(25, 17))
        self.assertIsInstance(resp, TaskExecutionResponse)
        self.assertEqual(resp.status, "ok")
        self.assertEqual(resp.result, 42)
        self.assertEqual(resp.executed_by, f"127.0.0.1:{self.worker_port}")
        self.assertEqual(resp.target_node_id, self.worker_node_id)
        self.assertGreater(resp.round_trip_ms, 0.0)

    async def test_remote_dispatch_multiple_arguments(self):
        """Verifies remote dispatch of function with multiple *args and **kwargs."""
        resp = await self.router.dispatch_task(
            remote_compute_volume,
            args=(10.0, 5.0, 2.0),
            kwargs={"unit": "m^3"},
        )
        self.assertEqual(resp.status, "ok")
        self.assertEqual(resp.result, "100.0 m^3")
        self.assertEqual(resp.executed_by, f"127.0.0.1:{self.worker_port}")

    async def test_remote_dispatch_complex_function(self):
        """Verifies remote dispatch of complex closure with captured state and recursion."""
        closure = build_complex_closure(multiplier=2.5)
        test_data = list(range(1, 11))  # 1..10

        resp = await self.router.dispatch_task(
            closure,
            args=(test_data,),
            kwargs={"depth": 12},
        )
        self.assertEqual(resp.status, "ok")
        result = resp.result
        self.assertIsInstance(result, dict)
        self.assertEqual(result["fib_factor"], 144)  # fib(12)
        self.assertEqual(result["count"], 5)  # [2, 4, 6, 8, 10]
        self.assertEqual(result["sum"], 75.0)  # (2+4+6+8+10)*2.5 = 30*2.5 = 75.0

    async def test_remote_dispatch_independent_process_isolation(self):
        """
        Confirms task actually executed in the independent remote process,
        not in the local client process (remote worker PID != local client PID).
        """
        local_pid = os.getpid()
        resp = await self.router.dispatch_task(remote_get_execution_info)

        self.assertEqual(resp.status, "ok")
        remote_info = resp.result
        self.assertIsInstance(remote_info, dict)
        self.assertEqual(remote_info["pid"], self.worker_proc.pid)
        self.assertNotEqual(remote_info["pid"], local_pid)

    async def test_structured_response_fields(self):
        """Verifies all required fields of TaskExecutionResponse are populated and correctly typed."""
        custom_id = "test_custom_task_id_999"
        resp = await self.router.dispatch_task(
            remote_add,
            args=(1, 1),
            task_id=custom_id,
        )
        self.assertEqual(resp.task_id, custom_id)
        self.assertEqual(resp.status, "ok")
        self.assertEqual(resp.result, 2)
        self.assertEqual(resp.executed_by, f"127.0.0.1:{self.worker_port}")
        self.assertEqual(resp.target_node_id, self.worker_node_id)
        self.assertIsInstance(resp.round_trip_ms, float)
        self.assertGreater(resp.round_trip_ms, 0.0)

        # Test dictionary conversion
        d = resp.to_dict()
        self.assertEqual(d["task_id"], custom_id)
        self.assertEqual(d["status"], "ok")
        self.assertEqual(d["result"], 2)
        self.assertEqual(d["executed_by"], f"127.0.0.1:{self.worker_port}")
        self.assertEqual(d["target_node_id"], self.worker_node_id)
        self.assertEqual(d["round_trip_ms"], resp.round_trip_ms)

    async def test_remote_execution_error_propagation(self):
        """Verifies remote exceptions return status='error' with full traceback."""
        resp = await self.router.dispatch_task(
            remote_failing_function,
            args=("Cluster overload simulation",),
        )
        self.assertEqual(resp.status, "error")
        self.assertIn("ValueError", str(resp.result))
        self.assertIn("Intentional remote failure: Cluster overload simulation", str(resp.result))
        self.assertEqual(resp.executed_by, f"127.0.0.1:{self.worker_port}")

    async def test_remote_dispatch_timeout_handling(self):
        """Verifies TimeoutError is raised when target remote node is unresponsive."""
        # Point to an unallocated port
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(("127.0.0.1", 0))
            unreachable_port = s.getsockname()[1]

        dead_peer = Peer(ip="127.0.0.1", port=unreachable_port, node_id="dead_node_999")
        self.gossip_local.peer_telemetry["dead_node_999"] = PeerTelemetry(
            peer=dead_peer,
            metrics=NodeResourceMetrics(
                cpu_percent=1.0,
                ram_total_mb=16384.0,
                ram_used_mb=1000.0,
                ram_free_mb=15384.0,
                ram_percent=6.1,
                timestamp=time.time(),
            ),
            last_updated=time.time(),
        )

        # Dead peer has lower CPU (1.0 vs 15.0), so TaskRouter selects it
        best_peer, _ = self.router.select_target_peer()
        self.assertEqual(best_peer.port, unreachable_port)

        # Dispatch should timeout
        with self.assertRaises((TimeoutError, ConnectionResetError)):
            await self.router.dispatch_task(remote_add, args=(1, 2), timeout=0.3)

    async def test_dispatch_local_fallback_when_no_peer(self):
        """Verifies fallback to local execution when no remote peers are eligible."""
        fallback_router = TaskRouter(
            gossip_engine=self.gossip_local,
            stale_threshold=30.0,
            fallback_to_local=True,
        )
        # Exclude the only available worker
        resp = await fallback_router.dispatch_task(
            remote_add,
            args=(50, 50),
            exclude_nodes=[self.worker_node_id],
        )
        self.assertEqual(resp.status, "ok")
        self.assertEqual(resp.result, 100)
        self.assertEqual(resp.executed_by, "local")
        self.assertIsNone(resp.target_node_id)
        self.assertGreaterEqual(resp.round_trip_ms, 0.0)

    async def test_dispatch_raises_when_no_peer_and_no_fallback(self):
        """Verifies NoPeerAvailableError raised when no peer is available and fallback is False."""
        with self.assertRaises(NoPeerAvailableError):
            await self.router.dispatch_task(
                remote_add,
                args=(10, 20),
                exclude_nodes=[self.worker_node_id],
            )


if __name__ == "__main__":
    unittest.main()

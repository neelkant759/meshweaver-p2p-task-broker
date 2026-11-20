"""
MeshWeaver Demo 3: Remote Complex Function Serialization & Execution

Demonstrates:
1. Formulating a non-trivial Python callable:
   - Closure capturing an external configuration factor.
   - Recursive memoized calculation (Fibonacci generator).
   - Numerical data transformation over lists.
   - Statistical aggregation into structured dictionaries.
2. Cloudpickle binary serialization into MWTK: wire format.
3. Asynchronous UDP transmission to a remote worker node.
4. Protected remote execution inside executor.py.
5. MWRS: binary result reception, status validation, and output verification.

Usage (from project root):
    python demos/demo_complex_task.py
"""

from __future__ import annotations

import asyncio
import math
import os
import socket
import subprocess
import sys
import time

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from udp_client import AsyncUDPClient


def build_analytics_pipeline(scale_multiplier: float):
    """
    Higher-order factory creating a closure with captured state,
    recursion, and structured list/dict data processing.
    """
    memo_table = {}

    def analytics_worker(dataset, fib_depth=12):
        # 1. Recursive memoized helper
        def compute_fib(n):
            if n in memo_table:
                return memo_table[n]
            if n <= 1:
                return n
            val = compute_fib(n - 1) + compute_fib(n - 2)
            memo_table[n] = val
            return val

        fib_result = compute_fib(fib_depth)

        # 2. Filtering and scaling using captured closure variable
        scaled_evens = [x * scale_multiplier for x in dataset if x % 2 == 0]

        # 3. Aggregation into structured dictionary (including remote executing PID)
        summary = {
            "executing_pid": os.getpid(),
            "input_sample_size": len(dataset),
            "evens_processed": len(scaled_evens),
            "fibonacci_factor": fib_result,
            "scaled_sum": round(sum(scaled_evens), 4),
            "scaled_mean": round(sum(scaled_evens) / max(1, len(scaled_evens)), 4),
            "std_dev_approx": round(
                math.sqrt(sum((x - (sum(scaled_evens) / max(1, len(scaled_evens)))) ** 2 for x in scaled_evens) / max(1, len(scaled_evens))), 4
            )
        }
        return summary

    return analytics_worker


async def run_complex_task_demo() -> None:
    # Allocate worker port
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        worker_port = s.getsockname()[1]

    client_pid = os.getpid()

    print("=" * 72)
    print("      MeshWeaver Demonstration: Remote Execution of Complex Callable")
    print("=" * 72)
    print(f"[*] Client Process PID         : {client_pid}")
    print(f"[*] Starting Independent Worker Node Process on 127.0.0.1:{worker_port}...")

    # Spawn worker as an independent Python process using node.py
    worker_proc = subprocess.Popen(
        [sys.executable, "-u", os.path.join(PROJECT_ROOT, "node.py"), "--host", "127.0.0.1", "--port", str(worker_port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    print(f"[+] Independent Worker Node PID: {worker_proc.pid}")
    await asyncio.sleep(0.5)

    client = AsyncUDPClient("127.0.0.1", worker_port, timeout=4.0)
    client.start()

    try:
        # Prepare complex closure
        pipeline_func = build_analytics_pipeline(scale_multiplier=3.14159)
        test_dataset = list(range(1, 26))  # Integers 1..25
        depth_parameter = 14

        print("\n[*] Formulated Complex Task:")
        print(f"    - Function Type : Closure capturing scale_multiplier=3.14159")
        print(f"    - Dataset Input : List of {len(test_dataset)} elements [1..25]")
        print(f"    - Kwargs Passed : fib_depth={depth_parameter} (Recursive calculation)")

        # Local ground truth calculation
        expected_result = pipeline_func(test_dataset, fib_depth=depth_parameter)

        # Transmit to remote worker
        print("\n[*] Serializing task via Cloudpickle and transmitting over UDP...")
        start_time = time.perf_counter()
        await client.send_task(pipeline_func, args=(test_dataset,), kwargs={"fib_depth": depth_parameter})

        # Await result
        status, remote_result = await client.receive_task_result()
        duration_ms = (time.perf_counter() - start_time) * 1000.0

        print(f"[+] Result received in {duration_ms:.2f} ms")
        print(f"    - Status: {status}")
        print(f"    - Remote Output Data:")
        for key, val in remote_result.items():
            print(f"        * {key:20}: {val}")

        # Verification check
        assert status == "ok", f"Remote execution failed with status: {status}"
        remote_executing_pid = remote_result["executing_pid"]
        print(f"\n[*] Process Verification:")
        print(f"    - Client PID                : {client_pid}")
        print(f"    - Remote Worker Process PID : {worker_proc.pid}")
        print(f"    - Task Executed in PID      : {remote_executing_pid}")
        assert remote_executing_pid == worker_proc.pid, (
            f"Execution PID mismatch! Expected worker PID {worker_proc.pid}, got {remote_executing_pid}"
        )
        assert remote_executing_pid != client_pid, "Task unexpectedly executed in client process!"
        print(f"[+] Verification SUCCESS: Confirmed task executed in independent remote process (PID {remote_executing_pid})!")

        # Compare output data (excluding executing_pid)
        for k in expected_result:
            if k != "executing_pid":
                assert remote_result[k] == expected_result[k], f"Mismatch for key {k}!"
        print("[+] Output Verification SUCCESS: Remote computation matches expected ground truth 100%!")

    finally:
        client.close()
        print("\n[*] Terminating independent worker process (PID: %d)..." % worker_proc.pid)
        worker_proc.terminate()
        try:
            worker_proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            worker_proc.kill()
        print("[+] Independent worker process shutdown cleanly.")


def main() -> None:
    try:
        asyncio.run(run_complex_task_demo())
    except KeyboardInterrupt:
        print("\nDemo interrupted by user.")


if __name__ == "__main__":
    main()

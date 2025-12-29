import argparse
import os
import signal
import subprocess
import sys
import threading
import time

from shepherd import constants
from shepherd import fs


class FailureExit(Exception):
    def __init__(self, exit_code, reason, detail=None):
        super().__init__(reason)
        self.exit_code = exit_code
        self.reason = reason
        self.detail = detail


def _now():
    return int(time.time())


def _hostname():
    return os.environ.get("SLURMD_NODENAME") or os.uname().nodename


def _write_failure(path, exit_code, reason, detail=None):
    payload = {
        "timestamp": _now(),
        "exit_code": exit_code,
        "reason": reason,
        "detail": detail,
        "node": _hostname(),
        "job_id": os.environ.get("SLURM_JOB_ID"),
    }
    fs.atomic_write_json(path, payload)


def _write_final(path):
    fs.atomic_write_json(path, {"timestamp": _now()})


def _run_cmd(cmd, timeout_sec=None):
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_sec,
        check=False,
    )


def _probe_gpu_visibility():
    result = _run_cmd(["nvidia-smi", "-L"], timeout_sec=10)
    if result.returncode != 0:
        raise FailureExit(constants.EXIT_NODE_FAULT, "gpu_visibility_failed", result.stderr)
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        raise FailureExit(constants.EXIT_NODE_FAULT, "gpu_visibility_empty", result.stdout)
    return result.stdout


def _probe_expected_counts():
    expected = os.environ.get("SHEPHERD_EXPECTED_GPU_COUNT")
    expected_mig = os.environ.get("SHEPHERD_EXPECTED_MIG_COUNT")
    if not expected and not expected_mig:
        return
    result = _run_cmd(["nvidia-smi", "-L"], timeout_sec=10)
    if result.returncode != 0:
        raise FailureExit(constants.EXIT_NODE_FAULT, "gpu_visibility_failed", result.stderr)
    lines = result.stdout.splitlines()
    if expected:
        count = sum(1 for line in lines if line.strip().startswith("GPU "))
        if count != int(expected):
            raise FailureExit(constants.EXIT_NODE_FAULT, "gpu_count_mismatch", result.stdout)
    if expected_mig:
        count = sum(1 for line in lines if "MIG" in line)
        if count != int(expected_mig):
            raise FailureExit(constants.EXIT_NODE_FAULT, "mig_count_mismatch", result.stdout)


def _probe_cuda_smoke():
    if os.environ.get("SHEPHERD_SKIP_CUDA_SMOKE") == "1":
        return
    script = r"""
import sys
def try_torch():
    import torch
    if not torch.cuda.is_available():
        raise RuntimeError("cuda not available")
    a = torch.randn(1024, device="cuda")
    b = a * 2.0
    torch.cuda.synchronize()
def try_cupy():
    import cupy
    a = cupy.random.randn(1024)
    b = a * 2
    cupy.cuda.Stream.null.synchronize()
def try_numba():
    from numba import cuda
    import numpy as np
    @cuda.jit
    def add(a, b, out):
        i = cuda.grid(1)
        if i < out.size:
            out[i] = a[i] + b[i]
    a = np.ones(128, dtype=np.float32)
    b = np.ones(128, dtype=np.float32)
    out = np.zeros_like(a)
    d_a = cuda.to_device(a)
    d_b = cuda.to_device(b)
    d_out = cuda.to_device(out)
    add[1, 128](d_a, d_b, d_out)
    d_out.copy_to_host()
tested = False
for fn in (try_torch, try_cupy, try_numba):
    try:
        fn()
        tested = True
        print(f"CUDA_OK: {fn.__name__}", file=sys.stderr)
        sys.exit(0)  # Success
    except ImportError:
        continue  # Package not installed, try next
    except Exception as e:
        # Package installed but CUDA failed - this is a real failure
        print(f"CUDA_FAIL: {fn.__name__}: {e}", file=sys.stderr)
        sys.exit(2)
# No packages installed - skip test but warn
print("CUDA_SKIP: no torch/cupy/numba installed", file=sys.stderr)
sys.exit(0)
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    # Log the smoke test result for debugging
    smoke_output = result.stderr.strip()
    if smoke_output:
        print(f"[shepherd] smoke test: {smoke_output}", file=sys.stderr)
    # Only fail on exit code 2 (actual CUDA failure), not 1 (no packages) or 0 (success)
    if result.returncode == 2:
        detail = result.stderr.strip() or result.stdout.strip()
        raise FailureExit(constants.EXIT_CUDA_FAILURE, "cuda_smoke_failed", detail)


def _probe_trespassers():
    if os.environ.get("SHEPHERD_TRESPASSER_CHECK") != "1":
        return
    result = _run_cmd(
        ["nvidia-smi", "--query-compute-apps=pid,process_name", "--format=csv,noheader"],
        timeout_sec=10,
    )
    if result.returncode != 0:
        return
    current_user = os.environ.get("USER")
    for line in result.stdout.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if not parts or not parts[0].isdigit():
            continue
        pid = parts[0]
        ps = _run_cmd(["ps", "-o", "user=", "-p", pid], timeout_sec=5)
        user = ps.stdout.strip() if ps.returncode == 0 else None
        if user and current_user and user != current_user:
            raise FailureExit(constants.EXIT_TRESPASSER, "foreign_gpu_process", line)


def _heartbeat_loop(path, interval_sec, stop_event):
    while not stop_event.is_set():
        fs.atomic_write_text(path, f"{_now()}\n")
        stop_event.wait(interval_sec)


def _run_workload(cmd):
    return subprocess.call(cmd)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="shepherd-wrapper")
    parser.add_argument("--run-id")
    parser.add_argument("--run-mode", choices=["run_once", "indefinite"])
    parser.add_argument("--state-dir")
    parser.add_argument("--heartbeat-interval", type=int, default=30)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    run_id = args.run_id or os.environ.get("SHEPHERD_RUN_ID")
    if not run_id:
        raise SystemExit("Missing run id")

    state_dir = args.state_dir or os.path.expanduser("~/.slurm_shepherd")
    run_dir = os.path.join(state_dir, "runs", run_id)
    heartbeat_path = os.path.join(run_dir, constants.HEARTBEAT_FILENAME)
    failure_path = os.path.join(run_dir, constants.FAILURE_FILENAME)
    final_path = os.path.join(run_dir, constants.FINAL_FILENAME)

    if args.command and args.command[0] == "--":
        cmd = args.command[1:]
    else:
        cmd = args.command
    if not cmd:
        raise SystemExit("Missing workload command")

    def _handle_signal(signum, frame):
        _write_failure(failure_path, constants.EXIT_WORKLOAD_FAILURE, "terminated", str(signum))
        raise SystemExit(constants.EXIT_WORKLOAD_FAILURE)

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _handle_signal)

    try:
        _probe_gpu_visibility()
        _probe_expected_counts()
        _probe_cuda_smoke()
        _probe_trespassers()
    except FailureExit as exc:
        _write_failure(failure_path, exc.exit_code, exc.reason, exc.detail)
        return exc.exit_code

    stop_event = threading.Event()
    thread = threading.Thread(
        target=_heartbeat_loop,
        args=(heartbeat_path, args.heartbeat_interval, stop_event),
        daemon=True,
    )
    thread.start()

    exit_code = _run_workload(cmd)
    stop_event.set()
    thread.join(timeout=5)

    if exit_code != 0:
        _write_failure(
            failure_path,
            constants.EXIT_WORKLOAD_FAILURE,
            "workload_failure",
            {"exit_code": exit_code},
        )
        return constants.EXIT_WORKLOAD_FAILURE

    run_mode = args.run_mode or os.environ.get("SHEPHERD_RUN_MODE")
    if run_mode == "run_once":
        _write_final(final_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())

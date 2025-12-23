import atexit
import os
import shlex
import signal
import sys
import time

from shepherd import backoff
from shepherd import blacklist
from shepherd import constants
from shepherd import fs
from shepherd import heartbeat
from shepherd import slurm
from shepherd import state


def is_daemon_running():
    """Check if a daemon is already running by reading the PID file."""
    pid_path = constants.DAEMON_PID_PATH
    if not os.path.exists(pid_path):
        return False
    try:
        with open(pid_path, "r") as f:
            pid = int(f.read().strip())
        # Check if process exists
        os.kill(pid, 0)
        return True
    except (ValueError, ProcessLookupError, PermissionError, FileNotFoundError):
        # PID file is stale or invalid
        try:
            os.remove(pid_path)
        except OSError:
            pass
        return False


def _write_pid_file():
    """Write current PID to the daemon PID file."""
    os.makedirs(os.path.dirname(constants.DAEMON_PID_PATH), exist_ok=True)
    with open(constants.DAEMON_PID_PATH, "w") as f:
        f.write(str(os.getpid()))


def _remove_pid_file():
    """Remove the daemon PID file."""
    try:
        os.remove(constants.DAEMON_PID_PATH)
    except OSError:
        pass


class ShepherdDaemon:
    def __init__(self, poll_interval_sec=10):
        self.poll_interval_sec = poll_interval_sec
        self._running = False

    def run(self):
        fs.ensure_dirs()
        if is_daemon_running():
            print("Daemon is already running", file=sys.stderr)
            return 1
        _write_pid_file()
        atexit.register(_remove_pid_file)

        def _handle_signal(signum, frame):
            self._running = False

        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)

        self._running = True
        try:
            while self._running:
                self._tick()
                time.sleep(self.poll_interval_sec)
        finally:
            _remove_pid_file()
        return 0

    def stop(self):
        self._running = False

    def _tick(self):
        run_ids = fs.list_runs()
        job_map = {}
        meta_map = {}
        for run_id in run_ids:
            meta = fs.read_json(fs.run_file(run_id, constants.META_FILENAME))
            meta_map[run_id] = meta
            if isinstance(meta, dict):
                job_id = meta.get("slurm_job_id")
                if job_id:
                    job_map[run_id] = str(job_id)

        slurm_result = {"jobs": {}}
        if job_map:
            slurm_result = slurm.squeue(list(job_map.values()))

        for run_id in run_ids:
            with fs.run_lock(run_id) as lock:
                if lock is None:
                    continue
                self._handle_run(run_id, meta_map.get(run_id), slurm_result)

    def _handle_run(self, run_id, meta, slurm_result):
        now = int(time.time())
        if meta is None or isinstance(meta, dict) and meta.get("_corrupt"):
            return

        control = fs.read_json(fs.run_file(run_id, constants.CONTROL_FILENAME)) or {}
        ended = fs.read_json(fs.run_file(run_id, constants.ENDED_FILENAME))
        final = fs.read_json(fs.run_file(run_id, constants.FINAL_FILENAME))
        failure = fs.read_json(fs.run_file(run_id, constants.FAILURE_FILENAME))
        hb_ts = heartbeat.read_heartbeat(fs.run_file(run_id, constants.HEARTBEAT_FILENAME))
        progress = fs.read_json(fs.run_file(run_id, constants.PROGRESS_FILENAME))

        meta = _apply_overrides(meta, control)

        if ended is not None:
            if control.get("restart_requested"):
                self._clear_terminal_state(run_id, meta)
                state.update_control(run_id, {"restart_requested": False, "stop_requested": False})
                ended = None
                final = None
                failure = None
            else:
                return

        run_mode = meta.get("run_mode")
        keep_alive_sec = meta.get("keep_alive_sec")
        created_at = meta.get("created_at") or now
        started_at = meta.get("started_at") or created_at
        slurm_job_id = meta.get("slurm_job_id")

        if run_mode == "indefinite" and keep_alive_sec is not None:
            if now - started_at >= int(keep_alive_sec):
                self._expire_run(run_id, meta, control, slurm_job_id)
                return

        if control.get("stop_requested"):
            self._stop_run(run_id, meta, slurm_job_id)
            return

        if control.get("restart_requested"):
            if slurm_job_id:
                slurm.scancel(slurm_job_id)
            state.update_control(run_id, {"restart_requested": False})
            slurm_job_id = None

        slurm_state = None
        slurm_reason = None
        if slurm_job_id:
            job_info = slurm_result.get("jobs", {}).get(str(slurm_job_id))
            if job_info:
                slurm_state = job_info.get("state")
                slurm_reason = job_info.get("reason")
            else:
                # Job not in squeue - check sacct for final state
                sacct_info = slurm.sacct(slurm_job_id)
                if sacct_info:
                    sacct_state = sacct_info.get("state", "")
                    exit_code = sacct_info.get("exit_code", 1)
                    node = sacct_info.get("node")

                    if sacct_state == "COMPLETED" and exit_code == 0:
                        # Job completed successfully - mark as done for run_once
                        if run_mode == "run_once" and final is None:
                            state.write_final(run_id)
                            state.write_ended(run_id, {"reason": "completed", "timestamp": now})
                            return
                    elif sacct_state in ("FAILED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL"):
                        # Job failed - record for restart and maybe blacklist
                        if node and sacct_state in ("NODE_FAIL", "TIMEOUT"):
                            ttl = meta.get("blacklist_ttl_sec")
                            blacklist.add_node(node, ttl_sec=ttl, reason=sacct_state)
                        self._record_restart(run_id, meta, now, reason=sacct_state.lower())
                        state.update_meta(run_id, {"slurm_job_id": None})
                        return
                    elif sacct_state in ("CANCELLED", "PREEMPTED"):
                        # Preempted - restart without penalty
                        state.update_meta(run_id, {"slurm_job_id": None, "next_submit_at": now})
                        return

                slurm_job_id = None

        if slurm_state:
            state.update_meta(
                run_id,
                {"slurm_state": slurm_state, "slurm_reason": slurm_reason},
            )

        if slurm_state == "RUNNING":
            interval = meta.get("heartbeat_interval_sec", constants.DEFAULT_HEARTBEAT_INTERVAL_SEC)
            grace = meta.get("heartbeat_grace_sec", constants.DEFAULT_HEARTBEAT_GRACE_SEC)
            if heartbeat.is_stale(hb_ts, interval_sec=interval, grace_sec=grace, now=now):
                slurm.scancel(slurm_job_id)
                self._record_restart(run_id, meta, now, reason="heartbeat_stale")
                return
            if slurm_reason:
                state.update_meta(run_id, {"last_node": slurm_reason})
            if _progress_stale(progress, meta, now):
                slurm.scancel(slurm_job_id)
                self._record_restart(run_id, meta, now, reason="progress_stale")
                return
            return

        if slurm_job_id and slurm_state:
            return

        if self._finalize_if_complete(run_id, meta, final, run_mode):
            return

        if control.get("paused"):
            return

        if run_mode == "run_once":
            max_retries = meta.get("max_retries")
            restart_count = meta.get("restart_count", 0)
            if max_retries is not None and restart_count >= int(max_retries):
                state.write_ended(run_id, {"reason": "max_retries", "timestamp": now})
                return

        self._apply_failure_blacklist(run_id, meta, failure)
        if not self._ready_for_submit(meta, now):
            return
        self._submit_run(run_id, meta, now)

    def _expire_run(self, run_id, meta, control, slurm_job_id):
        if slurm_job_id:
            slurm.scancel(slurm_job_id)
        state.write_ended(run_id, {"reason": "expired", "timestamp": int(time.time())})

    def _stop_run(self, run_id, meta, slurm_job_id):
        if slurm_job_id:
            slurm.scancel(slurm_job_id)
            state.update_meta(run_id, {"slurm_job_id": None})
            return
        state.write_ended(run_id, {"reason": "stopped", "timestamp": int(time.time())})

    def _finalize_if_complete(self, run_id, meta, final, run_mode):
        if run_mode == "run_once" and final is not None:
            state.write_ended(run_id, {"reason": "completed", "timestamp": int(time.time())})
            return True
        return False

    def _ready_for_submit(self, meta, now):
        next_submit_at = meta.get("next_submit_at")
        if next_submit_at is None:
            return True
        return now >= int(next_submit_at)

    def _record_restart(self, run_id, meta, now, reason=None):
        restart_count = int(meta.get("restart_count", 0)) + 1
        base = meta.get("backoff_base_sec", 10)
        max_sec = meta.get("backoff_max_sec", 300)
        delay = backoff.compute_backoff(restart_count, base_sec=base, max_sec=max_sec)
        updates = {
            "restart_count": restart_count,
            "last_restart_at": now,
            "next_submit_at": now + delay,
            "restart_reason": reason,
        }
        state.update_meta(run_id, updates)

    def _apply_failure_blacklist(self, run_id, meta, failure):
        if not isinstance(failure, dict):
            return
        exit_code = failure.get("exit_code")
        node = failure.get("node")
        ts = failure.get("timestamp")
        if exit_code not in {
            constants.EXIT_NODE_FAULT,
            constants.EXIT_TRESPASSER,
            constants.EXIT_CUDA_FAILURE,
        }:
            return
        last_seen = meta.get("last_failure_ts")
        if ts is not None and last_seen == ts:
            return
        if node:
            ttl = meta.get("blacklist_ttl_sec")
            blacklist.add_node(node, ttl_sec=ttl, reason=failure.get("reason"))
            self._append_badnode_event(run_id, node, failure)
        state.update_meta(run_id, {"last_failure_ts": ts})

    def _append_badnode_event(self, run_id, node, failure):
        path = fs.run_file(run_id, constants.BADNODE_EVENTS_FILENAME)
        line = "{ts} node={node} exit={code} reason={reason}\n".format(
            ts=failure.get("timestamp"),
            node=node,
            code=failure.get("exit_code"),
            reason=failure.get("reason"),
        )
        existing = fs.read_text(path)
        if isinstance(existing, dict):
            existing = ""
        fs.atomic_write_text(path, (existing or "") + line)

    def _get_partition_arg(self, meta, now):
        """Determine the partition argument to use, considering fallback logic."""
        fallback_config = meta.get("partition_fallback")
        if not fallback_config:
            return None

        partitions = fallback_config.get("partitions", [])
        if not partitions:
            return None

        current_index = meta.get("current_partition_index", 0)
        reset_sec = fallback_config.get(
            "reset_to_preferred_sec", constants.DEFAULT_RESET_TO_PREFERRED_SEC
        )
        last_preferred = meta.get("last_preferred_attempt_at")

        # Periodically try preferred partition again
        if current_index > 0 and last_preferred is not None:
            if now - int(last_preferred) >= reset_sec:
                current_index = 0
                state.update_meta(
                    meta.get("run_id"),
                    {
                        "current_partition_index": 0,
                        "partition_failure_count": 0,
                        "last_preferred_attempt_at": now,
                    },
                )

        # Clamp index to valid range
        if current_index >= len(partitions):
            current_index = len(partitions) - 1

        return f"--partition={partitions[current_index]}"

    def _handle_sbatch_failure(self, run_id, meta, now, result):
        """Handle sbatch failure with partition fallback logic."""
        fallback_config = meta.get("partition_fallback")

        if not fallback_config or not fallback_config.get("partitions"):
            # No fallback configured - use existing behavior
            self._record_restart(run_id, meta, now, reason="sbatch_failed")
            return False

        partitions = fallback_config.get("partitions", [])
        retry_per_partition = fallback_config.get(
            "retry_per_partition", constants.DEFAULT_RETRY_PER_PARTITION
        )

        current_index = meta.get("current_partition_index", 0)
        failure_count = meta.get("partition_failure_count", 0) + 1

        if failure_count >= retry_per_partition:
            # Advance to next partition
            next_index = current_index + 1
            if next_index < len(partitions):
                state.update_meta(
                    run_id,
                    {
                        "current_partition_index": next_index,
                        "partition_failure_count": 0,
                        "last_partition_fallback_at": now,
                    },
                )
                # Return True to signal immediate retry on new partition
                return True
            else:
                # Exhausted all partitions - wrap around and apply backoff
                state.update_meta(
                    run_id,
                    {
                        "current_partition_index": 0,
                        "partition_failure_count": 0,
                        "last_preferred_attempt_at": now,
                    },
                )
        else:
            # Increment failure count, stay on current partition
            state.update_meta(run_id, {"partition_failure_count": failure_count})

        # Apply standard backoff
        current_partition = partitions[current_index] if current_index < len(partitions) else "unknown"
        self._record_restart(run_id, meta, now, reason=f"sbatch_failed_partition_{current_partition}")
        return False

    def _submit_run(self, run_id, meta, now):
        script_path = meta.get("sbatch_script") or meta.get("sbatch_script_path") or meta.get("sbatch_path")
        if not script_path:
            return
        extra_args = meta.get("sbatch_args") or []
        if isinstance(extra_args, str):
            extra_args = shlex.split(extra_args)
        extra_args = list(extra_args)

        # Auto-inject --gres=gpu:N if gpus specified and not already set
        gpus = meta.get("gpus")
        if gpus and not any(arg.startswith("--gres") for arg in extra_args):
            extra_args.append(f"--gres=gpu:{gpus}")

        # Handle partition fallback
        partition_arg = self._get_partition_arg(meta, now)
        if partition_arg:
            # Remove any existing --partition from extra_args
            extra_args = [arg for arg in extra_args if not arg.startswith("--partition")]
            extra_args.append(partition_arg)

        bl_data = blacklist.load_blacklist()
        limit = meta.get("blacklist_limit", constants.DEFAULT_BLACKLIST_LIMIT)
        exclude_nodes = blacklist.exclude_list(bl_data, limit=limit)
        if exclude_nodes:
            extra_args = extra_args + [f"--exclude={','.join(exclude_nodes)}"]

        # Auto-wrap script with shepherd wrapper
        run_mode = meta.get("run_mode", "run_once")
        wrapped_script = self._generate_wrapped_script(script_path, run_id, run_mode)

        result = slurm.sbatch_script(wrapped_script, extra_args=extra_args)
        if not result["ok"]:
            retry_now = self._handle_sbatch_failure(run_id, meta, now, result)
            if retry_now:
                # Reload meta and retry immediately with new partition
                updated_meta = fs.read_json(fs.run_file(run_id, constants.META_FILENAME))
                if updated_meta and not updated_meta.get("_corrupt"):
                    self._submit_run(run_id, updated_meta, now)
            return

        job_id = _parse_sbatch_job_id(result["stdout"])
        updates = {
            "slurm_job_id": job_id,
            "slurm_state": "PENDING",
            "slurm_reason": None,
            "last_submit_at": now,
            "partition_failure_count": 0,  # Reset on success
        }
        if partition_arg:
            updates["current_partition"] = partition_arg.split("=")[1]
        if not meta.get("started_at"):
            updates["started_at"] = now
        restart_count = int(meta.get("restart_count", 0))
        updates["restart_count"] = restart_count
        state.update_meta(run_id, updates)

    def _generate_wrapped_script(self, script_path, run_id, run_mode):
        """Generate a wrapper script that includes shepherd.wrapper."""
        import os
        expanded_path = os.path.expanduser(script_path)

        # Read original script
        try:
            with open(expanded_path, "r") as f:
                original = f.read()
        except Exception:
            # Fallback to just running the script directly
            return f"""#!/bin/bash
python -m shepherd.wrapper --run-id {run_id} --run-mode {run_mode} -- bash {expanded_path}
"""

        # Extract #SBATCH directives and script body
        lines = original.splitlines()
        sbatch_lines = []
        body_lines = []
        in_header = True

        for line in lines:
            stripped = line.strip()
            if in_header:
                if stripped.startswith("#!"):
                    sbatch_lines.append(line)
                elif stripped.startswith("#SBATCH"):
                    sbatch_lines.append(line)
                elif stripped.startswith("#SHEPHERD"):
                    # Skip #SHEPHERD directives - already processed
                    continue
                elif stripped.startswith("#") or stripped == "":
                    sbatch_lines.append(line)
                else:
                    in_header = False
                    body_lines.append(line)
            else:
                body_lines.append(line)

        # Build wrapped script using heredoc for robustness with complex scripts
        body = "\n".join(body_lines)
        wrapped = "\n".join(sbatch_lines)
        wrapped += f"""

# Auto-wrapped by shepherd
python -m shepherd.wrapper --run-id {run_id} --run-mode {run_mode} -- bash << '__SHEPHERD_SCRIPT_END__'
{body}
__SHEPHERD_SCRIPT_END__
"""
        return wrapped

    def _clear_terminal_state(self, run_id, meta):
        for filename in (constants.ENDED_FILENAME, constants.FINAL_FILENAME, constants.FAILURE_FILENAME):
            path = fs.run_file(run_id, filename)
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
            except OSError:
                pass
        state.update_meta(
            run_id,
            {"slurm_job_id": None, "slurm_state": None, "slurm_reason": None, "next_submit_at": None},
        )


def _parse_sbatch_job_id(stdout):
    for token in stdout.split():
        if token.isdigit():
            return token
    return None


def _apply_overrides(meta, control):
    overrides = {}
    if isinstance(control, dict):
        overrides = control.get("config_overrides") or {}
    if not overrides:
        return meta
    allowed = {
        "heartbeat_interval_sec",
        "heartbeat_grace_sec",
        "max_retries",
        "backoff_base_sec",
        "backoff_max_sec",
        "blacklist_ttl_sec",
        "blacklist_limit",
        "keep_alive_sec",
        "sbatch_args",
        "sbatch_script",
        "progress_stall_sec",
        "partition_fallback",
    }
    merged = dict(meta)
    for key, value in overrides.items():
        if key in allowed:
            merged[key] = value
    return merged


def _progress_stale(progress, meta, now):
    if not isinstance(progress, dict):
        return False
    stall_sec = meta.get("progress_stall_sec")
    if stall_sec is None:
        return False
    ts = progress.get("timestamp") or progress.get("updated_at")
    if ts is None:
        return False
    return now - int(ts) > int(stall_sec)

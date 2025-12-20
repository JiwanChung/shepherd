import os
import shlex
import time

from shepherd import backoff
from shepherd import blacklist
from shepherd import constants
from shepherd import fs
from shepherd import heartbeat
from shepherd import slurm
from shepherd import state


class ShepherdDaemon:
    def __init__(self, poll_interval_sec=10):
        self.poll_interval_sec = poll_interval_sec
        self._running = False

    def run(self):
        fs.ensure_dirs()
        self._running = True
        while self._running:
            self._tick()
            time.sleep(self.poll_interval_sec)

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

    def _submit_run(self, run_id, meta, now):
        script_path = meta.get("sbatch_script") or meta.get("sbatch_script_path") or meta.get("sbatch_path")
        if not script_path:
            return
        extra_args = meta.get("sbatch_args") or []
        if isinstance(extra_args, str):
            extra_args = shlex.split(extra_args)

        bl_data = blacklist.load_blacklist()
        limit = meta.get("blacklist_limit", constants.DEFAULT_BLACKLIST_LIMIT)
        exclude_nodes = blacklist.exclude_list(bl_data, limit=limit)
        if exclude_nodes:
            extra_args = list(extra_args) + [f"--exclude={','.join(exclude_nodes)}"]

        result = slurm.sbatch(script_path, extra_args=extra_args)
        if not result["ok"]:
            self._record_restart(run_id, meta, now, reason="sbatch_failed")
            return

        job_id = _parse_sbatch_job_id(result["stdout"])
        updates = {
            "slurm_job_id": job_id,
            "slurm_state": "PENDING",
            "slurm_reason": None,
            "last_submit_at": now,
        }
        if not meta.get("started_at"):
            updates["started_at"] = now
        restart_count = int(meta.get("restart_count", 0))
        updates["restart_count"] = restart_count
        state.update_meta(run_id, updates)

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

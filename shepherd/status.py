from shepherd import heartbeat


STATUS_HEALTHY = "healthy_running"
STATUS_DEGRADED = "running_degraded"
STATUS_UNRESPONSIVE = "unresponsive"
STATUS_PENDING = "pending"
STATUS_RESTARTING = "restarting"
STATUS_CRASH_LOOP = "crash_loop"
STATUS_COMPLETED = "completed_success"
STATUS_EXPIRED = "ended_expired"
STATUS_STOPPED = "stopped_manual"
STATUS_UNKNOWN = "error_unknown"


def compute_status(meta, control, ended, final, heartbeat_ts, slurm_state=None, now=None):
    if ended is not None:
        if final is not None:
            return STATUS_COMPLETED
        if isinstance(ended, dict) and ended.get("reason") == "expired":
            return STATUS_EXPIRED
        if isinstance(control, dict) and control.get("stop_requested"):
            return STATUS_STOPPED
        return STATUS_UNKNOWN

    if slurm_state is not None:
        slurm_state = slurm_state.upper()
        if slurm_state == "PENDING":
            return STATUS_PENDING
        if slurm_state == "RUNNING":
            if heartbeat.is_stale(heartbeat_ts, now=now):
                return STATUS_UNRESPONSIVE
            if isinstance(control, dict) and control.get("paused"):
                return STATUS_DEGRADED
            return STATUS_HEALTHY
        if slurm_state in {"FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY"}:
            return STATUS_RESTARTING

    # No slurm_state means job not submitted yet
    slurm_job_id = meta.get("slurm_job_id") if isinstance(meta, dict) else None
    if slurm_job_id is None:
        # Job waiting to be submitted
        if isinstance(control, dict) and control.get("paused"):
            return STATUS_STOPPED
        return STATUS_PENDING

    # Job was submitted but no longer in queue (finished or lost)
    if heartbeat_ts is not None and heartbeat.is_stale(heartbeat_ts, now=now):
        return STATUS_UNRESPONSIVE

    if isinstance(control, dict) and control.get("paused"):
        return STATUS_DEGRADED

    return STATUS_RESTARTING

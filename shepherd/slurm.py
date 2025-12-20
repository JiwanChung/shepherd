import subprocess


def _run(cmd, timeout_sec=10):
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_sec,
            check=False,
            text=True,
        )
        return {
            "ok": result.returncode == 0,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    except subprocess.TimeoutExpired:
        return {"ok": False, "returncode": None, "stdout": "", "stderr": "timeout"}


def squeue(job_ids=None):
    cmd = ["squeue", "-h", "-o", "%i|%T|%R"]
    if job_ids:
        cmd.extend(["-j", ",".join(str(j) for j in job_ids)])
    result = _run(cmd, timeout_sec=5)
    if not result["ok"]:
        return result
    jobs = {}
    for line in result["stdout"].splitlines():
        parts = line.split("|")
        if len(parts) != 3:
            continue
        job_id, state, reason = parts
        jobs[job_id] = {"state": state, "reason": reason}
    result["jobs"] = jobs
    return result


def sbatch(script_path, extra_args=None):
    cmd = ["sbatch"]
    if extra_args:
        cmd.extend(extra_args)
    cmd.append(script_path)
    return _run(cmd, timeout_sec=10)


def scancel(job_id):
    return _run(["scancel", str(job_id)], timeout_sec=5)

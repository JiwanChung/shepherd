import re
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
    import os
    cmd = ["sbatch"]
    if extra_args:
        cmd.extend(extra_args)
    cmd.append(os.path.expanduser(script_path))
    return _run(cmd, timeout_sec=10)


def sbatch_script(script_content, extra_args=None):
    """Submit a script via stdin."""
    cmd = ["sbatch"]
    if extra_args:
        cmd.extend(extra_args)
    try:
        result = subprocess.run(
            cmd,
            input=script_content,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
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


def scancel(job_id):
    return _run(["scancel", str(job_id)], timeout_sec=5)


def sacct(job_id):
    """Get completed job info from sacct.

    Returns dict with state, exit_code, node, or None if not found.
    """
    cmd = [
        "sacct", "-j", str(job_id),
        "--format=JobID,State,ExitCode,NodeList",
        "--noheader", "--parsable2", "-X"
    ]
    result = _run(cmd, timeout_sec=5)
    if not result["ok"]:
        return None

    for line in result["stdout"].splitlines():
        parts = line.split("|")
        if len(parts) >= 4:
            # Parse exit code (format: "0:0" = exit:signal)
            exit_code = 0
            if ":" in parts[2]:
                try:
                    exit_code = int(parts[2].split(":")[0])
                except ValueError:
                    pass
            return {
                "job_id": parts[0],
                "state": parts[1],
                "exit_code": exit_code,
                "node": parts[3],
            }
    return None


PARTITION_FAILURE_PATTERNS = [
    "Invalid partition name",
    "Requested partition configuration not available",
    "Unable to allocate resources",
    "QOSMaxJobsPerUserLimit",
    "PartitionDown",
    "PartitionNodeLimit",
    "PartitionTimeLimit",
    "ReqNodeNotAvail",
    "QOSMaxGRESPerUser",
    "QOSMaxCpuPerUserLimit",
]


def parse_sbatch_failure_reason(stderr):
    """Extract a canonical reason from sbatch failure stderr."""
    if not stderr:
        return "unknown"
    stderr_lower = stderr.lower()
    for pattern in PARTITION_FAILURE_PATTERNS:
        if pattern.lower() in stderr_lower:
            return pattern
    return "unknown"


# Known GPU VRAM in GB (common types)
GPU_VRAM = {
    "a100": 80,
    "a100-80": 80,
    "a100-40": 40,
    "a100_80g": 80,
    "a100_40g": 40,
    "h100": 80,
    "h200": 141,
    "v100": 32,
    "v100-32": 32,
    "v100-16": 16,
    "a40": 48,
    "a30": 24,
    "a10": 24,
    "l40": 48,
    "l40s": 48,
    "l4": 24,
    "t4": 16,
    "p100": 16,
    "p40": 24,
    "rtx3090": 24,
    "rtx4090": 24,
    "rtx6000": 48,
}


def _parse_gpu_info(gres):
    """Parse GPU type and count from GRES string.

    Examples: "gpu:a100:4", "gpu:4", "gpu:tesla_v100:2"
    Returns: (gpu_type, gpu_count, vram_gb)
    """
    gpu_type = None
    gpu_count = 0
    vram_gb = 0

    if "gpu" not in gres.lower():
        return gpu_type, gpu_count, vram_gb

    # Match patterns like gpu:a100:4, gpu:tesla_v100:2, gpu:4
    match = re.search(r'gpu:([^:,\s]+):(\d+)', gres.lower())
    if match:
        gpu_type = match.group(1)
        gpu_count = int(match.group(2))
    else:
        # Simple gpu:N pattern
        match = re.search(r'gpu:(\d+)', gres.lower())
        if match:
            gpu_count = int(match.group(1))
        elif "gpu" in gres.lower():
            gpu_count = 1

    # Lookup VRAM from known types
    if gpu_type:
        # Try exact match first, then partial matches
        gpu_type_clean = gpu_type.replace("_", "").replace("-", "").lower()
        for known, vram in GPU_VRAM.items():
            known_clean = known.replace("_", "").replace("-", "").lower()
            if known_clean in gpu_type_clean or gpu_type_clean in known_clean:
                vram_gb = vram
                break

    return gpu_type, gpu_count, vram_gb


def parse_shepherd_directives(script_path):
    """Parse #SHEPHERD directives from sbatch script.

    Example:
        #SHEPHERD --gpus 4 --min-vram 40 --prefer min
        #SHEPHERD --mode indefinite --keep-alive 3600
        #SHEPHERD --max-retries 5 --backoff-base 30

    Returns dict with parsed values.
    """
    import os
    directives = {}
    if not script_path:
        return directives

    # Map of arg name -> (dict key, type)
    arg_map = {
        "--gpus": ("gpus", int),
        "--min-vram": ("min_vram", int),
        "--max-vram": ("max_vram", int),
        "--prefer": ("prefer", str),
        "--mode": ("run_mode", str),
        "--run-mode": ("run_mode", str),
        "--partitions": ("partitions", str),
        "--max-retries": ("max_retries", int),
        "--keep-alive": ("keep_alive_sec", int),
        "--heartbeat-interval": ("heartbeat_interval_sec", int),
        "--heartbeat-grace": ("heartbeat_grace_sec", int),
        "--backoff-base": ("backoff_base_sec", int),
        "--backoff-max": ("backoff_max_sec", int),
        "--blacklist-ttl": ("blacklist_ttl_sec", int),
        "--run-id": ("run_id", str),
    }

    try:
        expanded = os.path.expanduser(script_path)
        with open(expanded, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line.startswith("#SHEPHERD"):
                    continue
                # Parse directive
                parts = line[len("#SHEPHERD"):].strip().split()
                i = 0
                while i < len(parts):
                    arg = parts[i]
                    if arg in arg_map and i + 1 < len(parts):
                        key, typ = arg_map[arg]
                        try:
                            directives[key] = typ(parts[i + 1])
                        except ValueError:
                            pass
                        i += 2
                    else:
                        i += 1
    except Exception:
        pass
    return directives


def list_nodes():
    """List all nodes with their state and partition info.

    Returns list of dicts with: node, partition, state, gres
    Returns empty list if sinfo unavailable.
    """
    try:
        cmd = ["sinfo", "-h", "-N", "-o", "%N|%P|%T|%G"]
        result = _run(cmd, timeout_sec=5)
        if not result["ok"]:
            return []
    except FileNotFoundError:
        return []

    nodes = []
    seen = set()
    for line in result["stdout"].splitlines():
        parts = line.split("|")
        if len(parts) < 4:
            continue
        node = parts[0]
        if node in seen:
            continue
        seen.add(node)
        partition = parts[1].rstrip("*")
        state = parts[2]
        gres = parts[3]
        gpu_type, gpu_count, vram_gb = _parse_gpu_info(gres)
        nodes.append({
            "node": node,
            "partition": partition,
            "state": state,
            "gpu_type": gpu_type,
            "gpu_count": gpu_count,
            "vram_gb": vram_gb,
        })
    return nodes


def discover_gpu_partitions(min_gpus=None, min_vram=None, max_vram=None, prefer="max"):
    """Discover partitions with GPU resources.

    Args:
        min_gpus: Minimum number of GPUs required per node
        min_vram: Minimum VRAM per GPU in GB
        max_vram: Maximum VRAM per GPU in GB (to avoid wasting expensive GPUs)
        prefer: "max" for best GPUs first, "min" for minimal sufficient GPUs first

    Returns list of partition names, ordered by preference.
    """
    cmd = ["sinfo", "-h", "-o", "%P|%G|%a|%D"]
    result = _run(cmd, timeout_sec=5)
    if not result["ok"]:
        return []

    partitions = []
    for line in result["stdout"].splitlines():
        parts = line.split("|")
        if len(parts) < 4:
            continue
        partition = parts[0].rstrip("*")  # Remove default marker
        gres = parts[1]
        avail = parts[2]

        # Skip unavailable partitions
        if avail != "up":
            continue

        gpu_type, gpu_count, vram_gb = _parse_gpu_info(gres)

        if gpu_count == 0:
            continue

        # Filter by requirements
        if min_gpus and gpu_count < min_gpus:
            continue
        if min_vram and vram_gb and vram_gb < min_vram:
            continue
        if max_vram and vram_gb and vram_gb > max_vram:
            continue

        partitions.append({
            "partition": partition,
            "gpu_type": gpu_type,
            "gpu_count": gpu_count,
            "vram_gb": vram_gb,
        })

    # Sort by preference
    if prefer == "max":
        # Best first: highest VRAM, then highest GPU count
        partitions.sort(key=lambda x: (-x["vram_gb"], -x["gpu_count"], x["partition"]))
    else:
        # Minimal sufficient: lowest VRAM that meets requirements, then lowest GPU count
        partitions.sort(key=lambda x: (x["vram_gb"] or 999, x["gpu_count"], x["partition"]))

    return [p["partition"] for p in partitions]

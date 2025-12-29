import argparse
import json
import os
import shlex
import subprocess
import sys
import time

from shepherd import blacklist
from shepherd import constants
from shepherd import daemon as daemon_mod
from shepherd import fs
from shepherd import heartbeat
from shepherd import remotes
from shepherd import status
from shepherd import tui as tui_mod


def _get_remote_cmd_prefix(args):
    """Get the command prefix to run shepherd on remote host."""
    if getattr(args, "remote_python", None):
        return args.remote_python
    # Default: use PYTHONPATH with the synced directory
    remote_dir = getattr(args, "remote_dir", "~/.local/lib/shepherd")
    return f"PYTHONPATH={remote_dir} python -m shepherd"


def _run_remote(args, remote_args):
    """Execute shepherd command on remote host via SSH."""
    _auto_sync_if_needed(args)
    remote = args.remote
    prefix = _get_remote_cmd_prefix(args)

    # Build the remote command - keep prefix as-is (may contain env vars)
    # and quote only the shepherd arguments
    quoted_args = " ".join(shlex.quote(p) for p in remote_args)
    if args.json:
        quoted_args = "--json " + quoted_args
    remote_cmd = f"{prefix} {quoted_args}"

    ssh_cmd = ["ssh", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR", remote, remote_cmd]
    result = subprocess.run(ssh_cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        # Filter out SSH warnings
        for line in result.stderr.splitlines():
            if not line.startswith("** "):
                print(line, file=sys.stderr)
    return result.returncode


def _run_remote_interactive(args, remote_args):
    """Execute interactive shepherd command on remote host via SSH with TTY."""
    _auto_sync_if_needed(args)
    remote = args.remote
    prefix = _get_remote_cmd_prefix(args)

    quoted_args = " ".join(shlex.quote(p) for p in remote_args)
    remote_cmd = f"{prefix} {quoted_args}"

    # Use -t for TTY allocation (required for curses TUI)
    ssh_cmd = ["ssh", "-t", "-o", "LogLevel=ERROR", remote, remote_cmd]
    return subprocess.call(ssh_cmd)


def _ensure_daemon(args):
    """Start daemon in background if not already running."""
    if getattr(args, "no_daemon", False):
        return
    if getattr(args, "remote", None):
        # For remote, ensure daemon on remote host
        _ensure_remote_daemon(args)
        return
    if daemon_mod.is_daemon_running():
        return
    # Start daemon in background
    subprocess.Popen(
        [sys.executable, "-m", "shepherd", "daemon"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    # Brief wait for daemon to start
    time.sleep(0.2)


def _ensure_remote_daemon(args):
    """Ensure daemon is running on remote host."""
    remote = args.remote
    prefix = _get_remote_cmd_prefix(args)
    # Check if daemon is running
    check_cmd = ["ssh", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR", remote,
                 f"{prefix} daemon-status --json"]
    result = subprocess.run(check_cmd, capture_output=True, text=True)
    if result.returncode == 0:
        try:
            data = json.loads(result.stdout)
            if data.get("running"):
                return
        except json.JSONDecodeError:
            pass
    # Start daemon on remote - use nohup for proper backgrounding
    remote_dir = getattr(args, "remote_dir", "~/.local/lib/shepherd")
    start_cmd = ["ssh", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR", remote,
                 f"PYTHONPATH={remote_dir} nohup python -m shepherd daemon > /dev/null 2>&1 &"]
    subprocess.run(start_cmd, capture_output=True)
    time.sleep(1)


def cmd_daemon_status(args):
    """Check if daemon is running."""
    if getattr(args, "remote", None):
        return _run_remote(args, ["daemon-status"])
    running = daemon_mod.is_daemon_running()
    if args.json:
        _print_json({"running": running})
    else:
        if running:
            print("Daemon is running")
        else:
            print("Daemon is not running")


def _get_local_code_hash():
    """Get hash of local shepherd source files."""
    import hashlib
    import shepherd
    src_dir = os.path.dirname(shepherd.__file__)

    hasher = hashlib.md5()
    for root, dirs, files in os.walk(src_dir):
        dirs[:] = [d for d in dirs if not d.startswith(('__pycache__', '.'))]
        for fname in sorted(files):
            if fname.endswith('.py'):
                fpath = os.path.join(root, fname)
                try:
                    with open(fpath, 'rb') as f:
                        hasher.update(f.read())
                except IOError:
                    pass
    return hasher.hexdigest()[:12]


def _do_sync(remote, remote_dir, quiet=False):
    """Perform the actual sync to remote host."""
    import shepherd
    src_dir = os.path.dirname(shepherd.__file__)

    # Create remote directory
    mkdir_cmd = ["ssh", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR", remote,
                 f"mkdir -p {remote_dir}"]
    subprocess.run(mkdir_cmd, check=True, capture_output=quiet)

    # Sync code
    rsync_cmd = ["rsync", "-az", "--delete", "-e", "ssh -o LogLevel=ERROR",
                 f"{src_dir}/", f"{remote}:{remote_dir}/shepherd/"]
    result = subprocess.run(rsync_cmd, capture_output=quiet)
    if result.returncode != 0:
        return False

    # Write hash file to remote
    local_hash = _get_local_code_hash()
    hash_cmd = ["ssh", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR", remote,
                f"echo '{local_hash}' > {remote_dir}/.shepherd_hash"]
    subprocess.run(hash_cmd, capture_output=True)

    return True


def _sync_script_to_remote(remote, script_path, remote_dir="~/.local/lib/shepherd"):
    """Sync a script file to the remote host. Returns remote path."""
    expanded = os.path.expanduser(script_path)
    if not os.path.isfile(expanded):
        print(f"Script not found: {expanded}", file=sys.stderr)
        return None

    # Create scripts directory on remote
    scripts_dir = f"{remote_dir}/scripts"
    mkdir_cmd = ["ssh", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR", remote,
                 f"mkdir -p {scripts_dir}"]
    subprocess.run(mkdir_cmd, capture_output=True)

    # Copy script to remote
    basename = os.path.basename(expanded)
    remote_path = f"{scripts_dir}/{basename}"
    scp_cmd = ["scp", "-o", "LogLevel=ERROR", expanded, f"{remote}:{remote_path}"]
    result = subprocess.run(scp_cmd, capture_output=True)
    if result.returncode != 0:
        print(f"Failed to sync script to remote: {result.stderr.decode()}", file=sys.stderr)
        return None

    return remote_path


def _sync_blacklist(args):
    """Sync blacklist from remote to local only (remote is authoritative)."""
    if not getattr(args, "remote", None):
        return
    if getattr(args, "no_sync", False):
        return

    remote = args.remote
    state_dir = "~/.slurm_shepherd"

    # Read remote blacklist (authoritative)
    read_cmd = ["ssh", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR", remote,
                f"cat {state_dir}/blacklist.json 2>/dev/null || echo '{{}}'"]
    result = subprocess.run(read_cmd, capture_output=True, text=True)
    try:
        remote_data = json.loads(result.stdout.strip() or "{}")
    except json.JSONDecodeError:
        remote_data = {}

    remote_nodes = remote_data.get("nodes", {})

    # Prune expired entries
    now = int(time.time())
    for node in list(remote_nodes.keys()):
        expires_at = remote_nodes[node].get("expires_at")
        if expires_at is not None and expires_at <= now:
            del remote_nodes[node]

    # Update local with remote data (remote is authoritative for this remote)
    merged_data = {"nodes": remote_nodes, "updated_at": now}
    blacklist.save_blacklist(merged_data)


def _auto_sync_if_needed(args):
    """Auto-sync to remote if code has changed."""
    if not getattr(args, "remote", None):
        return
    if getattr(args, "no_sync", False):
        return

    remote = args.remote
    remote_dir = getattr(args, "remote_dir", "~/.local/lib/shepherd")

    # Sync blacklist
    _sync_blacklist(args)

    # Get local hash
    local_hash = _get_local_code_hash()

    # Get remote hash
    hash_cmd = ["ssh", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR", remote,
                f"cat {remote_dir}/.shepherd_hash 2>/dev/null || echo ''"]
    result = subprocess.run(hash_cmd, capture_output=True, text=True)
    remote_hash = result.stdout.strip()

    if local_hash != remote_hash:
        print(f"Auto-syncing to {remote}...", file=sys.stderr)
        if _do_sync(remote, remote_dir, quiet=True):
            print(f"Synced ({local_hash})", file=sys.stderr)
        else:
            print("Sync failed", file=sys.stderr)


def cmd_sync(args):
    """Sync shepherd code to remote host and optionally restart daemon."""
    if not args.remote:
        print("--remote is required for sync", file=sys.stderr)
        return 1

    remote_dir = args.remote_dir
    remote = args.remote

    # Sync code
    if not _do_sync(remote, remote_dir, quiet=False):
        print("Sync failed", file=sys.stderr)
        return 1

    local_hash = _get_local_code_hash()
    print(f"Synced to {remote}:{remote_dir}/shepherd/ ({local_hash})")

    # Clear pycache
    clear_cmd = ["ssh", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR", remote,
                 f"rm -rf {remote_dir}/shepherd/__pycache__"]
    subprocess.run(clear_cmd, capture_output=True)
    print("Cleared pycache")

    # Restart daemon if requested or by default
    if not args.no_restart:
        print("Restarting daemon...")
        # Kill existing daemon
        kill_cmd = ["ssh", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR", remote,
                    "pkill -9 -f 'python -m shepherd daemon'; rm -f ~/.slurm_shepherd/daemon.pid"]
        subprocess.run(kill_cmd, capture_output=True)

        # Start new daemon - expand ~ in PYTHONPATH by remote shell
        remote_dir = args.remote_dir
        prefix = _get_remote_cmd_prefix(args)
        start_cmd = ["ssh", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR", remote,
                     f"PYTHONPATH={remote_dir} nohup python -m shepherd daemon > /dev/null 2>&1 &"]
        subprocess.run(start_cmd, capture_output=True)
        time.sleep(2)

        # Verify daemon is running (--json is a global flag, must come before subcommand)
        check_cmd = ["ssh", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR", remote,
                     f"{prefix} --json daemon-status"]
        result = subprocess.run(check_cmd, capture_output=True, text=True)
        try:
            data = json.loads(result.stdout)
            if data.get("running"):
                print("Daemon restarted successfully")
            else:
                print("Warning: Daemon may not have started", file=sys.stderr)
        except json.JSONDecodeError:
            print("Warning: Could not verify daemon status", file=sys.stderr)

    return 0


def _now():
    return int(time.time())


def _load_run(run_id):
    meta = fs.read_json(fs.run_file(run_id, constants.META_FILENAME))
    control = fs.read_json(fs.run_file(run_id, constants.CONTROL_FILENAME))
    ended = fs.read_json(fs.run_file(run_id, constants.ENDED_FILENAME))
    final = fs.read_json(fs.run_file(run_id, constants.FINAL_FILENAME))
    hb_ts = heartbeat.read_heartbeat(fs.run_file(run_id, constants.HEARTBEAT_FILENAME))
    slurm_state = None
    if isinstance(meta, dict):
        slurm_state = meta.get("slurm_state")
    return {
        "run_id": run_id,
        "meta": meta,
        "control": control,
        "ended": ended,
        "final": final,
        "heartbeat_ts": hb_ts,
        "slurm_state": slurm_state,
    }


def _summarize_run(run):
    meta = run["meta"] if isinstance(run["meta"], dict) else {}
    status_value = status.compute_status(
        meta,
        run["control"] if isinstance(run["control"], dict) else None,
        run["ended"] if isinstance(run["ended"], dict) else None,
        run["final"] if isinstance(run["final"], dict) else None,
        run["heartbeat_ts"],
        run["slurm_state"],
    )
    summary = {
        "run_id": run["run_id"],
        "status": status_value,
        "run_mode": meta.get("run_mode"),
        "slurm_job_id": meta.get("slurm_job_id"),
        "last_heartbeat": run["heartbeat_ts"],
        "restart_count": meta.get("restart_count"),
        "restart_reason": meta.get("restart_reason"),
    }
    # Add partition info if using fallback
    if meta.get("partition_fallback"):
        summary["current_partition"] = meta.get("current_partition")
        summary["partition_index"] = meta.get("current_partition_index", 0)
    return summary


def cmd_list(args):
    if getattr(args, "remote", None):
        _ensure_daemon(args)
        return _run_remote(args, ["list"])
    fs.ensure_dirs()
    _ensure_daemon(args)
    runs = [_summarize_run(_load_run(run_id)) for run_id in fs.list_runs()]
    if args.json:
        _print_json({"runs": runs})
    else:
        if not runs:
            print("No runs found")
            return
        for run in runs:
            parts = [run["run_id"], run["status"]]
            if run.get("current_partition"):
                parts.append(f"partition={run['current_partition']}")
            elif run.get("partition_index") is not None:
                parts.append(f"partition_idx={run['partition_index']}")
            if run.get("slurm_job_id"):
                parts.append(f"job={run['slurm_job_id']}")
            if run.get("restart_count"):
                parts.append(f"restarts={run['restart_count']}")
            if run.get("restart_reason"):
                parts.append(f"reason={run['restart_reason']}")
            print(" ".join(parts))


def cmd_status(args):
    if getattr(args, "remote", None):
        _ensure_daemon(args)
        return _run_remote(args, ["status", "--run-id", args.run_id])
    fs.ensure_dirs()
    _ensure_daemon(args)
    run = _load_run(args.run_id)
    summary = _summarize_run(run)
    payload = {"summary": summary, "meta": run["meta"], "control": run["control"]}
    if args.json:
        _print_json(payload)
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))


def _update_control(run_id, updates):
    path = fs.run_file(run_id, constants.CONTROL_FILENAME)
    current = fs.read_json(path)
    if current is None or isinstance(current, dict) and current.get("_corrupt"):
        current = {}
    if "config_overrides" in updates:
        merged = dict(current.get("config_overrides") or {})
        merged.update(updates["config_overrides"])
        updates = dict(updates)
        updates["config_overrides"] = merged
    current.update(updates)
    current["updated_at"] = _now()
    fs.atomic_write_json(path, current)


def cmd_control(args):
    if getattr(args, "remote", None):
        _ensure_daemon(args)
        remote_args = ["control", args.op, "--run-id", args.run_id]
        if args.node:
            remote_args.extend(["--node", args.node])
        if args.ttl:
            remote_args.extend(["--ttl", str(args.ttl)])
        if args.reason:
            remote_args.extend(["--reason", args.reason])
        if args.key:
            remote_args.extend(["--key", args.key])
        if args.value:
            remote_args.extend(["--value", args.value])
        return _run_remote(args, remote_args)
    fs.ensure_dirs()
    _ensure_daemon(args)
    if args.op in {"pause", "unpause", "stop", "restart", "start"}:
        updates = {}
        if args.op == "pause":
            updates["paused"] = True
        elif args.op == "unpause":
            updates["paused"] = False
        elif args.op == "stop":
            updates["stop_requested"] = True
        elif args.op == "restart":
            updates["restart_requested"] = True
            updates["paused"] = False
            updates["stop_requested"] = False
        elif args.op == "start":
            updates["paused"] = False
            updates["stop_requested"] = False
        _update_control(args.run_id, updates)
        return _emit_ok(args)
    if args.op == "blacklist-add":
        blacklist.add_node(args.node, ttl_sec=args.ttl, reason=args.reason)
        return _emit_ok(args)
    if args.op == "blacklist-remove":
        blacklist.remove_node(args.node)
        return _emit_ok(args)
    if args.op == "config-set":
        updates = {"config_overrides": {args.key: args.value}}
        _update_control(args.run_id, updates)
        return _emit_ok(args)
    raise SystemExit("Unknown control op")


def cmd_daemon(args):
    daemon = daemon_mod.ShepherdDaemon(poll_interval_sec=args.interval)
    return daemon.run()


def cmd_tui(args):
    if getattr(args, "remote", None):
        _ensure_daemon(args)
        return _run_remote_interactive(args, ["tui"])
    fs.ensure_dirs()
    _ensure_daemon(args)
    return tui_mod.run_tui()


def cmd_new(args):
    from shepherd import slurm

    script = args.script

    # Handle remote execution early - let remote host do partition discovery
    if getattr(args, "remote", None):
        # Sync script to remote host first
        remote_dir = getattr(args, "remote_dir", "~/.local/lib/shepherd")
        remote_script = _sync_script_to_remote(args.remote, script, remote_dir)
        if not remote_script:
            return 1

        # Build remote args, passing through GPU/partition options
        # Remote host will do the partition discovery
        remote_args = ["new", remote_script]
        if args.run_id:
            remote_args.extend(["--run-id", args.run_id])
        if args.mode:
            remote_args.extend(["--mode", args.mode])
        if args.gpus:
            remote_args.extend(["--gpus", str(args.gpus)])
        if args.min_vram:
            remote_args.extend(["--min-vram", str(args.min_vram)])
        if args.max_vram:
            remote_args.extend(["--max-vram", str(args.max_vram)])
        if args.prefer:
            remote_args.extend(["--prefer", args.prefer])
        if args.partitions:
            remote_args.extend(["--partitions", args.partitions])
        if args.no_auto_partitions:
            remote_args.append("--no-auto-partitions")
        if args.no_blacklist:
            remote_args.append("--no-blacklist")
        return _run_remote(args, remote_args)

    # Parse #SHEPHERD directives from script (CLI args override)
    directives = slurm.parse_shepherd_directives(script)

    # Helper to get value with CLI override
    def get_val(cli_val, directive_key, default=None):
        if cli_val is not None:
            return cli_val
        return directives.get(directive_key, default)

    # Derive run_id from script name if not provided
    run_id = args.run_id or directives.get("run_id")
    if not run_id:
        basename = os.path.basename(script)
        run_id = os.path.splitext(basename)[0]

    # GPU/partition settings
    gpus = get_val(args.gpus, "gpus")
    min_vram = get_val(args.min_vram, "min_vram")
    max_vram = get_val(args.max_vram, "max_vram")
    # Default to 1 GPU if VRAM constraints are specified
    if (min_vram or max_vram) and not gpus:
        gpus = 1
    prefer = args.prefer if args.prefer != "max" else directives.get("prefer", "max")
    mode = get_val(args.mode, "run_mode", "run_once")

    # Auto-discover GPU partitions if not specified and not disabled
    partitions_str = args.partitions or directives.get("partitions")
    if partitions_str:
        partition_list = [p.strip() for p in partitions_str.split(",") if p.strip()]
    elif args.no_auto_partitions:
        partition_list = []
    else:
        partition_list = slurm.discover_gpu_partitions(
            min_gpus=gpus,
            min_vram=min_vram,
            max_vram=max_vram,
            prefer=prefer,
        )

    fs.ensure_dirs()

    run_dir = os.path.join(constants.RUNS_DIR, run_id)
    if os.path.exists(run_dir):
        print(f"Run '{run_id}' already exists", file=sys.stderr)
        return 1

    os.makedirs(run_dir, exist_ok=True)
    meta = {
        "run_id": run_id,
        "run_mode": mode,
        "sbatch_script": script,
        "created_at": _now(),
    }

    # Add GPU config
    if gpus:
        meta["gpus"] = gpus

    # Add no_blacklist flag
    if args.no_blacklist:
        meta["no_blacklist"] = True

    # Add partition fallback
    if partition_list:
        meta["partition_fallback"] = {"partitions": partition_list}

    # Add optional settings from directives
    optional_fields = [
        "max_retries",
        "keep_alive_sec",
        "heartbeat_interval_sec",
        "heartbeat_grace_sec",
        "backoff_base_sec",
        "backoff_max_sec",
        "blacklist_ttl_sec",
    ]
    for field in optional_fields:
        if field in directives:
            meta[field] = directives[field]

    fs.atomic_write_json(os.path.join(run_dir, constants.META_FILENAME), meta)
    if args.json:
        _print_json({"created": run_id, "partitions": partition_list})
    else:
        print(f"Created run: {run_id}")
        if partition_list:
            print(f"Partitions: {', '.join(partition_list)}")
    _ensure_daemon(args)
    return 0


def _get_nodes_list():
    """Get merged list of nodes from sinfo and blacklist."""
    from shepherd import slurm

    nodes = slurm.list_nodes()
    bl_data = blacklist.load_blacklist()
    blacklist.prune_expired(bl_data)
    bl_nodes = bl_data.get("nodes", {})

    results = []
    for node_info in nodes:
        node_name = node_info["node"]
        entry = {
            "node": node_name,
            "partition": node_info["partition"],
            "state": node_info["state"],
            "gpu_type": node_info["gpu_type"],
            "gpu_count": node_info["gpu_count"],
            "vram_gb": node_info["vram_gb"],
            "gpus_available": node_info.get("gpus_available", 0),
            "blacklisted": node_name in bl_nodes,
        }
        if node_name in bl_nodes:
            bl_entry = bl_nodes[node_name]
            entry["blacklist_reason"] = bl_entry.get("reason")
            entry["blacklist_expires_at"] = bl_entry.get("expires_at")
            entry["blacklist_added_at"] = bl_entry.get("added_at")
        results.append(entry)

    # Include blacklisted nodes not in sinfo
    seen_nodes = {n["node"] for n in nodes}
    for node_name, bl_entry in bl_nodes.items():
        if node_name not in seen_nodes:
            results.append({
                "node": node_name,
                "partition": None,
                "state": "unknown",
                "gpu_type": None,
                "gpu_count": 0,
                "vram_gb": 0,
                "gpus_available": 0,
                "blacklisted": True,
                "blacklist_reason": bl_entry.get("reason"),
                "blacklist_expires_at": bl_entry.get("expires_at"),
                "blacklist_added_at": bl_entry.get("added_at"),
            })

    # Sort: blacklisted first, then by partition, state, name
    state_priority = {"idle": 0, "mixed": 1, "allocated": 2, "down": 3, "drain": 4, "unknown": 5}
    results.sort(key=lambda x: (
        not x["blacklisted"],
        x["partition"] or "zzz",
        state_priority.get(x["state"], 99),
        x["node"],
    ))
    return results


def _print_nodes_list(results, numbered=False):
    """Print nodes list, optionally with numbers for selection."""
    if not results:
        print("No nodes found")
        return

    current_partition = None
    for i, entry in enumerate(results):
        partition = entry["partition"] or "(unknown)"
        if partition != current_partition:
            if current_partition is not None:
                print()
            current_partition = partition
            print(f"── {partition} ──")

        # Build GPU info
        gpu_info = ""
        if entry["gpu_count"]:
            gpu_info = f"{entry['gpu_count']}× {entry['gpu_type'] or 'gpu'}"
            if entry["vram_gb"]:
                gpu_info += f" {entry['vram_gb']}GB"

        # Build status
        state = entry["state"]
        if entry["blacklisted"]:
            bl_parts = ["⛔ BANNED"]
            if entry.get("blacklist_reason"):
                bl_parts.append(entry["blacklist_reason"])
            if entry.get("blacklist_expires_at"):
                remaining = entry["blacklist_expires_at"] - _now()
                if remaining > 0:
                    if remaining < 3600:
                        bl_parts.append(f"{remaining // 60}m left")
                    else:
                        bl_parts.append(f"{remaining // 3600}h left")
            status = " ".join(bl_parts)
        else:
            state_icons = {"idle": "●", "mixed": "◐", "allocated": "○", "down": "✖", "drain": "⚠"}
            icon = state_icons.get(state, "?")
            status = f"{icon} {state}"

        if numbered:
            print(f"  [{i:2}] {entry['node']:<16} {gpu_info:<18} {status}")
        else:
            print(f"  {entry['node']:<18} {gpu_info:<20} {status}")


def _interactive_nodes_simple():
    """Simple text-based interactive node management (fallback)."""
    while True:
        results = _get_nodes_list()
        print()
        _print_nodes_list(results, numbered=True)
        print()
        print("Enter node number to ban/unban, 'q' to quit:")

        try:
            choice = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if choice.lower() in ("q", "quit", "exit", ""):
            break

        try:
            idx = int(choice)
            if idx < 0 or idx >= len(results):
                print(f"Invalid selection: {idx}")
                continue
        except ValueError:
            print(f"Invalid input: {choice}")
            continue

        node = results[idx]
        node_name = node["node"]

        if node["blacklisted"]:
            print(f"\n{node_name} is currently BANNED. Unban? [y/N]")
            try:
                if input("> ").strip().lower() == "y":
                    blacklist.remove_node(node_name)
                    print(f"Unbanned {node_name}")
            except (EOFError, KeyboardInterrupt):
                print()
        else:
            print(f"\nBan {node_name}? Enter reason (empty to cancel):")
            try:
                reason = input("> ").strip()
                if reason:
                    print("TTL in seconds (empty for permanent):")
                    ttl_str = input("> ").strip()
                    ttl = int(ttl_str) if ttl_str else None
                    blacklist.add_node(node_name, ttl_sec=ttl, reason=reason)
                    print(f"Banned {node_name}")
            except (EOFError, KeyboardInterrupt):
                print()
            except ValueError:
                print("Invalid TTL")


def _interactive_nodes():
    """Interactive node management TUI with modern design."""
    import curses
    import sys

    # Check if we have a proper TTY
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        return _interactive_nodes_simple()

    def run_tui(stdscr):
        curses.curs_set(0)
        curses.use_default_colors()

        # Modern color scheme
        curses.init_pair(1, 16, 255)   # Selected: dark on light
        curses.init_pair(2, 203, -1)   # Banned: soft red
        curses.init_pair(3, 114, -1)   # Idle: soft green
        curses.init_pair(4, 220, -1)   # Mixed: soft yellow
        curses.init_pair(5, 75, -1)    # Header: soft blue
        curses.init_pair(6, 245, -1)   # Dim: gray
        curses.init_pair(7, 255, 237)  # Status bar: light on dark

        # Fallback for terminals without 256 colors
        if curses.COLORS < 256:
            curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE)
            curses.init_pair(2, curses.COLOR_RED, -1)
            curses.init_pair(3, curses.COLOR_GREEN, -1)
            curses.init_pair(4, curses.COLOR_YELLOW, -1)
            curses.init_pair(5, curses.COLOR_CYAN, -1)
            curses.init_pair(6, curses.COLOR_WHITE, -1)
            curses.init_pair(7, curses.COLOR_BLACK, curses.COLOR_WHITE)

        COL_SEL = curses.color_pair(1)
        COL_BAN = curses.color_pair(2)
        COL_OK = curses.color_pair(3)
        COL_WARN = curses.color_pair(4)
        COL_HEAD = curses.color_pair(5)
        COL_DIM = curses.color_pair(6)
        COL_BAR = curses.color_pair(7)

        selected = 0
        scroll_offset = 0
        message = ""
        message_type = ""  # "success", "error", ""
        input_mode = None
        input_buffer = ""
        ban_reason = ""
        smoke_results = {}  # {node: "OK" | "FAIL" | "SKIP" | "..." | error}
        smoke_running = False
        smoke_thread = None

        def run_smoke_tests(nodes):
            """Run smoke tests on all nodes via srun."""
            nonlocal smoke_running, smoke_results
            import threading
            import concurrent.futures

            conda_script = remotes.get_local_conda_activation_script()
            smoke_script = r'''
import sys
def try_torch():
    import torch
    if not torch.cuda.is_available():
        raise RuntimeError("no cuda")
    torch.randn(1, device="cuda")
def try_cupy():
    import cupy
    cupy.random.randn(1)
tested = False
for fn in (try_torch, try_cupy):
    try:
        fn()
        print("OK")
        sys.exit(0)
    except ImportError:
        continue
    except Exception as e:
        print(f"FAIL:{e}")
        sys.exit(2)
print("SKIP")
'''
            def test_node(node_info):
                node_name = node_info["node"]
                partition = node_info.get("partition")
                smoke_results[node_name] = "..."
                try:
                    # Build shell script with conda activation using heredoc for Python
                    if conda_script:
                        full_script = f"""{conda_script.strip()}
python << 'SMOKE_EOF'
{smoke_script}
SMOKE_EOF"""
                    else:
                        full_script = f"""python << 'SMOKE_EOF'
{smoke_script}
SMOKE_EOF"""

                    # Build srun command with partition if available
                    srun_cmd = ["srun", "--nodelist=" + node_name, "-n1", "--time=1", "--gres=gpu:1"]
                    if partition:
                        srun_cmd.extend(["--partition=" + partition])
                    # Use login shell (-l) to get conda/mamba in PATH
                    srun_cmd.extend(["bash", "-l", "-c", full_script])

                    result = subprocess.run(
                        srun_cmd,
                        capture_output=True, text=True, timeout=20
                    )
                    output = result.stdout.strip()
                    stderr = result.stderr.strip()

                    if output.startswith("OK"):
                        smoke_results[node_name] = "OK"
                    elif output.startswith("FAIL"):
                        smoke_results[node_name] = "FAIL"
                    elif output.startswith("SKIP"):
                        smoke_results[node_name] = "SKIP"
                    elif result.returncode != 0 or "queued and waiting" in stderr:
                        # Categorize common SLURM errors
                        if "queued and waiting" in stderr:
                            smoke_results[node_name] = "BUSY"
                        elif "Unable to allocate" in stderr:
                            if "qos" in stderr.lower() or "priority" in stderr.lower():
                                smoke_results[node_name] = "NO_QOS"
                            elif "resources" in stderr.lower():
                                smoke_results[node_name] = "BUSY"
                            else:
                                smoke_results[node_name] = "NO_ALLOC"
                        elif "not in this partition" in stderr:
                            smoke_results[node_name] = "WRONG_PART"
                        else:
                            # Show abbreviated error
                            err_hint = stderr.split('\n')[-1][:15] if stderr else f"rc={result.returncode}"
                            smoke_results[node_name] = f"ERR:{err_hint}"
                    else:
                        smoke_results[node_name] = "ERR"
                except subprocess.TimeoutExpired:
                    smoke_results[node_name] = "TIMEOUT"
                except Exception as e:
                    smoke_results[node_name] = f"ERR:{str(e)[:15]}"

            # Run tests in parallel with limited concurrency
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                executor.map(test_node, nodes)

            smoke_running = False

        while True:
            stdscr.erase()
            height, width = stdscr.getmaxyx()
            max_width = min(width, 100)

            results = _get_nodes_list()
            total = len(results)
            banned = sum(1 for r in results if r["blacklisted"])

            # ═══ Header ═══
            title = " NODES "
            header_line = "─" * max_width
            mid = (max_width - len(title)) // 2
            header_line = header_line[:mid] + title + header_line[mid + len(title):]
            stdscr.attron(COL_HEAD | curses.A_BOLD)
            stdscr.addstr(0, 0, header_line)
            stdscr.attroff(COL_HEAD | curses.A_BOLD)

            # Subtitle with counts
            subtitle = f"{total} nodes"
            if banned:
                subtitle += f" · {banned} banned"
            stdscr.attron(COL_DIM)
            stdscr.addstr(1, 2, subtitle)
            stdscr.attroff(COL_DIM)

            # ═══ Column Headers ═══
            col_header = f"{'NODE':<22}{'PARTITION':<14}{'GPU':<20}{'STATUS'}"
            stdscr.attron(COL_DIM)
            stdscr.addstr(3, 2, col_header[:max_width-4])
            stdscr.attroff(COL_DIM)

            # ═══ Node List ═══
            list_start = 5
            list_height = height - 9

            if not results:
                empty_msg = "No nodes found"
                stdscr.attron(COL_DIM)
                stdscr.addstr(list_start + 2, (max_width - len(empty_msg)) // 2, empty_msg)
                stdscr.attroff(COL_DIM)
            else:
                # Adjust scroll
                if selected < scroll_offset:
                    scroll_offset = selected
                elif selected >= scroll_offset + list_height:
                    scroll_offset = selected - list_height + 1

                visible = results[scroll_offset:scroll_offset + list_height]

                for i, entry in enumerate(visible):
                    idx = scroll_offset + i
                    y = list_start + i
                    if y >= height - 4:
                        break

                    node_name = entry["node"][:20]
                    partition = (entry["partition"] or "—")[:12]

                    gpu_info = ""
                    if entry["gpu_count"]:
                        gpu_info = f"{entry['gpu_count']}× {entry['gpu_type'] or 'gpu'}"
                        if entry["vram_gb"]:
                            gpu_info += f" {entry['vram_gb']}G"
                    gpu_info = gpu_info[:18] if gpu_info else "—"

                    # Status with icon
                    if entry["blacklisted"]:
                        reason = entry.get("blacklist_reason", "")
                        status = f"✗ banned {reason}".strip()
                        status_col = COL_BAN
                    else:
                        state = entry["state"]
                        icons = {"idle": "○", "mixed": "◐", "allocated": "●", "down": "✗", "drain": "!"}
                        status = f"{icons.get(state, '?')} {state}"
                        status_col = COL_OK if state == "idle" else (COL_WARN if state in ("mixed", "allocated") else COL_BAN)

                    # Smoke test result
                    smoke = smoke_results.get(entry["node"], "")
                    if smoke:
                        smoke_icons = {"OK": "✓", "FAIL": "✗", "SKIP": "○", "...": "◌", "TIMEOUT": "⏱",
                                       "NO_QOS": "⊘", "BUSY": "●", "NO_ALLOC": "⊘", "WRONG_PART": "⊘"}
                        smoke_display = smoke_icons.get(smoke, "!")
                        if smoke == "OK":
                            smoke_col = COL_OK
                        elif smoke in ("FAIL", "TIMEOUT") or smoke.startswith("ERR"):
                            smoke_col = COL_BAN
                        else:
                            smoke_col = COL_DIM  # NO_QOS, BUSY, SKIP, etc - not failures

                    # Build line
                    is_selected = (idx == selected)
                    prefix = "▸ " if is_selected else "  "

                    if is_selected:
                        stdscr.attron(COL_SEL | curses.A_BOLD)
                        line = f"{prefix}{node_name:<22}{partition:<14}{gpu_info:<20}{status}"
                        if smoke:
                            line += f"  {smoke_display} {smoke}"
                        stdscr.addstr(y, 0, " " * max_width)
                        stdscr.addstr(y, 0, line[:max_width-1])
                        stdscr.attroff(COL_SEL | curses.A_BOLD)
                    else:
                        stdscr.addstr(y, 0, prefix)
                        stdscr.addstr(node_name.ljust(22))
                        stdscr.attron(COL_DIM)
                        stdscr.addstr(partition.ljust(14))
                        stdscr.attroff(COL_DIM)
                        stdscr.addstr(gpu_info.ljust(20))
                        stdscr.attron(status_col)
                        stdscr.addstr(status)
                        stdscr.attroff(status_col)
                        if smoke:
                            stdscr.addstr("  ")
                            stdscr.attron(smoke_col)
                            stdscr.addstr(f"{smoke_display} {smoke}")
                            stdscr.attroff(smoke_col)

                # Scroll indicator
                if total > list_height:
                    scroll_pct = (scroll_offset + list_height / 2) / total
                    indicator_h = max(1, list_height * list_height // total)
                    indicator_y = int((list_height - indicator_h) * scroll_pct)
                    for sy in range(list_height):
                        char = "┃" if indicator_y <= sy < indicator_y + indicator_h else "│"
                        stdscr.attron(COL_DIM)
                        stdscr.addstr(list_start + sy, max_width - 1, char)
                        stdscr.attroff(COL_DIM)

            # ═══ Input / Message Area ═══
            msg_y = height - 4
            stdscr.attron(COL_DIM)
            stdscr.addstr(msg_y, 0, "─" * max_width)
            stdscr.attroff(COL_DIM)

            if input_mode:
                if input_mode == "reason":
                    prompt = "Ban reason: "
                else:
                    prompt = "TTL seconds (empty=permanent): "
                stdscr.addstr(msg_y + 1, 2, prompt)
                stdscr.attron(curses.A_BOLD)
                stdscr.addstr(input_buffer)
                stdscr.attroff(curses.A_BOLD)
                stdscr.addstr("▌")
            elif message:
                msg_col = COL_OK if message_type == "success" else (COL_BAN if message_type == "error" else COL_DIM)
                stdscr.attron(msg_col)
                stdscr.addstr(msg_y + 1, 2, message[:max_width-4])
                stdscr.attroff(msg_col)

            # ═══ Help Bar ═══
            help_y = height - 2
            if input_mode:
                keys = [("Enter", "confirm"), ("Esc", "cancel")]
            elif smoke_running:
                keys = [("↑↓", "navigate"), ("s", "running..."), ("r", "refresh"), ("q", "quit")]
            else:
                keys = [("↑↓", "navigate"), ("Enter", "ban/unban"), ("s", "smoke test"), ("r", "refresh"), ("q", "quit")]

            stdscr.attron(COL_BAR)
            stdscr.addstr(help_y, 0, " " * max_width)
            x = 1
            for key, desc in keys:
                if x + len(key) + len(desc) + 4 > max_width:
                    break
                stdscr.addstr(help_y, x, f" {key} ")
                stdscr.attroff(COL_BAR)
                stdscr.attron(COL_DIM)
                stdscr.addstr(f"{desc}  ")
                stdscr.attroff(COL_DIM)
                stdscr.attron(COL_BAR)
                x += len(key) + len(desc) + 5
            stdscr.attroff(COL_BAR)

            stdscr.refresh()

            # ═══ Input Handling ═══
            # Use timeout during smoke tests for live updates
            if smoke_running:
                stdscr.timeout(500)  # 500ms refresh
            else:
                stdscr.timeout(-1)  # Blocking

            try:
                key = stdscr.getch()
            except KeyboardInterrupt:
                break

            if key == -1:  # Timeout, just refresh
                continue

            if input_mode:
                if key == 27:  # Escape
                    input_mode = None
                    input_buffer = ""
                    ban_reason = ""
                    message = "Cancelled"
                    message_type = ""
                elif key in (10, 13):  # Enter
                    if input_mode == "reason":
                        if input_buffer.strip():
                            ban_reason = input_buffer.strip()
                            input_buffer = ""
                            input_mode = "ttl"
                        else:
                            input_mode = None
                            message = "Cancelled"
                            message_type = ""
                    elif input_mode == "ttl":
                        ttl = None
                        if input_buffer.strip():
                            try:
                                ttl = int(input_buffer.strip())
                            except ValueError:
                                pass
                        node_name = results[selected]["node"]
                        blacklist.add_node(node_name, ttl_sec=ttl, reason=ban_reason)
                        message = f"Banned {node_name}"
                        message_type = "success"
                        input_mode = None
                        input_buffer = ""
                        ban_reason = ""
                elif key in (8, 127, curses.KEY_BACKSPACE):
                    input_buffer = input_buffer[:-1]
                elif 32 <= key <= 126:
                    input_buffer += chr(key)
            else:
                message = ""
                message_type = ""
                if key in (ord('q'), ord('Q'), 27):
                    break
                elif key in (ord('r'), ord('R')):
                    message = "Refreshed"
                    message_type = "success"
                elif key in (ord('s'), ord('S')):
                    if not smoke_running:
                        import threading
                        smoke_running = True
                        smoke_results.clear()
                        # Fetch fresh node list to get ALL nodes, not just visible
                        all_nodes = _get_nodes_list()
                        # Filter: must be up, have GPUs, and have at least 1 GPU available
                        testable_nodes = [
                            r for r in all_nodes
                            if r["state"] in ("idle", "mixed")
                            and r.get("gpu_count", 0) > 0
                            and r.get("gpus_available", 0) > 0
                        ]
                        # Mark fully allocated nodes as BUSY without testing
                        for r in all_nodes:
                            if r["state"] == "allocated" or r.get("gpus_available", 0) == 0:
                                if r.get("gpu_count", 0) > 0:
                                    smoke_results[r["node"]] = "BUSY"
                        message = f"Running smoke tests on {len(testable_nodes)} nodes..."
                        message_type = ""
                        smoke_thread = threading.Thread(target=run_smoke_tests, args=(testable_nodes,), daemon=True)
                        smoke_thread.start()
                elif key == curses.KEY_UP or key == ord('k'):
                    if results and selected > 0:
                        selected -= 1
                elif key == curses.KEY_DOWN or key == ord('j'):
                    if results and selected < len(results) - 1:
                        selected += 1
                elif key == ord('g'):
                    selected = 0
                elif key == ord('G'):
                    if results:
                        selected = len(results) - 1
                elif key == curses.KEY_PPAGE or key == 21:
                    selected = max(0, selected - list_height // 2)
                elif key == curses.KEY_NPAGE or key == 4:
                    if results:
                        selected = min(len(results) - 1, selected + list_height // 2)
                elif key in (10, 13, ord(' ')):
                    if results:
                        node = results[selected]
                        if node["blacklisted"]:
                            blacklist.remove_node(node["node"])
                            message = f"Unbanned {node['node']}"
                            message_type = "success"
                        else:
                            input_mode = "reason"
                            input_buffer = ""

    try:
        curses.wrapper(run_tui)
    except curses.error:
        return _interactive_nodes_simple()


def cmd_nodes(args):
    """List nodes, show blacklist status, ban/unban nodes."""
    from shepherd import slurm

    op = args.op or "list"

    # Interactive mode for remote
    if getattr(args, "remote", None):
        if op == "interactive":
            return _run_remote_interactive(args, ["nodes", "interactive"])
        remote_args = ["nodes", op]
        if args.node:
            remote_args.extend(["--node", args.node])
        if args.ttl:
            remote_args.extend(["--ttl", str(args.ttl)])
        if args.reason:
            remote_args.extend(["--reason", args.reason])
        return _run_remote(args, remote_args)

    fs.ensure_dirs()

    if op == "interactive":
        return _interactive_nodes()

    if op == "list":
        results = _get_nodes_list()
        if args.json:
            _print_json({"nodes": results})
        else:
            _print_nodes_list(results)

    elif op == "ban":
        if not args.node:
            print("--node is required for ban", file=sys.stderr)
            return 1
        blacklist.add_node(args.node, ttl_sec=args.ttl, reason=args.reason)
        return _emit_ok(args)

    elif op == "unban":
        if not args.node:
            print("--node is required for unban", file=sys.stderr)
            return 1
        blacklist.remove_node(args.node)
        return _emit_ok(args)

    else:
        print(f"Unknown operation: {op}", file=sys.stderr)
        return 1

    return 0


def cmd_config(args):
    """Manage remote configuration (conda env, etc.)."""
    remote = getattr(args, "remote", None)
    op = args.op

    if op == "list":
        if remote:
            config = remotes.get_remote_config(remote)
            if args.json:
                _print_json({"remote": remote, "config": config})
            else:
                if config:
                    print(f"Config for {remote}:")
                    for k, v in config.items():
                        print(f"  {k}: {v}")
                else:
                    print(f"No config for {remote}")
        else:
            all_remotes = remotes.load_remotes()
            if args.json:
                _print_json({"remotes": all_remotes})
            else:
                if all_remotes:
                    for name, config in all_remotes.items():
                        print(f"{name}:")
                        for k, v in config.items():
                            print(f"  {k}: {v}")
                else:
                    print("No remote configurations")
        return 0

    if not remote:
        print("--remote is required for get/set operations", file=sys.stderr)
        return 1

    if op == "get":
        if not args.key:
            print("key is required for get", file=sys.stderr)
            return 1
        config = remotes.get_remote_config(remote)
        value = config.get(args.key)
        if args.json:
            _print_json({"remote": remote, "key": args.key, "value": value})
        else:
            if value is not None:
                print(value)
            else:
                print(f"{args.key} not set for {remote}")
        return 0

    if op == "set":
        if not args.key:
            print("key is required for set", file=sys.stderr)
            return 1
        if args.value is None:
            print("value is required for set", file=sys.stderr)
            return 1
        # Save locally
        remotes.set_remote_config(remote, args.key, args.value)
        # Also push to remote's config.json
        _sync_config_to_remote(remote, args.key, args.value)
        if args.json:
            _print_json({"ok": True, "remote": remote, "key": args.key, "value": args.value})
        else:
            print(f"Set {args.key}={args.value} for {remote}")
        return 0

    return 0


def _sync_config_to_remote(remote, key, value):
    """Push a config value to the remote's config.json."""
    state_dir = "~/.slurm_shepherd"
    # Read existing remote config
    read_cmd = ["ssh", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR", remote,
                f"cat {state_dir}/config.json 2>/dev/null || echo '{{}}'"]
    result = subprocess.run(read_cmd, capture_output=True, text=True)
    try:
        remote_config = json.loads(result.stdout.strip() or "{}")
    except json.JSONDecodeError:
        remote_config = {}

    # Update and write back
    remote_config[key] = value
    config_json = json.dumps(remote_config)
    write_cmd = ["ssh", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR", remote,
                 f"mkdir -p {state_dir} && cat > {state_dir}/config.json"]
    subprocess.run(write_cmd, input=config_json, text=True, capture_output=True)


def cmd_logs(args):
    """View SLURM stdout/stderr logs for a run."""
    run_id = args.run_id
    filename = constants.STDERR_FILENAME if args.stderr else constants.STDOUT_FILENAME

    if getattr(args, "remote", None):
        # Remote log viewing
        remote_args = ["logs", "--run-id", run_id]
        if args.stderr:
            remote_args.append("--stderr")
        if args.follow:
            remote_args.append("--follow")
        if args.lines != 50:
            remote_args.extend(["-n", str(args.lines)])
        if args.follow:
            return _run_remote_interactive(args, remote_args)
        return _run_remote(args, remote_args)

    log_path = fs.run_file(run_id, filename)

    if not os.path.exists(log_path):
        print(f"Log file not found: {log_path}", file=sys.stderr)
        print("(Job may not have started yet)", file=sys.stderr)
        return 1

    if args.follow:
        # Use tail -f for following
        try:
            subprocess.call(["tail", "-f", log_path])
        except KeyboardInterrupt:
            pass
        return 0

    # Read and display log
    try:
        with open(log_path, "r") as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading log: {e}", file=sys.stderr)
        return 1

    if args.lines == 0:
        # Show all
        for line in lines:
            print(line, end="")
    else:
        # Show last N lines
        for line in lines[-args.lines:]:
            print(line, end="")

    return 0


def _get_gpu_summary():
    """Aggregate GPU info by partition, finding max per single node."""
    from shepherd import slurm
    nodes = slurm.list_nodes()

    # Aggregate by partition
    partitions = {}
    for node in nodes:
        partition = node.get("partition", "")
        if not partition:
            continue

        gpu_type = node.get("gpu_type")
        gpu_count = node.get("gpu_count", 0)
        gpus_available = node.get("gpus_available", 0)
        vram = node.get("vram_gb", 0)
        state = node.get("state", "")

        if partition not in partitions:
            partitions[partition] = {
                "max_count": 0,
                "max_available": 0,
                "vram_gb": vram,
                "gpu_type": gpu_type,
                "nodes_total": 0,
                "nodes_with_free": 0,
                "total_gpus": 0,
                "total_available": 0,
            }

        entry = partitions[partition]
        entry["max_count"] = max(entry["max_count"], gpu_count)
        entry["nodes_total"] += 1
        entry["total_gpus"] += gpu_count
        # Only count available GPUs from nodes that can actually be used
        if state in ("idle", "mixed"):
            entry["max_available"] = max(entry["max_available"], gpus_available)
            entry["total_available"] += gpus_available
            if gpus_available > 0:
                entry["nodes_with_free"] += 1
        # Update gpu_type if not set
        if not entry["gpu_type"] and gpu_type:
            entry["gpu_type"] = gpu_type
        if not entry["vram_gb"] and vram:
            entry["vram_gb"] = vram

    # Convert to list and sort by VRAM (highest first), then by max available
    result = []
    for partition, info in partitions.items():
        if info["max_count"] == 0:  # Skip non-GPU partitions
            continue
        result.append({
            "partition": partition,
            "gpu_type": info["gpu_type"] or "unknown",
            "max_count": info["max_count"],
            "max_available": info["max_available"],
            "vram_gb": info["vram_gb"],
            "nodes_total": info["nodes_total"],
            "nodes_with_free": info["nodes_with_free"],
            "total_gpus": info["total_gpus"],
            "total_available": info["total_available"],
        })

    result.sort(key=lambda x: (-x["vram_gb"], -x["max_available"], -x["max_count"], x["partition"]))
    return result


# ANSI color codes
class _C:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    CYAN = "\033[36m"


def cmd_gpus(args):
    """Show max assignable GPUs per partition."""
    if getattr(args, "remote", None):
        return _run_remote(args, ["gpus"])

    gpu_summary = _get_gpu_summary()

    if args.json:
        _print_json({"gpus": gpu_summary})
        return 0

    if not gpu_summary:
        print("No GPU partitions found")
        return 0

    # Print table header
    header = f"{'Partition':<24} {'GPU':<14} {'VRAM':<7} {'Max':<5} {'Avail':<6} {'Nodes':<10} {'Total':<12}"
    print(f"{_C.BOLD}{header}{_C.RESET}")
    print("─" * 85)

    for entry in gpu_summary:
        partition = entry["partition"]
        gpu_type = entry["gpu_type"]
        vram = f"{entry['vram_gb']}GB" if entry["vram_gb"] else "?"
        max_count = entry["max_count"]
        max_avail = entry["max_available"]
        nodes_free = entry["nodes_with_free"]
        nodes_total = entry["nodes_total"]
        total_gpus = entry["total_gpus"]
        total_avail = entry["total_available"]

        # Color based on availability
        if max_avail == 0:
            avail_color = _C.RED
            status_icon = "●"  # full
        elif max_avail == max_count:
            avail_color = _C.GREEN
            status_icon = "○"  # empty/available
        else:
            avail_color = _C.YELLOW
            status_icon = "◐"  # partial

        nodes_info = f"{nodes_free}/{nodes_total}"
        total_info = f"{total_avail}/{total_gpus}"

        # Build colored line
        line = f"{_C.CYAN}{partition:<24}{_C.RESET}"
        line += f"{_C.DIM}{gpu_type:<14}{_C.RESET}"
        line += f"{vram:<7}"
        line += f"{max_count:<5}"
        line += f"{avail_color}{status_icon} {max_avail:<4}{_C.RESET}"
        line += f"{nodes_info:<10}"
        line += f"{_C.DIM}{total_info:<12}{_C.RESET}"

        print(line)

    # Legend
    print()
    print(f"{_C.DIM}Max: max GPUs on single node | Avail: max available on single node | Total: cluster-wide{_C.RESET}")

    return 0


def _emit_ok(args):
    if args.json:
        _print_json({"ok": True})
    else:
        print("ok")


def _print_json(payload):
    print(json.dumps(payload, indent=2, sort_keys=True))


def build_parser():
    parser = argparse.ArgumentParser(prog="shepherd")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-daemon", action="store_true", help="Don't auto-start daemon")
    parser.add_argument("--remote", metavar="HOST", help="Run on remote host via SSH (use SSH config name)")
    parser.add_argument("--remote-python", metavar="CMD",
                        help="Python command on remote (default: python -m shepherd)")
    parser.add_argument("--remote-dir", metavar="DIR", default="~/.local/lib/shepherd",
                        help="Remote directory for synced code (default: ~/.local/lib/shepherd)")
    parser.add_argument("--no-sync", action="store_true",
                        help="Skip auto-sync for remote commands")
    subparsers = parser.add_subparsers(dest="command")

    list_parser = subparsers.add_parser("list")
    list_parser.set_defaults(func=cmd_list)

    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("--run-id", required=True)
    status_parser.set_defaults(func=cmd_status)

    control_parser = subparsers.add_parser("control")
    control_parser.add_argument("op")
    control_parser.add_argument("--run-id", required=True)
    control_parser.add_argument("--node")
    control_parser.add_argument("--ttl", type=int)
    control_parser.add_argument("--reason")
    control_parser.add_argument("--key")
    control_parser.add_argument("--value")
    control_parser.set_defaults(func=cmd_control)

    daemon_parser = subparsers.add_parser("daemon")
    daemon_parser.add_argument("--interval", type=int, default=10)
    daemon_parser.set_defaults(func=cmd_daemon)

    daemon_status_parser = subparsers.add_parser("daemon-status")
    daemon_status_parser.set_defaults(func=cmd_daemon_status)

    sync_parser = subparsers.add_parser("sync", help="Sync shepherd code to remote host")
    sync_parser.add_argument("--no-restart", action="store_true", help="Don't restart daemon after sync")
    sync_parser.set_defaults(func=cmd_sync)

    tui_parser = subparsers.add_parser("tui")
    tui_parser.set_defaults(func=cmd_tui)

    new_parser = subparsers.add_parser("new", help="Create a new run")
    new_parser.add_argument("script", help="Path to sbatch script")
    new_parser.add_argument("--run-id", dest="run_id", help="Run identifier (default: script name)")
    new_parser.add_argument("--mode", choices=["run_once", "indefinite"], default="run_once")
    new_parser.add_argument("--gpus", type=int, help="Minimum GPUs per node")
    new_parser.add_argument("--min-vram", type=int, dest="min_vram", help="Minimum VRAM per GPU in GB")
    new_parser.add_argument("--max-vram", type=int, dest="max_vram", help="Maximum VRAM per GPU in GB")
    new_parser.add_argument("--prefer", choices=["min", "max"], default="max",
                            help="'max' for best GPUs first, 'min' for cheapest sufficient")
    new_parser.add_argument("--partitions", help="Manual partition list (skips auto-discovery)")
    new_parser.add_argument("--no-auto-partitions", action="store_true", help="Disable auto-discovery")
    new_parser.add_argument("--no-blacklist", action="store_true", help="Disable node blacklisting for this run")
    new_parser.set_defaults(func=cmd_new)

    nodes_parser = subparsers.add_parser("nodes", help="List nodes and manage blacklist")
    nodes_parser.add_argument("op", nargs="?", default="interactive",
                              choices=["list", "ban", "unban", "interactive"],
                              help="Operation: interactive (default), list, ban, unban")
    nodes_parser.add_argument("--node", help="Node name (for ban/unban)")
    nodes_parser.add_argument("--ttl", type=int, help="Ban TTL in seconds (default: permanent)")
    nodes_parser.add_argument("--reason", help="Reason for banning")
    nodes_parser.set_defaults(func=cmd_nodes)

    # Config subcommand for remote settings
    config_parser = subparsers.add_parser("config", help="Manage remote configuration")
    config_parser.add_argument("op", choices=["get", "set", "list"],
                               help="Operation: get, set, or list")
    config_parser.add_argument("key", nargs="?", help="Config key (e.g., conda_env)")
    config_parser.add_argument("value", nargs="?", help="Value to set")
    config_parser.set_defaults(func=cmd_config)

    # Logs subcommand
    logs_parser = subparsers.add_parser("logs", help="View SLURM stdout/stderr logs")
    logs_parser.add_argument("--run-id", dest="run_id", required=True, help="Run identifier")
    logs_parser.add_argument("--stderr", action="store_true", help="Show stderr instead of stdout")
    logs_parser.add_argument("-f", "--follow", action="store_true", help="Follow log output (like tail -f)")
    logs_parser.add_argument("-n", "--lines", type=int, default=50, help="Number of lines to show (default: 50, 0 for all)")
    logs_parser.set_defaults(func=cmd_logs)

    gpus_parser = subparsers.add_parser("gpus", help="Show max assignable GPUs per GPU type")
    gpus_parser.set_defaults(func=cmd_gpus)

    return parser


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    subcommands = {"list", "status", "control", "daemon", "daemon-status", "sync", "tui", "new", "nodes", "config", "logs", "gpus"}
    global_flags_with_value = {"--remote", "--remote-python", "--remote-dir"}
    global_flags_no_value = {"--json", "--no-daemon", "--no-sync"}

    # Check if we already have a subcommand
    has_subcommand = any(arg in subcommands for arg in argv if not arg.startswith("-"))

    if not has_subcommand:
        # Check if there's any positional argument (script path)
        # Skip values of flags that take arguments
        has_positional = False
        skip_next = False
        for arg in argv:
            if skip_next:
                skip_next = False
                continue
            if arg in global_flags_with_value:
                skip_next = True
                continue
            if not arg.startswith("-"):
                has_positional = True
                break

        if has_positional:
            # Separate global flags from new-specific args
            # Global flags must come before the subcommand
            global_args = []
            rest_args = []
            i = 0
            while i < len(argv):
                arg = argv[i]
                if arg in global_flags_no_value:
                    global_args.append(arg)
                    i += 1
                elif arg in global_flags_with_value and i + 1 < len(argv):
                    global_args.extend([arg, argv[i + 1]])
                    i += 2
                else:
                    rest_args.append(arg)
                    i += 1
            argv = global_args + ["new"] + rest_args
        else:
            # No positionals at all, default to tui
            argv = argv + ["tui"]

    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())

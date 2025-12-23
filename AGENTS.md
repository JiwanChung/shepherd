# AGENT.md

**Resilient Slurm Job Shepherd with Heartbeat, Auto-Restart, Node Blacklisting, and TUI**

---

## 0. purpose

Design and implement a **user-space job orchestration system** for noisy Slurm HPC environments that:

* runs without admin privileges
* tolerates faulty nodes, broken GPUs, MIG/container mismatches, and trespassers
* keeps workloads progressing via **automatic restart + resume**
* supports both:

  * **run-once jobs** (retry until successful completion)
  * **indefinite jobs** (kept alive for a fixed period)
* provides a **terminal UI (TUI)** for live monitoring and control

This system MUST operate entirely in user space, using Slurm CLI tools and shared filesystem state.

---

## implementation status (current)

### Core Features (Complete)

* **Compute-side wrapper** (`shepherd/wrapper.py`)
  * GPU visibility probe (`nvidia-smi -L`)
  * CUDA smoke test (torch/cupy/numba)
  * MIG count validation
  * Trespasser detection
  * Heartbeat thread
  * Failure/final markers with exit codes (42/43/44/50)

* **Shepherd daemon** (`shepherd/daemon.py`)
  * Batched Slurm queries (`squeue`, `sacct`)
  * Heartbeat stall detection
  * Progress stall detection
  * Automatic restart with exponential backoff
  * Node blacklisting with TTL
  * Partition fallback (automatic failover across queues)
  * Auto-wrapping of scripts with heredoc for complex multiline scripts

* **CLI** (`shepherd/cli.py`)
  * `shepherd <script>` — submit new run (auto-discovers GPU partitions)
  * `shepherd list` — list all runs
  * `shepherd status --run-id X` — detailed status
  * `shepherd control <op> --run-id X` — pause/unpause/stop/restart/start
  * `shepherd nodes` — interactive node management TUI
  * `shepherd nodes ban/unban` — blacklist management
  * `shepherd tui` — main monitoring TUI
  * `shepherd sync` — sync code to remote
  * `--remote HOST` — execute on remote cluster via SSH
  * `--json` — machine-readable output for all commands

* **Modern TUI** (`shepherd/tui.py`)
  * 256-color scheme with 8-color fallback
  * Split-pane layout (runs list + detail/script/logs)
  * Vim-style navigation (j/k, g/G, Ctrl+u/d)
  * Status icons (●, ◐, ○, ↻, ✗, ✓, ■)
  * Scroll indicators
  * Real-time log tailing
  * Syntax highlighting (#SBATCH green, #SHEPHERD purple)

* **Nodes TUI** (`shepherd/cli.py:_interactive_nodes`)
  * List all nodes with GPU info
  * Show blacklist status and reasons
  * Interactive ban/unban with TTL
  * TTY detection with text fallback

* **GPU Partition Discovery** (`shepherd/slurm.py`)
  * Auto-discover partitions via `sinfo`
  * Filter by GPU count, min/max VRAM
  * Preference modes: `max` (best first) or `min` (cheapest first)
  * Known GPU VRAM database (A100, H100, V100, L40, etc.)

* **#SHEPHERD Directives** (`shepherd/slurm.py:parse_shepherd_directives`)
  * `--gpus N` — minimum GPUs
  * `--min-vram N` / `--max-vram N` — VRAM filtering
  * `--prefer min|max` — partition ordering
  * `--mode run_once|indefinite` — run mode
  * `--partitions a,b,c` — explicit partition list
  * `--max-retries N`, `--keep-alive N`, `--backoff-*`, etc.

* **Remote Execution** (`shepherd/cli.py`)
  * `--remote HOST` for all commands
  * Auto-sync code on changes (MD5 hash comparison)
  * TTY forwarding for interactive TUI
  * Remote daemon auto-start

* **Partition Fallback** (`shepherd/daemon.py`)
  * Automatic failover after N failures per partition
  * Periodic retry of preferred partition
  * Immediate retry on partition switch
  * Configurable via `partition_fallback` in meta.json

* **Unit Tests** (`tests/`)
  * `test_backoff.py` — exponential backoff logic
  * `test_blacklist.py` — node blacklist add/remove/TTL
  * `test_heartbeat.py` — heartbeat read/stale detection
  * `test_state.py` — state file parsing, corrupt file handling
  * `test_daemon.py` — partition fallback, heartbeat cancellation, restart logic

### Pending / Partial

* Full JSON schema validation for `meta.json` (not enforced)
* Integration tests for probe failures, crash loops, expiry windows
* Multi-node job support (currently single-node only)

---

## 1. constraints and assumptions

* No admin rights (no Slurm config, no prolog/epilog, no node draining)
* User may run long-lived processes on login nodes
* Slurm CLI available: `sbatch`, `squeue`, `scancel` (optionally `sacct`)
* Shared filesystem visible to login + compute nodes
* Workloads support checkpoint/resume (mandatory for correctness)

---

## 2. high-level architecture

### components

1. **compute-side wrapper**

   * runs inside Slurm allocation
   * performs preflight probes
   * launches workload with resume
   * emits heartbeat + progress
   * emits structured failure records

2. **login-node shepherd daemon**

   * submits jobs
   * monitors Slurm + heartbeat
   * restarts jobs automatically
   * maintains node blacklist
   * enforces run semantics (run-once vs indefinite)

3. **interactive TUI**

   * live dashboard of all runs
   * view status, logs, failures
   * control runs (start/stop/restart/pause)
   * manage blacklist

---

## 3. run modes

Each run declares a `run_mode`.

### 3.1 run_once

**Goal:** finish successfully once.

Rules:

* Job is retried indefinitely (or until `max_retries`) until:

  * wrapper exits with `exit_code=0`
  * AND `final.json` exists
* Any failure, stall, or preemption triggers restart
* On success, run terminates permanently

### 3.2 indefinite

**Goal:** keep job alive and responsive for a fixed period.

Rules:

* Run has `keep_alive_sec`
* Any termination or stall triggers restart **within window**
* Clean exit is treated as failure unless configured otherwise
* When window expires:

  * shepherd requests graceful shutdown
  * job is cancelled if needed
  * run ends

---

## 4. shared filesystem layout (authoritative)

All state lives under:

```
~/.slurm_shepherd/
├── runs/
│   └── <run_id>/
│       ├── meta.json
│       ├── control.json
│       ├── heartbeat
│       ├── progress.json          (optional)
│       ├── failure.json           (on failure)
│       ├── final.json             (run_once success)
│       ├── ended.json             (run termination)
│       └── badnode_events.log
├── blacklist.json
└── locks/
    └── <run_id>.lock
```

All JSON files:

* UTF-8
* newline-terminated
* written atomically (temp + rename)

---

## 5. JSON schemas (canonical)

### 5.1 `meta.json`

Authoritative run metadata.

Includes:

* run identity
* run mode
* lifecycle timestamps
* sbatch parameters
* runtime linkage to Slurm job
* policy thresholds

(See full schema exactly as defined earlier; **must not be altered**.)

---

### 5.2 `heartbeat`

Plain text:

```
<epoch_seconds>\n
```

Updated every `heartbeat_interval_sec`.

---

### 5.3 `progress.json` (optional)

Domain-specific progress used for soft-stall detection and UI display.

---

### 5.4 `failure.json`

Machine-readable failure record.

Used for:

* blacklist decisions
* TUI display
* debugging

---

### 5.5 `final.json` (run_once only)

Unambiguous success marker.

Run is considered successful **only if**:

* wrapper exit code = 0
* AND this file exists

---

### 5.6 `ended.json`

Terminal record for all runs.

---

### 5.7 `control.json`

User/TUI-controlled flags:

* paused
* stop_requested

---

### 5.8 `blacklist.json`

Persistent node health memory with TTL support.

---

## 6. compute-side wrapper contract

### responsibilities

* run **preflight probes** before workload
* fail fast on bad nodes
* emit heartbeat
* checkpoint/resume workload
* emit structured failure record

### preflight probes (time-bounded)

Required probes:

1. **GPU visibility**

   * `nvidia-smi -L`
   * fail if command fails or shows 0 GPUs

2. **CUDA kernel smoke test**

   * allocate tensor + kernel + synchronize
   * fail on runtime errors

3. **MIG / container mismatch (best-effort)**

   * validate CUDA visibility matches expectations

4. **Trespasser detection (best-effort)**

   * detect foreign GPU processes via `nvidia-smi`
   * fail fast if detected

### exit code contract

| Code | Meaning                    |
| ---: | -------------------------- |
|    0 | clean exit                 |
|   42 | node hardware/config fault |
|   43 | foreign GPU processes      |
|   44 | CUDA kernel failure        |
|   50 | workload failure           |

Wrapper MUST write `failure.json` on nonzero exit.

---

## 7. shepherd daemon behavior

### core loop

For each `run_id`:

1. Acquire lock
2. Load state
3. Query Slurm (batched)
4. Evaluate job state + heartbeat
5. Decide action:

   * submit
   * cancel
   * resubmit
   * wait
   * terminate run

### heartbeat handling

* RUNNING + heartbeat stale → cancel + resubmit
* startup grace period honored

### blacklist logic

* node-fault exit codes add node to blacklist
* TTL supported
* exclude list capped (top-K)
* applied via `sbatch --exclude=...`

### backoff

* exponential backoff on crash loops
* prevents scheduler hammering

---

## 8. Slurm interaction rules

* All Slurm calls must be time-bounded
* Slurm queries must be aggregated
* Shepherd must survive Slurm command failures
* `--exclude` length must be capped

---

## 9. shepherd CLI (JSON-first)

All commands support `--json`.

### required commands

* `shepherd list`
* `shepherd status --run-id X`
* `shepherd control <op> --run-id X`

  * start
  * stop
  * restart
  * pause / unpause
  * blacklist-add / blacklist-remove
  * config-set (safe subset only)

Schemas for outputs are fixed and MUST be followed.

---

## 10. normalized status model (for UI)

Statuses:

* healthy_running
* running_degraded
* unresponsive
* pending
* restarting
* crash_loop
* completed_success
* ended_expired
* stopped_manual
* error_unknown

Priority rules are deterministic and centralized in shepherd.

---

## 11. terminal UI (TUI)

### goals

* live dashboard of all runs
* interactive control
* SSH-friendly
* no direct Slurm manipulation (via shepherd API only)

### screens

1. main dashboard (table of runs)
2. run detail view
3. log tail view
4. blacklist manager

### operations

* start / stop / restart
* pause / unpause
* blacklist nodes
* edit safe config
* tail logs

### performance

* handles ≥200 runs
* batched Slurm queries
* resilient to Slurm hangs

---

## 12. TUI keybindings (mandatory)

### global

* `q` quit
* `?` help
* `R` refresh
* `/` search
* `Esc` back

### dashboard

* `j/k` navigate
* `Enter` details
* `r` restart
* `s` stop
* `p` pause
* `t` logs
* `b` blacklist node
* `n` new run

### detail

* `r/s/p/t/b/e`
* `Backspace` return

### logs

* `space` follow
* `o/e` stdout/stderr
* `/` search

---

## 13. safety rules

* Never kill foreign processes
* Never exceed Slurm query rate limits
* Never assume Slurm correctness
* Always treat state files as potentially corrupt
* Prefer restart over hang

---

## 14. testing requirements

### unit

* heartbeat logic
* blacklist TTL
* backoff
* state parsing

### integration

* simulate probe failures
* simulate heartbeat stall
* simulate crash loop
* simulate expiry

---

## 15. non-goals (explicit)

* no admin integration
* no Slurm policy changes
* no cluster enforcement
* no privileged operations

---

## 16. guiding principle

> **Progress beats purity.**
> If the job is not making progress, restart it elsewhere.

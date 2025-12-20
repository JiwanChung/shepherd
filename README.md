# Shepherd

User-space Slurm job shepherd with automatic restart, heartbeat monitoring, node blacklisting, and a TUI.

## Features

- Run-once and indefinite run modes
- Heartbeat and progress stall detection
- Automatic restart with backoff
- Node blacklist with TTL
- Terminal UI for monitoring and control
- Works without admin privileges (Slurm CLI + shared filesystem)

## Install

This repo is pure Python and runs from source.

## Quick start

1. Create a run directory and `meta.json`:

```
mkdir -p ~/.slurm_shepherd/runs/demo
cat > ~/.slurm_shepherd/runs/demo/meta.json <<'JSON'
{
  "run_id": "demo",
  "run_mode": "run_once",
  "sbatch_script": "/path/to/sbatch_wrapper.sh",
  "sbatch_args": "",
  "created_at": 0
}
JSON
```

2. Start the daemon:

```
python -m shepherd daemon
```

3. Open the TUI:

```
python -m shepherd tui
```

## Wrapper usage

Use the wrapper inside your sbatch script:

```
python -m shepherd.wrapper \
  --run-id demo \
  --run-mode run_once \
  --state-dir ~/.slurm_shepherd \
  --heartbeat-interval 30 \
  -- \
  /path/to/your/workload --arg1 value1
```

An example sbatch script is in `examples/sbatch_wrapper.sh`.

## CLI

```
python -m shepherd list
python -m shepherd status --run-id demo
python -m shepherd control pause --run-id demo
python -m shepherd control restart --run-id demo
python -m shepherd control blacklist-add --node node001
```

## State layout

All state lives under `~/.slurm_shepherd/`:

```
runs/<run_id>/
  meta.json
  control.json
  heartbeat
  progress.json
  failure.json
  final.json
  ended.json
  badnode_events.log
blacklist.json
locks/<run_id>.lock
```

## Tests

```
python -m unittest discover -s tests
```

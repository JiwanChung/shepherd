#!/usr/bin/env python3
"""Generate mock data for shepherd demo/screenshots."""

import json
import os
import shutil
import time

DEMO_STATE_DIR = os.path.expanduser("~/.slurm_shepherd_demo")


def setup_demo():
    """Create demo state directory with mock runs."""
    # Clean up any existing demo data
    if os.path.exists(DEMO_STATE_DIR):
        shutil.rmtree(DEMO_STATE_DIR)

    runs_dir = os.path.join(DEMO_STATE_DIR, "runs")
    os.makedirs(runs_dir, exist_ok=True)
    os.makedirs(os.path.join(DEMO_STATE_DIR, "locks"), exist_ok=True)

    now = int(time.time())

    # Mock runs with different states
    mock_runs = [
        {
            "run_id": "llama-finetune",
            "meta": {
                "run_id": "llama-finetune",
                "run_mode": "indefinite",
                "sbatch_script": "~/jobs/llama_finetune.sh",
                "slurm_job_id": "284719",
                "slurm_state": "RUNNING",
                "created_at": now - 7200,
                "started_at": now - 7200,
                "restart_count": 2,
                "current_partition": "gpu-a100",
                "partition_fallback": {
                    "partitions": ["gpu-a100", "gpu-h100", "gpu-v100"]
                },
                "gpus": 4,
            },
            "heartbeat": now - 5,
            "script": """#!/bin/bash
#SBATCH --job-name=llama-finetune
#SBATCH --output=logs/%j.out
#SBATCH --time=72:00:00
#SBATCH --nodes=1

#SHEPHERD --gpus 4 --min-vram 80
#SHEPHERD --mode indefinite --keep-alive 259200

source ~/.bashrc
conda activate llama

torchrun --nproc_per_node=4 \\
    finetune.py \\
    --model meta-llama/Llama-2-70b \\
    --dataset alpaca \\
    --batch_size 4 \\
    --gradient_accumulation 8 \\
    --learning_rate 2e-5 \\
    --epochs 3
""",
        },
        {
            "run_id": "gpt-evaluation",
            "meta": {
                "run_id": "gpt-evaluation",
                "run_mode": "run_once",
                "sbatch_script": "~/jobs/eval.sh",
                "slurm_job_id": "284720",
                "slurm_state": "RUNNING",
                "created_at": now - 2700,
                "started_at": now - 2700,
                "restart_count": 0,
                "current_partition": "gpu-v100",
                "gpus": 2,
            },
            "heartbeat": now - 12,
            "script": """#!/bin/bash
#SBATCH --job-name=gpt-eval
#SBATCH --output=logs/%j.out
#SBATCH --time=4:00:00

#SHEPHERD --gpus 2 --min-vram 32

python evaluate.py \\
    --model gpt-4 \\
    --benchmark mmlu \\
    --output results/
""",
        },
        {
            "run_id": "mistral-pretrain",
            "meta": {
                "run_id": "mistral-pretrain",
                "run_mode": "indefinite",
                "sbatch_script": "~/jobs/pretrain.sh",
                "slurm_job_id": "284721",
                "slurm_state": "PENDING",
                "slurm_reason": "Priority",
                "created_at": now - 300,
                "restart_count": 0,
                "current_partition": "gpu-h100",
                "partition_fallback": {
                    "partitions": ["gpu-h100", "gpu-a100"]
                },
                "gpus": 8,
            },
            "heartbeat": None,
            "script": """#!/bin/bash
#SBATCH --job-name=mistral-pretrain
#SBATCH --output=logs/%j.out
#SBATCH --time=168:00:00
#SBATCH --nodes=1

#SHEPHERD --gpus 8 --min-vram 80
#SHEPHERD --mode indefinite

torchrun --nproc_per_node=8 \\
    pretrain.py \\
    --config configs/mistral_7b.yaml
""",
        },
        {
            "run_id": "data-preprocessing",
            "meta": {
                "run_id": "data-preprocessing",
                "run_mode": "run_once",
                "sbatch_script": "~/jobs/preprocess.sh",
                "created_at": now - 86400,
                "started_at": now - 86400,
                "restart_count": 1,
            },
            "heartbeat": None,
            "final": {"timestamp": now - 3600},
            "ended": {"reason": "completed", "timestamp": now - 3600},
            "script": """#!/bin/bash
#SBATCH --job-name=preprocess
#SBATCH --output=logs/%j.out

python preprocess.py --input raw/ --output processed/
""",
        },
        {
            "run_id": "bert-training",
            "meta": {
                "run_id": "bert-training",
                "run_mode": "run_once",
                "sbatch_script": "~/jobs/bert.sh",
                "slurm_job_id": "284650",
                "slurm_state": "RUNNING",
                "created_at": now - 14400,
                "started_at": now - 14400,
                "restart_count": 3,
                "restart_reason": "node_fail",
                "current_partition": "gpu-a100",
                "gpus": 4,
            },
            "heartbeat": now - 180,  # Stale heartbeat - will show as unresponsive
            "script": """#!/bin/bash
#SBATCH --job-name=bert-train
#SBATCH --output=logs/%j.out
#SBATCH --time=24:00:00

#SHEPHERD --gpus 4 --max-retries 5

python train_bert.py --epochs 10
""",
        },
    ]

    # Create run directories and files
    for run in mock_runs:
        run_dir = os.path.join(runs_dir, run["run_id"])
        os.makedirs(run_dir, exist_ok=True)

        # Write meta.json
        with open(os.path.join(run_dir, "meta.json"), "w") as f:
            json.dump(run["meta"], f, indent=2)

        # Write heartbeat
        if run.get("heartbeat"):
            with open(os.path.join(run_dir, "heartbeat"), "w") as f:
                f.write(f"{run['heartbeat']}\n")

        # Write final.json if exists
        if run.get("final"):
            with open(os.path.join(run_dir, "final.json"), "w") as f:
                json.dump(run["final"], f)

        # Write ended.json if exists
        if run.get("ended"):
            with open(os.path.join(run_dir, "ended.json"), "w") as f:
                json.dump(run["ended"], f)

        # Write mock script
        script_path = os.path.join(run_dir, "script.sh")
        with open(script_path, "w") as f:
            f.write(run["script"])

    # Create mock blacklist
    blacklist = {
        "nodes": {
            "gpu-node-017": {
                "added_at": now - 1800,
                "expires_at": now + 1800,
                "reason": "CUDA error",
            },
            "gpu-node-042": {
                "added_at": now - 3600,
                "expires_at": now + 7200,
                "reason": "Node unresponsive",
            },
        }
    }
    with open(os.path.join(DEMO_STATE_DIR, "blacklist.json"), "w") as f:
        json.dump(blacklist, f, indent=2)

    print(f"Demo data created in {DEMO_STATE_DIR}")
    print()
    print("To run the demo TUI:")
    print()
    print("  SHEPHERD_STATE_DIR=~/.slurm_shepherd_demo python -m shepherd tui")
    print()
    print("To record a GIF, use a tool like:")
    print("  - asciinema (asciinema rec demo.cast)")
    print("  - terminalizer (terminalizer record demo)")
    print("  - vhs (vhs demo.tape)")
    print()


if __name__ == "__main__":
    setup_demo()

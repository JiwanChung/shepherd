#!/bin/bash
#
# Example sbatch script that runs the shepherd wrapper and your workload.
#
# Usage:
#   sbatch examples/sbatch_wrapper.sh
#

#SBATCH --job-name=shepherd-run
#SBATCH --output=%x-%j.out
#SBATCH --error=%x-%j.err
#SBATCH --gres=gpu:1
#SBATCH --time=01:00:00

set -euo pipefail

RUN_ID="${SHEPHERD_RUN_ID:-example-run}"
RUN_MODE="${SHEPHERD_RUN_MODE:-run_once}"
STATE_DIR="${SHEPHERD_STATE_DIR:-$HOME/.slurm_shepherd}"

python -m shepherd.wrapper \
  --run-id "${RUN_ID}" \
  --run-mode "${RUN_MODE}" \
  --state-dir "${STATE_DIR}" \
  --heartbeat-interval 30 \
  -- \
  /path/to/your/workload --arg1 value1

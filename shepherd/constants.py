import os

STATE_DIR = os.path.expanduser("~/.slurm_shepherd")
RUNS_DIR = os.path.join(STATE_DIR, "runs")
LOCKS_DIR = os.path.join(STATE_DIR, "locks")
BLACKLIST_PATH = os.path.join(STATE_DIR, "blacklist.json")

META_FILENAME = "meta.json"
CONTROL_FILENAME = "control.json"
HEARTBEAT_FILENAME = "heartbeat"
PROGRESS_FILENAME = "progress.json"
FAILURE_FILENAME = "failure.json"
FINAL_FILENAME = "final.json"
ENDED_FILENAME = "ended.json"
BADNODE_EVENTS_FILENAME = "badnode_events.log"

# Exit codes defined by the wrapper contract.
EXIT_NODE_FAULT = 42
EXIT_TRESPASSER = 43
EXIT_CUDA_FAILURE = 44
EXIT_WORKLOAD_FAILURE = 50

DEFAULT_HEARTBEAT_GRACE_SEC = 90
DEFAULT_HEARTBEAT_INTERVAL_SEC = 30
DEFAULT_BLACKLIST_LIMIT = 64


def set_state_dir(path):
    global STATE_DIR, RUNS_DIR, LOCKS_DIR, BLACKLIST_PATH
    STATE_DIR = path
    RUNS_DIR = os.path.join(STATE_DIR, "runs")
    LOCKS_DIR = os.path.join(STATE_DIR, "locks")
    BLACKLIST_PATH = os.path.join(STATE_DIR, "blacklist.json")

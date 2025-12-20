import argparse
import json
import sys
import time

from shepherd import blacklist
from shepherd import constants
from shepherd import daemon as daemon_mod
from shepherd import fs
from shepherd import heartbeat
from shepherd import status
from shepherd import tui as tui_mod


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
    return {
        "run_id": run["run_id"],
        "status": status_value,
        "run_mode": meta.get("run_mode"),
        "slurm_job_id": meta.get("slurm_job_id"),
        "last_heartbeat": run["heartbeat_ts"],
    }


def cmd_list(args):
    fs.ensure_dirs()
    runs = [_summarize_run(_load_run(run_id)) for run_id in fs.list_runs()]
    if args.json:
        _print_json({"runs": runs})
    else:
        for run in runs:
            print(
                "{run_id} {status} mode={run_mode} job={slurm_job_id} hb={last_heartbeat}".format(
                    **run
                )
            )


def cmd_status(args):
    fs.ensure_dirs()
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
    fs.ensure_dirs()
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
    return tui_mod.run_tui()


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
    subparsers = parser.add_subparsers(dest="command", required=True)

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

    tui_parser = subparsers.add_parser("tui")
    tui_parser.set_defaults(func=cmd_tui)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())

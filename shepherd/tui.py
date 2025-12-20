import curses
import json
import os
import time

from shepherd import blacklist
from shepherd import constants
from shepherd import fs
from shepherd import heartbeat
from shepherd import status


def run_tui():
    curses.wrapper(_main)


def _main(stdscr):
    app = _App(stdscr)
    app.loop()


class _App:
    def __init__(self, stdscr):
        self.stdscr = stdscr
        self.mode = "dashboard"
        self.selected = 0
        self.filter_text = ""
        self.log_follow = True
        self.log_view = "stdout"
        self.log_search = ""
        self.log_offset = 0
        self.refresh_interval = 2
        self.last_refresh = 0
        self.runs = []
        self.run_details = {}

    def loop(self):
        curses.curs_set(0)
        self.stdscr.nodelay(True)
        while True:
            now = time.time()
            if now - self.last_refresh >= self.refresh_interval:
                self._refresh_runs()
                self.last_refresh = now
            self._render()
            key = self.stdscr.getch()
            if key == -1:
                time.sleep(0.05)
                continue
            if self._handle_global(key):
                continue
            if self.mode == "dashboard":
                self._handle_dashboard(key)
            elif self.mode == "detail":
                self._handle_detail(key)
            elif self.mode == "logs":
                self._handle_logs(key)
            elif self.mode == "blacklist":
                self._handle_blacklist(key)
            elif self.mode == "help":
                self.mode = "dashboard"

    def _refresh_runs(self):
        fs.ensure_dirs()
        runs = []
        details = {}
        for run_id in fs.list_runs():
            meta = fs.read_json(fs.run_file(run_id, constants.META_FILENAME))
            control = fs.read_json(fs.run_file(run_id, constants.CONTROL_FILENAME))
            ended = fs.read_json(fs.run_file(run_id, constants.ENDED_FILENAME))
            final = fs.read_json(fs.run_file(run_id, constants.FINAL_FILENAME))
            hb_ts = heartbeat.read_heartbeat(fs.run_file(run_id, constants.HEARTBEAT_FILENAME))
            slurm_state = None
            if isinstance(meta, dict):
                slurm_state = meta.get("slurm_state")
            status_value = status.compute_status(
                meta if isinstance(meta, dict) else {},
                control if isinstance(control, dict) else None,
                ended if isinstance(ended, dict) else None,
                final if isinstance(final, dict) else None,
                hb_ts,
                slurm_state,
            )
            entry = {
                "run_id": run_id,
                "status": status_value,
                "run_mode": (meta or {}).get("run_mode"),
                "job_id": (meta or {}).get("slurm_job_id"),
                "heartbeat": hb_ts,
            }
            if self.filter_text and self.filter_text not in run_id:
                continue
            runs.append(entry)
            details[run_id] = {
                "meta": meta,
                "control": control,
                "ended": ended,
                "final": final,
                "failure": fs.read_json(fs.run_file(run_id, constants.FAILURE_FILENAME)),
            }
        self.runs = runs
        self.run_details = details
        if self.selected >= len(self.runs):
            self.selected = max(0, len(self.runs) - 1)

    def _handle_global(self, key):
        if key in (ord("q"), ord("Q")):
            raise SystemExit(0)
        if key == ord("?"):
            self.mode = "help"
            return True
        if key == ord("R"):
            self._refresh_runs()
            return True
        if key == ord("/") and self.mode != "logs":
            self.filter_text = self._prompt("search")
            self._refresh_runs()
            return True
        if key == 27:
            self.mode = "dashboard"
            return True
        return False

    def _handle_dashboard(self, key):
        if key in (ord("j"), curses.KEY_DOWN):
            self.selected = min(self.selected + 1, len(self.runs) - 1)
        elif key in (ord("k"), curses.KEY_UP):
            self.selected = max(self.selected - 1, 0)
        elif key in (curses.KEY_ENTER, 10, 13):
            if self._selected_run():
                self.mode = "detail"
        elif key == ord("r"):
            self._control_selected("restart")
        elif key == ord("s"):
            self._control_selected("stop")
        elif key == ord("p"):
            self._control_selected("pause")
        elif key == ord("t"):
            self.mode = "logs"
            self.log_offset = 0
        elif key == ord("b"):
            self._blacklist_selected()
        elif key == ord("n"):
            self._new_run()

    def _handle_detail(self, key):
        if key == ord("r"):
            self._control_selected("restart")
        elif key == ord("s"):
            self._control_selected("stop")
        elif key == ord("p"):
            self._control_selected("pause")
        elif key == ord("t"):
            self.mode = "logs"
            self.log_offset = 0
        elif key == ord("b"):
            self._blacklist_selected()
        elif key == ord("e"):
            self._edit_config()
        elif key == curses.KEY_BACKSPACE or key == 127:
            self.mode = "dashboard"

    def _handle_logs(self, key):
        if key == ord(" "):
            self.log_follow = not self.log_follow
        elif key == ord("o"):
            self.log_view = "stdout"
        elif key == ord("e"):
            self.log_view = "stderr"
        elif key == ord("/"):
            self.log_search = self._prompt("log search")
        elif key in (ord("k"), curses.KEY_UP):
            self.log_follow = False
            self.log_offset += 1
        elif key in (ord("j"), curses.KEY_DOWN):
            self.log_offset = max(0, self.log_offset - 1)
        elif key == 27:
            self.mode = "detail"

    def _handle_blacklist(self, key):
        if key == ord("a"):
            node = self._prompt("node to add")
            if node:
                blacklist.add_node(node)
        elif key == ord("d"):
            node = self._prompt("node to remove")
            if node:
                blacklist.remove_node(node)
        elif key == 27:
            self.mode = "dashboard"

    def _render(self):
        self.stdscr.erase()
        if self.mode == "dashboard":
            self._render_dashboard()
        elif self.mode == "detail":
            self._render_detail()
        elif self.mode == "logs":
            self._render_logs()
        elif self.mode == "blacklist":
            self._render_blacklist()
        elif self.mode == "help":
            self._render_help()
        self.stdscr.refresh()

    def _render_dashboard(self):
        self._render_header("Dashboard (j/k navigate, Enter details, r/s/p/t/b, n new)")
        for idx, run in enumerate(self.runs):
            line = "{run_id:20} {status:18} mode={run_mode} job={job_id} hb={heartbeat}".format(
                **run
            )
            self._render_line(2 + idx, line, highlight=(idx == self.selected))

    def _render_detail(self):
        run = self._selected_run()
        self._render_header("Detail (r/s/p/t/b/e, Backspace to return)")
        if not run:
            self._render_line(2, "No run selected")
            return
        details = self.run_details.get(run["run_id"], {})
        lines = [
            f"run_id: {run['run_id']}",
            f"status: {run['status']}",
            f"run_mode: {run['run_mode']}",
            f"job_id: {run['job_id']}",
            "meta:",
            json.dumps(details.get("meta"), indent=2, sort_keys=True),
            "control:",
            json.dumps(details.get("control"), indent=2, sort_keys=True),
            "failure:",
            json.dumps(details.get("failure"), indent=2, sort_keys=True),
        ]
        row = 2
        for line in lines:
            for chunk in line.splitlines() or [""]:
                self._render_line(row, chunk)
                row += 1

    def _render_logs(self):
        run = self._selected_run()
        self._render_header("Logs (space follow, o/e stdout|stderr, Esc back)")
        if not run:
            self._render_line(2, "No run selected")
            return
        meta = (self.run_details.get(run["run_id"], {}).get("meta") or {})
        path = meta.get(f"{self.log_view}_path")
        if not path:
            path = fs.run_file(run["run_id"], f"{self.log_view}.log")
        lines = _tail_lines(path, limit=self._max_body_lines() + self.log_offset)
        if not lines:
            self._render_line(2, f"No {self.log_view} logs at {path}")
            return
        if self.log_search:
            lines = [line for line in lines if self.log_search in line]
        if self.log_follow:
            lines = lines[-self._max_body_lines():]
        else:
            end = len(lines) - self.log_offset
            start = max(0, end - self._max_body_lines())
            lines = lines[start:end]
        for idx, line in enumerate(lines):
            self._render_line(2 + idx, line)

    def _render_blacklist(self):
        self._render_header("Blacklist (a add, d remove, Esc back)")
        data = blacklist.load_blacklist()
        nodes = sorted(data.get("nodes", {}).keys())
        for idx, node in enumerate(nodes):
            self._render_line(2 + idx, node)

    def _render_help(self):
        lines = [
            "q quit, R refresh, / search, Esc back",
            "dashboard: j/k move, Enter details, r restart, s stop, p pause, t logs, b blacklist, n new run",
            "detail: r/s/p/t/b/e, Backspace return",
            "logs: space follow, o/e stdout/stderr",
        ]
        self._render_header("Help")
        for idx, line in enumerate(lines):
            self._render_line(2 + idx, line)

    def _render_header(self, text):
        self._render_line(0, text)
        self._render_line(1, "-" * max(10, len(text)))

    def _render_line(self, row, text, highlight=False):
        max_y, max_x = self.stdscr.getmaxyx()
        if row >= max_y:
            return
        line = text[: max_x - 1]
        if highlight:
            self.stdscr.addstr(row, 0, line, curses.A_REVERSE)
        else:
            self.stdscr.addstr(row, 0, line)

    def _max_body_lines(self):
        max_y, _ = self.stdscr.getmaxyx()
        return max(1, max_y - 3)

    def _selected_run(self):
        if not self.runs:
            return None
        return self.runs[self.selected]

    def _prompt(self, label):
        max_y, max_x = self.stdscr.getmaxyx()
        prompt = f"{label}: "
        self.stdscr.addstr(max_y - 1, 0, " " * (max_x - 1))
        self.stdscr.addstr(max_y - 1, 0, prompt)
        curses.echo()
        self.stdscr.nodelay(False)
        value = self.stdscr.getstr(max_y - 1, len(prompt)).decode("utf-8")
        self.stdscr.nodelay(True)
        curses.noecho()
        return value.strip()

    def _control_selected(self, op):
        run = self._selected_run()
        if not run:
            return
        path = fs.run_file(run["run_id"], constants.CONTROL_FILENAME)
        control = fs.read_json(path)
        if control is None or isinstance(control, dict) and control.get("_corrupt"):
            control = {}
        if op == "pause":
            control["paused"] = True
        elif op == "restart":
            control["restart_requested"] = True
            control["paused"] = False
            control["stop_requested"] = False
        elif op == "stop":
            control["stop_requested"] = True
        control["updated_at"] = int(time.time())
        fs.atomic_write_json(path, control)

    def _blacklist_selected(self):
        run = self._selected_run()
        if not run:
            return
        meta = self.run_details.get(run["run_id"], {}).get("meta") or {}
        node = meta.get("last_node") or meta.get("slurm_node")
        if not node:
            node = self._prompt("node to blacklist")
        if node:
            blacklist.add_node(node)

    def _new_run(self):
        run_id = self._prompt("run_id")
        if not run_id:
            return
        run_mode = self._prompt("run_mode (run_once|indefinite)") or "run_once"
        sbatch_script = self._prompt("sbatch script path")
        sbatch_args = self._prompt("sbatch args (optional)")
        keep_alive = None
        if run_mode == "indefinite":
            keep_alive = self._prompt("keep_alive_sec")
        run_dir = fs.run_dir(run_id)
        os.makedirs(run_dir, exist_ok=True)
        meta = {
            "run_id": run_id,
            "run_mode": run_mode,
            "sbatch_script": sbatch_script,
            "sbatch_args": sbatch_args,
            "keep_alive_sec": int(keep_alive) if keep_alive else None,
            "created_at": int(time.time()),
        }
        fs.atomic_write_json(fs.run_file(run_id, constants.META_FILENAME), meta)

    def _edit_config(self):
        run = self._selected_run()
        if not run:
            return
        key = self._prompt("config key")
        value = self._prompt("config value")
        if not key:
            return
        path = fs.run_file(run["run_id"], constants.CONTROL_FILENAME)
        control = fs.read_json(path)
        if control is None or isinstance(control, dict) and control.get("_corrupt"):
            control = {}
        overrides = dict(control.get("config_overrides") or {})
        overrides[key] = value
        control["config_overrides"] = overrides
        control["updated_at"] = int(time.time())
        fs.atomic_write_json(path, control)


def _tail_lines(path, limit=200):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            lines = handle.read().splitlines()
        return lines[-limit:]
    except FileNotFoundError:
        return []
    except Exception:
        return []

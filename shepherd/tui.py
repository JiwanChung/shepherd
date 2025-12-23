import curses
import json
import os
import subprocess
import time

from shepherd import blacklist
from shepherd import constants
from shepherd import fs
from shepherd import heartbeat
from shepherd import status as status_mod


def run_tui():
    curses.wrapper(_main)


def _main(stdscr):
    app = TUIApp(stdscr)
    app.run()


# Status icons (modern)
STATUS_ICONS = {
    "healthy_running": "●",
    "running_degraded": "◐",
    "pending": "○",
    "restarting": "↻",
    "unresponsive": "✗",
    "crash_loop": "!",
    "completed_success": "✓",
    "ended_expired": "◇",
    "stopped_manual": "■",
    "error_unknown": "?",
}

# Status to color type mapping
STATUS_TYPES = {
    "healthy_running": "ok",
    "running_degraded": "warn",
    "pending": "dim",
    "restarting": "warn",
    "unresponsive": "error",
    "crash_loop": "error",
    "completed_success": "ok",
    "ended_expired": "dim",
    "stopped_manual": "dim",
    "error_unknown": "error",
}


def _format_ago(timestamp):
    """Format timestamp as 'X ago'."""
    if timestamp is None:
        return "—"
    ago = int(time.time()) - int(timestamp)
    if ago < 0:
        return "now"
    if ago < 60:
        return f"{ago}s"
    if ago < 3600:
        return f"{ago // 60}m"
    if ago < 86400:
        return f"{ago // 3600}h"
    return f"{ago // 86400}d"


def _get_slurm_job_info(job_id):
    """Get detailed job info from squeue."""
    if not job_id:
        return None
    try:
        result = subprocess.run(
            ["squeue", "-j", str(job_id), "-o", "%i|%P|%j|%u|%T|%M|%l|%D|%R|%C|%m", "--noheader"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None
        parts = result.stdout.strip().split("|")
        if len(parts) >= 10:
            return {
                "job_id": parts[0],
                "partition": parts[1],
                "name": parts[2],
                "user": parts[3],
                "state": parts[4],
                "time": parts[5],
                "time_limit": parts[6],
                "nodes": parts[7],
                "nodelist": parts[8],
                "cpus": parts[9],
                "memory": parts[10] if len(parts) > 10 else "—",
            }
    except Exception:
        pass
    return None


def _read_script(path):
    """Read sbatch script content."""
    if not path:
        return []
    try:
        expanded = os.path.expanduser(path)
        with open(expanded, "r", encoding="utf-8", errors="replace") as f:
            return f.read().splitlines()
    except Exception:
        return []


def _parse_sbatch_output_paths(script_path):
    """Parse #SBATCH --output and --error from sbatch script."""
    stdout_path = None
    stderr_path = None
    if not script_path:
        return stdout_path, stderr_path
    try:
        expanded = os.path.expanduser(script_path)
        script_dir = os.path.dirname(expanded)
        with open(expanded, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if line.startswith("#SBATCH"):
                    if "--output=" in line:
                        stdout_path = line.split("--output=")[1].split()[0]
                    elif " -o " in line:
                        parts = line.split(" -o ")
                        if len(parts) > 1:
                            stdout_path = parts[1].split()[0]
                    if "--error=" in line:
                        stderr_path = line.split("--error=")[1].split()[0]
                    elif " -e " in line:
                        parts = line.split(" -e ")
                        if len(parts) > 1:
                            stderr_path = parts[1].split()[0]
        if stdout_path and not os.path.isabs(stdout_path):
            stdout_path = os.path.join(script_dir, stdout_path)
        if stderr_path and not os.path.isabs(stderr_path):
            stderr_path = os.path.join(script_dir, stderr_path)
    except Exception:
        pass
    return stdout_path, stderr_path


def _find_slurm_output(script_path, job_id, log_type="stdout"):
    """Find the Slurm output file for a job."""
    stdout_path, stderr_path = _parse_sbatch_output_paths(script_path)

    if log_type == "stdout" and stdout_path:
        expanded = os.path.expanduser(stdout_path)
        if job_id:
            expanded = expanded.replace("%j", str(job_id))
        if os.path.exists(expanded):
            return expanded

    if log_type == "stderr" and stderr_path:
        expanded = os.path.expanduser(stderr_path)
        if job_id:
            expanded = expanded.replace("%j", str(job_id))
        if os.path.exists(expanded):
            return expanded

    if job_id:
        if script_path:
            script_dir = os.path.dirname(os.path.expanduser(script_path))
            default_out = os.path.join(script_dir, f"slurm-{job_id}.out")
            if os.path.exists(default_out):
                return default_out
        default_out = f"slurm-{job_id}.out"
        if os.path.exists(default_out):
            return default_out

    return None


def _tail_lines(path, limit=200):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read().splitlines()[-limit:]
    except Exception:
        return []


class TUIApp:
    def __init__(self, stdscr):
        self.stdscr = stdscr
        self._init_colors()

        # State
        self.mode = "dashboard"
        self.selected = 0
        self.scroll_offset = 0
        self.filter_text = ""
        self.sort_by = "status"
        self.sort_reverse = False

        # Right panel
        self.right_panel_mode = "info"
        self.script_scroll = 0
        self.log_scroll = 0
        self.log_follow = True
        self.log_view = "stdout"

        # Data
        self.runs = []
        self.run_details = {}
        self.blacklist_data = {}
        self.slurm_info = {}
        self.script_cache = {}

        # Refresh
        self.refresh_interval = 2
        self.last_refresh = 0

        # Message
        self.message = None
        self.message_type = ""
        self.message_time = 0

    def _init_colors(self):
        curses.start_color()
        curses.use_default_colors()

        # Modern 256-color scheme
        if curses.COLORS >= 256:
            curses.init_pair(1, 16, 255)    # Selected: dark on light
            curses.init_pair(2, 203, -1)    # Error: soft red
            curses.init_pair(3, 114, -1)    # OK: soft green
            curses.init_pair(4, 220, -1)    # Warn: soft yellow
            curses.init_pair(5, 75, -1)     # Accent: soft blue
            curses.init_pair(6, 245, -1)    # Dim: gray
            curses.init_pair(7, 255, 237)   # Bar: light on dark
            curses.init_pair(8, 183, -1)    # Purple
        else:
            curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE)
            curses.init_pair(2, curses.COLOR_RED, -1)
            curses.init_pair(3, curses.COLOR_GREEN, -1)
            curses.init_pair(4, curses.COLOR_YELLOW, -1)
            curses.init_pair(5, curses.COLOR_CYAN, -1)
            curses.init_pair(6, curses.COLOR_WHITE, -1)
            curses.init_pair(7, curses.COLOR_BLACK, curses.COLOR_WHITE)
            curses.init_pair(8, curses.COLOR_MAGENTA, -1)

        self.COL_SEL = curses.color_pair(1)
        self.COL_ERR = curses.color_pair(2)
        self.COL_OK = curses.color_pair(3)
        self.COL_WARN = curses.color_pair(4)
        self.COL_ACC = curses.color_pair(5)
        self.COL_DIM = curses.color_pair(6)
        self.COL_BAR = curses.color_pair(7)
        self.COL_PUR = curses.color_pair(8)

    def _status_color(self, status):
        t = STATUS_TYPES.get(status, "dim")
        if t == "ok":
            return self.COL_OK
        elif t == "warn":
            return self.COL_WARN
        elif t == "error":
            return self.COL_ERR
        return self.COL_DIM

    def run(self):
        curses.curs_set(0)
        self.stdscr.nodelay(True)
        self.stdscr.keypad(True)

        while True:
            now = time.time()
            if now - self.last_refresh >= self.refresh_interval:
                self._refresh_data()
                self.last_refresh = now

            self._render()

            key = self.stdscr.getch()
            if key == -1:
                time.sleep(0.05)
                continue

            if self._handle_key(key):
                break

    def _refresh_data(self):
        fs.ensure_dirs()
        runs = []
        details = {}

        for run_id in fs.list_runs():
            meta = fs.read_json(fs.run_file(run_id, constants.META_FILENAME))
            control = fs.read_json(fs.run_file(run_id, constants.CONTROL_FILENAME))
            ended = fs.read_json(fs.run_file(run_id, constants.ENDED_FILENAME))
            final = fs.read_json(fs.run_file(run_id, constants.FINAL_FILENAME))
            failure = fs.read_json(fs.run_file(run_id, constants.FAILURE_FILENAME))
            hb_ts = heartbeat.read_heartbeat(fs.run_file(run_id, constants.HEARTBEAT_FILENAME))

            meta = meta if isinstance(meta, dict) else {}
            slurm_state = meta.get("slurm_state")

            status_value = status_mod.compute_status(
                meta,
                control if isinstance(control, dict) else None,
                ended if isinstance(ended, dict) else None,
                final if isinstance(final, dict) else None,
                hb_ts,
                slurm_state,
            )

            if self.filter_text and self.filter_text.lower() not in run_id.lower():
                continue

            entry = {
                "run_id": run_id,
                "status": status_value,
                "run_mode": meta.get("run_mode", "—"),
                "job_id": meta.get("slurm_job_id"),
                "partition": meta.get("current_partition"),
                "node": meta.get("last_node") or meta.get("slurm_reason"),
                "heartbeat": hb_ts,
                "restart_count": meta.get("restart_count", 0),
                "started_at": meta.get("started_at"),
                "sbatch_script": meta.get("sbatch_script"),
            }
            runs.append(entry)
            details[run_id] = {
                "meta": meta,
                "control": control,
                "ended": ended,
                "final": final,
                "failure": failure,
                "heartbeat": hb_ts,
            }

            job_id = meta.get("slurm_job_id")
            if job_id and status_value in ("healthy_running", "running_degraded", "pending"):
                self.slurm_info[run_id] = _get_slurm_job_info(job_id)

        # Sort
        def sort_key(r):
            if self.sort_by == "name":
                return r["run_id"]
            elif self.sort_by == "job_id":
                return r["job_id"] or ""
            else:
                order = {
                    "healthy_running": 0, "running_degraded": 1, "pending": 2,
                    "restarting": 3, "unresponsive": 4, "crash_loop": 5,
                    "stopped_manual": 6, "completed_success": 7,
                    "ended_expired": 8, "error_unknown": 9,
                }
                return (order.get(r["status"], 99), r["run_id"])

        runs.sort(key=sort_key, reverse=self.sort_reverse)
        self.runs = runs
        self.run_details = details
        self.blacklist_data = blacklist.load_blacklist()

        if self.selected >= len(self.runs):
            self.selected = max(0, len(self.runs) - 1)

    def _handle_key(self, key):
        self.message = None

        if key in (ord("q"), ord("Q")):
            return True
        if key == ord("?"):
            self.mode = "help"
            return False
        if key == ord("R"):
            self._refresh_data()
            self._show_message("Refreshed", "ok")
            return False
        if key == 27:
            if self.mode != "dashboard":
                self.mode = "dashboard"
            return False

        if key == ord("\t") or key == 9:
            modes = ["info", "script", "logs"]
            idx = modes.index(self.right_panel_mode)
            self.right_panel_mode = modes[(idx + 1) % len(modes)]
            self.script_scroll = 0
            self.log_scroll = 0
            return False

        if self.mode == "dashboard":
            self._handle_dashboard_key(key)
        elif self.mode == "detail":
            self._handle_detail_key(key)
        elif self.mode == "blacklist":
            self._handle_blacklist_key(key)
        elif self.mode == "help":
            self.mode = "dashboard"

        return False

    def _handle_dashboard_key(self, key):
        max_visible = self._list_height()

        if key in (ord("j"), curses.KEY_DOWN):
            if self.selected < len(self.runs) - 1:
                self.selected += 1
                self.script_scroll = 0
                self.log_scroll = 0
                if self.selected >= self.scroll_offset + max_visible:
                    self.scroll_offset = self.selected - max_visible + 1
        elif key in (ord("k"), curses.KEY_UP):
            if self.selected > 0:
                self.selected -= 1
                self.script_scroll = 0
                self.log_scroll = 0
                if self.selected < self.scroll_offset:
                    self.scroll_offset = self.selected
        elif key == ord("g"):
            self.selected = 0
            self.scroll_offset = 0
        elif key == ord("G"):
            self.selected = max(0, len(self.runs) - 1)
            self.scroll_offset = max(0, len(self.runs) - max_visible)
        elif key == 21:  # Ctrl+u
            self.selected = max(0, self.selected - max_visible // 2)
            self.scroll_offset = max(0, self.scroll_offset - max_visible // 2)
        elif key == 4:  # Ctrl+d
            self.selected = min(len(self.runs) - 1, self.selected + max_visible // 2)
            if self.selected >= self.scroll_offset + max_visible:
                self.scroll_offset = self.selected - max_visible + 1
        elif key in (curses.KEY_ENTER, 10, 13):
            if self._selected_run():
                self.mode = "detail"
        elif key == ord("r"):
            self._control_selected("restart")
        elif key == ord("s"):
            self._control_selected("stop")
        elif key == ord("p"):
            self._control_selected("pause")
        elif key == ord("u"):
            self._control_selected("unpause")
        elif key == ord("b"):
            self.mode = "blacklist"
        elif key == ord("n"):
            self._new_run_wizard()
        elif key == ord("/"):
            self.filter_text = self._prompt("Filter: ")
            self._refresh_data()
        elif key == ord("o"):
            sorts = ["status", "name", "job_id"]
            idx = sorts.index(self.sort_by)
            self.sort_by = sorts[(idx + 1) % len(sorts)]
            self._refresh_data()
            self._show_message(f"Sort: {self.sort_by}", "ok")
        elif key == ord("1"):
            self.log_view = "stdout"
        elif key == ord("2"):
            self.log_view = "stderr"
        elif key in (ord("["), ord("-")):
            if self.right_panel_mode == "script":
                self.script_scroll = max(0, self.script_scroll - 1)
            else:
                self.log_scroll = max(0, self.log_scroll - 1)
        elif key in (ord("]"), ord("=")):
            if self.right_panel_mode == "script":
                self.script_scroll += 1
            else:
                self.log_scroll += 1
        elif key == curses.KEY_PPAGE:
            if self.right_panel_mode == "script":
                self.script_scroll = max(0, self.script_scroll - 10)
            else:
                self.log_scroll = max(0, self.log_scroll - 10)
        elif key == curses.KEY_NPAGE:
            if self.right_panel_mode == "script":
                self.script_scroll += 10
            else:
                self.log_scroll += 10

    def _handle_detail_key(self, key):
        if key == ord("r"):
            self._control_selected("restart")
        elif key == ord("s"):
            self._control_selected("stop")
        elif key == ord("p"):
            self._control_selected("pause")
        elif key == ord("u"):
            self._control_selected("unpause")
        elif key == ord("d"):
            self._delete_run()
        elif key in (curses.KEY_BACKSPACE, 127, ord("h"), 27):
            self.mode = "dashboard"

    def _handle_blacklist_key(self, key):
        if key == ord("a"):
            node = self._prompt("Add node: ")
            if node:
                reason = self._prompt("Reason: ") or "manual"
                blacklist.add_node(node, reason=reason)
                self._refresh_data()
                self._show_message(f"Added {node}", "ok")
        elif key == ord("d"):
            node = self._prompt("Remove node: ")
            if node:
                blacklist.remove_node(node)
                self._refresh_data()
                self._show_message(f"Removed {node}", "ok")
        elif key in (curses.KEY_BACKSPACE, 127, 27):
            self.mode = "dashboard"

    def _render(self):
        self.stdscr.erase()
        height, width = self.stdscr.getmaxyx()

        if self.mode == "help":
            self._render_help()
        elif self.mode == "blacklist":
            self._render_blacklist()
        elif self.mode == "detail":
            self._render_detail()
        else:
            left_width = min(width // 2, 50)
            right_start = left_width + 1
            right_width = width - right_start

            self._render_left_panel(left_width)
            self._render_divider(left_width, height)
            self._render_right_panel(right_start, right_width)

        self._render_help_bar()
        self.stdscr.refresh()

    def _render_left_panel(self, width):
        height, _ = self.stdscr.getmaxyx()

        # Header
        running = sum(1 for r in self.runs if "running" in r["status"])
        pending = sum(1 for r in self.runs if r["status"] == "pending")

        title = " SHEPHERD "
        header = "─" * width
        mid = (width - len(title)) // 2
        header = header[:mid] + title + header[mid + len(title):]

        self.stdscr.attron(self.COL_ACC | curses.A_BOLD)
        self.stdscr.addstr(0, 0, header[:width])
        self.stdscr.attroff(self.COL_ACC | curses.A_BOLD)

        # Subtitle
        subtitle = f"{len(self.runs)} runs"
        if running:
            subtitle += f" · {running} running"
        if pending:
            subtitle += f" · {pending} pending"
        if self.filter_text:
            subtitle += f" · filter: {self.filter_text}"

        self.stdscr.attron(self.COL_DIM)
        self.stdscr.addstr(1, 2, subtitle[:width - 4])
        self.stdscr.attroff(self.COL_DIM)

        # Column header (responsive to width)
        content_w = width - 4
        if content_w >= 50:
            col_hdr = f"{'RUN':<22} {'JOB':<8} {'HB':<5} {'STATUS'}"
        elif content_w >= 35:
            col_hdr = f"{'RUN':<20} {'JOB':<8} {'STATUS'}"
        else:
            col_hdr = f"{'RUN':<{content_w - 10}} {'STATUS'}"
        self.stdscr.attron(self.COL_DIM)
        self.stdscr.addstr(3, 2, col_hdr[:width - 4])
        self.stdscr.attroff(self.COL_DIM)

        # Runs list
        list_start = 5
        list_height = self._list_height()

        if not self.runs:
            self.stdscr.attron(self.COL_DIM)
            self.stdscr.addstr(list_start + 2, (width - 18) // 2, "No runs found")
            self.stdscr.addstr(list_start + 3, (width - 22) // 2, "Press 'n' to create")
            self.stdscr.attroff(self.COL_DIM)
        else:
            visible = self.runs[self.scroll_offset:self.scroll_offset + list_height]

            for i, run in enumerate(visible):
                idx = self.scroll_offset + i
                y = list_start + i
                if y >= height - 3:
                    break

                is_selected = (idx == self.selected)
                status = run["status"]
                icon = STATUS_ICONS.get(status, "?")
                status_col = self._status_color(status)

                run_id = run["run_id"][:22]
                job_id = str(run["job_id"] or "—")[:8]
                hb = _format_ago(run["heartbeat"])[:5]
                status_text = status.replace("_", " ")[:15]

                prefix = "▸ " if is_selected else "  "

                # Calculate available width for content (leave room for scroll indicator)
                content_width = width - 2

                # Adjust column widths based on available space
                name_w = min(22, content_width - 20)
                job_w = min(8, max(0, content_width - name_w - 12))
                hb_w = min(5, max(0, content_width - name_w - job_w - 8))

                run_id_disp = run_id[:name_w]
                job_id_disp = job_id[:job_w] if job_w > 0 else ""
                hb_disp = hb[:hb_w] if hb_w > 0 else ""

                if is_selected:
                    self.stdscr.attron(self.COL_SEL | curses.A_BOLD)
                    line = f"{prefix}{run_id_disp:<{name_w}}"
                    if job_w > 0:
                        line += f" {job_id_disp:<{job_w}}"
                    if hb_w > 0:
                        line += f" {hb_disp:<{hb_w}}"
                    line += f" {icon} {status_text}"
                    self.stdscr.addstr(y, 0, " " * width)
                    self.stdscr.addstr(y, 0, line[:width - 2])
                    self.stdscr.attroff(self.COL_SEL | curses.A_BOLD)
                else:
                    x = 0
                    self.stdscr.addstr(y, x, prefix)
                    x += len(prefix)
                    self.stdscr.addstr(y, x, run_id_disp.ljust(name_w)[:width - x - 2])
                    x += name_w
                    if job_w > 0 and x < width - 2:
                        self.stdscr.attron(self.COL_DIM)
                        self.stdscr.addstr(y, x, (" " + job_id_disp.ljust(job_w))[:width - x - 2])
                        x += job_w + 1
                        self.stdscr.attroff(self.COL_DIM)
                    if hb_w > 0 and x < width - 2:
                        self.stdscr.attron(self.COL_DIM)
                        self.stdscr.addstr(y, x, (" " + hb_disp.ljust(hb_w))[:width - x - 2])
                        x += hb_w + 1
                        self.stdscr.attroff(self.COL_DIM)
                    if x < width - 2:
                        status_str = f" {icon} {status_text}"
                        self.stdscr.attron(status_col)
                        self.stdscr.addstr(y, x, status_str[:width - x - 2])
                        self.stdscr.attroff(status_col)

            # Scroll indicator
            total = len(self.runs)
            if total > list_height:
                scroll_pct = (self.scroll_offset + list_height / 2) / total
                ind_h = max(1, list_height * list_height // total)
                ind_y = int((list_height - ind_h) * scroll_pct)
                for sy in range(list_height):
                    char = "┃" if ind_y <= sy < ind_y + ind_h else "│"
                    self.stdscr.attron(self.COL_DIM)
                    try:
                        self.stdscr.addstr(list_start + sy, width - 1, char)
                    except curses.error:
                        pass
                    self.stdscr.attroff(self.COL_DIM)

    def _render_divider(self, x, height):
        self.stdscr.attron(self.COL_DIM)
        for row in range(height - 2):
            try:
                self.stdscr.addstr(row, x, "│")
            except curses.error:
                pass
        self.stdscr.attroff(self.COL_DIM)

    def _render_right_panel(self, start_x, width):
        height, _ = self.stdscr.getmaxyx()
        run = self._selected_run()

        # Tabs header
        tabs = [("INFO", "info"), ("SCRIPT", "script"), ("LOGS", "logs")]
        tab_line = ""
        for label, mode in tabs:
            if mode == self.right_panel_mode:
                tab_line += f" [{label}] "
            else:
                tab_line += f"  {label}  "

        self.stdscr.attron(self.COL_ACC | curses.A_BOLD)
        self.stdscr.addstr(0, start_x, tab_line[:width])
        self.stdscr.attroff(self.COL_ACC | curses.A_BOLD)

        if not run:
            self.stdscr.attron(self.COL_DIM)
            self.stdscr.addstr(3, start_x + 2, "Select a run")
            self.stdscr.attroff(self.COL_DIM)
            return

        if self.right_panel_mode == "info":
            self._render_info_panel(start_x, width, run)
        elif self.right_panel_mode == "script":
            self._render_script_panel(start_x, width, run)
        elif self.right_panel_mode == "logs":
            self._render_logs_panel(start_x, width, run)

    def _render_info_panel(self, start_x, width, run):
        height, _ = self.stdscr.getmaxyx()
        details = self.run_details.get(run["run_id"], {})
        meta = details.get("meta") or {}
        failure = details.get("failure")
        slurm = self.slurm_info.get(run["run_id"])

        row = 2

        def section(title):
            nonlocal row
            if row >= height - 3:
                return
            self.stdscr.attron(self.COL_ACC)
            self.stdscr.addstr(row, start_x + 1, f"─ {title} ─")
            self.stdscr.attroff(self.COL_ACC)
            row += 1

        def field(label, value, color=None):
            nonlocal row
            if row >= height - 3:
                return
            self.stdscr.attron(self.COL_DIM)
            self.stdscr.addstr(row, start_x + 2, f"{label}:")
            self.stdscr.attroff(self.COL_DIM)
            col = color or 0
            self.stdscr.attron(col)
            self.stdscr.addstr(row, start_x + 14, str(value)[:width - 16])
            self.stdscr.attroff(col)
            row += 1

        # Status
        status = run["status"]
        icon = STATUS_ICONS.get(status, "?")
        status_col = self._status_color(status)
        field("Status", f"{icon} {status.replace('_', ' ')}", status_col)
        row += 1

        # Slurm section
        section("SLURM")
        if slurm:
            field("Job ID", slurm["job_id"])
            field("State", slurm["state"], self.COL_OK if slurm["state"] == "RUNNING" else self.COL_WARN)
            field("Partition", slurm["partition"])
            field("Node", slurm["nodelist"])
            field("Time", f"{slurm['time']} / {slurm['time_limit']}")
        elif run["job_id"]:
            field("Job ID", run["job_id"])
            self.stdscr.attron(self.COL_DIM)
            self.stdscr.addstr(row, start_x + 2, "(not in queue)")
            self.stdscr.attroff(self.COL_DIM)
            row += 1
        else:
            self.stdscr.attron(self.COL_DIM)
            self.stdscr.addstr(row, start_x + 2, "No active job")
            self.stdscr.attroff(self.COL_DIM)
            row += 1

        row += 1

        # Shepherd section
        section("RUN")
        field("Mode", run["run_mode"])
        field("Restarts", run["restart_count"])
        field("Heartbeat", _format_ago(run["heartbeat"]))
        field("Started", _format_ago(meta.get("started_at")))
        if meta.get("restart_reason"):
            field("Reason", meta["restart_reason"], self.COL_WARN)

        # Partitions
        pf = meta.get("partition_fallback")
        if pf:
            row += 1
            section("PARTITIONS")
            partitions = pf.get("partitions", [])
            current_idx = meta.get("current_partition_index", 0)
            for i, p in enumerate(partitions[:5]):
                marker = "▸" if i == current_idx else " "
                col = self.COL_OK if i == current_idx else self.COL_DIM
                self.stdscr.attron(col)
                self.stdscr.addstr(row, start_x + 2, f"{marker} {p}")
                self.stdscr.attroff(col)
                row += 1

        # Failure
        if failure:
            row += 1
            section("FAILURE")
            field("Exit", failure.get("exit_code"), self.COL_ERR)
            field("Reason", failure.get("reason"), self.COL_ERR)

    def _render_script_panel(self, start_x, width, run):
        height, _ = self.stdscr.getmaxyx()
        script_path = run.get("sbatch_script")

        row = 2
        if not script_path:
            self.stdscr.attron(self.COL_DIM)
            self.stdscr.addstr(row, start_x + 2, "No script configured")
            self.stdscr.attroff(self.COL_DIM)
            return

        # Path
        path_display = script_path if len(script_path) < width - 4 else "..." + script_path[-(width - 7):]
        self.stdscr.attron(self.COL_DIM)
        self.stdscr.addstr(row, start_x + 1, path_display)
        self.stdscr.attroff(self.COL_DIM)
        row += 2

        # Script content
        if script_path not in self.script_cache:
            self.script_cache[script_path] = _read_script(script_path)

        lines = self.script_cache.get(script_path, [])
        if not lines:
            self.stdscr.attron(self.COL_ERR)
            self.stdscr.addstr(row, start_x + 1, "Cannot read script")
            self.stdscr.attroff(self.COL_ERR)
            return

        visible = lines[self.script_scroll:]
        for i, line in enumerate(visible):
            if row >= height - 3:
                break

            line_num = self.script_scroll + i + 1
            self.stdscr.attron(self.COL_DIM)
            self.stdscr.addstr(row, start_x + 1, f"{line_num:3} ")
            self.stdscr.attroff(self.COL_DIM)

            content = line[:width - 6]
            if line.startswith("#SBATCH"):
                self.stdscr.attron(self.COL_OK)
            elif line.startswith("#SHEPHERD"):
                self.stdscr.attron(self.COL_PUR)
            elif line.startswith("#"):
                self.stdscr.attron(self.COL_DIM)
            else:
                self.stdscr.attron(0)

            self.stdscr.addstr(row, start_x + 5, content)
            self.stdscr.attroff(self.COL_OK | self.COL_PUR | self.COL_DIM)
            row += 1

    def _render_logs_panel(self, start_x, width, run):
        height, _ = self.stdscr.getmaxyx()
        details = self.run_details.get(run["run_id"], {})
        meta = details.get("meta") or {}

        row = 2

        # Log type tabs
        self.stdscr.addstr(row, start_x + 1, "")
        for name, key in [("stdout", "1"), ("stderr", "2")]:
            if name == self.log_view:
                self.stdscr.attron(curses.A_BOLD)
                self.stdscr.addstr(f" [{key}:{name}] ")
                self.stdscr.attroff(curses.A_BOLD)
            else:
                self.stdscr.attron(self.COL_DIM)
                self.stdscr.addstr(f"  {key}:{name}  ")
                self.stdscr.attroff(self.COL_DIM)
        row += 2

        # Find log file
        path = meta.get(f"{self.log_view}_path")
        if path and os.path.exists(os.path.expanduser(path)):
            source = "meta"
        else:
            path = None
            script_path = meta.get("sbatch_script")
            job_id = meta.get("slurm_job_id")
            path = _find_slurm_output(script_path, job_id, self.log_view)
            source = "slurm" if path else None

        if not path:
            fallback = fs.run_file(run["run_id"], f"{self.log_view}.log")
            if os.path.exists(fallback):
                path = fallback
                source = "shepherd"

        if not path:
            self.stdscr.attron(self.COL_DIM)
            self.stdscr.addstr(row, start_x + 2, f"No {self.log_view} logs found")
            self.stdscr.attroff(self.COL_DIM)
            return

        # Show path
        self.stdscr.attron(self.COL_DIM)
        path_str = f"[{source}] {path}"
        if len(path_str) > width - 4:
            path_str = f"[{source}] ...{path[-(width - 12):]}"
        self.stdscr.addstr(row, start_x + 1, path_str[:width - 2])
        self.stdscr.attroff(self.COL_DIM)
        row += 1

        # Log content
        lines = _tail_lines(path, limit=500)
        if not lines:
            self.stdscr.attron(self.COL_DIM)
            self.stdscr.addstr(row, start_x + 2, "(empty)")
            self.stdscr.attroff(self.COL_DIM)
            return

        avail = height - row - 2
        if self.log_follow:
            visible = lines[-avail:]
        else:
            end = len(lines) - self.log_scroll
            start = max(0, end - avail)
            visible = lines[start:end]

        for line in visible:
            if row >= height - 2:
                break
            self.stdscr.addstr(row, start_x + 1, line[:width - 2])
            row += 1

    def _render_detail(self):
        height, width = self.stdscr.getmaxyx()
        run = self._selected_run()
        if not run:
            return

        # Header
        title = f" {run['run_id']} "
        header = "─" * width
        mid = (width - len(title)) // 2
        header = header[:mid] + title + header[mid + len(title):]
        self.stdscr.attron(self.COL_ACC | curses.A_BOLD)
        self.stdscr.addstr(0, 0, header)
        self.stdscr.attroff(self.COL_ACC | curses.A_BOLD)

        # Use full width for info
        self._render_info_panel(0, width, run)

    def _render_blacklist(self):
        height, width = self.stdscr.getmaxyx()
        nodes = self.blacklist_data.get("nodes", {})

        title = f" BLACKLIST ({len(nodes)}) "
        header = "─" * width
        mid = (width - len(title)) // 2
        header = header[:mid] + title + header[mid + len(title):]
        self.stdscr.attron(self.COL_ACC | curses.A_BOLD)
        self.stdscr.addstr(0, 0, header)
        self.stdscr.attroff(self.COL_ACC | curses.A_BOLD)

        row = 3
        self.stdscr.attron(self.COL_DIM)
        self.stdscr.addstr(row, 2, f"{'NODE':<20}{'REASON':<30}{'ADDED'}")
        self.stdscr.attroff(self.COL_DIM)
        row += 1

        for node, info in sorted(nodes.items()):
            if row >= height - 2:
                break
            reason = (info.get("reason") or "—")[:28]
            added = _format_ago(info.get("added_at"))
            self.stdscr.addstr(row, 2, f"{node:<20}")
            self.stdscr.attron(self.COL_DIM)
            self.stdscr.addstr(f"{reason:<30}{added}")
            self.stdscr.attroff(self.COL_DIM)
            row += 1

        if not nodes:
            self.stdscr.attron(self.COL_OK)
            self.stdscr.addstr(5, (width - 20) // 2, "No blacklisted nodes")
            self.stdscr.attroff(self.COL_OK)

    def _render_help(self):
        height, width = self.stdscr.getmaxyx()

        title = " HELP "
        header = "─" * width
        mid = (width - len(title)) // 2
        header = header[:mid] + title + header[mid + len(title):]
        self.stdscr.attron(self.COL_ACC | curses.A_BOLD)
        self.stdscr.addstr(0, 0, header)
        self.stdscr.attroff(self.COL_ACC | curses.A_BOLD)

        sections = [
            ("NAVIGATION", [
                ("j/k ↑/↓", "Move up/down"),
                ("g / G", "Top / Bottom"),
                ("^u / ^d", "Half page up/down"),
                ("Enter", "Detail view"),
                ("Tab", "Cycle panel: Info → Script → Logs"),
                ("[ ]  - =", "Scroll panel"),
            ]),
            ("ACTIONS", [
                ("r", "Restart run"),
                ("s", "Stop run"),
                ("p / u", "Pause / Unpause"),
                ("n", "New run"),
                ("b", "Blacklist"),
            ]),
            ("OTHER", [
                ("/", "Filter runs"),
                ("o", "Sort order"),
                ("R", "Refresh"),
                ("q", "Quit"),
            ]),
        ]

        row = 2
        for section, items in sections:
            if row >= height - 3:
                break
            self.stdscr.attron(self.COL_ACC | curses.A_BOLD)
            self.stdscr.addstr(row, 2, section)
            self.stdscr.attroff(self.COL_ACC | curses.A_BOLD)
            row += 1

            for key, desc in items:
                if row >= height - 3:
                    break
                self.stdscr.attron(curses.A_BOLD)
                self.stdscr.addstr(row, 4, f"{key:<12}")
                self.stdscr.attroff(curses.A_BOLD)
                self.stdscr.attron(self.COL_DIM)
                self.stdscr.addstr(desc)
                self.stdscr.attroff(self.COL_DIM)
                row += 1
            row += 1

    def _render_help_bar(self):
        height, width = self.stdscr.getmaxyx()

        if self.mode == "dashboard":
            keys = [("j/k", "nav"), ("Tab", "panel"), ("r", "restart"), ("n", "new"), ("?", "help"), ("q", "quit")]
        elif self.mode == "detail":
            keys = [("r", "restart"), ("s", "stop"), ("d", "delete"), ("Bksp", "back")]
        elif self.mode == "blacklist":
            keys = [("a", "add"), ("d", "remove"), ("Bksp", "back")]
        else:
            keys = [("any key", "back")]

        # Message area
        msg_y = height - 2
        if self.message and time.time() - self.message_time < 3:
            msg_col = self.COL_OK if self.message_type == "ok" else self.COL_ERR
            self.stdscr.attron(msg_col)
            self.stdscr.addstr(msg_y, 2, self.message[:width - 4])
            self.stdscr.attroff(msg_col)

        # Help bar
        self.stdscr.attron(self.COL_BAR)
        self.stdscr.addstr(height - 1, 0, " " * (width - 1))
        x = 1
        for key, desc in keys:
            if x + len(key) + len(desc) + 4 > width:
                break
            self.stdscr.addstr(height - 1, x, f" {key} ")
            self.stdscr.attroff(self.COL_BAR)
            self.stdscr.attron(self.COL_DIM)
            self.stdscr.addstr(f"{desc}  ")
            self.stdscr.attroff(self.COL_DIM)
            self.stdscr.attron(self.COL_BAR)
            x += len(key) + len(desc) + 5
        self.stdscr.attroff(self.COL_BAR)

    def _list_height(self):
        height, _ = self.stdscr.getmaxyx()
        return max(1, height - 8)

    def _selected_run(self):
        if not self.runs or self.selected >= len(self.runs):
            return None
        return self.runs[self.selected]

    def _prompt(self, label):
        height, width = self.stdscr.getmaxyx()
        self.stdscr.addstr(height - 2, 0, " " * (width - 1))
        self.stdscr.addstr(height - 2, 2, label, curses.A_BOLD)
        self.stdscr.addstr("▌")
        curses.echo()
        curses.curs_set(1)
        self.stdscr.nodelay(False)
        try:
            value = self.stdscr.getstr(height - 2, len(label) + 3, 60).decode("utf-8")
        except KeyboardInterrupt:
            value = ""
        self.stdscr.nodelay(True)
        curses.noecho()
        curses.curs_set(0)
        return value.strip()

    def _confirm(self, message):
        height, width = self.stdscr.getmaxyx()
        self.stdscr.addstr(height - 2, 0, " " * (width - 1))
        self.stdscr.attron(self.COL_WARN | curses.A_BOLD)
        self.stdscr.addstr(height - 2, 2, f"{message} [y/N]: ")
        self.stdscr.attroff(self.COL_WARN | curses.A_BOLD)
        self.stdscr.nodelay(False)
        key = self.stdscr.getch()
        self.stdscr.nodelay(True)
        return key in (ord("y"), ord("Y"))

    def _show_message(self, msg, msg_type=""):
        self.message = msg
        self.message_type = msg_type
        self.message_time = time.time()

    def _control_selected(self, op):
        run = self._selected_run()
        if not run:
            return

        path = fs.run_file(run["run_id"], constants.CONTROL_FILENAME)
        control = fs.read_json(path)
        if control is None or (isinstance(control, dict) and control.get("_corrupt")):
            control = {}

        if op == "pause":
            control["paused"] = True
            self._show_message(f"Paused {run['run_id']}", "ok")
        elif op == "unpause":
            control["paused"] = False
            self._show_message(f"Unpaused {run['run_id']}", "ok")
        elif op == "restart":
            control["restart_requested"] = True
            control["paused"] = False
            control["stop_requested"] = False
            self._show_message(f"Restarting {run['run_id']}", "ok")
        elif op == "stop":
            control["stop_requested"] = True
            self._show_message(f"Stopping {run['run_id']}", "ok")

        control["updated_at"] = int(time.time())
        fs.atomic_write_json(path, control)
        self._refresh_data()

    def _delete_run(self):
        run = self._selected_run()
        if not run:
            return

        if not self._confirm(f"Delete '{run['run_id']}'?"):
            return

        details = self.run_details.get(run["run_id"], {})
        meta = details.get("meta") or {}
        job_id = meta.get("slurm_job_id")
        if job_id:
            try:
                from shepherd import slurm
                slurm.scancel(job_id)
            except Exception:
                pass

        import shutil
        run_dir = fs.run_dir(run["run_id"])
        try:
            shutil.rmtree(run_dir)
            self._show_message(f"Deleted {run['run_id']}", "ok")
        except Exception as e:
            self._show_message(f"Error: {e}", "error")

        self._refresh_data()
        self.mode = "dashboard"

    def _new_run_wizard(self):
        run_id = self._prompt("Run ID: ")
        if not run_id:
            return

        sbatch_script = self._prompt("Script path: ")
        if not sbatch_script:
            return

        run_dir = fs.run_dir(run_id)
        os.makedirs(run_dir, exist_ok=True)

        meta = {
            "run_id": run_id,
            "run_mode": "run_once",
            "sbatch_script": sbatch_script,
            "created_at": int(time.time()),
        }

        fs.atomic_write_json(fs.run_file(run_id, constants.META_FILENAME), meta)
        self._show_message(f"Created {run_id}", "ok")
        self._refresh_data()

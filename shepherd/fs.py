import json
import os
import tempfile

try:
    import fcntl
except ImportError:
    fcntl = None

from shepherd import constants


def ensure_dirs():
    os.makedirs(constants.RUNS_DIR, exist_ok=True)
    os.makedirs(constants.LOCKS_DIR, exist_ok=True)


def run_dir(run_id):
    return os.path.join(constants.RUNS_DIR, run_id)


def run_file(run_id, filename):
    return os.path.join(run_dir(run_id), filename)


def list_runs():
    if not os.path.isdir(constants.RUNS_DIR):
        return []
    runs = []
    for name in os.listdir(constants.RUNS_DIR):
        path = os.path.join(constants.RUNS_DIR, name)
        if os.path.isdir(path):
            runs.append(name)
    runs.sort()
    return runs


class RunLock:
    def __init__(self, run_id):
        self.run_id = run_id
        self._handle = None

    def acquire(self):
        if fcntl is None:
            return True
        os.makedirs(constants.LOCKS_DIR, exist_ok=True)
        lock_path = os.path.join(constants.LOCKS_DIR, f"{self.run_id}.lock")
        self._handle = open(lock_path, "a", encoding="utf-8")
        try:
            fcntl.flock(self._handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            self._handle.close()
            self._handle = None
            return False

    def release(self):
        if self._handle is None or fcntl is None:
            return
        try:
            fcntl.flock(self._handle, fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None

    def __enter__(self):
        if not self.acquire():
            return None
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()


def run_lock(run_id):
    return RunLock(run_id)


def read_text(path):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return handle.read()
    except FileNotFoundError:
        return None
    except Exception as exc:
        return {"_corrupt": True, "_error": str(exc)}


def read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return None
    except Exception as exc:
        return {"_corrupt": True, "_error": str(exc)}


def atomic_write_text(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def atomic_write_json(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=True, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass

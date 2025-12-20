import time

from shepherd import constants
from shepherd import fs


def _now():
    return int(time.time())


def read_heartbeat(path):
    text = fs.read_text(path)
    if text is None:
        return None
    if isinstance(text, dict) and text.get("_corrupt"):
        return None
    try:
        return int(text.strip())
    except ValueError:
        return None


def is_stale(last_beat, interval_sec=None, grace_sec=None, now=None):
    if last_beat is None:
        return True
    if now is None:
        now = _now()
    if interval_sec is None:
        interval_sec = constants.DEFAULT_HEARTBEAT_INTERVAL_SEC
    if grace_sec is None:
        grace_sec = constants.DEFAULT_HEARTBEAT_GRACE_SEC
    return now - last_beat > (interval_sec + grace_sec)

import time

from shepherd import constants
from shepherd import fs


def _now():
    return int(time.time())


def load_blacklist(path=constants.BLACKLIST_PATH):
    data = fs.read_json(path)
    if data is None or isinstance(data, dict) and data.get("_corrupt"):
        return {"nodes": {}, "updated_at": None}
    if "nodes" not in data:
        data["nodes"] = {}
    return data


def save_blacklist(data, path=constants.BLACKLIST_PATH):
    data["updated_at"] = _now()
    fs.atomic_write_json(path, data)


def add_node(node, ttl_sec=None, reason=None, path=constants.BLACKLIST_PATH):
    data = load_blacklist(path)
    expires_at = None
    if ttl_sec is not None:
        expires_at = _now() + int(ttl_sec)
    data["nodes"][node] = {
        "added_at": _now(),
        "expires_at": expires_at,
        "reason": reason,
    }
    save_blacklist(data, path)
    return data


def remove_node(node, path=constants.BLACKLIST_PATH):
    data = load_blacklist(path)
    if node in data["nodes"]:
        del data["nodes"][node]
        save_blacklist(data, path)
    return data


def prune_expired(data):
    now = _now()
    nodes = data.get("nodes", {})
    to_remove = []
    for node, entry in nodes.items():
        expires_at = entry.get("expires_at")
        if expires_at is not None and expires_at <= now:
            to_remove.append(node)
    for node in to_remove:
        del nodes[node]
    return data


def exclude_list(data, limit=constants.DEFAULT_BLACKLIST_LIMIT):
    data = prune_expired(data)
    nodes = list(data.get("nodes", {}).keys())
    nodes.sort()
    if limit is not None:
        nodes = nodes[: int(limit)]
    return nodes

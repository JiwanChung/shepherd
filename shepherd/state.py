from shepherd import constants
from shepherd import fs


def load_run_state(run_id):
    return {
        "run_id": run_id,
        "meta": fs.read_json(fs.run_file(run_id, constants.META_FILENAME)),
        "control": fs.read_json(fs.run_file(run_id, constants.CONTROL_FILENAME)),
        "ended": fs.read_json(fs.run_file(run_id, constants.ENDED_FILENAME)),
        "final": fs.read_json(fs.run_file(run_id, constants.FINAL_FILENAME)),
        "failure": fs.read_json(fs.run_file(run_id, constants.FAILURE_FILENAME)),
        "heartbeat": fs.read_text(fs.run_file(run_id, constants.HEARTBEAT_FILENAME)),
    }


def update_meta(run_id, updates):
    path = fs.run_file(run_id, constants.META_FILENAME)
    meta = fs.read_json(path)
    if meta is None or isinstance(meta, dict) and meta.get("_corrupt"):
        return None
    meta.update(updates)
    fs.atomic_write_json(path, meta)
    return meta


def update_control(run_id, updates):
    path = fs.run_file(run_id, constants.CONTROL_FILENAME)
    control = fs.read_json(path)
    if control is None or isinstance(control, dict) and control.get("_corrupt"):
        control = {}
    control.update(updates)
    fs.atomic_write_json(path, control)
    return control


def write_ended(run_id, payload):
    path = fs.run_file(run_id, constants.ENDED_FILENAME)
    fs.atomic_write_json(path, payload)


def write_final(run_id):
    import time
    path = fs.run_file(run_id, constants.FINAL_FILENAME)
    fs.atomic_write_json(path, {"timestamp": int(time.time())})

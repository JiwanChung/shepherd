import os
import tempfile
import unittest

from shepherd import constants
from shepherd import fs
from shepherd import state


class StateTests(unittest.TestCase):
    def setUp(self):
        self._orig_state = (constants.STATE_DIR, constants.RUNS_DIR, constants.LOCKS_DIR, constants.BLACKLIST_PATH)
        self.tempdir = tempfile.TemporaryDirectory()
        constants.set_state_dir(self.tempdir.name)
        fs.ensure_dirs()

    def tearDown(self):
        constants.STATE_DIR, constants.RUNS_DIR, constants.LOCKS_DIR, constants.BLACKLIST_PATH = self._orig_state
        self.tempdir.cleanup()

    def test_load_run_state(self):
        run_id = "run-1"
        os.makedirs(fs.run_dir(run_id), exist_ok=True)
        fs.atomic_write_json(fs.run_file(run_id, constants.META_FILENAME), {"run_id": run_id})
        data = state.load_run_state(run_id)
        self.assertEqual(data["meta"]["run_id"], run_id)

    def test_update_meta_skips_corrupt(self):
        run_id = "run-2"
        os.makedirs(fs.run_dir(run_id), exist_ok=True)
        path = fs.run_file(run_id, constants.META_FILENAME)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("{invalid-json")
        result = state.update_meta(run_id, {"foo": "bar"})
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()

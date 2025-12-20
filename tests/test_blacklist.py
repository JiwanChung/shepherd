import tempfile
import unittest

from shepherd import blacklist
from shepherd import constants


class BlacklistTests(unittest.TestCase):
    def setUp(self):
        self._orig_state = (constants.STATE_DIR, constants.RUNS_DIR, constants.LOCKS_DIR, constants.BLACKLIST_PATH)
        self.tempdir = tempfile.TemporaryDirectory()
        constants.set_state_dir(self.tempdir.name)

    def tearDown(self):
        constants.STATE_DIR, constants.RUNS_DIR, constants.LOCKS_DIR, constants.BLACKLIST_PATH = self._orig_state
        self.tempdir.cleanup()

    def test_add_and_remove(self):
        blacklist.add_node("node-a")
        data = blacklist.load_blacklist()
        self.assertIn("node-a", data["nodes"])
        blacklist.remove_node("node-a")
        data = blacklist.load_blacklist()
        self.assertNotIn("node-a", data["nodes"])

    def test_prune_expired(self):
        data = {"nodes": {"node-a": {"expires_at": 1}, "node-b": {"expires_at": None}}}
        pruned = blacklist.prune_expired(data)
        self.assertNotIn("node-a", pruned["nodes"])
        self.assertIn("node-b", pruned["nodes"])


if __name__ == "__main__":
    unittest.main()

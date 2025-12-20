import os
import tempfile
import unittest

from shepherd import heartbeat
from shepherd import fs


class HeartbeatTests(unittest.TestCase):
    def test_read_heartbeat(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "heartbeat")
            fs.atomic_write_text(path, "123\n")
            self.assertEqual(heartbeat.read_heartbeat(path), 123)

    def test_is_stale(self):
        now = 200
        last = 100
        self.assertTrue(heartbeat.is_stale(last, interval_sec=30, grace_sec=10, now=now))
        self.assertFalse(heartbeat.is_stale(last, interval_sec=150, grace_sec=10, now=now))


if __name__ == "__main__":
    unittest.main()

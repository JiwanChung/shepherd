import unittest

from shepherd import backoff


class BackoffTests(unittest.TestCase):
    def test_backoff_growth(self):
        self.assertEqual(backoff.compute_backoff(0, base_sec=10, max_sec=300), 0)
        self.assertEqual(backoff.compute_backoff(1, base_sec=10, max_sec=300), 20)
        self.assertEqual(backoff.compute_backoff(2, base_sec=10, max_sec=300), 40)
        self.assertEqual(backoff.compute_backoff(6, base_sec=10, max_sec=300), 300)


if __name__ == "__main__":
    unittest.main()

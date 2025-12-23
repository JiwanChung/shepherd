import os
import tempfile
import unittest
from unittest import mock

from shepherd import constants
from shepherd import daemon
from shepherd import fs


class DaemonTests(unittest.TestCase):
    def setUp(self):
        self._orig_state = (
            constants.STATE_DIR,
            constants.RUNS_DIR,
            constants.LOCKS_DIR,
            constants.BLACKLIST_PATH,
        )
        self.tempdir = tempfile.TemporaryDirectory()
        constants.set_state_dir(self.tempdir.name)
        fs.ensure_dirs()

    def tearDown(self):
        constants.STATE_DIR, constants.RUNS_DIR, constants.LOCKS_DIR, constants.BLACKLIST_PATH = self._orig_state
        self.tempdir.cleanup()

    def _write_meta(self, run_id, meta):
        os.makedirs(fs.run_dir(run_id), exist_ok=True)
        fs.atomic_write_json(fs.run_file(run_id, constants.META_FILENAME), meta)

    def test_submit_on_missing_job(self):
        run_id = "run-submit"
        meta = {"run_id": run_id, "run_mode": "run_once", "sbatch_script": "/tmp/job.sh"}
        self._write_meta(run_id, meta)
        agent = daemon.ShepherdDaemon()
        with mock.patch("shepherd.slurm.sbatch_script") as sbatch_mock:
            sbatch_mock.return_value = {"ok": True, "stdout": "Submitted batch job 123"}
            agent._handle_run(run_id, meta, {"jobs": {}})
        updated = fs.read_json(fs.run_file(run_id, constants.META_FILENAME))
        self.assertEqual(updated.get("slurm_job_id"), "123")
        self.assertEqual(updated.get("slurm_state"), "PENDING")

    def test_heartbeat_stale_cancels(self):
        run_id = "run-stale"
        meta = {
            "run_id": run_id,
            "run_mode": "run_once",
            "sbatch_script": "/tmp/job.sh",
            "slurm_job_id": "123",
            "heartbeat_interval_sec": 30,
            "heartbeat_grace_sec": 10,
        }
        self._write_meta(run_id, meta)
        fs.atomic_write_text(fs.run_file(run_id, constants.HEARTBEAT_FILENAME), "100\n")
        agent = daemon.ShepherdDaemon()
        slurm_result = {"jobs": {"123": {"state": "RUNNING", "reason": "node-1"}}}
        with mock.patch("shepherd.daemon.time.time", return_value=200):
            with mock.patch("shepherd.slurm.scancel") as scancel_mock:
                agent._handle_run(run_id, meta, slurm_result)
                scancel_mock.assert_called_once_with("123")
        updated = fs.read_json(fs.run_file(run_id, constants.META_FILENAME))
        self.assertEqual(updated.get("restart_count"), 1)
        self.assertGreater(updated.get("next_submit_at"), 200)

    def test_restart_clears_terminal_state(self):
        run_id = "run-restart"
        meta = {"run_id": run_id, "run_mode": "run_once", "sbatch_script": "/tmp/job.sh"}
        self._write_meta(run_id, meta)
        fs.atomic_write_json(fs.run_file(run_id, constants.ENDED_FILENAME), {"reason": "stopped"})
        fs.atomic_write_json(fs.run_file(run_id, constants.FINAL_FILENAME), {"timestamp": 1})
        fs.atomic_write_json(fs.run_file(run_id, constants.FAILURE_FILENAME), {"exit_code": 50})
        fs.atomic_write_json(
            fs.run_file(run_id, constants.CONTROL_FILENAME),
            {"restart_requested": True},
        )
        agent = daemon.ShepherdDaemon()
        with mock.patch("shepherd.slurm.sbatch_script") as sbatch_mock:
            sbatch_mock.return_value = {"ok": True, "stdout": "Submitted batch job 456"}
            agent._handle_run(run_id, meta, {"jobs": {}})
        self.assertIsNone(fs.read_json(fs.run_file(run_id, constants.ENDED_FILENAME)))
        control = fs.read_json(fs.run_file(run_id, constants.CONTROL_FILENAME))
        self.assertFalse(control.get("restart_requested"))

    def test_partition_fallback_advances_on_failure(self):
        """Test that partition advances after retry_per_partition failures."""
        run_id = "run-partition"
        meta = {
            "run_id": run_id,
            "run_mode": "run_once",
            "sbatch_script": "/tmp/job.sh",
            "partition_fallback": {
                "partitions": ["gpu-high", "gpu-low", "cpu"],
                "retry_per_partition": 2,
            },
        }
        self._write_meta(run_id, meta)
        agent = daemon.ShepherdDaemon()

        # First failure on gpu-high
        with mock.patch("shepherd.slurm.sbatch_script") as sbatch_mock:
            sbatch_mock.return_value = {"ok": False, "stderr": "PartitionDown"}
            agent._handle_run(run_id, meta, {"jobs": {}})

        updated = fs.read_json(fs.run_file(run_id, constants.META_FILENAME))
        self.assertEqual(updated.get("partition_failure_count"), 1)
        self.assertEqual(updated.get("current_partition_index", 0), 0)

        # Second failure - should advance to gpu-low and immediate retry succeeds
        with mock.patch("shepherd.slurm.sbatch_script") as sbatch_mock:
            # First call fails (second failure on gpu-high), immediate retry succeeds (gpu-low)
            sbatch_mock.side_effect = [
                {"ok": False, "stderr": "PartitionDown"},
                {"ok": True, "stdout": "Submitted batch job 555"},
            ]
            updated["next_submit_at"] = None  # Allow immediate retry
            agent._handle_run(run_id, updated, {"jobs": {}})

        updated = fs.read_json(fs.run_file(run_id, constants.META_FILENAME))
        self.assertEqual(updated.get("current_partition_index"), 1)
        self.assertEqual(updated.get("partition_failure_count"), 0)
        self.assertEqual(updated.get("current_partition"), "gpu-low")

    def test_partition_fallback_success_resets_count(self):
        """Test that successful submission resets partition_failure_count."""
        run_id = "run-partition-success"
        meta = {
            "run_id": run_id,
            "run_mode": "run_once",
            "sbatch_script": "/tmp/job.sh",
            "partition_fallback": {
                "partitions": ["gpu", "cpu"],
                "retry_per_partition": 2,
            },
            "partition_failure_count": 1,
        }
        self._write_meta(run_id, meta)
        agent = daemon.ShepherdDaemon()

        with mock.patch("shepherd.slurm.sbatch_script") as sbatch_mock:
            sbatch_mock.return_value = {"ok": True, "stdout": "Submitted batch job 789"}
            agent._handle_run(run_id, meta, {"jobs": {}})

        updated = fs.read_json(fs.run_file(run_id, constants.META_FILENAME))
        self.assertEqual(updated.get("partition_failure_count"), 0)
        self.assertEqual(updated.get("current_partition"), "gpu")
        self.assertEqual(updated.get("slurm_job_id"), "789")

    def test_partition_fallback_wraps_around(self):
        """Test that exhausting all partitions wraps to preferred with backoff."""
        run_id = "run-partition-wrap"
        meta = {
            "run_id": run_id,
            "run_mode": "run_once",
            "sbatch_script": "/tmp/job.sh",
            "partition_fallback": {
                "partitions": ["gpu", "cpu"],
                "retry_per_partition": 1,
            },
            "current_partition_index": 1,  # Already on last partition
        }
        self._write_meta(run_id, meta)
        agent = daemon.ShepherdDaemon()

        with mock.patch("shepherd.slurm.sbatch_script") as sbatch_mock:
            sbatch_mock.return_value = {"ok": False, "stderr": "PartitionDown"}
            agent._handle_run(run_id, meta, {"jobs": {}})

        updated = fs.read_json(fs.run_file(run_id, constants.META_FILENAME))
        # Should wrap back to 0 and have backoff applied
        self.assertEqual(updated.get("current_partition_index"), 0)
        self.assertIsNotNone(updated.get("next_submit_at"))

    def test_partition_reset_to_preferred(self):
        """Test that preferred partition is retried after reset_to_preferred_sec."""
        run_id = "run-partition-reset"
        meta = {
            "run_id": run_id,
            "run_mode": "run_once",
            "sbatch_script": "/tmp/job.sh",
            "partition_fallback": {
                "partitions": ["gpu-high", "gpu-low"],
                "retry_per_partition": 2,
                "reset_to_preferred_sec": 60,
            },
            "current_partition_index": 1,
            "last_preferred_attempt_at": 100,
        }
        self._write_meta(run_id, meta)
        agent = daemon.ShepherdDaemon()

        # Time has passed beyond reset_to_preferred_sec
        with mock.patch("shepherd.daemon.time.time", return_value=200):
            with mock.patch("shepherd.slurm.sbatch_script") as sbatch_mock:
                sbatch_mock.return_value = {"ok": True, "stdout": "Submitted batch job 999"}
                agent._handle_run(run_id, meta, {"jobs": {}})

        updated = fs.read_json(fs.run_file(run_id, constants.META_FILENAME))
        # Should have reset to preferred partition (index 0)
        self.assertEqual(updated.get("current_partition"), "gpu-high")


if __name__ == "__main__":
    unittest.main()

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
        with mock.patch("shepherd.slurm.sbatch") as sbatch_mock:
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
        with mock.patch("shepherd.slurm.sbatch") as sbatch_mock:
            sbatch_mock.return_value = {"ok": True, "stdout": "Submitted batch job 456"}
            agent._handle_run(run_id, meta, {"jobs": {}})
        self.assertIsNone(fs.read_json(fs.run_file(run_id, constants.ENDED_FILENAME)))
        control = fs.read_json(fs.run_file(run_id, constants.CONTROL_FILENAME))
        self.assertFalse(control.get("restart_requested"))


if __name__ == "__main__":
    unittest.main()

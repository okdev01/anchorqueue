from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from anchorqueue import LeaseLost, Queue


def claim_process(path):
    job = Queue(path).claim()
    return job.id if job else None


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "jobs.db"
        self.now = 1000.0
        self.q = Queue(self.path, clock=lambda: self.now)

    def test_persistence_and_json(self):
        identity = self.q.enqueue("work", {"text": "Türkçe", "items": [1, None]})
        reopened = Queue(self.path, clock=lambda: self.now)
        job = reopened.claim()
        self.assertEqual(job.id, identity)
        self.assertEqual(job.payload["text"], "Türkçe")
        reopened.ack(job)
        self.assertEqual(self.q.stats()["done"], 1)

    def test_priority_delay_and_fifo(self):
        first = self.q.enqueue("work", 1)
        self.q.enqueue("work", 2, priority=100, delay=10)
        high = self.q.enqueue("work", 3, priority=5)
        last = self.q.enqueue("work", 4)
        self.assertEqual([self.q.claim().id for _ in range(3)], [high, first, last])
        self.assertIsNone(self.q.claim())
        self.now += 10
        self.assertEqual(self.q.claim().payload, 2)

    def test_dedupe_survives_completion(self):
        identity = self.q.enqueue("work", 1, dedupe_key="order-1")
        self.q.ack(self.q.claim())
        self.assertEqual(self.q.enqueue("work", 2, dedupe_key="order-1"), identity)
        self.assertEqual(sum(self.q.stats().values()), 1)

    def test_backoff_dead_letter_and_manual_retry(self):
        identity = self.q.enqueue("work", 1, max_attempts=2)
        self.q.fail(self.q.claim(), "first failure", base_delay=5)
        self.now += 4
        self.assertIsNone(self.q.claim())
        self.now += 1
        job = self.q.claim()
        self.assertEqual(job.attempt, 2)
        self.q.fail(job, "final failure")
        self.assertEqual(self.q.inspect(identity)["state"], "dead")
        self.assertTrue(self.q.retry(identity))
        self.assertFalse(self.q.retry(identity))
        self.assertEqual(self.q.claim().attempt, 1)

    def test_expired_worker_cannot_mutate_new_lease(self):
        self.q.enqueue("work", 1)
        stale = self.q.claim(lease_seconds=5)
        self.now += 5
        fresh = self.q.claim()
        self.assertEqual(stale.id, fresh.id)
        self.assertNotEqual(stale.token, fresh.token)
        for action in (self.q.ack, self.q.heartbeat, lambda j: self.q.fail(j, "late")):
            with self.assertRaises(LeaseLost):
                action(stale)
        self.q.ack(fresh)

    def test_expiry_rejects_ack_even_without_reclaim(self):
        self.q.enqueue("work", None)
        job = self.q.claim(lease_seconds=1)
        self.now += 1
        with self.assertRaises(LeaseLost):
            self.q.ack(job)

    def test_crash_exhausts_attempt_budget(self):
        identity = self.q.enqueue("work", None, max_attempts=1)
        self.q.claim(lease_seconds=1)
        self.now += 1
        self.assertIsNone(self.q.claim())
        self.assertEqual(self.q.inspect(identity)["state"], "dead")

    def test_heartbeat_extends_and_never_shortens(self):
        self.q.enqueue("work", None)
        job = self.q.claim(lease_seconds=10)
        self.assertEqual(self.q.heartbeat(job, lease_seconds=1), 1010)
        self.now += 8
        self.assertEqual(self.q.heartbeat(job, lease_seconds=10), 1018)
        self.now += 4
        self.assertIsNone(self.q.claim())
        self.q.ack(job)

    def test_concurrent_claims_are_exclusive(self):
        expected = {self.q.enqueue("work", i) for i in range(30)}
        with ThreadPoolExecutor(max_workers=4) as pool:
            jobs = list(pool.map(lambda _: self.q.claim(), range(40)))
        actual = [job.id for job in jobs if job]
        self.assertEqual(len(actual), len(set(actual)))
        self.assertEqual(set(actual), expected)

    def test_concurrent_producers_deduplicate(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            ids = list(pool.map(lambda _: self.q.enqueue("work", 1, dedupe_key="same"), range(16)))
        self.assertEqual(len(set(ids)), 1)

    def test_separate_process_claims(self):
        q = Queue(self.path)
        ids = {q.enqueue("work", i) for i in range(8)}
        with ProcessPoolExecutor(max_workers=2) as pool:
            claimed = list(pool.map(claim_process, [str(self.path)] * 10))
        self.assertEqual(set(claimed) - {None}, ids)
        self.assertEqual(len([i for i in claimed if i]), 8)

    def test_inspect_hides_token_and_missing_is_none(self):
        identity = self.q.enqueue("work", 1)
        self.q.claim()
        self.assertNotIn("token", self.q.inspect(identity))
        self.assertIsNone(self.q.inspect("missing"))

    def test_invalid_inputs_do_not_write(self):
        for kwargs in ({"delay": -1}, {"delay": float("nan")}, {"max_attempts": 0},
                       {"max_attempts": 1.5}, {"priority": "high"}, {"dedupe_key": ""}):
            with self.assertRaises(ValueError):
                self.q.enqueue("work", 1, **kwargs)
        with self.assertRaises(ValueError):
            self.q.enqueue("", 1)
        with self.assertRaises(ValueError):
            self.q.enqueue("work", float("nan"))
        with self.assertRaises(ValueError):
            self.q.claim(lease_seconds=0)
        self.assertEqual(sum(self.q.stats().values()), 0)

    def test_cli_demo_and_invalid_json(self):
        result = subprocess.run([sys.executable, "-m", "anchorqueue", "--db", str(self.path), "demo"],
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('"done": 1', result.stdout)
        result = subprocess.run([sys.executable, "-m", "anchorqueue", "--db", str(self.path),
                                 "enqueue", "work", "{invalid"], text=True, capture_output=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn('"error"', result.stderr)


if __name__ == "__main__":
    unittest.main()

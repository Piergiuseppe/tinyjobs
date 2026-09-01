from __future__ import annotations

import unittest

from tinyjobs.exceptions import Abandon, Retry
from tinyjobs.retry import RetryPolicy


class RetryPolicyTests(unittest.TestCase):
    def test_single_attempt_never_retries(self):
        policy = RetryPolicy(max_attempts=1)
        self.assertFalse(policy.should_retry(1, policy.max_attempts, ValueError()))

    def test_retries_until_attempts_are_exhausted(self):
        policy = RetryPolicy(max_attempts=3)
        self.assertTrue(policy.should_retry(1, policy.max_attempts, ValueError()))
        self.assertTrue(policy.should_retry(2, policy.max_attempts, ValueError()))
        self.assertFalse(policy.should_retry(3, policy.max_attempts, ValueError()))

    def test_only_listed_exceptions_are_retried(self):
        policy = RetryPolicy(max_attempts=3, retry_on=(ConnectionError,))
        self.assertTrue(policy.should_retry(1, policy.max_attempts, ConnectionError()))
        self.assertFalse(policy.should_retry(1, policy.max_attempts, ValueError()))

    def test_abandon_stops_retrying_immediately(self):
        policy = RetryPolicy(max_attempts=5)
        self.assertFalse(policy.should_retry(1, policy.max_attempts, Abandon("permanent")))

    def test_retry_exception_is_always_retryable_within_budget(self):
        policy = RetryPolicy(max_attempts=3, retry_on=(ConnectionError,))
        self.assertTrue(policy.should_retry(1, policy.max_attempts, Retry(in_=5)))
        self.assertFalse(policy.should_retry(3, policy.max_attempts, Retry(in_=5)))

    def test_exponential_backoff_doubles_and_caps(self):
        policy = RetryPolicy(base=1.0, maximum=8.0, jitter=False)
        self.assertEqual(
            [policy.delay_for(n) for n in range(1, 6)], [1.0, 2.0, 4.0, 8.0, 8.0]
        )

    def test_fixed_backoff_is_constant(self):
        policy = RetryPolicy(backoff="fixed", base=2.5, jitter=False)
        self.assertEqual([policy.delay_for(n) for n in (1, 4)], [2.5, 2.5])

    def test_callable_backoff_is_honoured(self):
        policy = RetryPolicy(backoff=lambda attempt: attempt * 3, jitter=False)
        self.assertEqual(policy.delay_for(4), 12.0)

    def test_jitter_stays_within_half_the_delay(self):
        policy = RetryPolicy(base=10.0, maximum=10.0, jitter=True)
        delays = [policy.delay_for(1) for _ in range(200)]
        self.assertTrue(all(5.0 <= d <= 10.0 for d in delays))
        self.assertGreater(len(set(delays)), 1)

    def test_explicit_retry_delay_overrides_the_curve(self):
        policy = RetryPolicy(base=100.0, jitter=False)
        self.assertEqual(policy.delay_for(3, Retry(in_=7)), 7.0)

    def test_retry_rejects_both_in_and_at(self):
        with self.assertRaises(ValueError):
            Retry(in_=1, at=2)


if __name__ == "__main__":
    unittest.main()

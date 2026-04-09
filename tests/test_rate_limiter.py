"""Tests for allways.validator.rate_limiter and axon blacklist integration."""

import time

from allways.validator.axon_handlers import _check_rate_limit, _get_caller_ip
from allways.validator.rate_limiter import AxonRateLimiter

# =========================================================================
# Mock objects for blacklist handler tests
# =========================================================================


class _Dendrite:
    def __init__(self, ip=None):
        self.ip = ip


class _Synapse:
    def __init__(self, dendrite=None):
        self.dendrite = dendrite


class _Validator:
    def __init__(self, rate_limiter=None):
        if rate_limiter is not None:
            self.rate_limiter = rate_limiter


# =========================================================================
# AxonRateLimiter
# =========================================================================


class TestRateLimiterAllowDeny:
    """Core mechanism: allow up to limit, block after, and rejections don't inflate the count."""

    def test_allows_up_to_limit(self):
        limiter = AxonRateLimiter(max_requests=3, window_seconds=60)
        for _ in range(3):
            ok, _ = limiter.is_allowed('10.0.0.1')
            assert ok

    def test_blocks_at_limit(self):
        limiter = AxonRateLimiter(max_requests=3, window_seconds=60)
        for _ in range(3):
            limiter.is_allowed('10.0.0.1')

        ok, msg = limiter.is_allowed('10.0.0.1')
        assert not ok
        assert 'Rate limited' in msg

    def test_rejected_requests_not_counted(self):
        limiter = AxonRateLimiter(max_requests=2, window_seconds=60)
        limiter.is_allowed('x')
        limiter.is_allowed('x')
        for _ in range(50):
            ok, _ = limiter.is_allowed('x')
            assert not ok
        # Only 1 IP entry, deque didn't grow beyond 2
        assert limiter.active_count() == 1


class TestRateLimiterIsolation:
    """Different IPs tracked independently; empty IP always passes."""

    def test_independent_per_ip(self):
        limiter = AxonRateLimiter(max_requests=1, window_seconds=60)
        ok_a, _ = limiter.is_allowed('A')
        ok_b, _ = limiter.is_allowed('B')
        assert ok_a and ok_b

        blocked_a, _ = limiter.is_allowed('A')
        assert not blocked_a

    def test_empty_ip_always_passes(self):
        limiter = AxonRateLimiter(max_requests=1, window_seconds=60)
        for _ in range(100):
            ok, msg = limiter.is_allowed('')
            assert ok
            assert msg == 'No IP available'


class TestRateLimiterWindow:
    """Sliding window expires old requests; stale IPs are garbage-collected."""

    def test_window_expiry_resets(self):
        limiter = AxonRateLimiter(max_requests=1, window_seconds=0.2)
        ok, _ = limiter.is_allowed('ip')
        assert ok
        ok, _ = limiter.is_allowed('ip')
        assert not ok

        time.sleep(0.3)

        ok, _ = limiter.is_allowed('ip')
        assert ok

    def test_stale_entries_cleaned(self):
        limiter = AxonRateLimiter(max_requests=5, window_seconds=0.2, cleanup_seconds=0)
        for i in range(10):
            limiter.is_allowed(f'ip-{i}')
        assert limiter.active_count() == 10

        time.sleep(0.3)
        limiter.is_allowed('trigger')
        assert limiter.active_count() == 1


# =========================================================================
# _get_caller_ip
# =========================================================================


class TestGetCallerIp:
    def test_none_dendrite(self):
        assert _get_caller_ip(_Synapse(None)) == ''

    def test_none_ip(self):
        assert _get_caller_ip(_Synapse(_Dendrite(None))) == ''

    def test_valid_ip(self):
        assert _get_caller_ip(_Synapse(_Dendrite('1.2.3.4'))) == '1.2.3.4'


# =========================================================================
# _check_rate_limit
# =========================================================================


class TestCheckRateLimit:
    """Blacklist integration: fail-open when limiter/IP missing, enforce when present."""

    def test_no_limiter_passes(self):
        blacklisted, _ = _check_rate_limit(_Validator(), _Synapse(_Dendrite('1.1.1.1')))
        assert not blacklisted

    def test_no_ip_passes(self):
        blacklisted, _ = _check_rate_limit(_Validator(AxonRateLimiter()), _Synapse(None))
        assert not blacklisted

    def test_within_limit_passes(self):
        limiter = AxonRateLimiter(max_requests=5, window_seconds=60)
        blacklisted, reason = _check_rate_limit(_Validator(limiter), _Synapse(_Dendrite('2.2.2.2')))
        assert not blacklisted
        assert reason == 'Passed'

    def test_over_limit_blocks(self):
        limiter = AxonRateLimiter(max_requests=1, window_seconds=60)
        _check_rate_limit(_Validator(limiter), _Synapse(_Dendrite('3.3.3.3')))

        blacklisted, reason = _check_rate_limit(_Validator(limiter), _Synapse(_Dendrite('3.3.3.3')))
        assert blacklisted
        assert 'Rate limited' in reason

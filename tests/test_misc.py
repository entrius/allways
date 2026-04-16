"""Tests for allways.utils.misc."""

import time
from unittest.mock import MagicMock

from allways.utils.misc import ttl_cache, ttl_get_block, ttl_hash_gen


class TestTtlHashGen:
    def test_yields_same_value_within_window(self):
        gen = ttl_hash_gen(seconds=3600)
        a = next(gen)
        b = next(gen)
        assert a == b

    def test_short_ttl_eventually_changes(self):
        gen = ttl_hash_gen(seconds=1)
        first = next(gen)
        time.sleep(1.1)
        assert next(gen) != first


class TestTtlCache:
    def test_caches_repeated_calls(self):
        calls = []

        @ttl_cache(maxsize=8, ttl=60)
        def f(x):
            calls.append(x)
            return x * 2

        assert f(3) == 6
        assert f(3) == 6
        assert calls == [3]

    def test_different_args_miss_cache(self):
        calls = []

        @ttl_cache(maxsize=8, ttl=60)
        def f(x):
            calls.append(x)
            return x + 1

        f(1)
        f(2)
        assert calls == [1, 2]

    def test_negative_ttl_uses_default(self):
        # ttl <= 0 → substituted with 65536 internally
        @ttl_cache(ttl=-1)
        def f(x):
            return x

        assert f(5) == 5
        assert f(5) == 5

    def test_preserves_function_metadata(self):
        @ttl_cache(ttl=10)
        def named_fn(x):
            """doc"""
            return x

        assert named_fn.__name__ == 'named_fn'
        assert named_fn.__doc__ == 'doc'

    def test_expired_entry_recomputed(self):
        calls = []

        @ttl_cache(maxsize=8, ttl=1)
        def f(x):
            calls.append(x)
            return x

        f(7)
        time.sleep(1.1)
        f(7)
        assert calls == [7, 7]


class TestTtlGetBlock:
    def test_returns_current_block_from_subtensor(self):
        obj = MagicMock()
        obj.subtensor.get_current_block.return_value = 123
        assert ttl_get_block(obj) == 123

    def test_cached_within_ttl(self):
        obj = MagicMock()
        obj.subtensor.get_current_block.side_effect = [100, 200, 300]
        first = ttl_get_block(obj)
        second = ttl_get_block(obj)
        # maxsize=1 cache keyed on (ttl_hash, self); same obj within window → hit
        assert first == second

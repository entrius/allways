"""Fixtures the lifted alpha tests expect, trimmed to what they actually use."""

import threading
from dataclasses import replace

from allways.miner.alpha.config import AlphaConfig

_BASE_CONFIG = AlphaConfig(alpha_enabled=True)


def make_config(**overrides) -> AlphaConfig:
    return replace(_BASE_CONFIG, **overrides)


class DiscordDown(list):
    """An alert sink that records the key, then fails like a notifier whose webhook is down."""

    def append(self, key) -> None:
        super().append(key)
        raise RuntimeError('webhook down')


def lock_free(lock) -> bool:
    """Whether another thread can take the lock right now (an RLock re-enters on its own thread)."""
    taken = []

    def probe():
        if lock.acquire(blocking=False):
            taken.append(True)
            lock.release()

    thread = threading.Thread(target=probe)
    thread.start()
    thread.join(1)
    return bool(taken)

import time
from functools import lru_cache, update_wrapper
from math import floor
from typing import Any, Callable


def is_reserved(reserved_until: int, current_block: int) -> bool:
    """True iff the miner is still reserved at ``current_block``.

    Mirrors the on-chain contract, which treats ``reserved_until >= current_block``
    as active (see ``smart-contracts/ink/lib.rs`` — ``reserve`` blocks new
    reservations while this predicate holds, and ``initiate_swap`` accepts the
    confirm at exactly that boundary). CLI, validator axon, and state store
    must agree with the contract or the boundary block leaks a mismatch
    between the user-visible message and the on-chain path.
    """
    return reserved_until >= current_block


# LRU Cache with TTL
def ttl_cache(maxsize: int = 128, typed: bool = False, ttl: int = -1):
    """
    Decorator that creates a cache of the most recently used function calls with a time-to-live (TTL) feature.
    The cache evicts the least recently used entries if the cache exceeds the `maxsize` or if an entry has
    been in the cache longer than the `ttl` period.
    """
    if ttl <= 0:
        ttl = 65536
    hash_gen = ttl_hash_gen(ttl)

    def wrapper(func: Callable) -> Callable:
        @lru_cache(maxsize, typed)
        def ttl_func(ttl_hash, *args, **kwargs):
            return func(*args, **kwargs)

        def wrapped(*args, **kwargs) -> Any:
            th = next(hash_gen)
            return ttl_func(th, *args, **kwargs)

        return update_wrapper(wrapped, func)

    return wrapper


def ttl_hash_gen(seconds: int):
    """Generate a new hash value at regular time intervals for the ttl_cache decorator."""
    start_time = time.time()
    while True:
        yield floor((time.time() - start_time) / seconds)


# 12 seconds updating block.
@ttl_cache(maxsize=1, ttl=12)
def ttl_get_block(self) -> int:
    """
    Retrieves the current block number from the blockchain. This method is cached with a time-to-live (TTL)
    of 12 seconds.
    """
    return self.subtensor.get_current_block()

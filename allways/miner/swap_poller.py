"""Polls the Solana program for swaps assigned to this miner.

Solana keys swaps by `swap_key` (== keccak(from_tx_hash)) and exposes them through a single
`getProgramAccounts` snapshot per poll, partitioned by status locally, so there is no incremental
cursor and no per-id transient miss to defend against (the old ink! poller scanned `get_swap(id)`
one id at a time): each poll is an atomic, authoritative view. Whether a swap has already been handled is tracked by ``SwapFulfiller``'s
persistent send cache — this poller reports raw on-chain state only.
"""

from typing import List, Set, Tuple

import bittensor as bt

from allways.solana.client import SolanaSwap, _as_pubkey, swap_from_solana

# Statuses the miner acts on: ACTIVE needs fulfillment; FULFILLED is awaiting validator confirm but is
# still reported so the fulfiller retains its send-cache entry until the swap closes on-chain.
ACTIVE_STATUSES = ('Active', 'Fulfilled')


class SwapPoller:
    """Enumerates the program's swaps and returns the ones assigned to this miner."""

    def __init__(self, solana_client, miner_pubkey):
        self.client = solana_client
        self.miner_pubkey = _as_pubkey(miner_pubkey)
        self.known: Set[str] = set()  # swap_key hexes already logged as discovered (cosmetic)
        self.last_poll_ok: bool = True
        self._counters = None  # (successful_swaps, failed_swaps) baseline for naming terminal outcomes

    def poll(self) -> Tuple[List[SolanaSwap], List[SolanaSwap]]:
        """Snapshot poll. Returns (active, fulfilled) for this miner. On RPC failure returns ([], [])
        with ``last_poll_ok`` False so the caller skips send-cache cleanup against an empty set."""
        try:
            result = self.poll_inner()
            self.last_poll_ok = True
            return result
        except Exception as e:
            bt.logging.error(f'SwapPoller poll error: {type(e).__name__}: {e}')
            self.last_poll_ok = False
            return [], []

    def _mine(self, rows, status: str) -> List[SolanaSwap]:
        out = []
        for _pubkey, acct in rows:
            if type(acct.status).__name__ != status:
                continue
            if _as_pubkey(acct.miner) != self.miner_pubkey:
                continue
            swap = swap_from_solana(acct)
            if swap.key_hex not in self.known:
                bt.logging.info(
                    f'Discovered swap {swap.key_hex[:16]}: {swap.from_chain} -> {swap.to_chain}, '
                    f'collateral_amount={swap.collateral_amount}, status={swap.status}'
                )
                self.known.add(swap.key_hex)
            out.append(swap)
        return out

    def poll_inner(self) -> Tuple[List[SolanaSwap], List[SolanaSwap]]:
        # One snapshot fetch, partitioned locally — both statuses come from the same atomic view
        # (and half the getProgramAccounts of fetching per status).
        rows = self.client.get_swaps()
        active = self._mine(rows, 'Active')
        fulfilled = self._mine(rows, 'Fulfilled')
        # Forget keys no longer present so a reused-tx swap re-logs; bounded to the live set.
        live = {s.key_hex for s in active} | {s.key_hex for s in fulfilled}
        gone = self.known - live
        if gone or self._counters is None:
            self._log_terminal(gone)
        self.known &= live
        return active, fulfilled

    def _read_counters(self):
        ms = self.client.get_miner_state(self.miner_pubkey)
        return (int(ms.successful_swaps), int(ms.failed_swaps))

    def _log_terminal(self, gone: Set[str]) -> None:
        """Name each closed swap's terminal outcome — paid vs slashed — from the on-chain lifetime
        counters, so the miner's own log tells the whole story (the Swap account is already gone).
        Counters only move on closures, so the delta since the last baseline attributes exactly."""
        prev = self._counters
        try:
            self._counters = self._read_counters()
        except Exception as e:
            for g in gone:
                bt.logging.warning(f'Swap {g[:16]}: resolved (Completed or TimedOut — outcome read failed: {e})')
            return
        if not gone:
            return  # first poll: baseline seeded
        if prev is None:
            for g in gone:
                bt.logging.info(f'Swap {g[:16]}: left active set — resolved (Completed or TimedOut)')
            return
        ok, failed = self._counters[0] - prev[0], self._counters[1] - prev[1]
        if len(gone) == 1 and ok + failed == 1:
            g = next(iter(gone))
            if ok:
                bt.logging.success(f'Swap {g[:16]}: COMPLETED — paid out (successful_swaps={self._counters[0]})')
            else:
                bt.logging.error(f'Swap {g[:16]}: TIMED OUT — collateral slashed (failed_swaps={self._counters[1]})')
            return
        bt.logging.info(
            f'{len(gone)} swap(s) resolved: +{ok} completed, +{failed} timed out '
            f'(lifetime {self._counters[0]} ok / {self._counters[1]} failed)'
        )

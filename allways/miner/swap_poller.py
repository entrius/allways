"""Polls the Solana program for swaps assigned to this miner.

Solana keys swaps by `swap_key` (== keccak(from_tx_hash)) and exposes them through a single
`getProgramAccounts` snapshot per status, so there is no incremental cursor and no per-id transient
miss to defend against (the old ink! poller scanned `get_swap(id)` one id at a time): each poll is an
atomic, authoritative view. Whether a swap has already been handled is tracked by ``SwapFulfiller``'s
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

    def _mine(self, status: str) -> List[SolanaSwap]:
        out = []
        for _pubkey, acct in self.client.get_swaps(status=status):
            if _as_pubkey(acct.miner) != self.miner_pubkey:
                continue
            swap = swap_from_solana(acct)
            if swap.key_hex not in self.known:
                bt.logging.info(
                    f'Discovered swap {swap.key_hex[:16]}: {swap.from_chain} -> {swap.to_chain}, '
                    f'sol_amount={swap.sol_amount}, status={swap.status}'
                )
                self.known.add(swap.key_hex)
            out.append(swap)
        return out

    def poll_inner(self) -> Tuple[List[SolanaSwap], List[SolanaSwap]]:
        active = self._mine('Active')
        fulfilled = self._mine('Fulfilled')
        # Forget keys no longer present so a reused-tx swap re-logs; bounded to the live set.
        live = {s.key_hex for s in active} | {s.key_hex for s in fulfilled}
        self.known &= live
        return active, fulfilled

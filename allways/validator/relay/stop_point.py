"""Where the vault last stopped counting fees, for a validator whose fee ledger starts empty.

A miner's cumulative total in the fee batch is the vault's settled counter plus every fee since the
boundary the vault last settled. A validator that watched that settle knows the boundary; one with a
fresh database doesn't, and the obvious guess (the latest boundary) is wrong exactly when that round is
still waiting on this validator's own vote — it would then count one cadence short forever, and with
two validators no round could ever reach quorum to correct it.

So it is looked up. A cadence round for boundary T only fires after T and lives one round TTL, so the
settled counters move in (T, next boundary] only if T's round applied. Reading them at each boundary
inside the feed's history window (and now) on an archive node, the newest window in which any of them
moved names the boundary the vault stands at. A window moved by something else (a late older round,
an exit's residual settle) can only make the answer too NEW, which counts short: safe, and corrected
once the vault settles more. No movement at all answers the oldest boundary in the window — nothing
older is countable from this validator's history anyway.

On a single-validator vault every round applies on our own vote, so the latest boundary is right and
no archive is needed."""

from __future__ import annotations

import os
from typing import Any, Callable, Dict, List, Optional

import bittensor as bt

from allways.constants import TAO_HUB_VAULT_ADDRESSES
from allways.utils.subtensor import ARCHIVE_ENDPOINTS_ENV, archive_substrate, parse_endpoints
from allways.validator.feed.events import HISTORY_HORIZON_SECS

BLOCK_SECS = 12
# Archive reads per lookup are a few dozen; past this many probes the block search gives up.
MAX_PROBES = 40


def boundaries(now: int, cadence: int, horizon: int = HISTORY_HORIZON_SECS) -> List[int]:
    """Fee boundaries inside the history window, newest first. Never empty: the latest boundary is
    always one."""
    last = (now // cadence) * cadence
    out = [last]
    while out[-1] - cadence >= now - horizon:
        out.append(out[-1] - cadence)
    return out


def resolve(
    now: int,
    cadence: int,
    validator_count: Optional[int],
    hotkeys: List[str],
    settled_at: Callable[[str, int], Optional[int]],
) -> Optional[int]:
    """The boundary the vault last settled, or None when a read failed (retry later, never guess).
    ``settled_at(hotkey, t)`` reads the vault's settled counter as it stood at time ``t``."""
    if validator_count is None:
        return None
    candidates = boundaries(now, cadence)
    if validator_count <= 1 or not hotkeys:
        return candidates[0]
    edges = [now] + candidates
    snapshots: Dict[int, Dict[str, int]] = {}
    for t in edges:
        snap = {}
        for hotkey in hotkeys:
            value = settled_at(hotkey, t)
            if value is None:
                return None
            snap[hotkey] = value
        snapshots[t] = snap
    for i, boundary in enumerate(candidates):
        if snapshots[edges[i]] != snapshots[boundary]:
            return boundary
    return candidates[-1]


class ArchiveReader:
    """The vault's settled counters at a past time, read off an archive node. The connection opens on
    first use: a validator that never boots fresh never dials one."""

    def __init__(self, vault: Any, connect: Optional[Callable[[str], Any]] = None):
        self.vault = vault
        self._connect = connect or archive_substrate
        self._substrate: Any = None
        self._blocks: Dict[int, str] = {}

    def endpoints(self) -> List[str]:
        configured = parse_endpoints(os.environ.get(ARCHIVE_ENDPOINTS_ENV))
        if configured:
            return configured
        # Which chain by the vault of record, not the endpoint: a keyed provider URL names neither. The
        # public testnet node keeps full state; mainnet's public archive is its own endpoint.
        network = {addr: net for net, addr in TAO_HUB_VAULT_ADDRESSES.items()}.get(getattr(self.vault, 'address', None))
        return {'test': ['test'], 'finney': ['archive']}.get(network, [])

    def substrate(self) -> Any:
        if self._substrate is None:
            errors = []
            for url in self.endpoints():
                try:
                    self._substrate = self._connect(url)
                    break
                except Exception as e:
                    errors.append(f'{type(e).__name__}')
            if self._substrate is None:
                raise ConnectionError(
                    f'no archive endpoint answered ({", ".join(errors)})'
                    if errors
                    else f'no archive endpoint for this vault — set {ARCHIVE_ENDPOINTS_ENV}'
                )
        return self._substrate

    def block_at(self, t: int) -> str:
        """Hash of the first block at or after wall time ``t``."""
        if t in self._blocks:
            return self._blocks[t]
        sub = self.substrate()
        head = sub.get_block_number(sub.get_chain_head())
        probes = 0

        def time_of(block: int) -> int:
            nonlocal probes
            probes += 1
            if probes > MAX_PROBES:
                raise RuntimeError(f'block search for {t} did not converge')
            return int(getattr(sub.query('Timestamp', 'Now', block_hash=sub.get_block_hash(block)), 'value', 0)) // 1000

        guess = head - max(0, time_of(head) - t) // BLOCK_SECS
        guess = min(head, guess - (time_of(guess) - t) // BLOCK_SECS)
        lo, hi = guess - 20, min(head, guess + 20)
        while time_of(lo) >= t:
            lo -= 40
        while hi < head and time_of(hi) < t:
            hi = min(head, hi + 40)
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if time_of(mid) >= t:
                hi = mid
            else:
                lo = mid
        self._blocks[t] = sub.get_block_hash(hi)
        return self._blocks[t]

    def settled_at(self, hotkey: str, t: int) -> Optional[int]:
        try:
            return self.vault.get_settled_total_at(hotkey, self.block_at(t), substrate=self.substrate())
        except Exception as e:
            bt.logging.warning(f'relay: archive read of the vault at {t} failed: {e}')
            self._substrate = None  # the next attempt dials again
            return None

"""Validator stake-weight vote — keeps the contract's reservation-lottery draw weights stake-true.

Every ``WEIGHTS_VOTE_INTERVAL_BLOCKS`` (~12h) each validator derives the same vector — one draw
weight per whitelisted validator, index-aligned to ``Config.validators`` — and votes it via
``vote_set_weights``; quorum applies it. Weight = floor(alpha_stake / WEIGHTS_STAKE_BUCKET_ALPHA),
attributing each validator pubkey to its metagraph hotkey through the same sr25519 bindings miners
use (``alw bind-hotkey``); no valid binding or no metagraph presence → 0 (an all-zero vector is
safe: the draw falls back to uniform). The block-aligned cadence has every validator read the
metagraph at ~the same stake snapshot, so the hash-bound vectors converge; vote rounds are keyed
per snapshot (round PDA = the vector's hash), so a divergent proposal coexists instead of blocking —
whichever snapshot reaches quorum first wins, and stragglers retry on the throttle.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List

import bittensor as bt
from solders.pubkey import Pubkey

from allways.constants import (
    SECONDS_PER_BLOCK,
    WEIGHTS_STAKE_BUCKET_ALPHA,
    WEIGHTS_VOTE_INTERVAL_BLOCKS,
    WEIGHTS_VOTE_RETRY_SECS,
)
from allways.validator.binding import build_attribution

if TYPE_CHECKING:
    from neurons.validator import Validator

# Expected contract rejections in the normal multi-validator dance: we already voted this round
# (quorum pending), the on-chain cadence floor, or we were de-whitelisted mid-attempt.
# Retried/settled at the next throttled attempt.
_BENIGN_WEIGHTS_MARKERS = ('AlreadyVoted', 'WeightsUpdateTooSoon', 'NotValidator')


def derive_weight_vector(validators, attribution: Dict[str, str], metagraph) -> List[int]:
    """Draw weight per whitelisted validator, index-aligned to ``Config.validators``:
    max(1, floor(alpha_stake / bucket)); no binding or not on the metagraph → 0.

    The floor of 1 keeps a bound, sub-bucket validator ahead of native (weight-0) bidders —
    all-zero weights make resolve_pool's draw uniform and break routed win odds."""
    uid_of = {hk: uid for uid, hk in enumerate(metagraph.hotkeys)}
    weights = []
    for v in validators:
        uid = uid_of.get(attribution.get(str(Pubkey.from_bytes(bytes(v.key)))))
        alpha = float(metagraph.alpha_stake[uid]) if uid is not None else 0.0
        weights.append(max(1, int(alpha // WEIGHTS_STAKE_BUCKET_ALPHA)) if uid is not None else 0)
    return weights


def maybe_vote_weights(self: Validator, now: int) -> None:
    """One block-aligned stake-weight vote step. Never raises — a weights hiccup must not break the pass."""
    try:
        _step(self, now)
    except Exception as e:
        bt.logging.warning(f'weights vote: {e}')


def _step(self: Validator, now: int) -> None:
    epoch = self.block // WEIGHTS_VOTE_INTERVAL_BLOCKS
    if epoch == self.weights_epoch_done:
        return
    if now - self.last_weights_attempt < WEIGHTS_VOTE_RETRY_SECS:
        return
    self.last_weights_attempt = now

    config = self.solana_client.get_config()
    # Already landed this epoch (a never-updated contract has last==0, which is always due).
    boundary_time = now - (self.block % WEIGHTS_VOTE_INTERVAL_BLOCKS) * SECONDS_PER_BLOCK
    if int(config.last_weights_update) >= boundary_time:
        self.weights_epoch_done = epoch
        return

    me = str(self.solana_client.keypair.pubkey())
    if me not in (str(Pubkey.from_bytes(bytes(v.key))) for v in config.validators):
        # Epoch memo rate-limits this to once per ~12h — visible, not spam.
        bt.logging.warning(f'weights vote: {me} not in the contract validator whitelist — skipping until added')
        self.weights_epoch_done = epoch
        return

    vector = derive_weight_vector(config.validators, build_attribution(self.solana_client), self.metagraph)
    if vector == [int(v.weight) for v in config.validators]:
        self.weights_epoch_done = epoch
        return

    if self.solana_swap_loop.read_only:
        bt.logging.info(f'weights vote: WOULD vote {vector} (watch mode)')
        self.weights_epoch_done = epoch
        return

    try:
        sig = self.solana_client.vote_set_weights(vector, [bytes(v.key) for v in config.validators])
    except Exception as e:
        if any(m in str(e) for m in _BENIGN_WEIGHTS_MARKERS):
            bt.logging.debug(f'weights vote: no-op ({e})')
            return
        raise
    bt.logging.success(f'weights vote: {vector} submitted (sig {sig[:16]}…)')

"""Emission recycle mechanism — caps miner emission to actual fee revenue."""

from __future__ import annotations

from typing import TYPE_CHECKING, Tuple

import bittensor as bt
import numpy as np

from allways.constants import (
    DAILY_EMISSION_ALPHA,
    RECYCLE_UID,
    SCORING_WINDOW_BLOCKS,
    TAO_TO_RAO,
)
from allways.validator.swap_tracker import SwapTracker
from allways.validator.utils.fees import windowed_fees_rao

if TYPE_CHECKING:
    from neurons.validator import Validator


def apply_recycle(
    self: Validator,
    rewards: np.ndarray,
    uids: set,
    tracker: SwapTracker,
) -> Tuple[np.ndarray, set]:
    """Scale miner rewards proportional to fees vs emission, recycling the remainder.

    If miners earned less in fees than the proportional emission for this window,
    their rewards are scaled down and the difference is assigned to RECYCLE_UID
    (subnet owner), which gets recycled on-chain.
    """
    if RECYCLE_UID < self.metagraph.n.item():
        recycle_uid = RECYCLE_UID
    else:
        recycle_uid = 0
        bt.logging.warning(f'RECYCLE_UID {RECYCLE_UID} out of bounds (n={self.metagraph.n.item()}), falling back to 0')

    if len(uids) == 0 or len(rewards) == 0:
        bt.logging.info('Recycle: no miners scored, recycling all emission')
        return np.array([1.0], dtype=np.float32), {recycle_uid}

    fees_tao = windowed_fees_rao(tracker.window, self.fee_divisor) / TAO_TO_RAO

    try:
        alpha_price_tao = self.subtensor.get_subnet_price(self.config.netuid).tao
    except Exception as e:
        bt.logging.warning(f'Recycle: failed to get subnet price, skipping: {e}')
        return rewards, uids
    window_fraction = SCORING_WINDOW_BLOCKS / 7200
    emission_alpha = DAILY_EMISSION_ALPHA * window_fraction
    emission_tao = emission_alpha * alpha_price_tao

    if emission_tao > 0:
        recycle_fraction = max(0.0, 1.0 - fees_tao / emission_tao)
    else:
        recycle_fraction = 0.0

    bt.logging.info(
        f'Recycle: fees={fees_tao:.4f} TAO, emission={emission_tao:.4f} TAO, recycle={recycle_fraction:.4f}'
    )

    if recycle_fraction == 0.0:
        return rewards, uids

    rewards = rewards * (1.0 - recycle_fraction)

    sorted_uids = sorted(uids)
    uid_rewards = {uid: rewards[i] for i, uid in enumerate(sorted_uids)}

    if recycle_uid in uid_rewards:
        uid_rewards[recycle_uid] += recycle_fraction
    else:
        uid_rewards[recycle_uid] = recycle_fraction

    new_uids = set(uid_rewards.keys())
    new_sorted = sorted(new_uids)
    new_rewards = np.array([uid_rewards[uid] for uid in new_sorted], dtype=np.float32)

    return new_rewards, new_uids

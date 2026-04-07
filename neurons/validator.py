"""Allways Validator - Entry Point

Monitors swaps, verifies transactions on both chains, and votes on outcomes.
Processes synapse requests from miners (dendrite) and users (dendrite-lite)
via axon handlers with multi-validator consensus.

Usage:
    python neurons/validator.py --netuid 7 --wallet.name default --wallet.hotkey default
"""

import threading
import time
from functools import partial
from pathlib import Path

import bittensor as bt
from dotenv import load_dotenv

from allways.chain_providers import create_chain_providers
from allways.constants import DEFAULT_FEE_DIVISOR, DEFAULT_FULFILLMENT_TIMEOUT_BLOCKS, SCORING_WINDOW_BLOCKS
from allways.contract_client import AllwaysContractClient
from allways.validator.axon_handlers import (
    blacklist_miner_activate,
    blacklist_swap_confirm,
    blacklist_swap_reserve,
    handle_miner_activate,
    handle_swap_confirm,
    handle_swap_reserve,
    priority_miner_activate,
    priority_swap_confirm,
    priority_swap_reserve,
)
from allways.validator.chain_verification import SwapVerifier
from allways.validator.forward import forward
from allways.validator.pending_confirms import PendingConfirmQueue
from allways.validator.scoring_store import ScoringWindowStore
from allways.validator.swap_tracker import SwapTracker
from allways.validator.voting import SwapVoter
from neurons.base.validator import BaseValidatorNeuron

load_dotenv()


class Validator(BaseValidatorNeuron):
    """Allways validator neuron.

    Monitors the smart contract for active swaps, verifies both
    sides of each swap using chain providers, and confirms or
    times out swaps. Processes synapse requests via axon handlers
    for miner activation, swap reservations, and swap confirmations.
    """

    def __init__(self, config=None):
        super().__init__(config=config)

        self.contract_client = AllwaysContractClient(subtensor=self.subtensor)
        self.chain_providers = create_chain_providers(check=True, require_send=False, subtensor=self.subtensor)

        timeout_blocks = self.contract_client.get_fulfillment_timeout() or DEFAULT_FULFILLMENT_TIMEOUT_BLOCKS
        try:
            self.fee_divisor = self.contract_client.get_fee_divisor() or DEFAULT_FEE_DIVISOR
        except Exception as e:
            bt.logging.warning(f'Failed to read fee_divisor, using default {DEFAULT_FEE_DIVISOR}: {e}')
            self.fee_divisor = DEFAULT_FEE_DIVISOR
        scoring_cache_path = Path(self.config.neuron.full_path) / 'scoring_window.json'
        self.scoring_store = ScoringWindowStore(scoring_cache_path)
        self.swap_tracker = SwapTracker(
            client=self.contract_client,
            fulfillment_timeout_blocks=timeout_blocks,
            window_blocks=SCORING_WINDOW_BLOCKS,
            store=self.scoring_store,
        )
        self.swap_tracker.initialize(self.block)
        bt.logging.debug(f'Validator components: fee_divisor={self.fee_divisor}, timeout={timeout_blocks}')

        self.swap_verifier = SwapVerifier(
            chain_providers=self.chain_providers,
            subtensor=self.subtensor,
            netuid=self.config.netuid,
            metagraph=self.metagraph,
            fee_divisor=self.fee_divisor,
        )

        self.swap_voter = SwapVoter(
            contract_client=self.contract_client,
            wallet=self.wallet,
        )

        # Pending confirmation queue (axon handler thread → forward loop thread)
        self.pending_confirms = PendingConfirmQueue()

        # Separate subtensor/contract/providers for axon handlers (thread safety).
        # axon_lock serialises substrate websocket calls across handler threads
        # to prevent "cannot call recv while another coroutine is already running recv" errors.
        self.axon_lock = threading.Lock()
        self.axon_subtensor = bt.Subtensor(config=self.config)
        self.axon_contract_client = AllwaysContractClient(subtensor=self.axon_subtensor)
        self.axon_chain_providers = create_chain_providers(subtensor=self.axon_subtensor)

        # Attach synapse handlers to axon
        self._attach_axon_handlers()

        bt.logging.info(f'Validator initialized: hotkey={self.wallet.hotkey.ss58_address}')

    def _attach_axon_handlers(self):
        """Attach all synapse handlers to the axon."""
        self.axon.attach(
            forward_fn=partial(handle_miner_activate, self),
            blacklist_fn=partial(blacklist_miner_activate, self),
            priority_fn=partial(priority_miner_activate, self),
        ).attach(
            forward_fn=partial(handle_swap_reserve, self),
            blacklist_fn=partial(blacklist_swap_reserve, self),
            priority_fn=partial(priority_swap_reserve, self),
        ).attach(
            forward_fn=partial(handle_swap_confirm, self),
            blacklist_fn=partial(blacklist_swap_confirm, self),
            priority_fn=partial(priority_swap_confirm, self),
        )
        bt.logging.info('Axon handlers attached: MinerActivate, SwapReserve, SwapConfirm')

    async def forward(self):
        """Validator forward pass - delegates to allways.validator.forward."""
        return await forward(self)


# Main entry point
if __name__ == '__main__':
    with Validator() as validator:
        while True:
            bt.logging.info(f'Validator running... step={validator.step}')
            time.sleep(60)

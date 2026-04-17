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
from allways.constants import (
    DEFAULT_FULFILLMENT_TIMEOUT_BLOCKS,
    FEE_DIVISOR,
)
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
from allways.validator.event_watcher import ContractEventWatcher
from allways.validator.forward import forward
from allways.validator.state_store import ValidatorStateStore
from allways.validator.swap_tracker import SwapTracker
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
        self.fee_divisor = FEE_DIVISOR

        # Single store owning every validator-local table. Must be created
        # before SwapTracker so the tracker can persist swap outcomes into
        # the credibility ledger, and before the axon handler wiring so the
        # handler thread can enqueue pending confirms. Exposes current block
        # so pending_confirms can purge expired reservations lazily on read.
        self.state_store = ValidatorStateStore(current_block_fn=lambda: self.block)
        self.last_known_rates: dict[tuple[str, str, str], float] = {}
        # (miner_hotkey, from_tx_hash) → reserved_until at vote time. Skips
        # redundant vote_extend_reservation extrinsics — auto-clears once the
        # contract bumps reserved_until past the voted value, so the next
        # extension round is open.
        self.extend_reservation_voted_at: dict[tuple[str, str], int] = {}

        # Event-sourced miner state. ``sync_to(current_block)`` runs each
        # forward step; scoring reads the active set from the watcher's
        # in-memory dicts and trusts the contract's active flag for all
        # collateral-floor invariants.
        metadata_path = Path(__file__).resolve().parent.parent / 'allways' / 'metadata' / 'allways_swap_manager.json'
        self.event_watcher = ContractEventWatcher(
            substrate=self.subtensor.substrate,
            contract_address=self.contract_client.contract_address,
            metadata_path=metadata_path,
            state_store=self.state_store,
        )
        self.event_watcher.initialize(
            current_block=self.block,
            metagraph_hotkeys=list(self.metagraph.hotkeys),
            contract_client=self.contract_client,
        )

        self.swap_tracker = SwapTracker(
            client=self.contract_client,
            fulfillment_timeout_blocks=timeout_blocks,
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

        # Separate subtensor/contract/providers for axon handlers (thread safety).
        # axon_lock serialises substrate websocket calls across handler threads
        # to prevent "cannot call recv while another coroutine is already running recv" errors.
        self.axon_lock = threading.Lock()
        self.axon_subtensor = bt.Subtensor(config=self.config)
        self.axon_contract_client = AllwaysContractClient(subtensor=self.axon_subtensor)
        self.axon_chain_providers = create_chain_providers(subtensor=self.axon_subtensor)

        # Attach synapse handlers to axon
        self.attach_axon_handlers()

        bt.logging.info(f'Validator initialized: hotkey={self.wallet.hotkey.ss58_address}')

    def attach_axon_handlers(self):
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

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.state_store.close()


# Main entry point
if __name__ == '__main__':
    with Validator() as validator:
        while True:
            bt.logging.info(f'Validator running... step={validator.step}')
            time.sleep(60)

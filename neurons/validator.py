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
from allways.validator.bounds_cache import BoundsCache
from allways.validator.chain_verification import SwapVerifier
from allways.validator.event_watcher import ContractEventWatcher
from allways.validator.forward import forward
from allways.validator.optimistic_extensions import OptimisticExtensionWatcher
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

        self.contract_client = AllwaysContractClient(
            subtensor=self.subtensor,
            reconnect_subtensor=self.reconnect_and_propagate,
        )
        self.chain_providers = create_chain_providers(check=True, require_send=False, subtensor=self.subtensor)

        try:
            timeout_blocks = self.contract_client.get_fulfillment_timeout() or DEFAULT_FULFILLMENT_TIMEOUT_BLOCKS
        except Exception as e:
            bt.logging.warning(f'fulfillment_timeout read failed at init, using default: {e}')
            timeout_blocks = DEFAULT_FULFILLMENT_TIMEOUT_BLOCKS
        self.fee_divisor = FEE_DIVISOR

        # Single store owning every validator-local table. Must be created
        # before SwapTracker so the tracker can persist swap outcomes into
        # the credibility ledger, and before the axon handler wiring so the
        # handler thread can enqueue pending confirms. Exposes current block
        # so pending_confirms can purge expired reservations lazily on read.
        # db path is overridable so a multi-validator dev env can give each
        # process its own file — shared DBs race on pending_confirms delete.
        state_db_path = getattr(getattr(self.config, 'validator', None), 'state_db_path', None)
        self.state_store = ValidatorStateStore(
            db_path=state_db_path,
            current_block_fn=lambda: self.block,
        )
        self.last_known_rates: dict[tuple[str, str, str], float] = {}
        # (miner_hotkey, from_tx_hash) → consecutive "tx not found" poll count.
        # Used to absorb mempool propagation lag before dropping a pending entry.
        self.pending_confirm_null_polls: dict[tuple[str, str], int] = {}

        # Optimistic propose/challenge/finalize for reservation + timeout
        # extensions. Stateless decision class — the forward loop drives it
        # per-iteration with the state it already has in hand.
        self.optimistic_extensions = OptimisticExtensionWatcher(
            contract_client=self.contract_client,
            wallet=self.wallet,
        )

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

        self.swap_tracker = SwapTracker(client=self.contract_client)
        self.swap_tracker.initialize()
        # Late-bind the tracker so TimeoutExtensionFinalized events can write
        # the new timeout_block straight into the in-memory active swap.
        self.event_watcher.swap_tracker = self.swap_tracker
        bt.logging.debug(f'Validator components: fee_divisor={self.fee_divisor}, timeout={timeout_blocks}')

        self.swap_verifier = SwapVerifier(
            chain_providers=self.chain_providers,
            fee_divisor=self.fee_divisor,
        )

        # Separate subtensor/contract/providers for axon handlers (thread safety).
        # axon_lock serialises substrate websocket calls across handler threads
        # to prevent "cannot call recv while another coroutine is already running recv" errors.
        self.axon_lock = threading.Lock()
        self.axon_subtensor = bt.Subtensor(config=self.config)
        self.axon_contract_client = AllwaysContractClient(
            subtensor=self.axon_subtensor,
            reconnect_subtensor=self.reconnect_axon_subtensor,
        )
        self.axon_chain_providers = create_chain_providers(subtensor=self.axon_subtensor)
        # Must read the current block via axon_subtensor — the block getter on
        # self (self.block) goes through self.subtensor, which the forward loop
        # is already using; concurrent axon + forward reads collide on the same
        # websocket and raise ConcurrencyError.
        self.bounds_cache = BoundsCache(
            self.axon_contract_client,
            self.axon_subtensor.get_current_block,
        )

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

    def reconnect_and_propagate(self):
        """Rebuild the main subtensor and update components that hold it."""
        self.reconnect_subtensor()
        self.contract_client.subtensor = self.subtensor
        tao_provider = self.chain_providers.get('tao')
        if tao_provider and hasattr(tao_provider, 'subtensor'):
            tao_provider.subtensor = self.subtensor

    def reconnect_axon_subtensor(self):
        """Rebuild the axon-side subtensor used by handler threads."""
        bt.logging.info('Reconnecting axon subtensor...')
        old = self.axon_subtensor
        self.axon_subtensor = bt.Subtensor(config=self.config)
        self.axon_contract_client.subtensor = self.axon_subtensor
        try:
            old.close()
        except Exception:
            pass

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

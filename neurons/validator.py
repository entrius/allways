"""Allways Validator - Entry Point

Monitors swaps, verifies transactions on both chains, and votes on outcomes.
Processes synapse requests from miners (dendrite) and users (dendrite-lite)
via axon handlers with multi-validator consensus.

Usage:
    python neurons/validator.py --netuid 7 --wallet.name default --wallet.hotkey default
"""

import os
import sys
import threading
import time
from functools import partial

from dotenv import load_dotenv

# Must precede the allways imports: they resolve env-backed settings, and a later load would be a no-op.
load_dotenv()

import bittensor as bt  # noqa: E402
import wandb  # noqa: E402

from allways import __version__  # noqa: E402
from allways.assets import create_assets  # noqa: E402
from allways.chains import apply_testnet_network_defaults  # noqa: E402
from allways.constants import (  # noqa: E402
    FEE_DIVISOR,
    FORWARD_STALL_THRESHOLD_SECONDS,
    NETUID_FINNEY,
    SCORING_WINDOW_BLOCKS,
    SCORING_WINDOW_SECS,
)
from allways.solana import keys  # noqa: E402
from allways.solana.client import AllwaysSolanaClient  # noqa: E402
from allways.solana.events import SolanaEventIngest  # noqa: E402
from allways.solana.rpc import assert_cluster_safe, resolve_rpc_url  # noqa: E402
from allways.validator.axon_handlers import (  # noqa: E402
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
from allways.validator.binding import warn_if_unbound  # noqa: E402
from allways.validator.bounds_cache import SolanaConfigCache  # noqa: E402
from allways.validator.event_index import SolanaEventIndex  # noqa: E402
from allways.validator.floor_sweep import CollateralFloorSweep  # noqa: E402
from allways.validator.forward import forward  # noqa: E402
from allways.validator.relay.wiring import build_bond_relay  # noqa: E402
from allways.validator.seam_http import maybe_start_seam  # noqa: E402
from allways.validator.solana_swap_loop import SolanaSwapLoop  # noqa: E402
from allways.validator.state_store import ValidatorStateStore  # noqa: E402
from allways.validator.storage import DatabaseStorage  # noqa: E402
from neurons.base.neuron import validator_mode  # noqa: E402
from neurons.base.validator import BaseValidatorNeuron  # noqa: E402

WANDB_ENTITY = os.getenv('WANDB_ENTITY', 'entrius-gittensor')
WANDB_PROJECT = os.getenv('WANDB_PROJECT', 'allways-validators')
WANDB_VALIDATOR_NAME = os.getenv('WANDB_VALIDATOR_NAME', 'vali')


class Validator(BaseValidatorNeuron):
    """Allways validator neuron.

    Monitors the smart contract for active swaps, verifies both
    sides of each swap using chain providers, and confirms or
    times out swaps. Processes synapse requests via axon handlers
    for miner activation, swap reservations, and swap confirmations.
    """

    def __init__(self, config=None):
        super().__init__(config=config)

        # A testnet neuron must not silently verify against mainnet spokes: unset {PREFIX}_NETWORK
        # defaults to mainnet in the providers, so a test swap would be checked on mainnet. Default
        # unset spokes to testnet here (an explicit env var still wins).
        if int(self.config.netuid) != NETUID_FINNEY:
            applied = apply_testnet_network_defaults()
            if applied:
                bt.logging.warning(
                    f'testnet neuron (netuid {self.config.netuid}): defaulted unset spoke networks to '
                    f'testnet {applied} — set {{PREFIX}}_NETWORK to override.'
                )

        # One rpc-url source of truth shared by every SOL consumer: the chain
        # providers (source-leg verification) and the solana_client below.
        solana_rpc_url = resolve_rpc_url()
        self.assets = create_assets(
            check=True, require_send=False, subtensor=self.subtensor, solana_rpc_url=solana_rpc_url
        )
        self.fee_divisor = FEE_DIVISOR

        # Single store owning every validator-local table (crown event tables +
        # the Solana ingest cursor + the axon reservation pins). Created before
        # the axon handler wiring so handler threads can read/write it. Exposes
        # current block so the axon reservation pins purge lazily on read. db
        # path is overridable so a multi-validator dev env can give each process
        # its own file — shared DBs race on writes.
        state_db_path = getattr(getattr(self.config, 'validator', None), 'state_db_path', None)
        self.state_store = ValidatorStateStore(
            db_path=state_db_path,
            current_block_fn=lambda: self.block,
        )
        # Mirrors crown_holders / miner_scores into Postgres for the miner
        # dashboard. Disabled by default; opt in per host with
        # STORE_DB_RESULTS=true and DB_* env vars. When disabled the scoring
        # path's storage tee is a no-op — zero overhead for validators that
        # don't write to the dashboard DB.
        self.database_storage = DatabaseStorage()

        # Solana swap loop: discovers live swaps off the contract
        # (getProgramAccounts), decides per status, verifies both legs with
        # replay-freshness gates, and casts the on-chain consensus vote. This
        # subsumes the old substrate swap_tracker discovery + verifier.
        # VALIDATOR_MODE ladder (see validator_mode()): 'watch' = observe-only (swap loop logs
        # "WOULD …", no weights), 'vote' = live contract votes but no weights, 'full' = production.
        # should_set_weights() enforces the weights half; read_only below enforces the vote half.
        mode = validator_mode()
        if mode == 'watch':
            bt.logging.warning('VALIDATOR_MODE=watch — observe-only: no Solana votes, no set_weights.')
        elif mode == 'vote':
            bt.logging.warning('VALIDATOR_MODE=vote — Solana contract votes ON, set_weights OFF.')
        solana_read_only = mode == 'watch'
        self.solana_client = AllwaysSolanaClient(solana_rpc_url, keypair=keys.load_or_create())
        # Fail closed if a testnet neuron is pointed at mainnet, or the program id is on the wrong
        # cluster — positively classified by genesis hash, not the mainnet-substring blind spot.
        assert_cluster_safe(self.solana_client.rpc, self.solana_client.program_id, self.config.netuid, role='validator')
        warn_if_unbound(self.solana_client)
        # `solana_config_cache` serves swap bounds + halt (and the relay's heartbeat freshness)
        # off one TTL-cached Config read, replacing the old substrate reads.
        self.solana_config_cache = SolanaConfigCache(self.solana_client)
        # W3 bond relay: the Bittensor-vault half of split collateral. Built only when a vault is
        # configured — a SOL-only deployment runs exactly as before. Its own subtensor connection
        # keeps a block-waiting vault extrinsic off the sockets scoring and the axon use.
        self.vault_subtensor = None
        # Kept so the forward pass can retry construction when the boot-time build returns None (F3).
        self._relay_read_only = solana_read_only
        self.bond_relay = build_bond_relay(self, read_only=solana_read_only)
        self.solana_swap_loop = SolanaSwapLoop(
            self.solana_client,
            self.assets,
            fee_divisor=self.fee_divisor,
            read_only=solana_read_only,
            relay=self.bond_relay,
        )
        # Crown-time state is sourced entirely from Solana program events (B3.6):
        # `event_ingest` polls the program's signature stream each forward step,
        # `event_index` folds the decoded events into the state_store crown
        # tables, and scoring replays those tables.
        self.event_ingest = SolanaEventIngest(self.solana_client)
        # Kicks miners stranded active under a raised min_collateral (they can't
        # be reserved, so the contract's fee/slash auto-deactivation never fires).
        self.floor_sweep = CollateralFloorSweep(self.solana_client, read_only=solana_read_only)
        # event_index synthesizes each reservation's RESERVE_EXPIRE at
        # block_time + reservation_ttl_secs, read off the config cache (D4).
        self.event_index = SolanaEventIndex(self.state_store, self.solana_config_cache.reservation_ttl_secs)

        # Forces one scoring pass per fresh process so a mid-window restart
        # doesn't leave self.scores stale until the next scoring boundary
        # (which would route emissions to RECYCLE via the empty-norm fallback).
        self.initial_scoring_done = False
        # Last completed scoring round. `last_scored_block` gates the cadence
        # (subtensor block); `last_scored_time` anchors the crown replay
        # window's start (unix seconds, the blockTime axis). Both seeded one
        # window back so a fresh process scores one trailing window.
        self.last_scored_block = max(0, self.block - SCORING_WINDOW_BLOCKS)
        self.last_scored_time = max(0, int(time.time()) - SCORING_WINDOW_SECS)

        # Stake-weight vote state (weights_vote.py): last satisfied epoch + in-epoch retry throttle.
        self.weights_epoch_done = None
        self.last_weights_attempt = 0

        # Separate subtensor + chain providers for the axon handlers (thread safety).
        # axon_lock serialises every call on axon_subtensor's websocket so two handler
        # threads can't both land in recv. The TAO provider gets its own dedicated
        # connection instead of sharing that websocket: a TAO source verify can scan
        # blocks for seconds, so holding axon_lock across it would stall every other
        # handler, and running it unlocked would race the locked reads' recv (#456).
        self.axon_lock = threading.RLock()
        self.axon_subtensor = bt.Subtensor(config=self.config)
        self.axon_assets = create_assets(subtensor=bt.Subtensor(config=self.config), solana_rpc_url=solana_rpc_url)
        bt.logging.debug(f'Validator components: fee_divisor={self.fee_divisor}')

        # Attach synapse handlers to axon
        self.attach_axon_handlers()

        # Optional localhost seam for a product offering to enter reservations on-behalf (off unless
        # ALLWAYS_SEAM_SECRET is set). Generic validators run without it.
        self.seam_server = maybe_start_seam(self)

        bt.logging.info(f'Validator initialized: hotkey={self.wallet.hotkey.ss58_address}')

        # wandb captures the validator's console output for the run; no per-step
        # log calls needed. Wrapped so a wandb outage never takes down scoring.
        if not self.config.neuron.wandb_off:
            try:
                wandb.init(
                    entity=WANDB_ENTITY,
                    project=WANDB_PROJECT,
                    name=f'{WANDB_VALIDATOR_NAME}-{self.uid}-{__version__}',
                    config=self.config,
                    reinit=True,
                )
            except Exception as e:
                bt.logging.error(f'Failed to initialize wandb run: {e}')

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
        tao_provider = self.assets.get('tao')
        if tao_provider and hasattr(tao_provider, 'subtensor'):
            tao_provider.subtensor = self.subtensor

    def reconnect_axon_subtensor(self):
        """Rebuild the axon-side subtensor used by handler threads."""
        bt.logging.info('Reconnecting axon subtensor...')
        old = self.axon_subtensor
        self.axon_subtensor = bt.Subtensor(config=self.config)
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
            if getattr(self, 'seam_server', None) is not None:
                self.seam_server.shutdown()
            if getattr(self, 'vault_subtensor', None) is not None:
                try:
                    self.vault_subtensor.close()
                except Exception:
                    pass
            self.state_store.close()
            self.database_storage.close()


# Main entry point
if __name__ == '__main__':
    with Validator() as validator:
        while True:
            forward_age = time.time() - validator.last_forward_time
            if not validator.thread.is_alive():
                bt.logging.error(f'Forward thread is dead (last forward {forward_age:.0f}s ago) — exiting for restart')
                sys.exit(1)
            if forward_age > FORWARD_STALL_THRESHOLD_SECONDS:
                bt.logging.error(
                    f'Forward progress stalled for {forward_age:.0f}s '
                    f'(>{FORWARD_STALL_THRESHOLD_SECONDS}s) — exiting for restart'
                )
                sys.exit(1)
            bt.logging.info(f'Validator running... step={validator.step} (last forward {forward_age:.0f}s ago)')
            time.sleep(60)

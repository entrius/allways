"""Allways Miner - Entry Point

Polls the smart contract for new swaps, verifies receipt, and fulfills orders.

Usage:
    python neurons/miner.py --netuid 7 --wallet.name default --wallet.hotkey default
"""

import time
from pathlib import Path
from typing import Dict, Optional

import bittensor as bt
from dotenv import load_dotenv

from allways.chain_providers import create_chain_providers
from allways.commitments import read_miner_commitment
from allways.constants import DEFAULT_FEE_DIVISOR, MINER_STATUS_LOG_INTERVAL_STEPS, TAO_TO_RAO
from allways.contract_client import AllwaysContractClient, ContractError
from allways.miner.fulfillment import SwapFulfiller
from allways.miner.swap_poller import SwapPoller
from neurons.base.miner import BaseMinerNeuron

load_dotenv()


class Miner(BaseMinerNeuron):
    """Allways miner neuron.

    Polls the contract for new swaps assigned to this miner,
    verifies the user sent source funds, then fulfills the order
    by sending destination funds.
    """

    def __init__(self, config=None):
        super().__init__(config=config)

        self.contract_client = AllwaysContractClient(subtensor=self.subtensor)
        self.chain_providers = create_chain_providers(check=True, subtensor=self.subtensor, wallet=self.wallet)

        self.swap_poller = SwapPoller(
            contract_client=self.contract_client,
            miner_hotkey=self.wallet.hotkey.ss58_address,
        )

        try:
            fee_divisor = self.contract_client.get_fee_divisor() or DEFAULT_FEE_DIVISOR
        except Exception:
            fee_divisor = DEFAULT_FEE_DIVISOR

        hotkey = self.wallet.hotkey.ss58_address
        sent_cache_path = Path.home() / '.allways' / 'miner' / f'sent_cache_{hotkey[:12]}.json'
        self._rate_flag_path = Path.home() / '.allways' / 'miner' / f'rate_posted_{hotkey[:12]}.flag'

        self.my_addresses: Dict[str, str] = self._load_my_addresses()

        self.swap_fulfiller = SwapFulfiller(
            contract_client=self.contract_client,
            chain_providers=self.chain_providers,
            wallet=self.wallet,
            subtensor=self.subtensor,
            netuid=self.config.netuid,
            metagraph=self.metagraph,
            fee_divisor=fee_divisor,
            sent_cache_path=sent_cache_path,
            my_addresses=self.my_addresses,
        )

        self._last_status_step = 0
        self._consecutive_poll_failures = 0
        self._last_pair: Optional[str] = None
        self._last_active: Optional[bool] = None
        self._last_collateral: Optional[int] = None

        bt.logging.info(f'Miner initialized: hotkey={self.wallet.hotkey.ss58_address}')
        self._log_status()

    def _load_my_addresses(self) -> Dict[str, str]:
        """Read this miner's committed pair once and map chain → address.

        Stored as ``self.my_addresses`` and shared with ``SwapFulfiller`` so
        the fulfill path doesn't need to reach back into substrate storage on
        every send. Refreshed whenever the CLI signals a rate post via the
        flag file written by ``alw miner post``.
        """
        hotkey = self.wallet.hotkey.ss58_address
        try:
            pair = read_miner_commitment(self.subtensor, self.config.netuid, hotkey, metagraph=self.metagraph)
        except Exception as e:
            bt.logging.warning(f'Could not read own commitment at startup: {e}')
            return {}
        if pair is None:
            return {}
        return {pair.source_chain: pair.source_address, pair.dest_chain: pair.dest_address}

    def _maybe_reload_my_addresses(self) -> None:
        """If the CLI wrote a rate-posted flag, refresh the address cache."""
        try:
            if not self._rate_flag_path.exists():
                return
            fresh = self._load_my_addresses()
            if fresh:
                self.my_addresses.clear()
                self.my_addresses.update(fresh)
                bt.logging.info(f'Miner addresses refreshed after rate post: {self.my_addresses}')
            self._rate_flag_path.unlink(missing_ok=True)
        except Exception as e:
            bt.logging.debug(f'Rate-posted flag check failed: {e}')

    def _read_current_state(self) -> tuple:
        """Read current miner state from contract and chain. Returns (pair_key, pair, is_active, collateral_rao)."""
        hotkey = self.wallet.hotkey.ss58_address

        pair = read_miner_commitment(self.subtensor, self.config.netuid, hotkey, metagraph=self.metagraph)
        pair_key = f'{pair.source_chain}:{pair.dest_chain}:{pair.rate}' if pair else None

        is_active = None
        collateral_rao = None
        try:
            collateral_rao = self.contract_client.get_miner_collateral(hotkey)
            is_active = self.contract_client.get_miner_active_flag(hotkey)
        except ContractError:
            bt.logging.warning('Could not read contract state')

        return pair_key, pair, is_active, collateral_rao

    def _log_status(self) -> None:
        """Log full miner state: collateral, active flag, committed pair."""
        pair_key, pair, is_active, collateral_rao = self._read_current_state()

        if is_active is not None and collateral_rao is not None:
            collateral_tao = collateral_rao / TAO_TO_RAO
            status = 'ACTIVE' if is_active else 'INACTIVE'
            bt.logging.info(f'Status: {status} | collateral: {collateral_tao:.4f} TAO')
            self._last_active = is_active
            self._last_collateral = collateral_rao

        if pair:
            bt.logging.info(f'Committed pair: {pair.source_chain.upper()} -> {pair.dest_chain.upper()} @ {pair.rate}')
        else:
            bt.logging.warning('No committed pair found on chain')
        self._last_pair = pair_key

    def _check_state_changes(self) -> None:
        """Detect and log commitment/contract state changes since last check."""
        pair_key, pair, is_active, collateral_rao = self._read_current_state()

        if pair_key != self._last_pair:
            if pair:
                bt.logging.info(f'Pair updated: {pair.source_chain.upper()} -> {pair.dest_chain.upper()} @ {pair.rate}')
            else:
                bt.logging.warning('Committed pair removed')
            self._last_pair = pair_key

        if is_active is not None:
            if is_active != self._last_active and self._last_active is not None:
                action = 'Activated' if is_active else 'Deactivated'
                bt.logging.info(f'Miner {action}')
            self._last_active = is_active

        if collateral_rao is not None:
            if collateral_rao != self._last_collateral and self._last_collateral is not None:
                collateral_tao = collateral_rao / TAO_TO_RAO
                bt.logging.info(f'Collateral changed: {collateral_tao:.4f} TAO')
            self._last_collateral = collateral_rao

    def _refresh_fee_divisor(self):
        """Refresh fee_divisor from contract to stay in sync."""
        try:
            new_divisor = self.contract_client.get_fee_divisor()
            if new_divisor and new_divisor != self.swap_fulfiller.fee_divisor:
                bt.logging.info(f'Fee divisor updated: {self.swap_fulfiller.fee_divisor} -> {new_divisor}')
                self.swap_fulfiller.fee_divisor = new_divisor
        except Exception:
            pass

    def _reconnect_and_propagate(self):
        """Reconnect subtensor and propagate the new connection to all dependents."""
        self._reconnect_subtensor()
        self.contract_client.subtensor = self.subtensor
        self.swap_fulfiller.subtensor = self.subtensor
        tao_provider = self.chain_providers.get('tao')
        if tao_provider and hasattr(tao_provider, 'subtensor'):
            tao_provider.subtensor = self.subtensor

    async def forward(self):
        """Main miner forward pass — polls for swaps and processes each one."""
        self._maybe_reload_my_addresses()
        if self.step - self._last_status_step >= MINER_STATUS_LOG_INTERVAL_STEPS:
            self._log_status()
            self._refresh_fee_divisor()
            self._last_status_step = self.step
        else:
            self._check_state_changes()

        active_swaps, _fulfilled = self.swap_poller.poll()

        if not self.swap_poller.last_poll_ok:
            self._consecutive_poll_failures += 1
            if self._consecutive_poll_failures >= 3:
                bt.logging.warning(
                    f'{self._consecutive_poll_failures} consecutive poll failures, reconnecting subtensor'
                )
                self._reconnect_and_propagate()
                self._consecutive_poll_failures = 0
            return
        self._consecutive_poll_failures = 0

        active_count = len(self.swap_poller.active)

        self.swap_fulfiller.cleanup_stale_sends(set(self.swap_poller.active.keys()))

        if active_swaps:
            bt.logging.debug(f'Processing {len(active_swaps)} active swap(s)')
        elif active_count > 0:
            bt.logging.debug(f'Tracking {active_count} active swap(s)')
        else:
            bt.logging.debug('Polling... no active swaps')

        for swap in active_swaps:
            try:
                success = self.swap_fulfiller.process_swap(swap)
                if not success:
                    bt.logging.debug(f'Swap {swap.id} not ready for fulfillment yet')
            except Exception as e:
                bt.logging.error(f'Error processing swap {swap.id}: {e}')


# Main entry point
if __name__ == '__main__':
    with Miner() as miner:
        while True:
            bt.logging.info(f'Miner running... step={miner.step}')
            time.sleep(60)

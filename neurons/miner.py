"""Allways Miner - Entry Point

Polls the smart contract for new swaps, verifies receipt, and fulfills orders.

Usage:
    python neurons/miner.py --netuid 7 --wallet.name default --wallet.hotkey default
"""

import os
import time
from pathlib import Path
from typing import Dict

import bittensor as bt
from dotenv import load_dotenv

from allways.chain_providers import create_chain_providers
from allways.commitments import read_miner_commitment
from allways.constants import FEE_DIVISOR
from allways.contract_client import AllwaysContractClient
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

        self.unlock_coldkey()

        self.contract_client = AllwaysContractClient(
            subtensor=self.subtensor,
            reconnect_subtensor=self.reconnect_and_propagate,
        )
        self.chain_providers = create_chain_providers(check=True, subtensor=self.subtensor, wallet=self.wallet)

        self.swap_poller = SwapPoller(
            contract_client=self.contract_client,
            miner_hotkey=self.wallet.hotkey.ss58_address,
        )

        hotkey = self.wallet.hotkey.ss58_address
        sent_cache_path = Path.home() / '.allways' / 'miner' / f'sent_cache_{hotkey[:12]}.json'
        self.rate_flag_path = Path.home() / '.allways' / 'miner' / f'rate_posted_{hotkey[:12]}.flag'

        self.my_addresses: Dict[str, str] = self.load_my_addresses()

        self.swap_fulfiller = SwapFulfiller(
            contract_client=self.contract_client,
            chain_providers=self.chain_providers,
            wallet=self.wallet,
            subtensor=self.subtensor,
            fee_divisor=FEE_DIVISOR,
            sent_cache_path=sent_cache_path,
            my_addresses=self.my_addresses,
        )

        self.consecutive_poll_failures = 0

        bt.logging.info(f'Miner initialized: hotkey={self.wallet.hotkey.ss58_address} | addresses={self.my_addresses}')

    def unlock_coldkey(self) -> None:
        """Cache the coldkey password through bittensor's keyfile so every
        later ``unlock_coldkey()`` call is non-interactive. As of bittensor
        10.3.0, ``subtensor.transfer`` re-runs ``unlock_coldkey()`` on every
        extrinsic; without a cached password, each transfer re-prompts and a
        detached miner reads garbage from stdin, producing spurious
        "password invalid" errors mid-swap.

        Reads ``MINER_BITTENSOR_COLDKEY_PASSWORD`` if set, otherwise prompts
        once at startup. Uses ``save_password_to_env`` (not direct
        ``os.environ`` assignment) because bittensor-wallet 4.0.1 stores the
        password via its own encoding — a plaintext value in the
        ``BT_PW__...`` env var is rejected as malformed base64.
        """
        from getpass import getpass

        password = os.environ.get('MINER_BITTENSOR_COLDKEY_PASSWORD')
        if not password:
            password = getpass(f'Enter password to unlock coldkey ({self.wallet.name}): ')
        self.wallet.coldkey_file.save_password_to_env(password)
        self.wallet.unlock_coldkey()
        bt.logging.info('Bittensor coldkey unlocked')

    def load_my_addresses(self) -> Dict[str, str]:
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
        return {pair.from_chain: pair.from_address, pair.to_chain: pair.to_address}

    def maybe_reload_my_addresses(self) -> None:
        """If the CLI wrote a rate-posted flag, refresh the address cache."""
        try:
            if not self.rate_flag_path.exists():
                return
            fresh = self.load_my_addresses()
            if fresh:
                self.my_addresses.clear()
                self.my_addresses.update(fresh)
                bt.logging.info(f'Miner addresses refreshed after rate post: {self.my_addresses}')
            self.rate_flag_path.unlink(missing_ok=True)
        except Exception as e:
            bt.logging.debug(f'Rate-posted flag check failed: {e}')

    def reconnect_and_propagate(self):
        """Reconnect subtensor and propagate the new connection to all dependents."""
        self.reconnect_subtensor()
        self.contract_client.subtensor = self.subtensor
        self.swap_fulfiller.subtensor = self.subtensor
        tao_provider = self.chain_providers.get('tao')
        if tao_provider and hasattr(tao_provider, 'subtensor'):
            tao_provider.subtensor = self.subtensor

    async def forward(self):
        """Main miner forward pass — polls for swaps and processes each one."""
        self.check_block_progress(self.reconnect_and_propagate)
        self.maybe_reload_my_addresses()

        active_swaps, _fulfilled = self.swap_poller.poll()

        if not self.swap_poller.last_poll_ok:
            self.consecutive_poll_failures += 1
            if self.consecutive_poll_failures >= 3:
                bt.logging.warning(
                    f'{self.consecutive_poll_failures} consecutive poll failures, reconnecting subtensor'
                )
                self.reconnect_and_propagate()
                self.consecutive_poll_failures = 0
            return
        self.consecutive_poll_failures = 0

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

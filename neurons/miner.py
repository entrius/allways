"""Allways Miner - Entry Point

Polls the smart contract for new swaps, verifies receipt, and fulfills orders.

Usage:
    python neurons/miner.py --netuid 7 --wallet.name default --wallet.hotkey default
"""

import os
import sys
import time
from pathlib import Path
from typing import Dict

from dotenv import load_dotenv

# Must precede the allways imports: they resolve env-backed settings, and a later load would be a no-op.
load_dotenv()

import bittensor as bt  # noqa: E402
from bittensor import Keypair as BtKeypair  # noqa: E402

from allways.chain_providers import create_chain_providers  # noqa: E402
from allways.constants import FORWARD_STALL_THRESHOLD_SECONDS  # noqa: E402
from allways.miner.fulfillment import SwapFulfiller  # noqa: E402
from allways.miner.swap_poller import SwapPoller  # noqa: E402
from allways.solana import keys  # noqa: E402
from allways.solana.client import AllwaysSolanaClient  # noqa: E402
from allways.solana.rpc import resolve_rpc_url  # noqa: E402
from neurons.base.miner import BaseMinerNeuron  # noqa: E402


class Miner(BaseMinerNeuron):
    """Allways miner neuron.

    Polls the contract for new swaps assigned to this miner,
    verifies the user sent source funds, then fulfills the order
    by sending destination funds.
    """

    def __init__(self, config=None):
        super().__init__(config=config)

        self.unlock_coldkey()

        # Solana program client (signer = the miner's Solana keypair; separate from the bt wallet).
        solana_rpc_url = resolve_rpc_url()
        self.solana_client = AllwaysSolanaClient(solana_rpc_url, keypair=keys.load_or_create())
        self.solana_pubkey = self.solana_client.keypair.pubkey()
        # SOL swap-leg provider signs the dest leg with the same Solana keypair (peer-to-peer
        # user↔miner transfer; separate from the program client that never custodies swap assets).
        self.chain_providers = create_chain_providers(
            check=True,
            subtensor=self.subtensor,
            wallet=self.wallet,
            solana_rpc_url=solana_rpc_url,
            solana_keypair=self.solana_client.keypair,
        )

        # Bind the bt hotkey ↔ Solana pubkey so on-chain state (keyed by pubkey) attributes to this UID.
        self.ensure_hotkey_bound()

        self.swap_poller = SwapPoller(self.solana_client, self.solana_pubkey)

        hotkey = self.wallet.hotkey.ss58_address
        sent_cache_path = Path.home() / '.allways' / 'miner' / f'sent_cache_{hotkey[:12]}.json'
        self.rate_flag_path = Path.home() / '.allways' / 'miner' / f'rate_posted_{hotkey[:12]}.flag'

        self.my_addresses: Dict[str, str] = self.load_my_addresses()

        self.swap_fulfiller = SwapFulfiller(
            solana_client=self.solana_client,
            chain_providers=self.chain_providers,
            sent_cache_path=sent_cache_path,
            my_addresses=self.my_addresses,
        )

        self.consecutive_poll_failures = 0

        bt.logging.info(
            f'Miner initialized: hotkey={hotkey} | pubkey={self.solana_pubkey} | addresses={self.my_addresses}'
        )

    def ensure_hotkey_bound(self) -> None:
        """Best-effort: bind the bt hotkey to this Solana pubkey if not already bound.

        The miner's hotkey (sr25519) signs its own Solana pubkey bytes; the contract stores the hotkey +
        signature on a per-miner `Binding` PDA and validators verify it off-chain. Idempotent — skips if
        a binding already exists. On failure the miner keeps running; `alw miner bind-hotkey` can retry.
        """
        try:
            if self.solana_client.get_binding(self.solana_pubkey) is not None:
                return
        except Exception as e:
            bt.logging.warning(f'Could not read binding state: {e}; skipping auto-bind')
            return
        hotkey_bytes = bytes(self.wallet.hotkey.public_key)
        sig = self.wallet.hotkey.sign(bytes(self.solana_pubkey))
        # Sanity: re-verify locally exactly as the validator will (sr25519 hotkey over the pubkey bytes).
        if not BtKeypair(public_key='0x' + hotkey_bytes.hex()).verify(bytes(self.solana_pubkey), sig):
            bt.logging.error('Self-produced hotkey binding failed local verify; not submitting')
            return
        try:
            self.solana_client.bind_hotkey(hotkey_bytes, sig)
            bt.logging.success(f'Bound hotkey {self.wallet.hotkey.ss58_address} → Solana pubkey {self.solana_pubkey}')
        except Exception as e:
            bt.logging.error(f'bind_hotkey failed (run `alw miner bind-hotkey` to retry): {e}')

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
        """Read this miner's on-chain quotes once and map chain → address.

        Each `MinerQuote` PDA carries the miner's address on both legs of a pair
        (``miner_from_addr`` on ``from_chain``, ``miner_to_addr`` on ``to_chain``).
        Stored as ``self.my_addresses`` and shared with ``SwapFulfiller`` so the
        fulfill path has the dest-chain sending address without a per-send read.
        Refreshed whenever the CLI signals a quote post via the flag file written
        by ``alw miner post``.
        """
        my = str(self.solana_pubkey)
        addrs: Dict[str, str] = {}
        try:
            for _pda, q in self.solana_client.get_all('MinerQuote'):
                if str(q.miner) != my:
                    continue
                addrs[q.from_chain] = q.miner_from_addr
                addrs[q.to_chain] = q.miner_to_addr
        except Exception as e:
            bt.logging.warning(f'Could not read own quotes at startup: {e}')
            return {}
        if not addrs:
            bt.logging.warning(
                'No on-chain quotes found for this miner; it will not be able to fulfill swaps until '
                '`alw miner post` is run'
            )
        return addrs

    def maybe_reload_my_addresses(self) -> None:
        """If the CLI wrote a quote-posted flag, refresh the address cache."""
        try:
            if not self.rate_flag_path.exists():
                return
            fresh = self.load_my_addresses()
            if fresh:
                self.my_addresses.clear()
                self.my_addresses.update(fresh)
                bt.logging.info(f'Miner addresses refreshed after quote post: {self.my_addresses}')
            else:
                bt.logging.warning(
                    'Quote-posted flag set but no quote readable on chain yet; address cache left untouched'
                )
            self.rate_flag_path.unlink(missing_ok=True)
        except Exception as e:
            bt.logging.debug(f'Quote-posted flag check failed: {e}')

    def reconnect_and_propagate(self):
        """Reconnect subtensor and propagate the new connection to its dependents.

        The Solana client uses its own RPC (not subtensor), so only the subtensor-backed
        chain providers need the refreshed connection.
        """
        self.reconnect_subtensor()
        tao_provider = self.chain_providers.get('tao')
        if tao_provider and hasattr(tao_provider, 'subtensor'):
            tao_provider.subtensor = self.subtensor

    async def forward(self):
        """Main miner forward pass — polls for swaps and processes each one."""
        self.check_block_progress(self.reconnect_and_propagate)
        self.maybe_reload_my_addresses()

        active_swaps, fulfilled_swaps = self.swap_poller.poll()

        if not self.swap_poller.last_poll_ok:
            self.consecutive_poll_failures += 1
            if self.consecutive_poll_failures >= 3:
                bt.logging.warning(
                    f'{self.consecutive_poll_failures} consecutive poll failures, reconnecting subtensor'
                )
                self.reconnect_and_propagate()
                self.consecutive_poll_failures = 0
            return
        if self.consecutive_poll_failures > 0:
            bt.logging.info(f'Poll recovered after {self.consecutive_poll_failures} consecutive failure(s)')
        self.consecutive_poll_failures = 0

        # Retain send-cache entries for every swap still live on-chain (Active or Fulfilled-awaiting-confirm).
        live_keys = {s.key_hex for s in active_swaps} | {s.key_hex for s in fulfilled_swaps}
        self.swap_fulfiller.cleanup_stale_sends(live_keys)

        if active_swaps:
            bt.logging.debug(f'Processing {len(active_swaps)} active swap(s)')
        elif live_keys:
            bt.logging.debug(f'Tracking {len(live_keys)} live swap(s)')
        else:
            bt.logging.debug('Polling... no active swaps')

        for swap in active_swaps:
            try:
                self.swap_fulfiller.process_swap(swap)
            except Exception as e:
                bt.logging.error(f'Error processing swap {swap.key_hex[:16]}: {type(e).__name__}: {e}')


# Main entry point
if __name__ == '__main__':
    with Miner() as miner:
        while True:
            forward_age = time.time() - miner.last_forward_time
            if not miner.thread.is_alive():
                bt.logging.error(f'Forward thread is dead (last forward {forward_age:.0f}s ago) — exiting for restart')
                sys.exit(1)
            if forward_age > FORWARD_STALL_THRESHOLD_SECONDS:
                bt.logging.error(
                    f'Forward progress stalled for {forward_age:.0f}s '
                    f'(>{FORWARD_STALL_THRESHOLD_SECONDS}s) — exiting for restart'
                )
                sys.exit(1)
            bt.logging.info(f'Miner running... step={miner.step} (last forward {forward_age:.0f}s ago)')
            time.sleep(60)

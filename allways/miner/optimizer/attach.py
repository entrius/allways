"""Attach the quote optimizer to a running base miner — the one call ``neurons/miner.py`` makes.

Everything the optimizer needs it reads from the miner it is handed; the base miner knows nothing about it beyond
that call. It runs on its own thread, so a slow API call or transaction never holds up swap fulfillment, and it
pulls its quotes when the process exits. Removing it is deleting ``allways/miner/optimizer/`` and the call.
"""

import atexit
import signal
import sys
import threading
from pathlib import Path
from typing import Optional

import bittensor as bt

from allways.miner.optimizer.market_feed import MarketFeed
from allways.miner.optimizer.miner_api import AllwaysApi, resolve_api_url
from allways.miner.optimizer.quote_optimizer import (
    DEFAULT_OPTIMIZER_CONFIG_PATH,
    OptimizerConfig,
    QuoteOptimizer,
    pending_payouts,
)
from allways.solana.client import AllwaysSolanaClient
from allways.solana.rpc import resolve_ws_url


def attach_optimizer(miner, config_path: Path = DEFAULT_OPTIMIZER_CONFIG_PATH) -> Optional[QuoteOptimizer]:
    """Start the optimizer on ``miner`` if ``optimizer.json`` enables it, else return None. A malformed config
    raises: the operator meant to turn it on, and quoting on a config they didn't write is worse than not starting."""
    path = Path(config_path).expanduser()
    cfg = OptimizerConfig.load(path)
    if not cfg.enabled:
        bt.logging.info(f'Quote optimizer off (set "enabled": true in {path} to turn it on)')
        return None
    missing = sorted(chain for chain in cfg.chains() if chain not in miner.assets)
    if missing:
        bt.logging.error(f'Quote optimizer not started: this miner has no {", ".join(missing)} chain provider')
        return None
    hotkey = miner.wallet.hotkey.ss58_address
    rpc_url = miner.solana_client.rpc.url
    # Its own client (same RPC, same signer), so the hourly usage line counts the optimizer's calls alone.
    client = AllwaysSolanaClient(rpc_url, keypair=miner.solana_client.keypair)
    optimizer = QuoteOptimizer(
        cfg=cfg,
        solana_client=client,
        assets=miner.assets,
        hotkey=hotkey,
        # A dry run paper-trades into its own state file, so a later live run never mistakes a paper pull for a real one.
        state_path=Path.home()
        / '.allways'
        / 'miner'
        / f'optimizer_state_{hotkey[:12]}{".dry_run" if cfg.dry_run else ""}.json',
        pending_payouts_fn=lambda chain: pending_payouts(
            miner.swap_fulfiller.active_obligations, miner.swap_fulfiller.sent, chain
        ),
        feed=MarketFeed(
            resolve_ws_url(rpc_url), client.program_id, client.keypair.pubkey(), cfg.lanes, decode=client._decode
        ),
        # A re-posted quote's addresses join the fulfiller's address cache (the same dict), so its swaps get paid.
        on_quote_posted=lambda lane, from_addr, to_addr: miner.my_addresses.update(
            {lane.from_chain: from_addr, lane.to_chain: to_addr}
        ),
        # What a dry run paper-posts with on a lane that has no quote: the Solana key and the TAO coldkey.
        paper_addresses={
            **miner.my_addresses,
            'sol': str(client.keypair.pubkey()),
            'tao': miner.wallet.coldkeypub.ss58_address,
        },
        is_registered=lambda: hotkey in miner.metagraph.hotkeys,
        api=AllwaysApi(resolve_api_url(miner.config.netuid)),
    )
    optimizer.start_thread()
    atexit.register(optimizer.shutdown, 'miner stopping')
    exit_cleanly_on_sigterm()
    return optimizer


def exit_cleanly_on_sigterm() -> None:
    """``docker stop`` sends SIGTERM, which by default ends Python without running exit handlers — the managed quotes
    would stay up on a stopped miner. Turn it into a normal exit, after any handler already installed."""
    if threading.current_thread() is not threading.main_thread():
        return
    previous = signal.getsignal(signal.SIGTERM)
    if previous is signal.SIG_IGN:
        return

    def on_sigterm(signum, frame):
        if callable(previous):
            previous(signum, frame)
        sys.exit(0)

    signal.signal(signal.SIGTERM, on_sigterm)

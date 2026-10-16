"""Allways validator, component-based (validator_v2). Runs in SHADOW by default: it decides everything
and logs what it would send, but sends nothing and sets no weights. Same command line and .env as
neurons/validator.py, so it can run beside the current validator for a side-by-side comparison.

    python neurons/validator_v2.py --netuid 19 --subtensor.network test --wallet.name v --wallet.hotkey v

Env: VALIDATOR_V2_LIVE=1 sends for real (not before the cutover). ALLWAYS_RPC_ENDPOINTS sets per-endpoint
rate limits and daily budgets, e.g. ``helius=mainnet.helius-rpc.com:8:16:150000``.
"""

import os
import sys

from dotenv import load_dotenv

load_dotenv()  # before the allways imports: they read env-backed settings at import

import bittensor as bt  # noqa: E402

from allways.assets import create_assets  # noqa: E402
from allways.chains import apply_testnet_network_defaults  # noqa: E402
from allways.constants import HUB_CHAINS, NETUID_FINNEY  # noqa: E402
from allways.solana import keys  # noqa: E402
from allways.solana.client import AllwaysSolanaClient  # noqa: E402
from allways.solana.rpc import assert_cluster_safe, resolve_rpc_url  # noqa: E402
from allways.utils.config import add_args, add_validator_args, check_config, config  # noqa: E402
from allways.utils.subtensor import build_subtensor, serialize_subtensor  # noqa: E402
from allways.validator_v2.app import Config, build  # noqa: E402
from allways.validator_v2.chain.gateway import parse_endpoints  # noqa: E402


class ValidatorV2:
    neuron_type = 'ValidatorNeuron'

    @classmethod
    def add_args(cls, parser):
        add_args(cls, parser)
        add_validator_args(cls, parser)


def main() -> None:
    cfg = config(ValidatorV2)
    check_config(ValidatorV2, cfg)
    bt.logging.set_config(config=cfg.logging)
    netuid = int(cfg.netuid)
    if netuid != NETUID_FINNEY and (applied := apply_testnet_network_defaults()):
        bt.logging.warning(f'testnet neuron (netuid {netuid}): unset spoke networks default to testnet {applied}')

    rpc_url = resolve_rpc_url()
    keypair = keys.load_or_create()
    probe = AllwaysSolanaClient(rpc_url, keypair=keypair)
    # Fail closed if a testnet neuron points at mainnet or the program id is on the wrong cluster.
    assert_cluster_safe(probe.rpc, probe.program_id, netuid, role='validator')

    live = os.environ.get('VALIDATOR_V2_LIVE') == '1'
    if live:
        bt.logging.warning('VALIDATOR_V2_LIVE=1: validator_v2 will SEND program transactions')
    state_dir = os.path.join(cfg.neuron.full_path, 'v2')
    os.makedirs(state_dir, exist_ok=True)
    # The spoke providers get their own subtensor: the TAO watcher is its only user.
    assets = create_assets(
        check=True,
        require_send=False,
        required_chains=set(HUB_CHAINS),
        subtensor=serialize_subtensor(build_subtensor(config=cfg)),
        solana_rpc_url=rpc_url,
    )
    app = build(
        Config(
            solana_rpc_url=rpc_url,
            program_id=probe.program_id,
            keypair=keypair,
            state_dir=state_dir,
            netuid=netuid,
            endpoints=tuple(parse_endpoints(os.environ.get('ALLWAYS_RPC_ENDPOINTS', ''))),
            shadow=not live,
        ),
        assets=assets,
        subtensor_factory=lambda: build_subtensor(config=cfg),
    )
    bt.logging.info(
        f'validator_v2 {"LIVE" if live else "shadow"}: {len(app.components)} components, state in {state_dir}'
    )
    app.supervisor.run_forever()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

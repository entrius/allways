"""Allways validator, component-based (validator_v2). Same command line and .env as neurons/validator.py.

    python neurons/validator_v2.py --netuid 19 --subtensor.network test --wallet.name v --wallet.hotkey v

Env: ALLWAYS_RPC_ENDPOINTS sets per-endpoint rate limits and daily budgets, e.g. ``helius=mainnet.helius-rpc.com:8:16:150000``.
"""

import os
import sys

from dotenv import load_dotenv

load_dotenv()  # before the allways imports: they read env-backed settings at import

import bittensor as bt  # noqa: E402

from allways.assets import create_assets  # noqa: E402
from allways.chains import apply_testnet_network_defaults  # noqa: E402
from allways.constants import HUB_CHAINS, NETUID_FINNEY  # noqa: E402
from allways.solana import keys, pdas  # noqa: E402
from allways.solana.client import AllwaysSolanaClient  # noqa: E402
from allways.solana.rpc import assert_cluster_safe, resolve_rpc_url  # noqa: E402
from allways.utils.config import add_args, add_validator_args, check_config, config  # noqa: E402
from allways.utils.subtensor import build_subtensor, serialize_subtensor  # noqa: E402
from allways.validator_v2.app import Config, build  # noqa: E402
from allways.validator_v2.chain.gateway import parse_endpoints  # noqa: E402
from allways.validator_v2.export.storage import DatabaseStorage, export_enabled  # noqa: E402
from allways.validator_v2.relay.component import ink_vault  # noqa: E402
from allways.vault import VaultConfigError  # noqa: E402
from neurons.base.neuron import validator_mode  # noqa: E402


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

    mode = validator_mode()
    if mode == 'watch':
        raise SystemExit('VALIDATOR_MODE=watch is not supported by validator_v2: it always casts its votes')
    # 'vote' casts program votes but leaves Bittensor weights alone.
    sets_weights = mode == 'full' and not cfg.neuron.disable_set_weights

    rpc_url = resolve_rpc_url()
    keypair = keys.load_or_create()
    probe = AllwaysSolanaClient(rpc_url, keypair=keypair)
    # Fail closed if a testnet neuron points at mainnet or the program id is on the wrong cluster.
    assert_cluster_safe(probe.rpc, probe.program_id, netuid, role='validator')

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
    wallet = bt.Wallet(config=cfg)
    vaults = {}
    try:  # the vault address is the relay's on/off switch: without one, nothing is relayed
        vaults[pdas.BACKING_CHAIN_TAO] = ink_vault(lambda: build_subtensor(config=cfg), wallet)
    except VaultConfigError as e:
        bt.logging.info(f'bond relay off: {e}')
    # Serving: the axon unless --neuron.axon_off, the seam when ALLWAYS_SEAM_SECRET is set. It checks chains on
    # its own connections, as the old axon handlers did.
    serve_axon = not cfg.neuron.axon_off
    secret = os.environ.get('ALLWAYS_SEAM_SECRET', '')
    seam = (int(os.environ.get('ALLWAYS_SEAM_PORT', '8710')), secret) if secret else None
    serving_assets = {}
    if serve_axon or seam:
        own = create_assets(subtensor=serialize_subtensor(build_subtensor(config=cfg)), solana_rpc_url=rpc_url)
        serving_assets = {chain: provider for chain, provider in own.items() if chain in assets}
    app = build(
        Config(
            solana_rpc_url=rpc_url,
            program_id=probe.program_id,
            keypair=keypair,
            state_dir=state_dir,
            netuid=netuid,
            endpoints=tuple(parse_endpoints(os.environ.get('ALLWAYS_RPC_ENDPOINTS', ''))),
            wallet=wallet if sets_weights else None,
            vaults=vaults,
            axon_wallet=wallet if serve_axon else None,
            neuron_config=cfg,
            seam=seam,
            serving_assets=serving_assets,
            weights_alpha=float(cfg.neuron.moving_average_alpha),
            epoch_length=int(cfg.neuron.epoch_length),
        ),
        assets=assets,
        subtensor_factory=lambda: build_subtensor(config=cfg),
        export_storage=DatabaseStorage() if export_enabled() else None,
    )
    bt.logging.info(f'validator_v2: {len(app.components)} components, state in {state_dir}')
    app.supervisor.run_forever()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

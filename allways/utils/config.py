import argparse
import os

import bittensor as bt

from allways.constants import MINER_POLL_INTERVAL_SECONDS, SCORING_EMA_ALPHA, VALIDATOR_POLL_INTERVAL_SECONDS
from allways.utils.logging import setup_events_logger


def check_config(cls, config: 'bt.Config'):
    r"""Checks/validates the config namespace object."""
    bt.logging.check_config(config)

    full_path = os.path.expanduser(
        '{}/{}/{}/netuid{}/{}'.format(
            config.logging.logging_dir,
            config.wallet.name,
            config.wallet.hotkey,
            config.netuid,
            config.neuron.name,
        )
    )
    config.neuron.full_path = os.path.expanduser(full_path)
    if not os.path.exists(config.neuron.full_path):
        os.makedirs(config.neuron.full_path, exist_ok=True)

    if not config.neuron.dont_save_events:
        events_logger = setup_events_logger(config.neuron.full_path, config.neuron.events_retention_size)
        bt.logging.register_primary_logger(events_logger.name)


def add_args(cls, parser):
    """Adds relevant arguments to the parser for operation."""

    parser.add_argument('--netuid', type=int, help='Subnet netuid', default=7)

    parser.add_argument(
        '--neuron.epoch_length',
        type=int,
        help='The default epoch length (how often we set weights, measured in 12 second blocks).',
        default=100,
    )

    parser.add_argument(
        '--neuron.events_retention_size',
        type=int,
        help='Events retention size.',
        default=2 * 1024 * 1024 * 1024,  # 2 GB
    )

    parser.add_argument(
        '--neuron.dont_save_events',
        action='store_true',
        help='If set, we dont save events to a log file.',
        default=False,
    )


def add_miner_args(cls, parser):
    """Add miner specific arguments to the parser."""

    parser.add_argument(
        '--neuron.name',
        type=str,
        help='Trials for this neuron go in neuron.root / (wallet_cold - wallet_hot) / neuron.name.',
        default='miner',
    )

    parser.add_argument(
        '--miner.poll_interval',
        type=int,
        help='Polling interval in seconds for checking new swaps.',
        default=MINER_POLL_INTERVAL_SECONDS,
    )


def add_validator_args(cls, parser):
    """Add validator specific arguments to the parser."""

    parser.add_argument(
        '--neuron.name',
        type=str,
        help='Trials for this neuron go in neuron.root / (wallet_cold - wallet_hot) / neuron.name.',
        default='validator',
    )

    parser.add_argument(
        '--neuron.disable_set_weights',
        action='store_true',
        help='Disables setting weights.',
        default=False,
    )

    parser.add_argument(
        '--neuron.moving_average_alpha',
        type=float,
        help='Moving average alpha parameter, how much to add of the new observation.',
        default=SCORING_EMA_ALPHA,
    )

    parser.add_argument(
        '--neuron.axon_off',
        '--axon_off',
        action='store_true',
        help='Set this flag to not attempt to serve an Axon.',
        default=False,
    )

    parser.add_argument(
        '--validator.poll_interval',
        type=int,
        help='Polling interval in seconds for checking swaps.',
        default=VALIDATOR_POLL_INTERVAL_SECONDS,
    )


def config(cls):
    """Returns the configuration object specific to this miner or validator after adding relevant arguments."""
    parser = argparse.ArgumentParser()
    bt.Wallet.add_args(parser)
    bt.Subtensor.add_args(parser)
    bt.logging.add_args(parser)
    bt.Axon.add_args(parser)
    cls.add_args(parser)
    return bt.Config(parser)

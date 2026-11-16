"""Allways (SN7) validator: every epoch, put 100% of weight on the pinned treasury UID.

Falls back to the burn UID when the treasury UID is unset, out of range, or held by a different hotkey.
"""

import argparse
import os
import sys
import time
from typing import Optional, Tuple

import bittensor as bt
from bittensor.utils.btlogging import loggingmachine as bt_loggingmachine

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from allways import __spec_version__, __version__  # noqa: E402
from allways.constants import (  # noqa: E402
    BURN_UID,
    EPOCH_LENGTH_BLOCKS,
    NETUID_FINNEY,
    POLL_INTERVAL_SECONDS,
    TREASURY_HOTKEY,
    TREASURY_UID,
)
from allways.subtensor import connect, endpoint_list, redact_endpoint  # noqa: E402
from allways.weights import select_target, weight_vector  # noqa: E402

VALIDATOR_MODES = ('full', 'vote', 'watch')


def validator_mode() -> str:
    """VALIDATOR_MODE from the previous validator's env contract: only 'full' (the default) sets weights.

    'vote' and 'watch' (and legacy VALIDATOR_DEV_MODE=1) never set weights before, so they still do not.
    """
    raw = os.environ.get('VALIDATOR_MODE', '').strip().lower()
    if raw:
        if raw not in VALIDATOR_MODES:
            raise ValueError(f'VALIDATOR_MODE={raw!r} invalid, want one of {", ".join(VALIDATOR_MODES)}')
        return raw
    return 'watch' if os.environ.get('VALIDATOR_DEV_MODE', '0') == '1' else 'full'


def get_config() -> 'bt.Config':
    # bittensor >= 10.5 skips CLI parsing unless this env opts in; without it every flag comes back None.
    os.environ.setdefault('BT_NO_PARSE_CLI_ARGS', 'false')
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    bt.Wallet.add_args(parser)
    bt.Subtensor.add_args(parser)
    bt.logging.add_args(parser)
    parser.add_argument('--netuid', type=int, default=NETUID_FINNEY, help='Subnet netuid.')
    parser.add_argument(
        '--neuron.epoch_length',
        type=int,
        default=EPOCH_LENGTH_BLOCKS,
        help='Blocks between weight sets (never shorter than the chain weights rate limit).',
    )
    parser.add_argument(
        '--neuron.disable_set_weights',
        action='store_true',
        default=False,
        help='Run the loop but only log the weights instead of submitting them.',
    )
    parser.add_argument(
        '--dry-run',
        '--dry_run',
        dest='dry_run',
        action='store_true',
        default=False,
        help='Log the weights that would be set, then exit. Needs no wallet and submits nothing.',
    )
    # Accepted so existing deployments keep starting; this validator serves no axon.
    parser.add_argument('--axon.port', type=int, default=None, help='Ignored (no axon is served).')
    # Non-strict: flags left over from the previous validator are ignored, not fatal.
    return bt.Config(parser)


def setup_logging(config: 'bt.Config') -> None:
    # bittensor hard-codes 25 MB x 10 for --logging.record_log; size it from the existing env contract first.
    bt_loggingmachine.DEFAULT_MAX_ROTATING_LOG_FILE_SIZE = int(os.getenv('LOG_FILE_MAX_MB', '10')) * 1024 * 1024
    bt_loggingmachine.DEFAULT_LOG_BACKUP_COUNT = int(os.getenv('LOG_FILE_BACKUPS', '4'))
    if config.logging.record_log and config.logging.logging_dir:
        os.makedirs(os.path.expanduser(config.logging.logging_dir), exist_ok=True)
    bt.logging.set_config(config=config.logging)
    if not (config.logging.debug or config.logging.trace):
        bt.logging.set_info()


def pick_target(subtensor: 'bt.Subtensor', netuid: int) -> Tuple[int, Optional[str]]:
    """Sync the metagraph and choose the UID that gets the full weight."""
    metagraph = subtensor.metagraph(netuid)
    uid, reason = select_target(list(metagraph.hotkeys), TREASURY_UID, TREASURY_HOTKEY, BURN_UID)
    if reason is None:
        bt.logging.info(f'Target: treasury UID {uid} ({TREASURY_HOTKEY})')
    else:
        bt.logging.warning(f'TREASURY FALLBACK: burning to UID {uid} because {reason}')
    return uid, reason


def dry_run(config: 'bt.Config') -> None:
    subtensor = connect(endpoint_list(primary_endpoint(config))[0])
    try:
        uid, _ = pick_target(subtensor, config.netuid)
        uids, weights = weight_vector(uid)
        bt.logging.info(
            f'DRY RUN: would set weights uids={uids} weights={weights} on netuid {config.netuid} '
            f'(version_key={__spec_version__}, chain weights_version='
            f'{subtensor.get_subnet_hyperparameters(config.netuid).weights_version}). Nothing submitted.'
        )
    finally:
        subtensor.close()


def primary_endpoint(config: 'bt.Config') -> str:
    return config.subtensor.chain_endpoint or config.subtensor.network or 'finney'


def step(subtensor: 'bt.Subtensor', wallet: 'bt.Wallet', config: 'bt.Config', submit: bool) -> None:
    """One pass: set weights if this validator is registered and its last update is old enough."""
    netuid = config.netuid
    hotkey = wallet.hotkey.ss58_address
    my_uid = subtensor.get_uid_for_hotkey_on_subnet(hotkey, netuid)
    if my_uid is None:
        bt.logging.error(f'Hotkey {hotkey} is not registered on netuid {netuid}; register it with btcli.')
        return

    since = subtensor.blocks_since_last_update(netuid, my_uid)
    interval = max(config.neuron.epoch_length, subtensor.weights_rate_limit(netuid) or 0)
    if since is not None and since <= interval:
        bt.logging.debug(f'Weights not due: {since} blocks since last update, interval {interval}')
        return

    target, _ = pick_target(subtensor, netuid)
    uids, weights = weight_vector(target)
    if not submit:
        bt.logging.info(f'Weights NOT submitted (set_weights disabled): would set uids={uids} weights={weights}')
        return

    # The SDK commits (commit-reveal subnets) or sets directly, and re-checks the chain rate limit itself.
    response = subtensor.set_weights(
        wallet=wallet,
        netuid=netuid,
        uids=uids,
        weights=weights,
        version_key=__spec_version__,
        wait_for_inclusion=True,
        wait_for_finalization=False,
    )
    if response.success:
        bt.logging.success(f'set_weights ok: uids={uids} weights={weights} (validator UID {my_uid})')
    else:
        bt.logging.error(f'set_weights failed: uids={uids} weights={weights}: {response.message}')


def run(config: 'bt.Config') -> None:
    mode = validator_mode()
    submit = mode == 'full' and not config.neuron.disable_set_weights
    if not submit:
        bt.logging.warning(
            f'set_weights is OFF (VALIDATOR_MODE={mode}, disable_set_weights={config.neuron.disable_set_weights})'
        )

    wallet = bt.Wallet(config=config)
    # Read the hotkey up front: a missing or unreadable wallet stops the validator here, with the SDK's own error.
    bt.logging.info(f'Wallet: {wallet} hotkey {wallet.hotkey.ss58_address}')
    endpoints = endpoint_list(primary_endpoint(config))
    attempt = 0
    subtensor = None
    while True:
        try:
            if subtensor is None:
                subtensor = connect(endpoints[attempt % len(endpoints)])
            step(subtensor, wallet, config, submit)
            attempt = 0
        except KeyboardInterrupt:
            raise
        except Exception as e:
            # Any chain error: drop the connection and rebuild it next pass, moving on to the next endpoint.
            bt.logging.error(f'Validator pass failed ({type(e).__name__}: {e}); reconnecting')
            attempt += 1
            if subtensor is not None:
                try:
                    subtensor.close()
                except Exception:
                    pass
                subtensor = None
            if len(endpoints) > 1:
                bt.logging.warning(f'Next subtensor endpoint: {redact_endpoint(endpoints[attempt % len(endpoints)])}')
        time.sleep(POLL_INTERVAL_SECONDS)


def main() -> None:
    config = get_config()
    setup_logging(config)
    bt.logging.info(f'Allways validator {__version__} (version_key {__spec_version__}), netuid {config.netuid}')
    if config.dry_run:
        dry_run(config)
        return
    try:
        run(config)
    except KeyboardInterrupt:
        bt.logging.info('Validator stopped.')


if __name__ == '__main__':
    main()

"""Allways (SN7) validator: every epoch, put 100% of weight on the pinned treasury UID.

Falls back to the subnet owner's UID (burned by the chain) when the treasury UID is unset, out of range, held by a
different hotkey, or the validator runs on another netuid.
"""

import argparse
import os
import sys
import time
from typing import Optional

import bittensor as bt
from bittensor.utils.btlogging import loggingmachine as bt_loggingmachine

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from allways import __spec_version__, __version__  # noqa: E402
from allways.constants import (  # noqa: E402
    EPOCH_LENGTH_BLOCKS,
    NETUID_FINNEY,
    POLL_INTERVAL_SECONDS,
    TREASURY_HOTKEY,
    TREASURY_UID,
)
from allways.subtensor import Connections, close, redact_text  # noqa: E402
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


def env_int(name: str, default: int) -> int:
    """An integer env var; unset, empty or non-numeric means the default."""
    try:
        return int(os.environ.get(name, '').strip())
    except ValueError:
        return default


def setup_logging(config: 'bt.Config') -> None:
    # bittensor hard-codes 25 MB x 10 for --logging.record_log; size it from the existing env contract first.
    bt_loggingmachine.DEFAULT_MAX_ROTATING_LOG_FILE_SIZE = env_int('LOG_FILE_MAX_MB', 10) * 1024 * 1024
    bt_loggingmachine.DEFAULT_LOG_BACKUP_COUNT = env_int('LOG_FILE_BACKUPS', 4)
    if config.logging.record_log and config.logging.logging_dir:
        os.makedirs(os.path.expanduser(config.logging.logging_dir), exist_ok=True)
    bt.logging.set_config(config=config.logging)
    if not (config.logging.debug or config.logging.trace):
        bt.logging.set_info()


def primary_endpoint(config: 'bt.Config') -> str:
    """The endpoint to dial: --subtensor.network (a name or a URL), or --subtensor.chain_endpoint when only it is given.

    config.subtensor.chain_endpoint always carries the SDK's finney default, so it is never read unless set.
    """
    if config.is_set('subtensor.chain_endpoint') and not config.is_set('subtensor.network'):
        return config.subtensor.chain_endpoint
    return bt.Subtensor.setup_config(None, config)[0]


def pick_target(subtensor: 'bt.Subtensor', metagraph, netuid: int) -> Optional[int]:
    """The UID that gets the full weight, or None when there is no safe target this pass."""
    owner_hotkey = subtensor.get_subnet_owner_hotkey(netuid)
    uid, reason = select_target(
        list(metagraph.hotkeys), netuid, NETUID_FINNEY, TREASURY_UID, TREASURY_HOTKEY, owner_hotkey
    )
    if uid is None:
        bt.logging.error(f'NO WEIGHTS SET: {reason}')
    elif reason is None:
        bt.logging.info(f'Target: treasury UID {uid} ({TREASURY_HOTKEY})')
    else:
        bt.logging.warning(f'TREASURY FALLBACK: burning to subnet owner UID {uid} ({owner_hotkey}) because {reason}')
    return uid


def dry_run(config: 'bt.Config', connections: Optional[Connections] = None) -> int:
    """Log the weights that would be set; the exit code is 1 when there is no safe target."""
    connections = connections or Connections(primary_endpoint(config), fallbacks=[])
    subtensor, _ = connections.open()
    try:
        netuid = config.netuid
        uid = pick_target(subtensor, subtensor.metagraph(netuid), netuid)
        if uid is None:
            bt.logging.info('DRY RUN: would set no weights. Nothing submitted.')
            return 1
        uids, weights = weight_vector(uid)
        bt.logging.info(
            f'DRY RUN: would set weights uids={uids} weights={weights} on netuid {netuid} '
            f'(version_key={__spec_version__}, chain weights_version='
            f'{subtensor.get_subnet_hyperparameters(netuid).weights_version}). Nothing submitted.'
        )
        return 0
    finally:
        close(subtensor)


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

    metagraph = subtensor.metagraph(netuid)
    target = pick_target(subtensor, metagraph, netuid)
    if target is None:
        return
    uids, weights = weight_vector(target)
    if not submit:
        bt.logging.info(f'Weights NOT submitted (set_weights disabled): would set uids={uids} weights={weights}')
        return
    if not bool(metagraph.validator_permit[my_uid]):
        bt.logging.error(f'UID {my_uid} ({hotkey}) has no validator permit on netuid {netuid}; weights not submitted.')
        return

    # The SDK commits (commit-reveal subnets) or sets directly. One attempt: a rejection is retried next epoch check.
    response = subtensor.set_weights(
        wallet=wallet,
        netuid=netuid,
        uids=uids,
        weights=weights,
        version_key=__spec_version__,
        max_attempts=1,
        mev_protection=False,
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
    connections = Connections(primary_endpoint(config))
    subtensor, on_primary = None, True
    while True:
        try:
            if subtensor is None:
                subtensor, on_primary = connections.open()
            step(subtensor, wallet, config, submit)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            # Any chain error: drop the connection and rebuild it next pass.
            bt.logging.error(f'Validator pass failed ({type(e).__name__}: {redact_text(str(e))}); reconnecting')
            if subtensor is not None and on_primary:
                connections.primary_failed()
            close(subtensor)
            subtensor = None
        if not on_primary:
            # A fallback serves one pass only: the next pass starts from the primary again.
            close(subtensor)
            subtensor, on_primary = None, True
        time.sleep(POLL_INTERVAL_SECONDS)


def main() -> None:
    config = get_config()
    setup_logging(config)
    bt.logging.info(f'Allways validator {__version__} (version_key {__spec_version__}), netuid {config.netuid}')
    if config.dry_run:
        sys.exit(dry_run(config))
    try:
        run(config)
    except KeyboardInterrupt:
        bt.logging.info('Validator stopped.')


if __name__ == '__main__':
    main()

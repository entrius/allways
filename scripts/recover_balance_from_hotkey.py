#!/usr/bin/env python3
"""
recover_balance_from_hotkey.py

Sweep (or transfer) the FREE balance of a Bittensor *hotkey* account back to a
coldkey (or any ss58 destination), signing with the hotkey's own keypair.

Most users should prefer the CLI command, which does the same thing with the
wallet/network already resolved from your config:

    alw collateral recover-from-hotkey

This standalone script is the break-glass fallback for when the bittensor SDK
environment is broken and `alw` won't even import (see the note below) — it
avoids `import bittensor` on purpose.

Why this exists:
    `btcli wallet transfer` always signs with the COLDKEY, so it cannot move funds
    that were accidentally sent to a HOTKEY's ss58 address. This script signs the
    `Balances` transfer with the hotkey keypair, which is the only key that can
    spend that account's free balance.

    NOTE: This is NOT unstaking. To move STAKED tao back to your coldkey use:
        btcli stake remove --wallet-name <NAME> --hotkey <HOTKEY> --network <NET>

This script intentionally avoids `import bittensor` so it works even when the
bittensor SDK's pinned deps are broken; it uses `bittensor_wallet` (keypair) and
`async_substrate_interface` (chain) directly.

Examples:
    # Sweep the entire hotkey free balance to a destination coldkey on finney:
    python recover_balance_from_hotkey.py \\
        --wallet-name <COLDKEY_NAME> --wallet-hotkey <HOTKEY_NAME> \\
        --dest <DEST_SS58> --network finney

    # Transfer a specific amount instead of everything:
    python recover_balance_from_hotkey.py -w <COLDKEY_NAME> -H <HOTKEY_NAME> \\
        -d <DEST_SS58> -a 0.5 --network test

    # Point at a custom chain endpoint and skip the confirmation prompt:
    python recover_balance_from_hotkey.py -w <COLDKEY_NAME> -H <HOTKEY_NAME> \\
        -d <DEST_SS58> --network wss://my.node:443 --yes
"""

import argparse
import sys

from async_substrate_interface.sync_substrate import SubstrateInterface
from bittensor_wallet import Wallet

RAO = 1_000_000_000  # 1 TAO = 1e9 rao

# Friendly network names -> websocket endpoints. Anything starting with ws:// or
# wss:// is passed through unchanged, so custom nodes work too.
NETWORK_ENDPOINTS = {
    'finney': 'wss://entrypoint-finney.opentensor.ai:443',
    'main': 'wss://entrypoint-finney.opentensor.ai:443',
    'test': 'wss://test.finney.opentensor.ai:443',
    'testnet': 'wss://test.finney.opentensor.ai:443',
    'archive': 'wss://archive.chain.opentensor.ai:443',
    'local': 'ws://127.0.0.1:9944',
}


def resolve_endpoint(network: str) -> str:
    if network.startswith(('ws://', 'wss://')):
        return network
    key = network.lower()
    if key not in NETWORK_ENDPOINTS:
        raise SystemExit(
            f"Unknown network '{network}'. Use one of {sorted(NETWORK_ENDPOINTS)} or a full ws(s):// endpoint URL."
        )
    return NETWORK_ENDPOINTS[key]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Transfer a Bittensor hotkey's free balance to a destination ss58.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        '--wallet-name',
        '--name',
        '-w',
        dest='wallet_name',
        required=True,
        help='Name of the wallet (coldkey) that owns the hotkey.',
    )
    p.add_argument(
        '--wallet-hotkey',
        '--hotkey',
        '-H',
        dest='wallet_hotkey',
        required=True,
        help='Name of the hotkey whose free balance you want to move.',
    )
    p.add_argument(
        '--wallet-path',
        '-p',
        dest='wallet_path',
        default=None,
        help='Path to the wallets directory (default: ~/.bittensor/wallets).',
    )
    p.add_argument(
        '--dest',
        '--destination',
        '-d',
        dest='dest',
        required=True,
        help='Destination ss58 address (usually your coldkey).',
    )
    p.add_argument(
        '--network',
        '--chain',
        dest='network',
        default='finney',
        help='Network name (finney/test/local/archive) or a full ws(s):// URL.',
    )
    p.add_argument(
        '--amount',
        '-a',
        dest='amount',
        type=float,
        default=None,
        help='Amount in TAO to transfer. Omit (or use --all) to sweep everything.',
    )
    p.add_argument(
        '--all',
        dest='sweep_all',
        action='store_true',
        help='Transfer the entire free balance (default when --amount is omitted).',
    )
    p.add_argument(
        '--keep-alive',
        dest='keep_alive',
        action='store_true',
        help='Keep the source account above the existential deposit '
        '(only relevant with --amount; a full sweep always allows death).',
    )
    p.add_argument('--yes', '-y', dest='yes', action='store_true', help='Skip the confirmation prompt.')
    return p.parse_args()


def confirm(prompt: str) -> bool:
    try:
        return input(f'{prompt} [y/N]: ').strip().lower() in ('y', 'yes')
    except (EOFError, KeyboardInterrupt):
        return False


def main() -> int:
    args = parse_args()

    wallet_kwargs = {'name': args.wallet_name, 'hotkey': args.wallet_hotkey}
    if args.wallet_path:
        wallet_kwargs['path'] = args.wallet_path
    wallet = Wallet(**wallet_kwargs)

    keypair = wallet.hotkey  # loads the hotkey keypair (prompts if encrypted)
    src = keypair.ss58_address

    endpoint = resolve_endpoint(args.network)
    sub = SubstrateInterface(url=endpoint)
    try:
        free_rao = int(sub.query('System', 'Account', [src]).value['data']['free'])
        print(f'Network       : {args.network}  ({endpoint})')
        print(f'Source hotkey : {src}')
        print(f'Free balance  : {free_rao / RAO:.9f} TAO ({free_rao} rao)')
        print(f'Destination   : {args.dest}')

        if free_rao <= 0:
            print('Nothing to transfer. Exiting.')
            return 0

        sweep = args.sweep_all or args.amount is None
        if sweep:
            action = 'Sweep ENTIRE free balance (minus fee)'
            call = sub.compose_call(
                call_module='Balances',
                call_function='transfer_all',
                call_params={'dest': args.dest, 'keep_alive': False},
            )
        else:
            amount_rao = int(round(args.amount * RAO))
            if amount_rao <= 0:
                print('Amount must be greater than 0.')
                return 1
            if amount_rao > free_rao:
                print(f'Amount {args.amount} TAO exceeds free balance {free_rao / RAO:.9f} TAO.')
                return 1
            fn = 'transfer_keep_alive' if args.keep_alive else 'transfer_allow_death'
            action = f'Transfer {args.amount} TAO ({fn})'
            call = sub.compose_call(
                call_module='Balances',
                call_function=fn,
                call_params={'dest': args.dest, 'value': amount_rao},
            )

        print(f'Action        : {action}')
        if not args.yes and not confirm('Proceed?'):
            print('Aborted.')
            return 1

        ext = sub.create_signed_extrinsic(call=call, keypair=keypair)
        print('Submitting ...')
        receipt = sub.submit_extrinsic(ext, wait_for_inclusion=True)

        if receipt.is_success:
            print(f'✅ Success. Block hash: {receipt.block_hash}')
            return 0
        print(f'❌ Failed: {receipt.error_message}')
        return 1
    finally:
        # Close the websocket so the process exits instead of hanging on a
        # lingering background connection thread.
        sub.close()


if __name__ == '__main__':
    sys.exit(main())

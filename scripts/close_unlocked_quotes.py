#!/usr/bin/env python3
"""List, and with --send reap, quotes whose wallets aren't both locked to their miner.

Run once after the program upgrade that adds wallet locks: quotes posted before it carry no locks, so
they are closed (rent back to each miner) and miners re-post. A re-posted quote locks its wallets, so
this finds nothing afterwards. `close_unlocked_quote` is permissionless; the sender only pays tx fees.

Usage: python scripts/close_unlocked_quotes.py [--rpc-url URL] [--program-id ID] [--send]
"""

import argparse
import sys

from solders.pubkey import Pubkey

from allways.cli.swap_commands.helpers import get_effective_config, load_cli_keypair, resolve_solana_rpc
from allways.solana.client import AllwaysSolanaClient
from allways.solana.program import resolve_program_id


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--rpc-url', help='Solana RPC (default: the alw config)')
    parser.add_argument('--program-id', help='program id (default: the alw config)')
    parser.add_argument('--send', action='store_true', help='close them (default: list only)')
    args = parser.parse_args()

    config = get_effective_config()
    keypair = load_cli_keypair(config) if args.send else None
    client = AllwaysSolanaClient(
        args.rpc_url or resolve_solana_rpc(config),
        program_id=Pubkey.from_string(args.program_id) if args.program_id else resolve_program_id(config),
        keypair=keypair,
    )

    unlocked = client.unlocked_quotes()
    print(f'unlocked quotes {len(unlocked)}')
    failed = 0
    for pda, q in unlocked:
        line = f'  {pda}  miner={q.miner}  {q.from_chain}->{q.to_chain} ({q.collateral_chain})'
        if not args.send:
            print(line)
            continue
        try:
            print(f'{line}  closed {client.close_unlocked_quote(pda, q)}')
        except Exception as e:
            failed += 1
            print(f'{line}  FAILED {e}')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())

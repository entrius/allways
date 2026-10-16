#!/usr/bin/env python3
"""Read-only preflight for the program upgrade that re-keys the source lock.

A reservation filled before the upgrade holds a lock at the OLD address. After the upgrade,
`extend_reservation` and `vote_initiate` derive the NEW address, so such a reservation can no longer
be extended or attested. Upgrade only when the program is halted and none are left:

  - live reservations (reserved_until >= now), claimed or not
  - swaps still PendingAttestation (their attest vote releases the lock)

Usage: python scripts/source_lock_upgrade_check.py [--rpc-url URL] [--program-id ID]
Exits 0 when it is safe to upgrade, 1 otherwise. Sends nothing.
"""

import argparse
import sys
import time

from solders.pubkey import Pubkey

from allways.cli.swap_commands.helpers import get_effective_config, resolve_solana_rpc
from allways.solana.client import AllwaysSolanaClient
from allways.solana.program import resolve_program_id


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--rpc-url', help='Solana RPC (default: the alw config)')
    parser.add_argument('--program-id', help='program id (default: the alw config)')
    args = parser.parse_args()

    config = get_effective_config()
    rpc_url = args.rpc_url or resolve_solana_rpc(config)
    program_id = Pubkey.from_string(args.program_id) if args.program_id else resolve_program_id(config)
    client = AllwaysSolanaClient(rpc_url, program_id=program_id, keypair=None)

    now = int(time.time())
    halted = bool(client.get_config().halted)
    live = [
        (pda, r)
        for pda, r in client.get_all('Reservation')
        if int(r.reserved_until) != 0 and int(r.reserved_until) >= now
    ]
    pending = client.get_swaps('PendingAttestation')

    print(f'program        {program_id}')
    print(f'halted         {halted}')
    print(f'live reservations   {len(live)}')
    for pda, r in live:
        print(
            f'  {pda}  {r.from_chain}->{r.to_chain}  reserved_until={r.reserved_until} (+{int(r.reserved_until) - now}s)'
        )
    print(f'pending attestation {len(pending)}')
    for pda, s in pending:
        print(f'  {pda}  {s.from_chain}->{s.to_chain}  from_tx={s.from_tx_hash[:16]}…')

    ok = halted and not live and not pending
    print('SAFE TO UPGRADE' if ok else 'NOT SAFE: halt first and wait for the lists above to drain')
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python3
"""v10/v13/v14 → v17 migration driver (run AFTER `solana program deploy` upgrades the program).

Signs with the Config admin (= dev-asm on devnet, which is also the upgrade authority). Order matters:
migrate_config MUST run first (an unmigrated v10 Config can't deserialize under the new layout, so
migrate_miner_state — which loads Account<Config> — is unrunnable until then). All cranks are idempotent.
Legacy Pool/Reservation are closed so the next open_or_request recreates them fresh under v14.

Drain gate: v3 changed the `Swap` byte layout, so a Swap that outlives the upgrade is undecodable by the
new program (neither timeout nor confirm can close it). The upgrade must land with ZERO live Swaps — halt
the program and drain every swap first. This driver refuses to migrate while any Swap PDA remains.

v17 seeds each miner's on-chain fee counters as it grows the account: SEEDS names a JSON file of
{miner pubkey: {backing: cumulative fees owed under the current vault generation}}, summed from the
SwapCompleted history (a miner it leaves out starts at 0). The seed is read only by the crank that
grows the account, so get it right before the first run. With no live Swap required for v14 → v17,
the drain gate applies only to the older layouts.

  cd smart-contracts/solana
  RPC=https://api.devnet.solana.com ADMIN=~/.config/solana/dev-asm.json SEEDS=fee_seeds.json \
    uv --project ../.. run python migrate_devnet.py [--dry-run]
"""

import hashlib
import json
import os
import sys
from pathlib import Path

from solders.instruction import AccountMeta
from solders.keypair import Keypair
from solders.pubkey import Pubkey

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from allways.solana import pdas  # noqa: E402
from allways.solana.client import AllwaysSolanaClient  # noqa: E402
from allways.solana.layouts import CONFIG_VERSION, IX_MIGRATE_MINER_ARGS, MAX_BACKING_SLOTS  # noqa: E402

SYSTEM = Pubkey.from_string('11111111111111111111111111111111')


def disc(name):
    return hashlib.sha256(f'account:{name}'.encode()).digest()[:8]


def seed_slots(per_backing):
    """{backing: amount} -> the per-hub array the crank takes, indexed by backing-bit position."""
    slots = [0] * MAX_BACKING_SLOTS
    for backing, amount in per_backing.items():
        slots[pdas.BACKING_BITS[backing].bit_length() - 1] = int(amount)
    return slots


def main():
    dry = '--dry-run' in sys.argv
    rpc = os.environ.get('RPC', 'https://api.devnet.solana.com')
    admin_path = os.path.expanduser(os.environ.get('ADMIN', '~/.config/solana/dev-asm.json'))
    admin = Keypair.from_bytes(bytes(json.load(open(admin_path))))
    c = AllwaysSolanaClient(rpc, keypair=admin)
    print(f'RPC {rpc} | admin {admin.pubkey()} | program {c.program_id}{" | DRY-RUN" if dry else ""}')

    cfg_pda = pdas.config_pda(c.program_id)
    accts = c.rpc.get_program_accounts(c.program_id)
    miners = [Pubkey(bytes(d[8:40])) for _, d in accts if bytes(d[:8]) == disc('MinerState')]
    addrset = {a for a, _ in accts}
    print(f'miners: {len(miners)}')
    seeds_path = os.environ.get('SEEDS')
    seeds = json.load(open(seeds_path)) if seeds_path else {}
    print(
        f'fee seeds: {len(seeds)} miner(s){f" from {seeds_path}" if seeds_path else " (none: every counter starts at 0)"}'
    )

    # Drain gate — no upgrade may migrate state while a live Swap exists: v3 changed the Swap layout,
    # so a survivor is undecodable and its collateral strands. Abort (dry-run included) until it's 0.
    # v14 → v17 moves no field, so a live Swap survives it; only an older layout needs the drain.
    v14_len = 8 + 253  # the v14-v16 MinerState allocation (state.rs before the v17 counters)
    legacy = [a for a, d in accts if bytes(d[:8]) == disc('MinerState') and len(d) < v14_len]
    live_swaps = [a for a, d in accts if bytes(d[:8]) == disc('Swap')] if legacy else []
    if live_swaps:
        print(f'  ABORT: {len(live_swaps)} live Swap PDA(s) remain — halt and drain before upgrading.')
        for a in live_swaps:
            print(f'    swap {a}')
        return
    print('swap drain gate: 0 live Swaps — clear to migrate.')

    def run(label, fn):
        if dry:
            print(f'  [dry] would {label}')
            return
        try:
            sig = fn()
            print(f'  ok  {label}  ({str(sig)[:16]}…)')
        except Exception as e:
            print(f'  ERR {label}: {e}')

    # 1. migrate_config (admin signer, config unchecked+mut, system) — MUST be first.
    metas = [
        AccountMeta(admin.pubkey(), True, True),
        AccountMeta(cfg_pda, False, True),
        AccountMeta(SYSTEM, False, False),
    ]
    run('migrate_config', lambda: c._send([c._ix('migrate_config', b'', metas)]))

    if not dry:
        v = c.get_config().version
        print(f'  Config.version now: {v}')
        if v != CONFIG_VERSION:
            print(f'  ABORT: config did not reach v{CONFIG_VERSION}; not migrating miners.')
            return

    # 2. migrate_miner_state per miner, seeding its fee counters.
    for m in miners:
        ms = pdas.miner_state_pda(m, c.program_id)
        slots = seed_slots(seeds.get(str(m), {}))
        args = IX_MIGRATE_MINER_ARGS.build({'accrued_fees': slots})
        metas = [
            AccountMeta(admin.pubkey(), True, True),
            AccountMeta(cfg_pda, False, False),
            AccountMeta(m, False, False),
            AccountMeta(ms, False, True),
            AccountMeta(SYSTEM, False, False),
        ]
        run(
            f'migrate_miner_state {str(m)[:8]} seed={slots}',
            lambda mm=metas, a=args: c._send([c._ix('migrate_miner_state', a, mm)]),
        )

    # 3. Reap what the v3.1 re-seeding orphaned. Pool/Reservation moved to [SEED, miner, backing] and
    # the initiate round to [vote, REQ_INITIATE, swap_key], so the old addresses are unreachable rather
    # than mis-decoded — rent to reclaim, not a lockout. Derive the RETIRED address, never the live one.
    for m in miners:
        if str(pdas.legacy_pool_pda(m, c.program_id)) in addrset:
            run(f'close_legacy_pool {str(m)[:8]}', lambda mm=m: c.close_legacy_pool(mm))
        if str(pdas.legacy_reservation_pda(m, c.program_id)) in addrset:
            run(f'close_legacy_reservation {str(m)[:8]}', lambda mm=m: c.close_legacy_reservation(mm))
        if str(pdas.legacy_initiate_round_pda(m, c.program_id)) in addrset:
            run(f'close_legacy_initiate_round {str(m)[:8]}', lambda mm=m: c.close_legacy_initiate_round(mm))

    # NOTE (mainnet): orphaned pre-v3 MinerQuotes share the live discriminator but not its layout, so a
    # miner's startup get_all('MinerQuote') throws and it falls back to requiring every spoke's key. This
    # devnet set has none (all 6 resolve at current seeds); a mainnet run MUST decode miner/from/to from
    # the raw bytes and close_legacy_quote each before miners restart.
    print('done.')


if __name__ == '__main__':
    main()

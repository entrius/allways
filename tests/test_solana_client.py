"""B0 solana_client tests.

Unit tier (no chain): borsh layout round-trips + an independent discriminator check (Anchor's
sha256("account:<Name>")[:8] formula) + PDA derivation sanity. The integration tier (round-trip vs a
local solana-test-validator) is gated behind @pytest.mark.integration.
"""

import hashlib

from solders.pubkey import Pubkey

from allways.solana import layouts, pdas


def _disc(name: str) -> bytes:
    return hashlib.sha256(f'account:{name}'.encode()).digest()[:8]


def test_discriminators_match_anchor_formula():
    # Independent of the hardcoded values: recompute Anchor's account discriminator and compare.
    for name, hardcoded in layouts.DISCRIMINATORS.items():
        assert hardcoded == _disc(name), f'{name} discriminator mismatch vs sha256(account:{name})'
    # every readable account has a layout
    assert set(layouts.DISCRIMINATORS) == set(layouts.ACCOUNT_LAYOUTS)


def _roundtrip(layout, value: dict):
    assert layout.parse(layout.build(value)) is not None
    return layout.parse(layout.build(value))


def test_minerstate_roundtrip():
    v = {
        'miner': bytes(range(32)),
        'collateral': 5_000_000_000,
        'active': True,
        'active_backings': 1,
        'has_active_swap': False,
        'busy_until': 1_700_000_500,
        'settling_until': 0,
        'deactivation_at': 0,
        'successful_swaps': 3,
        'failed_swaps': 1,
        'bump': 254,
    }
    p = _roundtrip(layouts.MinerState, v)
    assert p.collateral == v['collateral'] and p.active is True and p.successful_swaps == 3
    assert p.active_backings == 1 and p.settling_until == 0


def test_reservation_roundtrip_u128_rate_and_strings():
    v = {
        'router': bytes(range(32)),
        'from_addr': 'userBTCaddr',
        'user': bytes(range(32)),
        'user_to_addr': 'userSOLaddr',
        'from_chain': 'BTC',
        'to_chain': 'SOL',
        'collateral_chain': 'sol',
        'collateral_amount': 2_000_000_000,
        'from_amount': 100_000,
        'to_amount': 7,
        'miner_from_addr': 'minerBTCaddr',
        'miner_to_addr': 'minerSOLaddr',
        'rate': 1_500_000_000_000_000_000,  # 1.5 * RATE_PRECISION
        'created_at': 1_700_000_000,
        'reserved_until': 1_700_001_800,
        'finalize_by': 1_700_000_060,
        'max_extend_at': 1_700_005_400,
        'claimed_swap_key': bytes(32),
        'bump': 255,
    }
    p = _roundtrip(layouts.Reservation, v)
    assert p.rate == v['rate'] and p.from_chain == 'BTC' and p.created_at == 1_700_000_000
    assert p.finalize_by == 1_700_000_060 and p.collateral_amount == 2_000_000_000


def test_swap_roundtrip_enum_status():
    v = {
        'user': bytes(32),
        'miner': bytes(range(32)),
        'from_chain': 'BTC',
        'to_chain': 'SOL',
        'user_from_addr': 'a',
        'user_to_addr': 'b',
        'miner_from_addr': 'c',
        'miner_to_addr': 'd',
        'rate': 1_500_000_000_000_000_000,
        'collateral_chain': 'sol',
        'collateral_amount': 1,
        'from_amount': 2,
        'to_amount': 3,
        'from_tx_hash': 'deadbeef',
        'from_tx_block': 800_000,
        'to_tx_hash': '',
        'to_tx_block': 0,
        'status': layouts.SwapStatus.enum.PendingAttestation(),
        'initiated_at': 0,
        'timeout_at': 0,
        'max_extend_at': 0,
        'fulfilled_at': 0,
        'bump': 1,
    }
    p = _roundtrip(layouts.Swap, v)
    assert p.from_tx_hash == 'deadbeef' and type(p.status).__name__ == 'PendingAttestation'


def test_pool_roundtrip_vec_of_request():
    req = {'router': bytes(range(32))}
    v = {
        'miner': bytes(32),
        'from_chain': 'BTC',
        'to_chain': 'SOL',
        'miner_from_addr': 'm1',
        'miner_to_addr': 'm2',
        'rate': 1_000_000_000_000_000_000,
        'opened_at': 1,
        'closes_at': 2,
        'seed_slot': 1234,
        'requests': [req, req],
        'bump': 1,
    }
    p = _roundtrip(layouts.Pool, v)
    assert len(p.requests) == 2 and bytes(p.requests[0].router) == bytes(range(32))


def test_config_roundtrip_vec_of_validatorinfo():
    v = {
        'admin': bytes(32),
        'version': 12,
        'min_collateral': 1,
        'max_collateral': 2,
        'fulfillment_timeout_secs': 100,
        'min_swap_amount': 0,
        'max_swap_amount': 0,
        'tao_min_swap_amount': 1_000,
        'tao_max_swap_amount': 0,
        'tao_min_collateral': 100_000_000,
        'settlement_grace_secs': 900,
        'last_attest_heartbeat': 0,
        'attest_max_age_secs': 86_400,
        'reservation_ttl_secs': 1800,
        'consensus_threshold_percent': 66,
        'validators': [{'key': bytes(range(32)), 'weight': 7}],
        'last_weights_update': 0,
        'halted': False,
        'reservation_fee_lamports': 20_000_000,
        'pool_window_secs': 60,
        'finalize_window_secs': 60,
        'weights_update_min_interval_secs': 0,
        'max_total_extension_secs': 5400,
        'bump': 1,
    }
    p = _roundtrip(layouts.Config, v)
    assert p.version == 12 and len(p.validators) == 1 and p.validators[0].weight == 7


def test_bond_attestation_roundtrip():
    v = {
        'miner': bytes(range(32)),
        'chain': 'tao',
        'effective_balance': 3_300_000_000,
        'locked': True,
        'epoch': 4,
        'attested_at': 1_700_000_000,
        'bump': 253,
    }
    p = _roundtrip(layouts.BondAttestation, v)
    assert p.chain == 'tao' and p.locked is True and p.epoch == 4
    assert p.effective_balance == v['effective_balance']


def test_pda_derivation():
    miner = Pubkey.default()
    assert isinstance(pdas.config_pda(), Pubkey)
    assert isinstance(pdas.miner_state_pda(miner), Pubkey)
    assert isinstance(pdas.quote_pda(miner, 'BTC', 'SOL'), Pubkey)
    assert isinstance(pdas.vote_round_pda(pdas.REQ_INITIATE, miner), Pubkey)
    # global weights round (no target)
    assert isinstance(pdas.vote_round_pda(pdas.REQ_SET_WEIGHTS), Pubkey)
    # W2: attestation PDAs are per (miner, backing chain) — the chain is part of the seed, so two
    # backings never collide, and the round shares the same composite target.
    assert pdas.bond_attestation_pda(miner, 'tao') != pdas.bond_attestation_pda(miner, 'eth')
    assert isinstance(pdas.attestation_round_pda(miner, 'tao'), Pubkey)

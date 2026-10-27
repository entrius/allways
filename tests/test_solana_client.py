"""solana_client tests.

Unit tier (no chain): borsh layout round-trips + an independent discriminator check (Anchor's
sha256("account:<Name>")[:8] formula) + swap PDA derivation. The integration tier (round-trip vs a
local solana-test-validator) is gated behind @pytest.mark.integration.
"""

import hashlib

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
        'hotkey': bytes(range(32)),
    }
    p = _roundtrip(layouts.Swap, v)
    assert p.from_tx_hash == 'deadbeef' and type(p.status).__name__ == 'PendingAttestation'
    assert bytes(p.hotkey) == bytes(range(32))  # the hotkey stays raw bytes


def test_swap_pda_accepts_hex_string_forms():
    # The swap_key circulates as bytes on-chain but as a hex string in the CLI, API, and logs
    # — every surface form must derive the same PDA, so a pasted key just works.
    key = bytes(range(32))
    from_bytes = pdas.swap_pda(key)
    assert pdas.swap_pda(key.hex()) == from_bytes
    assert pdas.swap_pda('0x' + key.hex()) == from_bytes

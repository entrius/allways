"""A1 — decode_transfer is gated to Balances.{transfer_allow_death, transfer_keep_alive}.

The old decode substring-matched any ``*transfer*`` call with no module check, and the raw
SCALE path listed call index 7 (transfer_all), whose ``keep_alive`` bool parsed as the amount.
transfer_all's true amount lives only in the Balances.Transfer event, so decoding it from the
extrinsic must yield None in BOTH paths — never an amount the verifier could approve.
Backends are mocked — no network.
"""

from unittest.mock import MagicMock

from allways.chain_providers.subtensor import SubtensorProvider


def _structured_ext(call_module, call_function, args, tx_hash='0xhash', sender='userTAO'):
    return {
        'extrinsic_hash': tx_hash,
        'address': sender,
        'call': {
            'call_module': call_module,
            'call_function': call_function,
            'call_args': [{'name': k, 'value': v} for k, v in args.items()],
        },
    }


def _transfer_args(dest='minerTAO', amount=5_000):
    return {'dest': {'Id': dest}, 'value': amount}


# ─── Structured path ────────────────────────────────────────────────────────


def test_decodes_balances_transfer_keep_alive():
    ext = _structured_ext('Balances', 'transfer_keep_alive', _transfer_args())
    assert SubtensorProvider.decode_transfer(ext, False) == ('0xhash', 'minerTAO', 5_000, 'userTAO')


def test_decodes_balances_transfer_allow_death():
    ext = _structured_ext('Balances', 'transfer_allow_death', _transfer_args())
    assert SubtensorProvider.decode_transfer(ext, False) == ('0xhash', 'minerTAO', 5_000, 'userTAO')


def test_transfer_all_never_decodes():
    ext = _structured_ext('Balances', 'transfer_all', {'dest': {'Id': 'minerTAO'}, 'keep_alive': True})
    assert SubtensorProvider.decode_transfer(ext, False) is None


def test_wrong_module_transfer_never_decodes():
    ext = _structured_ext('EvilPallet', 'transfer_keep_alive', _transfer_args())
    assert SubtensorProvider.decode_transfer(ext, False) is None


def test_transfer_stake_rejected_by_decode_transfer():
    ext = _structured_ext('SubtensorModule', 'transfer_stake', {'destination_coldkey': 'ourCold'})
    assert SubtensorProvider.decode_transfer(ext, False) is None


# ─── Exploit pin: transfer_all must never satisfy the verifier ──────────────


def test_transfer_all_never_approves_fat_expectation():
    ext = _structured_ext('Balances', 'transfer_all', {'dest': {'Id': 'minerTAO'}, 'keep_alive': False})
    p = SubtensorProvider.__new__(SubtensorProvider)  # skip __init__ (no real subtensor needed)
    p.subtensor = MagicMock()
    p.subtensor.get_current_block.return_value = 100
    p.block_cache = {}
    p.get_block = lambda n: {'extrinsics': [ext]}
    assert p.fetch_matching_tx('0xhash', 'minerTAO', 1_500_000_000, block_hint=100) is None


# ─── decode_stake_transfer ──────────────────────────────────────────────────


def test_decode_stake_transfer_extracts_all_args():
    ext = _structured_ext(
        'SubtensorModule',
        'transfer_stake',
        {
            'destination_coldkey': {'Id': 'ourCold'},
            'hotkey': 'ourHot',
            'origin_netuid': 7,
            'destination_netuid': 7,
            'alpha_amount': 123_456,
        },
    )
    assert SubtensorProvider.decode_stake_transfer(ext) == {
        'tx_hash': '0xhash',
        'sender': 'userTAO',
        'destination_coldkey': 'ourCold',
        'hotkey': 'ourHot',
        'origin_netuid': 7,
        'destination_netuid': 7,
        'alpha_amount': 123_456,
    }


def test_decode_stake_transfer_rejects_other_calls():
    assert (
        SubtensorProvider.decode_stake_transfer(_structured_ext('Balances', 'transfer_keep_alive', _transfer_args()))
        is None
    )
    assert SubtensorProvider.decode_stake_transfer(_structured_ext('SubtensorModule', 'add_stake', {})) is None
    assert SubtensorProvider.decode_stake_transfer({'no': 'hash'}) is None


# ─── Raw SCALE path ─────────────────────────────────────────────────────────


def _raw_transfer_hex(call_idx, amount=5_000):
    body = bytes([0x84]) + bytes([1] * 32)  # signed flag + sender AccountId
    body += bytes([SubtensorProvider._BALANCES_PALLET, call_idx, 0x00]) + bytes([2] * 32)  # call + dest
    body += ((amount << 2) | 0x01).to_bytes(2, 'little')  # compact amount (mode 1)
    return '0x00' + body.hex()  # compact length prefix (value unused by the parser)


def test_raw_path_parses_transfer_keep_alive():
    parsed = SubtensorProvider.parse_raw_extrinsic(_raw_transfer_hex(call_idx=3))
    assert parsed is not None
    assert parsed['call_function'] == 'transfer_keep_alive'
    assert parsed['amount'] == 5_000


def test_raw_path_rejects_transfer_all_index():
    assert SubtensorProvider.parse_raw_extrinsic(_raw_transfer_hex(call_idx=7)) is None

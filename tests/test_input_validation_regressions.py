"""Regression tests for previously-fixed input-validation bugs (from the merged-PR bug scan).

- SS58 checksum validation (PR #312): a base58-shaped but checksum-corrupted address must be rejected,
  not just shape-matched — else TAO could be sent to an address that decodes to nothing.
- Empty/whitespace source-tx-hash rejection (PR #167): intake must reject a blank hash before any chain
  lookup. The original CLI guard was lost when reserve→initiate moved on-chain; the surviving guard is
  in reserve_engine.confirm_deposit, which now strips + rejects whitespace again.
"""

import bittensor as bt

from allways.chain_providers.subtensor import SubtensorProvider
from allways.validator.reserve_engine import confirm_deposit


def test_is_valid_address_verifies_checksum_not_just_shape():
    # PR #312: is_valid_address must verify the SS58 checksum. A genuinely-valid key passes; a 48-char
    # base58 string with a corrupted payload/checksum fails. Generate the valid address so the test
    # stays correct across bittensor SS58-format changes.
    p = SubtensorProvider.__new__(SubtensorProvider)  # no live subtensor needed for pure validation
    valid = bt.Keypair.create_from_uri('//RegressionTest').ss58_address
    assert p.is_valid_address(valid) is True, 'a genuinely valid SS58 must pass'

    corrupted = valid[:20] + ('A' if valid[20] != 'A' else 'B') + valid[21:]
    assert p.is_valid_address(corrupted) is False, 'checksum-corrupted address must be rejected'
    assert p.is_valid_address('1' * 48) is False, 'base58-shaped but invalid payload must be rejected'
    assert p.is_valid_address('too-short') is False, 'wrong length must be rejected'
    assert p.is_valid_address(None) is False, 'non-string must be rejected'


def test_confirm_deposit_rejects_empty_and_whitespace_hash():
    # PR #167: an empty or whitespace-only source tx hash must be rejected at intake, before any chain
    # lookup or client access (validator is never dereferenced on this path). A real tx hash is never
    # blank, so stripping-then-rejecting is strictly correct.
    for bad in ('', '   ', '\t\n'):
        res = confirm_deposit(None, 'somehotkey', bad)
        assert res.ok is False, f'blank hash {bad!r} must be rejected'
        assert 'Missing source tx hash' in res.reason

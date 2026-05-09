"""Tests for the centralized validator rejection translator.

Locks down the prefix→message mapping and the aggregation behavior so that
adding a new rejection cause on the validator side is caught here as a
diff to the rules table rather than as a regression in CLI UX.
"""

from dataclasses import dataclass
from typing import Optional

from rich.console import Console

from allways.cli.validator_rejections import RejectionInfo, render_and_aggregate


@dataclass
class FakeResp:
    accepted: Optional[bool] = None
    rejection_reason: Optional[str] = None


def _silent_console() -> Console:
    # record=True + file=open(os.devnull) would also work, but Rich's default
    # constructor is fine — we just don't assert against printed output here.
    return Console(quiet=True)


def test_all_accepted_no_headline():
    info = render_and_aggregate(_silent_console(), [FakeResp(accepted=True), FakeResp(accepted=True)])
    assert info.accepted == 2
    assert info.rejected == 0
    assert info.headline == ''
    assert info.deterministic is False


def test_all_queued_counted_separately():
    responses = [
        FakeResp(accepted=True, rejection_reason='Queued — 1/6 confirmations.'),
        FakeResp(accepted=True, rejection_reason='Queued — 2/6 confirmations.'),
    ]
    info = render_and_aggregate(_silent_console(), responses)
    assert info.accepted == 2
    assert info.queued == 2


def test_insufficient_source_balance_translates():
    responses = [FakeResp(accepted=False, rejection_reason='Insufficient source balance')]
    info = render_and_aggregate(
        _silent_console(),
        responses,
        context={
            'from_chain_upper': 'BTC',
            'from_address': 'tb1qabc',
            'from_amount_human': '0.0008',
        },
    )
    assert info.category == 'insufficient_source_balance'
    assert info.deterministic is True
    assert 'tb1qabc' in info.headline
    assert 'BTC' in info.headline
    assert '0.0008' in info.headline


def test_address_cooldown_includes_raw_reason():
    raw = 'Address on cooldown: ~50 blocks remaining (strike 2, 300-block window)'
    info = render_and_aggregate(
        _silent_console(),
        [FakeResp(accepted=False, rejection_reason=raw)],
        context={'from_address': 'tb1qabc'},
    )
    assert info.category == 'address_cooldown'
    assert info.deterministic is True
    assert '50 blocks remaining' in info.headline
    assert 'strike 2' in info.headline
    assert 'doubles' in info.headline
    # No double-wrapped 'Address on cooldown: Address on cooldown'
    assert info.headline.lower().count('address on cooldown') == 0


def test_miner_busy_is_not_deterministic():
    info = render_and_aggregate(
        _silent_console(),
        [FakeResp(accepted=False, rejection_reason='Miner has an active swap')],
        context={'miner_uid': 4},
    )
    assert info.category == 'miner_busy'
    assert info.deterministic is False
    assert 'UID 4' in info.headline


def test_mixed_reasons_no_headline():
    responses = [
        FakeResp(accepted=False, rejection_reason='Insufficient source balance'),
        FakeResp(accepted=False, rejection_reason='Miner already reserved'),
    ]
    info = render_and_aggregate(_silent_console(), responses, context={'miner_uid': 1})
    assert info.category == 'mixed'
    assert info.headline == ''
    assert info.deterministic is False  # mixed → let the user retry if they want


def test_no_response_only():
    responses = [FakeResp(accepted=False, rejection_reason=''), FakeResp(accepted=False, rejection_reason=None)]
    info = render_and_aggregate(_silent_console(), responses)
    assert info.category == 'no_response_only'
    assert info.no_response == 2
    assert info.deterministic is False
    assert 'no validators responded' in info.headline.lower()


def test_unmatched_falls_back_to_raw():
    info = render_and_aggregate(
        _silent_console(),
        [FakeResp(accepted=False, rejection_reason='Some brand-new validator error')],
    )
    assert info.category == 'unmatched'
    assert info.headline == 'Some brand-new validator error'
    assert info.deterministic is False  # unknown → safer to allow retry


def test_duplicate_source_tx_translates_with_deterministic_flag():
    raw = 'vote_initiate: DuplicateSourceTx — Source transaction hash already used in another swap'
    info = render_and_aggregate(_silent_console(), [FakeResp(accepted=False, rejection_reason=raw)])
    assert info.category == 'duplicate_source_tx'
    assert info.deterministic is True
    assert 'already used' in info.headline.lower()
    assert 'alw swap' in info.headline


def test_swap_confirm_tx_not_found_is_transient():
    info = render_and_aggregate(
        _silent_console(),
        [FakeResp(accepted=False, rejection_reason='Source transaction not found, amount or sender mismatch')],
    )
    assert info.category == 'tx_not_found'
    assert info.deterministic is False


def test_miner_activate_unregistered():
    info = render_and_aggregate(
        _silent_console(),
        [FakeResp(accepted=False, rejection_reason='Hotkey not registered on subnet')],
    )
    assert info.category == 'miner_not_registered'
    assert info.deterministic is True
    assert 'btcli subnets register' in info.headline


def test_returns_rejectioninfo_dataclass():
    info = render_and_aggregate(_silent_console(), [])
    assert isinstance(info, RejectionInfo)

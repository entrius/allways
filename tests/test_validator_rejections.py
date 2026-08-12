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


def test_queued_not_yet_visible_counted_as_queued():
    """Upgraded validators queue when the source tx isn't visible yet
    (propagation lag) instead of rejecting — the CLI should treat the
    new wording the same way it treats `Queued — N/M confirmations`."""
    responses = [
        FakeResp(
            accepted=True,
            rejection_reason=(
                'Queued — source tx not yet visible to validators (propagation lag '
                'or pending re-verify). Validator will auto-initiate once seen and confirmed.'
            ),
        ),
    ]
    info = render_and_aggregate(_silent_console(), responses)
    assert info.accepted == 1
    assert info.queued == 1
    assert info.rejected == 0


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


def _rate_limited_resp() -> FakeResp:
    # A 429 from the edge proxy: no synapse rejection_reason (the validator never
    # ran), but the dendrite records status_code 429 from the JSON error body.
    from types import SimpleNamespace

    resp = FakeResp(accepted=False, rejection_reason='')
    resp.dendrite = SimpleNamespace(status_code='429')
    return resp


def test_rate_limited_429_is_distinct_from_no_response():
    info = render_and_aggregate(_silent_console(), [_rate_limited_resp(), _rate_limited_resp()])
    assert info.rate_limited == 2
    assert info.no_response == 0
    assert info.category == 'rate_limited'
    assert info.deterministic is False
    assert 'rate limited' in info.headline.lower()


def test_rate_limited_mixed_with_rejection_does_not_claim_pure_headline():
    # 429 + a real rejection: must not be labeled the pure rate_limited category
    # (the auto-backoff retry keys off that), but stays transient so a retry is allowed.
    responses = [_rate_limited_resp(), FakeResp(accepted=False, rejection_reason='miner busy')]
    info = render_and_aggregate(_silent_console(), responses, context={'miner_uid': 1})
    assert info.rate_limited == 1
    assert info.category != 'rate_limited'
    assert info.deterministic is False


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


# ---- per-purse activation (W3.2) ----------------------------------------------------------


def _reason(text: str) -> RejectionInfo:
    return render_and_aggregate(_silent_console(), [FakeResp(accepted=False, rejection_reason=text)])


def test_already_active_names_the_purse_not_the_miner():
    # Purses activate separately, so "your miner is already active" would be a lie to a miner
    # asking for its second one. The rule follows the per-purse reason the validator now sends.
    info = _reason('Purse already active: TAO')
    assert info.category == 'already_active'
    assert info.deterministic is True and 'purse' in info.headline.lower()


def test_every_bond_refusal_lands_on_one_rule_and_stays_retryable():
    # These clear on their own once the relay catches up, so none of them may read as terminal.
    for reason in (
        'Bond attestation for your TAO purse is missing. Validators mirror the vault on their own cadence.',
        'Bond attestation is stale — it asserts lock epoch 3, the TAO vault is at 4.',
        'Bond attestation is fused off — the TAO heartbeat is 90000s old (max 86400s).',
        'Bond is not locked on the TAO vault (the attestation has yet to catch up).',
        'Bond relay not configured on this validator, so it cannot verify your TAO bond.',
    ):
        info = _reason(reason)
        assert info.category == 'bond_not_ready', reason
        assert info.deterministic is False and reason.split('—')[0].strip() in info.headline


def test_an_unknown_backing_keeps_its_own_category():
    # Must not fall into the bond rule: naming a backing this subnet doesn't have is the miner's
    # typo to fix, not a wait-for-the-relay.
    info = _reason('Unknown backing "btc" — this subnet backs: sol, tao')
    assert info.category == 'unknown_backing'
    assert info.deterministic is True and 'btc' in info.headline


def test_the_sol_collateral_refusal_is_untouched():
    info = _reason('Insufficient collateral: 10 < 1000000')
    assert info.category == 'insufficient_collateral'
    assert info.deterministic is True and 'alw collateral deposit' in info.headline

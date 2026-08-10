"""W2b — quote-level backing across the CLI: the D2 resolution ergonomics, the admin setters that
reach the W1/W2 config fields, and the labels that tell two quotes on one direction apart.

The resolver is the interesting half: a quote's backing decides what a taker is guaranteed when the
miner fails, so it is inferred only when there is exactly one answer and is NEVER broken by market
state — a scripted `alw miner post` has to mean the same thing on every run.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from solders.keypair import Keypair

from allways.cli.swap_commands.admin import admin_group
from allways.cli.swap_commands.helpers import backing_label, declarable_backings, resolve_quote_backing
from allways.cli.swap_commands.pair import post_pair
from allways.constants import TAO_TO_RAO
from allways.solana.pdas import BACKING_BIT_SOL, BACKING_BIT_TAO


def _state(mask):
    return SimpleNamespace(active_backings=mask, active=mask != 0)


def _resolve(mask, f, t, explicit=None):
    return resolve_quote_backing(_state(mask), f, t, explicit)


# --- which backings a pair can carry at all ---------------------------------------------------


def test_declarable_backings_follows_the_legs_not_the_pair():
    # One hub leg -> one answer; two hub legs -> a real choice; no hub leg -> nothing can back it.
    assert declarable_backings('btc', 'sol') == ['sol']
    assert declarable_backings('tao', 'btc') == ['tao']
    assert declarable_backings('sol', 'tao') == ['sol', 'tao']
    assert declarable_backings('btc', 'eth') == []


# --- inference (the common case) --------------------------------------------------------------


def test_a_one_hub_pair_infers_silently():
    both = BACKING_BIT_SOL | BACKING_BIT_TAO
    assert _resolve(both, 'btc', 'sol') == 'sol'
    assert _resolve(both, 'tao', 'btc') == 'tao'


def test_a_single_purse_miner_infers_on_a_hub_to_hub_pair():
    # sol<->tao could go either way, but only one of this miner's purses qualifies — no flag needed.
    assert _resolve(BACKING_BIT_SOL, 'sol', 'tao') == 'sol'
    assert _resolve(BACKING_BIT_TAO, 'sol', 'tao') == 'tao'


# --- the hard error (the whole point of the flag) ----------------------------------------------


def test_a_dual_purse_miner_must_name_the_backing_on_a_hub_to_hub_pair():
    with pytest.raises(SystemExit):
        _resolve(BACKING_BIT_SOL | BACKING_BIT_TAO, 'sol', 'tao')


def test_the_dual_purse_error_names_the_flag_and_both_options(capsys):
    with pytest.raises(SystemExit):
        _resolve(BACKING_BIT_SOL | BACKING_BIT_TAO, 'sol', 'tao')
    out = capsys.readouterr().out
    assert '--backing' in out
    assert 'sol' in out and 'tao' in out


def test_an_explicit_backing_settles_the_tie():
    both = BACKING_BIT_SOL | BACKING_BIT_TAO
    assert _resolve(both, 'sol', 'tao', 'tao') == 'tao'
    assert _resolve(both, 'sol', 'tao', 'sol') == 'sol'
    assert _resolve(both, 'sol', 'tao', 'TAO') == 'tao', 'case-insensitive at intake'


# --- refusals ----------------------------------------------------------------------------------


def test_a_backing_that_is_not_a_leg_is_refused():
    with pytest.raises(SystemExit):
        _resolve(BACKING_BIT_SOL | BACKING_BIT_TAO, 'btc', 'tao', 'sol')


def test_a_backing_whose_purse_is_dark_is_refused():
    with pytest.raises(SystemExit):
        _resolve(BACKING_BIT_SOL, 'sol', 'tao', 'tao')


def test_an_unknown_backing_is_refused():
    with pytest.raises(SystemExit):
        _resolve(BACKING_BIT_SOL, 'btc', 'sol', 'btc')


def test_a_pair_with_no_hub_leg_is_refused():
    with pytest.raises(SystemExit):
        _resolve(BACKING_BIT_SOL | BACKING_BIT_TAO, 'btc', 'eth')


def test_a_miner_with_no_active_purse_is_refused():
    with pytest.raises(SystemExit):
        _resolve(0, 'btc', 'sol')


# --- labels ------------------------------------------------------------------------------------


def test_backing_labels_name_the_guarantee():
    assert backing_label('sol') == 'sol-backed'
    assert backing_label('tao') == 'tao-backed'
    # An unrecognized backing is shown, not hidden — that is exactly what an operator needs to see.
    assert backing_label('eth') == 'eth-backed'
    assert backing_label(None) == 'unbacked'


# --- end to end through `alw miner post` -------------------------------------------------------


def _client(mask):
    c = MagicMock()
    c.keypair = Keypair()
    c.get_miner_state.return_value = _state(mask)
    c.get_quote.return_value = None  # no standing quote -> creation is free
    return c


def _post(mask, argv):
    wallet = SimpleNamespace(hotkey=SimpleNamespace(ss58_address='5Fhotkey'))
    c = _client(mask)
    with (
        patch('allways.cli.swap_commands.pair.get_cli_context', return_value=({}, wallet, None, None)),
        patch('allways.cli.swap_commands.pair.get_solana_cli_context', return_value=({}, c)),
        patch('allways.cli.swap_commands.pair.write_rate_posted_flag'),
    ):
        return c, CliRunner().invoke(post_pair, argv)


def test_post_pair_hard_errors_when_dual_qualified():
    c, res = _post(BACKING_BIT_SOL | BACKING_BIT_TAO, ['sol', 'So1src', 'tao', '5dst', '10', '--yes'])
    assert res.exit_code != 0
    assert '--backing' in res.output
    c.set_quote.assert_not_called()


def test_post_pair_accepts_the_flag_and_forwards_it_to_both_directions():
    c, res = _post(
        BACKING_BIT_SOL | BACKING_BIT_TAO,
        ['sol', 'So1src', 'tao', '5dst', '10', '--backing', 'tao', '--yes'],
    )
    assert res.exit_code == 0, res.output
    assert c.set_quote.call_count == 2
    assert {call.kwargs['backing'] for call in c.set_quote.call_args_list} == {'tao'}
    assert 'tao-backed' in res.output


def test_post_pair_infers_for_a_single_purse_miner_with_no_flag():
    c, res = _post(BACKING_BIT_TAO, ['sol', 'So1src', 'tao', '5dst', '10', '--yes'])
    assert res.exit_code == 0, res.output
    assert {call.kwargs['backing'] for call in c.set_quote.call_args_list} == {'tao'}


def test_post_pair_never_infers_from_market_state():
    # Both purses lit and a standing SOL-backed quote already in the book: the CLI still refuses to
    # pick, because "what you already quoted" is market state, not an instruction.
    c = _client(BACKING_BIT_SOL | BACKING_BIT_TAO)
    c.get_quote.return_value = SimpleNamespace(updated_at=0, rate=10**18)
    wallet = SimpleNamespace(hotkey=SimpleNamespace(ss58_address='5Fhotkey'))
    with (
        patch('allways.cli.swap_commands.pair.get_cli_context', return_value=({}, wallet, None, None)),
        patch('allways.cli.swap_commands.pair.get_solana_cli_context', return_value=({}, c)),
        patch('allways.cli.swap_commands.pair.write_rate_posted_flag'),
    ):
        res = CliRunner().invoke(post_pair, ['sol', 'So1src', 'tao', '5dst', '10', '--yes'])
    assert res.exit_code != 0
    assert '--backing' in res.output


# --- admin setters reaching the W1/W2 config fields ---------------------------------------------


def _admin_client(**cfg):
    base = dict(
        tao_min_swap_amount=1_000,
        tao_max_swap_amount=0,
        tao_min_collateral=100_000_000,
        settlement_grace_secs=900,
        attest_max_age_secs=86_400,
    )
    base.update(cfg)
    c = MagicMock()
    c.keypair = Keypair()
    c.get_config.return_value = SimpleNamespace(**base)
    return c


def _admin(argv):
    c = _admin_client()
    with patch('allways.cli.swap_commands.admin.get_solana_cli_context', return_value=({}, c)):
        return c, CliRunner().invoke(admin_group, ['--yes'] + argv)


@pytest.mark.parametrize(
    'argv, method, expected',
    [
        (['set-tao-min-swap', '0.5'], 'set_tao_min_swap_amount', int(0.5 * TAO_TO_RAO)),
        (['set-tao-max-swap', '25'], 'set_tao_max_swap_amount', 25 * TAO_TO_RAO),
        (['set-tao-min-collateral', '5'], 'set_tao_min_collateral', 5 * TAO_TO_RAO),
        (['set-settlement-grace', '900'], 'set_settlement_grace', 900),
        (['set-attest-max-age', '86400'], 'set_attest_max_age', 86_400),
    ],
)
def test_admin_setters_reach_the_split_collateral_fields(argv, method, expected):
    c, res = _admin(argv)
    assert res.exit_code == 0, res.output
    getattr(c, method).assert_called_once_with(expected)


@pytest.mark.parametrize(
    'argv',
    [
        ['set-settlement-grace', '30'],  # below the 1-minute floor
        ['set-settlement-grace', '9000'],  # above the 2-hour lid
        ['set-attest-max-age', '600'],  # below one heartbeat interval
        ['set-attest-max-age', '200000'],  # past being a circuit breaker
        ['set-tao-min-collateral', '0'],  # a dust bond must not activate a purse
    ],
)
def test_admin_setters_refuse_out_of_range_values_before_sending(argv):
    # The program enforces these too; refusing client-side keeps a doomed tx (and its fee) off-chain.
    c, res = _admin(argv)
    assert res.exit_code != 0
    for name in ('set_settlement_grace', 'set_attest_max_age', 'set_tao_min_collateral'):
        getattr(c, name).assert_not_called()


# --- taker side: the bid must land on the quote that won ----------------------------------------


class _TwoQuoteClient:
    """A dual-purse miner standing two offers on sol->tao at different rates — the D2 market."""

    def __init__(self):
        self.quotes = [
            SimpleNamespace(miner='m1', from_chain='sol', to_chain='tao', rate=10 * 10**18, collateral_chain='sol'),
            SimpleNamespace(miner='m1', from_chain='sol', to_chain='tao', rate=11 * 10**18, collateral_chain='tao'),
        ]
        self.state_reads = 0

    def get_all(self, name):
        assert name == 'MinerQuote'
        return [(f'pda{i}', q) for i, q in enumerate(self.quotes)]

    def get_miner_state(self, miner):
        self.state_reads += 1
        return SimpleNamespace(active=True, collateral=10**11)

    def get_bond_attestation(self, miner, chain='tao'):
        # The quorum's assertion about the TAO purse — locked, and deep enough to back the offer.
        return SimpleNamespace(effective_balance=10**11, locked=True, epoch=1)


def test_candidate_miners_surfaces_both_offers_with_their_backings():
    from allways.cli.swap_commands.swap_intake import candidate_miners

    c = _TwoQuoteClient()
    cands = candidate_miners(c, 'sol', 'tao')
    assert len(cands) == 2, 'both offers compete — one market per pair, mixed by rate'
    assert {x.backing for x in cands} == {'sol', 'tao'}
    # The two offers belong to the same miner, so its state is read once, not once per quote.
    assert c.state_reads == 1


def test_the_better_offer_carries_its_own_backing_to_the_bid():
    from allways.cli.swap_commands.swap_intake import candidate_miners, select_best_miner

    cands = candidate_miners(_TwoQuoteClient(), 'sol', 'tao')
    best = select_best_miner(cands, 'sol', 'tao', 10**9, 0, 0)
    assert best is not None
    # The tao-backed quote prices higher, so it wins on to_amount — and the bid must name "tao", or it
    # would land on the sol-backed quote's PDA (a different offer at a different rate).
    assert best[0].backing == 'tao'


def test_a_candidate_defaults_to_the_local_purse_when_a_quote_predates_the_field():
    from allways.cli.swap_commands.swap_intake import MinerCandidate

    assert MinerCandidate(miner='m', rate_display='1', collateral=0).backing == 'sol'


# --- taker preview: the guarantee is stated, not implied ----------------------------------------


def test_the_quote_preview_states_each_backings_guarantee():
    from allways.cli.swap_commands.quote import GUARANTEE

    # Timing is the difference, and both halves must say so — the SOL path is instant, the TAO path
    # is "shortly after". A taker choosing between two offers is choosing between these two lines.
    assert 'instant' in GUARANTEE['sol'].lower()
    assert 'tao' in GUARANTEE['tao'].lower() and 'timeout' in GUARANTEE['tao'].lower()
    assert set(GUARANTEE) == {'sol', 'tao'}, 'every backing needs stated copy before it can trade'


# --- W3 bid routing: the reserve engine bids the market, not a purse ------------------------------


class _RoutingClient:
    """A miner standing both offers on sol->tao, with a purse behind each."""

    def __init__(self, sol_rate=10 * 10**18, tao_rate=11 * 10**18, tao_purse=10**11, tao_locked=True):
        self.quotes = {
            'sol': SimpleNamespace(
                miner='m1', from_chain='sol', to_chain='tao', rate=sol_rate,
                collateral_chain='sol', miner_from_addr='',
            ),
            'tao': SimpleNamespace(
                miner='m1', from_chain='sol', to_chain='tao', rate=tao_rate,
                collateral_chain='tao', miner_from_addr='',
            ),
        }
        self.tao_purse = tao_purse
        self.tao_locked = tao_locked
        self.bids = []

    def get_quotes_for_direction(self, miner, from_chain, to_chain):
        return list(self.quotes.values())

    def get_quote(self, miner, from_chain, to_chain, backing='sol'):
        return self.quotes.get(backing)

    def get_miner_state(self, miner):
        return SimpleNamespace(active=True, has_active_swap=False, collateral=10**11)

    def get_bond_attestation(self, miner, chain='tao'):
        return SimpleNamespace(effective_balance=self.tao_purse, locked=self.tao_locked, epoch=1)

    def get_config(self):
        return SimpleNamespace(
            min_swap_amount=0, max_swap_amount=0, tao_min_swap_amount=0, tao_max_swap_amount=0
        )

    def get_pool(self, miner):
        return None


def _pick(client, from_amount=10**9):
    from allways.cli.swap_commands.swap_intake import bounds_from_config
    from allways.validator.reserve_engine import _best_offer

    state = client.get_miner_state('m1')
    bounds = bounds_from_config(client.get_config())
    return _best_offer(client, 'm1', state, 'sol', 'tao', from_amount, bounds)


def test_the_engine_bids_the_best_rate_regardless_of_which_purse_backs_it():
    offer, why = _pick(_RoutingClient(sol_rate=10 * 10**18, tao_rate=11 * 10**18))
    assert why == '' and offer[1] == 'tao'
    offer, _ = _pick(_RoutingClient(sol_rate=12 * 10**18, tao_rate=11 * 10**18))
    assert offer[1] == 'sol'


def test_an_exact_tie_goes_to_sol_because_its_failure_guarantee_is_instant():
    offer, _ = _pick(_RoutingClient(sol_rate=11 * 10**18, tao_rate=11 * 10**18))
    assert offer[1] == 'sol', 'same value to the taker, strictly better guarantee'


def test_an_offer_with_no_locked_bond_behind_it_is_not_bid_on():
    # The contract's entry gate refuses an unlocked attestation, so bidding would burn the fee.
    offer, _ = _pick(_RoutingClient(tao_locked=False))
    assert offer[1] == 'sol'


def test_a_tao_offer_is_sized_against_its_own_leg_not_the_sol_one():
    from allways.cli.swap_commands.swap_intake import compute_intake_amounts

    # sol->tao at 11 tao per sol: a 1 SOL source is a 11 TAO dest. A tao-backed swap is
    # collateralised against the TAO leg (rao), a sol-backed one against the SOL leg (lamports).
    tao = compute_intake_amounts('sol', 'tao', 10**9, '11', 'tao')
    sol = compute_intake_amounts('sol', 'tao', 10**9, '11', 'sol')
    assert tao.collateral_amount == 11 * 10**9 == tao.to_amount
    assert sol.collateral_amount == 10**9 == sol.from_amount


def test_a_backing_absent_from_the_legs_is_refused_the_way_the_program_refuses_it():
    from allways.cli.swap_commands.swap_intake import compute_intake_amounts

    with pytest.raises(ValueError, match='tao'):
        compute_intake_amounts('sol', 'btc', 10**9, '0.0021', 'tao')


def test_a_tao_offer_whose_bond_is_too_thin_says_so_in_tao():
    # 11 TAO leg needs 12.1 TAO of bond; a 1 TAO purse cannot take it, and the message has to be
    # denominated in the asset the miner actually posted.
    client = _RoutingClient(sol_rate=1, tao_rate=11 * 10**18, tao_purse=10**9)
    client.quotes.pop('sol')
    offer, why = _pick(client)
    assert offer is None
    assert 'collateral too low' in why and 'TAO' in why


def test_the_routed_bid_names_the_backing_it_won():
    import threading
    from unittest.mock import patch

    from allways.validator.reserve_engine import reserve_on_behalf

    client = _RoutingClient()
    client.open_or_request = lambda miner, f, t, backing='sol': client.bids.append(backing) or 'sig'
    validator = SimpleNamespace(
        solana_client=client,
        axon_lock=threading.RLock(),
        state_store=SimpleNamespace(upsert_routed_request=lambda *a: None),
        axon_assets={},
    )
    with patch('allways.validator.reserve_engine.resolve_miner_pubkey', return_value='m1'):
        res = reserve_on_behalf(
            validator, 'hk', 'sol', 'tao', '68ToGUYjjYpqi7Atx7QyhbybR2RCfo2tkmgcoNR3DxYF',
            'user-sol', 'user-tao', 10**9,
        )
    assert res.ok, res.reason
    assert client.bids == ['tao'], 'a bid on the wrong backing lands on a different offer entirely'

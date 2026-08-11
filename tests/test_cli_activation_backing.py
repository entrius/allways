"""W3.2 — `alw miner activate` picks ONE purse, per the same D2 ergonomics `alw miner post` uses.

Purses activate independently (W2), so activation names a backing. A candidate is a purse that is
dark AND funded above its own floor — the exact pair of facts the contract's guard checks — which
makes the common paths flag-free: a SOL-only miner infers "sol", and a miner who bonds TAO after
activating SOL infers "tao" because the lit purse is no longer a candidate. Two funded dark purses
is the one case that asks, because the backing is the guarantee the miner is choosing to sell.
"""

from types import SimpleNamespace

import pytest

from allways.cli.swap_commands.helpers import (
    activation_prerequisites,
    purse_states,
    resolve_activation_backing,
)
from allways.solana.pdas import BACKING_BIT_SOL, BACKING_BIT_TAO

SOL_FLOOR = 100_000_000  # 0.1 SOL in lamports
TAO_FLOOR = 250_000_000  # 0.25 TAO in rao

CONFIG = SimpleNamespace(min_collateral=SOL_FLOOR, tao_min_collateral=TAO_FLOOR)


def flat(capsys) -> str:
    """Rich hard-wraps to the terminal width, so assert against unwrapped text."""
    return ' '.join(capsys.readouterr().out.split())


class FakeClient:
    """Reads exactly what the resolver needs: the TAO purse is an attestation, never a balance."""

    def __init__(self, attestation=None):
        self.attestation = attestation

    def get_bond_attestation(self, miner, chain='tao'):
        return self.attestation


def states(*, collateral=0, mask=0, bond=None, locked=True):
    attestation = None if bond is None else SimpleNamespace(effective_balance=bond, locked=locked, epoch=1)
    miner_state = SimpleNamespace(collateral=collateral, active_backings=mask, active=mask != 0)
    return purse_states(FakeClient(attestation), 'minerpk', miner_state, CONFIG)


def resolve(explicit=None, **kw):
    return resolve_activation_backing(states(**kw), explicit)


# --- inference: the paths a miner should never need the flag for -------------------------------


def test_a_sol_only_miner_infers_sol():
    assert resolve(collateral=SOL_FLOOR) == 'sol'


def test_the_readme_onboarding_order_never_needs_the_flag():
    # bind -> deposit -> activate (infers sol) -> bond TAO -> activate again. The second call has
    # only one candidate BECAUSE the first purse is lit, which is what keeps the sequence flag-free.
    assert resolve(collateral=SOL_FLOOR) == 'sol'
    assert resolve(collateral=SOL_FLOOR, mask=BACKING_BIT_SOL, bond=50 * TAO_FLOOR) == 'tao'


def test_a_purse_under_its_own_floor_is_not_a_candidate():
    # The SOL purse is funded but short, so it doesn't make the TAO choice ambiguous. Floors are
    # per backing and never converted through a rate.
    assert resolve(collateral=SOL_FLOOR - 1, bond=50 * TAO_FLOOR) == 'tao'


def test_an_unlocked_bond_is_not_a_candidate():
    # An unlocked bond backs nothing — same rule backing_purse applies to quoting.
    assert resolve(collateral=SOL_FLOOR, bond=50 * TAO_FLOOR, locked=False) == 'sol'


# --- the hard error (why the flag exists) ------------------------------------------------------


def test_two_funded_dark_purses_must_name_one(capsys):
    with pytest.raises(SystemExit):
        resolve(collateral=SOL_FLOOR, bond=50 * TAO_FLOOR)
    assert '--backing' in flat(capsys)


def test_the_tie_is_never_broken_by_which_purse_holds_more():
    # Neither direction infers: a bigger bond is not a reason to sell a different guarantee.
    for collateral, bond in ((SOL_FLOOR, 500 * TAO_FLOOR), (500 * SOL_FLOOR, TAO_FLOOR)):
        with pytest.raises(SystemExit):
            resolve(collateral=collateral, bond=bond)


# --- explicit --backing ------------------------------------------------------------------------


def test_explicit_backing_is_honoured_when_it_is_a_candidate():
    assert resolve('tao', collateral=SOL_FLOOR, bond=50 * TAO_FLOOR) == 'tao'
    assert resolve('sol', collateral=SOL_FLOOR, bond=50 * TAO_FLOOR) == 'sol'


def test_explicit_backing_is_case_insensitive():
    assert resolve('TAO', bond=50 * TAO_FLOOR) == 'tao'


def test_an_unknown_backing_is_refused():
    with pytest.raises(SystemExit):
        resolve('btc', collateral=SOL_FLOOR)


def test_a_purse_that_is_already_serving_is_refused(capsys):
    with pytest.raises(SystemExit):
        resolve('sol', collateral=SOL_FLOOR, mask=BACKING_BIT_SOL)
    assert 'already serving' in flat(capsys)


def test_an_unfunded_explicit_purse_names_its_shortfall(capsys):
    with pytest.raises(SystemExit):
        resolve('tao', collateral=SOL_FLOOR, bond=TAO_FLOOR - 1)
    assert str(TAO_FLOOR) in flat(capsys)


# --- nothing to activate -----------------------------------------------------------------------


def test_no_candidate_says_what_each_dark_purse_is_missing(capsys):
    with pytest.raises(SystemExit):
        resolve()
    out = flat(capsys)
    assert 'alw collateral deposit' in out and 'no LOCKED bond' in out


def test_everything_already_serving_says_so(capsys):
    with pytest.raises(SystemExit):
        resolve(collateral=SOL_FLOOR, bond=50 * TAO_FLOOR, mask=BACKING_BIT_SOL | BACKING_BIT_TAO)
    assert 'already serving' in flat(capsys)


# --- the refusal checklist ---------------------------------------------------------------------


def test_the_prerequisites_shown_are_the_ones_for_the_purse_that_was_refused():
    # Sending a TAO miner to `alw collateral deposit` is worse than saying nothing: it points at
    # the wrong chain. Each checklist names only its own purse's chain.
    assert any('alw collateral deposit' in line for line in activation_prerequisites('sol'))
    tao = ' '.join(activation_prerequisites('tao'))
    assert 'alw vault lock' in tao and 'alw collateral deposit' not in tao

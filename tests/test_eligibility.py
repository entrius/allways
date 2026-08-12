"""B3.3 — flat eligibility gate, end-to-end pubkey→hotkey→UID attribution.

Unlike ``tests/test_scoring_v1.py`` (which patches attribution to identity for
the crown tests), this exercises ``build_eligibility`` through the *real*
sr25519 ``build_attribution`` path, so the on-chain pubkey-keyed ``MinerState``
counters are attributed to actual hotkey ss58 addresses before the gate runs.
"""

from types import SimpleNamespace

import bittensor as bt
import pytest
from solders.keypair import Keypair as SolKeypair

from allways.constants import MAX_FAILED_SWAPS, MIN_SUCCESSFUL_SWAPS
from allways.validator.scoring import build_eligibility, direction_eligible, is_eligible


def _hotkey():
    return bt.Keypair.create_from_mnemonic(bt.Keypair.generate_mnemonic())


def _binding(miner_pk, hotkey_kp, bound_at=1000):
    return SimpleNamespace(
        miner=miner_pk,
        hotkey=bytes.fromhex(hotkey_kp.public_key.hex()),
        hotkey_sig=hotkey_kp.sign(bytes(miner_pk)),
        bound_at=bound_at,
    )


def _miner_state(miner_pk, successful, failed, settling_until=0):
    return SimpleNamespace(
        miner=miner_pk, successful_swaps=successful, failed_swaps=failed, settling_until=settling_until
    )


class _Client:
    def __init__(self, bindings, states):
        self._bindings = bindings
        self._states = states

    def get_all(self, name):
        if name == 'Binding':
            return [(f'bpda{i}', b) for i, b in enumerate(self._bindings)]
        if name == 'MinerState':
            return [(f'mpda{i}', s) for i, s in enumerate(self._states)]
        return []


def test_attributes_pubkey_to_hotkey_then_gates():
    """Two bound miners: one above the success floor (eligible), one below."""
    m1, m2 = SolKeypair().pubkey(), SolKeypair().pubkey()
    hk1, hk2 = _hotkey(), _hotkey()
    client = _Client(
        bindings=[_binding(m1, hk1), _binding(m2, hk2)],
        states=[
            _miner_state(m1, MIN_SUCCESSFUL_SWAPS, 0),
            _miner_state(m2, MIN_SUCCESSFUL_SWAPS - 1, 0),
        ],
    )
    metagraph = SimpleNamespace(hotkeys=[hk1.ss58_address, hk2.ss58_address])
    assert build_eligibility(client, metagraph) == {
        hk1.ss58_address: True,
        hk2.ss58_address: False,
    }


def test_high_fail_miner_attributed_but_ineligible():
    m1 = SolKeypair().pubkey()
    hk1 = _hotkey()
    client = _Client(
        bindings=[_binding(m1, hk1)],
        states=[_miner_state(m1, 50, MAX_FAILED_SWAPS + 1)],
    )
    metagraph = SimpleNamespace(hotkeys=[hk1.ss58_address])
    assert build_eligibility(client, metagraph) == {hk1.ss58_address: False}


def test_unbound_miner_state_dropped():
    """A MinerState whose pubkey has no Binding can't map to a hotkey → skipped."""
    m1 = SolKeypair().pubkey()
    client = _Client(bindings=[], states=[_miner_state(m1, 5, 0)])
    metagraph = SimpleNamespace(hotkeys=['5anything'])
    assert build_eligibility(client, metagraph) == {}


def test_off_metagraph_hotkey_dropped():
    """A bound, eligible miner not registered on the metagraph has no UID to
    credit, so it's excluded from the eligibility map."""
    m1 = SolKeypair().pubkey()
    hk1 = _hotkey()
    client = _Client(bindings=[_binding(m1, hk1)], states=[_miner_state(m1, 5, 0)])
    metagraph = SimpleNamespace(hotkeys=[])
    assert build_eligibility(client, metagraph) == {}


def test_invalid_binding_sig_drops_miner():
    """A tampered binding sig fails sr25519 verify → no attribution → dropped."""
    m1 = SolKeypair().pubkey()
    hk1 = _hotkey()
    b = _binding(m1, hk1)
    b.hotkey_sig = bytes(bytearray(b.hotkey_sig)[:-1] + b'\x00')  # corrupt last byte
    client = _Client(bindings=[b], states=[_miner_state(m1, 5, 0)])
    metagraph = SimpleNamespace(hotkeys=[hk1.ss58_address])
    assert build_eligibility(client, metagraph) == {}


def _ns(successful, failed, settling_until=0):
    return SimpleNamespace(successful_swaps=successful, failed_swaps=failed, settling_until=settling_until)


def _ns_hub(successful, failed, tao_settling_until=0):
    # Per-hub array shape (v3.1): slot 0 = sol, slot 1 = tao.
    return SimpleNamespace(
        successful_swaps=successful, failed_swaps=failed, settling_until=[0, tao_settling_until] + [0] * 6
    )


def test_is_eligible_boundaries():
    assert is_eligible(_ns(MIN_SUCCESSFUL_SWAPS, MAX_FAILED_SWAPS))
    assert not is_eligible(_ns(MIN_SUCCESSFUL_SWAPS - 1, 0))
    assert not is_eligible(_ns(99, MAX_FAILED_SWAPS + 1))


def test_a_tao_settle_zeroes_only_tao_directions():
    """v3.1: a pending TAO seizure is not a rewardable state ON ITS HUB — the same window the
    contract refuses new TAO entry over pays that hub zero, while the SOL hub keeps earning.
    Self-clearing at the deadline — no crank re-enables the hub."""
    settling = _ns_hub(50, 0, tao_settling_until=2_000)
    # A direction whose only hub is TAO pays nothing while the seizure settles...
    assert not direction_eligible(settling, 'tao', 'btc', 1_999)
    assert direction_eligible(settling, 'tao', 'btc', 2_000)
    # ...while a SOL-hub direction keeps earning throughout.
    assert direction_eligible(settling, 'btc', 'sol', 1_999)
    # A hub↔hub direction earns while EITHER purse is clean.
    assert direction_eligible(settling, 'sol', 'tao', 1_999)
    # Strikes stay global: struck out ⇒ no direction earns, settling or not.
    struck = _ns_hub(50, MAX_FAILED_SWAPS + 1)
    assert not direction_eligible(struck, 'btc', 'sol', 1_999)


@pytest.mark.xfail(
    reason='V-2 deferred: zeroing only the TAO share of the sol↔tao hub↔hub direction mid-settle '
    'needs backing-aware score rows (ships with per-hub capacity). direction_eligible is '
    'whole-direction, so there is no per-backing gate yet — an any→all flip would wrongly zero the '
    'honest SOL share. Fix before any TAO-only (no SOL co-hub) direction ships.',
    strict=True,
)
def test_tao_share_of_hub_hub_direction_is_zeroed_mid_settle_xfail():
    """Documents the deferred gap: on sol↔tao a TAO-settling miner should earn its SOL-backed share
    but NOT its TAO-backed share. That requires a per-backing eligibility the code does not expose,
    so this asserts the intended shape and is expected to fail until backing-aware rows land."""
    settling = _ns_hub(50, 0, tao_settling_until=2_000)
    assert direction_eligible(settling, 'sol', 'tao', 1_999, backing='tao') is False  # TAO share excluded
    assert direction_eligible(settling, 'sol', 'tao', 1_999, backing='sol') is True  # SOL share earns


def test_the_pre_v31_scalar_settling_shape_still_gates():
    """Mixed-version tolerance: a scalar `settling_until` (pre-v3.1 decode) reads as the global
    exclusion it always was."""
    assert not direction_eligible(_ns(50, 0, settling_until=2_000), 'btc', 'sol', 1_999)
    assert direction_eligible(_ns(50, 0, settling_until=2_000), 'btc', 'sol', 2_000)


def test_strike_gate_reaches_the_eligibility_map():
    """The global gate has to bite through ``build_eligibility``, not just the helper —
    that map is what the trace log reports."""
    m1, m2 = SolKeypair().pubkey(), SolKeypair().pubkey()
    hk1, hk2 = _hotkey(), _hotkey()
    client = _Client(
        bindings=[_binding(m1, hk1), _binding(m2, hk2)],
        states=[_miner_state(m1, 50, MAX_FAILED_SWAPS + 1), _miner_state(m2, 50, 0)],
    )
    metagraph = SimpleNamespace(hotkeys=[hk1.ss58_address, hk2.ss58_address])
    assert build_eligibility(client, metagraph, now=1_999) == {
        hk1.ss58_address: False,
        hk2.ss58_address: True,
    }

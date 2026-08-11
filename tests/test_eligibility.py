"""B3.3 — flat eligibility gate, end-to-end pubkey→hotkey→UID attribution.

Unlike ``tests/test_scoring_v1.py`` (which patches attribution to identity for
the crown tests), this exercises ``build_eligibility`` through the *real*
sr25519 ``build_attribution`` path, so the on-chain pubkey-keyed ``MinerState``
counters are attributed to actual hotkey ss58 addresses before the gate runs.
"""

from types import SimpleNamespace

import bittensor as bt
from solders.keypair import Keypair as SolKeypair

from allways.constants import MAX_FAILED_SWAPS, MIN_SUCCESSFUL_SWAPS
from allways.validator.scoring import build_eligibility, is_eligible


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


def test_is_eligible_boundaries():
    assert is_eligible(_ns(MIN_SUCCESSFUL_SWAPS, MAX_FAILED_SWAPS))
    assert not is_eligible(_ns(MIN_SUCCESSFUL_SWAPS - 1, 0))
    assert not is_eligible(_ns(99, MAX_FAILED_SWAPS + 1))


def test_a_settling_miner_earns_nothing():
    """A pending off-chain seizure is not a rewardable state: the same window the
    contract refuses new entry over also pays zero. Self-clearing at the deadline —
    no crank re-enables the miner."""
    assert not is_eligible(_ns(50, 0, settling_until=2_000), now=1_999)
    assert is_eligible(_ns(50, 0, settling_until=2_000), now=2_000)
    assert is_eligible(_ns(50, 0, settling_until=0), now=1_999)


def test_settling_exclusion_reaches_the_eligibility_map():
    """The gate has to bite through ``build_eligibility``, not just the helper —
    that map is what the reward math multiplies by."""
    m1, m2 = SolKeypair().pubkey(), SolKeypair().pubkey()
    hk1, hk2 = _hotkey(), _hotkey()
    client = _Client(
        bindings=[_binding(m1, hk1), _binding(m2, hk2)],
        states=[_miner_state(m1, 50, 0, settling_until=2_000), _miner_state(m2, 50, 0)],
    )
    metagraph = SimpleNamespace(hotkeys=[hk1.ss58_address, hk2.ss58_address])
    assert build_eligibility(client, metagraph, now=1_999) == {
        hk1.ss58_address: False,
        hk2.ss58_address: True,
    }

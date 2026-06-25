"""B3.2 — sr25519 binding verification + pubkey→hotkey attribution."""

from types import SimpleNamespace

import bittensor as bt
from solders.keypair import Keypair as SolKeypair

from allways.validator import binding


def _hotkey():
    return bt.Keypair.create_from_mnemonic(bt.Keypair.generate_mnemonic())


def _binding(miner_pk, hotkey_kp, bound_at=1000, sign_msg=None):
    msg = sign_msg if sign_msg is not None else bytes(miner_pk)
    sig = hotkey_kp.sign(msg)
    return SimpleNamespace(
        miner=miner_pk,
        hotkey=bytes.fromhex(hotkey_kp.public_key.hex()),
        hotkey_sig=sig,
        bound_at=bound_at,
    )


def test_verify_binding_valid():
    miner = SolKeypair().pubkey()
    hk = _hotkey()
    b = _binding(miner, hk)
    assert binding.verify_binding(b.miner, b.hotkey, b.hotkey_sig) is True


def test_verify_binding_rejects_wrong_message():
    miner = SolKeypair().pubkey()
    other = SolKeypair().pubkey()
    hk = _hotkey()
    # Signature is over `other`, but we verify against `miner` → must fail.
    b = _binding(miner, hk, sign_msg=bytes(other))
    assert binding.verify_binding(b.miner, b.hotkey, b.hotkey_sig) is False


def test_verify_binding_rejects_tampered_sig():
    miner = SolKeypair().pubkey()
    hk = _hotkey()
    b = _binding(miner, hk)
    bad = bytearray(b.hotkey_sig)
    bad[0] ^= 0xFF
    assert binding.verify_binding(b.miner, b.hotkey, bytes(bad)) is False


def test_hotkey_ss58_roundtrip():
    hk = _hotkey()
    pub = bytes.fromhex(hk.public_key.hex())
    assert binding.hotkey_ss58(pub) == hk.ss58_address


def test_build_attribution_maps_valid_bindings():
    m1, m2 = SolKeypair().pubkey(), SolKeypair().pubkey()
    hk1, hk2 = _hotkey(), _hotkey()
    client = SimpleNamespace(
        get_all=lambda name: [('pda1', _binding(m1, hk1)), ('pda2', _binding(m2, hk2))]
    )
    amap = binding.build_attribution(client)
    assert amap[str(m1)] == hk1.ss58_address
    assert amap[str(m2)] == hk2.ss58_address


def test_build_attribution_skips_invalid_sig():
    m1 = SolKeypair().pubkey()
    hk1 = _hotkey()
    bad = _binding(m1, hk1, sign_msg=b'wrong')
    client = SimpleNamespace(get_all=lambda name: [('pda1', bad)])
    assert binding.build_attribution(client) == {}


def test_build_attribution_first_bound_wins_hotkey_collision():
    # Two pubkeys claim the SAME hotkey; earliest bound_at wins (mirrors on-chain set-once).
    m_early, m_late = SolKeypair().pubkey(), SolKeypair().pubkey()
    hk = _hotkey()
    client = SimpleNamespace(
        get_all=lambda name: [
            ('pda_late', _binding(m_late, hk, bound_at=2000)),
            ('pda_early', _binding(m_early, hk, bound_at=1000)),
        ]
    )
    amap = binding.build_attribution(client)
    assert amap == {str(m_early): hk.ss58_address}  # late one rejected

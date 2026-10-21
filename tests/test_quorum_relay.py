"""The confirm relay targets the contract's validator quorum, not every serving validator.

discover_quorum_axons resolves Config.validators → Binding → metagraph axon; resolve_relay_axons
falls back LOUDLY to broadcast-all whenever that chain breaks — the relay may get noisier on a
resolution failure, never narrower than the quorum.
"""

import re
from types import SimpleNamespace

from scalecodec.utils.ss58 import ss58_encode

from allways.cli.dendrite_lite import discover_quorum_axons
from allways.cli.swap_commands import post_tx

VALI_PUBKEY = b'\x07' * 32
VALI_HOTKEY = b'\x11' * 32
VALI_SS58 = ss58_encode(VALI_HOTKEY, ss58_format=42)


def _plain(out: str) -> str:
    """Captured console text minus ANSI codes and line wraps — FORCE_COLOR in the environment
    makes rich emit style codes mid-phrase at wrap points, which breaks substring asserts."""
    return ' '.join(re.sub(r'\x1b\[[0-9;]*m', '', out).split())


def _cfg(validators=None, threshold=67):
    entries = validators if validators is not None else [SimpleNamespace(key=VALI_PUBKEY, weight=82)]
    return SimpleNamespace(validators=entries, consensus_threshold_percent=threshold)


def _client(cfg=None, binding_hotkey=VALI_HOTKEY):
    client = SimpleNamespace()
    client.get_config = lambda: cfg if cfg is not None else _cfg()
    client.get_binding = lambda pk: SimpleNamespace(hotkey=binding_hotkey) if binding_hotkey else None
    return client


def _metagraph(hotkeys, serving=True):
    axons = [SimpleNamespace(is_serving=serving, hotkey=hk) for hk in hotkeys]
    return SimpleNamespace(n=len(hotkeys), hotkeys=hotkeys, axons=axons)


def _subtensor(metagraph):
    return SimpleNamespace(metagraph=lambda netuid: metagraph)


# ─── discover_quorum_axons ───────────────────────────────────────────────────


def test_resolves_whitelisted_validator_to_its_axon():
    sub = _subtensor(_metagraph(['5SomeoneElse', VALI_SS58]))
    axons, names = discover_quorum_axons(_client(), sub, netuid=7)
    assert len(axons) == 1
    assert axons[0].hotkey == VALI_SS58
    assert names == {VALI_SS58: 'vali 1'}


def test_unbound_validator_resolves_to_nothing():
    sub = _subtensor(_metagraph([VALI_SS58]))
    axons, names = discover_quorum_axons(_client(binding_hotkey=None), sub, netuid=7)
    assert axons == [] and names == {}


# ─── resolve_relay_axons ─────────────────────────────────────────────────────


def test_full_quorum_resolution_filters_the_relay():
    sub = _subtensor(_metagraph(['5SomeoneElse', VALI_SS58]))
    axons, names, needed = post_tx.resolve_relay_axons(_client(), sub, netuid=7)
    assert [a.hotkey for a in axons] == [VALI_SS58]
    assert names == {VALI_SS58: 'vali 1'}
    assert needed == 1  # ceil(67% of 1 validator)


def test_partial_resolution_falls_back_to_broadcast_all(monkeypatch, capsys):
    sentinel = [SimpleNamespace(hotkey='5All1'), SimpleNamespace(hotkey='5All2')]
    monkeypatch.setattr(post_tx, 'discover_validators', lambda subtensor, netuid: sentinel)
    sub = _subtensor(_metagraph([VALI_SS58]))

    axons, names, needed = post_tx.resolve_relay_axons(_client(binding_hotkey=None), sub, netuid=7)

    assert axons is sentinel
    assert names == {} and needed == 1
    assert 'every serving validator' in _plain(capsys.readouterr().out)


def test_config_read_failure_falls_back_to_broadcast_all(monkeypatch, capsys):
    sentinel = [SimpleNamespace(hotkey='5All1')]
    monkeypatch.setattr(post_tx, 'discover_validators', lambda subtensor, netuid: sentinel)
    client = SimpleNamespace(get_config=lambda: (_ for _ in ()).throw(RuntimeError('rpc down')))

    axons, names, needed = post_tx.resolve_relay_axons(client, _subtensor(None), netuid=7)

    assert axons is sentinel and needed == 1
    assert 'Quorum resolution failed' in _plain(capsys.readouterr().out)


# ─── identity-labeled rendering ──────────────────────────────────────────────


# ─── relay_deposit wiring ────────────────────────────────────────────────────

"""The confirm relay targets the contract's validator quorum, not every serving validator.

discover_quorum_axons resolves Config.validators → Binding → metagraph axon; resolve_relay_axons
falls back LOUDLY to broadcast-all whenever that chain breaks — the relay may get noisier on a
resolution failure, never narrower than the quorum.
"""

import io
import re
from types import SimpleNamespace

from rich.console import Console
from scalecodec.utils.ss58 import ss58_encode

from allways.cli.dendrite_lite import discover_quorum_axons
from allways.cli.swap_commands import post_tx
from allways.cli.validator_rejections import render_and_aggregate

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


def test_non_serving_axon_is_excluded():
    sub = _subtensor(_metagraph([VALI_SS58], serving=False))
    axons, _names = discover_quorum_axons(_client(), sub, netuid=7)
    assert axons == []


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


def _recording_console():
    return Console(file=io.StringIO(), width=200, force_terminal=False)


def test_named_response_renders_by_identity():
    console = _recording_console()
    resp = SimpleNamespace(accepted=True, rejection_reason='', axon=SimpleNamespace(hotkey=VALI_SS58))
    render_and_aggregate(console, [resp], names={VALI_SS58: 'vali 194'})
    out = console.file.getvalue()
    assert 'vali 194: ok' in out
    assert 'V1' not in out


def test_unnamed_response_keeps_the_positional_tag():
    console = _recording_console()
    resp = SimpleNamespace(accepted=True, rejection_reason='', axon=SimpleNamespace(hotkey='5Unknown'))
    render_and_aggregate(console, [resp], names={VALI_SS58: 'vali 194'})
    assert 'V1: ok' in console.file.getvalue()


# ─── relay_deposit wiring ────────────────────────────────────────────────────


def test_relay_deposit_stops_at_quorum(monkeypatch, capsys):
    calls = {}
    accept = SimpleNamespace(accepted=True, rejection_reason='', axon=SimpleNamespace(hotkey=VALI_SS58))

    def fake_broadcast(dendrite, axons, synapse, needed, timeout):
        calls['needed'] = needed
        return [accept]  # the second axon never answered — early return already fired

    monkeypatch.setattr(post_tx, 'broadcast_until_quorum', fake_broadcast)
    monkeypatch.setattr(post_tx, 'get_ephemeral_wallet', lambda: None)
    monkeypatch.setattr('bittensor.Dendrite', lambda wallet=None: object())
    monkeypatch.setattr(post_tx, 'clear_pending_swap', lambda: None)
    monkeypatch.setattr(post_tx, 'swap_key_from_tx_hash', lambda h: b'\x01' * 32)

    client = SimpleNamespace(rpc=SimpleNamespace(get_transaction=lambda h: {'slot': 1}))
    resv = SimpleNamespace(from_chain='sol', to_chain='eth', from_addr='src', user_to_addr='dst', user='u')
    axons = [SimpleNamespace(hotkey=VALI_SS58), SimpleNamespace(hotkey='5Slow')]

    key = post_tx.relay_deposit(
        client,
        resv,
        'MinerPk',
        '5MinerHot',
        'txhash',
        1,
        validator_axons=axons,
        validator_names={VALI_SS58: 'vali 194'},
        accepts_needed=1,
    )

    assert calls['needed'] == 1
    assert key == (b'\x01' * 32).hex()
    out = _plain(capsys.readouterr().out)
    assert 'stopped waiting on 1 slower validator' in out
    assert 'vali 194: ok' in out

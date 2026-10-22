"""The per-pass crown line lists held lanes only and counts the empty ones; INFO on change, DEBUG otherwise."""

from types import SimpleNamespace

import bittensor as bt

from allways.utils import logging as alog


def _capture(monkeypatch):
    out = []
    monkeypatch.setattr(bt.logging, 'info', lambda m: out.append(('info', m)))
    monkeypatch.setattr(bt.logging, 'debug', lambda m: out.append(('debug', m)))
    monkeypatch.setattr(alog, '_last_crown_holders', None)
    return out


def test_crown_line_lists_held_lanes_and_counts_empty_ones(monkeypatch):
    out = _capture(monkeypatch)
    metagraph = SimpleNamespace(hotkeys=['hkA', 'hkB'])
    row = ('x', 'x', 'x', 'hkB', 0.0, 326.42, 0)
    snapshot = {('btc', 'sol', 'sol'): [row], **{(f'sn{i}', 'sol', 'sol'): [] for i in range(1, 2001)}}
    alog.log_crown_winners(metagraph, 7, snapshot)
    level, line = out[0]
    assert level == 'info'
    assert line.startswith('forward: crown holders @ block=7 | btc->sol[sol] uid=1 rate=326.42 | 2,000 empty lanes')
    assert ' none' not in line and len(line) < 200


def test_crown_line_is_debug_until_the_holders_change(monkeypatch):
    out = _capture(monkeypatch)
    metagraph = SimpleNamespace(hotkeys=['hkA'])
    held = {('btc', 'sol', 'sol'): [('x', 'x', 'x', 'hkA', 0.0, 1.0, 0)]}
    alog.log_crown_winners(metagraph, 1, held)
    alog.log_crown_winners(metagraph, 2, held)  # same holders, new block → quiet
    alog.log_crown_winners(metagraph, 3, {('btc', 'sol', 'sol'): []})  # holder lost → loud
    assert [lvl for lvl, _ in out] == ['info', 'debug', 'info']
    assert out[2][1].startswith('forward: crown holders @ block=3 | no lane held | 1 empty lanes')

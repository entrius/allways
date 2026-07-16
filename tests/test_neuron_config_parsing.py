"""Canary: entrypoint CLI flags must reach the neuron config. bittensor >= 10.5 silently drops
argv unless BT_NO_PARSE_CLI_ARGS=false — if an SDK bump ever breaks parsing again, this fails
the suite instead of crash-looping every deployed neuron."""

import sys

from allways.utils import config as cfgmod


class _MinerLike:
    @classmethod
    def add_args(cls, parser):
        cfgmod.add_args(cls, parser)
        cfgmod.add_miner_args(cls, parser)


def test_cli_flags_reach_neuron_config(monkeypatch):
    monkeypatch.delenv('BT_NO_PARSE_CLI_ARGS', raising=False)
    monkeypatch.setattr(
        sys,
        'argv',
        ['miner.py', '--netuid', '19', '--wallet.name', 'w', '--wallet.hotkey', 'h', '--axon.port', '8091'],
    )

    cfg = cfgmod.config(_MinerLike)

    assert cfg.netuid == 19
    assert cfg.neuron.name == 'miner'
    assert cfg.wallet.name == 'w'
    assert cfg.wallet.hotkey == 'h'
    assert cfg.axon.port == 8091

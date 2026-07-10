import numpy as np

from neurons.base.validator import BaseValidatorNeuron


class _ConcreteValidator(BaseValidatorNeuron):
    def forward(self):  # satisfy the abstract method; never called here
        pass


class _FakeMetagraph:
    """Minimal stand-in: deepcopy-able, exposes the attrs resync_metagraph touches."""

    def __init__(self, hotkeys):
        self.hotkeys = list(hotkeys)
        self.axons = list(range(len(hotkeys)))
        self.n = len(hotkeys)

    def sync(self, subtensor=None):
        # Mutate axons so the pre-sync deepcopy differs → resync proceeds past the early-return.
        self.axons = ['changed']


def _fake_validator(prev_hotkeys, synced_hotkeys):
    v = _ConcreteValidator.__new__(_ConcreteValidator)
    v.metagraph = _FakeMetagraph(synced_hotkeys)
    v.subtensor = None
    v.hotkeys = list(prev_hotkeys)
    v.scores = np.zeros(len(prev_hotkeys), dtype=np.float32)
    return v


def test_resync_metagraph_handles_shrunk_metagraph():
    # Previous run cached 3 hotkeys; the re-synced metagraph has 1 — pre-fix this overran metagraph.hotkeys[uid].
    v = _fake_validator(['a', 'b', 'c'], ['a'])
    BaseValidatorNeuron.resync_metagraph(v)  # must not raise IndexError
    assert v.hotkeys == ['a']


def test_resync_metagraph_handles_grown_metagraph():
    v = _fake_validator(['a'], ['a', 'b', 'c'])
    BaseValidatorNeuron.resync_metagraph(v)
    assert v.hotkeys == ['a', 'b', 'c']
    assert len(v.scores) == 3

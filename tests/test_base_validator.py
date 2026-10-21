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


def _load(tmp_path, saved_hotkeys, saved_scores, live_hotkeys):
    np.savez(tmp_path / 'state.npz', step=7, scores=np.array(saved_scores, dtype=np.float32), hotkeys=saved_hotkeys)
    v = _fake_validator(live_hotkeys, live_hotkeys)
    v.config = type('C', (), {'neuron': type('N', (), {'full_path': str(tmp_path)})()})()
    v.step = 0
    BaseValidatorNeuron.load_state(v)
    return v


def test_load_state_fits_a_smaller_saved_state_to_a_grown_metagraph(tmp_path):
    # Saved under 2 uids, the subnet has 4 now: resync_metagraph never reconciles when the axons are unchanged.
    v = _load(tmp_path, ['a', 'b'], [0.25, 0.75], ['a', 'b', 'c', 'd'])
    assert v.step == 7
    assert v.hotkeys == ['a', 'b', 'c', 'd']
    assert v.scores.tolist() == [0.25, 0.75, 0.0, 0.0]


def test_load_state_zeroes_a_uid_whose_hotkey_changed_and_trims_a_shrunk_metagraph(tmp_path):
    v = _load(tmp_path, ['a', 'b', 'c'], [0.5, 0.25, 0.25], ['a', 'x'])
    assert v.hotkeys == ['a', 'x']
    assert v.scores.tolist() == [0.5, 0.0]


def test_load_state_keeps_a_matching_state_as_saved(tmp_path):
    v = _load(tmp_path, ['a', 'b'], [0.5, 0.5], ['a', 'b'])
    assert v.scores.tolist() == [0.5, 0.5] and v.hotkeys == ['a', 'b']

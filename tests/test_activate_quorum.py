"""broadcast_until_quorum — activation must not wait out dead validators once quorum is met."""

import asyncio
import time

from allways.cli.dendrite_lite import broadcast_until_quorum


class _FakeSynapse:
    def __init__(self, accepted=None):
        self.accepted = accepted

    def model_copy(self):
        return _FakeSynapse(self.accepted)


class _FakeDendrite:
    """Scripted per-axon behavior: 'accept' / 'reject' answer instantly, 'hang' sleeps forever."""

    def __init__(self, behaviors):
        self.behaviors = behaviors
        self.cancelled = 0

    async def call(self, target_axon, synapse, timeout, deserialize):
        behavior = self.behaviors[target_axon]
        if behavior == 'hang':
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                self.cancelled += 1
                raise
        if behavior == 'raise':
            raise ConnectionError('boom')
        synapse.accepted = behavior == 'accept'
        return synapse


def test_quorum_of_one_returns_without_waiting_for_hung_validators():
    dendrite = _FakeDendrite({0: 'hang', 1: 'accept', 2: 'hang', 3: 'hang'})
    start = time.monotonic()
    responses = broadcast_until_quorum(dendrite, [0, 1, 2, 3], _FakeSynapse(), needed=1, timeout=3600)
    assert time.monotonic() - start < 5
    assert sum(bool(r.accepted) for r in responses) == 1
    assert dendrite.cancelled == 3


def test_waits_for_as_many_accepts_as_the_headcount_needs():
    dendrite = _FakeDendrite({0: 'accept', 1: 'accept', 2: 'hang'})
    responses = broadcast_until_quorum(dendrite, [0, 1, 2], _FakeSynapse(), needed=2, timeout=3600)
    assert sum(bool(r.accepted) for r in responses) == 2
    assert dendrite.cancelled == 1


def test_no_accepts_still_collects_every_response_for_the_rejection_report():
    dendrite = _FakeDendrite({0: 'reject', 1: 'reject', 2: 'reject'})
    responses = broadcast_until_quorum(dendrite, [0, 1, 2], _FakeSynapse(), needed=1, timeout=3600)
    assert len(responses) == 3
    assert not any(r.accepted for r in responses)


def test_a_call_that_raises_is_dropped_not_fatal():
    dendrite = _FakeDendrite({0: 'raise', 1: 'accept'})
    responses = broadcast_until_quorum(dendrite, [0, 1], _FakeSynapse(), needed=1, timeout=3600)
    assert sum(bool(r.accepted) for r in responses) == 1


def test_zero_needed_is_clamped_so_the_broadcast_still_asks_someone():
    dendrite = _FakeDendrite({0: 'accept', 1: 'hang'})
    responses = broadcast_until_quorum(dendrite, [0, 1], _FakeSynapse(), needed=0, timeout=3600)
    assert sum(bool(r.accepted) for r in responses) == 1


def test_repeated_broadcasts_reuse_a_working_loop():
    dendrite = _FakeDendrite({0: 'accept'})
    for _ in range(3):
        responses = broadcast_until_quorum(dendrite, [0], _FakeSynapse(), needed=1, timeout=3600)
        assert responses[0].accepted

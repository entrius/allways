"""Unit tests for the block-aligned stake-weight vote (validator/weights_vote.py).

No chain: a fake client captures vote_set_weights calls; build_attribution is patched to a
canned pubkey→hotkey map. Covers vector derivation edges (buckets, floor, unbound,
off-metagraph), every gate in the step (epoch memo, retry throttle, landed-this-epoch,
whitelist, unchanged vector, watch mode, open-round dedup), and error swallowing.
"""

from types import SimpleNamespace

import pytest
from solders.keypair import Keypair

from allways.constants import WEIGHTS_VOTE_INTERVAL_BLOCKS, WEIGHTS_VOTE_RETRY_SECS
from allways.validator import weights_vote
from allways.validator.weights_vote import derive_weight_vector, maybe_vote_weights

NOW = 1_700_000_000
# 10 blocks past the epoch-5 boundary — boundary_time = NOW - 120.
BLOCK = WEIGHTS_VOTE_INTERVAL_BLOCKS * 5 + 10


def _vali(key, weight=1):
    return SimpleNamespace(key=bytes(key), weight=weight)


def _metagraph(hotkeys, alphas):
    return SimpleNamespace(hotkeys=hotkeys, alpha_stake=alphas)


class FakeClient:
    def __init__(self, config, vote_round=None, vote_error=None):
        self.keypair = Keypair()
        self.config = config
        self.vote_round = vote_round
        self.vote_error = vote_error
        self.voted = []
        self.config_reads = 0

    def get_config(self):
        self.config_reads += 1
        return self.config

    def get_vote_round(self, req_type, target=None):
        return self.vote_round

    def vote_set_weights(self, weights):
        if self.vote_error is not None:
            raise self.vote_error
        self.voted.append(weights)
        return 'S' * 64


def _validator(client, metagraph=None, read_only=False, block=BLOCK):
    return SimpleNamespace(
        block=block,
        solana_client=client,
        solana_swap_loop=SimpleNamespace(read_only=read_only),
        metagraph=metagraph or _metagraph([], []),
        weights_epoch_done=None,
        last_weights_attempt=0,
        weights_whitelist_warned=False,
    )


@pytest.fixture
def patch_attribution(monkeypatch):
    def apply(mapping):
        monkeypatch.setattr(weights_vote, 'build_attribution', lambda _client: mapping)

    return apply


# ---------- derive_weight_vector ----------


def test_derive_buckets_floor_and_alignment():
    keys = [Keypair().pubkey() for _ in range(4)]
    validators = [_vali(k) for k in keys]
    attribution = {str(keys[0]): 'hkA', str(keys[1]): 'hkB', str(keys[2]): 'hkC'}  # keys[3] unbound
    mg = _metagraph(['hkA', 'hkB', 'hkC'], [178_000.0, 49_999.0, 50_000.0])
    assert derive_weight_vector(validators, attribution, mg) == [3, 0, 1, 0]


def test_derive_hotkey_off_metagraph_is_zero():
    key = Keypair().pubkey()
    mg = _metagraph(['other-hk'], [999_999.0])
    assert derive_weight_vector([_vali(key)], {str(key): 'gone-hk'}, mg) == [0]


# ---------- maybe_vote_weights gates ----------


def _whitelisted_setup(patch_attribution, stake=178_000.0, onchain_weight=1, last_update=0, **kw):
    """One whitelisted validator (this client), bound to a hotkey with `stake` alpha."""
    client = FakeClient(config=None, **kw)
    me = client.keypair.pubkey()
    client.config = SimpleNamespace(validators=[_vali(me, onchain_weight)], last_weights_update=last_update)
    patch_attribution({str(me): 'hk'})
    vali = _validator(client, _metagraph(['hk'], [stake]))
    return client, vali


def test_bootstrap_votes_immediately(patch_attribution):
    client, vali = _whitelisted_setup(patch_attribution)
    maybe_vote_weights(vali, NOW)
    assert client.voted == [[3]]
    assert vali.weights_epoch_done is None  # done only once the quorum lands on-chain


def test_epoch_memo_skips_without_rpc(patch_attribution):
    client, vali = _whitelisted_setup(patch_attribution)
    vali.weights_epoch_done = BLOCK // WEIGHTS_VOTE_INTERVAL_BLOCKS
    maybe_vote_weights(vali, NOW)
    assert client.config_reads == 0 and client.voted == []


def test_retry_throttle_skips(patch_attribution):
    client, vali = _whitelisted_setup(patch_attribution)
    vali.last_weights_attempt = NOW - WEIGHTS_VOTE_RETRY_SECS + 1
    maybe_vote_weights(vali, NOW)
    assert client.config_reads == 0 and client.voted == []


def test_landed_this_epoch_marks_done(patch_attribution):
    client, vali = _whitelisted_setup(patch_attribution, last_update=NOW - 60)  # after the boundary (NOW-120)
    maybe_vote_weights(vali, NOW)
    assert client.voted == []
    assert vali.weights_epoch_done == BLOCK // WEIGHTS_VOTE_INTERVAL_BLOCKS


def test_stale_update_before_boundary_is_due(patch_attribution):
    client, vali = _whitelisted_setup(patch_attribution, last_update=NOW - 500)  # before the boundary
    maybe_vote_weights(vali, NOW)
    assert client.voted == [[3]]


def test_not_whitelisted_warns_once_and_skips(patch_attribution):
    client = FakeClient(config=SimpleNamespace(validators=[_vali(Keypair().pubkey())], last_weights_update=0))
    patch_attribution({})
    vali = _validator(client)
    maybe_vote_weights(vali, NOW)
    assert client.voted == []
    assert vali.weights_whitelist_warned
    assert vali.weights_epoch_done == BLOCK // WEIGHTS_VOTE_INTERVAL_BLOCKS


def test_unchanged_vector_skips(patch_attribution):
    client, vali = _whitelisted_setup(patch_attribution, onchain_weight=3)  # derived == on-chain
    maybe_vote_weights(vali, NOW)
    assert client.voted == []
    assert vali.weights_epoch_done == BLOCK // WEIGHTS_VOTE_INTERVAL_BLOCKS


def test_watch_mode_never_votes(patch_attribution):
    client, vali = _whitelisted_setup(patch_attribution)
    vali.solana_swap_loop.read_only = True
    maybe_vote_weights(vali, NOW)
    assert client.voted == []
    assert vali.weights_epoch_done == BLOCK // WEIGHTS_VOTE_INTERVAL_BLOCKS


def test_open_round_vote_dedup(patch_attribution):
    client, vali = _whitelisted_setup(patch_attribution)
    client.vote_round = SimpleNamespace(voters=[bytes(client.keypair.pubkey())], created_at=NOW - 10)
    maybe_vote_weights(vali, NOW)
    assert client.voted == []


def test_stale_round_votes_again(patch_attribution):
    client, vali = _whitelisted_setup(patch_attribution)
    client.vote_round = SimpleNamespace(voters=[bytes(client.keypair.pubkey())], created_at=NOW - 2_000)
    maybe_vote_weights(vali, NOW)
    assert client.voted == [[3]]


# ---------- error handling ----------


def test_benign_contract_error_swallowed(patch_attribution):
    client, vali = _whitelisted_setup(patch_attribution, vote_error=Exception('custom program error: VoteHashMismatch'))
    maybe_vote_weights(vali, NOW)  # must not raise
    assert vali.weights_epoch_done is None  # retried next throttled attempt


def test_unexpected_error_never_raises(patch_attribution):
    client, vali = _whitelisted_setup(patch_attribution, vote_error=RuntimeError('rpc exploded'))
    maybe_vote_weights(vali, NOW)  # must not raise
    assert vali.weights_epoch_done is None

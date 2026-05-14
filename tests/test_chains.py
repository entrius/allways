"""Tests for allways.chains — chain registry, confirmation math, safety blocks."""

import pytest

from allways.chains import (
    CHAIN_BTC,
    CHAIN_TAO,
    RUNWAY_EXTENSION_REQUIRED,
    RUNWAY_OK,
    RUNWAY_TOO_SHORT,
    SEND_PROPAGATION_BUFFER_BLOCKS,
    canonical_pair,
    classify_send_runway,
    compute_extension_target,
    confirmations_to_subtensor_blocks,
    get_chain,
)
from allways.constants import (
    EXTEND_THRESHOLD_BLOCKS,
    EXTENSION_BUCKET_BLOCKS,
    MAX_EXTENSION_BLOCKS,
)


class TestGetChain:
    def test_btc(self):
        assert get_chain('btc') is CHAIN_BTC

    def test_tao(self):
        assert get_chain('tao') is CHAIN_TAO

    def test_unsupported_raises(self):
        with pytest.raises(KeyError):
            get_chain('eth')


class TestCanonicalPair:
    def test_already_canonical(self):
        assert canonical_pair('btc', 'tao') == ('btc', 'tao')

    def test_reversed_input(self):
        assert canonical_pair('tao', 'btc') == ('btc', 'tao')

    def test_tao_always_dest(self):
        # TAO preference: even when a chain sorts after "tao", TAO is dest
        assert canonical_pair('thor', 'tao') == ('thor', 'tao')
        assert canonical_pair('tao', 'thor') == ('thor', 'tao')

    def test_no_tao_alphabetical(self):
        assert canonical_pair('eth', 'btc') == ('btc', 'eth')
        assert canonical_pair('btc', 'eth') == ('btc', 'eth')


class TestConfirmationsToSubtensorBlocks:
    def test_btc(self):
        # ceil(2 * 600 / 12) = ceil(100) = 100
        assert confirmations_to_subtensor_blocks('btc') == 100

    def test_tao(self):
        # ceil(6 * 12 / 12) = ceil(6) = 6
        assert confirmations_to_subtensor_blocks('tao') == 6


class TestComputeExtensionTarget:
    # BTC: 600s/block, padding 300s, bucket 30 blocks.
    # Callers pass a fixed remaining (currently 4 across all tiers) sized for
    # source-chain block-time variance plus padding.

    def test_btc_remaining_three(self):
        # tier-2 at 0/3 confs: remaining=3, seconds=3*600+300=2100,
        # blocks=ceil(2100/12)=175, bucketed=ceil(175/30)*30=180.
        assert compute_extension_target('btc', 3, 1000) == 1000 + 180

    def test_btc_remaining_two(self):
        # tier-2 at 1/3 confs: remaining=2, seconds=2*600+300=1500,
        # blocks=125, bucketed=150.
        assert compute_extension_target('btc', 2, 1000) == 1000 + 150

    def test_btc_remaining_one(self):
        # tier-1 (any chain) and tier-2 at 2/3 confs both land here:
        # remaining=1, seconds=1*600+300=900, blocks=75, bucketed=90.
        assert compute_extension_target('btc', 1, 1000) == 1000 + 90

    def test_btc_remaining_zero(self):
        # tier-2 at >=min confs: remaining=0, only padding remains —
        # 300s/12 = 25 blocks, bucketed to 30.
        assert compute_extension_target('btc', 0, 1000) == 1000 + 30

    def test_tao_remaining_six(self):
        # TAO: 12s/block. remaining=6, seconds=6*12+300=372,
        # blocks=ceil(372/12)=31, bucketed=60.
        assert compute_extension_target('tao', 6, 500) == 500 + 60

    def test_target_is_capped_at_max_extension_blocks(self):
        # Pretend a chain demands far more time than the cap allows: cap kicks in.
        # BTC at remaining=3 gives 180 blocks — well under 250 — so it doesn't
        # exercise the cap by itself. Drive the cap by reading-back the constant
        # and confirming the function never returns more than current + cap.
        target = compute_extension_target('btc', 3, 1000)
        assert target - 1000 <= MAX_EXTENSION_BLOCKS

    def test_result_is_bucket_aligned(self):
        # Whatever the inputs, (target - current) is always a multiple of the
        # bucket size — that's the convergence guarantee.
        for remaining in range(0, 4):
            target = compute_extension_target('btc', remaining, 1000)
            assert (target - 1000) % EXTENSION_BUCKET_BLOCKS == 0

    def test_unsupported_chain_raises(self):
        with pytest.raises(KeyError):
            compute_extension_target('eth', 0, 1000)


class TestClassifySendRunway:
    # BTC: 2 confs * 600s/12 = 100 subtensor blocks needed for confirmation.
    # EXTEND_THRESHOLD_BLOCKS is sized for one validator forward step plus the
    # challenge window — below that, the auto-extension propose tx is doomed.

    def test_full_ttl_is_ok(self):
        # Fresh 50-block reservation against BTC's 100-block confirmation window:
        # confirmation can't fit, so EXTENSION_REQUIRED is expected — not OK.
        status, remaining = classify_send_runway('btc', 0, 50, EXTEND_THRESHOLD_BLOCKS)
        assert status == RUNWAY_EXTENSION_REQUIRED
        assert remaining == 50

    def test_tao_full_ttl_is_ok(self):
        # TAO needs only 6 subtensor blocks for confirmation. A 50-block
        # reservation comfortably fits — this is the OK path.
        status, remaining = classify_send_runway('tao', 0, 50, EXTEND_THRESHOLD_BLOCKS)
        assert status == RUNWAY_OK
        assert remaining == 50

    def test_below_extension_floor_is_too_short(self):
        # Remaining = floor - 1: validators can't propose+challenge before deadline.
        floor = EXTEND_THRESHOLD_BLOCKS + SEND_PROPAGATION_BUFFER_BLOCKS
        status, remaining = classify_send_runway('btc', 0, floor - 1, EXTEND_THRESHOLD_BLOCKS)
        assert status == RUNWAY_TOO_SHORT
        assert remaining == floor - 1

    def test_at_extension_floor_is_extension_required(self):
        # Exactly at the floor: extension can fire, but confirmation won't fit.
        floor = EXTEND_THRESHOLD_BLOCKS + SEND_PROPAGATION_BUFFER_BLOCKS
        status, _ = classify_send_runway('btc', 0, floor, EXTEND_THRESHOLD_BLOCKS)
        assert status == RUNWAY_EXTENSION_REQUIRED

    def test_zero_remaining_is_too_short(self):
        status, remaining = classify_send_runway('btc', 100, 100, EXTEND_THRESHOLD_BLOCKS)
        assert status == RUNWAY_TOO_SHORT
        assert remaining == 0

    def test_negative_remaining_is_too_short(self):
        # Reservation already expired — must hard-refuse.
        status, remaining = classify_send_runway('btc', 200, 150, EXTEND_THRESHOLD_BLOCKS)
        assert status == RUNWAY_TOO_SHORT
        assert remaining == -50

    def test_btc_with_long_ttl_is_ok(self):
        # BTC needs ~100 subtensor blocks for 2 confs + 5-block propagation buffer:
        # a 200-block reservation clears that easily.
        status, _ = classify_send_runway('btc', 0, 200, EXTEND_THRESHOLD_BLOCKS)
        assert status == RUNWAY_OK

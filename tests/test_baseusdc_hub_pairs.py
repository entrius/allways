"""baseusdc as the third literal hub: pair validity, anchors, backings, and the four-family pool split."""

import pytest

from allways.chains import canonical_pair
from allways.constants import (
    DIRECTION_POOLS,
    FAMILY_PAIR_COUNTS,
    HUB_CHAINS,
    LAUNCH_ALPHAS,
    LAUNCH_PAIRS,
    MINER_POOL_SHARE,
    declarable_backings,
    hub_leg,
    is_same_peg,
    scoring_family,
)
from allways.miner.optimizer.quote_optimizer import default_lanes
from allways.solana.pdas import BACKING_BITS
from allways.validator.scoring import compute_direction_pools, family_notional

USDC_SPOKES = ('arbusdc', 'ethusdc', 'polusdc', 'solusdc', 'avaxusdc', 'hypeusdc')
BASEUSDC_SPOKES = ('btc', 'eth', 'hype', 'bnb', 'avax', 'cro', 'aster', 'uni', 'qnt', 'pol', 'paxg')


class TestPairRegistry:
    def test_hub_order(self):
        assert HUB_CHAINS == ('sol', 'tao', 'baseusdc')

    def test_pair_counts(self):
        assert len(LAUNCH_PAIRS) == 2480
        assert len(set(LAUNCH_PAIRS)) == len(LAUNCH_PAIRS)
        assert FAMILY_PAIR_COUNTS == {'sol': 19, 'tao': 18, 'alpha': 2432, 'baseusdc': 11}
        assert {pair for pair in LAUNCH_PAIRS if scoring_family(*pair) == 'baseusdc'} == {
            ('baseusdc', spoke) for spoke in BASEUSDC_SPOKES
        }

    def test_every_pair_is_keyed_under_its_anchor(self):
        assert all(pair == canonical_pair(*pair) and pair[0] == hub_leg(*pair) for pair in LAUNCH_PAIRS)

    def test_usdc_to_usdc_is_invalid(self):
        for spoke in USDC_SPOKES:
            assert is_same_peg('baseusdc', spoke) and is_same_peg(spoke, 'baseusdc')
            assert hub_leg('baseusdc', spoke) is None and hub_leg(spoke, 'baseusdc') is None
            assert declarable_backings('baseusdc', spoke) == []
            assert ('baseusdc', spoke) not in LAUNCH_PAIRS and (spoke, 'baseusdc') not in LAUNCH_PAIRS
        # A USDC spoke still pairs with the other hubs and with alphas.
        assert hub_leg('sol', 'arbusdc') == 'sol' and hub_leg('sn7', 'arbusdc') == 'sn7'
        assert not is_same_peg('sol', 'baseusdc') and not is_same_peg('sn7', 'baseusdc')


class TestAnchors:
    def test_hub_hub_pairs_keep_their_earlier_hub(self):
        assert hub_leg('baseusdc', 'sol') == 'sol' and canonical_pair('baseusdc', 'sol') == ('sol', 'baseusdc')
        assert hub_leg('baseusdc', 'tao') == 'tao' and canonical_pair('baseusdc', 'tao') == ('tao', 'baseusdc')
        assert scoring_family('sol', 'baseusdc') == 'sol' and scoring_family('tao', 'baseusdc') == 'tao'

    def test_alpha_baseusdc_stays_alpha_anchored(self):
        for alpha in ('sn1', 'sn7', 'sn128'):
            assert hub_leg('baseusdc', alpha) == alpha
            assert canonical_pair('baseusdc', alpha) == (alpha, 'baseusdc')
            assert scoring_family(alpha, 'baseusdc') == 'alpha'
        assert all((alpha, 'baseusdc') in LAUNCH_PAIRS for alpha in LAUNCH_ALPHAS)

    def test_baseusdc_anchors_its_spokes(self):
        for spoke in BASEUSDC_SPOKES:
            assert hub_leg(spoke, 'baseusdc') == 'baseusdc'
            assert canonical_pair(spoke, 'baseusdc') == ('baseusdc', spoke)


class TestDeclarableBackings:
    @pytest.mark.parametrize(
        'pair, backings',
        [
            (('sn7', 'baseusdc'), ['baseusdc']),
            (('baseusdc', 'sn7'), ['baseusdc']),
            (('sn7', 'avax'), ['tao']),
            (('sol', 'sn7'), ['tao']),
            (('sn7', 'arbusdc'), ['tao']),
            (('baseusdc', 'btc'), ['baseusdc']),
            (('paxg', 'baseusdc'), ['baseusdc']),
            (('sol', 'baseusdc'), ['sol', 'baseusdc']),
            (('baseusdc', 'tao'), ['tao', 'baseusdc']),
            (('sol', 'tao'), ['sol', 'tao']),
            (('baseusdc', 'ethusdc'), []),
        ],
    )
    def test_per_pair_shape(self, pair, backings):
        assert declarable_backings(*pair) == backings

    def test_baseusdc_stays_dark(self):
        assert 'baseusdc' not in BACKING_BITS
        assert all('baseusdc' not in lane[:2] for lane in default_lanes())


class TestFourFamilies:
    FAMILY = MINER_POOL_SHARE / 4

    @staticmethod
    def family_total(pools, family):
        return sum(v for (f, t, _b), v in pools.items() if scoring_family(f, t) == family)

    def test_silent_network_splits_four_ways(self):
        assert sum(DIRECTION_POOLS.values()) == pytest.approx(MINER_POOL_SHARE)
        pools = compute_direction_pools({})
        for family in ('sol', 'tao', 'alpha', 'baseusdc'):
            assert self.family_total(pools, family) == pytest.approx(self.FAMILY)
        assert sum(pools.values()) == pytest.approx(MINER_POOL_SHARE)

    def test_live_families_split_four_ways(self):
        pools = compute_direction_pools(
            {
                ('btc', 'sol'): {'a': (1, 10**12)},
                ('tao', 'eth'): {'b': (5, 10**9)},
                ('sol', 'sn7'): {'c': (1, 1, 10)},
                ('btc', 'baseusdc'): {'d': (10**8, 50 * 10**6)},
            }
        )
        for family in ('sol', 'tao', 'alpha', 'baseusdc'):
            assert self.family_total(pools, family) == pytest.approx(self.FAMILY)
        assert pools[('btc', 'baseusdc', 'baseusdc')] == pytest.approx(self.FAMILY / 2)
        assert sum(pools.values()) == pytest.approx(MINER_POOL_SHARE)


class TestUsdcBackedAlphaVolume:
    def test_usdc_backed_alpha_lane_scores_no_volume(self):
        # sums = (from, to, collateral); the collateral here would be µUSDC, not rao.
        assert family_notional('sn7', 'baseusdc', (10**9, 10**6, 10**6)) == 0
        assert family_notional('baseusdc', 'sn7', (10**6, 10**9, 10**6)) == 0
        assert family_notional('sn7', 'avax', (10**9, 10**18, 7)) == 7

    def test_it_never_makes_its_pair_live(self):
        live = {('btc', 'sol'): {'a': (1, 10**12)}}
        pools = compute_direction_pools({**live, ('sn7', 'baseusdc'): {'b': (10**9, 10**6, 10**12)}})
        assert pools == compute_direction_pools(live)
        assert pools[('sn7', 'baseusdc', 'baseusdc')] == 0.0

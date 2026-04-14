from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from allways.classes import MinerPair
from allways.constants import (
    COMMITMENT_POLL_INTERVAL_BLOCKS,
    MIN_COLLATERAL_REFRESH_INTERVAL_BLOCKS,
    RATE_UPDATE_MIN_INTERVAL_BLOCKS,
)
from allways.validator.forward import _poll_commitments, _refresh_min_collateral
from allways.validator.rate_state import RateStateStore


def _make_pair(hotkey: str, rate: float, counter_rate: float, source='tao', dest='btc') -> MinerPair:
    return MinerPair(
        uid=0,
        hotkey=hotkey,
        source_chain=source,
        source_address='src_addr',
        dest_chain=dest,
        dest_address='dst_addr',
        rate=rate,
        rate_str=str(rate),
        counter_rate=counter_rate,
        counter_rate_str=str(counter_rate),
    )


def _make_validator(tmp_path: Path, hotkeys=None) -> SimpleNamespace:
    """Construct a validator stub with the fields used by _poll_commitments."""
    store = RateStateStore(db_path=tmp_path / 'rate_state.db')
    metagraph = SimpleNamespace(hotkeys=list(hotkeys or ['hk_a', 'hk_b']))
    config = SimpleNamespace(netuid=2)
    return SimpleNamespace(
        block=1000,
        subtensor=MagicMock(),
        config=config,
        metagraph=metagraph,
        rate_state_store=store,
        contract_client=MagicMock(),
        _last_known_rates={},
        _last_commitment_poll_block=0,
        _min_collateral_rao=0,
        _last_min_collateral_refresh_block=0,
    )


class TestPollCommitmentsBasic:
    def test_first_poll_inserts_both_directions_per_miner(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        pairs = [
            _make_pair('hk_a', rate=0.00015, counter_rate=6500.0),
            _make_pair('hk_b', rate=0.00016, counter_rate=6400.0),
        ]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            _poll_commitments(v)

        # 4 rate_events: 2 miners * 2 directions
        tao_btc = v.rate_state_store.get_rate_events_in_range('tao', 'btc', 0, 2000)
        btc_tao = v.rate_state_store.get_rate_events_in_range('btc', 'tao', 0, 2000)
        assert len(tao_btc) == 2
        assert len(btc_tao) == 2
        assert v._last_commitment_poll_block == v.block
        assert v._last_known_rates == {
            ('hk_a', 'tao', 'btc'): 0.00015,
            ('hk_a', 'btc', 'tao'): 6500.0,
            ('hk_b', 'tao', 'btc'): 0.00016,
            ('hk_b', 'btc', 'tao'): 6400.0,
        }
        v.rate_state_store.close()

    def test_poll_within_interval_is_noop(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        v._last_commitment_poll_block = v.block - (COMMITMENT_POLL_INTERVAL_BLOCKS - 1)

        mock_read = MagicMock(return_value=[])
        with patch('allways.validator.forward.read_miner_commitments', mock_read):
            _poll_commitments(v)

        mock_read.assert_not_called()
        v.rate_state_store.close()


class TestPollCommitmentsChanges:
    def test_no_changes_across_polls_inserts_nothing_extra(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        pairs = [_make_pair('hk_a', rate=0.00015, counter_rate=6500.0)]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            _poll_commitments(v)

        v.block += COMMITMENT_POLL_INTERVAL_BLOCKS
        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            _poll_commitments(v)

        tao_btc = v.rate_state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        assert len(tao_btc) == 1
        v.rate_state_store.close()

    def test_rate_change_inserts_new_event_past_throttle(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        pairs_v1 = [_make_pair('hk_a', rate=0.00015, counter_rate=6500.0)]
        pairs_v2 = [_make_pair('hk_a', rate=0.00020, counter_rate=6500.0)]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs_v1):
            _poll_commitments(v)

        # Advance past both the poll interval AND the rate throttle
        v.block += RATE_UPDATE_MIN_INTERVAL_BLOCKS
        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs_v2):
            _poll_commitments(v)

        tao_btc = v.rate_state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        btc_tao = v.rate_state_store.get_rate_events_in_range('btc', 'tao', 0, 10_000)
        assert [e['rate'] for e in tao_btc] == [0.00015, 0.00020]
        # counter rate unchanged → still only one event
        assert [e['rate'] for e in btc_tao] == [6500.0]
        v.rate_state_store.close()

    def test_rate_change_blocked_by_throttle_updates_cache_on_next_success(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        pairs_v1 = [_make_pair('hk_a', rate=0.00015, counter_rate=0.0)]
        pairs_v2 = [_make_pair('hk_a', rate=0.00020, counter_rate=0.0)]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs_v1):
            _poll_commitments(v)

        # Only past poll interval, NOT past rate throttle
        v.block += COMMITMENT_POLL_INTERVAL_BLOCKS
        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs_v2):
            _poll_commitments(v)

        tao_btc = v.rate_state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        # Throttle blocked the insert
        assert [e['rate'] for e in tao_btc] == [0.00015]
        # Cache keeps the last accepted rate so we don't re-try every poll
        assert v._last_known_rates[('hk_a', 'tao', 'btc')] == 0.00015
        v.rate_state_store.close()


class TestPollCommitmentsZeroRate:
    def test_zero_rate_skips_only_that_direction(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        pairs = [_make_pair('hk_a', rate=0.00015, counter_rate=0.0)]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            _poll_commitments(v)

        tao_btc = v.rate_state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        btc_tao = v.rate_state_store.get_rate_events_in_range('btc', 'tao', 0, 10_000)
        assert len(tao_btc) == 1
        assert len(btc_tao) == 0
        assert ('hk_a', 'tao', 'btc') in v._last_known_rates
        assert ('hk_a', 'btc', 'tao') not in v._last_known_rates
        v.rate_state_store.close()


class TestPollCommitmentsDereg:
    def test_dereg_removes_hotkey_from_store_and_cache(self, tmp_path: Path):
        v = _make_validator(tmp_path, hotkeys=['hk_a', 'hk_b'])
        pairs = [
            _make_pair('hk_a', rate=0.00015, counter_rate=6500.0),
            _make_pair('hk_b', rate=0.00016, counter_rate=6400.0),
        ]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            _poll_commitments(v)

        # hk_b deregistered
        v.metagraph.hotkeys = ['hk_a']
        v.block += COMMITMENT_POLL_INTERVAL_BLOCKS
        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            _poll_commitments(v)

        assert v.rate_state_store.get_latest_rate_before('hk_b', 'tao', 'btc', block=10_000) is None
        assert v.rate_state_store.get_latest_rate_before('hk_a', 'tao', 'btc', block=10_000) is not None
        assert all(k[0] != 'hk_b' for k in v._last_known_rates.keys())
        v.rate_state_store.close()


class TestPollCommitmentsErrors:
    def test_read_raises_logs_and_returns_cleanly(self, tmp_path: Path):
        v = _make_validator(tmp_path)

        def _raise(*args, **kwargs):
            raise RuntimeError('websocket dead')

        with patch('allways.validator.forward.read_miner_commitments', side_effect=_raise):
            _poll_commitments(v)

        # No events, but the poll block WAS advanced so we don't hot-retry
        assert v.rate_state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000) == []
        assert v._last_commitment_poll_block == v.block
        v.rate_state_store.close()


class TestRefreshMinCollateral:
    def test_within_interval_is_noop(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        v._min_collateral_rao = 500_000_000
        v._last_min_collateral_refresh_block = v.block - (MIN_COLLATERAL_REFRESH_INTERVAL_BLOCKS - 1)
        v.contract_client.get_min_collateral.return_value = 400_000_000

        _refresh_min_collateral(v)

        v.contract_client.get_min_collateral.assert_not_called()
        assert v._min_collateral_rao == 500_000_000
        v.rate_state_store.close()

    def test_after_interval_updates_cached_value(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        v._min_collateral_rao = 500_000_000
        v._last_min_collateral_refresh_block = v.block - MIN_COLLATERAL_REFRESH_INTERVAL_BLOCKS
        v.contract_client.get_min_collateral.return_value = 400_000_000

        _refresh_min_collateral(v)

        assert v._min_collateral_rao == 400_000_000
        assert v._last_min_collateral_refresh_block == v.block
        v.rate_state_store.close()

    def test_exception_preserves_state(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        v._min_collateral_rao = 500_000_000
        prior_refresh_block = v.block - MIN_COLLATERAL_REFRESH_INTERVAL_BLOCKS
        v._last_min_collateral_refresh_block = prior_refresh_block
        v.contract_client.get_min_collateral.side_effect = RuntimeError('rpc down')

        _refresh_min_collateral(v)

        assert v._min_collateral_rao == 500_000_000
        assert v._last_min_collateral_refresh_block == prior_refresh_block
        v.rate_state_store.close()

    def test_unchanged_value_still_advances_refresh_block(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        v._min_collateral_rao = 500_000_000
        v._last_min_collateral_refresh_block = v.block - MIN_COLLATERAL_REFRESH_INTERVAL_BLOCKS
        v.contract_client.get_min_collateral.return_value = 500_000_000

        _refresh_min_collateral(v)

        assert v._min_collateral_rao == 500_000_000
        assert v._last_min_collateral_refresh_block == v.block
        v.rate_state_store.close()

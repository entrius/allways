from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from allways.classes import MinerPair
from allways.constants import (
    COMMITMENT_POLL_INTERVAL_BLOCKS,
    EVENT_RETENTION_BLOCKS,
    RATE_UPDATE_MIN_INTERVAL_BLOCKS,
)
from allways.validator.forward import _poll_commitments
from allways.validator.state_store import ValidatorStateStore


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
    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    metagraph = SimpleNamespace(hotkeys=list(hotkeys or ['hk_a', 'hk_b']))
    config = SimpleNamespace(netuid=2)
    return SimpleNamespace(
        block=1000,
        subtensor=MagicMock(),
        config=config,
        metagraph=metagraph,
        state_store=store,
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
        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 2000)
        btc_tao = v.state_store.get_rate_events_in_range('btc', 'tao', 0, 2000)
        assert len(tao_btc) == 2
        assert len(btc_tao) == 2
        assert v._last_commitment_poll_block == v.block
        assert v._last_known_rates == {
            ('hk_a', 'tao', 'btc'): 0.00015,
            ('hk_a', 'btc', 'tao'): 6500.0,
            ('hk_b', 'tao', 'btc'): 0.00016,
            ('hk_b', 'btc', 'tao'): 6400.0,
        }
        v.state_store.close()

    def test_poll_within_interval_is_noop(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        v._last_commitment_poll_block = v.block - (COMMITMENT_POLL_INTERVAL_BLOCKS - 1)

        mock_read = MagicMock(return_value=[])
        with patch('allways.validator.forward.read_miner_commitments', mock_read):
            _poll_commitments(v)

        mock_read.assert_not_called()
        v.state_store.close()


class TestPollCommitmentsChanges:
    def test_no_changes_across_polls_inserts_nothing_extra(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        pairs = [_make_pair('hk_a', rate=0.00015, counter_rate=6500.0)]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            _poll_commitments(v)

        v.block += COMMITMENT_POLL_INTERVAL_BLOCKS
        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            _poll_commitments(v)

        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        assert len(tao_btc) == 1
        v.state_store.close()

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

        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        btc_tao = v.state_store.get_rate_events_in_range('btc', 'tao', 0, 10_000)
        assert [e['rate'] for e in tao_btc] == [0.00015, 0.00020]
        # counter rate unchanged → still only one event
        assert [e['rate'] for e in btc_tao] == [6500.0]
        v.state_store.close()

    def test_rate_change_blocked_by_throttle_still_advances_cache(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        pairs_v1 = [_make_pair('hk_a', rate=0.00015, counter_rate=0.0)]
        pairs_v2 = [_make_pair('hk_a', rate=0.00020, counter_rate=0.0)]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs_v1):
            _poll_commitments(v)

        # Only past poll interval, NOT past rate throttle.
        v.block += COMMITMENT_POLL_INTERVAL_BLOCKS
        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs_v2):
            _poll_commitments(v)

        # Throttle blocked the store insert — only the first event lands.
        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        assert [e['rate'] for e in tao_btc] == [0.00015]
        # But the in-memory cache advances to the observed value so the next
        # poll doesn't waste an insert attempt on the same throttled rate.
        assert v._last_known_rates[('hk_a', 'tao', 'btc')] == 0.00020
        v.state_store.close()


class TestPollCommitmentsZeroRate:
    def test_zero_rate_skips_only_that_direction(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        pairs = [_make_pair('hk_a', rate=0.00015, counter_rate=0.0)]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            _poll_commitments(v)

        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        btc_tao = v.state_store.get_rate_events_in_range('btc', 'tao', 0, 10_000)
        assert len(tao_btc) == 1
        assert len(btc_tao) == 0
        assert ('hk_a', 'tao', 'btc') in v._last_known_rates
        assert ('hk_a', 'btc', 'tao') not in v._last_known_rates
        v.state_store.close()


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

        assert v.state_store.get_latest_rate_before('hk_b', 'tao', 'btc', block=10_000) is None
        assert v.state_store.get_latest_rate_before('hk_a', 'tao', 'btc', block=10_000) is not None
        assert all(k[0] != 'hk_b' for k in v._last_known_rates.keys())
        v.state_store.close()


class TestPollCommitmentsErrors:
    def test_read_raises_logs_and_returns_cleanly(self, tmp_path: Path):
        v = _make_validator(tmp_path)

        def _raise(*args, **kwargs):
            raise RuntimeError('websocket dead')

        with patch('allways.validator.forward.read_miner_commitments', side_effect=_raise):
            _poll_commitments(v)

        # No events, but the poll block WAS advanced so we don't hot-retry
        assert v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000) == []
        assert v._last_commitment_poll_block == v.block
        v.state_store.close()


class TestPollCommitmentsPruning:
    def test_prune_removes_events_older_than_retention_window(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        # Move the clock forward so the retention cutoff is meaningful.
        v.block = EVENT_RETENTION_BLOCKS + 1_000
        ancient_block = 1  # well before cutoff (v.block - EVENT_RETENTION_BLOCKS = 1000)
        recent_block = v.block - 100  # safely inside retention

        conn = v.state_store._require_connection()
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_ancient', 'tao', 'btc', 0.00010, ancient_block),
        )
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_recent', 'tao', 'btc', 0.00020, recent_block),
        )
        conn.commit()

        with patch('allways.validator.forward.read_miner_commitments', return_value=[]):
            _poll_commitments(v)

        rate_events = v.state_store.get_rate_events_in_range('tao', 'btc', 0, v.block + 1)
        surviving_blocks = {e['block'] for e in rate_events}
        assert ancient_block not in surviving_blocks
        assert recent_block in surviving_blocks
        v.state_store.close()

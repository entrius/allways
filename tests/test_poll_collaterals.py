from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from allways.constants import COLLATERAL_POLL_INTERVAL_BLOCKS
from allways.validator.forward import _poll_collaterals
from allways.validator.rate_state import RateStateStore


def _make_validator(
    tmp_path: Path,
    tracked_hotkeys=None,
    metagraph_hotkeys=None,
) -> SimpleNamespace:
    store = RateStateStore(db_path=tmp_path / 'rate_state.db')
    tracked = tracked_hotkeys if tracked_hotkeys is not None else ['hk_a', 'hk_b']
    metagraph = SimpleNamespace(hotkeys=list(metagraph_hotkeys if metagraph_hotkeys is not None else tracked))
    last_known_rates = {(hk, 'tao', 'btc'): 0.00015 for hk in tracked}
    return SimpleNamespace(
        block=1000,
        metagraph=metagraph,
        rate_state_store=store,
        contract_client=MagicMock(),
        _last_known_rates=last_known_rates,
        _last_known_collaterals={},
        _last_collateral_poll_block=0,
    )


class TestPollCollateralsBasic:
    def test_first_poll_records_each_tracked_miner(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        v.contract_client.get_miner_collateral.side_effect = lambda hk: {
            'hk_a': 500_000_000,
            'hk_b': 600_000_000,
        }[hk]

        _poll_collaterals(v)

        assert v._last_known_collaterals == {'hk_a': 500_000_000, 'hk_b': 600_000_000}
        assert v._last_collateral_poll_block == v.block

        hk_a_latest = v.rate_state_store.get_latest_collateral_before('hk_a', block=2000)
        hk_b_latest = v.rate_state_store.get_latest_collateral_before('hk_b', block=2000)
        assert hk_a_latest == (500_000_000, v.block)
        assert hk_b_latest == (600_000_000, v.block)
        v.rate_state_store.close()

    def test_within_interval_is_noop(self, tmp_path: Path):
        v = _make_validator(tmp_path)
        v._last_collateral_poll_block = v.block - (COLLATERAL_POLL_INTERVAL_BLOCKS - 1)

        _poll_collaterals(v)

        v.contract_client.get_miner_collateral.assert_not_called()
        v.rate_state_store.close()


class TestPollCollateralsChanges:
    def test_unchanged_value_skipped(self, tmp_path: Path):
        v = _make_validator(tmp_path, tracked_hotkeys=['hk_a'])
        v.contract_client.get_miner_collateral.return_value = 500_000_000

        _poll_collaterals(v)
        v.block += COLLATERAL_POLL_INTERVAL_BLOCKS
        _poll_collaterals(v)

        conn = v.rate_state_store._require_connection()
        count = conn.execute('SELECT COUNT(*) FROM collateral_events WHERE hotkey = ?', ('hk_a',)).fetchone()[0]
        assert count == 1
        v.rate_state_store.close()

    def test_changed_value_inserts(self, tmp_path: Path):
        v = _make_validator(tmp_path, tracked_hotkeys=['hk_a'])
        v.contract_client.get_miner_collateral.side_effect = [500_000_000, 400_000_000]

        _poll_collaterals(v)
        v.block += COLLATERAL_POLL_INTERVAL_BLOCKS
        _poll_collaterals(v)

        assert v._last_known_collaterals['hk_a'] == 400_000_000
        conn = v.rate_state_store._require_connection()
        count = conn.execute('SELECT COUNT(*) FROM collateral_events').fetchone()[0]
        assert count == 2
        v.rate_state_store.close()


class TestPollCollateralsErrors:
    def test_exception_skips_miner(self, tmp_path: Path):
        v = _make_validator(tmp_path, tracked_hotkeys=['hk_a', 'hk_b'])

        def _read(hk):
            if hk == 'hk_a':
                raise RuntimeError('rpc down')
            return 600_000_000

        v.contract_client.get_miner_collateral.side_effect = _read

        _poll_collaterals(v)

        # hk_a failed, hk_b succeeded
        assert 'hk_a' not in v._last_known_collaterals
        assert v._last_known_collaterals == {'hk_b': 600_000_000}
        v.rate_state_store.close()


class TestPollCollateralsMembership:
    def test_hotkey_not_in_metagraph_is_skipped(self, tmp_path: Path):
        v = _make_validator(
            tmp_path,
            tracked_hotkeys=['hk_a', 'hk_b'],
            metagraph_hotkeys=['hk_a'],  # hk_b dropped
        )
        v.contract_client.get_miner_collateral.return_value = 500_000_000

        _poll_collaterals(v)

        assert set(v._last_known_collaterals.keys()) == {'hk_a'}
        v.contract_client.get_miner_collateral.assert_called_once_with('hk_a')
        v.rate_state_store.close()

    def test_hotkey_not_in_rate_cache_is_skipped(self, tmp_path: Path):
        v = _make_validator(tmp_path, tracked_hotkeys=['hk_a'])
        # hk_c in metagraph but no rate cache
        v.metagraph.hotkeys = ['hk_a', 'hk_c']
        v.contract_client.get_miner_collateral.return_value = 500_000_000

        _poll_collaterals(v)

        v.contract_client.get_miner_collateral.assert_called_once_with('hk_a')
        v.rate_state_store.close()

    def test_dereg_cleans_collateral_cache(self, tmp_path: Path):
        v = _make_validator(tmp_path, tracked_hotkeys=['hk_a', 'hk_b'])
        v.contract_client.get_miner_collateral.return_value = 500_000_000
        _poll_collaterals(v)
        assert set(v._last_known_collaterals.keys()) == {'hk_a', 'hk_b'}

        # hk_b deregistered
        v.metagraph.hotkeys = ['hk_a']
        v.block += COLLATERAL_POLL_INTERVAL_BLOCKS
        _poll_collaterals(v)

        assert set(v._last_known_collaterals.keys()) == {'hk_a'}
        v.rate_state_store.close()

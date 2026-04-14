"""Unit tests for ContractEventWatcher state application.

These tests exercise the event→state pipeline without hitting a live chain —
events are injected via ``_apply_event`` directly. The substrate-side decode
path is covered by the live dev-env suite.
"""

from pathlib import Path
from unittest.mock import MagicMock

from allways.validator.event_watcher import ContractEventWatcher, load_event_registry
from allways.validator.state_store import ValidatorStateStore

METADATA_PATH = Path(__file__).parent.parent / 'allways' / 'metadata' / 'allways_swap_manager.json'


def _make_watcher(tmp_path: Path) -> ContractEventWatcher:
    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    return ContractEventWatcher(
        substrate=MagicMock(),
        contract_address='5contract',
        metadata_path=METADATA_PATH,
        state_store=store,
    )


class TestRegistryLoad:
    def test_registry_has_expected_events(self):
        registry = load_event_registry(METADATA_PATH)
        names = {e.name for e in registry.values()}
        for expected in (
            'CollateralPosted',
            'CollateralWithdrawn',
            'CollateralSlashed',
            'MinerActivated',
            'SwapCompleted',
            'SwapTimedOut',
            'ConfigUpdated',
        ):
            assert expected in names, f'missing event {expected}'


class TestCollateralDelta:
    def test_posted_increments_collateral(self, tmp_path: Path):
        w = _make_watcher(tmp_path)
        w._apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 500_000_000})
        assert w.collateral['hk_a'] == 500_000_000
        events = w.get_collateral_events_in_range(0, 1000)
        assert len(events) == 1
        assert events[0]['block'] == 100
        w.state_store.close()

    def test_withdrawn_decrements(self, tmp_path: Path):
        w = _make_watcher(tmp_path)
        w._apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 1_000})
        w._apply_event(200, 'CollateralWithdrawn', {'miner': 'hk_a', 'amount': 300})
        assert w.collateral['hk_a'] == 700
        w.state_store.close()

    def test_slashed_decrements_and_floors_at_zero(self, tmp_path: Path):
        w = _make_watcher(tmp_path)
        w._apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 500})
        w._apply_event(200, 'CollateralSlashed', {'miner': 'hk_a', 'amount': 1_000})
        # Slashed for more than we have — floor at 0
        assert w.collateral['hk_a'] == 0
        w.state_store.close()


class TestActiveFlag:
    def test_activation_adds_to_set(self, tmp_path: Path):
        w = _make_watcher(tmp_path)
        w._apply_event(100, 'MinerActivated', {'miner': 'hk_a', 'active': True})
        assert 'hk_a' in w.active_miners
        w._apply_event(200, 'MinerActivated', {'miner': 'hk_a', 'active': False})
        assert 'hk_a' not in w.active_miners
        w.state_store.close()


class TestConfigUpdated:
    def test_min_collateral_config_updates_field(self, tmp_path: Path):
        w = _make_watcher(tmp_path)
        w._apply_event(100, 'ConfigUpdated', {'key': 'min_collateral', 'value': 250_000_000})
        assert w.min_collateral == 250_000_000
        # Unrelated config keys do not affect min_collateral
        w._apply_event(200, 'ConfigUpdated', {'key': 'reservation_ttl', 'value': 1200})
        assert w.min_collateral == 250_000_000
        w.state_store.close()


class TestSwapOutcomePersistence:
    def test_completed_writes_ledger(self, tmp_path: Path):
        w = _make_watcher(tmp_path)
        w._apply_event(100, 'SwapCompleted', {'swap_id': 42, 'miner': 'hk_a'})
        stats = w.state_store.get_all_time_success_rates()
        assert stats['hk_a'] == (1, 0)
        w.state_store.close()

    def test_timed_out_writes_ledger(self, tmp_path: Path):
        w = _make_watcher(tmp_path)
        w._apply_event(100, 'SwapTimedOut', {'swap_id': 42, 'miner': 'hk_a'})
        stats = w.state_store.get_all_time_success_rates()
        assert stats['hk_a'] == (0, 1)
        w.state_store.close()

    def test_mixed_outcomes_counted(self, tmp_path: Path):
        w = _make_watcher(tmp_path)
        w._apply_event(100, 'SwapCompleted', {'swap_id': 1, 'miner': 'hk_a'})
        w._apply_event(101, 'SwapCompleted', {'swap_id': 2, 'miner': 'hk_a'})
        w._apply_event(102, 'SwapTimedOut', {'swap_id': 3, 'miner': 'hk_a'})
        stats = w.state_store.get_all_time_success_rates()
        assert stats['hk_a'] == (2, 1)
        w.state_store.close()


class TestCollateralEventsInRange:
    def test_events_are_block_filtered(self, tmp_path: Path):
        w = _make_watcher(tmp_path)
        w._apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 1_000})
        w._apply_event(200, 'CollateralPosted', {'miner': 'hk_a', 'amount': 2_000})
        w._apply_event(300, 'CollateralPosted', {'miner': 'hk_a', 'amount': 3_000})
        # Range is (start, end]: block 100 is excluded, 200/300 included
        events = w.get_collateral_events_in_range(100, 300)
        assert [e['block'] for e in events] == [200, 300]
        w.state_store.close()

    def test_latest_before_returns_most_recent(self, tmp_path: Path):
        w = _make_watcher(tmp_path)
        w._apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 1_000})
        w._apply_event(200, 'CollateralPosted', {'miner': 'hk_a', 'amount': 500})
        result = w.get_latest_collateral_before('hk_a', block=150)
        assert result is not None
        collateral, block = result
        assert collateral == 1_000
        assert block == 100
        w.state_store.close()

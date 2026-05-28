"""Validator forward — vote_deactivate when active miners fall below min_collateral."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from allways.validator.forward import enforce_under_min_collateral_deactivation


def make_validator(
    *,
    active_miners=frozenset(),
    min_collateral=10_000_000_000,
    collaterals=None,
    active_flags=None,
):
    collaterals = collaterals or {}
    active_flags = active_flags if active_flags is not None else {hk: True for hk in active_miners}

    contract_client = MagicMock()
    contract_client.get_miner_collateral.side_effect = lambda hk: collaterals.get(hk, 0)
    contract_client.get_miner_active_flag.side_effect = lambda hk: active_flags.get(hk, False)
    contract_client.vote_deactivate.return_value = '0xdead'

    bounds_cache = MagicMock()
    bounds_cache.min_collateral.return_value = min_collateral

    event_watcher = MagicMock()
    event_watcher.active_miners = set(active_miners)

    return SimpleNamespace(
        bounds_cache=bounds_cache,
        contract_client=contract_client,
        event_watcher=event_watcher,
        metagraph=SimpleNamespace(hotkeys=['miner-a', 'miner-b']),
        wallet=MagicMock(),
    )


class TestEnforceUnderMinCollateralDeactivation:
    def test_votes_deactivate_for_active_miner_below_floor(self):
        v = make_validator(
            active_miners={'miner-a'},
            min_collateral=10_000_000_000,
            collaterals={'miner-a': 5_000_000_000},
        )

        enforce_under_min_collateral_deactivation(v)

        v.contract_client.vote_deactivate.assert_called_once_with(
            wallet=v.wallet,
            miner_hotkey='miner-a',
        )

    def test_skips_miner_at_or_above_floor(self):
        v = make_validator(
            active_miners={'miner-a'},
            min_collateral=10_000_000_000,
            collaterals={'miner-a': 10_000_000_000},
        )

        enforce_under_min_collateral_deactivation(v)

        v.contract_client.vote_deactivate.assert_not_called()

    def test_skips_when_min_collateral_unset(self):
        v = make_validator(
            active_miners={'miner-a'},
            min_collateral=0,
            collaterals={'miner-a': 1},
        )

        enforce_under_min_collateral_deactivation(v)

        v.bounds_cache.min_collateral.assert_called_once()
        v.contract_client.vote_deactivate.assert_not_called()

    def test_skips_inactive_on_contract_despite_local_active_set(self):
        v = make_validator(
            active_miners={'miner-a'},
            min_collateral=10_000_000_000,
            collaterals={'miner-a': 1},
            active_flags={'miner-a': False},
        )

        enforce_under_min_collateral_deactivation(v)

        v.contract_client.vote_deactivate.assert_not_called()

    def test_checks_each_active_miner(self):
        v = make_validator(
            active_miners={'miner-a', 'miner-b'},
            min_collateral=10_000_000_000,
            collaterals={'miner-a': 5_000_000_000, 'miner-b': 20_000_000_000},
        )

        enforce_under_min_collateral_deactivation(v)

        v.contract_client.vote_deactivate.assert_called_once_with(
            wallet=v.wallet,
            miner_hotkey='miner-a',
        )
        assert {c.args[0] for c in v.contract_client.get_miner_collateral.call_args_list} == {
            'miner-a',
            'miner-b',
        }

    def test_min_collateral_read_failure_is_non_fatal(self):
        v = make_validator(active_miners={'miner-a'})
        v.bounds_cache.min_collateral.side_effect = RuntimeError('rpc down')

        enforce_under_min_collateral_deactivation(v)

        v.contract_client.vote_deactivate.assert_not_called()

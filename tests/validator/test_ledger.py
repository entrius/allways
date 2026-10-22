"""The scoring ledger's anchors: what the per-round reconcile may and may not change, and what prune keeps."""

from types import SimpleNamespace

from allways.classes import ActivityTransition
from allways.constants import EVENT_RETENTION_SECS
from allways.validator.scoring.ledger import ACTIVE, ACTIVITY, COLLATERAL, LEDGER_SINCE, RATE, Ledger
from allways.validator.scoring.rewards import recent_fill_hotkeys
from allways.validator.store.db import Store

LANE = ('btc', 'sol', 'sol')
NOW = 100_000
EARLY = NOW - 5_000  # well outside the reconcile's quiet window


def _ledger():
    return Ledger(Store(':memory:'))


def _rate(ledger, hk='hk', at=NOW):
    return ledger.get_latest_rates_before(*LANE[:2], at, 'sol').get(hk, (None,))[0]


def test_a_lane_whose_quote_left_the_book_is_zeroed():
    ledger = _ledger()
    ledger.put(EARLY, 'hk', RATE, 0.0002, *LANE)
    ledger.reconcile_live_quotes({('other', *LANE): 0.0003}, now=NOW)
    assert _rate(ledger) == 0.0


def test_a_diverged_rate_is_corrected_and_an_unseen_quote_added():
    ledger = _ledger()
    ledger.put(EARLY, 'hk', RATE, 0.0002, *LANE)
    ledger.reconcile_live_quotes({('hk', *LANE): 0.0003, ('new', *LANE): 0.0004}, now=NOW)
    assert _rate(ledger) == 0.0003 and _rate(ledger, 'new') == 0.0004


def test_a_zeroed_quote_is_never_revived_from_the_book():
    # The quote named an address its chain refuses: ingest zeroed it. Only a fresh, judged QuoteSet revives it.
    ledger = _ledger()
    quote = {'from_chain': 'btc', 'to_chain': 'sol', 'collateral_chain': 'sol', 'rate': 2 * 10**14}
    ledger.ingest(SimpleNamespace(name='QuoteSet', block_time=EARLY, fields=quote), 'hk', 600, quote_usable=False)
    ledger.reconcile_live_quotes({('hk', *LANE): 0.0002}, now=NOW)
    assert _rate(ledger) == 0.0


def test_an_empty_book_is_a_failed_read_and_changes_nothing():
    ledger = _ledger()
    ledger.put(EARLY, 'hk', RATE, 0.0002, *LANE)
    ledger.reconcile_live_quotes({}, now=NOW)
    assert _rate(ledger) == 0.0002


def test_live_state_corrects_a_missed_deactivation_and_seeds_a_missing_collateral():
    ledger = _ledger()
    ledger.put(EARLY, 'hk', ACTIVE, 1)
    ledger.reconcile_live_state({'hk': SimpleNamespace(active=False, collateral=7)}, now=NOW, live_bonds={'hk': 0})
    assert 'hk' not in ledger.get_active_miners_at(NOW)
    assert ledger.get_miner_collaterals_at(NOW, backing='sol') == {'hk': 7}
    assert ledger.get_miner_collaterals_at(NOW, backing='tao') == {'hk': 0}  # an unlocked bond backs nothing


def test_prune_keeps_each_keys_latest_state_by_time_not_by_arrival():
    # A live removal arrives first; the older, backfilled quote arrives after it.
    ledger = _ledger()
    ledger.put(2000, 'hk', RATE, 0.0, *LANE)
    ledger.put(1000, 'hk', RATE, 0.0001, *LANE)
    ledger.put(2000, 'hk', COLLATERAL, 0, backing='sol')
    ledger.put(1000, 'hk', COLLATERAL, 10**12, backing='sol')
    ledger.put(2000, 'hk', ACTIVE, 0)
    ledger.put(1000, 'hk', ACTIVE, 1)
    ledger.prune(2000 + EVENT_RETENTION_SECS + 1)
    later = 10**9
    assert _rate(ledger, at=later) == 0.0
    assert ledger.get_miner_collaterals_at(later, backing='sol') == {'hk': 0}
    assert 'hk' not in ledger.get_active_miners_at(later)


def test_a_fill_judged_inside_the_ingest_transaction_reads_the_ledger_age_without_writing():
    # Fill qualification asks the ledger's age from inside the ingest's own transaction.
    ledger = _ledger()
    ledger.put(EARLY, 'hk', ACTIVITY, int(ActivityTransition.RESERVE_START), hub='sol')
    fields = {
        'from_chain': 'btc',
        'to_chain': 'sol',
        'collateral_chain': 'sol',
        'swap_key': b'\x01' * 32,
        'from_amount': 1,
        'to_amount': 2,
        'collateral_amount': 3,
    }

    def held(hotkey, *_lane_and_size):
        assert recent_fill_hotkeys(ledger, NOW) is None  # a fresh ledger reads strikes-only
        return True

    with ledger.store.tx():
        ledger.ingest(SimpleNamespace(name='SwapCompleted', block_time=NOW, fields=fields), 'hk', 600, None, held)
    assert ledger.get_qualified_lane_volumes(0, NOW) != {}
    assert ledger.store.get_meta(LEDGER_SINCE) is None

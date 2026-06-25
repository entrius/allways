"""B3.4 — SolanaEventIndex: persist decoded program events into the state store and expose the crown's
per-instant read interface over them, on the unix-``blockTime`` axis, attributing each Solana pubkey to its
bound hotkey at write time."""

from pathlib import Path

from allways.constants import RATE_PRECISION
from allways.solana.events import EventRecord
from allways.validator.event_index import SolanaEventIndex
from allways.validator.scoring import replay_crown_time_window
from allways.validator.state_store import ValidatorStateStore

# pubkey str -> hotkey ss58 (what binding.build_attribution returns).
ATTR = {'pk_a': 'hk_a', 'pk_b': 'hk_b'}


def rec(name: str, *, miner: str = 'pk_a', block_time, slot: int = 0, **fields) -> EventRecord:
    fields = {'miner': miner, **fields}
    return EventRecord(name=name, fields=fields, slot=slot, block_time=block_time, signature=f'sig{slot}')


def make_store(tmp_path: Path) -> ValidatorStateStore:
    return ValidatorStateStore(db_path=tmp_path / 'state.db')


class TestIngestActive:
    def test_activated_deactivated_build_the_active_series(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        n = idx.ingest(
            [
                rec('MinerActivated', miner='pk_a', block_time=100, at=100),
                rec('MinerDeactivated', miner='pk_a', block_time=500, at=500),
            ],
            ATTR,
        )
        assert n == 2
        assert idx.get_active_miners_at(50) == set()
        assert idx.get_active_miners_at(100) == {'hk_a'}
        assert idx.get_active_miners_at(499) == {'hk_a'}
        assert idx.get_active_miners_at(500) == set()
        # Range is half-open on the left: an event at exactly start is excluded.
        assert [e['block'] for e in idx.get_active_events_in_range(100, 500)] == [500]
        store.close()


class TestIngestBusy:
    def test_swap_lifecycle_drives_busy(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.ingest(
            [
                rec('SwapInitiated', miner='pk_a', block_time=200),
                rec('SwapCompleted', miner='pk_a', block_time=400),
                rec('SwapInitiated', miner='pk_a', block_time=600),
                rec('SwapTimedOut', miner='pk_a', block_time=900),
            ],
            ATTR,
        )
        assert idx.get_busy_miners_at(100) == {}
        assert idx.get_busy_miners_at(200) == {'hk_a': 1}
        assert idx.get_busy_miners_at(400) == {}  # completed nets to 0
        assert idx.get_busy_miners_at(600) == {'hk_a': 1}
        assert idx.get_busy_miners_at(900) == {}  # timed out nets to 0
        deltas = [(e['block'], e['delta']) for e in idx.get_busy_events_in_range(0, 1000)]
        assert deltas == [(200, 1), (400, -1), (600, 1), (900, -1)]
        store.close()


class TestIngestCollateral:
    def test_posted_and_withdrawn_track_total(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.ingest(
            [
                rec('CollateralPosted', miner='pk_a', block_time=100, amount=100_000_000, total=100_000_000),
                rec('CollateralPosted', miner='pk_a', block_time=500, amount=150_000_000, total=250_000_000),
                rec('CollateralWithdrawn', miner='pk_a', block_time=800, amount=200_000_000, total=50_000_000),
            ],
            ATTR,
        )
        assert idx.get_miner_collaterals_at(50) == {}
        assert idx.get_miner_collaterals_at(100) == {'hk_a': 100_000_000}
        assert idx.get_miner_collaterals_at(499) == {'hk_a': 100_000_000}
        assert idx.get_miner_collaterals_at(500) == {'hk_a': 250_000_000}
        assert idx.get_miner_collaterals_at(800) == {'hk_a': 50_000_000}
        store.close()


class TestIngestRate:
    def test_quote_set_converts_fixed_point_to_display(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        # On-chain rate is display × RATE_PRECISION; the index must divide it back.
        idx.ingest(
            [rec('QuoteSet', miner='pk_a', block_time=100, from_chain='btc', to_chain='tao',
                 rate=326 * RATE_PRECISION, liquidity=0)],
            ATTR,
        )
        latest = store.get_latest_rate_before('hk_a', 'btc', 'tao', 200)
        assert latest == (326.0, 100)
        store.close()

    def test_quote_removed_writes_zero_optout(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.ingest(
            [
                rec('QuoteSet', miner='pk_a', block_time=100, from_chain='btc', to_chain='tao',
                    rate=200 * RATE_PRECISION, liquidity=0),
                rec('QuoteRemoved', miner='pk_a', block_time=500, from_chain='btc', to_chain='tao'),
            ],
            ATTR,
        )
        assert store.get_latest_rate_before('hk_a', 'btc', 'tao', 999) == (0.0, 500)
        store.close()

    def test_chain_strings_lowercased(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.ingest(
            [rec('QuoteSet', miner='pk_a', block_time=100, from_chain='BTC', to_chain='TAO',
                 rate=200 * RATE_PRECISION, liquidity=0)],
            ATTR,
        )
        # Stored under the lowercased direction the crown queries by.
        assert store.get_latest_rate_before('hk_a', 'btc', 'tao', 999) == (200.0, 100)
        store.close()


class TestAttributionAndSkips:
    def test_unbound_pubkey_is_dropped(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        # pk_c has no binding → no UID to credit → its event is skipped.
        n = idx.ingest([rec('MinerActivated', miner='pk_c', block_time=100, at=100)], ATTR)
        assert n == 0
        assert idx.get_active_miners_at(100) == set()
        store.close()

    def test_unstamped_tip_tx_is_skipped(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        # block_time None (not-yet-stamped tip tx) → skipped, cursor stays behind it.
        n = idx.ingest([rec('MinerActivated', miner='pk_a', block_time=None, at=0)], ATTR)
        assert n == 0
        assert idx.get_active_miners_at(10_000) == set()
        store.close()

    def test_unknown_event_name_is_ignored(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        n = idx.ingest([rec('PoolResolved', miner='pk_a', block_time=100)], ATTR)
        assert n == 0
        store.close()

    def test_attribution_maps_pubkey_to_bound_hotkey(self, tmp_path: Path):
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.ingest(
            [
                rec('MinerActivated', miner='pk_a', block_time=100, at=100),
                rec('MinerActivated', miner='pk_b', block_time=100, at=100),
            ],
            ATTR,
        )
        # Events keyed on-chain by pubkey land on the bound hotkeys.
        assert idx.get_active_miners_at(100) == {'hk_a', 'hk_b'}
        store.close()


class TestIngestEndToEndCrown:
    def test_ingested_events_drive_the_crown_to_funded_best_rate_holder(self, tmp_path: Path):
        """The full B3.4 path: decode-shaped records → index persistence →
        scoring's crown replay credits the active, funded, best-rate miner."""
        store = make_store(tmp_path)
        idx = SolanaEventIndex(store)
        idx.ingest(
            [
                # Both miners active + funded from t=0; btc→tao (higher rate wins).
                rec('MinerActivated', miner='pk_a', block_time=0, at=0),
                rec('MinerActivated', miner='pk_b', block_time=0, at=0),
                rec('CollateralPosted', miner='pk_a', block_time=0, amount=0, total=500_000_000),
                rec('CollateralPosted', miner='pk_b', block_time=0, amount=0, total=500_000_000),
                rec('QuoteSet', miner='pk_a', block_time=0, from_chain='btc', to_chain='tao',
                    rate=300 * RATE_PRECISION, liquidity=0),
                rec('QuoteSet', miner='pk_b', block_time=0, from_chain='btc', to_chain='tao',
                    rate=200 * RATE_PRECISION, liquidity=0),
                # A takes a swap mid-window — crown flips to B while busy.
                rec('SwapInitiated', miner='pk_a', block_time=400),
                rec('SwapCompleted', miner='pk_a', block_time=800),
            ],
            ATTR,
        )
        crown = replay_crown_time_window(
            store=store,
            event_index=idx,
            from_chain='btc',
            to_chain='tao',
            window_start=100,
            window_end=1100,
            rewardable_hotkeys={'hk_a', 'hk_b'},
            min_swap_rao=100_000_000,
            max_swap_rao=500_000_000,
        )
        # A: (100,400] + (800,1100] = 600. B: (400,800] = 400.
        assert crown == {'hk_a': 600.0, 'hk_b': 400.0}
        store.close()

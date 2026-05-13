"""Unit tests for ReservationIndex and event-watcher integration.

The index is the validator's defense against a single source address
locking multiple miners simultaneously by spreading its visible balance
across concurrent reservations (issue #295). These tests cover the
three surfaces that have to work end-to-end for the defense to hold:

1. ``ReservationIndex`` aggregation logic — sum / filter / expire.
2. ``ContractEventWatcher.apply_event`` upserts/removes the right
   rows in response to MinerReserved / ReservationCancelled /
   SwapInitiated.
3. ``handle_swap_reserve`` rejects an over-commit by consulting the
   index inside the lock, with the rejection message shaped so the
   existing ``insufficient_source_balance`` translator rule still
   matches.
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

from allways.classes import MinerPair, Reservation
from allways.synapses import SwapReserveSynapse
from allways.validator.axon_handlers import handle_swap_reserve
from allways.validator.event_watcher import ContractEventWatcher
from allways.validator.reservation_index import ReservationIndex
from allways.validator.state_store import ValidatorStateStore

METADATA_PATH = Path(__file__).parent.parent / 'allways' / 'metadata' / 'allways_swap_manager.json'
TEST_CONTRACT_ADDRESS = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'


def make_reservation(
    *,
    from_addr: str = 'bc1-user',
    from_chain: str = 'btc',
    to_chain: str = 'tao',
    from_amount: int = 100_000,
    tao_amount: int = 345_000_000,
    to_amount: int = 345_000_000,
    reserved_until: int = 2_000,
) -> Reservation:
    return Reservation(
        hash='0x' + '00' * 32,
        from_addr=from_addr,
        from_chain=from_chain,
        to_chain=to_chain,
        tao_amount=tao_amount,
        from_amount=from_amount,
        to_amount=to_amount,
        reserved_until=reserved_until,
    )


# ---------------------------------------------------------------------------
# ReservationIndex (pure logic)
# ---------------------------------------------------------------------------


class TestReservationIndexAggregation:
    def test_sum_across_two_miners_same_address_and_chain(self):
        idx = ReservationIndex()
        idx.upsert('miner-a', make_reservation(from_amount=100))
        idx.upsert('miner-b', make_reservation(from_amount=250))
        total = idx.committed_amount_for_address(from_address='bc1-user', from_chain='btc', current_block=1_000)
        assert total == 350

    def test_skips_different_source_address(self):
        idx = ReservationIndex()
        idx.upsert('miner-a', make_reservation(from_addr='bc1-user', from_amount=100))
        idx.upsert('miner-b', make_reservation(from_addr='bc1-other', from_amount=999))
        total = idx.committed_amount_for_address(from_address='bc1-user', from_chain='btc', current_block=1_000)
        assert total == 100

    def test_skips_different_from_chain(self):
        """Source-chain balance is per-chain — a TAO-side reservation must
        not consume the user's BTC balance even if the SS58/string ever
        collides."""
        idx = ReservationIndex()
        idx.upsert('miner-a', make_reservation(from_chain='btc', from_amount=100))
        idx.upsert('miner-b', make_reservation(from_chain='tao', from_amount=999))
        total = idx.committed_amount_for_address(from_address='bc1-user', from_chain='btc', current_block=1_000)
        assert total == 100

    def test_ignores_expired_rows(self):
        """Expired rows are removed lazily on-chain (next vote_reserve), so the
        index may briefly hold stale entries; the reserved_until gate keeps
        them from inflating the committed total."""
        idx = ReservationIndex()
        idx.upsert('miner-a', make_reservation(from_amount=100, reserved_until=500))
        idx.upsert('miner-b', make_reservation(from_amount=200, reserved_until=2_000))
        total = idx.committed_amount_for_address(from_address='bc1-user', from_chain='btc', current_block=1_000)
        assert total == 200

    def test_exclude_miner_drops_a_specific_row(self):
        """``exclude_miner`` lets the reserve handler skip the requested miner
        if its prior reservation just expired (defense-in-depth — the
        contract already rejects double-reserve, but the validator should
        not double-count itself either)."""
        idx = ReservationIndex()
        idx.upsert('miner-a', make_reservation(from_amount=100))
        idx.upsert('miner-b', make_reservation(from_amount=200))
        total = idx.committed_amount_for_address(
            from_address='bc1-user',
            from_chain='btc',
            current_block=1_000,
            exclude_miner='miner-a',
        )
        assert total == 200

    def test_remove_clears_entry(self):
        idx = ReservationIndex()
        idx.upsert('miner-a', make_reservation(from_amount=100))
        idx.remove('miner-a')
        assert idx.committed_amount_for_address('bc1-user', 'btc', 1_000) == 0

    def test_empty_address_or_chain_returns_zero(self):
        """Defensive: an empty source address or chain (malformed request)
        would otherwise compare against any row with the same default
        value and over-report the committed amount."""
        idx = ReservationIndex()
        idx.upsert('miner-a', make_reservation(from_amount=100))
        assert idx.committed_amount_for_address('', 'btc', 1_000) == 0
        assert idx.committed_amount_for_address('bc1-user', '', 1_000) == 0


class TestReservationIndexHydrate:
    def test_hydrate_loads_only_live_rows(self):
        """Bootstrap reads every metagraph hotkey once. Expired rows are
        skipped — they will be cleared lazily by the contract on the next
        vote_reserve, no need to mirror them locally."""
        client = MagicMock()
        client.get_reservation.side_effect = lambda hk: {
            'hk-a': make_reservation(from_amount=100, reserved_until=2_000),
            'hk-b': None,
            'hk-c': make_reservation(from_amount=999, reserved_until=500),  # expired
        }.get(hk)
        idx = ReservationIndex()
        idx.hydrate_from_contract(client, ['hk-a', 'hk-b', 'hk-c'], current_block=1_000)
        assert len(idx) == 1
        assert idx.committed_amount_for_address('bc1-user', 'btc', 1_000) == 100

    def test_hydrate_swallows_per_hotkey_errors(self):
        """A failing read on one hotkey must not abort the whole bootstrap;
        without this guarantee a single flaky RPC could leave the validator
        permanently un-mirrored and silently re-open the over-commit hole."""
        client = MagicMock()

        def side(hotkey):
            if hotkey == 'hk-bad':
                raise RuntimeError('rpc blew up')
            return make_reservation(from_amount=42, reserved_until=2_000)

        client.get_reservation.side_effect = side
        idx = ReservationIndex()
        idx.hydrate_from_contract(client, ['hk-bad', 'hk-ok'], current_block=1_000)
        assert len(idx) == 1


# ---------------------------------------------------------------------------
# ContractEventWatcher integration
# ---------------------------------------------------------------------------


def make_watcher_with_index(tmp_path: Path) -> tuple[ContractEventWatcher, ReservationIndex, MagicMock]:
    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    idx = ReservationIndex()
    client = MagicMock()
    watcher = ContractEventWatcher(
        substrate=MagicMock(),
        contract_address=TEST_CONTRACT_ADDRESS,
        metadata_path=METADATA_PATH,
        state_store=store,
        reservation_index=idx,
        contract_client=client,
    )
    return watcher, idx, client


class TestEventWatcherUpdatesIndex:
    def test_miner_reserved_upserts_via_contract_read(self, tmp_path: Path):
        """MinerReserved only carries (miner, reserved_until); the watcher
        must follow up with get_reservation so the index stores the full
        row (from_addr / from_chain / from_amount) needed at reserve time."""
        watcher, idx, client = make_watcher_with_index(tmp_path)
        client.get_reservation.return_value = make_reservation(from_amount=500)
        watcher.apply_event(100, 'MinerReserved', {'miner': 'hk-a', 'reserved_until': 2_000})
        client.get_reservation.assert_called_once_with('hk-a')
        assert idx.committed_amount_for_address('bc1-user', 'btc', 1_000) == 500
        watcher.state_store.close()

    def test_reservation_cancelled_drops_entry(self, tmp_path: Path):
        watcher, idx, _ = make_watcher_with_index(tmp_path)
        idx.upsert('hk-a', make_reservation(from_amount=500))
        watcher.apply_event(101, 'ReservationCancelled', {'miner': 'hk-a'})
        assert idx.committed_amount_for_address('bc1-user', 'btc', 1_000) == 0
        watcher.state_store.close()

    def test_swap_initiated_drops_reservation(self, tmp_path: Path):
        """When a reservation transitions into a live swap, the contract
        clears the reservation row in clear_confirmed_reservation. The
        validator-side mirror must drop it too — otherwise the user's
        next unrelated reserve attempt would be rejected for an amount
        that's already been spent into the swap."""
        watcher, idx, _ = make_watcher_with_index(tmp_path)
        idx.upsert('hk-a', make_reservation(from_amount=500))
        watcher.apply_event(102, 'SwapInitiated', {'swap_id': 7, 'miner': 'hk-a'})
        assert idx.committed_amount_for_address('bc1-user', 'btc', 1_000) == 0
        watcher.state_store.close()

    def test_miner_reserved_failed_read_does_not_crash(self, tmp_path: Path):
        """A transient RPC failure when fetching the full reservation must
        not propagate into apply_event — that would stall sync_to and cause
        a re-replay that would still fail. The row stays missing from the
        index until the next bootstrap or a future MinerReserved retry."""
        watcher, idx, client = make_watcher_with_index(tmp_path)
        client.get_reservation.side_effect = RuntimeError('rpc down')
        watcher.apply_event(100, 'MinerReserved', {'miner': 'hk-a', 'reserved_until': 2_000})
        assert len(idx) == 0
        watcher.state_store.close()


# ---------------------------------------------------------------------------
# handle_swap_reserve: cumulative-balance gate
# ---------------------------------------------------------------------------


def make_reserve_synapse(
    miner_hotkey: str = '5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty',
    from_amount: int = 100_000,
    from_address: str = 'bc1-user',
    from_chain: str = 'btc',
    to_chain: str = 'tao',
) -> SwapReserveSynapse:
    return SwapReserveSynapse(
        miner_hotkey=miner_hotkey,
        tao_amount=345_000_000,
        from_amount=from_amount,
        to_amount=345_000_000,
        from_address=from_address,
        from_address_proof='proof',
        block_anchor=900,
        from_chain=from_chain,
        to_chain=to_chain,
    )


def make_reserve_validator(
    *,
    block: int = 1_000,
    miner_active: bool = True,
    miner_has_swap: bool = False,
    miner_reserved_until: int = 0,
    collateral: int = 1_000_000_000,
    balance: int = 100_000,
    cooldown: tuple = (0, 0),
    reservation_index: ReservationIndex | None = None,
) -> MagicMock:
    validator = MagicMock()
    validator.config.netuid = 2
    validator.axon_lock = threading.Lock()
    validator.axon_subtensor.get_current_block.return_value = block
    validator.metagraph.hotkeys = ['5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty']

    contract = MagicMock()
    contract.get_miner_snapshot.return_value = (
        collateral,
        miner_active,
        miner_has_swap,
        miner_reserved_until,
        0,
    )
    contract.get_cooldown.return_value = cooldown
    contract.vote_reserve.return_value = '0xtxhash'
    validator.axon_contract_client = contract

    btc = MagicMock()
    btc.verify_from_proof.return_value = True
    btc.get_balance.return_value = balance
    validator.axon_chain_providers = {'btc': btc, 'tao': MagicMock()}

    bounds = MagicMock()
    bounds.min_collateral.return_value = 0
    bounds.min_swap_amount.return_value = 0
    bounds.max_swap_amount.return_value = 0
    validator.bounds_cache = bounds

    validator.reservation_index = reservation_index
    validator.wallet = MagicMock()
    return validator


def run_reserve_handler(validator, synapse, commitment=None):
    if commitment is None:
        commitment = MinerPair(
            uid=1,
            hotkey=synapse.miner_hotkey,
            from_chain='btc',
            from_address='bc1-miner',
            to_chain='tao',
            to_address='5miner',
            rate=345.0,
            rate_str='345',
            counter_rate=0.0029,
            counter_rate_str='0.0029',
        )
    with patch('allways.validator.axon_handlers.read_miner_commitment', return_value=commitment):
        return asyncio.run(handle_swap_reserve(validator, synapse))


class TestReserveCumulativeBalanceGate:
    def test_rejects_when_other_reservations_exhaust_balance(self):
        """User has just enough BTC balance for one swap. Index already
        shows the same address holds another live reservation against a
        different miner — the new request must be rejected with the
        ``insufficient_source_balance`` prefix so the CLI translator
        renders the dedicated headline."""
        idx = ReservationIndex()
        idx.upsert(
            '5OtherMiner000000000000000000000000000000000000000',
            make_reservation(from_addr='bc1-user', from_chain='btc', from_amount=80_000),
        )
        validator = make_reserve_validator(balance=100_000, reservation_index=idx)
        synapse = make_reserve_synapse(from_amount=80_000)
        result = run_reserve_handler(validator, synapse)
        assert result.accepted is False
        assert result.rejection_reason.lower().startswith('insufficient source balance')
        # Critical: must not have voted on the contract.
        validator.axon_contract_client.vote_reserve.assert_not_called()

    def test_accepts_when_total_committed_plus_request_fits_balance(self):
        """Balance covers committed + new request — the request should pass
        the gate and proceed to vote_reserve."""
        idx = ReservationIndex()
        idx.upsert(
            '5OtherMiner000000000000000000000000000000000000000',
            make_reservation(from_addr='bc1-user', from_chain='btc', from_amount=40_000),
        )
        validator = make_reserve_validator(balance=200_000, reservation_index=idx)
        synapse = make_reserve_synapse(from_amount=80_000)
        result = run_reserve_handler(validator, synapse)
        assert result.accepted is True
        validator.axon_contract_client.vote_reserve.assert_called_once()

    def test_other_address_committed_does_not_count(self):
        """A different user's reservations against the same miner pool must
        not consume *this* user's balance budget."""
        idx = ReservationIndex()
        idx.upsert(
            '5OtherMiner000000000000000000000000000000000000000',
            make_reservation(from_addr='bc1-someone-else', from_chain='btc', from_amount=999_999),
        )
        validator = make_reserve_validator(balance=100_000, reservation_index=idx)
        synapse = make_reserve_synapse(from_amount=80_000)
        result = run_reserve_handler(validator, synapse)
        assert result.accepted is True

    def test_expired_reservations_do_not_count(self):
        idx = ReservationIndex()
        idx.upsert(
            '5OtherMiner000000000000000000000000000000000000000',
            make_reservation(
                from_addr='bc1-user',
                from_chain='btc',
                from_amount=999_999,
                reserved_until=500,
            ),
        )
        validator = make_reserve_validator(block=1_000, balance=100_000, reservation_index=idx)
        synapse = make_reserve_synapse(from_amount=80_000)
        result = run_reserve_handler(validator, synapse)
        assert result.accepted is True

    def test_no_index_attribute_falls_through(self):
        """Tests that don't construct a ReservationIndex (legacy fixtures,
        the dendrite-lite test paths) keep working. The aggregate gate is
        defense-in-depth on top of the per-request balance check; missing
        index is logged-and-skipped, not a hard failure."""
        validator = make_reserve_validator(balance=100_000, reservation_index=None)
        synapse = make_reserve_synapse(from_amount=80_000)
        result = run_reserve_handler(validator, synapse)
        assert result.accepted is True

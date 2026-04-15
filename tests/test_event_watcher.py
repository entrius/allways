"""Unit tests for ContractEventWatcher.

Covers three layers:
  1. ``load_event_registry`` — metadata JSON → registry dict
  2. ``decode_data_fields`` / ``decode_topic_fields`` — raw bytes → values
     (driven by hand-encoded SCALE fixtures so we don't need a live node)
  3. ``apply_event`` — state transitions once events are decoded
"""

import struct
from pathlib import Path
from unittest.mock import MagicMock

from substrateinterface.utils.ss58 import ss58_decode

from allways.validator.event_watcher import (
    ContractEventWatcher,
    EventDef,
    FieldDef,
    decode_data_fields,
    decode_topic_fields,
    load_event_registry,
)
from allways.validator.state_store import ValidatorStateStore

METADATA_PATH = Path(__file__).parent.parent / 'allways' / 'metadata' / 'allways_swap_manager.json'

# Well-known test SS58 — Alice from the substrate dev keyring. Used as
# contract_address in fixtures so the decoder's address comparison doesn't
# receive a garbage string that could bypass validation on future codepaths.
TEST_CONTRACT_ADDRESS = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'


def make_watcher(tmp_path: Path) -> ContractEventWatcher:
    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    return ContractEventWatcher(
        substrate=MagicMock(),
        contract_address=TEST_CONTRACT_ADDRESS,
        metadata_path=METADATA_PATH,
        state_store=store,
    )


def encode_u64_le(v: int) -> bytes:
    return struct.pack('<Q', v)


def encode_u128_le(v: int) -> bytes:
    return struct.pack('<QQ', v & 0xFFFFFFFFFFFFFFFF, v >> 64)


def encode_bool(v: bool) -> bytes:
    return b'\x01' if v else b'\x00'


def ss58_to_bytes(addr: str) -> bytes:
    return bytes.fromhex(ss58_decode(addr))


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
        w = make_watcher(tmp_path)
        w.apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 500_000_000})
        assert w.collateral['hk_a'] == 500_000_000
        events = w.get_collateral_events_in_range(0, 1000)
        assert len(events) == 1
        assert events[0]['block'] == 100
        w.state_store.close()

    def test_withdrawn_decrements(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 1_000})
        w.apply_event(200, 'CollateralWithdrawn', {'miner': 'hk_a', 'amount': 300})
        assert w.collateral['hk_a'] == 700
        w.state_store.close()

    def test_slashed_decrements_and_floors_at_zero(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 500})
        w.apply_event(200, 'CollateralSlashed', {'miner': 'hk_a', 'amount': 1_000})
        # Slashed for more than we have — floor at 0
        assert w.collateral['hk_a'] == 0
        w.state_store.close()


class TestActiveFlag:
    def test_activation_adds_to_set(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'MinerActivated', {'miner': 'hk_a', 'active': True})
        assert 'hk_a' in w.active_miners
        w.apply_event(200, 'MinerActivated', {'miner': 'hk_a', 'active': False})
        assert 'hk_a' not in w.active_miners
        w.state_store.close()


class TestConfigUpdated:
    def test_min_collateral_config_updates_field(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'ConfigUpdated', {'key': 'min_collateral', 'value': 250_000_000})
        assert w.min_collateral == 250_000_000
        # Unrelated config keys do not affect min_collateral
        w.apply_event(200, 'ConfigUpdated', {'key': 'reservation_ttl', 'value': 1200})
        assert w.min_collateral == 250_000_000
        w.state_store.close()


class TestSwapOutcomePersistence:
    def test_completed_writes_ledger(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'SwapCompleted', {'swap_id': 42, 'miner': 'hk_a'})
        stats = w.state_store.get_success_rates_since(0)
        assert stats['hk_a'] == (1, 0)
        w.state_store.close()

    def test_timed_out_writes_ledger(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'SwapTimedOut', {'swap_id': 42, 'miner': 'hk_a'})
        stats = w.state_store.get_success_rates_since(0)
        assert stats['hk_a'] == (0, 1)
        w.state_store.close()

    def test_mixed_outcomes_counted(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'SwapCompleted', {'swap_id': 1, 'miner': 'hk_a'})
        w.apply_event(101, 'SwapCompleted', {'swap_id': 2, 'miner': 'hk_a'})
        w.apply_event(102, 'SwapTimedOut', {'swap_id': 3, 'miner': 'hk_a'})
        stats = w.state_store.get_success_rates_since(0)
        assert stats['hk_a'] == (2, 1)
        w.state_store.close()


class TestSCALEDecoder:
    """Decoder fixtures: hand-build event bytes and feed them through.

    ink! v5 emits all event fields in the data blob in declaration order,
    with topic_fields getting a second copy in the topics array. These
    fixtures mirror what substrate.get_events would produce.
    """

    ALICE = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'

    def test_decode_miner_activated(self):
        event = EventDef(
            name='MinerActivated',
            signature_topic='0x' + '00' * 32,
            topic_fields=[FieldDef('miner', 'AccountId')],
            data_fields=[FieldDef('active', 'bool')],
        )
        miner_bytes = ss58_to_bytes(self.ALICE)
        # data = AccountId (32b, the indexed-field copy) + active bool
        data = miner_bytes + encode_bool(True)
        topics = [b'\x00' * 32, miner_bytes]

        values = decode_topic_fields(event, topics)
        values.update(decode_data_fields(event, data))

        assert values['miner'] == self.ALICE
        assert values['active'] is True

    def test_decode_collateral_posted(self):
        event = EventDef(
            name='CollateralPosted',
            signature_topic='0x' + '00' * 32,
            topic_fields=[FieldDef('miner', 'AccountId')],
            data_fields=[FieldDef('amount', 'u128'), FieldDef('total', 'u128')],
        )
        miner_bytes = ss58_to_bytes(self.ALICE)
        amount = 250_000_000
        total = 750_000_000
        data = miner_bytes + encode_u128_le(amount) + encode_u128_le(total)
        topics = [b'\x00' * 32, miner_bytes]

        values = decode_topic_fields(event, topics)
        values.update(decode_data_fields(event, data))

        assert values['miner'] == self.ALICE
        assert values['amount'] == amount
        assert values['total'] == total

    def test_decode_swap_completed(self):
        event = EventDef(
            name='SwapCompleted',
            signature_topic='0x' + '00' * 32,
            topic_fields=[FieldDef('swap_id', 'u64'), FieldDef('miner', 'AccountId')],
            data_fields=[FieldDef('tao_amount', 'u128'), FieldDef('fee_amount', 'u128')],
        )
        miner_bytes = ss58_to_bytes(self.ALICE)
        data = encode_u64_le(42) + miner_bytes + encode_u128_le(500_000_000) + encode_u128_le(5_000_000)
        topics = [b'\x00' * 32, encode_u64_le(42), miner_bytes]

        values = decode_topic_fields(event, topics)
        values.update(decode_data_fields(event, data))

        assert values['swap_id'] == 42
        assert values['miner'] == self.ALICE
        assert values['tao_amount'] == 500_000_000
        assert values['fee_amount'] == 5_000_000

    def test_decoder_stops_on_unknown_type(self):
        """A FieldDef with a type the decoder doesn't know halts decoding
        cleanly — partial values are kept, the rest are skipped."""
        event = EventDef(
            name='Weird',
            signature_topic='0x' + '00' * 32,
            topic_fields=[],
            data_fields=[
                FieldDef('first', 'u128'),
                FieldDef('second', 'FloatNobodySupports'),
                FieldDef('third', 'u128'),
            ],
        )
        data = encode_u128_le(1) + encode_u128_le(2) + encode_u128_le(3)
        values = decode_data_fields(event, data)
        # First decodes fine; decoder bails at 'second' and never reaches third
        assert 'first' in values
        assert 'second' not in values
        assert 'third' not in values


class TestBootstrap:
    """initialize() snapshotting behavior — the M1 fix."""

    def test_bootstrap_seeds_collateral_and_active_from_contract(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        client = MagicMock()
        client.get_miner_collateral.side_effect = lambda hk: {'hk_a': 10, 'hk_b': 20}.get(hk, 0)
        client.get_miner_active_flag.side_effect = lambda hk: hk == 'hk_a'
        client.get_min_collateral.return_value = 5

        w.initialize(current_block=1000, metagraph_hotkeys=['hk_a', 'hk_b'], contract_client=client)

        assert w.collateral == {'hk_a': 10, 'hk_b': 20}
        assert w.active_miners == {'hk_a'}
        assert w.min_collateral == 5
        assert w.cursor == 1000
        w.state_store.close()

    def test_bootstrap_tolerates_contract_read_failures(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        client = MagicMock()
        client.get_miner_collateral.side_effect = RuntimeError('rpc down')
        client.get_miner_active_flag.side_effect = RuntimeError('rpc down')
        client.get_min_collateral.side_effect = RuntimeError('rpc down')

        w.initialize(current_block=500, metagraph_hotkeys=['hk_a'], contract_client=client)

        # Everything defaults to empty/starting state, no exception propagated
        assert w.collateral == {}
        assert w.active_miners == set()
        assert w.cursor == 500
        w.state_store.close()


class TestSetCollateral:
    def test_set_collateral_uses_total_from_collateral_posted(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 1_000, 'total': 10_000})
        # total (not amount) is authoritative
        assert w.collateral['hk_a'] == 10_000
        w.state_store.close()

    def test_set_collateral_uses_remaining_from_withdrawn(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 10_000, 'total': 10_000})
        w.apply_event(200, 'CollateralWithdrawn', {'miner': 'hk_a', 'amount': 3_000, 'remaining': 7_000})
        assert w.collateral['hk_a'] == 7_000
        w.state_store.close()

    def test_latest_before_uses_bisect_index(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        for block, amount in [(100, 1), (200, 2), (300, 3), (400, 4)]:
            w.set_collateral(block, 'hk_a', amount)
        # Before first event → None
        assert w.get_latest_collateral_before('hk_a', block=50) is None
        # On-boundary → hit that event
        assert w.get_latest_collateral_before('hk_a', block=200) == (2, 200)
        # Between events → previous event
        assert w.get_latest_collateral_before('hk_a', block=250) == (2, 200)
        # After last event → last event
        assert w.get_latest_collateral_before('hk_a', block=9999) == (4, 400)
        w.state_store.close()

    def test_latest_before_falls_back_to_snapshot_when_no_events(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        # No events yet; only bootstrap-seeded collateral
        w.collateral['hk_seed'] = 500
        result = w.get_latest_collateral_before('hk_seed', block=1000)
        assert result == (500, 0)
        w.state_store.close()


class TestCollateralEventsInRange:
    def test_events_are_block_filtered(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 1_000})
        w.apply_event(200, 'CollateralPosted', {'miner': 'hk_a', 'amount': 2_000})
        w.apply_event(300, 'CollateralPosted', {'miner': 'hk_a', 'amount': 3_000})
        # Range is (start, end]: block 100 is excluded, 200/300 included
        events = w.get_collateral_events_in_range(100, 300)
        assert [e['block'] for e in events] == [200, 300]
        w.state_store.close()

    def test_latest_before_returns_most_recent(self, tmp_path: Path):
        w = make_watcher(tmp_path)
        w.apply_event(100, 'CollateralPosted', {'miner': 'hk_a', 'amount': 1_000})
        w.apply_event(200, 'CollateralPosted', {'miner': 'hk_a', 'amount': 500})
        result = w.get_latest_collateral_before('hk_a', block=150)
        assert result is not None
        collateral, block = result
        assert collateral == 1_000
        assert block == 100
        w.state_store.close()

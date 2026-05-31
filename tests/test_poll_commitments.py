from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from allways.classes import MinerPair
from allways.constants import SCORING_WINDOW_BLOCKS
from allways.validator.forward import poll_commitments
from allways.validator.state_store import ValidatorStateStore


def make_pair(hotkey: str, rate: float, counter_rate: float, source='tao', dest='btc') -> MinerPair:
    return MinerPair(
        uid=0,
        hotkey=hotkey,
        from_chain=source,
        from_address='src_addr',
        to_chain=dest,
        to_address='dst_addr',
        rate=rate,
        rate_str=str(rate),
        counter_rate=counter_rate,
        counter_rate_str=str(counter_rate),
    )


def make_validator(tmp_path: Path, hotkeys=None) -> SimpleNamespace:
    """Construct a validator stub with the fields used by poll_commitments.

    Includes a MagicMock event_watcher defensively — poll_commitments
    doesn't touch it today, but if a helper ever does the tests should
    AttributeError loud, not silently pass or break confusingly.
    """
    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    metagraph = SimpleNamespace(hotkeys=list(hotkeys or ['hk_a', 'hk_b']))
    config = SimpleNamespace(netuid=2)
    bounds_cache = MagicMock()
    bounds_cache.min_swap_amount.return_value = 0
    bounds_cache.max_swap_amount.return_value = 0
    return SimpleNamespace(
        block=1000,
        subtensor=MagicMock(),
        config=config,
        metagraph=metagraph,
        state_store=store,
        contract_client=MagicMock(),
        event_watcher=MagicMock(),
        bounds_cache=bounds_cache,
        last_known_rates={},
    )


class TestPollCommitmentsBasic:
    def test_first_poll_inserts_both_directions_per_miner(self, tmp_path: Path):
        v = make_validator(tmp_path)
        pairs = [
            make_pair('hk_a', rate=0.00015, counter_rate=6500.0),
            make_pair('hk_b', rate=0.00016, counter_rate=6400.0),
        ]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            poll_commitments(v)

        # 4 rate_events: 2 miners * 2 directions
        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 2000)
        btc_tao = v.state_store.get_rate_events_in_range('btc', 'tao', 0, 2000)
        assert len(tao_btc) == 2
        assert len(btc_tao) == 2
        assert v.last_known_rates == {
            ('hk_a', 'tao', 'btc'): 0.00015,
            ('hk_a', 'btc', 'tao'): 6500.0,
            ('hk_b', 'tao', 'btc'): 0.00016,
            ('hk_b', 'btc', 'tao'): 6400.0,
        }
        v.state_store.close()


class TestPollCommitmentsChanges:
    def test_no_changes_across_polls_inserts_nothing_extra(self, tmp_path: Path):
        v = make_validator(tmp_path)
        pairs = [make_pair('hk_a', rate=0.00015, counter_rate=6500.0)]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            poll_commitments(v)

        v.block += 1
        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            poll_commitments(v)

        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        assert len(tao_btc) == 1
        v.state_store.close()

    def test_rate_change_inserts_new_event_every_block(self, tmp_path: Path):
        """Per-block polling — a rate change is recorded immediately with no
        throttle delay."""
        v = make_validator(tmp_path)
        pairs_v1 = [make_pair('hk_a', rate=0.00015, counter_rate=6500.0)]
        pairs_v2 = [make_pair('hk_a', rate=0.00020, counter_rate=6500.0)]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs_v1):
            poll_commitments(v)

        # A single block later — the throttle is gone, so the change lands.
        v.block += 1
        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs_v2):
            poll_commitments(v)

        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        btc_tao = v.state_store.get_rate_events_in_range('btc', 'tao', 0, 10_000)
        assert [e['rate'] for e in tao_btc] == [0.00015, 0.00020]
        # counter rate unchanged → still only one event
        assert [e['rate'] for e in btc_tao] == [6500.0]
        v.state_store.close()


class TestPollCommitmentsZeroRate:
    def test_zero_rate_skips_only_that_direction(self, tmp_path: Path):
        v = make_validator(tmp_path)
        pairs = [make_pair('hk_a', rate=0.00015, counter_rate=0.0)]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            poll_commitments(v)

        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        btc_tao = v.state_store.get_rate_events_in_range('btc', 'tao', 0, 10_000)
        assert len(tao_btc) == 1
        assert len(btc_tao) == 0
        assert ('hk_a', 'tao', 'btc') in v.last_known_rates
        assert ('hk_a', 'btc', 'tao') not in v.last_known_rates
        v.state_store.close()

    def test_zero_after_positive_records_optout(self, tmp_path: Path):
        """A miner dropping a previously-offered direction to 0 must persist a
        terminating zero so scoring stops crediting the stale positive rate."""
        v = make_validator(tmp_path)

        with patch(
            'allways.validator.forward.read_miner_commitments',
            return_value=[make_pair('hk_a', rate=0.00015, counter_rate=6500.0)],
        ):
            poll_commitments(v)

        v.block += 1
        with patch(
            'allways.validator.forward.read_miner_commitments',
            return_value=[make_pair('hk_a', rate=0.0, counter_rate=6500.0)],
        ):
            poll_commitments(v)

        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        assert [e['rate'] for e in tao_btc] == [0.00015, 0.0]
        assert v.last_known_rates[('hk_a', 'tao', 'btc')] == 0.0
        v.state_store.close()

    def test_zero_optout_is_recorded_once_not_every_poll(self, tmp_path: Path):
        """Once the zero terminator lands, repeated zero polls add nothing."""
        v = make_validator(tmp_path)
        with patch(
            'allways.validator.forward.read_miner_commitments',
            return_value=[make_pair('hk_a', rate=0.00015, counter_rate=6500.0)],
        ):
            poll_commitments(v)

        for _ in range(3):
            v.block += 1
            with patch(
                'allways.validator.forward.read_miner_commitments',
                return_value=[make_pair('hk_a', rate=0.0, counter_rate=6500.0)],
            ):
                poll_commitments(v)

        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        assert [e['rate'] for e in tao_btc] == [0.00015, 0.0]
        v.state_store.close()

    def test_reenable_after_optout(self, tmp_path: Path):
        """positive -> 0 -> positive yields [pos, 0, pos]: the direction
        re-enables normally after an opt-out."""
        v = make_validator(tmp_path)
        sequence = [0.00015, 0.0, 0.00020]
        for rate in sequence:
            with patch(
                'allways.validator.forward.read_miner_commitments',
                return_value=[make_pair('hk_a', rate=rate, counter_rate=6500.0)],
            ):
                poll_commitments(v)
            v.block += 1

        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        assert [e['rate'] for e in tao_btc] == [0.00015, 0.0, 0.00020]
        v.state_store.close()

    def test_zero_during_downtime_reconciles_against_persisted_rate(self, tmp_path: Path):
        """A zero posted while the validator was down (so last_known_rates is
        empty on restart) still terminates the prior positive on the next poll,
        because the check reads the persisted latest rate."""
        v = make_validator(tmp_path)
        v.state_store.insert_rate_event(
            hotkey='hk_a', from_chain='tao', to_chain='btc', rate=0.00015, block=v.block - 50
        )

        assert v.last_known_rates == {}  # simulate fresh process after restart
        with patch(
            'allways.validator.forward.read_miner_commitments',
            return_value=[make_pair('hk_a', rate=0.0, counter_rate=6500.0)],
        ):
            poll_commitments(v)

        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        assert [e['rate'] for e in tao_btc] == [0.00015, 0.0]
        v.state_store.close()


class TestPollCommitmentsDereg:
    def test_dereg_removes_hotkey_from_store_and_cache(self, tmp_path: Path):
        v = make_validator(tmp_path, hotkeys=['hk_a', 'hk_b'])
        pairs = [
            make_pair('hk_a', rate=0.00015, counter_rate=6500.0),
            make_pair('hk_b', rate=0.00016, counter_rate=6400.0),
        ]

        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            poll_commitments(v)

        # hk_b deregistered
        v.metagraph.hotkeys = ['hk_a']
        v.block += 1
        with patch('allways.validator.forward.read_miner_commitments', return_value=pairs):
            poll_commitments(v)

        assert v.state_store.get_latest_rate_before('hk_b', 'tao', 'btc', block=10_000) is None
        assert v.state_store.get_latest_rate_before('hk_a', 'tao', 'btc', block=10_000) is not None
        assert all(k[0] != 'hk_b' for k in v.last_known_rates.keys())
        v.state_store.close()


class TestPollCommitmentsErrors:
    def test_read_raises_logs_and_returns_cleanly(self, tmp_path: Path):
        v = make_validator(tmp_path)

        def raiser(*args, **kwargs):
            raise RuntimeError('websocket dead')

        with patch('allways.validator.forward.read_miner_commitments', side_effect=raiser):
            poll_commitments(v)

        # No events persisted on a failed read.
        assert v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000) == []
        v.state_store.close()


class TestPollCommitmentsSentinel:
    def test_previously_positive_direction_terminated_when_pair_drops(self, tmp_path: Path):
        """Regression guard for the parser-poison free-rider hole.

        Miner posts a sane rate, then overwrites their commitment with garbage
        (or rate goes unexecutable). hk_a's pair vanishes from the poll, but
        the prior positive rate is still in state_store. The second sweep must
        emit a 0-terminator so scoring stops crediting the stale rate.

        hk_b stays in the poll throughout — so pairs is non-empty (proving
        this isn't the RPC-failure case where the sweep is skipped).
        """
        v = make_validator(tmp_path, hotkeys=['hk_a', 'hk_b'])

        with patch(
            'allways.validator.forward.read_miner_commitments',
            return_value=[
                make_pair('hk_a', rate=0.00015, counter_rate=6500.0),
                make_pair('hk_b', rate=0.00016, counter_rate=6400.0),
            ],
        ):
            poll_commitments(v)

        v.block += 1
        # hk_a's commitment is parser-poisoned (vanishes); hk_b is still posting.
        with patch(
            'allways.validator.forward.read_miner_commitments',
            return_value=[make_pair('hk_b', rate=0.00016, counter_rate=6400.0)],
        ):
            poll_commitments(v)

        a_tao_btc = [
            e for e in v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000) if e['hotkey'] == 'hk_a'
        ]
        a_btc_tao = [
            e for e in v.state_store.get_rate_events_in_range('btc', 'tao', 0, 10_000) if e['hotkey'] == 'hk_a'
        ]
        assert [e['rate'] for e in a_tao_btc] == [0.00015, 0.0]
        assert [e['rate'] for e in a_btc_tao] == [6500.0, 0.0]
        assert v.last_known_rates[('hk_a', 'tao', 'btc')] == 0.0
        assert v.last_known_rates[('hk_a', 'btc', 'tao')] == 0.0
        v.state_store.close()

    def test_empty_pairs_does_not_terminate_known_positives(self, tmp_path: Path):
        """read_miner_commitments swallows transient RPC errors and returns [].
        If we treated [] as 'every miner vanished', a single websocket flake
        would zero every previously-positive miner. Skip the sweep instead."""
        v = make_validator(tmp_path)

        with patch(
            'allways.validator.forward.read_miner_commitments',
            return_value=[make_pair('hk_a', rate=0.00015, counter_rate=6500.0)],
        ):
            poll_commitments(v)

        v.block += 1
        # Simulate RPC failure: empty pairs (could be RPC dead OR genuine).
        with patch('allways.validator.forward.read_miner_commitments', return_value=[]):
            poll_commitments(v)

        tao_btc = v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000)
        btc_tao = v.state_store.get_rate_events_in_range('btc', 'tao', 0, 10_000)
        assert [e['rate'] for e in tao_btc] == [0.00015]
        assert [e['rate'] for e in btc_tao] == [6500.0]
        assert v.last_known_rates[('hk_a', 'tao', 'btc')] == 0.00015
        v.state_store.close()

    def test_no_terminator_when_never_offered(self, tmp_path: Path):
        """Direction that was never positive must not get a spurious 0 event."""
        v = make_validator(tmp_path)

        with patch('allways.validator.forward.read_miner_commitments', return_value=[]):
            poll_commitments(v)

        assert v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000) == []
        assert v.state_store.get_rate_events_in_range('btc', 'tao', 0, 10_000) == []
        v.state_store.close()

    def test_bounds_threaded_into_read(self, tmp_path: Path):
        """Validator bounds_cache values must flow into read_miner_commitments
        so the parser drops unexecutable pairs before they ever reach the loop.
        """
        v = make_validator(tmp_path)
        v.bounds_cache.min_swap_amount.return_value = 500_000_000
        v.bounds_cache.max_swap_amount.return_value = 5_000_000_000

        with patch('allways.validator.forward.read_miner_commitments', return_value=[]) as mock_read:
            poll_commitments(v)

        assert mock_read.call_args.kwargs['min_swap_rao'] == 500_000_000
        assert mock_read.call_args.kwargs['max_swap_rao'] == 5_000_000_000
        v.state_store.close()


class TestBootstrapHydratesLastKnownRates:
    """bootstrap_miner_rates must seed last_known_rates from persisted state so
    the runtime second sweep catches stale positives from miners
    parser-poisoned before this restart."""

    def _make_validator_with_bootstrap(self, tmp_path: Path, hotkeys=None) -> SimpleNamespace:
        v = make_validator(tmp_path, hotkeys=hotkeys)
        # bootstrap_miner_rates reads self.block and SCORING_WINDOW_BLOCKS to
        # pick an anchor; default v.block=1000 is fine.
        return v

    def test_bootstrap_seeds_from_state_store_for_stale_positives(self, tmp_path: Path):
        """A positive rate persisted before restart but absent from this poll
        must still be in last_known_rates after bootstrap."""
        from neurons.validator import Validator

        v = self._make_validator_with_bootstrap(tmp_path, hotkeys=['hk_a'])
        anchor_block = max(0, v.block - SCORING_WINDOW_BLOCKS)
        v.state_store.insert_rate_event(
            hotkey='hk_a', from_chain='tao', to_chain='btc', rate=0.00015, block=anchor_block - 10
        )

        with patch('neurons.validator.read_miner_commitments', return_value=[]):
            Validator.bootstrap_miner_rates(v)

        assert v.last_known_rates.get(('hk_a', 'tao', 'btc')) == 0.00015
        v.state_store.close()

    def test_post_bootstrap_first_poll_terminates_parser_poisoned_miner(self, tmp_path: Path):
        """End-to-end: persisted positive → bootstrap hydrates → next poll sees
        no commitment for the poisoned miner → 0-terminator emitted.

        hk_b posts throughout so pairs is non-empty (the empty-pairs sweep
        guard would otherwise skip termination)."""
        from neurons.validator import Validator

        v = self._make_validator_with_bootstrap(tmp_path, hotkeys=['hk_a', 'hk_b'])
        anchor_block = max(0, v.block - SCORING_WINDOW_BLOCKS)
        v.state_store.insert_rate_event(
            hotkey='hk_a', from_chain='tao', to_chain='btc', rate=0.00015, block=anchor_block - 10
        )

        with patch('neurons.validator.read_miner_commitments', return_value=[]):
            Validator.bootstrap_miner_rates(v)

        with patch(
            'allways.validator.forward.read_miner_commitments',
            return_value=[make_pair('hk_b', rate=0.00020, counter_rate=6400.0)],
        ):
            poll_commitments(v)

        a_tao_btc = [
            e for e in v.state_store.get_rate_events_in_range('tao', 'btc', 0, 10_000) if e['hotkey'] == 'hk_a'
        ]
        assert [e['rate'] for e in a_tao_btc] == [0.00015, 0.0]
        v.state_store.close()


class TestPollCommitmentsPruning:
    def test_prune_runs_via_scoring_pass_not_commitment_poll(self, tmp_path: Path):
        """Pruning moved out of the per-tick path and into the scoring round —
        verify both parts of that contract: commitment polling does NOT prune,
        and score_and_reward_miners DOES. The latest row per (hotkey, direction)
        is preserved as a state-reconstruction anchor even when it's older than
        the cutoff, so this test uses a hotkey with two rows to exercise the
        "older one drops, newer one survives" path."""
        from allways.validator.scoring import prune_rate_events

        v = make_validator(tmp_path)
        v.block = SCORING_WINDOW_BLOCKS + 1_000
        ancient_block = 1
        recent_block = v.block - 100

        conn = v.state_store.require_connection()
        # Two rows for the same direction — the ancient one must drop on prune
        # while the recent one survives.
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00010, ancient_block),
        )
        conn.execute(
            'INSERT INTO rate_events (hotkey, from_chain, to_chain, rate, block) VALUES (?, ?, ?, ?, ?)',
            ('hk_a', 'tao', 'btc', 0.00020, recent_block),
        )
        conn.commit()

        # 1. Commitment polling no longer prunes — both rows survive.
        with patch('allways.validator.forward.read_miner_commitments', return_value=[]):
            poll_commitments(v)
        surviving_blocks = {e['block'] for e in v.state_store.get_rate_events_in_range('tao', 'btc', 0, v.block + 1)}
        assert ancient_block in surviving_blocks, 'poll_commitments should not prune'
        assert recent_block in surviving_blocks

        # 2. Scoring pass prunes the ancient row; the latest row stays as anchor.
        prune_rate_events(v)
        surviving_blocks = {e['block'] for e in v.state_store.get_rate_events_in_range('tao', 'btc', 0, v.block + 1)}
        assert ancient_block not in surviving_blocks
        assert recent_block in surviving_blocks

        v.state_store.close()

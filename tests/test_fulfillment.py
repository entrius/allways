"""B4.1 — SwapFulfiller against the Solana program.

Deadlines are unix-seconds (``Swap.timeout_at``); the miner sends the full pinned ``to_amount`` (the 1%
fee is skimmed from collateral at confirm, not this leg); ``mark_fulfilled`` records only the dest tx
hash/block. The send-cache is keyed by ``swap_key`` hex and locks in the idempotency invariant: once
dest funds are sent, an unmarked entry keeps blocking a duplicate send until mark_fulfilled lands or the
swap is provably past its deadline.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from solders.keypair import Keypair

from allways.constants import MINER_TIMEOUT_CUSHION_SECS, SENT_CACHE_DISCARD_MARGIN_SECS
from allways.miner import fulfillment as fulfillment_mod
from allways.miner.fulfillment import SentSwap, SwapFulfiller
from allways.solana.client import SolanaClientError, SolanaSwap


@pytest.fixture
def at_now(monkeypatch):
    """Freeze fulfillment's wall clock; returns a setter."""

    def _set(t: int):
        monkeypatch.setattr(fulfillment_mod.time, 'time', lambda: t)

    _set(0)
    return _set


def make_fulfiller(**kw) -> SwapFulfiller:
    return SwapFulfiller(solana_client=MagicMock(), chain_providers={}, **kw)


def make_swap(
    timeout_at: int = 5000, to_amount: int = 345_000_000, miner_from: str = 'bc1q-miner', sid: int = 1
) -> SolanaSwap:
    return SolanaSwap(
        swap_key=bytes([sid] * 32),
        miner=Keypair().pubkey(),
        user=Keypair().pubkey(),
        from_chain='btc',
        to_chain='tao',
        user_from_addr='bc1q-user',
        user_to_addr='5user',
        miner_from_addr=miner_from,
        miner_to_addr='5miner',
        rate=345,
        collateral_amount=1_000_000,
        from_amount=1_000_000,
        to_amount=to_amount,
        from_tx_hash='deadbeef',
        from_tx_block=100,
        to_tx_hash='',
        to_tx_block=0,
        status='Active',
        initiated_at=1000,
        timeout_at=timeout_at,
        max_extend_at=timeout_at + 10_000,
        fulfilled_at=0,
    )


class TestVerifySwapSafetyCushion:
    """The cushion is MINER_TIMEOUT_CUSHION_SECS before timeout_at — the miner stops STARTING a fulfill
    that long before the deadline so the validator extension flow still has runway."""

    def test_allows_swap_well_before_cushion_window(self, at_now):
        f = make_fulfiller()
        at_now(5000 - MINER_TIMEOUT_CUSHION_SECS - 100)
        result = f.verify_swap_safety(make_swap(timeout_at=5000))
        assert result is not None
        assert result[1] == 'bc1q-miner'

    def test_blocks_swap_inside_cushion_window(self, at_now):
        f = make_fulfiller()
        at_now(5000 - MINER_TIMEOUT_CUSHION_SECS + 1)
        assert f.verify_swap_safety(make_swap(timeout_at=5000)) is None

    def test_blocks_swap_at_cushion_boundary(self, at_now):
        f = make_fulfiller()
        at_now(5000 - MINER_TIMEOUT_CUSHION_SECS)  # >= check → unsafe at the exact boundary
        assert f.verify_swap_safety(make_swap(timeout_at=5000)) is None

    def test_missing_miner_from_addr_or_zero_amount_fails_safety(self, at_now):
        f = make_fulfiller()
        at_now(0)
        assert f.verify_swap_safety(make_swap(miner_from='')) is None
        assert f.verify_swap_safety(make_swap(to_amount=0)) is None


class TestVerifySwapSafetyReturnsPostFeeAmount:
    """Option A: the miner delivers 99% of the pinned to_amount; the protocol takes its 1% from
    collateral at confirm. The validator's verify_fulfillment checks for exactly this 99%."""

    def test_returns_post_fee_99_percent(self, at_now):
        f = make_fulfiller()  # default fee_divisor = 100
        at_now(0)
        swap = make_swap(timeout_at=5000, to_amount=3_450_000_000)
        result = f.verify_swap_safety(swap)
        assert result is not None
        user_receives_amount, addr = result
        # 3_450_000_000 - 3_450_000_000 // 100 = 3_415_500_000
        assert user_receives_amount == 3_415_500_000
        assert addr == 'bc1q-miner'


class TestSentCacheCleanup:
    def test_unmarked_stale_retained_marked_stale_removed_within_deadline(self, at_now):
        f = make_fulfiller()
        at_now(100)  # well within all deadlines
        k1, k2, k3 = 'aa', 'bb', 'cc'
        f.sent = {
            k1: SentSwap('unmarked-stale-tx', 101, marked_fulfilled=False, timeout_at=5000),
            k2: SentSwap('marked-stale-tx', 102, marked_fulfilled=True, timeout_at=5000),
            k3: SentSwap('active-unmarked-tx', 103, marked_fulfilled=False, timeout_at=5000),
        }
        f.mark_fulfilled_attempts = {k1: 2, k2: 3, k3: 1}

        f.cleanup_stale_sends(active_swap_keys={k3})

        assert set(f.sent) == {k1, k3}  # marked-stale removed; unmarked-stale retained; active untouched
        assert f.mark_fulfilled_attempts == {k1: 2, k3: 1}

    def test_mark_fulfilled_retry_not_gated_by_cushion_after_send(self, at_now):
        # #462: once dest funds are out the swap stays Active until mark_fulfilled lands, so the cushion
        # (scoped to STARTING a fulfill) must NOT gate the post-send retry. Uses the REAL
        # verify_swap_safety so the cushion actually runs on the retry path.
        swap = make_swap(timeout_at=5000)
        f = make_fulfiller()
        at_now(5000 - MINER_TIMEOUT_CUSHION_SECS)  # inside cushion: a first SEND would be gated off
        f.sent[swap.key_hex] = SentSwap('already-sent-dest-tx', 777, marked_fulfilled=False, timeout_at=5000)
        f.send_dest_funds = MagicMock()
        f.client.mark_fulfilled.side_effect = SolanaClientError('transient rpc failure')

        result = f.process_swap(swap)

        assert result is False
        f.send_dest_funds.assert_not_called()  # never re-send
        f.client.mark_fulfilled.assert_called_once_with(
            swap_key=swap.swap_key,
            to_tx_hash='already-sent-dest-tx',
            to_tx_block=777,
        )
        assert f.sent[swap.key_hex].marked_fulfilled is False  # still retryable next pass

    def test_retained_send_blocks_resend_when_absent_then_rediscovered(self, at_now):
        swap = make_swap(timeout_at=5000)
        f = make_fulfiller()
        at_now(100)  # within deadline → retain
        f.sent[swap.key_hex] = SentSwap('already-sent-dest-tx', 777, marked_fulfilled=False, timeout_at=5000)

        # A transient empty snapshot must NOT drop the unmarked entry.
        f.cleanup_stale_sends(active_swap_keys=set())
        assert f.sent[swap.key_hex].to_tx_hash == 'already-sent-dest-tx'

        # Swap reappears; process_swap must retry mark_fulfilled, not resend funds.
        f.send_dest_funds = MagicMock(return_value=('second-dest-tx', 888))
        f.client.mark_fulfilled.side_effect = None

        assert f.process_swap(swap) is True
        f.send_dest_funds.assert_not_called()
        f.client.mark_fulfilled.assert_called_once_with(
            swap_key=swap.swap_key,
            to_tx_hash='already-sent-dest-tx',
            to_tx_block=777,
        )
        assert f.sent[swap.key_hex].marked_fulfilled is True

    def test_unmarked_stale_discarded_once_past_deadline_margin(self, at_now):
        f = make_fulfiller()
        f.sent = {'k': SentSwap('leaked-tx', 50, marked_fulfilled=False, timeout_at=1000)}
        at_now(1000 + SENT_CACHE_DISCARD_MARGIN_SECS + 1)  # provably past any extended deadline
        f.cleanup_stale_sends(active_swap_keys=set())
        assert f.sent == {}

    def test_unmarked_stale_retained_at_margin_boundary(self, at_now):
        f = make_fulfiller()
        f.sent = {'k': SentSwap('b', 2, marked_fulfilled=False, timeout_at=1000)}
        at_now(1000 + SENT_CACHE_DISCARD_MARGIN_SECS)  # exactly at margin → retain (discard uses strict >)
        f.cleanup_stale_sends(active_swap_keys=set())
        assert set(f.sent) == {'k'}

    def test_legacy_entry_without_deadline_never_discarded(self, at_now):
        f = make_fulfiller()
        f.sent = {'k': SentSwap('legacy-tx', 5, marked_fulfilled=False)}  # timeout_at defaults to 0
        at_now(10**12)
        f.cleanup_stale_sends(active_swap_keys=set())
        assert set(f.sent) == {'k'}

    def test_cache_persistence_roundtrips_timeout_at(self, tmp_path: Path):
        cache_path = tmp_path / 'sent_cache.json'
        writer = make_fulfiller(sent_cache_path=cache_path)
        writer.sent = {'7e': SentSwap('tx7', 123, marked_fulfilled=False, timeout_at=456)}
        writer.save_sent_cache()

        reader = make_fulfiller(sent_cache_path=cache_path)
        assert reader.sent['7e'] == SentSwap('tx7', 123, marked_fulfilled=False, timeout_at=456)

    def test_legacy_three_element_cache_loads_with_zero_timeout(self, tmp_path: Path):
        cache_path = tmp_path / 'sent_cache.json'
        cache_path.write_text('{"9a": ["legacy-tx", 999, false]}')  # pre-fix 3-element shape
        reader = make_fulfiller(sent_cache_path=cache_path)
        assert reader.sent['9a'] == SentSwap('legacy-tx', 999, marked_fulfilled=False, timeout_at=0)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

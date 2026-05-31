"""Tests for `alw swap quote` row selection.

Covers two correctness regressions fixed alongside this file:
- BTC→TAO no longer aborts globally when the top sorted row is a sentinel
  rate (e.g. 1.797e+308 TAO/BTC); a normal-rate miner still gets labeled
  "available" when the per-miner TAO leg lands in bounds.
- TAO→BTC sorts direction-aware: lower rate first (better user price),
  matching `alw swap now` and the validator crown.
"""

from unittest.mock import patch

from click.testing import CliRunner

from allways.classes import MinerPair
from allways.cli.swap_commands import quote as quote_module
from allways.cli.swap_commands.quote import quote_command

MIN_SWAP_RAO = 100_000_000  # 0.1 TAO
MAX_SWAP_RAO = 500_000_000  # 0.5 TAO


def _pair(uid: int, from_chain: str, to_chain: str, rate: float, rate_str: str | None = None) -> MinerPair:
    return MinerPair(
        uid=uid,
        hotkey=f'hk{uid}',
        from_chain=from_chain,
        from_address=f'addr{uid}',
        to_chain=to_chain,
        to_address=f'dst{uid}',
        rate=rate,
        rate_str=rate_str if rate_str is not None else repr(rate),
    )


class _FakeClient:
    """Stub for AllwaysContractClient — drives the per-miner viability path."""

    def __init__(self, collaterals: dict[str, int], in_swap: set[str] | None = None):
        self._collaterals = collaterals
        self._in_swap = in_swap or set()

    def get_miner_active_flag(self, hotkey: str) -> bool:
        return True

    def get_miner_collateral(self, hotkey: str) -> int:
        return self._collaterals.get(hotkey, 0)

    def get_miner_has_active_swap(self, hotkey: str) -> bool:
        return hotkey in self._in_swap

    def get_min_swap_amount(self) -> int:
        return MIN_SWAP_RAO

    def get_max_swap_amount(self) -> int:
        return MAX_SWAP_RAO


def _invoke(pairs: list[MinerPair], collaterals: dict[str, int], args: list[str], in_swap: set[str] | None = None):
    """Run quote_command with stubbed chain/client access."""
    fake_client = _FakeClient(collaterals, in_swap)
    config = {'netuid': 7}
    with (
        patch.object(quote_module, 'get_cli_context', return_value=(config, None, object(), fake_client)),
        patch.object(quote_module, 'read_miner_commitments', return_value=pairs),
        patch.object(quote_module, 'find_matching_miners', side_effect=lambda all_pairs, f, t: list(all_pairs)),
    ):
        runner = CliRunner()
        return runner.invoke(quote_command, args, catch_exceptions=False)


class TestBtcToTaoBoundsCheck:
    """Regression: the up-front bounds check used available[0]'s rate to derive
    the TAO leg, so a single sentinel row aborted with "No miner can accept this"
    even when other miners would have accepted the same source amount."""

    def test_sentinel_top_row_does_not_hide_viable_lower_row(self):
        pairs = [
            _pair(14, 'btc', 'tao', 1.797e308),  # overflow sentinel
            _pair(189, 'btc', 'tao', 226.42),  # normal — 0.0005 BTC → ~0.113 TAO, in bounds
        ]
        collaterals = {'hk14': 1_000_000_000, 'hk189': 1_000_000_000}
        result = _invoke(pairs, collaterals, ['--from', 'btc', '--to', 'tao', '--amount', '0.0005'])
        assert result.exit_code == 0, result.output
        # The bug emitted this exact line before; assert it doesn't anymore.
        assert 'Amount above contract maximum' not in result.output
        assert '189' in result.output
        assert 'available' in result.output

    def test_sentinel_row_labeled_unexecutable(self):
        pairs = [_pair(14, 'btc', 'tao', 1.797e308)]
        collaterals = {'hk14': 1_000_000_000}
        result = _invoke(pairs, collaterals, ['--from', 'btc', '--to', 'tao', '--amount', '0.0005'])
        assert result.exit_code == 0, result.output
        assert 'unexecutable rate' in result.output
        # Sentinel rows must NOT count as available.
        assert 'available' not in result.output

    def test_all_above_max_emits_decrease_amount_hint(self):
        # Every routable miner's TAO leg exceeds max_swap. The aggregate end
        # message replaces the old up-front abort and stays actionable.
        pairs = [_pair(189, 'btc', 'tao', 1000.0)]  # 0.0005 BTC * 1000 = 0.5 TAO -> at max; bump amount past max
        collaterals = {'hk189': 10_000_000_000}
        result = _invoke(pairs, collaterals, ['--from', 'btc', '--to', 'tao', '--amount', '0.001'])
        assert result.exit_code == 0, result.output
        assert 'Decrease --amount' in result.output


class TestTaoToBtcSort:
    """Regression: quote.py sorted descending unconditionally; TAO→BTC needs
    ascending (lower TAO/BTC = better user price)."""

    def test_lower_rate_appears_first(self):
        pairs = [
            _pair(253, 'tao', 'btc', 1000.0),
            _pair(14, 'tao', 'btc', 999.0),
            _pair(189, 'tao', 'btc', 339.62),
            _pair(65, 'tao', 'btc', 340.12),
        ]
        collaterals = {'hk253': 10**12, 'hk14': 10**12, 'hk189': 10**12, 'hk65': 10**12}
        result = _invoke(pairs, collaterals, ['--from', 'tao', '--to', 'btc', '--amount', '0.1'])
        assert result.exit_code == 0, result.output
        uids_in_order = []
        for token in ('189', '65', '14', '253'):
            idx = result.output.find(token)
            assert idx >= 0, f'expected UID {token} in output:\n{result.output}'
            uids_in_order.append((idx, token))
        ordered = [t for _, t in sorted(uids_in_order)]
        assert ordered == ['189', '65', '14', '253'], (
            f'expected ascending-by-rate order [189, 65, 14, 253]; got {ordered}\n{result.output}'
        )


class TestTaoToBtcAbsurdLowRate:
    """Regression: a miner posting r=1e-08 TAO/BTC promises an impossible BTC
    payout (e.g. 0.1 TAO → 1e7 BTC). The ascending-rate sort would normally
    put it on top as "best price" — is_executable_rate's symmetric branch
    must catch it so the CLI labels it unexecutable, and crown ineligibility
    follows from the same predicate."""

    def test_sentinel_low_rate_labeled_unexecutable(self):
        pairs = [
            _pair(189, 'tao', 'btc', 339.62),
            _pair(193, 'tao', 'btc', 1e-08),
        ]
        collaterals = {'hk189': 10**12, 'hk193': 10**12}
        result = _invoke(pairs, collaterals, ['--from', 'tao', '--to', 'btc', '--amount', '0.1'])
        assert result.exit_code == 0, result.output
        assert 'unexecutable rate' in result.output
        # UID 189 still routable.
        assert '189' in result.output
        # The sentinel row's status must NOT be "available" — that was the
        # original #396 trap (ascending sort + permissive predicate would
        # have made UID 193 the default pick).
        sentinel_line = next((line for line in result.output.splitlines() if '193' in line), '')
        assert 'unexecutable' in sentinel_line, f'expected sentinel-row marked unexecutable; got: {sentinel_line!r}'
        assert 'available' not in sentinel_line


class TestQuoteAllSentinelSummary:
    """When every available miner posts an unexecutable rate, the summary
    must surface that explicitly so the user doesn't think it's a transient
    no-viable-amount problem."""

    def test_quote_summary_lists_all_sentinel_case(self):
        pairs = [
            _pair(14, 'btc', 'tao', 1.797e308),
            _pair(189, 'btc', 'tao', 1.797e308),
        ]
        collaterals = {'hk14': 10**12, 'hk189': 10**12}
        result = _invoke(pairs, collaterals, ['--from', 'btc', '--to', 'tao', '--amount', '0.0005'])
        assert result.exit_code == 0, result.output
        assert 'unexecutable rate (sentinel)' in result.output
        # Make sure the generic "try a smaller amount" branch isn't also emitted.
        assert 'try a smaller amount' not in result.output

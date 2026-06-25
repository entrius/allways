"""Tests for validator axon_handlers.handle_swap_confirm and handle_swap_reserve.

Covers every rejection branch plus the queued-confirmation path. The
vote_initiate success path is not unit-tested here — it requires mocking
extrinsic submission and is exercised end-to-end in integration testing.
These tests focus on the validation layer, which is the security-critical
surface users and miners can reach directly via the axon.
"""

import asyncio
import threading
from unittest.mock import MagicMock, patch

from allways.chain_providers.base import TransactionInfo
from allways.classes import MinerPair, Reservation
from allways.contract_client import ContractError
from allways.synapses import MinerActivateSynapse, SwapConfirmSynapse, SwapReserveSynapse
from allways.validator.axon_handlers import handle_miner_activate, handle_swap_confirm, handle_swap_reserve
from allways.validator.state_store import PendingConfirm, ReservationPin, ValidatorStateStore


def make_synapse(
    reservation_id: str = 'miner-hotkey',
    from_tx_hash: str = 'abc123',
    from_tx_proof: str = 'proof',
    from_address: str = 'bc1-user',
    to_address: str = '5user',
    from_chain: str = 'btc',
    to_chain: str = 'tao',
) -> SwapConfirmSynapse:
    return SwapConfirmSynapse(
        reservation_id=reservation_id,
        from_tx_hash=from_tx_hash,
        from_tx_proof=from_tx_proof,
        from_address=from_address,
        to_address=to_address,
        from_chain=from_chain,
        to_chain=to_chain,
    )


def make_commitment(
    from_chain: str = 'btc',
    to_chain: str = 'tao',
    counter_rate: float = 0.0029,
    counter_rate_str: str = '0.0029',
) -> MinerPair:
    return MinerPair(
        uid=1,
        hotkey='miner-hotkey',
        from_chain=from_chain,
        from_address='bc1-miner',
        to_chain=to_chain,
        to_address='5miner',
        rate=345.0,
        rate_str='345',
        counter_rate=counter_rate,
        counter_rate_str=counter_rate_str,
    )


def make_pin(
    from_chain: str = 'btc',
    to_chain: str = 'tao',
    miner_from_address: str = 'bc1-miner',
    miner_to_address: str = '5miner',
    rate_str: str = '345',
    counter_rate_str: str = '0.0029',
    reserve_block: int = 900,
    reserved_until: int = 2000,
) -> ReservationPin:
    """A reservation pin as event_watcher.record_reservation_pin would write."""
    return ReservationPin(
        miner_hotkey='miner-hotkey',
        reserve_block=reserve_block,
        from_chain=from_chain,
        to_chain=to_chain,
        rate_str=rate_str,
        counter_rate_str=counter_rate_str,
        miner_from_address=miner_from_address,
        miner_to_address=miner_to_address,
        reserved_until=reserved_until,
    )


def make_tx_info(
    *,
    confirmed: bool = True,
    confirmations: int = 6,
    block_number: int | None = 500,
) -> TransactionInfo:
    return TransactionInfo(
        tx_hash='abc123',
        confirmed=confirmed,
        sender='bc1-user',
        recipient='bc1-miner',
        amount=100_000,
        block_number=block_number,
        confirmations=confirmations,
    )


def make_reservation(
    *,
    from_addr: str = 'bc1-user',
    from_chain: str = 'btc',
    to_chain: str = 'tao',
    tao_amount: int = 345_000_000,
    from_amount: int = 100_000,
    to_amount: int = 345_000_000,
    reserved_until: int = 2000,
) -> Reservation:
    return Reservation(
        hash='reservation-hash',
        from_addr=from_addr,
        from_chain=from_chain,
        to_chain=to_chain,
        tao_amount=tao_amount,
        from_amount=from_amount,
        to_amount=to_amount,
        reserved_until=reserved_until,
    )


_DEFAULT_RESERVATION = object()


def make_validator(
    *,
    block: int = 1000,
    reserved_until: int = 2000,
    reservation_data: tuple | None = (345_000_000, 100_000, 345_000_000),
    reservation: Reservation | None | object = _DEFAULT_RESERVATION,
    providers: dict | None = None,
) -> MagicMock:
    """Build a Validator mock with default-happy contract/chain state.

    Individual tests override specific attributes to simulate each branch.
    reservation_data tuple mirrors the on-chain layout used by
    handle_swap_confirm: (tao_amount, source_amount, dest_amount).
    """
    validator = MagicMock()
    validator.block = block
    validator.axon_subtensor.get_current_block.return_value = block
    validator.config.netuid = 2
    validator.axon_lock = threading.Lock()

    contract = MagicMock()
    contract.get_miner_reserved_until.return_value = reserved_until
    contract.get_reservation_data.return_value = reservation_data
    if reservation is _DEFAULT_RESERVATION:
        reservation = make_reservation(reserved_until=reserved_until)
    contract.get_reservation.return_value = reservation
    validator.axon_contract_client = contract

    if providers is None:
        btc = MagicMock()
        btc.is_valid_address.return_value = True
        btc.verify_transaction.return_value = make_tx_info()
        btc.get_chain.return_value = MagicMock(min_confirmations=6)

        tao = MagicMock()
        tao.is_valid_address.return_value = True
        tao.get_chain.return_value = MagicMock(min_confirmations=12)

        providers = {'btc': btc, 'tao': tao}
    validator.axon_chain_providers = providers

    validator.state_store = MagicMock()
    # No reservation pin by default — handler falls back to the live
    # commitment. Tests exercising the pinned path set this explicitly.
    validator.state_store.get_reservation_pin.return_value = None
    validator.wallet = MagicMock()
    return validator


_DEFAULT = object()  # distinct from None so tests can request "no commitment" explicitly


def run_handler(validator, synapse, commitment=_DEFAULT):
    """Patch read_miner_commitment and drive the async handler synchronously.

    Omitting ``commitment`` yields the happy-path default; passing ``None``
    simulates a miner with no commitment on-chain.
    """
    cmt = make_commitment() if commitment is _DEFAULT else commitment
    with patch('allways.validator.axon_handlers.read_miner_commitment', return_value=cmt):
        return asyncio.run(handle_swap_confirm(validator, synapse))



# ===========================================================================
# handle_swap_reserve — reserve-time rate recompute
# ===========================================================================
#
# The reservation pins amounts but not the miner's rate. handle_swap_reserve
# must recompute to_amount/tao_amount from the commitment rate it reads at
# reserve time and reject a request whose user-submitted amounts don't match —
# otherwise a quote computed against a momentarily-bad rate gets locked in.


# btc→tao, from_amount=100_000 sat at rate '345' → to_amount/tao_amount.
_RESERVE_FROM_AMOUNT = 100_000
_RESERVE_TO_AMOUNT = 345_000_000
_RESERVE_TAO_AMOUNT = 345_000_000

# handle_swap_reserve computes the request hash from the miner's public key
# before the lock, so the reserve fixtures need a real SS58 (the //Alice key).
_RESERVE_MINER_SS58 = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'


def make_reserve_synapse(
    miner_hotkey: str = _RESERVE_MINER_SS58,
    tao_amount: int = _RESERVE_TAO_AMOUNT,
    from_amount: int = _RESERVE_FROM_AMOUNT,
    to_amount: int = _RESERVE_TO_AMOUNT,
    from_address: str = 'bc1-user',
    from_address_proof: str = 'proof',
    from_chain: str = 'btc',
    to_chain: str = 'tao',
) -> SwapReserveSynapse:
    return SwapReserveSynapse(
        miner_hotkey=miner_hotkey,
        tao_amount=tao_amount,
        from_amount=from_amount,
        to_amount=to_amount,
        from_address=from_address,
        from_address_proof=from_address_proof,
        block_anchor=1000,
        from_chain=from_chain,
        to_chain=to_chain,
    )


def make_reserve_validator(
    *,
    block: int = 1000,
    reserved_until: int = 0,
    collateral: int = 10_000_000_000,
    active: bool = True,
    has_swap: bool = False,
) -> MagicMock:
    """Validator mock with default-happy state for handle_swap_reserve.

    get_miner_snapshot returns (collateral, active, has_swap, reserved_until,
    deactivation_block); get_cooldown returns (strikes, last_expired).
    """
    validator = MagicMock()
    validator.block = block
    validator.axon_subtensor.get_current_block.return_value = block
    validator.config.netuid = 2
    validator.axon_lock = threading.Lock()

    contract = MagicMock()
    contract.get_miner_snapshot.return_value = (collateral, active, has_swap, reserved_until, 0)
    contract.get_cooldown.return_value = (0, 0)
    validator.axon_contract_client = contract

    btc = MagicMock()
    btc.verify_from_proof.return_value = True
    btc.get_balance.return_value = 10**18
    validator.axon_chain_providers = {'btc': btc, 'tao': MagicMock()}

    validator.bounds_cache = MagicMock()
    validator.bounds_cache.min_collateral.return_value = 0
    validator.bounds_cache.min_swap_amount.return_value = 0
    validator.bounds_cache.max_swap_amount.return_value = 0
    validator.bounds_cache.halted.return_value = False
    validator.wallet = MagicMock()
    return validator


def run_reserve_handler(validator, synapse, commitment=_DEFAULT):
    """Patch read_miner_commitment and drive handle_swap_reserve synchronously."""
    cmt = make_commitment() if commitment is _DEFAULT else commitment
    with patch('allways.validator.axon_handlers.read_miner_commitment', return_value=cmt):
        return asyncio.run(handle_swap_reserve(validator, synapse))


class TestReserveRateRecompute:
    # ------------------------------------------------------------------ #
    # exact match / within band                                            #
    # ------------------------------------------------------------------ #

    def test_accepts_when_amounts_match_commitment_rate(self):
        """A correctly-quoted reservation (exact match) passes and votes."""
        validator = make_reserve_validator()
        result = run_reserve_handler(validator, make_reserve_synapse())
        assert result.accepted is True
        validator.axon_contract_client.vote_reserve.assert_called_once()

    def test_accepts_within_default_slippage_band(self):
        """Rate dropped 1% after the user's quote — within the default 2% band."""
        validator = make_reserve_validator()
        # User quoted at rate 345 → to_amount = 345_000_000.
        # Rate moved to ~341.55 (1% down) → recomputed ≈ 341_550_000.
        # Simulate by patching the commitment rate so recomputed < quoted by 1%.
        moved = make_commitment()
        moved.rate_str = str(345 * 0.99)  # '341.55'
        moved.rate = 345 * 0.99
        # User's synapse still reflects the old 345 quote
        result = run_reserve_handler(validator, make_reserve_synapse(), commitment=moved)
        assert result.accepted is True
        validator.axon_contract_client.vote_reserve.assert_called_once()

    def test_rejects_beyond_default_slippage_band(self):
        """Rate dropped 3% after the user's quote — exceeds the default 2% band."""
        validator = make_reserve_validator()
        # Rate moved to ~334.65 (3% down) → recomputed ≈ 334_650_000.
        moved = make_commitment()
        moved.rate_str = str(345 * 0.97)
        moved.rate = 345 * 0.97
        # User's synapse still reflects the old 345 quote
        result = run_reserve_handler(validator, make_reserve_synapse(), commitment=moved)
        assert result.accepted is False
        assert 'slippage band' in result.rejection_reason
        validator.axon_contract_client.vote_reserve.assert_not_called()

    def test_accepts_favorable_move_recomputed_above_quoted(self):
        """A favorable rate move (recomputed > quoted) always passes.

        The user quoted conservatively at 330; the miner's rate is still 345
        so recomputed (345_000_000) > quoted (330_000_000) — always passes.
        """
        validator = make_reserve_validator()
        # User quoted at 330 rate → to_amount = 330_000_000
        conservative_rate = 330.0
        conservative_to = int(_RESERVE_FROM_AMOUNT * conservative_rate * 10**9 // 10**8)  # sat→rao
        synapse = make_reserve_synapse(to_amount=conservative_to, tao_amount=conservative_to)
        # Commitment still at 345 → recomputed = 345_000_000 > conservative_to
        result = run_reserve_handler(validator, synapse)
        assert result.accepted is True

    def test_rejects_swap79_scale_gap(self):
        """A quote ~7x above the recomputed amount (swap-79 scenario) rejects."""
        validator = make_reserve_validator()
        # User quoted at 345 rate but miner moved to 49 (7x gap)
        moved = make_commitment()
        moved.rate_str = '49'
        moved.rate = 49.0
        # from_amount=100_000 sat at rate '49' → 49_000_000 rao
        result = run_reserve_handler(validator, make_reserve_synapse(), commitment=moved)
        assert result.accepted is False
        assert 'slippage band' in result.rejection_reason
        validator.axon_contract_client.vote_reserve.assert_not_called()

    def test_tighter_user_slippage_rejects_small_drift(self):
        """Rate dropped 0.5% — within the default 2% band but rejected with
        --slippage 0.3% (30 bps)."""
        validator = make_reserve_validator()
        # Rate moved to 99.5% of 345 → recomputed ≈ 99.5% of 345_000_000
        moved = make_commitment()
        moved.rate_str = str(345 * 0.995)
        moved.rate = 345 * 0.995
        synapse = make_reserve_synapse()
        synapse.slippage_bps = 30  # 0.3%
        result = run_reserve_handler(validator, synapse, commitment=moved)
        assert result.accepted is False
        assert 'slippage band' in result.rejection_reason

    def test_wider_user_slippage_accepts_large_drift(self):
        """Rate dropped 4% — the default 2% band rejects it, but --slippage 5%
        (500 bps) accepts it."""
        validator = make_reserve_validator()
        # Rate moved to 96% of 345 → recomputed ≈ 96% of 345_000_000
        moved = make_commitment()
        moved.rate_str = str(345 * 0.96)
        moved.rate = 345 * 0.96
        synapse = make_reserve_synapse()
        synapse.slippage_bps = 500  # 5%
        result = run_reserve_handler(validator, synapse, commitment=moved)
        assert result.accepted is True

    def test_slippage_max_bps_clamp_applied(self):
        """slippage_bps above RESERVE_SLIPPAGE_MAX_BPS is clamped, not errored,
        and the cap is tight enough to still gate a wildly-off quote.

        MAX_BPS must stay below 10_000 (100%) — at ≥10_000 the threshold goes
        non-positive and the gate becomes a no-op, which is what let swap-550's
        operator-test traffic settle a 71%-off quote without rejection. The
        clamp should accept the high request value but cap it to a band that
        still bites real misquotes.
        """
        from allways.constants import RESERVE_SLIPPAGE_MAX_BPS

        assert RESERVE_SLIPPAGE_MAX_BPS < 10_000, 'cap ≥10_000 makes quote_within_slippage a no-op — see swap 550'

        validator = make_reserve_validator()
        # Rate dropped 90% → recomputed = 10% of 345_000_000 = 34_500_000.
        # With the cap at 25% (or anything <90%), this gap is still rejected
        # even after the user-requested slippage gets clamped down.
        moved = make_commitment()
        moved.rate_str = str(345 * 0.10)
        moved.rate = 345 * 0.10
        synapse = make_reserve_synapse()
        synapse.slippage_bps = RESERVE_SLIPPAGE_MAX_BPS + 1_000_000  # absurdly large — clamped
        result = run_reserve_handler(validator, synapse, commitment=moved)
        assert result.accepted is False
        assert 'slippage band' in result.rejection_reason

    # ------------------------------------------------------------------ #
    # tao_amount internal consistency                                      #
    # ------------------------------------------------------------------ #

    def test_rejects_inconsistent_tao_amount(self):
        """tao_amount must equal derive_tao_leg(from_amount, to_amount) — an
        inconsistent triple is rejected before the slippage gate."""
        validator = make_reserve_validator()
        synapse = make_reserve_synapse(tao_amount=_RESERVE_TAO_AMOUNT + 5)
        result = run_reserve_handler(validator, synapse)
        assert result.accepted is False
        assert 'inconsistent' in result.rejection_reason
        validator.axon_contract_client.vote_reserve.assert_not_called()

    # ------------------------------------------------------------------ #
    # rate moved after quote                                               #
    # ------------------------------------------------------------------ #

    def test_rejects_when_rate_moved_after_quote(self):
        """User quotes against rate 345, miner re-committed to 300 — the quote
        is now ~15% above recomputed, outside the default 2% band."""
        validator = make_reserve_validator()
        moved = make_commitment()
        moved.rate_str = '300'
        moved.rate = 300.0
        # User still submits the amounts from the old 345 quote.
        result = run_reserve_handler(validator, make_reserve_synapse(), commitment=moved)
        assert result.accepted is False
        assert 'slippage band' in result.rejection_reason
        validator.axon_contract_client.vote_reserve.assert_not_called()

    def test_recompute_runs_before_vote(self):
        """A stale quote (rate moved 7x) is rejected before vote_reserve is called."""
        validator = make_reserve_validator()
        # Rate moved from 345 down to 49 — user's old quote is ~7x above recomputed.
        moved = make_commitment()
        moved.rate_str = '49'
        moved.rate = 49.0
        run_reserve_handler(validator, make_reserve_synapse(), commitment=moved)
        validator.axon_contract_client.vote_reserve.assert_not_called()


# ---------------------------------------------------------------------------
# Reservation pin — rate-swing and address-theft regressions
# ---------------------------------------------------------------------------


# A valid SS58 (Alice from the substrate dev keyring) — tests that reach the
# vote_initiate path need a parseable hotkey for the request_hash keypair.
VALID_MINER_SS58 = '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY'


class TestReservationPin:
    """handle_swap_reserve still writes a reservation pin synchronously. (The confirm-side pin consumer
    moved on-chain in B2 — submit_swap_claim copies the pinned terms from the Reservation; those tests
    now live in test_axon_solana_handlers.py.)"""

    def test_successful_reserve_pins_synchronously(self):
        """handle_swap_reserve writes the pin inline so a fast SwapConfirm finds it."""
        validator = make_reserve_validator()
        validator.axon_contract_client.get_miner_reserved_until.return_value = 1050

        run_reserve_handler(validator, make_reserve_synapse())

        validator.axon_contract_client.vote_reserve.assert_called_once()
        validator.state_store.upsert_reservation_pin.assert_called_once()
        pin = validator.state_store.upsert_reservation_pin.call_args[0][0]
        assert pin.rate_str == '345'
        assert pin.miner_from_address == 'bc1-miner'
        assert pin.miner_to_address == '5miner'
        assert pin.reserved_until == 1050


class TestReserveExecutabilityGate:
    def test_handle_swap_reserve_rejects_sentinel_rate(self):
        """An executable-bounded rate that is unexecutable under cached bounds
        must be rejected at reserve time so no reservation is voted on."""
        validator = make_reserve_validator()
        # Bounds that make BTC/TAO rate 1e9 unexecutable on the BTC→TAO leg.
        validator.bounds_cache.min_swap_amount.return_value = 500_000_000
        validator.bounds_cache.max_swap_amount.return_value = 5_000_000_000
        unexecutable = make_commitment()
        unexecutable.rate = 1e9
        unexecutable.rate_str = '1e9'

        result = run_reserve_handler(validator, make_reserve_synapse(), commitment=unexecutable)

        assert result.accepted is False
        assert 'not executable' in result.rejection_reason
        validator.axon_contract_client.vote_reserve.assert_not_called()


class TestSourceBalanceLock:
    """The source-balance check must serialise on axon_lock for a substrate
    source (TAO) but stay lock-free for an HTTP source (BTC) — otherwise the
    TAO get_balance races the lock-protected readers and trips the substrate
    `cannot call recv while another thread is already running recv` error."""

    def test_provider_uses_substrate_flags(self):
        """TAO provider hits the shared websocket; BTC is HTTP and lock-free."""
        from allways.chain_providers.base import ChainProvider
        from allways.chain_providers.bitcoin import BitcoinProvider
        from allways.chain_providers.subtensor import SubtensorProvider

        assert ChainProvider.uses_substrate is False
        assert SubtensorProvider.uses_substrate is True
        assert BitcoinProvider.uses_substrate is False

    def test_tao_source_balance_check_holds_axon_lock(self):
        """A TAO-sourced reserve must acquire axon_lock around get_balance."""
        from allways.utils.rate import derive_tao_leg
        from allways.validator.axon_handlers import recompute_reserve_amounts

        validator = make_reserve_validator()
        lock = validator.axon_lock

        tao = MagicMock()
        tao.uses_substrate = True
        tao.verify_from_proof.return_value = True
        # Record whether the lock is held at the moment get_balance runs.
        held = {}

        def _get_balance(_addr):
            held['locked'] = not lock.acquire(blocking=False)
            if not held['locked']:
                lock.release()
            return 10**18

        tao.get_balance.side_effect = _get_balance
        validator.axon_chain_providers = {'tao': tao, 'btc': MagicMock()}

        # The balance lookup now runs after the quote checks, so the request must
        # carry a self-consistent quote to reach it. Derive the amounts from the
        # same functions the handler uses so it passes exactly.
        commitment = make_commitment(from_chain='tao', to_chain='btc')
        from_amount = _RESERVE_TAO_AMOUNT
        to_amount = recompute_reserve_amounts(commitment, 'tao', 'btc', from_amount)
        tao_amount = derive_tao_leg('tao', from_amount, 'btc', to_amount)
        synapse = make_reserve_synapse(
            from_chain='tao',
            to_chain='btc',
            from_address='5user',
            from_amount=from_amount,
            to_amount=to_amount,
            tao_amount=tao_amount,
        )
        run_reserve_handler(validator, synapse, commitment=commitment)

        assert held.get('locked') is True

    def test_btc_source_balance_check_is_lock_free(self):
        """A BTC-sourced reserve must NOT hold axon_lock during get_balance, so
        a slow Esplora call can't stall the lock-protected forward loop."""
        validator = make_reserve_validator()
        lock = validator.axon_lock

        btc = validator.axon_chain_providers['btc']
        btc.uses_substrate = False
        held = {}

        def _get_balance(_addr):
            held['locked'] = not lock.acquire(blocking=False)
            if not held['locked']:
                lock.release()
            return 10**18

        btc.get_balance.side_effect = _get_balance

        run_reserve_handler(validator, make_reserve_synapse())

        assert held.get('locked') is False


class TestHaltFastReject:
    """A halted system rejects reservations without submitting any extrinsic."""

    def test_halted_rejects_without_voting(self):
        validator = make_reserve_validator()
        validator.bounds_cache.halted.return_value = True
        result = run_reserve_handler(validator, make_reserve_synapse())
        assert result.accepted is False
        assert 'halt' in (result.rejection_reason or '').lower()
        validator.axon_contract_client.vote_reserve.assert_not_called()

    def test_halted_short_circuits_before_substrate_work(self):
        validator = make_reserve_validator()
        validator.bounds_cache.halted.return_value = True
        with patch('allways.validator.axon_handlers.read_miner_commitment') as read_cmt:
            asyncio.run(handle_swap_reserve(validator, make_reserve_synapse()))
        read_cmt.assert_not_called()

"""TAO as a second hub: the multi-hub rate model, per-hub bounds, and the acceptance path.

The charter's falsifiable acceptance: a TAO↔ETH quote posts, prices, bounds-checks, and a swap
routes — entirely off-chain (the Solana program is already pair-agnostic), so everything here runs
against the same shared intake/rate utils the CLI, validator router, and scoring use. The
"no program diff / no provider diff" half of the acceptance is a review fact (git diff), not a test.
"""

import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace

import bittensor as bt
import pytest
from solders.keypair import Keypair as SolKeypair

from allways.chains import canonical_pair
from allways.cli.swap_commands.numeraire import derive_hub_numeraire_quotes
from allways.cli.swap_commands.swap_intake import (
    MinerCandidate,
    bounds_from_config,
    candidate_miners,
    compute_intake_amounts,
    hub_bounds,
    leg_value,
    max_intake_from_amount,
    required_collateral,
    select_best_miner,
    unviable_reason,
    viable_intakes,
)
from allways.constants import (
    DIRECTION_POOLS,
    HUB_CHAINS,
    LAUNCH_ALPHAS,
    LAUNCH_PAIRS,
    MINER_POOL_SHARE,
    RATE_PRECISION,
    declarable_backings,
    hub_leg,
    is_hub,
)
from allways.utils.rate import is_executable_rate, min_executable_hub_leg
from allways.validator.reserve_engine import _best_offer, reserve_on_behalf
from allways.validator.state_store import ValidatorStateStore

TAO = 1_000_000_000  # 1 TAO in rao (9 dec)
SOL = 1_000_000_000  # 1 SOL in lamports (9 dec)
ETH = 10**18  # 1 ETH in wei (18 dec)
RATE = '0.05'  # canonical 'ETH per 1 TAO' (~$200 TAO vs ~$4000 ETH)
TAO_MIN, TAO_MAX = TAO // 10, TAO  # deploy-config shape: 0.1 τ / 1 τ, in rao


class TestHubSet:
    def test_hub_membership(self):
        assert is_hub('sol') and is_hub('tao')
        assert not is_hub('btc') and not is_hub('eth')

    def test_hub_leg_anchors(self):
        assert hub_leg('sol', 'btc') == 'sol'
        assert hub_leg('eth', 'tao') == 'tao'
        # hub↔hub: HUB_CHAINS order wins — sol↔tao stays SOL-anchored (grandfathered).
        assert hub_leg('tao', 'sol') == 'sol'
        assert hub_leg('btc', 'eth') is None
        assert hub_leg('sn7', 'avax') == 'sn7'
        assert hub_leg('sn7', 'sn74') == 'sn7'

    def test_declarable_backings_follow_leg_families(self):
        assert declarable_backings('sn7', 'avax') == ['tao']
        assert declarable_backings('sol', 'sn7') == ['sol', 'tao']
        assert declarable_backings('sn7', 'sn74') == ['tao']

    def test_hub_leg_is_the_canonical_source(self):
        for a, b in (('tao', 'eth'), ('eth', 'tao'), ('sol', 'tao'), ('btc', 'sol')):
            assert canonical_pair(a, b)[0] == hub_leg(a, b)

    def test_launch_pairs_cover_every_hub_spoke_once(self):
        assert ('sol', 'tao') in LAUNCH_PAIRS  # counted once, under its anchor
        assert ('tao', 'sol') not in LAUNCH_PAIRS
        assert ('tao', 'eth') in LAUNCH_PAIRS and ('tao', 'btc') in LAUNCH_PAIRS
        assert len(LAUNCH_PAIRS) == len(set(LAUNCH_PAIRS))
        assert all(hub in HUB_CHAINS and spoke != hub for hub, spoke in LAUNCH_PAIRS)
        assert all(pair == canonical_pair(*pair) for pair in LAUNCH_PAIRS)
        assert all((hub, alpha) in LAUNCH_PAIRS for hub in HUB_CHAINS for alpha in LAUNCH_ALPHAS)

    def test_direction_pools_span_both_families_and_conserve(self):
        assert len(DIRECTION_POOLS) == 2 * len(LAUNCH_PAIRS)
        assert ('tao', 'eth') in DIRECTION_POOLS and ('eth', 'tao') in DIRECTION_POOLS
        assert sum(DIRECTION_POOLS.values()) == pytest.approx(MINER_POOL_SHARE)

    def test_hub_bounds_reads_the_anchor_leg(self):
        bounds = {'sol': (5, 6), 'tao': (TAO_MIN, TAO_MAX)}
        assert hub_bounds(bounds, 'tao', 'eth') == (TAO_MIN, TAO_MAX)
        assert hub_bounds(bounds, 'eth', 'tao') == (TAO_MIN, TAO_MAX)
        # sol↔tao is SOL-anchored even though a tao-backed quote's SIZE gates on the TAO bounds.
        assert hub_bounds(bounds, 'tao', 'sol') == (5, 6)
        assert hub_bounds(bounds, 'btc', 'eth') == (0, 0)

    def test_bounds_from_config_carries_both_hubs(self):
        cfg = SimpleNamespace(
            min_swap_amount=1, max_swap_amount=2, tao_min_swap_amount=TAO_MIN, tao_max_swap_amount=TAO_MAX
        )
        assert bounds_from_config(cfg) == {'sol': (1, 2), 'tao': (TAO_MIN, TAO_MAX)}


class TestTaoHubExecutability:
    """is_executable_rate anchored on the TAO leg: bounds are rao, the spoke-side
    routability question is the same one the SOL hub asks."""

    def test_sane_rate_routes_both_directions(self):
        assert is_executable_rate(0.05, 'tao', 'eth', TAO_MIN, TAO_MAX) is True
        assert is_executable_rate(0.05, 'eth', 'tao', TAO_MIN, TAO_MAX) is True

    def test_squat_rate_rejected(self):
        # An absurdly LOW canonical rate (lowest wins the eth→tao sort) makes even the
        # ETH dust floor overshoot tao_max — the crown-squat, unroutable at any size.
        assert is_executable_rate(1e-15, 'eth', 'tao', TAO_MIN, TAO_MAX) is False
        assert is_executable_rate(1e-15, 'tao', 'eth', TAO_MIN, TAO_MAX) is False

    def test_unset_bounds_are_permissive(self):
        assert is_executable_rate(1e-15, 'eth', 'tao', 0, 0) is True

    def test_min_hub_leg_is_rao_denominated(self):
        # tao source: the smallest in-bounds hub leg is tao_min itself.
        assert min_executable_hub_leg(0.05, 'tao', 'eth', TAO_MIN, TAO_MAX) == TAO_MIN
        # eth source: smallest fundable ETH whose TAO leg clears tao_min → exactly tao_min back.
        assert min_executable_hub_leg(0.05, 'eth', 'tao', TAO_MIN, TAO_MAX) == pytest.approx(TAO_MIN)

    def test_sol_tao_pair_still_anchors_on_sol(self):
        # Grandfathered hub↔hub: the executability anchor is the SOL leg, exactly as before.
        sol_min, sol_max = 100_000_000, 500_000_000
        assert is_executable_rate(2.0, 'tao', 'sol', sol_min, sol_max) is True
        assert is_executable_rate(1e-12, 'tao', 'sol', sol_min, sol_max) is False


class TestTaoHubIntake:
    def test_tao_to_eth_amounts(self):
        # 1 TAO at 0.05 ETH/TAO → 0.05 ETH; the tao-backed collateral leg IS the source.
        a = compute_intake_amounts('tao', 'eth', TAO, RATE, backing='tao')
        assert a.to_amount == ETH // 20
        assert a.collateral_amount == TAO

    def test_eth_to_tao_amounts(self):
        # 0.05 ETH at 0.05 ETH/TAO → 1 TAO; the tao leg is the dest.
        a = compute_intake_amounts('eth', 'tao', ETH // 20, RATE, backing='tao')
        assert a.to_amount == TAO
        assert a.collateral_amount == TAO

    def test_spoke_spoke_pair_rejected(self):
        with pytest.raises(ValueError, match='anchor leg'):
            compute_intake_amounts('btc', 'eth', 100, '20', backing='btc')

    def test_leg_value_binds_an_exact_leg_without_a_provider(self):
        # Exact first, either side — and sn7<->tao keeps the exact TAO leg, never a spot read.
        assert leg_value('tao', 'tao', TAO, 'eth', ETH // 20) == TAO
        assert leg_value('tao', 'eth', ETH // 20, 'tao', TAO) == TAO
        assert leg_value('tao', 'sn7', 5 * TAO, 'tao', TAO) == TAO
        assert leg_value('sol', 'sol', SOL, 'sn7', 5 * TAO) == SOL

    def test_leg_value_prices_a_declared_alpha_leg_at_spot(self):
        sn7 = SimpleNamespace(value_rao=lambda amount: amount * 3)
        assert leg_value('tao', 'sol', SOL, 'sn7', 5 * TAO, {'sn7': sn7}) == 15 * TAO
        with pytest.raises(ValueError, match='provider'):
            leg_value('tao', 'sol', SOL, 'sn7', 5 * TAO)
        with pytest.raises(ValueError, match='no leg'):
            leg_value('tao', 'sol', SOL, 'avax', 1, {'sn7': sn7})

    def test_sol_to_sn7_is_sized_by_its_backing(self):
        sn7 = SimpleNamespace(value_rao=lambda amount: 7 * TAO)
        declared = compute_intake_amounts('sol', 'sn7', SOL, RATE, backing='tao', providers={'sn7': sn7})
        assert declared.collateral_amount == 7 * TAO
        assert compute_intake_amounts('sol', 'sn7', SOL, RATE, backing='sol').collateral_amount == SOL

    def test_selectors_route_a_declared_leg_only_with_its_provider(self):
        sn7 = SimpleNamespace(value_rao=lambda amount: 7 * TAO)
        offer = MinerCandidate(object(), RATE, required_collateral(7 * TAO), backing='tao')
        bounds = {'tao': (TAO_MIN, 10 * TAO)}
        best = select_best_miner([offer], 'sol', 'sn7', SOL, 0, 0, bounds, {'sn7': sn7})
        assert best is not None and best[1].collateral_amount == 7 * TAO
        assert select_best_miner([offer], 'sol', 'sn7', SOL, 0, 0, bounds) is None
        assert 'provider' in unviable_reason([offer], 'sol', 'sn7', SOL, 0, 0, bounds)

    def test_exact_leg_selection_never_reads_a_price(self):
        def boom(amount):
            raise AssertionError('exact leg priced at spot')

        offer = MinerCandidate(object(), RATE, required_collateral(TAO), backing='tao')
        assert select_best_miner(
            [offer], 'tao', 'eth', TAO, TAO_MIN, TAO_MAX, None, {'tao': SimpleNamespace(value_rao=boom)}
        )

    def test_viability_gates_on_rao_bounds(self):
        bounds = {'sol': (0, 0), 'tao': (TAO_MIN, TAO_MAX)}
        funded = MinerCandidate(object(), RATE, required_collateral(TAO), backing='tao')
        viable = viable_intakes([funded], 'tao', 'eth', TAO, TAO_MIN, TAO_MAX, bounds)
        assert len(viable) == 1
        # Above tao_max (2 TAO leg) → the same candidate is unviable at that size.
        assert viable_intakes([funded], 'tao', 'eth', 2 * TAO, TAO_MIN, TAO_MAX, bounds) == []
        # Underfunded purse (below the 1.1× floor for the tao leg) → unviable.
        thin = MinerCandidate(object(), RATE, TAO, backing='tao')
        assert viable_intakes([thin], 'tao', 'eth', TAO, TAO_MIN, TAO_MAX, bounds) == []

    def test_max_intake_inverts_the_tao_dest_leg(self):
        # eth→tao: the bounded leg is the dest; depth must invert to_amount exactly.
        cand = MinerCandidate(object(), RATE, required_collateral(TAO), backing='tao')
        bounds = {'tao': (TAO_MIN, TAO_MAX)}
        cap = max_intake_from_amount(cand, 'eth', 'tao', TAO_MIN, TAO_MAX, bounds)
        assert cap > 0
        assert compute_intake_amounts('eth', 'tao', cap, RATE, backing='tao').to_amount <= TAO
        assert compute_intake_amounts('eth', 'tao', cap + 1, RATE, backing='tao').to_amount > TAO


class TestHubNumeraireQuotes:
    def test_tao_hub_derivation(self):
        specs = derive_hub_numeraire_quotes('tao', 'TAOADDR', {'eth': (0.05, 'ETHADDR'), 'btc': (0.003, 'BTCADDR')})
        assert {(s.from_chain, s.to_chain) for s in specs} == {
            ('tao', 'eth'),
            ('eth', 'tao'),
            ('tao', 'btc'),
            ('btc', 'tao'),
        }
        by_dir = {(s.from_chain, s.to_chain): s for s in specs}
        assert by_dir[('tao', 'eth')].from_addr == 'TAOADDR'
        assert by_dir[('eth', 'tao')].to_addr == 'TAOADDR'
        assert by_dir[('tao', 'eth')].rate == pytest.approx(0.05)

    def test_hub_entry_in_specs_is_skipped(self):
        specs = derive_hub_numeraire_quotes('tao', 'T', {'tao': (1.0, 'T'), 'eth': (0.05, 'E')})
        assert {(s.from_chain, s.to_chain) for s in specs} == {('tao', 'eth'), ('eth', 'tao')}


# ─── Acceptance: a TAO↔ETH quote posts, prices, bounds-checks, and a swap routes ─────────
# Mirrors test_reserve_engine's fake-client pattern; no chain, no provider, no program.

HK = bt.Keypair.create_from_seed('0x' + '22' * 32)
HOTKEY = HK.ss58_address
MINER_PK = SolKeypair().pubkey()
HOTKEY_BYTES = bytes.fromhex(HK.public_key.hex())
BINDING_SIG = HK.sign(bytes(MINER_PK))
USER_PK = str(SolKeypair().pubkey())
FUTURE = 9_999_999_999


class TaoHubClient:
    """A miner quoting tao→eth, tao-backed, with a locked attested TAO bond."""

    def __init__(self):
        self.quote = SimpleNamespace(
            miner=MINER_PK,
            rate=int(float(RATE) * RATE_PRECISION),
            from_chain='tao',
            to_chain='eth',
            collateral_chain='tao',
            miner_from_addr='minerTAOaddr',
        )
        self.miner_state = SimpleNamespace(active=True, has_active_swap=False, collateral=0)
        self.attestation = SimpleNamespace(locked=True, effective_balance=required_collateral(TAO))
        self.calls = []

    def get_hotkey_binding(self, hotkey_bytes):
        return SimpleNamespace(miner=MINER_PK)

    def get_binding(self, miner):
        return SimpleNamespace(miner=MINER_PK, hotkey=HOTKEY_BYTES, hotkey_sig=BINDING_SIG)

    def get_all(self, account):
        return [(str(MINER_PK), self.quote)]

    def get_miner_state(self, miner):
        return self.miner_state

    def get_pool(self, miner, backing='sol'):
        # Per-(miner, hub) contest slots since v3.1 — no open pool on either purse here.
        return None

    def get_quote(self, miner, from_chain, to_chain, backing='sol'):
        return self.quote if backing == 'tao' else None

    def get_quotes_for_direction(self, miner, from_chain, to_chain):
        return [self.quote]

    def get_bond_attestation(self, miner, chain='tao'):
        return self.attestation if chain == 'tao' else None

    def get_config(self):
        return SimpleNamespace(
            min_swap_amount=0, max_swap_amount=0, tao_min_swap_amount=TAO_MIN, tao_max_swap_amount=TAO_MAX
        )

    def open_or_request(self, miner, from_chain, to_chain, backing='sol'):
        self.calls.append(('open_or_request', from_chain, to_chain, backing))
        return 'sig-tao-eth'


class TestTaoEthAcceptance:
    def test_quote_prices_and_selector_routes(self):
        client = TaoHubClient()
        cands = candidate_miners(client, 'tao', 'eth')
        assert len(cands) == 1 and cands[0].backing == 'tao'
        assert cands[0].collateral == required_collateral(TAO)  # the attested rao purse, not lamports

        bounds = bounds_from_config(client.get_config())
        min_swap, max_swap = hub_bounds(bounds, 'tao', 'eth')
        best = select_best_miner(cands, 'tao', 'eth', TAO, min_swap, max_swap, bounds)
        assert best is not None
        cand, amts = best
        assert amts.to_amount == ETH // 20  # priced off the canonical 'ETH per 1 TAO' quote
        assert amts.collateral_amount == TAO  # the contract's backing-leg notional, in rao

    def test_routed_offer_prices_a_declared_alpha_leg_through_the_validator_providers(self):
        client = TaoHubClient()
        client.quote.from_chain, client.quote.to_chain = 'sol', 'sn7'
        sn7 = SimpleNamespace(value_rao=lambda amount: TAO // 2)  # inside the fixture's TAO bounds
        bounds = bounds_from_config(client.get_config())
        offer, _ = _best_offer(client, MINER_PK, client.miner_state, 'sol', 'sn7', SOL, bounds, {'sn7': sn7})
        assert offer == (client.quote, 'tao')
        offer, why = _best_offer(client, MINER_PK, client.miner_state, 'sol', 'sn7', SOL, bounds)
        assert offer is None and 'provider' in why

    def test_routed_reservation_bids_the_tao_backing(self):
        client = TaoHubClient()
        store = ValidatorStateStore(db_path=Path(tempfile.mkdtemp()) / 'state.db')
        validator = SimpleNamespace(
            solana_client=client, axon_lock=threading.RLock(), state_store=store, axon_assets={}
        )
        result = reserve_on_behalf(validator, HOTKEY, 'tao', 'eth', USER_PK, 'userTAOaddr', 'userETHaddr', TAO)
        assert result.ok, result.reason
        assert client.calls == [('open_or_request', 'tao', 'eth', 'tao')]

    def test_oversize_swap_rejected_in_rao(self):
        client = TaoHubClient()
        store = ValidatorStateStore(db_path=Path(tempfile.mkdtemp()) / 'state.db')
        validator = SimpleNamespace(
            solana_client=client, axon_lock=threading.RLock(), state_store=store, axon_assets={}
        )
        result = reserve_on_behalf(validator, HOTKEY, 'tao', 'eth', USER_PK, 'userTAOaddr', 'userETHaddr', 3 * TAO)
        assert not result.ok
        assert 'max swap' in result.reason

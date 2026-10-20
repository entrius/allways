"""Source tx hash spelling and claimed-deposit tracking, run through the real chain canonicalizers.

A swap key hashes the raw from_tx_hash string, so one deposit spelled two ways (``0x…lower`` and
``0X…UPPER``) would back two swaps and be paid twice. Every claim must carry its chain's one canonical
spelling, and every ledger keys the deposit case-blind. Covers the canonicalizers, the claim relay
(``confirm_deposit``), the attest and confirm rules (``decide`` over ``WorldContext``) and the World's
event-built ledgers. The other rebuild tests stub ``canonical_tx_hash`` as identity; nothing here does.
"""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from allways.assets import ASSET_REGISTRY
from allways.assets.asset import TransactionInfo
from allways.assets.evm import EvmChain
from allways.assets.tao import Tao
from allways.chains import get_chain_def
from allways.solana.client import swap_key_from_tx_hash
from allways.utils.rate import expected_swap_amounts
from allways.validator.actuator.swap_context import SwapMemory
from allways.validator.actuator.swaps import Decision, attest_reject_reason, decide
from allways.validator.serving.requests import confirm_deposit
from allways.validator.serving.store import ServingStore
from allways.validator.store.db import Store
from allways.validator.world.snapshot import DepositClaim, Fulfillment, PayoutRoute, World, tx_key
from tests.validator.fakes import Ctx
from tests.validator.test_reserve_engine import HOTKEY, _confirm_reservation, _ConfirmClient
from tests.validator.test_swap_rules import RecordingProvider, Rules, _Context, make_reservation, make_swap
from tests.validator.test_world import MINER as WORLD_MINER
from tests.validator.test_world import NOW as WORLD_NOW
from tests.validator.test_world import _Client, _ev, _reader

HEX = '9dA0' * 16  # 64 hex chars, mixed case
SOL_SIG = '5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW'
CREATED_AT = 1000
FRESH = CREATED_AT + 5
URL_SUFFIXES = ('#1', '#2', '?x', '/', '%20', ' ')


@pytest.fixture(autouse=True)
def mainnet_env(monkeypatch):
    for spec in ASSET_REGISTRY:
        prefix = get_chain_def(spec.chain_id).env_prefix
        if prefix:
            monkeypatch.setenv(f'{prefix}_NETWORK', 'mainnet')


def _build(spec):
    available = {'solana_rpc_url': 'http://offline.invalid', 'solana_keypair': MagicMock(), 'subtensor': MagicMock()}
    return spec.cls(**{name: available[name] for name in spec.kwarg_names if name in available})


def _spec(chain_id):
    return next(s for s in ASSET_REGISTRY if s.chain_id == chain_id)


def _hex_hash_specs():
    """Every source asset whose chain spells hashes as 0x-hex: each EVM network, TAO and one alpha."""
    out, alpha_seen = [], False
    for spec in ASSET_REGISTRY:
        if spec.chain_id.startswith('sn'):
            if alpha_seen:
                continue
            alpha_seen = True
        out.append(spec)
    return [s for s in out if isinstance(_build(s).chain, (EvmChain, Tao))]


HEX_SPECS = _hex_hash_specs()
BTC_SPEC, SOL_SPEC, ETH_SPEC = _spec('btc'), _spec('sol'), _spec('eth')
# One per hash family: UTXO (bare hex), EVM native, ERC-20, TAO, alpha.
FAMILY_SPECS = [BTC_SPEC] + [s for s in HEX_SPECS if s.chain_id in ('eth', 'baseusdc', 'tao') or s.chain_id[:2] == 'sn']


def _canonical(spec):
    return HEX.lower() if spec is BTC_SPEC else '0x' + HEX.lower()


def _variants(spec):
    """Every honest spelling of the one deposit the RPC would accept, canonical one excluded."""
    if spec is BTC_SPEC:
        return [HEX, HEX.upper()]
    return ['0x' + HEX, '0x' + HEX.upper(), '0X' + HEX, '0X' + HEX.lower()]


def _junk(spec):
    canonical = _canonical(spec)
    wrong_prefix = '0x' + HEX if spec is BTC_SPEC else HEX
    return [canonical + s for s in URL_SUFFIXES + ('\n',)] + [' ' + canonical, wrong_prefix, canonical[:-1], '']


def _family_cases(spellings):
    return [pytest.param(s, h, id=f'{s.chain_id}-{h!r}') for s in FAMILY_SPECS for h in spellings(s)]


# ── canonicalizers ───────────────────────────────────────────────────────────


def test_hex_hash_specs_cover_every_evm_network_and_tao():
    ids = {s.chain_id for s in HEX_SPECS}
    assert {'eth', 'baseusdc', 'tao'} <= ids and any(i.startswith('sn') for i in ids)


@pytest.mark.parametrize('spec', HEX_SPECS, ids=lambda s: s.chain_id)
def test_hex_chains_collapse_every_casing_to_lowercase_0x(spec):
    chain = _build(spec).chain
    canonical = '0x' + HEX.lower()
    for spelling in _variants(spec) + [canonical]:
        assert chain.canonical_tx_hash(spelling) == canonical
    for bad in (HEX, '0x' + HEX[:-1], '0x' + HEX + '00', '0x' + HEX[:-1] + 'g', '0x' + HEX + '\n', '', '123-4'):
        assert chain.canonical_tx_hash(bad) is None


def test_btc_collapses_to_bare_lowercase_hex():
    btc = _build(BTC_SPEC).chain
    assert btc.canonical_tx_hash(HEX.upper()) == btc.canonical_tx_hash(HEX) == HEX.lower()
    assert btc.canonical_tx_hash('0x' + HEX) is None


@pytest.mark.parametrize('suffix', URL_SUFFIXES)
def test_hash_with_url_suffix_has_no_canonical_form(suffix):
    # Lookups interpolate the hash into a URL path, where a fragment or query string is dropped.
    assert _build(BTC_SPEC).chain.canonical_tx_hash(HEX.lower() + suffix) is None
    assert _build(ETH_SPEC).chain.canonical_tx_hash('0x' + HEX.lower() + suffix) is None


@pytest.mark.parametrize('chain_id', ['sol', 'solusdc'])
def test_solana_signatures_are_untouched(chain_id):
    chain = _build(_spec(chain_id)).chain
    assert chain.canonical_tx_hash(SOL_SIG) == SOL_SIG  # base58 is case-sensitive
    assert chain.canonical_tx_hash(SOL_SIG.lower()) == SOL_SIG.lower()


@pytest.mark.parametrize('spec', FAMILY_SPECS, ids=lambda s: s.chain_id)
def test_the_ledger_key_agrees_with_every_chains_canonical_spelling(spec):
    # Events carry no chain, so the ledgers key by tx_key: it must fold every honest spelling onto the
    # canonical one's key, or a variant claim lands in its own ledger row.
    chain = _build(spec).chain
    for spelling in _variants(spec):
        assert tx_key(spelling) == tx_key(_canonical(spec)) == tx_key(chain.canonical_tx_hash(spelling))
    assert tx_key(SOL_SIG) == SOL_SIG and tx_key(SOL_SIG.lower()) != SOL_SIG


# ── relay: confirm_deposit ──────────────────────────────────────────────────


class _AnySpellingProvider:
    """A real chain's canonicalizer over an RPC that finds the deposit under any spelling."""

    def __init__(self, chain):
        self.chain = chain
        self.chain_def = SimpleNamespace(replay_grace_secs=60)
        self.verified = []

    def verify_transaction(self, tx_hash, **kw):
        self.verified.append(tx_hash)
        return TransactionInfo(
            tx_hash=tx_hash,
            confirmed=True,
            sender='0xuser',
            recipient='0xminer',
            amount=100_000,
            block_number=500,
            confirmations=6,
            block_time=FRESH,
        )


class _Relay:
    """A claim relay over a fake program client, a real ServingStore and a World the test can move."""

    def __init__(self, spec, reservations, tmp_path):
        resv = {hub: _confirm_reservation(from_chain=spec.chain_id, **over) for hub, over in reservations.items()}
        self.client = _ConfirmClient(resv)
        self.provider = _AnySpellingProvider(_build(spec).chain)
        self.spec = spec
        self.world = World(0, 1, True, None, {})
        providers = {spec.chain_id: self.provider}
        store = Store(str(tmp_path / 'state.db'))
        self.memory = SwapMemory(store)
        self.store = ServingStore(store, lambda: self.world, self.memory, providers)
        self.ctx = Ctx(self.client, store=self.store, providers=providers)

    def confirm(self, tx_hash):
        return confirm_deposit(self.ctx, HOTKEY, tx_hash)

    def new_reservation(self, **over):
        """The miner's next reservation: its first swap's PDA is closed."""
        self.client.swaps.clear()
        self.client._reservation = {'sol': _confirm_reservation(from_chain=self.spec.chain_id, **over)}


@pytest.mark.parametrize('spec', FAMILY_SPECS, ids=lambda s: s.chain_id)
def test_canonical_claim_lands_under_the_canonical_string(spec, tmp_path):
    relay = _Relay(spec, {'sol': dict(created_at=CREATED_AT)}, tmp_path)
    r = relay.confirm(_variants(spec)[-1])
    canonical = _canonical(spec)
    assert r.ok and r.from_tx_hash == canonical and relay.provider.verified == [canonical]
    assert relay.client.claims[0][:2] == (swap_key_from_tx_hash(canonical), canonical)
    assert relay.store.deposit_claim(canonical) == (swap_key_from_tx_hash(canonical).hex(), False)


@pytest.mark.parametrize('spec', FAMILY_SPECS, ids=lambda s: s.chain_id)
def test_claimed_deposit_is_not_claimed_again_on_the_other_hub(spec, tmp_path):
    # Two live slots on different hubs whose senders normalize equal.
    hubs = {
        'sol': dict(from_addr='0xABC', created_at=CREATED_AT),
        'tao': dict(from_addr='0xabc', created_at=CREATED_AT),
    }
    relay = _Relay(spec, hubs, tmp_path)
    assert relay.confirm(_canonical(spec)).ok
    for spelling in _variants(spec) + [_canonical(spec)]:
        r = relay.confirm(spelling)
        assert not r.ok and 'already claimed' in r.reason
    assert len(relay.client.claims) == 1


@pytest.mark.parametrize('spec', FAMILY_SPECS, ids=lambda s: s.chain_id)
def test_a_variant_claim_in_the_ledger_blocks_the_canonical_claim(spec, tmp_path):
    # The 10-16 shape: the deposit was already claimed under another spelling, so under another swap key.
    # The variant's swap PDA is elsewhere; only the case-blind ledger links the two.
    relay = _Relay(spec, {'sol': dict(created_at=CREATED_AT)}, tmp_path)
    variant = _variants(spec)[-1]
    relay.world = replace(
        relay.world, deposits={tx_key(variant): DepositClaim(swap_key_from_tx_hash(variant).hex(), 1, 1)}
    )
    for spelling in _variants(spec) + [_canonical(spec)]:
        r = relay.confirm(spelling)
        assert not r.ok and 'already claimed' in r.reason
    assert relay.client.claims == []


@pytest.mark.parametrize('spec', FAMILY_SPECS, ids=lambda s: s.chain_id)
def test_deposit_stays_used_after_its_swap_closes(spec, tmp_path):
    # The first swap was initiated and its PDA closed; a later reservation sees the World's ledger row.
    relay = _Relay(spec, {'sol': dict(created_at=CREATED_AT)}, tmp_path)
    canonical = _canonical(spec)
    assert relay.confirm(canonical).ok
    key = swap_key_from_tx_hash(canonical).hex()
    relay.world = replace(relay.world, deposits={tx_key(canonical): DepositClaim(key, 1, 1, initiated=True)})
    relay.new_reservation(created_at=CREATED_AT + 3)
    for spelling in [canonical] + _variants(spec):
        r = relay.confirm(spelling)
        assert not r.ok and 'already claimed' in r.reason
    assert len(relay.client.claims) == 1


def test_a_reaped_claim_can_be_claimed_again(tmp_path):
    # Never initiated, so the deposit never backed a swap: the taker can claim it on a new reservation.
    relay = _Relay(ETH_SPEC, {'sol': dict(created_at=CREATED_AT)}, tmp_path)
    canonical = _canonical(ETH_SPEC)
    assert relay.confirm('0x' + HEX).ok
    key = swap_key_from_tx_hash(canonical).hex()
    relay.world = replace(relay.world, deposits={tx_key(canonical): DepositClaim(key, 1, 1)})  # reaped, not initiated
    relay.new_reservation(created_at=CREATED_AT + 3)
    assert relay.confirm('0X' + HEX.upper()).ok
    assert [c[1] for c in relay.client.claims] == [canonical, canonical]


def test_relay_on_another_validator_sees_the_live_swap(tmp_path):
    # Its store never saw the claim, but the Swap at the canonical key is in the World.
    relay = _Relay(ETH_SPEC, {'sol': dict(created_at=CREATED_AT)}, tmp_path)
    canonical = _canonical(ETH_SPEC)
    relay.client.swaps[swap_key_from_tx_hash(canonical)] = SimpleNamespace(from_tx_hash=canonical)
    r = relay.confirm('0x' + HEX.upper())
    assert not r.ok and 'already claimed' in r.reason and not relay.client.claims


def test_failed_submit_can_be_retried(tmp_path):
    relay = _Relay(ETH_SPEC, {'sol': dict(created_at=CREATED_AT)}, tmp_path)
    real_submit = relay.client.submit_swap_claim

    def flaky(*a, **kw):
        relay.client.submit_swap_claim = real_submit
        raise RuntimeError('rpc dropped')

    relay.client.submit_swap_claim = flaky
    with pytest.raises(RuntimeError):
        relay.confirm('0x' + HEX)
    assert relay.confirm('0x' + HEX).ok


@pytest.mark.parametrize('spec', FAMILY_SPECS, ids=lambda s: s.chain_id)
def test_junk_spellings_never_claim(spec, tmp_path):
    relay = _Relay(spec, {'sol': dict(created_at=CREATED_AT)}, tmp_path)
    for junk in _junk(spec):
        if junk.strip() == _canonical(spec):
            continue  # surrounding whitespace is the request's, stripped before anything (below)
        assert not relay.confirm(junk).ok, junk
    assert not relay.client.claims and not relay.provider.verified
    assert relay.confirm(_canonical(spec)).ok  # the plain hash still claims


def test_surrounding_whitespace_is_stripped_before_claiming(tmp_path):
    relay = _Relay(BTC_SPEC, {'sol': dict(created_at=CREATED_AT)}, tmp_path)
    assert relay.confirm(' ' + HEX.upper() + ' ').ok
    assert relay.client.claims[0][1] == HEX.lower()


def test_solana_signature_claims_exactly_as_given(tmp_path):
    relay = _Relay(SOL_SPEC, {'sol': dict(created_at=CREATED_AT)}, tmp_path)
    r = relay.confirm(SOL_SIG)
    assert r.ok and relay.client.claims[0][1] == SOL_SIG


# A miner refilling its deposit wallet through allways announces the payout before it lands; a taker who
# reserves naming the payer as source must not be able to claim that delivery as their deposit.
PAYER = '0x' + 'aB' * 20  # the paying miner's payout wallet, as posted
DEPOSIT_WALLET = '0x' + 'cD' * 20  # the refilling miner's deposit wallet, as posted


def _payout_relay(tmp_path):
    resv = dict(created_at=CREATED_AT, from_addr=PAYER.lower(), miner_from_addr=DEPOSIT_WALLET.lower())
    return _Relay(ETH_SPEC, {'sol': resv}, tmp_path)


def test_a_live_swaps_payout_cannot_be_claimed_as_a_deposit(tmp_path):
    relay = _payout_relay(tmp_path)
    relay.world = replace(relay.world, routes={'aa' * 32: PayoutRoute('eth', PAYER, DEPOSIT_WALLET, CREATED_AT)})
    r = relay.confirm('0x' + HEX)
    assert not r.ok and 'payout' in r.reason and not relay.client.claims


def test_a_delivery_this_validator_verified_cannot_be_claimed(tmp_path):
    relay = _payout_relay(tmp_path)
    relay.memory.claim_delivery('payingMiner', tx_key('0X' + HEX.upper()), 'aa' * 32, CREATED_AT)
    r = relay.confirm('0x' + HEX)
    assert not r.ok and 'payout' in r.reason and not relay.client.claims


def test_a_delivery_a_miner_only_reported_does_not_veto_the_claim(tmp_path):
    # mark_fulfilled's to_tx_hash is free text: any miner could name a user's real deposit.
    relay = _payout_relay(tmp_path)
    relay.world = replace(relay.world, fulfillments={'aa' * 32: Fulfillment('0x' + HEX.upper(), CREATED_AT, 'sybil')})
    assert relay.confirm('0x' + HEX).ok


def test_an_honest_deposit_is_unaffected_by_other_routes(tmp_path):
    relay = _payout_relay(tmp_path)
    relay.world = replace(
        relay.world,
        routes={
            'aa' * 32: PayoutRoute('eth', PAYER, '0x' + 'ee' * 20, CREATED_AT),  # pays someone else
            'bb' * 32: PayoutRoute('base', PAYER, DEPOSIT_WALLET, CREATED_AT),  # another chain
        },
    )
    relay.memory.claim_delivery('payingMiner', tx_key('0x' + 'f' * 64), 'cc' * 32, CREATED_AT)
    assert relay.confirm('0x' + HEX).ok and len(relay.client.claims) == 1


# ── attest gate: decide over WorldContext ────────────────────────────────────


class _ChainProvider(RecordingProvider):
    """The rules' recording provider, spelling hashes and addresses with a real chain."""

    def __init__(self, spec, result=True):
        super().__init__(result)
        self.real = _build(spec).chain

    def canonical_tx_hash(self, tx_hash):
        return self.real.canonical_tx_hash(tx_hash)

    def normalize_address(self, address):
        return self.real.normalize_address(address)


class _WorldRules(Rules):
    """The production rules and context over a World the test sets."""

    def __init__(self, providers, reservation):
        super().__init__(providers, reservation)
        self.world = World(0, 1, True, None, {})

    def context(self, now):
        ctx = _Context(self, now)
        ctx.world = self.world
        return ctx

    def decide(self, swap, now):
        return decide(swap, self.context(now), now, fee_divisor=100)


def _claim(spec, from_tx_hash, created_at=1200):
    """A PendingAttestation claim on a ``spec`` deposit, its reservation pinning it."""
    rules = _WorldRules(
        {spec.chain_id: _ChainProvider(spec), 'sol': RecordingProvider()},
        make_reservation(created_at=created_at, claimed_swap_key=swap_key_from_tx_hash(from_tx_hash)),
    )
    swap = make_swap(status='PendingAttestation', from_chain=spec.chain_id, to_chain='sol', from_amount=10**18)
    swap.from_tx_hash, swap.swap_key = from_tx_hash, swap_key_from_tx_hash(from_tx_hash)
    swap.to_amount, _ = expected_swap_amounts(swap, 100)
    swap.collateral_amount = swap.to_amount
    return rules, swap


@pytest.mark.parametrize('spec', FAMILY_SPECS, ids=lambda s: s.chain_id)
def test_canonical_in_flight_claim_attests(spec):
    rules, swap = _claim(spec, _canonical(spec))
    assert rules.decide(swap, now=1500).decision == Decision.ATTEST


@pytest.mark.parametrize('spec,spelling', _family_cases(_variants) + _family_cases(_junk))
def test_non_canonical_in_flight_claim_refuses_to_attest(spec, spelling):
    rules, swap = _claim(spec, spelling)
    action = rules.decide(swap, now=1500)
    assert action.decision == Decision.REJECT and 'canonical' in action.reason
    assert 'canonical' in attest_reject_reason(rules.providers, swap, 100)


@pytest.mark.parametrize('spec,variant', _family_cases(_variants))
def test_a_variant_claim_in_the_ledger_refuses_the_canonical_claim(spec, variant):
    # One deposit, two spellings, two swap keys: the earlier claim holds the case-blind ledger row.
    rules, swap = _claim(spec, _canonical(spec))
    rules.world = replace(
        rules.world, deposits={tx_key(variant): DepositClaim(swap_key_from_tx_hash(variant).hex(), 1, 1)}
    )
    action = rules.decide(swap, now=1500)
    assert action.decision == Decision.REJECT and 'already claimed' in action.reason


@pytest.mark.parametrize('spec', FAMILY_SPECS, ids=lambda s: s.chain_id)
def test_a_live_swap_on_a_variant_spelling_refuses_the_canonical_claim(spec):
    # Even before its ledger row arrives, a live swap spending the deposit under another key blocks it.
    rules, swap = _claim(spec, _canonical(spec))
    live = make_swap(status='Active', from_chain=spec.chain_id, key=b'\x09' * 32)
    live.from_tx_hash = _variants(spec)[-1]
    rules.world = replace(rules.world, accounts={'Swap': {'pda': live}})
    action = rules.decide(swap, now=1500)
    assert action.decision == Decision.REJECT and 'already claimed' in action.reason


def test_claim_on_an_initiated_deposit_refuses_to_attest():
    # Same canonical key from a later reservation, after the first swap was initiated and closed.
    canonical = _canonical(ETH_SPEC)
    rules, swap = _claim(ETH_SPEC, canonical, created_at=1230)
    key = swap_key_from_tx_hash(canonical).hex()
    rules.world = replace(rules.world, deposits={tx_key('0X' + HEX): DepositClaim(key, 1, 1, initiated=True)})
    action = rules.decide(swap, now=1700)
    assert action.decision == Decision.REJECT and 'already claimed' in action.reason


def test_relay_and_attest_gate_agree_on_the_claim():
    # A claim this validator relayed (its own ledger row, same key, not yet initiated) still attests,
    # and so does a reaped claim re-claimed on a new reservation.
    canonical = _canonical(ETH_SPEC)
    rules, swap = _claim(ETH_SPEC, canonical)
    key = swap_key_from_tx_hash(canonical).hex()
    rules.world = replace(rules.world, deposits={tx_key(canonical): DepositClaim(key, 1, 1)})
    assert rules.decide(swap, now=1500).decision == Decision.ATTEST
    assert rules.decide(swap, now=1501).decision == Decision.ATTEST  # re-deciding the same claim is fine


def test_attest_gate_refuses_a_deposit_this_validator_verified_as_a_delivery():
    rules, swap = _claim(ETH_SPEC, _canonical(ETH_SPEC))
    rules.memory.claim_delivery('payingMiner', tx_key('0X' + HEX.upper()), 'aa' * 32, 1400)
    action = rules.decide(swap, now=1500)
    assert action.decision == Decision.REJECT and 'payout' in action.reason


def test_attest_gate_refuses_a_claim_matching_a_live_payout_route():
    # Addresses compare through the chain's own normalizer: a checksum-cased route still matches.
    rules, swap = _claim(ETH_SPEC, _canonical(ETH_SPEC))
    swap.user_from_addr, swap.miner_from_addr = PAYER.lower(), DEPOSIT_WALLET.lower()
    assert rules.decide(swap, now=1500).decision == Decision.ATTEST  # no route known yet
    rules.world = replace(rules.world, routes={'aa' * 32: PayoutRoute('eth', PAYER, DEPOSIT_WALLET, 1400)})
    action = rules.decide(swap, now=1501)
    assert action.decision == Decision.REJECT and 'payout' in action.reason
    rules.world = replace(rules.world, routes={'aa' * 32: PayoutRoute('base', PAYER, DEPOSIT_WALLET, 1400)})
    assert rules.decide(swap, now=1502).decision == Decision.ATTEST  # a route on another chain is not this one


def test_attest_gate_ignores_a_delivery_a_miner_only_reported_in_any_spelling():
    rules, swap = _claim(ETH_SPEC, _canonical(ETH_SPEC))
    rules.world = replace(rules.world, fulfillments={'aa' * 32: Fulfillment('0X' + HEX.upper(), 1400, 'sybil', True)})
    assert rules.decide(swap, now=1500).decision == Decision.ATTEST


# ── delivery leg ─────────────────────────────────────────────────────────────
# The miner writes to_tx_hash itself, so the validator cannot respell it: junk never verifies, while an
# honest casing verifies in canonical form and keys the same delivery ledger row.


def _deliveries(spec):
    rules = _WorldRules({'btc': RecordingProvider(), spec.chain_id: _ChainProvider(spec)}, make_reservation())
    return rules, rules.providers[spec.chain_id]


def _delivery(spec, to_tx_hash, n=1):
    swap = make_swap(from_chain='btc', to_chain=spec.chain_id, key=bytes([n]) * 32)
    swap.from_tx_hash = f'srctx{n}'  # a swap's key is its deposit's: two swaps, two deposits
    swap.to_tx_hash = to_tx_hash
    return swap


@pytest.mark.parametrize('spec,spelling', _family_cases(_variants))
def test_honest_delivery_spelling_verifies_in_canonical_form(spec, spelling):
    rules, dest = _deliveries(spec)
    assert rules.decide(_delivery(spec, spelling), now=1500).decision == Decision.CONFIRM
    assert dest.calls[-1].tx_hash == _canonical(spec)


@pytest.mark.parametrize('spec,spelling', _family_cases(_junk))
def test_junk_delivery_spelling_never_verifies(spec, spelling):
    rules, dest = _deliveries(spec)
    swap = _delivery(spec, spelling)
    assert rules.decide(swap, now=1500).decision == Decision.WAIT
    assert rules.decide(swap, now=2500).decision == Decision.TIMEOUT
    assert [c for c in dest.calls if not isinstance(c, tuple)] == []  # never sent to the chain


@pytest.mark.parametrize('spec,spelling', _family_cases(_variants) + _family_cases(_junk))
def test_one_delivery_confirms_one_swap_whatever_the_second_spelling(spec, spelling):
    rules, _ = _deliveries(spec)
    first, second = _delivery(spec, _canonical(spec)), _delivery(spec, spelling, 2)
    assert rules.decide(first, now=1500).decision == Decision.CONFIRM
    assert rules.decide(second, now=1500).decision == Decision.WAIT
    assert rules.decide(second, now=2500).decision == Decision.TIMEOUT
    assert rules.decide(first, now=1501).decision == Decision.CONFIRM  # the second never blocks the first


def test_solana_delivery_signature_is_verified_as_given():
    # base58 has one spelling per signature; a malformed one is the RPC's to reject.
    rules, dest = _deliveries(SOL_SPEC)
    assert rules.decide(_delivery(SOL_SPEC, SOL_SIG), now=1500).decision == Decision.CONFIRM
    assert dest.calls[-1].tx_hash == SOL_SIG
    assert rules.decide(_delivery(SOL_SPEC, '', 2), now=1500).decision == Decision.WAIT


@pytest.mark.parametrize('spec,spelling', _family_cases(_variants))
def test_completed_swap_replaces_a_local_delivery_claim(spec, spelling):
    # This validator verified swap 1 on the tx first; the chain completing swap 2 on it wins the tx.
    rules, _ = _deliveries(spec)
    first = _delivery(spec, _canonical(spec))
    assert rules.decide(first, now=1500).decision == Decision.CONFIRM
    other = swap_key_from_tx_hash('srctx2').hex()
    reported = Fulfillment(spelling, 1400, str(first.miner))
    rules.world = replace(rules.world, fulfillments={other: reported})
    assert rules.decide(first, now=1501).decision == Decision.CONFIRM  # only reported: no claim on the tx
    rules.world = replace(rules.world, fulfillments={other: replace(reported, settled=True)})
    action = rules.decide(first, now=1502)
    assert action.decision == Decision.WAIT and 'another swap' in action.reason


def test_another_miners_completion_does_not_take_the_tx():
    rules, _ = _deliveries(ETH_SPEC)
    first = _delivery(ETH_SPEC, _canonical(ETH_SPEC))
    rules.world = replace(rules.world, fulfillments={'bb' * 32: Fulfillment('0X' + HEX, 1400, 'otherPK', True)})
    assert rules.decide(first, now=1500).decision == Decision.CONFIRM


# ── the World's ledgers, built from program events ───────────────────────────


def _claimed(tx_hash, slot, sig):
    fields = {'miner': WORLD_MINER, 'user': WORLD_MINER, 'from_tx_hash': tx_hash, 'from_tx_block': 1}
    return _ev('SwapClaimed', {**fields, 'swap_key': swap_key_from_tx_hash(tx_hash)}, slot=slot, sig=sig)


def test_two_spellings_of_one_deposit_fold_into_one_ledger_row(tmp_path):
    # The 10-16 replay end to end: '0X…UPPER' claimed first, '0x…lower' later. The World keeps one row (the
    # earlier claim), and the canonical claim is refused attestation off it.
    reader, events, _, _, out = _reader(_Client(), tmp_path)
    reader.tick(WORLD_NOW)
    upper, lower = '0X' + HEX.upper(), '0x' + HEX.lower()
    events.publish(_claimed(lower, slot=300, sig='b'))
    events.publish(_claimed(upper, slot=200, sig='a'))
    reader.tick(WORLD_NOW)
    world = out.get()[0]
    (held,) = world.deposits.values()
    assert held.swap_key == swap_key_from_tx_hash(upper).hex() and not held.initiated

    rules, swap = _claim(ETH_SPEC, lower)
    rules.world = world
    action = rules.decide(swap, now=1500)
    assert action.decision == Decision.REJECT and 'already claimed' in action.reason


def test_an_initiation_marks_the_deposit_spent_under_every_spelling(tmp_path):
    reader, events, _, _, out = _reader(_Client(), tmp_path)
    reader.tick(WORLD_NOW)
    canonical = _canonical(ETH_SPEC)
    events.publish(_claimed(canonical, slot=200, sig='a'))
    events.publish(_ev('SwapInitiated', {'swap_key': swap_key_from_tx_hash(canonical), 'miner': WORLD_MINER}, 210))
    reader.tick(WORLD_NOW)
    world = out.get()[0]
    for spelling in _variants(ETH_SPEC):
        assert world.deposits[tx_key(spelling)].initiated

    rules, swap = _claim(ETH_SPEC, canonical, created_at=1230)  # a later reservation, same deposit
    rules.world = world
    action = rules.decide(swap, now=1700)
    assert action.decision == Decision.REJECT and 'already claimed' in action.reason


def test_a_completed_swap_spends_its_delivery_in_every_spelling(tmp_path):
    # Only a completed swap's delivery counts; a fulfilled-but-unconfirmed one leaves the tx free.
    reader, events, _, _, out = _reader(_Client(), tmp_path)
    reader.tick(WORLD_NOW)
    done, open_ = swap_key_from_tx_hash('srctx8'), swap_key_from_tx_hash('srctx9')
    for key in (done, open_):
        fields = {'swap_key': key, 'miner': WORLD_MINER, 'to_tx_hash': '0X' + HEX.upper(), 'to_amount': 1}
        events.publish(_ev('SwapFulfilled', fields, 200, sig=key.hex()[:4]))
    events.publish(_ev('SwapCompleted', {'swap_key': done, 'miner': WORLD_MINER}, 210))
    reader.tick(WORLD_NOW)
    world = out.get()[0]

    rules, _ = _deliveries(ETH_SPEC)
    rules.world = world
    mine = _delivery(ETH_SPEC, _canonical(ETH_SPEC), 3)
    mine.miner = WORLD_MINER
    assert rules.decide(mine, now=1500).decision == Decision.WAIT  # the completed swap holds the tx
    rules.world = replace(world, fulfillments={k: f for k, f in world.fulfillments.items() if k != done.hex()})
    assert rules.decide(mine, now=1500).decision == Decision.CONFIRM  # the open one alone does not


def test_the_relay_holds_its_own_claim_until_the_world_has_it(tmp_path):
    # Between submit and the SwapClaimed event, a second request on another spelling still sees the claim.
    relay = _Relay(ETH_SPEC, {'sol': dict(created_at=CREATED_AT)}, tmp_path)
    assert relay.confirm('0x' + HEX).ok
    assert relay.store.deposit_claim('0X' + HEX.upper()) == (swap_key_from_tx_hash(_canonical(ETH_SPEC)).hex(), False)

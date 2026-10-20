"""Source tx hash normalization and claimed-deposit tracking.

Swap keys hash the raw from_tx_hash string, so every claim carries its chain's canonical spelling and
each deposit backs one swap. Covers the chain canonicalizers, confirm_deposit and the attest gate.
"""

import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from allways.assets import ASSET_REGISTRY
from allways.assets.asset import TransactionInfo
from allways.assets.chain import canonical_hex_tx_hash
from allways.assets.evm import EvmChain
from allways.assets.tao import Tao
from allways.chains import get_chain_def
from allways.solana.client import swap_key_from_tx_hash
from allways.solana.events import EventRecord
from allways.validator.event_index import SolanaEventIndex
from allways.validator.reserve_engine import confirm_deposit
from allways.validator.solana_swap_loop import SwapDecision, attest_reject_reason
from allways.validator.state_store import ValidatorStateStore
from tests.test_reserve_engine import HOTKEY, _confirm_reservation, _ConfirmClient
from tests.test_solana_swap_loop import RESV_CREATED_AT, loop_with, make_reservation, make_swap

HEX = '9dA0' * 16  # 64 hex chars, mixed case
SOL_SIG = '5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW'
CREATED_AT = 1000
FRESH = CREATED_AT + 5


@pytest.fixture(autouse=True)
def mainnet_env(monkeypatch):
    for spec in ASSET_REGISTRY:
        prefix = get_chain_def(spec.chain_id).env_prefix
        if prefix:
            monkeypatch.setenv(f'{prefix}_NETWORK', 'mainnet')


def _build(spec):
    available = {'solana_rpc_url': 'http://offline.invalid', 'solana_keypair': MagicMock(), 'subtensor': MagicMock()}
    return spec.cls(**{name: available[name] for name in spec.kwarg_names if name in available})


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


# ── canonicalizers ───────────────────────────────────────────────────────────


def test_hex_hash_specs_cover_every_evm_network_and_tao():
    ids = {s.chain_id for s in HEX_SPECS}
    assert {'eth', 'baseusdc', 'tao'} <= ids and any(i.startswith('sn') for i in ids)


@pytest.mark.parametrize('spec', HEX_SPECS, ids=lambda s: s.chain_id)
def test_hex_chains_collapse_every_casing_to_lowercase_0x(spec):
    chain = _build(spec).chain
    canonical = '0x' + HEX.lower()
    for spelling in ('0x' + HEX, '0x' + HEX.upper(), '0X' + HEX, canonical):
        assert chain.canonical_tx_hash(spelling) == canonical
    for bad in (HEX, '0x' + HEX[:-1], '0x' + HEX + '00', '0x' + HEX[:-1] + 'g', '0x' + HEX + '\n', '', '123-4'):
        assert chain.canonical_tx_hash(bad) is None


def test_btc_collapses_to_bare_lowercase_hex():
    btc = _build(next(s for s in ASSET_REGISTRY if s.chain_id == 'btc')).chain
    assert btc.canonical_tx_hash(HEX.upper()) == btc.canonical_tx_hash(HEX) == HEX.lower()
    assert btc.canonical_tx_hash('0x' + HEX) is None


URL_SUFFIXES = ('#1', '#2', '?x', '/', '%20', ' ')


@pytest.mark.parametrize('suffix', URL_SUFFIXES)
def test_hash_with_url_suffix_has_no_canonical_form(suffix):
    # Lookups interpolate the hash into a URL path, where a fragment or query string is dropped.
    btc = _build(next(s for s in ASSET_REGISTRY if s.chain_id == 'btc')).chain
    eth = _build(next(s for s in ASSET_REGISTRY if s.chain_id == 'eth')).chain
    assert btc.canonical_tx_hash(HEX.lower() + suffix) is None
    assert eth.canonical_tx_hash('0x' + HEX.lower() + suffix) is None


@pytest.mark.parametrize('chain_id', ['sol', 'solusdc'])
def test_solana_signatures_are_untouched(chain_id):
    chain = _build(next(s for s in ASSET_REGISTRY if s.chain_id == chain_id)).chain
    assert chain.canonical_tx_hash(SOL_SIG) == SOL_SIG  # base58 is case-sensitive
    assert chain.canonical_tx_hash(SOL_SIG.lower()) == SOL_SIG.lower()


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


def _relay(spec, reservations, store=None):
    resv = {hub: _confirm_reservation(from_chain=spec.chain_id, **over) for hub, over in reservations.items()}
    client = _ConfirmClient(resv)
    provider = _AnySpellingProvider(_build(spec).chain)
    validator = SimpleNamespace(
        solana_client=client,
        axon_assets={spec.chain_id: provider},
        axon_lock=threading.RLock(),
        state_store=store or ValidatorStateStore(db_path=Path(tempfile.mkdtemp()) / 'state.db'),
    )
    return validator, client, provider


@pytest.mark.parametrize('spec', HEX_SPECS, ids=lambda s: s.chain_id)
def test_canonical_claim_lands_under_the_canonical_string(spec):
    validator, client, provider = _relay(spec, {'sol': dict(created_at=CREATED_AT)})
    r = confirm_deposit(validator, HOTKEY, '0x' + HEX.upper())
    canonical = '0x' + HEX.lower()
    assert r.ok and r.from_tx_hash == canonical and provider.verified == [canonical]
    assert client.claims[0][:2] == (swap_key_from_tx_hash(canonical), canonical)


@pytest.mark.parametrize('spec', HEX_SPECS, ids=lambda s: s.chain_id)
def test_claimed_deposit_is_not_claimed_again_on_the_other_hub(spec):
    # Two live slots on different hubs whose senders normalize equal.
    validator, client, _ = _relay(
        spec,
        {'sol': dict(from_addr='0xABC', created_at=CREATED_AT), 'tao': dict(from_addr='0xabc', created_at=CREATED_AT)},
    )
    assert confirm_deposit(validator, HOTKEY, '0x' + HEX).ok
    for spelling in ('0x' + HEX.lower(), '0x' + HEX.upper(), '0X' + HEX):
        r = confirm_deposit(validator, HOTKEY, spelling)
        assert not r.ok and 'already claimed' in r.reason
    assert len(client.claims) == 1


@pytest.mark.parametrize('spec', HEX_SPECS, ids=lambda s: s.chain_id)
def test_deposit_stays_used_after_its_swap_closes(spec):
    # The first swap was initiated and its PDA closed; a later reservation inside the grace window sees it.
    store = ValidatorStateStore(db_path=Path(tempfile.mkdtemp()) / 'state.db')
    validator, client, _ = _relay(spec, {'sol': dict(created_at=CREATED_AT)}, store)
    canonical = '0x' + HEX.lower()
    assert confirm_deposit(validator, HOTKEY, canonical).ok
    store.mark_deposit_initiated(swap_key_from_tx_hash(canonical).hex())
    client.swaps.clear()
    client._reservation = {'sol': _confirm_reservation(from_chain=spec.chain_id, created_at=CREATED_AT + 3)}
    for spelling in (canonical, '0x' + HEX.upper()):
        r = confirm_deposit(validator, HOTKEY, spelling)
        assert not r.ok and 'already claimed' in r.reason
    assert len(client.claims) == 1


def test_a_reaped_claim_can_be_claimed_again():
    # Never initiated, so the deposit never backed a swap: the taker can claim it on a new reservation.
    spec = next(s for s in HEX_SPECS if s.chain_id == 'eth')
    validator, client, _ = _relay(spec, {'sol': dict(created_at=CREATED_AT)})
    assert confirm_deposit(validator, HOTKEY, '0x' + HEX).ok
    client.swaps.clear()
    client._reservation = {'sol': _confirm_reservation(from_chain=spec.chain_id, created_at=CREATED_AT + 3)}
    assert confirm_deposit(validator, HOTKEY, '0x' + HEX).ok
    assert len(client.claims) == 2


def test_relay_on_another_validator_sees_the_live_swap():
    # Its store never saw the claim, but the Swap PDA at the canonical key is on chain.
    spec = next(s for s in HEX_SPECS if s.chain_id == 'eth')
    validator, client, _ = _relay(spec, {'sol': dict(created_at=CREATED_AT)})
    canonical = '0x' + HEX.lower()
    client.swaps[swap_key_from_tx_hash(canonical)] = SimpleNamespace(from_tx_hash=canonical)
    r = confirm_deposit(validator, HOTKEY, '0x' + HEX.upper())
    assert not r.ok and 'already claimed' in r.reason and not client.claims


def test_failed_submit_can_be_retried():
    spec = next(s for s in HEX_SPECS if s.chain_id == 'eth')
    validator, client, _ = _relay(spec, {'sol': dict(created_at=CREATED_AT)})
    real_submit = client.submit_swap_claim

    def flaky(*a, **kw):
        client.submit_swap_claim = real_submit
        raise RuntimeError('rpc dropped')

    client.submit_swap_claim = flaky
    with pytest.raises(RuntimeError):
        confirm_deposit(validator, HOTKEY, '0x' + HEX)
    assert confirm_deposit(validator, HOTKEY, '0x' + HEX).ok


@pytest.mark.parametrize('spec', HEX_SPECS, ids=lambda s: s.chain_id)
def test_malformed_hash_never_claims(spec):
    validator, client, provider = _relay(spec, {'sol': dict(created_at=CREATED_AT)})
    assert not confirm_deposit(validator, HOTKEY, HEX).ok  # missing 0x
    assert not client.claims and not provider.verified


@pytest.mark.parametrize('chain_id,base', [('btc', HEX.lower()), ('eth', '0x' + HEX.lower())])
@pytest.mark.parametrize('suffix', [x for x in URL_SUFFIXES if x.strip()])
def test_hash_with_url_suffix_never_claims(chain_id, base, suffix):
    spec = next(s for s in ASSET_REGISTRY if s.chain_id == chain_id)
    validator, client, provider = _relay(spec, {'sol': dict(created_at=CREATED_AT)})
    assert not confirm_deposit(validator, HOTKEY, base + suffix).ok
    assert not client.claims and not provider.verified
    assert confirm_deposit(validator, HOTKEY, base).ok  # the plain hash still claims


def test_surrounding_whitespace_is_stripped_before_claiming():
    spec = next(s for s in ASSET_REGISTRY if s.chain_id == 'btc')
    validator, client, _ = _relay(spec, {'sol': dict(created_at=CREATED_AT)})
    assert confirm_deposit(validator, HOTKEY, ' ' + HEX.upper() + ' ').ok
    assert client.claims[0][1] == HEX.lower()


def test_solana_signature_claims_exactly_as_given():
    spec = next(s for s in ASSET_REGISTRY if s.chain_id == 'sol')
    validator, client, _ = _relay(spec, {'sol': dict(created_at=CREATED_AT)})
    r = confirm_deposit(validator, HOTKEY, SOL_SIG)
    assert r.ok and client.claims[0][1] == SOL_SIG


# ── attest gate ─────────────────────────────────────────────────────────────


def _hex_source_loop(from_tx_hash, state_store=None, created_at=RESV_CREATED_AT):
    loop, providers = loop_with(
        result=True,
        reservation=make_reservation(created_at=created_at, claimed_swap_key=swap_key_from_tx_hash(from_tx_hash)),
        state_store=state_store,
    )
    # The source leg is an EVM/Substrate-style chain: 0x-hex hashes.
    providers['btc'].canonical_tx_hash = lambda h: canonical_hex_tx_hash(h, prefixed=True)
    swap = make_swap(status='PendingAttestation', to_amount=1000)
    swap.from_tx_hash = from_tx_hash
    swap.swap_key = swap_key_from_tx_hash(from_tx_hash)
    return loop, swap


def test_canonical_in_flight_claim_attests():
    loop, swap = _hex_source_loop('0x' + HEX.lower())
    assert loop.decide(swap, now=1500).decision == SwapDecision.ATTEST


@pytest.mark.parametrize('spelling', ['0x' + HEX, '0x' + HEX.upper(), '0X' + HEX.lower()])
def test_non_canonical_in_flight_claim_refuses_to_attest(spelling):
    loop, swap = _hex_source_loop(spelling)
    action = loop.decide(swap, now=1500)
    assert action.decision == SwapDecision.REJECT and 'canonical' in action.reason
    assert 'canonical' in attest_reject_reason(loop.providers, swap, loop.fee_divisor)


def test_claim_on_an_initiated_deposit_refuses_to_attest(tmp_path):
    # Same canonical key from a later reservation, after the first swap was initiated and closed.
    store = ValidatorStateStore(tmp_path / 'state.db')
    canonical = '0x' + HEX.lower()
    first, swap = _hex_source_loop(canonical, store)
    assert first.decide(swap, now=1500).decision == SwapDecision.ATTEST
    assert first.decide(swap, now=1501).decision == SwapDecision.ATTEST  # re-deciding the same claim is fine
    swap.status = 'Active'
    first.decide(swap, now=1600)  # the loop sees it initiated
    later, swap = _hex_source_loop(canonical, store, created_at=RESV_CREATED_AT + 30)
    action = later.decide(swap, now=1700)
    assert action.decision == SwapDecision.REJECT and 'already claimed' in action.reason


def test_relay_and_loop_agree_on_the_claim(tmp_path):
    # A claim this validator relayed must not later trip its own attest gate.
    store = ValidatorStateStore(tmp_path / 'state.db')
    canonical = '0x' + HEX.lower()
    store.record_deposit_claim(canonical, swap_key_from_tx_hash(canonical).hex(), 1400)
    loop, swap = _hex_source_loop(canonical, store)
    assert loop.decide(swap, now=1500).decision == SwapDecision.ATTEST


# ── a swap's payout is never a deposit ───────────────────────────────────────
# A miner refilling its deposit wallet through allways announces the payout before it lands; a taker
# who reserves naming the payer as source must not be able to claim that delivery as their deposit.

PAYER = '0x' + 'aB' * 20  # the paying miner's payout wallet
DEPOSIT_WALLET = '0x' + 'cD' * 20  # the refilling miner's deposit wallet


def _payout_relay(store):
    spec = next(s for s in HEX_SPECS if s.chain_id == 'eth')
    resv = dict(created_at=CREATED_AT, from_addr=PAYER, miner_from_addr=DEPOSIT_WALLET)
    return _relay(spec, {'sol': resv}, store)


@pytest.mark.parametrize('reported', ['0x' + HEX.upper(), '0x' + HEX.lower()])
def test_a_reported_payout_hash_alone_does_not_block_a_deposit(tmp_path, reported):
    # Another swap's SwapFulfilled names this hash, but no validator verified it as a delivery.
    store = ValidatorStateStore(tmp_path / 'state.db')
    store.record_swap_fulfillment('aa' * 32, reported, CREATED_AT)
    validator, client, _ = _payout_relay(store)
    assert confirm_deposit(validator, HOTKEY, '0x' + HEX).ok and len(client.claims) == 1


def test_a_verified_delivery_cannot_be_claimed(tmp_path):
    store = ValidatorStateStore(tmp_path / 'state.db')
    store.claim_delivery('payingMiner', '0x' + HEX, 'aa' * 32, CREATED_AT)
    validator, client, _ = _payout_relay(store)
    assert not confirm_deposit(validator, HOTKEY, '0x' + HEX).ok and not client.claims


def test_an_honest_deposit_is_unaffected_by_reported_fulfillments(tmp_path):
    store = ValidatorStateStore(tmp_path / 'state.db')
    store.record_swap_fulfillment('cc' * 32, '0x' + 'f' * 64, CREATED_AT)
    validator, client, _ = _payout_relay(store)
    assert confirm_deposit(validator, HOTKEY, '0x' + HEX).ok and len(client.claims) == 1


def _refill_and_claim(store):
    """A live refill paying the miner's deposit wallet, and a claim naming its payer as source."""
    loop, claim = _hex_source_loop('0x' + HEX.lower(), store)
    claim.user_from_addr, claim.miner_from_addr = 'payerBTC', 'minerBTC'
    refill = make_swap(status='Active', from_chain='sol', to_chain='btc', key=b'\x02' * 32)
    refill.miner_to_addr, refill.user_to_addr = 'payerBTC', 'minerBTC'
    return loop, claim, refill


def test_a_live_swap_paying_the_same_wallets_does_not_block_a_claim(tmp_path):
    # A live swap pays from the claim's source to its deposit wallet; the claim still attests on its own merits.
    store = ValidatorStateStore(tmp_path / 'state.db')
    loop, claim, refill = _refill_and_claim(store)
    loop.read_only = True
    loop.client.get_swaps = lambda: [(None, claim), (None, refill)]
    decisions = dict(loop.run_once(now=1500))
    assert decisions[claim.swap_key.hex()] == SwapDecision.ATTEST


# ── ledger rebuilt from program events ───────────────────────────────────────


def _event(name, **fields):
    return EventRecord(name=name, fields=fields, slot=1, block_time=1400, signature='sig')


def test_program_events_rebuild_the_ledger_on_a_fresh_store(tmp_path):
    # A fresh database replays program history; claims and initiations land whoever the miner is.
    store = ValidatorStateStore(tmp_path / 'state.db')
    index = SolanaEventIndex(store)
    upper = '0x' + HEX.upper()
    key = swap_key_from_tx_hash(upper)
    miner = bytes(32)
    index.ingest(
        [_event('SwapClaimed', swap_key=key, miner=miner, user=miner, from_tx_hash=upper, from_tx_block=1)], {}
    )
    assert store.deposit_claim('0x' + HEX.lower()) == (key.hex(), False)  # hex hashes key case-blind
    index.ingest(
        [
            _event(
                'SwapInitiated',
                swap_key=key,
                user=miner,
                miner=miner,
                collateral_amount=1,
                from_amount=1,
                to_amount=1,
                initiated_at=1400,
                collateral_chain='sol',
            )
        ],
        {},
    )
    assert store.deposit_claim(upper) == (key.hex(), True)


def test_solana_signatures_key_the_ledger_as_given(tmp_path):
    store = ValidatorStateStore(tmp_path / 'state.db')
    store.record_deposit_claim(SOL_SIG, 'k', 1)
    assert store.deposit_claim(SOL_SIG) == ('k', False) and store.deposit_claim(SOL_SIG.lower()) is None


def test_completed_swaps_rebuild_the_delivery_ledger(tmp_path):
    # Only a completed swap's delivery counts; a fulfilled-but-unconfirmed one leaves the tx free.
    store = ValidatorStateStore(tmp_path / 'state.db')
    index = SolanaEventIndex(store)
    miner, done, open_ = bytes(32), b'\x01' * 32, b'\x02' * 32
    delivery = '0x' + HEX.upper()
    for key in (done, open_):
        index.ingest([_event('SwapFulfilled', swap_key=key, miner=miner, to_tx_hash=delivery, to_amount=1)], {})
    index.ingest(
        [
            _event(
                'SwapCompleted',
                swap_key=done,
                miner=miner,
                collateral_amount=1,
                fee=0,
                from_chain='eth',
                to_chain='sol',
                from_amount=1,
                to_amount=1,
            )
        ],
        {},
    )
    assert not store.claim_delivery(str(miner), delivery, open_.hex(), 1500)
    assert store.claim_delivery(str(miner), '0x' + HEX.lower(), done.hex(), 1500)


def test_completed_swap_replaces_a_local_delivery_claim(tmp_path):
    # This validator first verified the other swap; the chain completing one wins the tx.
    store = ValidatorStateStore(tmp_path / 'state.db')
    assert store.claim_delivery('m', '0x' + HEX, 'local', 1)
    assert store.claim_delivery('m', '0x' + HEX, 'onchain', 2, settled=True)
    assert not store.claim_delivery('m', '0x' + HEX, 'local', 3)


# ── delivery leg ─────────────────────────────────────────────────────────────
# The miner writes to_tx_hash itself, so the validator cannot respell it: junk never verifies, while an
# honest casing verifies in canonical form and keys the same delivery ledger row.

BTC_SPEC = next(s for s in ASSET_REGISTRY if s.chain_id == 'btc')
SOL_SPEC = next(s for s in ASSET_REGISTRY if s.chain_id == 'sol')
# One per hash family: UTXO, EVM native, ERC-20, TAO, alpha.
DEST_SPECS = [BTC_SPEC] + [s for s in HEX_SPECS if s.chain_id in ('eth', 'baseusdc', 'tao') or s.chain_id[:2] == 'sn']


def _canonical(spec):
    return HEX.lower() if spec is BTC_SPEC else '0x' + HEX.lower()


def _honest_spellings(spec):
    if spec is BTC_SPEC:
        return [HEX.lower(), HEX, HEX.upper()]
    return ['0x' + HEX.lower(), '0x' + HEX, '0x' + HEX.upper(), '0X' + HEX]


def _junk_spellings(spec):
    canonical = _canonical(spec)
    wrong_prefix = '0x' + HEX if spec is BTC_SPEC else HEX
    return [canonical + s for s in URL_SUFFIXES + ('\n',)] + [' ' + canonical, wrong_prefix, canonical[:-1], '']


def _cases(spellings):
    return [pytest.param(s, h, id=f'{s.chain_id}-{h!r}') for s in DEST_SPECS for h in spellings(s)]


def _dest_loop(spec, store=None):
    loop, providers = loop_with(result=True, state_store=store)
    dest = providers['sol']
    dest.canonical_tx_hash = _build(spec).chain.canonical_tx_hash
    loop.providers[spec.chain_id] = dest
    return loop, dest


def _delivery(spec, to_tx_hash, key=b'\x01' * 32):
    swap = make_swap(from_chain='sol', to_chain=spec.chain_id, key=key)
    swap.to_tx_hash = to_tx_hash
    return swap


def test_dest_specs_cover_every_hash_family():
    assert {'btc', 'eth', 'baseusdc', 'tao'} <= {s.chain_id for s in DEST_SPECS}
    assert any(s.chain_id.startswith('sn') for s in DEST_SPECS)


@pytest.mark.parametrize('spec,spelling', _cases(_honest_spellings))
def test_honest_delivery_spelling_verifies_in_canonical_form(spec, spelling):
    loop, dest = _dest_loop(spec)
    assert loop.decide(_delivery(spec, spelling), now=1500).decision == SwapDecision.CONFIRM
    assert dest.calls[-1].tx_hash == _canonical(spec)


@pytest.mark.parametrize('spec,spelling', _cases(_junk_spellings))
def test_junk_delivery_spelling_never_verifies(spec, spelling):
    loop, dest = _dest_loop(spec)
    swap = _delivery(spec, spelling)
    assert loop.decide(swap, now=1500).decision == SwapDecision.WAIT
    assert loop.decide(swap, now=2500).decision == SwapDecision.TIMEOUT
    assert dest.calls == []


@pytest.mark.parametrize('spec,spelling', _cases(_honest_spellings) + _cases(_junk_spellings))
def test_one_delivery_confirms_one_swap_whatever_the_second_spelling(spec, spelling, tmp_path):
    loop, _ = _dest_loop(spec, ValidatorStateStore(tmp_path / 'state.db'))
    first, second = _delivery(spec, _canonical(spec)), _delivery(spec, spelling, b'\x02' * 32)
    assert loop.decide(first, now=1500).decision == SwapDecision.CONFIRM
    assert loop.decide(second, now=1500).decision == SwapDecision.WAIT
    assert loop.decide(second, now=2500).decision == SwapDecision.TIMEOUT


@pytest.mark.parametrize('spec', DEST_SPECS, ids=lambda s: s.chain_id)
def test_a_claimed_source_deposit_never_confirms_a_delivery(spec, tmp_path):
    store = ValidatorStateStore(tmp_path / 'state.db')
    store.record_deposit_claim(_canonical(spec), 'ee' * 32, 1400)
    loop, _ = _dest_loop(spec, store)
    swap = _delivery(spec, _canonical(spec).upper() if spec is BTC_SPEC else _canonical(spec))
    assert loop.decide(swap, now=1500).decision == SwapDecision.WAIT
    assert loop.decide(swap, now=2500).decision == SwapDecision.TIMEOUT


def test_solana_delivery_signature_is_verified_as_given():
    # base58 has one spelling per signature; a malformed one is the RPC's to reject.
    loop, dest = _dest_loop(SOL_SPEC)
    assert loop.decide(_delivery(SOL_SPEC, SOL_SIG), now=1500).decision == SwapDecision.CONFIRM
    assert dest.calls[-1].tx_hash == SOL_SIG
    assert loop.decide(_delivery(SOL_SPEC, ''), now=1500).decision == SwapDecision.WAIT

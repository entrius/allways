"""A quote naming an address its chain rejects is never a live rate: zero from the event that posts it
until a good quote replaces it, lane by lane, judged from the quote's own on-chain account."""

from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import bittensor as bt
import numpy as np
import pytest
from solders.keypair import Keypair

from allways.assets import ASSET_REGISTRY
from allways.chains import SUPPORTED_CHAINS, canonical_pair
from allways.classes import ActivityTransition
from allways.constants import (
    CROWN_RATE_BAND,
    DIRECTION_POOLS,
    ELIGIBILITY_FILL_WINDOW_SECS,
    LAUNCH_PAIRS,
    QUALITY_VOLUME_BETA,
    RATE_PRECISION,
    RECONCILE_QUIET_SECS,
    RECYCLE_UID,
    SCORING_WINDOW_BLOCKS,
    declarable_backings,
)
from allways.solana.events import EventRecord
from allways.solana.pdas import BACKING_BITS
from allways.validator import event_index as event_index_mod
from allways.validator import scoring as scoring_mod
from allways.validator.event_index import SolanaEventIndex
from allways.validator.scoring import (
    calculate_miner_rewards,
    compute_direction_pools,
    fill_held_crown,
    lane_volumes_to_directions,
    snapshot_current_crown_holders,
)
from allways.validator.state_store import ValidatorStateStore

BAD = 'bad'
HONEST = 'honest'
ATTRIBUTION = {BAD: BAD, HONEST: HONEST}
BTC_ADDRESS = 'bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh'
TAO_ADDRESS = bt.Keypair.create_from_seed('0x' + '11' * 32).ss58_address
SOL_ADDRESS = str(Keypair.from_seed(bytes.fromhex('22' * 32)).pubkey())
FROZEN = str(Keypair.from_seed(bytes.fromhex('33' * 32)).pubkey())  # well-formed, refused by the fake probe
UNREADABLE = str(Keypair.from_seed(bytes.fromhex('44' * 32)).pubkey())  # well-formed, the fake probe raises
EVM_ADDRESS = '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266'
ZERO_EVM_ADDRESS = '0x' + '0' * 40
BAD_EIP55_ADDRESS = '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92267'
GOOD_BY_FAMILY = {
    'btc': BTC_ADDRESS,
    'ss58': TAO_ADDRESS,
    'solana': SOL_ADDRESS,
    'evm': EVM_ADDRESS,
}


def address_family(provider) -> str:
    accepted = [family for family, address in GOOD_BY_FAMILY.items() if provider.chain.is_valid_address(address)]
    assert len(accepted) == 1, f'{type(provider.chain).__name__} accepted address families {accepted}'
    return accepted[0]


def pair_shape(providers, chain_id: str, pair: tuple[str, str]) -> tuple[str, tuple[str, ...]]:
    other = pair[1] if pair[0] == chain_id else pair[0]
    return address_family(providers[other]), tuple(declarable_backings(*pair))


def representative_pairs(providers, chain_id: str) -> list[tuple[str, str]]:
    representatives = {}
    for pair in LAUNCH_PAIRS:
        if chain_id in pair:
            representatives.setdefault(pair_shape(providers, chain_id, pair), pair)
    return list(representatives.values())


def lane_cases(providers):
    """A compact covering set, derived from live provider/validator objects: every address-validator
    class, both sides, both directions, every backing, and every foreign address family."""
    representatives = {}
    for chain_id, provider in providers.items():
        representatives.setdefault(type(provider.chain), chain_id)

    candidates = []
    for chain_id in representatives.values():
        own_family = address_family(providers[chain_id])
        invalid = [('', 'empty'), ('not-an-address', 'garbage')]
        invalid += [
            (address, f'{family}-on-{own_family}') for family, address in GOOD_BY_FAMILY.items() if family != own_family
        ]
        invalid += [(ZERO_EVM_ADDRESS, 'zero-evm'), (BAD_EIP55_ADDRESS, 'bad-eip55')]
        pairs = representative_pairs(providers, chain_id)
        for index, (bad_address, bad_name) in enumerate(invalid):
            pair = pairs[index % len(pairs)]
            directed = pair if index % 2 == 0 else pair[::-1]
            side = 'from' if directed[0] == chain_id else 'to'
            backings = declarable_backings(*directed)
            backing = backings[index % len(backings)]
            candidates.append((*directed, backing, side, bad_address, f'{chain_id}-{side}-{bad_name}'))

        # Ensure every pair shape and backing exposed by this validator class appears.
        for pair in pairs:
            for direction_index, directed in enumerate((pair, pair[::-1])):
                side = 'from' if directed[0] == chain_id else 'to'
                for backing in declarable_backings(*directed):
                    candidates.append((*directed, backing, side, '', f'{chain_id}-{direction_index}-{backing}'))

    unique = {}
    for case in candidates:
        unique.setdefault(case[:-1], case)
    return list(unique.values())


def constructor_kwargs(spec):
    available = {
        'solana_rpc_url': 'http://offline.invalid',
        'solana_keypair': MagicMock(),
        'subtensor': MagicMock(),
    }
    return {name: available[name] for name in spec.kwarg_names if name in available}


@pytest.fixture(scope='session')
def providers():
    monkeypatch = pytest.MonkeyPatch()
    for chain in SUPPORTED_CHAINS.values():
        if chain.env_prefix:
            monkeypatch.setenv(f'{chain.env_prefix}_NETWORK', 'mainnet')
    built = {spec.chain_id: spec.cls(**constructor_kwargs(spec)) for spec in ASSET_REGISTRY}
    for provider in built.values():
        provider.address_refused = refused_probe  # the chain's own probe is RPC; the asset tests cover it
    yield built
    monkeypatch.undo()


PROBED: list[str] = []


def refused_probe(address: str) -> bool:
    PROBED.append(address)
    if address == UNREADABLE:
        raise ConnectionError('rpc down')
    return address == FROZEN


@pytest.fixture(autouse=True)
def fresh_probe():
    PROBED.clear()


@pytest.fixture(autouse=True)
def identity_attribution(monkeypatch):
    monkeypatch.setattr(scoring_mod, 'build_attribution', lambda client: ATTRIBUTION)


def lane_of(quote):
    return (quote.miner, quote.from_chain, quote.to_chain, quote.collateral_chain)


class FakeSolanaClient:
    """The live chain: every miner active and clean, ``quotes`` the live MinerQuote book the round's
    reconcile reads, ``accounts`` the MinerQuote accounts the ingest reads (set by ``post_quotes``)."""

    def __init__(self, quotes):
        self.quotes = quotes
        self.accounts = {}
        self.reads = Counter()
        self.reachable = True

    def get_quote(self, miner, from_chain, to_chain, backing):
        self.reads[(miner, from_chain, to_chain, backing)] += 1
        if not self.reachable:
            raise ConnectionError('rpc down')
        return self.accounts.get((miner, from_chain, to_chain, backing))

    def get_all(self, name: str):
        if name == 'MinerState':
            return [
                (
                    f'pda-{hotkey}',
                    SimpleNamespace(
                        miner=hotkey, successful_swaps=0, failed_swaps=0, active=True, collateral=0, settling_until=0
                    ),
                )
                for hotkey in (BAD, HONEST)
            ]
        if name == 'MinerQuote':
            return [(f'quote-{i}', quote) for i, quote in enumerate(self.quotes)]
        return []


def make_quote(hotkey, from_chain, to_chain, backing, rate, from_addr, to_addr):
    return SimpleNamespace(
        miner=hotkey,
        from_chain=from_chain,
        to_chain=to_chain,
        collateral_chain=backing,
        rate=int(rate * RATE_PRECISION),
        miner_from_addr=from_addr,
        miner_to_addr=to_addr,
    )


def pad_hotkeys():
    hotkeys = [BAD, HONEST]
    while len(hotkeys) <= RECYCLE_UID:
        hotkeys.append(f'filler-{len(hotkeys)}')
    return hotkeys


def make_validator(tmp_path: Path, live_quotes, providers, now=10_000):
    hotkeys = pad_hotkeys()
    store = ValidatorStateStore(db_path=tmp_path / 'state.db')
    store.set_relay_meta(ValidatorStateStore.LEDGER_SINCE_KEY, str(now - ELIGIBILITY_FILL_WINDOW_SECS))
    for hotkey in (BAD, HONEST):
        store.insert_active_event(0, hotkey, True)
        for backing in BACKING_BITS:
            store.insert_clearing_rate(now - 1, hotkey, 'btc', 'sol', 1, 1, f'elig-{hotkey}-{backing}', backing)
    config = MagicMock()
    config.config.return_value = SimpleNamespace(
        min_swap_amount=0, max_swap_amount=0, tao_min_swap_amount=0, tao_max_swap_amount=0
    )
    storage = MagicMock()
    storage.is_enabled.return_value = False
    chain = FakeSolanaClient(live_quotes)
    return SimpleNamespace(
        block=now,
        last_scored_block=now - SCORING_WINDOW_BLOCKS,
        last_scored_time=now - SCORING_WINDOW_BLOCKS,
        metagraph=SimpleNamespace(n=SimpleNamespace(item=lambda: len(hotkeys)), hotkeys=hotkeys),
        state_store=store,
        event_index=SolanaEventIndex(store, providers=providers, quote_reader=chain.get_quote),
        solana_config_cache=config,
        database_storage=storage,
        solana_client=chain,
        assets=providers,
        alpha_prices={},
    )


def quote_record(quote, at: int) -> EventRecord:
    """The QuoteSet the ingest hands the index: no addresses, those live on the quote's account."""
    fields = {
        'miner': quote.miner,
        'from_chain': quote.from_chain,
        'to_chain': quote.to_chain,
        'collateral_chain': quote.collateral_chain,
        'rate': quote.rate,
    }
    return EventRecord('QuoteSet', fields, slot=at, block_time=at, signature=f'{quote.miner}@{at}')


def post_quotes(validator, quotes, at=0, on_chain=True):
    """Write each quote's account (unless it was removed again before the ingest read), then ingest."""
    if on_chain:
        validator.solana_client.accounts.update({lane_of(quote): quote for quote in quotes})
    validator.event_index.ingest([quote_record(quote, at) for quote in quotes], ATTRIBUTION)


def post_quotes_account_missing(validator, quotes, at):
    post_quotes(validator, quotes, at, on_chain=False)


def post_quotes_reader_raising(validator, quotes, at):
    validator.solana_client.reachable = False
    post_quotes(validator, quotes, at)
    validator.solana_client.reachable = True


def post_quotes_no_reader(validator, quotes, at):
    validator.event_index = SolanaEventIndex(validator.state_store, providers=validator.assets)
    post_quotes(validator, quotes, at)


def write_pre_upgrade_rows(validator, quotes, at=0):
    """What a validator wrote before the ingest judged addresses: the posted rate, as is."""
    for quote in quotes:
        validator.state_store.insert_rate_event(
            quote.miner, quote.from_chain, quote.to_chain, quote.rate / RATE_PRECISION, at, quote.collateral_chain
        )


def derived_rate(validator, quote):
    return validator.state_store.lanes_with_live_rate(validator.block).get(lane_of(quote))


def score(validator) -> np.ndarray:
    rewards, _ = calculate_miner_rewards(validator, validator.block)
    return rewards


def next_round(validator):
    validator.last_scored_time = validator.block
    validator.block += SCORING_WINDOW_BLOCKS


def rates_for(from_chain: str, to_chain: str):
    """BAD's rate, then HONEST's: BAD best by more than the band, so they never share a crown."""
    canonical = canonical_pair(from_chain, to_chain)
    lower_wins = from_chain != canonical[0]
    return (1.0, 1.0 + 2 * CROWN_RATE_BAND) if lower_wins else (2.0, 1.0)


def good_address(providers, chain: str) -> str:
    return GOOD_BY_FAMILY[address_family(providers[chain])]


def quote_pair_for(providers, from_chain, to_chain, backing, bad_address, side):
    bad_rate, honest_rate = rates_for(from_chain, to_chain)
    good_from = good_address(providers, from_chain)
    good_to = good_address(providers, to_chain)
    bad_from = bad_address if side == 'from' else good_from
    bad_to = bad_address if side == 'to' else good_to
    return [
        make_quote(BAD, from_chain, to_chain, backing, bad_rate, bad_from, bad_to),
        make_quote(HONEST, from_chain, to_chain, backing, honest_rate, good_from, good_to),
    ]


def first_lane(pair):
    return (*pair, declarable_backings(*pair)[0])


def first_lane_quotes(providers, pair, bad_address):
    """BAD and HONEST quoting ``pair`` on its first backing; BAD's from-address is ``bad_address``."""
    return quote_pair_for(providers, *first_lane(pair), bad_address, 'from')


def lane_reward(from_chain, to_chain, backing) -> float:
    """What one miner earns holding a lane's whole crown for the whole window, no fills."""
    pool = DIRECTION_POOLS[(from_chain, to_chain)] / len(declarable_backings(from_chain, to_chain))
    return (1 - QUALITY_VOLUME_BETA) * pool


def assert_rewards(rewards, bad: float, honest: float):
    np.testing.assert_allclose(rewards[:2], [bad, honest], rtol=1e-6, atol=0)


# ─── lane-level zero, exact to the event ──────────────────────────────────────


def test_bad_address_lanes_get_zero_and_the_honest_next_best_takes_the_crown(tmp_path, providers):
    for index, (from_chain, to_chain, backing, side, bad_address, case_id) in enumerate(lane_cases(providers)):
        case_path = tmp_path / str(index)
        case_path.mkdir()
        quotes = quote_pair_for(providers, from_chain, to_chain, backing, bad_address, side)
        validator = make_validator(case_path, quotes, providers)
        post_quotes(validator, quotes)
        rewards = score(validator)
        validator.state_store.close()
        assert rewards[0] == 0.0, f'{case_id}: bad-address miner earned {rewards[0]}'
        # A backing not yet served (baseusdc) has no purse stream, so its crown fails closed for everyone.
        honest = lane_reward(from_chain, to_chain, backing) if backing in BACKING_BITS else 0.0
        np.testing.assert_allclose(rewards[1], honest, rtol=1e-6, err_msg=case_id)


def test_guard_every_supported_chain_rejects_a_foreign_family_address(providers):
    assert set(providers) == set(SUPPORTED_CHAINS)
    for chain_id, provider in providers.items():
        own_family = address_family(provider)
        foreign = next(address for family, address in GOOD_BY_FAMILY.items() if family != own_family)
        assert not provider.chain.is_valid_address(foreign), chain_id


def test_guard_fully_valid_quotes_keep_earning(tmp_path, providers):
    pair = LAUNCH_PAIRS[0]
    quotes = first_lane_quotes(providers, pair, good_address(providers, pair[0]))
    validator = make_validator(tmp_path, quotes, providers)
    post_quotes(validator, quotes)
    before = score(validator)
    after = score(validator)
    np.testing.assert_array_equal(after, before)
    assert_rewards(before, lane_reward(*first_lane(pair)), 0.0)
    validator.state_store.close()


def pair_taking(providers, address) -> tuple[str, str]:
    """A launch pair whose first chain accepts ``address`` offline, so only the chain probe can refuse it."""
    return next(pair for pair in LAUNCH_PAIRS if providers[pair[0]].chain.is_valid_address(address))


@pytest.mark.parametrize('side', ['from', 'to'])
def test_an_address_its_chain_refuses_earns_zero_from_the_post(tmp_path, providers, side):
    pair = pair_taking(providers, FROZEN)[:: 1 if side == 'from' else -1]
    lane = (*pair, declarable_backings(*pair)[0])
    quotes = quote_pair_for(providers, *lane, FROZEN, side)
    validator = make_validator(tmp_path, quotes, providers)
    post_quotes(validator, quotes)
    assert_rewards(score(validator), 0.0, lane_reward(*lane))
    validator.state_store.close()


def test_an_address_is_probed_once_and_an_unreadable_probe_judges_nothing(tmp_path, providers):
    quotes = quote_pair_for(providers, *first_lane(pair_taking(providers, UNREADABLE)), UNREADABLE, 'from')
    validator = make_validator(tmp_path, quotes, providers)
    post_quotes(validator, quotes)
    post_quotes(validator, quotes, at=1)
    assert derived_rate(validator, quotes[0]) == quotes[0].rate / RATE_PRECISION, 'an unread probe refuses nothing'
    assert PROBED.count(UNREADABLE) == 2, 'unread is asked again on the next post'
    assert PROBED.count(quotes[1].miner_from_addr) == 1, 'a read verdict is kept'
    validator.state_store.close()


def test_a_bad_address_bans_the_miner_on_every_lane(tmp_path, providers):
    first, second = LAUNCH_PAIRS[:2]
    quotes = first_lane_quotes(providers, first, '') + first_lane_quotes(
        providers, second, good_address(providers, second[0])
    )
    validator = make_validator(tmp_path, quotes, providers)
    post_quotes(validator, quotes)
    assert_rewards(score(validator), 0.0, lane_reward(*first_lane(first)) + lane_reward(*first_lane(second)))
    validator.state_store.close()


def test_a_corrected_quote_earns_only_after_the_ban_ends(tmp_path, providers, monkeypatch):
    monkeypatch.setattr(scoring_mod, 'ADDRESS_BAN_SECS', SCORING_WINDOW_BLOCKS)
    pair = LAUNCH_PAIRS[0]
    lane = first_lane(pair)
    bad, honest = first_lane_quotes(providers, pair, '')
    good = make_quote(BAD, *lane, bad.rate / RATE_PRECISION, good_address(providers, pair[0]), bad.miner_to_addr)
    validator = make_validator(tmp_path, [good, honest], providers)
    post_quotes(validator, [honest], at=validator.last_scored_time - 1)
    post_quotes(validator, [bad], at=validator.last_scored_time + 1)
    post_quotes(validator, [good], at=validator.last_scored_time + 2)
    assert_rewards(score(validator), 0.0, lane_reward(*lane))
    next_round(validator)
    assert_rewards(score(validator), lane_reward(*lane), 0.0)
    validator.state_store.close()


# ─── downstream of the zeroed lane: fills, pools, the live crown ──────────────


@pytest.fixture
def bad_quote_reserved(tmp_path, providers):
    """BAD's bad-address quote and HONEST's good one both live, both miners reserved on the lane's hub."""
    pair = LAUNCH_PAIRS[0]
    lane = first_lane(pair)
    quotes = first_lane_quotes(providers, pair, '')
    validator = make_validator(tmp_path, quotes, providers)
    post_quotes(validator, quotes)
    reserved_at = validator.block - 100
    for hotkey in (BAD, HONEST):
        validator.state_store.insert_activity_event(reserved_at, hotkey, ActivityTransition.RESERVE_START, hub=lane[2])
    yield validator, lane, reserved_at
    validator.state_store.close()


def held_crown(validator, hotkey, lane, reserved_at):
    return fill_held_crown(validator, hotkey, *lane, 100_000_000, reserved_at)


def test_the_honest_fill_is_qualified_while_the_bad_quote_showed_the_best_rate(bad_quote_reserved):
    validator, lane, reserved_at = bad_quote_reserved
    assert held_crown(validator, HONEST, lane, reserved_at)
    assert not held_crown(validator, BAD, lane, reserved_at)


def test_the_honest_fill_keeps_the_pair_live_and_earns_beta(bad_quote_reserved):
    validator, lane, reserved_at = bad_quote_reserved
    for hotkey in (BAD, HONEST):
        validator.state_store.insert_clearing_rate(
            validator.block - 1,
            hotkey,
            *lane[:2],
            1,
            2,
            f'{hotkey}-fill',
            backing=lane[2],
            qualified=held_crown(validator, hotkey, lane, reserved_at),
            collateral_amount=100_000_000,
        )
    lane_volumes = validator.state_store.get_qualified_lane_volumes(0, validator.block)
    pools = compute_direction_pools(lane_volumes_to_directions(lane_volumes))
    assert pools[lane] > 0
    assert list(lane_volumes[lane]) == [HONEST]
    rewards = score(validator)
    assert rewards[0] == 0.0
    assert rewards[1] >= QUALITY_VOLUME_BETA * pools[lane]


def test_the_live_crown_snapshot_excludes_the_bad_lane(bad_quote_reserved):
    validator, lane, reserved_at = bad_quote_reserved
    holders = [row[3] for row in snapshot_current_crown_holders(validator, reserved_at - 1)[lane]]
    assert holders == [HONEST]


# ─── unjudged quotes: the once-per-round reconcile against the live book ──────


def test_a_missing_provider_leaves_the_lane_unjudged(tmp_path, providers):
    pair = LAUNCH_PAIRS[0]
    quotes = first_lane_quotes(providers, pair, '')
    assets = dict(providers)
    assets.pop(pair[0])
    validator = make_validator(tmp_path, quotes, assets)
    post_quotes(validator, quotes)
    for _round in range(2):
        assert_rewards(score(validator), lane_reward(*first_lane(pair)), 0.0)
        next_round(validator)
    validator.state_store.close()


@pytest.mark.parametrize(
    'posted_before_round', [RECONCILE_QUIET_SECS + 1, RECONCILE_QUIET_SECS - 1], ids=['quiet', 'inside-quiet-window']
)
@pytest.mark.parametrize(
    'post',
    [
        post_quotes,
        post_quotes_account_missing,
        post_quotes_reader_raising,
        post_quotes_no_reader,
        write_pre_upgrade_rows,
    ],
    ids=['judged', 'account-missing', 'reader-raises', 'no-reader', 'pre-upgrade-row'],
)
def test_a_bad_quote_is_zeroed_by_its_event_or_else_by_the_first_quiet_reconcile_and_never_restored(
    tmp_path, providers, posted_before_round, post
):
    zeroed_by_ingest = post is post_quotes
    pair = LAUNCH_PAIRS[0]
    lane = first_lane(pair)
    quotes = first_lane_quotes(providers, pair, '')
    validator = make_validator(tmp_path, quotes, providers)
    post(validator, quotes, validator.block - posted_before_round)
    assert (derived_rate(validator, quotes[0]) is None) is zeroed_by_ingest, 'an unjudged quote ingests as posted'
    # The reconcile stamps its zero at the round's end, and skips a lane touched inside the quiet window.
    zeroed_after_round = 0 if zeroed_by_ingest else (1 if posted_before_round > RECONCILE_QUIET_SECS else 2)
    for round_number in (1, 2, 3, 4):
        rewards = score(validator)
        paid = round_number <= zeroed_after_round
        assert_rewards(rewards, lane_reward(*lane) if paid else 0.0, 0.0 if paid else lane_reward(*lane))
        assert (derived_rate(validator, quotes[0]) is None) == (round_number >= zeroed_after_round)
        next_round(validator)  # the bad quote stays live in the book: the zero is never "corrected" back
    validator.state_store.close()


# ─── ingest: one account read per QuoteSet, one chain probe per address ──────


def test_each_quote_set_reads_its_account_once(tmp_path, providers):
    quotes = first_lane_quotes(providers, LAUNCH_PAIRS[0], '')
    validator = make_validator(tmp_path, quotes, providers)
    post_quotes(validator, quotes)
    assert validator.solana_client.reads == Counter({lane_of(quote): 1 for quote in quotes})
    validator.state_store.close()


def test_a_full_refusal_memo_still_probes_a_new_address(tmp_path, providers, monkeypatch):
    monkeypatch.setattr(event_index_mod, 'ADDRESS_REFUSAL_MEMO_SIZE', 1)
    lane = first_lane(pair_taking(providers, FROZEN))
    quotes = quote_pair_for(providers, *lane, FROZEN, 'from')
    validator = make_validator(tmp_path, quotes, providers)
    post_quotes(validator, quotes[::-1])  # the honest quote's addresses fill the memo first
    assert_rewards(score(validator), 0.0, lane_reward(*lane))
    assert len(validator.event_index._address_refusals) == 1
    validator.state_store.close()

"""Quote optimizer — opt-in auto-quoting for a miner's standing quotes.

The operator funds the wallets and collateral and posts each managed quote once by hand; from then on
the optimizer keeps those quotes priced and says what it did on a webhook.

Pricing. Each lane follows the best OTHER qualifying quote (the leader): it quotes ``CROWN_EDGE_INSET``
inside the worse edge of the validators' crown band (``CROWN_RATE_BAND``), so it shares crown time
without setting the price and a one-tick undercut can't push it out. The leader is followed only while
that edge is within ``[-max_worse, +max_better]`` % of the USD spot rate, measured as what the taker
receives. With no leader, or a leader stingier than ``-max_worse``, it leads at ``-max_worse`` — the
least generous rate the operator tolerates. A leader more generous than ``+max_better`` is not followed.

Costs. Routine repricing waits until a quote is past the contract's churn window (plus a random
``REQUOTE_JITTER_SECS``, so a rival can't time a move to land just before it), so it is always free;
re-creating a pulled quote is free too. A fee is paid only to protect against a loss, on fresh data:
- market drift made our quote more than ``+max_better`` generous, we are the first quote a taker would
  pick, and what we'd lose on a full-size fill beyond the tolerated edge is at least the fee;
- the delivery wallet can't cover a full-size fill, and a missed fill (the timeout premium) would cost
  more than the fee — or the next strike would end the hotkey's emissions;
- the optimizer can't see the market (its websocket feed has been down past ``DEAD_MAN_SECS``) or the miner stops.

Data. Program state comes from websocket pushes (``market_feed.MarketFeed``) seeded from the allways API, so an
opted-in miner makes no RPC reads in steady state: Helius is used to send ``set_quote`` / ``remove_quote``, plus
a Config and SOL-balance read each time the feed has to be re-seeded. Every data gap leans the miner's way: no
API at start keeps the optimizer off, a re-seed the API can't answer holds everything but funding pulls, an
unreadable price holds every quote, an empty quote list reads as "no leader" (lead at the least generous rate),
and a balance read as 0 never pays a fee on its own. Every rule reads the lane's canonical pair, backing and chain
provider, so a new pair needs its chains in ``OPTIMIZER_CHAINS`` and a price id in ``market_price.COINGECKO_IDS``.
"""

import json
import random
import time
from collections import defaultdict
from dataclasses import dataclass, field, fields
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Callable, Dict, List, NamedTuple, Optional, Tuple

import bittensor as bt
import requests

from allways.chains import canonical_pair, get_chain_def
from allways.constants import (
    COLLATERAL_REQUIREMENT_BPS,
    CROWN_RATE_BAND,
    ELIGIBILITY_FILL_WINDOW_SECS,
    FEE_DIVISOR,
    MAX_FAILED_SWAPS,
    QUOTE_UPDATE_FEE_TIERS,
    RATE_PRECISION,
    RATE_SIG_FIGS,
    declarable_backings,
    quote_update_fee_lamports,
    required_collateral,
)
from allways.miner.market_feed import fixed_rate, make_quote, market_from_das
from allways.miner.market_price import MarketPrices
from allways.miner.miner_api import AllwaysApi
from allways.solana.client import benign_marker
from allways.solana.layouts import hub_busy_until, hub_swap_on
from allways.solana.pdas import BACKING_BITS
from allways.utils.logging import log_on_change
from allways.utils.rate import apply_fee_deduction, calculate_to_amount, is_executable_rate, quantize_rate_fixed

# Chains the optimizer manages: the two hubs, whose RPCs every miner already runs.
OPTIMIZER_CHAINS = ('sol', 'tao')
OPTIMIZER_TICK_SECONDS = 60
DEFAULT_OPTIMIZER_CONFIG_PATH = Path.home() / '.allways' / 'miner' / 'optimizer.json'
# Routine updates wait until a quote's churn fee has decayed to zero.
QUOTE_UPDATE_FREE_AFTER_SECS = QUOTE_UPDATE_FEE_TIERS[-1][0]
# A routine requote waits a further random 0..this many seconds past the free window, so rivals can't time a move
# to land just before it.
REQUOTE_JITTER_SECS = 30
# How far inside the crown band's worse edge a follower quotes. At the bare edge a rival's one-tick
# improvement moves the band past us until our next free update; this makes them give takers 0.1% more
# every time instead.
CROWN_EDGE_INSET = 0.001
# Consecutive short readings before a funding pull — one blip should not pull a quote.
FUNDING_SHORT_CONFIRM_TICKS = 2
# What a missed fill costs past the swap itself: the user is made whole out of the 1.10× reserve while
# the miner keeps the source leg, so the loss is the over-collateralization premium.
TIMEOUT_PREMIUM_BPS = COLLATERAL_REQUIREMENT_BPS - 10_000
# Validators need a moment to see a fresh quote before its eligibility means anything.
ELIGIBILITY_CHECK_DELAY_SECS = 120
ELIGIBILITY_RECHECK_SECS = 600
# Blind this long (the feed not live), every managed quote is pulled.
DEAD_MAN_SECS = 120
# Our own transaction's block time is a little after we send it; until its push lands, the quote is assumed this
# much newer than the send, so a free-update check never runs early.
TX_LANDING_MARGIN_SECS = 30
USAGE_LOG_SECS = 3600
# Helius websocket metering: 2 credits per 0.1 MB streamed, 1 per connection opened.
WS_CREDIT_BYTES = 50_000
NO_PRICE_REASON = 'no fresh market price'
QUOTE_GONE_ERRORS = ('AccountNotInitialized', 'AccountOwnedByWrongProgram')
WEBHOOK_TIMEOUT_SECS = 5
WEBHOOK_MAX_CHARS = 1900

_CROWN_BAND = Fraction(CROWN_RATE_BAND).limit_denominator(10**6)
_EDGE_BAND = _CROWN_BAND - Fraction(CROWN_EDGE_INSET).limit_denominator(10**6)
_LEAD_HOLD = Fraction(CROWN_EDGE_INSET).limit_denominator(10**6)


class Lane(NamedTuple):
    """One quote slot: a direction plus the purse that backs it (the PDA's five seeds, minus the miner)."""

    from_chain: str
    to_chain: str
    backing: str

    @property
    def key(self) -> str:
        return f'{self.from_chain}:{self.to_chain}:{self.backing}'

    @property
    def label(self) -> str:
        return f'{self.from_chain.upper()}->{self.to_chain.upper()} [{self.backing}]'

    @property
    def reverse(self) -> bool:
        """Lower canonical rate is better for the taker — the validator's ``lower_rate_wins``."""
        return self.from_chain != canonical_pair(self.from_chain, self.to_chain)[0]


def parse_lane(spec: str) -> Lane:
    parts = [p.strip().lower() for p in str(spec).split(':')]
    if len(parts) != 3 or not all(parts):
        raise ValueError(f'lane {spec!r}: want "from:to:backing", e.g. "sol:tao:sol"')
    from_chain, to_chain, backing = parts
    for chain in (from_chain, to_chain):
        if chain not in OPTIMIZER_CHAINS:
            raise ValueError(f'lane {spec!r}: {chain} is not an optimizer chain ({", ".join(OPTIMIZER_CHAINS)})')
    if from_chain == to_chain:
        raise ValueError(f'lane {spec!r} repeats a chain')
    if backing not in declarable_backings(from_chain, to_chain):
        raise ValueError(f'lane {spec!r}: backing must be one of {declarable_backings(from_chain, to_chain)}')
    return Lane(from_chain, to_chain, backing)


def default_lanes() -> List[Lane]:
    """Every lane among the optimizer chains: both directions, each declarable backing."""
    return [
        Lane(a, b, backing)
        for a in OPTIMIZER_CHAINS
        for b in OPTIMIZER_CHAINS
        if a != b
        for backing in declarable_backings(a, b)
    ]


@dataclass
class OptimizerConfig:
    """``optimizer.json``. A missing file is the disabled default; a malformed one fails the miner at
    startup rather than quoting on a config the operator didn't write. ``lanes`` order is priority:
    when a wallet can't cover every lane paying out of it, the earlier lanes stay live."""

    enabled: bool = False
    dry_run: bool = False
    webhook_url: str = ''
    lanes: List[Lane] = field(default_factory=default_lanes)
    max_better_than_market_pct: float = 2.0
    max_worse_than_market_pct: float = 1.0
    repost_buffer_pct: float = 1.0
    sol_fee_reserve: float = 0.05
    pull_on_shutdown: bool = True
    price_usd: Dict[str, float] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> 'OptimizerConfig':
        if not path.exists():
            return cls()
        raw = json.loads(path.read_text())
        if not isinstance(raw, dict):
            raise ValueError(f'{path}: expected a JSON object')
        known = {f.name for f in fields(cls)}
        unknown = sorted(set(raw) - known)
        if unknown:
            raise ValueError(f'{path}: unknown keys {unknown} (known: {sorted(known)})')
        if 'lanes' in raw:
            raw = {**raw, 'lanes': [parse_lane(spec) for spec in raw['lanes']]}
        cfg = cls(**raw)
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.lanes:
            raise ValueError('optimizer config: lanes is empty')
        if len({lane.key for lane in self.lanes}) != len(self.lanes):
            raise ValueError('optimizer config: a lane is listed twice')
        for name in ('max_better_than_market_pct', 'max_worse_than_market_pct', 'repost_buffer_pct', 'sol_fee_reserve'):
            if float(getattr(self, name)) < 0:
                raise ValueError(f'optimizer config: {name} must be >= 0')
        if self.max_worse_than_market_pct >= 100:
            raise ValueError('optimizer config: max_worse_than_market_pct must be < 100')

    def chains(self) -> set:
        return {chain for lane in self.lanes for chain in (lane.from_chain, lane.to_chain)} | {'sol'}


# ─── Program state and pure rules ────────────────────────────────────────────


def backing_slot(backing: str) -> int:
    return BACKING_BITS[backing].bit_length() - 1


@dataclass
class ProgramView:
    """One tick's snapshot of the feed's cache: config, every quote, miner state and bond it knows, and the
    lamports of each watched wallet."""

    now: int
    config: object
    quotes: List[object]
    states: Dict[str, object]
    bonds: Dict[Tuple[str, str], object]
    lamports: Dict[str, int] = field(default_factory=dict)

    def bounds(self, backing: str) -> Tuple[int, int]:
        """Swap bounds in the backing's own unit — mirrors ``backing.rs::swap_bounds``."""
        prefix = '' if backing == 'sol' else f'{backing}_'
        return (
            int(getattr(self.config, f'{prefix}min_swap_amount', 0) or 0),
            int(getattr(self.config, f'{prefix}max_swap_amount', 0) or 0),
        )

    def eligibility_floor(self, backing: str) -> int:
        """What a purse must hold to stay eligible: the backing's activation floor (``backing.rs::activation_floor``
        — a purse under it is dropped) and enough to back a minimum swap, whichever is higher."""
        prefix = '' if backing == 'sol' else f'{backing}_'
        activation = int(getattr(self.config, f'{prefix}min_collateral', 0) or 0)
        return max(activation, required_collateral(max(self.bounds(backing)[0], 1)))

    def purse(self, miner: str, backing: str) -> int:
        """Mirrors ``backing.rs::backing_purse``: SOL collateral on Solana, else the attested locked bond."""
        if backing == 'sol':
            ms = self.states.get(miner)
            return int(ms.collateral) if ms is not None else 0
        bond = self.bonds.get((miner, backing))
        return int(bond.effective_balance) if bond is not None and bond.locked else 0

    def available_purse(self, miner: str, backing: str) -> int:
        ms = self.states.get(miner)
        reserved = int(ms.reserved_collateral[backing_slot(backing)]) if ms is not None else 0
        return max(0, self.purse(miner, backing) - reserved)

    def busy(self, miner: str, backing: str) -> bool:
        """The purse holds a reservation or an in-flight swap — one swap per purse, so it can't be taken."""
        ms = self.states.get(miner)
        if ms is None:
            return False
        bit = BACKING_BITS[backing]
        return hub_swap_on(ms, bit) or hub_busy_until(ms, bit) > self.now


def quote_qualifies(view: ProgramView, quote, lane: Lane) -> bool:
    """Crown candidacy as far as the feed shows it (``lane_eligible_hotkeys`` plus the replay's rate
    gates): purse active, strikes within the limit, hub not settling, a positive executable rate, and a
    purse that backs a minimum swap. A reserved or fulfilling miner still counts: its quote is still the
    market's best price while it is busy, and following idle miners only is what lets followers ratchet
    each other worse every time the leader takes a fill. The validators' recent-fill gate is not on
    chain, and strikes and settling locks are unknown for a miner seeded from the API until its state is
    pushed; the market tolerance covers a leader they would not crown."""
    ms = view.states.get(str(quote.miner))
    if ms is None:
        return False
    if not int(ms.active_backings) & BACKING_BITS[lane.backing]:
        return False
    if int(ms.failed_swaps) > MAX_FAILED_SWAPS:
        return False
    if view.now < int(ms.settling_until[backing_slot(lane.backing)]):
        return False
    rate = int(quote.rate)
    if rate <= 0:
        return False
    min_swap, max_swap = view.bounds(lane.backing)
    if not is_executable_rate(rate / RATE_PRECISION, lane.from_chain, lane.to_chain, min_swap, max_swap):
        return False
    return view.purse(str(quote.miner), lane.backing) >= required_collateral(max(min_swap, 1))


def other_quotes(view: ProgramView, lane: Lane, me: str) -> List[object]:
    return [
        q
        for q in view.quotes
        if (q.from_chain, q.to_chain, q.collateral_chain) == lane
        and str(q.miner) != me
        and quote_qualifies(view, q, lane)
    ]


def better_for_taker(a: int, b: int, reverse: bool) -> bool:
    """Whether canonical rate ``a`` gives the taker strictly more than ``b``."""
    return a < b if reverse else a > b


def best_other_rate(view: ProgramView, lane: Lane, me: str) -> Optional[int]:
    """The leader: the best qualifying rate on ``lane`` from any miner but ``me`` — following our own
    quote would walk it worse on every update."""
    rates = [int(q.rate) for q in other_quotes(view, lane, me)]
    if not rates:
        return None
    return min(rates) if lane.reverse else max(rates)


def quantize_toward(rate_fixed: int, up: bool) -> int:
    """Snap to ``RATE_SIG_FIGS``, rounding up or down (the contract itself floors)."""
    snapped = quantize_rate_fixed(rate_fixed)
    if up and snapped < rate_fixed:
        snapped += 10 ** (len(str(rate_fixed)) - RATE_SIG_FIGS)
    return snapped


def band_edge_fixed(anchor_fixed: int, reverse: bool) -> int:
    """``CROWN_EDGE_INSET`` inside the worse edge of the crown band around ``anchor_fixed``, snapped toward
    the inside (up for a higher-wins lane, down for a lower-wins one) so the snap can't leave the band."""
    num, den = _EDGE_BAND.numerator, _EDGE_BAND.denominator
    if reverse:
        return max(quantize_toward(anchor_fixed * (den + num) // den, up=False), anchor_fixed)
    return min(quantize_toward(-(-anchor_fixed * (den - num) // den), up=True), anchor_fixed)


def lead_rate_fixed(market_rate: float, reverse: bool, max_worse_pct: float) -> int:
    """The least generous rate the operator tolerates: ``max_worse_pct`` below market for the taker,
    snapped in the miner's favor (down for a higher-wins lane, up for a lower-wins one)."""
    keep = 1 - Decimal(str(max_worse_pct)) / 100
    market = Decimal(repr(market_rate))
    rate = market / keep if reverse else market * keep
    return quantize_toward(int(rate * RATE_PRECISION), up=reverse)


def in_crown_band(rate_fixed: int, anchor_fixed: int, reverse: bool) -> bool:
    """Whether ``rate_fixed`` shares the crown with ``anchor_fixed`` without beating it."""
    if reverse:
        return anchor_fixed <= rate_fixed <= anchor_fixed * (1 + _CROWN_BAND)
    return anchor_fixed * (1 - _CROWN_BAND) <= rate_fixed <= anchor_fixed


def deviation_pct(rate_fixed: int, market_rate: float, reverse: bool) -> float:
    """How much better (+) or worse (−) for the taker a canonical rate is than the market rate, in %."""
    rate = rate_fixed / RATE_PRECISION
    if reverse:
        return (market_rate / rate - 1) * 100
    return (rate / market_rate - 1) * 100


def max_fill_leg(view: ProgramView, miner: str, backing: str) -> int:
    """The largest backing leg a taker can reserve right now — what ``finalize_reservation`` allows:
    available purse >= 1.10 × leg, inside the backing's swap bounds. 0 when not even a minimum fits."""
    min_swap, max_swap = view.bounds(backing)
    leg = view.available_purse(miner, backing) * 10_000 // COLLATERAL_REQUIREMENT_BPS
    if max_swap:
        leg = min(leg, max_swap)
    return leg if leg >= max(min_swap, 1) else 0


def payout_for_leg(lane: Lane, rate_fixed: int, leg: int) -> int:
    """What the miner delivers (99% of ``to_amount``) on a fill whose backing leg is ``leg``."""
    if leg <= 0 or rate_fixed <= 0:
        return 0
    if lane.backing == lane.to_chain:
        to_amount = leg
    else:
        canon_from, canon_to = canonical_pair(lane.from_chain, lane.to_chain)
        to_amount = calculate_to_amount(
            leg, rate_fixed, lane.reverse, get_chain_def(canon_to).decimals, get_chain_def(canon_from).decimals
        )
    return apply_fee_deduction(to_amount, FEE_DIVISOR)


def pending_payouts(obligations, sent_keys, chain: str) -> int:
    """Payouts still owed on ``chain`` by live swaps not yet sent — wallet balance already spoken for."""
    return sum(
        apply_fee_deduction(int(s.to_amount), FEE_DIVISOR)
        for s in obligations
        if s.to_chain == chain and s.key_hex not in sent_keys
    )


def format_amount(amount: int, chain: str, places: int = 4) -> str:
    return f'{amount / 10 ** get_chain_def(chain).decimals:.{places}f} {chain.upper()}'


def collateral_command(backing: str, amount: int) -> str:
    whole = f'{amount / 10 ** get_chain_def(backing).decimals:.2f}'
    if backing == 'sol':
        return f'`alw collateral deposit --amount {whole}`'
    return f'`alw vault deposit --amount {whole}` (then `alw vault lock` if the bond is not locked)'


# ─── Operator alerts and persisted lane memory ───────────────────────────────


class WebhookNotifier:
    """Operator alerts to any webhook taking a JSON body (Discord reads ``content``, Slack ``text``; both
    are sent). An ``alert`` posts once when its condition starts or its message changes, and ``resolve``
    once when it clears; an ``event`` always posts. Everything is logged whether or not a URL is set."""

    def __init__(self, url: str, label: str):
        self.url = (url or '').strip()
        self.label = label
        self.active: Dict[str, str] = {}

    def event(self, message: str) -> None:
        bt.logging.info(f'optimizer: {message}')
        self.send(message)

    def alert(self, key: str, message: str) -> None:
        if self.active.get(key) == message:
            return
        self.active[key] = message
        bt.logging.warning(f'optimizer: {message}')
        self.send(message)

    def resolve(self, key: str, message: Optional[str] = None) -> None:
        if self.active.pop(key, None) is not None and message:
            self.event(message)

    def send(self, message: str) -> None:
        if not self.url:
            return
        text = f'[{self.label}] {message}'[:WEBHOOK_MAX_CHARS]
        try:
            requests.post(self.url, json={'content': text, 'text': text}, timeout=WEBHOOK_TIMEOUT_SECS)
        except Exception as e:
            bt.logging.debug(f'optimizer: webhook post failed: {e}')


class OptimizerState:
    """Per-lane memory that outlives a restart. A pulled quote's PDA is closed, so the addresses and
    liquidity a re-post needs live here, and ``pulled`` marks quotes the optimizer took down itself —
    only those are re-posted; a quote the operator removed stays removed."""

    def __init__(self, path: Path):
        self.path = path
        self.lanes: Dict[str, dict] = {}
        try:
            self.lanes = dict(json.loads(path.read_text()).get('lanes', {}))
        except FileNotFoundError:
            pass
        except Exception as e:
            bt.logging.warning(f'optimizer: state file {path} unreadable ({e}); starting fresh')

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix('.tmp')
        tmp.write_text(json.dumps({'lanes': self.lanes}, indent=2, sort_keys=True))
        tmp.replace(self.path)

    def seen_live(self, lane: Lane, quote) -> None:
        record = {
            'from_addr': str(quote.miner_from_addr),
            'to_addr': str(quote.miner_to_addr),
            'liquidity': int(quote.liquidity),
            'pulled': False,
        }
        if self.lanes.get(lane.key) != record:
            self.lanes[lane.key] = record
            self.save()

    def mark_pulled(self, lane: Lane, reason: str, now: int) -> None:
        record = self.lanes.get(lane.key)
        if record is not None:
            record.update(pulled=True, reason=reason, pulled_at=now)
            self.save()

    def mark_reposted(self, lane: Lane) -> None:
        record = self.lanes.get(lane.key)
        if record is not None:
            self.lanes[lane.key] = {k: record[k] for k in ('from_addr', 'to_addr', 'liquidity')} | {'pulled': False}
            self.save()

    def pulled(self, lane: Lane) -> Optional[dict]:
        record = self.lanes.get(lane.key)
        return record if record is not None and record.get('pulled') else None

    def live(self, lane: Lane) -> bool:
        record = self.lanes.get(lane.key)
        return record is not None and not record.get('pulled')

    def forget(self, lane: Lane) -> None:
        if self.lanes.pop(lane.key, None) is not None:
            self.save()


# ─── The optimizer ───────────────────────────────────────────────────────────


@dataclass
class Target:
    """Where a lane should quote. ``mode`` is 'follow' (inset inside the leader's band), 'lead' (the least
    generous tolerated rate), or None with ``reason`` saying why it shouldn't quote."""

    rate_fixed: Optional[int]
    anchor_fixed: Optional[int]
    mode: Optional[str]
    reason: str


@dataclass
class Funding:
    """One lane's slice of its delivery wallet. ``need`` includes every lane ahead of it on the wallet."""

    fits: bool
    fits_with_buffer: bool
    chain: str
    address: str
    balance: Optional[int]
    need: int
    leg: int
    fee_payer_short: bool

    @property
    def balance_known(self) -> bool:
        """A reading above 0 — a 0 may be a failed read, and never justifies a fee on its own."""
        return bool(self.balance)


class QuoteOptimizer:
    """Runs from the miner's forward loop. ``tick`` watches the feed on every call (the dead-man switch) and
    decides once per ``OPTIMIZER_TICK_SECONDS``."""

    def __init__(
        self,
        cfg: OptimizerConfig,
        solana_client,
        assets: Dict[str, object],
        hotkey: str,
        state_path: Path,
        pending_payouts_fn: Callable[[str], int],
        feed,
        own_state: Callable[[], object],
        on_quote_posted: Optional[Callable[[Lane, str, str], None]] = None,
        is_registered: Callable[[], bool] = lambda: True,
        api: Optional[AllwaysApi] = None,
        clock: Callable[[], float] = time.time,
        prices: Optional[MarketPrices] = None,
        notifier: Optional[WebhookNotifier] = None,
        rng: Optional[random.Random] = None,
    ):
        self.cfg = cfg
        self.client = solana_client
        self.assets = assets
        self.hotkey = hotkey
        self.me = str(solana_client.keypair.pubkey())
        self.state = OptimizerState(state_path)
        self.pending_payouts = pending_payouts_fn
        self.feed = feed
        self.own_state = own_state
        self.on_quote_posted = on_quote_posted or (lambda _lane, _from_addr, _to_addr: None)
        self.is_registered = is_registered
        self.api = api
        self.clock = clock
        self.prices = prices or MarketPrices(pins=cfg.price_usd)
        self.notifier = notifier or WebhookNotifier(cfg.webhook_url, f'allways miner {hotkey[:8]}')
        self.reserve_lamports = int(round(cfg.sol_fee_reserve * 10 ** get_chain_def('sol').decimals))
        self.last_tick = 0
        self.started = False
        self.seeded_generation: Optional[int] = None
        self.not_live_since: Optional[int] = None
        self.dead_man_at: Optional[int] = None
        self.short_ticks: Dict[Lane, int] = defaultdict(int)
        self.failed_swaps: Optional[int] = None
        self.eligibility_due: Dict[Lane, int] = {}
        # Each lane's mode ('follow' / 'lead' / None), logged when it changes.
        self.modes: Dict[Lane, Optional[str]] = {}
        self.rng = rng or random.Random()
        # Each lane's jittered free-update moment, keyed to the quote's updated_at it was drawn for.
        self.requote_at: Dict[Lane, Tuple[int, int]] = {}
        # The earliest jittered requote waiting on a pass; ``tick`` runs one then instead of at the next regular tick.
        self.wake_at: Optional[int] = None
        self.rpc_calls = 0
        self.usage_mark: Optional[Tuple[int, int, int, int]] = None
        self.count_rpc_calls()

    def count_rpc_calls(self) -> None:
        """Count every RPC call this optimizer's client makes — it has its own client, so the hourly line is the
        optimizer's alone."""
        rpc = getattr(self.client, 'rpc', None)
        call = getattr(rpc, '_call', None)
        if call is None:
            return

        def counted(method, params):
            self.rpc_calls += 1
            return call(method, params)

        rpc._call = counted

    # ─── lifecycle ───

    def start(self) -> None:
        self.last_tick = int(self.clock())  # a failed start is retried a full tick later
        self.try_start(self.last_tick)

    def try_start(self, now: int) -> bool:
        """Start only once the allways API answers: without it there is nothing to seed from. The miner keeps
        fulfilling swaps meanwhile; ``tick`` retries every ``OPTIMIZER_TICK_SECONDS``."""
        if self.api is None or not self.api.health():
            self.notifier.alert(
                'api_down',
                'quote optimizer waiting for the allways API (das) — not started yet; the miner still fulfils swaps',
            )
            return False
        self.notifier.resolve('api_down')
        self.started = True
        self.not_live_since = now
        self.feed.start()
        mode = ' in DRY RUN (no transactions)' if self.cfg.dry_run else ''
        self.notifier.event(
            f'quote optimizer started{mode} — lanes {", ".join(lane.label for lane in self.cfg.lanes)}; '
            f'follows the crown from -{self.cfg.max_worse_than_market_pct:g}% to '
            f'+{self.cfg.max_better_than_market_pct:g}% of market'
        )
        return True

    def tick(self) -> None:
        now = int(self.clock())
        if self.started:
            self.watch_feed(now)
        due = self.wake_at is not None and now >= self.wake_at
        if now - self.last_tick < OPTIMIZER_TICK_SECONDS and not due:
            return
        self.last_tick = now
        self.wake_at = None
        try:
            self.step(now)
        except Exception as e:
            self.notifier.alert('tick_error', f'optimizer tick failed: {type(e).__name__}: {e}')
        else:
            self.notifier.resolve('tick_error', 'optimizer ticks recovered')

    def step(self, now: int) -> None:
        if not self.started and not self.try_start(now):
            return
        self.log_usage(now)
        if not self.feed.live:
            return
        if self.seeded_generation != self.feed.generation and not self.seed(now):
            if self.seeded_generation is not None:
                self.run_once(now, hold=True)  # a stale picture still justifies a funding pull, nothing else
            return
        self.run_once(now)

    def shutdown(self, reason: str) -> None:
        """Pull every managed quote (a stopped miner still quoting is a strike waiting to happen) and say so.
        The pulls are marked, so the next start re-posts them."""
        pulled = []
        if self.cfg.pull_on_shutdown and not self.cfg.dry_run:
            now = int(self.clock())
            for lane in self.managed_live_lanes():
                if self.pull(lane, f'shutdown: {reason}', now):
                    pulled.append(lane.label)
        tail = f'; pulled {", ".join(pulled)} (re-posted on restart)' if pulled else ''
        self.notifier.event(f'quote optimizer stopped ({reason}){tail}')

    # ─── feed health ───

    def watch_feed(self, now: int) -> None:
        """The dead-man switch: blind for more than ``DEAD_MAN_SECS``, pull every managed quote (always pays —
        a quote nobody is watching is worse than the fee), keep reconnecting, re-seed and re-post on recovery."""
        live = self.feed.live
        log_on_change(
            'optimizer.feed',
            live,
            'optimizer: feed live' if live else 'optimizer: feed down — holding every quote while it reconnects',
        )
        if live:
            self.not_live_since = None
            if self.dead_man_at is not None:
                self.dead_man_at = None
                self.notifier.resolve(
                    'dead_man', 'optimizer feed recovered — re-seeding, then re-posting what the dead-man switch pulled'
                )
            return
        if self.not_live_since is None:
            self.not_live_since = now
        if self.dead_man_at is None and now - self.not_live_since > DEAD_MAN_SECS:
            self.dead_man_pull(now)

    def dead_man_pull(self, now: int) -> None:
        self.dead_man_at = now
        lanes = self.managed_live_lanes()
        if self.cfg.dry_run:
            done = [lane.label for lane in lanes]
            verb = '[dry run] would pull'
        else:
            done = [lane.label for lane in lanes if self.pull(lane, 'dead-man switch: optimizer feed down', now)]
            verb = 'pulled'
        what = f'{verb} {", ".join(done)}; re-posted once it recovers' if done else 'no live quotes to pull'
        self.notifier.alert(
            'dead_man', f'optimizer feed down for over {DEAD_MAN_SECS // 60} min (dead-man switch) — {what}'
        )

    def managed_live_lanes(self) -> List[Lane]:
        """Managed lanes with a quote standing, as far as the cache or the state file knows."""
        _config, quotes, _states, _bonds, _lamports = self.feed.snapshot()
        mine = {Lane(q.from_chain, q.to_chain, q.collateral_chain): q for q in quotes if str(q.miner) == self.me}
        lanes = []
        for lane in self.cfg.lanes:
            if lane in mine:
                self.state.seen_live(lane, mine[lane])
                lanes.append(lane)
            elif self.state.live(lane):
                lanes.append(lane)
        return lanes

    def log_usage(self, now: int) -> None:
        """Hourly: what the optimizer cost on the Helius meter (websocket bytes, connections, RPC calls)."""
        mark = (now, int(self.feed.bytes_received), int(self.feed.connections_opened), self.rpc_calls)
        if self.usage_mark is None:
            self.usage_mark = mark
            return
        if now - self.usage_mark[0] < USAGE_LOG_SECS:
            return
        received, opened, calls = (mark[i] - self.usage_mark[i] for i in (1, 2, 3))
        credits = opened + -(-received // WS_CREDIT_BYTES) + calls
        bt.logging.info(
            f'optimizer: last hour {received / 1e6:.3f} MB websocket received, {opened} connection(s) opened, '
            f'{calls} RPC call(s) — about {credits} Helius credits'
        )
        self.usage_mark = mark

    # ─── seeding ───

    def seed(self, now: int) -> bool:
        """A starting picture after the feed (re)connects: other miners from ``GET /miners``, our own quotes'
        last change from the rate history, our MinerState from the miner's own read; Config and SOL balances
        are the only RPC reads. False (hold) when the API or our state can't answer yet."""
        generation = self.feed.generation
        rows = self.api.miners() if self.api is not None else None
        history = self.api.rate_history(self.hotkey) if rows is not None else None
        own_state = self.own_state()
        if rows is None or history is None or own_state is None:
            missing = "this miner's MinerState" if rows is not None and history is not None else 'the allways API'
            log_on_change(
                'optimizer.seed',
                missing,
                f'optimizer: waiting for {missing} to seed its picture — holding everything but funding pulls',
            )
            return False
        config = self.rpc_read('program config', self.client.get_config)
        if config is None and self.feed.snapshot()[0] is None:
            return False
        quotes, states, bonds = market_from_das(rows, self.cfg.lanes, now)
        quotes = self.own_quotes(quotes, history, now)
        states[self.me] = own_state
        wallets = {self.me} | {str(q.miner_to_addr) for q in quotes if str(q.miner) == self.me and q.to_chain == 'sol'}
        wallets |= {r['to_addr'] for key, r in self.state.lanes.items() if key.split(':')[1] == 'sol'}
        lamports = {}
        for address in sorted(wallets):
            self.feed.watch_wallet(address)
            lamports[address] = self.rpc_read(
                f'SOL balance of {address}', lambda a=address: self.client.rpc.get_balance(a)
            )
        self.feed.seed(quotes, states, bonds, config=config, lamports=lamports)
        self.seeded_generation = generation
        log_on_change('optimizer.seed', None, 'optimizer: seeded from the allways API; following the feed')
        return True

    def own_quotes(self, quotes: List[object], history: List[dict], now: int) -> List[object]:
        """Our own quotes' rate and ``updated_at`` from the rate history (``t`` is the QuoteSet block time, so it
        is exact, and it is indexed as it lands, so it is newer than ``/miners``). A lane with no history in the
        window is assumed updated right now — never free early; a lane whose last change is a removal is gone."""
        latest: Dict[Lane, dict] = {}
        for row in history:
            if not isinstance(row, dict):
                continue
            lane = Lane(row.get('fromChain'), row.get('toChain'), row.get('backing') or 'sol')
            if lane in self.cfg.lanes and int(row.get('t') or 0) >= int(latest.get(lane, {}).get('t') or 0):
                latest[lane] = row
        others = [q for q in quotes if str(q.miner) != self.me]
        own = {Lane(q.from_chain, q.to_chain, q.collateral_chain): q for q in quotes if str(q.miner) == self.me}
        for lane in self.cfg.lanes:
            row, quote = latest.get(lane), own.get(lane)
            if row is None:
                if quote is not None:
                    quote.updated_at = now
                continue
            rate = fixed_rate(row.get('rate'))
            if rate <= 0:
                own.pop(lane, None)
                continue
            if quote is None:
                record = self.state.lanes.get(lane.key)
                if record is None:
                    continue  # addresses unknown until the quote's next push
                quote = own[lane] = make_quote(
                    self.me, lane, record['from_addr'], record['to_addr'], rate, record.get('liquidity', 0)
                )
            quote.rate, quote.updated_at = rate, int(row['t'])
        return others + list(own.values())

    def rpc_read(self, what: str, read: Callable[[], object]):
        try:
            return read()
        except Exception as e:
            bt.logging.warning(f'optimizer: {what} read failed: {e}')
            return None

    # ─── one pass ───

    def read_view(self, now: int) -> ProgramView:
        config, quotes, states, bonds, lamports = self.feed.snapshot()
        return ProgramView(now=now, config=config, quotes=quotes, states=states, bonds=bonds, lamports=lamports)

    def run_once(self, now: int, hold: bool = False) -> None:
        view = self.read_view(now)
        if view.config is None:
            log_on_change('optimizer.config', False, 'optimizer: no program config yet — holding every quote')
            return
        halted = bool(view.config.halted)
        log_on_change(
            'optimizer.halted', halted, f'optimizer: program {"halted — holding every quote" if halted else "live"}'
        )
        if halted:
            return
        ms = view.states.get(self.me)
        if ms is None:
            self.notifier.alert('no_miner_state', 'no MinerState for this miner yet — post collateral first')
            return
        self.notifier.resolve('no_miner_state')
        strikes = int(ms.failed_swaps)
        self.watch_strikes(strikes)
        self.prices.refresh(sorted(self.cfg.chains()), now)
        self.watch_prices(now)
        self.watch_collateral(view)

        mine = {}
        for q in view.quotes:
            lane = Lane(q.from_chain, q.to_chain, q.collateral_chain)
            if str(q.miner) == self.me and lane in self.cfg.lanes:
                mine[lane] = q
                self.state.seen_live(lane, q)
        for lane in self.cfg.lanes:
            if not hold and lane not in mine and self.state.live(lane):
                self.state.forget(lane)
                self.eligibility_due.pop(lane, None)
                self.notifier.event(
                    f'{lane.label} was removed outside the optimizer — unmanaged until you post it again'
                )

        targets = {lane: self.target_for(view, lane, now) for lane in self.cfg.lanes}
        self.watch_modes(targets)
        funding = self.plan_funding(view, mine, targets)
        for lane in self.cfg.lanes:
            if lane in mine:
                self.manage_live(view, lane, mine[lane], targets[lane], funding.get(lane), strikes, hold)
            elif not hold and self.state.pulled(lane) is not None:
                self.maybe_repost(view, lane, targets[lane], funding.get(lane))
        self.watch_eligibility(view, mine)

    def market_rate(self, lane: Lane, now: int) -> Optional[float]:
        """The USD spot rate in the lane's canonical unit (dest per 1 canonical source)."""
        canon_from, canon_to = canonical_pair(lane.from_chain, lane.to_chain)
        usd_from, usd_to = self.prices.usd(canon_from, now), self.prices.usd(canon_to, now)
        if usd_from is None or usd_to is None:
            return None
        return usd_from / usd_to

    def target_for(self, view: ProgramView, lane: Lane, now: int) -> Target:
        market = self.market_rate(lane, now)
        if market is None:
            return Target(None, None, None, NO_PRICE_REASON)
        worse, better = self.cfg.max_worse_than_market_pct, self.cfg.max_better_than_market_pct
        anchor = best_other_rate(view, lane, self.me)
        if anchor is not None:
            edge = band_edge_fixed(anchor, lane.reverse)
            dev = deviation_pct(edge, market, lane.reverse)
            if dev > better:
                return Target(None, anchor, None, f'the crown is past +{better:g}% of market — not following')
            if dev >= -worse:
                return Target(edge, anchor, 'follow', 'following the crown')
        why = 'no crown to follow' if anchor is None else f'the crown is below -{worse:g}% of market'
        return Target(lead_rate_fixed(market, lane.reverse, worse), anchor, 'lead', f'{why}; leading at -{worse:g}%')

    def at_target(self, rate: int, target: Target, lane: Lane) -> bool:
        if target.mode == 'follow':
            # Anywhere from our inset edge up to the leader: in the band with the cushion intact.
            return not better_for_taker(target.rate_fixed, rate, lane.reverse) and not better_for_taker(
                rate, target.anchor_fixed, lane.reverse
            )
        return abs(Fraction(rate, target.rate_fixed) - 1) <= _LEAD_HOLD

    def plan_funding(
        self, view: ProgramView, mine: Dict[Lane, object], targets: Dict[Lane, Target]
    ) -> Dict[Lane, Funding]:
        """Share each delivery wallet across the lanes paying out of it: live lanes claim it first, in
        config order, each at its largest fundable fill; pulled lanes are re-postable only into what is
        left, with the buffer on top. A busy purse can't be taken again until its swap resolves, so its
        lanes claim nothing past the payout already in flight. The SOL wallet also keeps the fee reserve —
        without it no ``mark_fulfilled`` lands, so a short fee payer fails every lane."""
        groups: Dict[Tuple[str, str], List[Tuple[Lane, int, bool]]] = defaultdict(list)
        for lane in self.cfg.lanes:
            quote = mine.get(lane)
            if quote is not None:
                groups[(lane.to_chain, str(quote.miner_to_addr))].append((lane, int(quote.rate), True))
                continue
            record = self.state.pulled(lane)
            if record is not None and targets[lane].rate_fixed is not None:
                groups[(lane.to_chain, record['to_addr'])].append((lane, targets[lane].rate_fixed, False))

        fee_payer = self.balance(view, 'sol', self.me)
        fee_payer_short = fee_payer is not None and fee_payer < self.reserve_lamports
        buffer = 1 + Fraction(str(self.cfg.repost_buffer_pct)) / 100
        out: Dict[Lane, Funding] = {}
        for (chain, address), entries in groups.items():
            balance = self.balance(view, chain, address)
            committed = self.pending_payouts(chain) + (self.reserve_lamports if chain == 'sol' else 0)
            for lane, rate, live in sorted(entries, key=lambda e: not e[2]):
                leg = 0 if view.busy(self.me, lane.backing) else max_fill_leg(view, self.me, lane.backing)
                total = committed + payout_for_leg(lane, rate, leg)
                usable = balance is not None and not fee_payer_short
                fits = usable and total <= balance
                fits_with_buffer = usable and total * buffer <= balance
                out[lane] = Funding(
                    fits=fits,
                    fits_with_buffer=fits_with_buffer,
                    chain=chain,
                    address=address,
                    balance=balance,
                    need=total,
                    leg=leg,
                    fee_payer_short=fee_payer_short,
                )
                if fits if live else fits_with_buffer:
                    committed = total
        return out

    def manage_live(
        self,
        view: ProgramView,
        lane: Lane,
        quote,
        target: Target,
        funding: Optional[Funding],
        strikes: int,
        hold: bool = False,
    ) -> None:
        """The decision map for a standing quote. Only a free routine requote (R1) ever touches a busy purse,
        and a hold (a re-seed the API couldn't answer) allows only a funding pull (P1)."""
        rate = int(quote.rate)
        fee = quote_update_fee_lamports(view.now - int(quote.updated_at))
        busy = view.busy(self.me, lane.backing)
        funded = funding is not None and funding.fits
        better = self.cfg.max_better_than_market_pct
        if funded:
            self.short_ticks.pop(lane, None)
            self.notifier.resolve(f'funds:{lane.key}', f'{lane.label}: wallet covers a full-size fill again')
        else:
            self.short_ticks[lane] += 1
            self.notifier.alert(f'funds:{lane.key}', f'{lane.label}: {self.funding_reason(funding)}')

        # Market drift: our quote became more generous than the tolerance allows.
        market = None if hold else self.market_rate(lane, view.now)
        drift = deviation_pct(rate, market, lane.reverse) if market is not None else None
        if drift is not None and drift > better:
            reason = f'market moved; the quote is past +{better:g}% of market'
            if target.rate_fixed is not None and funded:
                if fee == 0:  # R1
                    self.send_quote(view, lane, quote, target.rate_fixed, reason, 0)
                elif not busy and self.drift_worth_paying(view, lane, rate, drift, fee):  # R2
                    self.send_quote(view, lane, quote, target.rate_fixed, reason, fee)
                else:
                    self.defer(
                        lane, quote, f'the quote is past +{better:g}% of market, but a paid requote is not worth it'
                    )
                return
            why = target.reason if target.rate_fixed is None else self.funding_reason(funding)
            if not busy and (fee == 0 or self.drift_worth_paying(view, lane, rate, drift, fee)):  # P2
                self.remove(view, lane, quote, f'{reason}; {why}', fee)
                return
            if funded:
                self.defer(lane, quote, f'the quote is past +{better:g}% of market, but a pull now is not worth it')
                return

        # Funding: a quote the wallet can't honour at full size is pulled (P1).
        if not funded:
            if self.short_ticks[lane] < FUNDING_SHORT_CONFIRM_TICKS:
                return
            if busy:
                self.defer(lane, quote, 'the wallet is short, but the purse is busy and cannot be taken')
            elif self.funding_pull_pays(lane, funding, fee, strikes):
                self.remove(view, lane, quote, self.funding_reason(funding), fee)
            else:
                self.defer(lane, quote, 'the wallet is short, but a pull now costs more than it protects')
            return
        if hold:
            return

        # Routine (R1): follow or lead, free only.
        if target.rate_fixed is None:
            log_on_change(
                f'optimizer.hold.{lane.key}',
                target.reason,
                f'optimizer: {lane.label} holding {rate / RATE_PRECISION:g} — {target.reason}',
            )
            return
        if self.at_target(rate, target, lane):
            log_on_change(
                f'optimizer.hold.{lane.key}',
                (target.mode, target.anchor_fixed),
                f'optimizer: {lane.label} at target ({target.reason})',
            )
            return
        free_at = self.requote_free_at(lane, quote)
        if fee or view.now < free_at:
            self.wake_at = free_at if self.wake_at is None else min(self.wake_at, free_at)
            log_on_change(
                f'optimizer.wait.{lane.key}',
                target.rate_fixed,
                f'optimizer: {lane.label} wants {target.rate_fixed / RATE_PRECISION:g}; waiting for the free update',
            )
            return
        self.send_quote(view, lane, quote, target.rate_fixed, target.reason, 0)

    def requote_free_at(self, lane: Lane, quote) -> int:
        """When a routine requote may go: the free window plus a random delay, drawn once per quote version."""
        updated_at = int(quote.updated_at)
        drawn = self.requote_at.get(lane)
        if drawn is None or drawn[0] != updated_at:
            jitter = self.rng.randint(0, REQUOTE_JITTER_SECS)
            drawn = self.requote_at[lane] = (updated_at, updated_at + QUOTE_UPDATE_FREE_AFTER_SECS + jitter)
        return drawn[1]

    def maybe_repost(self, view: ProgramView, lane: Lane, target: Target, funding: Optional[Funding]) -> None:
        """C1: a lane the optimizer pulled comes back, free, once it has a target, an active idle purse and a
        wallet that covers a full-size fill with the buffer on top."""
        record = self.state.pulled(lane)
        if target.rate_fixed is None:
            log_on_change(
                f'optimizer.repost.{lane.key}', target.reason, f'optimizer: {lane.label} stays pulled — {target.reason}'
            )
            return
        ms = view.states.get(self.me)
        if not int(ms.active_backings) & BACKING_BITS[lane.backing]:
            self.notifier.alert(
                f'inactive:{lane.backing}', f'your {lane.backing} purse is not active — cannot re-post {lane.label}'
            )
            return
        self.notifier.resolve(f'inactive:{lane.backing}')
        if view.busy(self.me, lane.backing):
            log_on_change(
                f'optimizer.repost.{lane.key}', 'busy', f'optimizer: {lane.label} stays pulled while its purse is busy'
            )
            return
        if funding is None or not funding.fits_with_buffer:
            self.notifier.alert(
                f'funds:{lane.key}', f'{lane.label} stays pulled: {self.funding_reason(funding, buffered=True)}'
            )
            return
        self.notifier.resolve(f'funds:{lane.key}')
        posted = make_quote(
            self.me, lane, record['from_addr'], record['to_addr'], target.rate_fixed, int(record.get('liquidity', 0))
        )
        if self.send_quote(view, lane, posted, target.rate_fixed, target.reason, 0, repost=True):
            self.state.mark_reposted(lane)

    # ─── paying for protection ───

    def funding_pull_pays(self, lane: Lane, funding: Optional[Funding], fee: int, strikes: int) -> bool:
        if fee == 0 or strikes >= MAX_FAILED_SWAPS:
            return True  # free, or the next strike ends the hotkey's emissions
        if funding is None or not funding.balance_known:
            return False  # a 0 balance may be a failed read
        risk = self.to_lamports(funding.leg * TIMEOUT_PREMIUM_BPS // 10_000, lane.backing)
        return risk is not None and fee <= risk

    def drift_worth_paying(self, view: ProgramView, lane: Lane, rate: int, drift: float, fee: int) -> bool:
        """Pay to fix a drifted quote only if a taker would pick it first (no takeable quote beats it) and a
        full-size fill would lose more past the tolerated edge than the fee."""
        for q in other_quotes(view, lane, self.me):
            if not view.busy(str(q.miner), lane.backing) and better_for_taker(int(q.rate), rate, lane.reverse):
                return False
        value = self.to_lamports(max_fill_leg(view, self.me, lane.backing), lane.backing)
        return value is not None and value * (drift - self.cfg.max_better_than_market_pct) / 100 >= fee

    def defer(self, lane: Lane, quote, why: str) -> None:
        free_at = time.strftime('%H:%M UTC', time.gmtime(int(quote.updated_at) + QUOTE_UPDATE_FREE_AFTER_SECS))
        log_on_change(
            f'optimizer.defer.{lane.key}', why, f'optimizer: {lane.label} not acting yet — {why}; free at {free_at}'
        )

    # ─── transactions ───

    def pull(self, lane: Lane, reason: str, now: int) -> bool:
        """Send ``remove_quote`` and mark the lane for a re-post. Marked first: if the transaction lands but the
        call raises, the lane is still re-postable, and the next push or seed clears the mark if it didn't land.
        A quote that is already gone was removed by someone else, so its lane is forgotten instead."""
        self.state.mark_pulled(lane, reason, now)
        try:
            self.client.remove_quote(lane.from_chain, lane.to_chain, backing=lane.backing)
        except Exception as e:
            if benign_marker(e, QUOTE_GONE_ERRORS):
                self.state.forget(lane)
                self.feed.drop_quote(self.me, lane)
                bt.logging.info(f'optimizer: {lane.label} was already gone; unmanaged until you post it again')
                return False
            self.notifier.alert(f'tx_failed:{lane.key}', f'failed to pull {lane.label}: {e}')
            return False
        self.notifier.resolve(f'tx_failed:{lane.key}')
        self.feed.drop_quote(self.me, lane)
        self.short_ticks.pop(lane, None)
        self.eligibility_due.pop(lane, None)
        return True

    def remove(self, view: ProgramView, lane: Lane, quote, reason: str, fee: int) -> None:
        fee_note = f' (paid {fee / 1e9:g} SOL churn fee)' if fee else ''
        if self.cfg.dry_run:
            self.notifier.alert(f'dry:{lane.key}', f'[dry run] would pull {lane.label}: {reason}{fee_note}')
            return
        if self.pull(lane, reason, view.now):
            self.notifier.event(f'pulled {lane.label}: {reason}{fee_note}')

    def send_quote(
        self,
        view: ProgramView,
        lane: Lane,
        quote,
        rate_fixed: int,
        reason: str,
        fee: int,
        repost: bool = False,
    ) -> bool:
        from_addr, to_addr, liquidity = str(quote.miner_from_addr), str(quote.miner_to_addr), int(quote.liquidity)
        fee_note = f', paid {fee / 1e9:g} SOL churn fee' if fee else ''
        if repost:
            message = f'posted {lane.label} at {rate_fixed / RATE_PRECISION:g} ({reason})'
        else:
            message = (
                f'requoted {lane.label}: {int(quote.rate) / RATE_PRECISION:g} -> '
                f'{rate_fixed / RATE_PRECISION:g} ({reason}{fee_note})'
            )
        # A free requote is routine and only logged; posts and paid requotes are worth a ping.
        routine = not repost and not fee
        if self.cfg.dry_run:
            if routine:
                log_on_change(f'optimizer.dry.{lane.key}', rate_fixed, f'optimizer: [dry run] would have {message}')
            else:
                self.notifier.alert(f'dry:{lane.key}', f'[dry run] would have {message}')
            return False
        try:
            self.client.set_quote(
                lane.from_chain, lane.to_chain, from_addr, to_addr, rate_fixed, liquidity, backing=lane.backing
            )
        except Exception as e:
            self.notifier.alert(f'tx_failed:{lane.key}', f'failed to post {lane.label}: {e}')
            return False
        self.notifier.resolve(f'tx_failed:{lane.key}')
        self.feed.note_quote(
            make_quote(self.me, lane, from_addr, to_addr, rate_fixed, liquidity, view.now + TX_LANDING_MARGIN_SECS)
        )
        if routine:
            bt.logging.info(f'optimizer: {message}')
        else:
            self.notifier.event(message)
        if repost:
            self.on_quote_posted(lane, from_addr, to_addr)
        self.eligibility_due[lane] = view.now + ELIGIBILITY_CHECK_DELAY_SECS
        return True

    def describe_mode(self, mode: Optional[str]) -> str:
        if mode == 'follow':
            return 'following the crown'
        if mode == 'lead':
            return f'leading at -{self.cfg.max_worse_than_market_pct:g}%'
        return 'not following'

    def watch_modes(self, targets: Dict[Lane, Target]) -> None:
        """Log when a lane switches between following, leading and not following. A missing price is its own
        alert, so it doesn't count as a switch."""
        for lane, target in targets.items():
            if target.reason == NO_PRICE_REASON:
                continue
            if lane in self.modes and self.modes[lane] != target.mode:
                bt.logging.info(f'optimizer: {lane.label} is now {self.describe_mode(target.mode)}: {target.reason}')
            self.modes[lane] = target.mode

    # ─── helpers ───

    def balance(self, view: ProgramView, chain: str, address: str) -> Optional[int]:
        """SOL from the feed's pushed lamports (a wallet seen for the first time is read once); other chains
        from their provider, which never touches the Solana RPC."""
        if chain == 'sol':
            if self.feed.watch_wallet(address):
                balance = self.rpc_read(f'SOL balance of {address}', lambda: self.client.rpc.get_balance(address))
                if balance is not None:
                    self.feed.set_lamports(address, balance)
                    view.lamports[address] = int(balance)
            return view.lamports.get(address)
        provider = self.assets.get(chain)
        if provider is None:
            return None
        return int(provider.get_balance(address))

    def to_lamports(self, amount: int, chain: str) -> Optional[int]:
        if chain == 'sol':
            return amount
        usd_chain, usd_sol = self.prices.usd(chain), self.prices.usd('sol')
        if usd_chain is None or usd_sol is None:
            return None
        scale = 10 ** (get_chain_def('sol').decimals - get_chain_def(chain).decimals)
        return int(amount * usd_chain / usd_sol * scale)

    def funding_reason(self, funding: Optional[Funding], buffered: bool = False) -> str:
        if funding is None:
            return 'no delivery wallet to check'
        if funding.fee_payer_short:
            return f'SOL wallet {self.me} is below the {self.cfg.sol_fee_reserve:g} SOL fee reserve — send SOL to it'
        if funding.balance is None:
            return f'no balance reading yet for {funding.chain.upper()} wallet {funding.address}'
        need = int(funding.need * (1 + self.cfg.repost_buffer_pct / 100)) if buffered else funding.need
        short = max(0, need - funding.balance)
        return (
            f'{funding.chain.upper()} wallet {funding.address} holds {format_amount(funding.balance, funding.chain)}; '
            f'a full-size fill plus what it already owes needs {format_amount(need, funding.chain)} — '
            f'send at least {format_amount(short, funding.chain)}'
        )

    def watch_strikes(self, failed: int) -> None:
        if self.failed_swaps is not None and failed > self.failed_swaps:
            left = MAX_FAILED_SWAPS - failed
            if left < 0:
                tail = 'this hotkey no longer earns emissions'
            elif left == 0:
                tail = "the next one ends this hotkey's emissions"
            else:
                tail = f'{left} more allowed'
            self.notifier.event(f'timeout strike recorded — {failed} lifetime ({tail})')
        self.failed_swaps = failed

    def watch_prices(self, now: int) -> None:
        missing = [c for c in sorted(self.cfg.chains()) if self.prices.usd(c, now) is None]
        if missing:
            self.notifier.alert('prices', f'no fresh market price for {", ".join(missing)} — holding every quote')
        else:
            self.notifier.resolve('prices', 'market prices recovered')

    def watch_collateral(self, view: ProgramView) -> None:
        """A purse under its eligibility floor earns nothing (the contract refuses fills it can't back, so it
        is no strike risk). Say how much to add, and how."""
        for backing in sorted({lane.backing for lane in self.cfg.lanes}):
            purse = view.purse(self.me, backing)
            floor = view.eligibility_floor(backing)
            if purse < floor:
                self.notifier.alert(
                    f'collateral:{backing}',
                    f'your {backing} purse holds {format_amount(purse, backing, 2)}, under the '
                    f'{format_amount(floor, backing, 2)} needed to stay eligible — add '
                    f'{format_amount(floor - purse, backing, 2)}: {collateral_command(backing, floor - purse)}',
                )
            else:
                self.notifier.resolve(
                    f'collateral:{backing}', f'your {backing} purse is back above its eligibility floor'
                )

    def watch_eligibility(self, view: ProgramView, mine: Dict[Lane, object]) -> None:
        """After a quote is posted (and while it stays ineligible), check whether validators would pay it,
        and say why not."""
        for lane, due_at in list(self.eligibility_due.items()):
            if due_at > view.now:
                continue
            if lane not in mine:
                self.eligibility_due.pop(lane)
                continue
            reasons = self.ineligibility_reasons(view, lane, mine[lane])
            key = f'ineligible:{lane.key}'
            if reasons:
                self.notifier.alert(key, f'{lane.label} is live but not earning emissions: {"; ".join(reasons)}')
                self.eligibility_due[lane] = view.now + ELIGIBILITY_RECHECK_SECS
            else:
                self.notifier.resolve(key, f'{lane.label} is eligible for emissions again')
                self.eligibility_due.pop(lane)

    def ineligibility_reasons(self, view: ProgramView, lane: Lane, quote) -> List[str]:
        backing = lane.backing
        ms = view.states.get(self.me)
        reasons = []
        if not self.is_registered():
            reasons.append('the hotkey is not registered on the subnet')
        failed = int(ms.failed_swaps)
        if failed > MAX_FAILED_SWAPS:
            reasons.append(f'{failed} lifetime timeout strikes (limit {MAX_FAILED_SWAPS}); re-register to earn again')
        if not int(ms.active_backings) & BACKING_BITS[backing]:
            reasons.append(f'the {backing} purse is not active (`alw miner activate --backing {backing}`)')
        if view.now < int(ms.settling_until[backing_slot(backing)]):
            reasons.append(f'the {backing} purse is settling a timeout')
        min_swap, max_swap = view.bounds(backing)
        floor = view.eligibility_floor(backing)
        if view.purse(self.me, backing) < floor:
            reasons.append(f'the {backing} purse is under its {format_amount(floor, backing, 2)} eligibility floor')
        if not is_executable_rate(int(quote.rate) / RATE_PRECISION, lane.from_chain, lane.to_chain, min_swap, max_swap):
            reasons.append('the quoted rate cannot route under the swap bounds')
        if self.api is not None:
            fills = self.api.last_fill_times(self.hotkey)
            if fills is not None:
                last = fills.get(backing, 0)
                if view.now - last > ELIGIBILITY_FILL_WINDOW_SECS:
                    reasons.append(
                        f'no completed swap on the {backing} purse in the last {ELIGIBILITY_FILL_WINDOW_SECS // 3600}h; '
                        'validators credit crown only after one fills'
                    )
            live = self.api.live_lanes()
            if live is not None and live.get(tuple(lane)) is False:
                reasons.append('the pair had no qualified fill in the pool window, so its pool pays 0')
        return reasons
